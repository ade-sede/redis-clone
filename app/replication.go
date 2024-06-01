package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func generateReplId() string {
	bytes := make([]byte, 20)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

var replicationInfo struct {
	replicaof            string
	replicaMasterAddress string
	replicaMasterPort    int
	replicaMasterHost    string
	masterReplId         string
	masterReplOffset     int
	masterConnection     *net.Conn
}

// Handshake for replication is done in 3 steps:
// 1. slave sends `PING` to master
// 2. slave sends `REPLCONF` to master twice, in order to configure basic parameters of the replication such as which port the slave can be reached on
// 3. slave sends `PSYNC` to initiate the replication

func initReplication(slaveListeningPort int) (*net.Conn, error) {
	// We are master, it is not on us to initiate the replication
	if replicationInfo.replicaof == "" {
		replicationInfo.masterReplId = generateReplId()
		replicationInfo.masterReplOffset = 0

		return nil, nil
	}

	buf := make([]byte, 4096)

	replicationInfo.masterReplId = "?"
	replicationInfo.masterReplOffset = -1

	fields := strings.Fields(replicationInfo.replicaof)
	if len(fields) != 2 {
		return nil, fmt.Errorf("Invalid replication host: %s", replicationInfo.replicaof)
	}

	var err error

	replicationInfo.replicaMasterAddress = fields[0]
	replicationInfo.replicaMasterPort, err = strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}

	replicationInfo.replicaMasterHost = fmt.Sprintf("%s:%d",
		replicationInfo.replicaMasterAddress,
		replicationInfo.replicaMasterPort)

	conn, err := net.Dial("tcp", replicationInfo.replicaMasterHost)
	if err != nil {
		return nil, err
	}

	replicationInfo.masterConnection = &conn

	pingCommand := encodeStringArray([]string{"PING"})
	_, err = conn.Write(pingCommand)
	if err != nil {
		return nil, err
	}

	_, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}

	offset := 0
	query, err := parseResp(buf, &offset)
	if err != nil {
		return nil, err
	}

	pong, err := query.asString()
	if pong != "PONG" {
		return nil, fmt.Errorf("Expected PONG as an answer to PING")
	}

	replConfCommand := encodeStringArray([]string{
		"REPLCONF",
		"listening-port",
		strconv.Itoa(slaveListeningPort),
	})
	_, err = conn.Write(replConfCommand)
	if err != nil {
		return nil, err
	}

	_, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}

	offset = 0
	query, err = parseResp(buf, &offset)
	if err != nil {
		return nil, err
	}

	ok, err := query.asString()
	if ok != "OK" {
		return nil, fmt.Errorf("Expected OK as an answer to REPLCONF")
	}

	replConfCommand = encodeStringArray([]string{
		"REPLCONF",
		"capa",
		"psync2",
		"capa",
		"eof",
	})
	_, err = conn.Write(replConfCommand)
	if err != nil {
		return nil, err
	}

	_, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}

	offset = 0
	query, err = parseResp(buf, &offset)
	if err != nil {
		return nil, err
	}

	ok, err = query.asString()
	if ok != "OK" {
		return nil, fmt.Errorf("Expected OK as an answer to REPLCONF")
	}

	psyncCommand := encodeStringArray([]string{
		"PSYNC",
		replicationInfo.masterReplId,
		strconv.Itoa(replicationInfo.masterReplOffset),
	})

	_, err = conn.Write(psyncCommand)
	if err != nil {
		return nil, err
	}

	_, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}

	offset = 0
	query, err = parseResp(buf, &offset)
	if err != nil {
		return nil, err
	}

	s, err := query.asString()
	array := strings.Fields(s)
	if len(array) != 3 || array[0] != "FULLRESYNC" {
		return nil, fmt.Errorf("Expected reponse in the format: FULLRESYNC <REPL_ID> <REPL_OFFSET>")
	}

	_, err = hex.DecodeString(array[1])
	if err != nil || len(array[1]) != 40 {
		return nil, fmt.Errorf("Expected repl ID to be a 40 digit hex string")
	}

	replicationInfo.masterReplId = array[1]

	replOffset, err := strconv.Atoi(array[2])
	if err != nil || replOffset < 0 {
		return nil, fmt.Errorf("Expected repl offset to be a positive integer")

	}

	replicationInfo.masterReplOffset = offset

	return &conn, nil
}
