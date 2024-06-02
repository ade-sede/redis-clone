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

type replica struct {
	host            string
	capabilites     []string
	needsFullResync bool
}

var replicationInfo struct {
	// Are we master ?
	isMaster bool

	// If we are the master, this is our own info.
	// If we are a slave, this is our local copy of the master's info.
	masterReplId     string
	masterReplOffset int

	// If we are slave, who is master we are tracking ?
	// The 4 following fields contain the same information in different formats
	// Replica of is the input format of the program: "10.10.3.4 5000"
	// Replica address is the address part
	// Replica port is the port part
	// Replica host is the address and port merged: "10.10.3.4:5000"
	replicaof            string
	replicaMasterAddress string
	replicaMasterPort    int
	replicaMasterHost    string

	masterConnection *net.Conn

	replicas []replica
}

// The slave is responsible for initiating the replication.
// Handshake for replication is done in 3 steps:
// 1. slave sends `PING` to master.
// 2. slave sends `REPLCONF` to master twice, in order to configure basic parameters of the replication such as which port the slave can be reached on.
// 3. slave sends `PSYNC` to initiate the replication.
//
// Once the handshake is over, slave and master are ready to start syncing,
// The simplest pattern to implement is FULLRESYNC.
// As an aswer to `PSYNC` master answers with `FULLRESYNC` and proceeds to send
// the whole RDB file to the slave.

func initReplication(slaveListeningPort int) (*net.Conn, error) {
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

	// TODO encapsulate handshake + response expectation

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
