package main

import (
	"fmt"
	"net"
	"strings"
)

var replicationInfo struct {
	replicaHost      string
	replicaof        string
	masterReplid     string
	masterReplOffset int
	masterConnection *net.Conn
}

func initReplication() (*net.Conn, error) {
	if replicationInfo.replicaof == "" {
		replicationInfo.masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		replicationInfo.masterReplOffset = 0

		return nil, nil
	}
	fields := strings.Fields(replicationInfo.replicaof)
	if len(fields) != 2 {
		return nil, fmt.Errorf("Invalid replication host: %s", replicationInfo.replicaof)
	}

	replicationInfo.replicaHost = fmt.Sprintf("%s:%s", fields[0], fields[1])

	conn, err := net.Dial("tcp", replicationInfo.replicaHost)
	if err != nil {
		return nil, err
	}

	replicationInfo.masterConnection = &conn

	pingCommand := encodeStringArray([]string{"PING"})
	_, err = conn.Write(pingCommand)
	if err != nil {
		return nil, err
	}

	return &conn, nil
}
