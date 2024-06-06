package main

import (
	"fmt"
	"net"
	"strings"
)

type command int

const (
	UNKNOWN command = iota
	SET
	GET
	PING
	ECHO
	INFO
	PSYNC
	REPLCONF
	REPLCONF_GETACK
	WAIT
)

const EMPTY_RDB_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

func ping() []byte {
	return []byte("+PONG\r\n")
}

func echo(args []string) []byte {
	return encodeBulkString(args[0])
}

func info(args []string) []byte {
	requestedSections := 0
	replicationRequested := false

	for _, section := range args {
		requestedSections += 1

		if section == "replication" {
			replicationRequested = true
		} else if section == "all" {
			replicationRequested = true
		}
	}

	// no section requested or "all" section requested should result in
	// every supported sections to be printed
	if replicationRequested || requestedSections == 0 {
		var role string

		if status.replicaof != "" {
			role = "slave"
		} else {
			role = "master"
		}

		response := fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%d`,
			role,
			status.replId,
			status.replOffset)

		return encodeBulkString(response)
	}

	panic("Unreachable code")
}

func execute(conn *connection, query *query) ([]byte, command, error) {
	if query.queryType != Array {
		return nil, UNKNOWN, fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	array, _ := query.asStringArray()

	fmt.Printf("(FromRemote: %s) Executing command: %v\n", conn.handler.RemoteAddr().String(), array)

	command := array[0]
	args := array[1:]

	if strings.EqualFold(command, "PING") {
		response := ping()
		return response, PING, nil
	}

	if strings.EqualFold(command, "ECHO") {
		response := echo(args)
		return response, ECHO, nil
	}

	if strings.EqualFold(command, "SET") {
		response, err := set(args)
		return response, SET, err
	}

	if strings.EqualFold(command, "GET") {
		response, err := get(args)
		return response, GET, err
	}

	if strings.EqualFold(command, "INFO") {
		response := info(args)
		return response, INFO, nil
	}

	if strings.EqualFold(command, "REPLCONF") {
		response, command, err := replconf(conn, args)
		return response, command, err
	}

	if strings.EqualFold(command, "PSYNC") {
		response, err := psync(conn)
		return response, PSYNC, err
	}

	if strings.EqualFold(command, "WAIT") {
		response := wait(args)
		return response, WAIT, nil
	}

	return nil, UNKNOWN, fmt.Errorf("%w unknown command '%s'", ErrRespSimpleError, command)
}

func sendCommand(conn net.Conn, command []string) (*query, int, error) {
	fmt.Printf("(ToRemote: %s) Sending command: %v\n", conn.RemoteAddr().String(), command)

	offset := 0
	buf := make([]byte, 4096)
	commandArray := encodeStringArray(command)

	writeLen, err := conn.Write(commandArray)
	if err != nil {
		// TODO can I trust writeLen in case of err ?
		// Any risk it is -1 ?
		return nil, 0, err
	}

	n, err := conn.Read(buf)
	if err != nil {
		return nil, writeLen, err
	}
	query, _, _ := parseResp(buf[:n], &offset)

	// Verbose logs to help with debuging
	if query != nil {
		str, err := query.asString()
		if err != nil {
			fmt.Printf("(FromRemote: %s) Received response %s\n", conn.RemoteAddr().String(), buf)
		} else {
			fmt.Printf("(FromRemote: %s) Received response %s\n", conn.RemoteAddr().String(), str)
		}
	} else {
		fmt.Printf("(FromRemote: %s) Received response %s\n", conn.RemoteAddr().String(), buf[:n])
	}

	return query, writeLen, nil
}
