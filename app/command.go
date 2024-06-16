package main

import (
	"fmt"
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
	SELECT
	CONFIG
	KEYS
)

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

	if strings.EqualFold(command, "SELECT") {
		response := selectFunc(args)
		return response, SELECT, nil
	}

	if strings.EqualFold(command, "CONFIG") {
		response, err := config(args)
		return response, CONFIG, err
	}

	if strings.EqualFold(command, "KEYS") {
		response, err := keys(args)
		return response, KEYS, err
	}

	return nil, UNKNOWN, fmt.Errorf("%w unknown command '%s'", ErrRespSimpleError, command)
}
