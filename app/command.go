package main

import (
	"bufio"
	"bytes"
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
	SAVE
	DEL
	TYPE
	XADD
	XRANGE
	XREAD
	INCR
	MULTI
	EXEC
	QUEUE
	DISCARD
)

func discard(multi []query) ([]byte, error) {
	if multi == nil {
		return nil, fmt.Errorf("%w DISCARD without MULTI\r\n", ErrRespSimpleError)
	}

	return []byte("+OK\r\n"), nil
}

func execFunc(conn *connection, multi []query) ([]byte, error) {
	if multi == nil {
		return nil, fmt.Errorf("%w EXEC without MULTI\r\n", ErrRespSimpleError)
	}

	if len(multi) == 0 {
		return []byte("*0\r\n"), nil
	}

	allResponses := make([][]byte, 0)
	for _, query := range multi {
		response, _, _ := execute(conn, &query, nil)

		allResponses = append(allResponses, response)
	}

	return encodeRespArray(allResponses), nil
}

func multiFunc(multi []query) ([]byte, error) {
	if multi != nil {
		return nil, fmt.Errorf("%w nested multi\r\n", ErrRespSimpleError)
	}

	return []byte("+OK\r\n"), nil
}

func ping() []byte {
	return []byte("+PONG\r\n")
}

func echo(args []string) []byte {
	return encodeRespBulkString(args[0])
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

		return encodeRespBulkString(response)
	}

	panic("Unreachable code")
}

func execute(conn *connection, query *query, multi []query) ([]byte, command, error) {
	if query.queryType == RDBFile {
		fileContent := query.value.([]byte)
		reader := bufio.NewReader(bytes.NewReader(fileContent))
		// TODO dump existing store return new store rather than assigning directly to global
		err := readRDBFile(reader)
		if err != nil {
			return nil, UNKNOWN, err
		}

		return nil, UNKNOWN, nil
	}

	if query.queryType != Array {
		return nil, UNKNOWN, fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	array, _ := query.asStringArray()

	fmt.Printf("(FromRemote: %s) Executing command: %v\n", conn.handler.RemoteAddr().String(), array)

	command := array[0]
	args := array[1:]

	if multi != nil &&
		!strings.EqualFold(command, "EXEC") &&
		!strings.EqualFold(command, "MULTI") &&
		!strings.EqualFold(command, "DISCARD") {
		return []byte("+QUEUED\r\n"), QUEUE, nil
	}

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

	if strings.EqualFold(command, "SAVE") {
		response, err := save()
		return response, SAVE, err
	}

	if strings.EqualFold(command, "DEL") {
		response, err := del(args)
		return response, DEL, err
	}

	if strings.EqualFold(command, "TYPE") {
		response, err := typeFunc(args)
		return response, TYPE, err
	}

	if strings.EqualFold(command, "XADD") {
		response, err := xadd(args)
		return response, XADD, err
	}

	if strings.EqualFold(command, "xrange") {
		response, err := xrange(args)
		return response, XRANGE, err
	}

	if strings.EqualFold(command, "xread") {
		response, err := xread(args)
		return response, XREAD, err
	}

	if strings.EqualFold(command, "incr") {
		response, err := incr(args)
		return response, INCR, err
	}

	if strings.EqualFold(command, "multi") {
		response, err := multiFunc(multi)
		return response, MULTI, err
	}

	if strings.EqualFold(command, "exec") {
		response, err := execFunc(conn, multi)
		return response, EXEC, err
	}

	if strings.EqualFold(command, "discard") {
		response, err := discard(multi)
		return response, DISCARD, err
	}

	return nil, UNKNOWN, fmt.Errorf("%w unknown command '%s'", ErrRespSimpleError, command)
}
