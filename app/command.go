package main

import (
	"fmt"
	"strings"
)

func ping() []byte {
	return []byte("+PONG\r\n")
}

func echo(args []*query) ([]byte, error) {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments\r\n"), nil
	}

	if args[0].queryType != BulkString {
		return []byte("-ERR invalid argument\r\n"), nil
	}

	bulkString, err := args[0].asBulkString()
	if err != nil {
		return nil, err
	}

	return encodeBulkString(bulkString), nil
}

func execute(query *query) ([]byte, error) {
	if query.queryType != Array {
		return nil, fmt.Errorf("Invalid query type: %d", query.queryType)
	}

	array, err := query.asArray()
	if err != nil {
		return nil, err
	}

	command, err := array[0].asBulkString()
	if err != nil {
		return nil, err
	}

	if strings.EqualFold(command, "PING") {
		return ping(), nil
	}

	if strings.EqualFold(command, "ECHO") {
		return echo(array[1:])
	}

	errorResponse := fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
	return []byte(errorResponse), nil
}
