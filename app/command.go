package main

import (
	"fmt"
	"strings"
)

var data map[string]any

func set(args []*query) ([]byte, error) {
	if len(args) != 2 {
		return []byte("-ERR wrong number of arguments\r\n"), nil
	}

	key, err := args[0].asBulkString()
	if err != nil {
		return nil, err
	}

	value, err := args[1].asBulkString()
	if err != nil {
		return nil, err
	}

	data[key] = value

	return []byte("+OK\r\n"), nil
}

func get(args []*query) ([]byte, error) {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments\r\n"), nil
	}

	key, err := args[0].asBulkString()
	if err != nil {
		return nil, err
	}

	value, ok := data[key]
	if !ok {
		return []byte("$-1\r\n"), nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("Invalid value type: %T", value)
	}

	return encodeBulkString(str), nil
}

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

	if strings.EqualFold(command, "SET") {
		return set(array[1:])
	}

	if strings.EqualFold(command, "GET") {
		return get(array[1:])
	}

	errorResponse := fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
	return []byte(errorResponse), nil
}
