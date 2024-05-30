package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type entry struct {
	value     any
	expiresAt *time.Time
}

var data map[string]entry

func set(args []*query) ([]byte, error) {
	var expiresAt *time.Time = nil

	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments\r\n"), nil
	}

	key, err := args[0].asString()
	if err != nil {
		return nil, err
	}

	value, err := args[1].asString()
	if err != nil {
		return nil, err
	}

	if len(args) >= 3 {
		option, err := args[2].asString()
		if err != nil {
			return nil, err
		}

		if strings.EqualFold(option, "PX") {
			if len(args) < 4 {
				return []byte("-ERR wrong number of arguments\r\n"), nil
			}

			durationString, err := args[3].asString()
			if err != nil {
				return nil, err
			}

			durationMs, err := strconv.Atoi(durationString)
			if err != nil {
				return nil, err
			}

			expiresAt = new(time.Time)
			*expiresAt = time.Now().Add(time.Duration(durationMs) * time.Millisecond)
		} else {
			errorResponse := fmt.Sprintf("-ERR unsupported option '%s'\r\n", option)
			return []byte(errorResponse), nil

		}
	}

	data[key] = entry{value: value, expiresAt: expiresAt}

	return []byte("+OK\r\n"), nil
}

func get(args []*query) ([]byte, error) {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments\r\n"), nil
	}

	key, err := args[0].asString()
	if err != nil {
		return nil, err
	}

	entry, ok := data[key]
	if !ok {
		return []byte("$-1\r\n"), nil
	}

	str, ok := entry.value.(string)
	if !ok {
		return nil, fmt.Errorf("Invalid value type: %T", entry.value)
	}

	if entry.expiresAt != nil && entry.expiresAt.Before(time.Now()) {
		delete(data, key)
		return []byte("$-1\r\n"), nil
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
		return []byte("-ERR invalid argument type\r\n"), nil
	}

	bulkString, err := args[0].asString()
	if err != nil {
		return nil, err
	}

	return encodeBulkString(bulkString), nil
}

func execute(query *query) ([]byte, error) {
	if query.queryType != Array {
		return nil, fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	array, err := query.asArray()
	if err != nil {
		return nil, err
	}

	command, err := array[0].asString()
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
