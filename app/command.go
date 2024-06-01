package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var expiryDurationOptionNames = []string{"EX", "PX"}

func isExpiryDurationOption(optionName string) string {
	for _, opt := range expiryDurationOptionNames {
		if strings.EqualFold(optionName, opt) {
			return opt
		}
	}

	return ""
}

func set(args []*query) ([]byte, error) {
	var expiresAt *time.Time = nil
	var durationMultiplier time.Duration = 0
	var duration int = 0

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

	options := args[2:]

	for i, option := range options {
		option, err := option.asString()
		if err != nil {
			return nil, err
		}

		expiryDurationOption := isExpiryDurationOption(option)

		if expiryDurationOption != "" {
			if len(options) < i+2 {
				return nil, ErrOutOfBounds
			}

			durationString, err := options[i+1].asString()
			if err != nil {
				return nil, err
			}

			duration, err = strconv.Atoi(durationString)
			if err != nil {
				return nil, err
			}

			if expiryDurationOption == "PX" {
				durationMultiplier = time.Millisecond
			} else if expiryDurationOption == "EX" {
				durationMultiplier = time.Second
			}
		}
	}

	if duration > 0 {
		expiresAt = new(time.Time)
		*expiresAt = time.Now().Add(time.Duration(duration) * durationMultiplier)
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

func info(args []*query) ([]byte, error) {

	requestedSections := 0
	replicationRequested := false

	for _, option := range args {
		optionName, err := option.asString()
		if err != nil {
			return nil, err
		}

		if optionName == "replication" {
			replicationRequested = true
			requestedSections += 1
		} else {
			response := fmt.Sprintf("-ERR unsupported info section: %s\r\n", optionName)
			return []byte(response), nil
		}
	}

	// no section requested or "all" section requested should result in
	// every supported sections to be printed
	if replicationRequested || requestedSections == 0 {
		var role string

		if replicationInfo.replicaof != "" {
			role = "slave"
		} else {
			role = "master"
		}

		response := fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%d`,
			role,
			replicationInfo.masterReplId,
			replicationInfo.masterReplOffset)

		return encodeBulkString(response), nil
	}

	panic("Unreachable code")
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

	if strings.EqualFold(command, "INFO") {
		return info(array[1:])
	}

	errorResponse := fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
	return []byte(errorResponse), nil
}
