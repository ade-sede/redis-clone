package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type command int

const (
	UNKNOWN command = -1
	SET     command = iota
	GET
	PING
	ECHO
	INFO
	PSYNC
	REPLCONF
	REPLCONF_GETACK
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

func set(args []string) ([]byte, error) {
	var expiresAt *time.Time = nil
	var durationMultiplier time.Duration = 0
	var duration int = 0
	var err error

	if len(args) < 2 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]
	value := args[1]
	options := args[2:]

	for i, option := range options {
		expiryDurationOption := isExpiryDurationOption(option)

		if expiryDurationOption != "" {
			if len(options) < i+2 {
				return nil, ErrOutOfBounds
			}

			durationString := options[i+1]

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

func get(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]

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

func echo(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}
	return encodeBulkString(args[0]), nil
}

func info(args []string) ([]byte, error) {
	requestedSections := 0
	replicationRequested := false

	for _, section := range args {
		requestedSections += 1

		if section == "replication" {
			replicationRequested = true
		} else if section == "all" {
			replicationRequested = true
		} else {
			return nil, fmt.Errorf("%w unsupported info section: %s", ErrRespSimpleError, section)
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

// the GETACK subcommand involves special behavior, which is why we must lift
// it to the top level
func replconf(conn net.Conn, args []string) ([]byte, command, error) {
	var isGetAck bool = false

	existingReplica, err := replicationInfo.findReplica(conn)
	if err != nil {
		return nil, REPLCONF, err
	}

	for i, arg := range args {
		if arg == "listening-port" {
			if len(args) < i+2 {
				return nil, REPLCONF, ErrOutOfBounds
			}

			port, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, REPLCONF, err
			}

			// TODO remove from list on EOF on conn
			if existingReplica == nil {
				newReplica := replica{
					capabilites: make([]string, 0),
					conn:        conn,
					port:        port,
				}

				replicationInfo.replicas = append(replicationInfo.replicas, newReplica)
			} else {
				existingReplica.conn = conn
				existingReplica.port = port
			}
		}

		if arg == "capa" {
			if len(args) < i+2 {
				return nil, REPLCONF, ErrOutOfBounds
			}

			newCapa := args[i+1]

			if existingReplica == nil {
				return nil, REPLCONF, fmt.Errorf("Can't add a capability, no replica registered for %s", conn.RemoteAddr().String())
			}

			existingReplica.capabilites = append(existingReplica.capabilites, newCapa)
		}

		if strings.EqualFold(arg, "GETACK") {
			isGetAck = true

			if len(args) < i+2 {
				return nil, REPLCONF_GETACK, ErrOutOfBounds
			}

			if args[i+1] != "*" {
				return nil, REPLCONF_GETACK, fmt.Errorf("Expected `*` as argument for getack, got %s", args[i+1])
			}
		}
	}

	if isGetAck {
		response := encodeStringArray([]string{
			"REPLCONF",
			"ACK",
			strconv.Itoa(replicationInfo.masterReplOffset),
		})
		return []byte(response), REPLCONF_GETACK, nil
	} else {
		return []byte("+OK\r\n"), REPLCONF, nil

	}
}

func psync(conn net.Conn, args []string) ([]byte, error) {
	existingReplica, err := replicationInfo.findReplica(conn)
	if err != nil {
		return nil, err
	}

	if existingReplica == nil {
		return nil, fmt.Errorf("No replica registered for %s", conn.RemoteAddr().String())
	}

	args = nil
	fullResync := encodeBulkString(fmt.Sprintf("FULLRESYNC %s %d",
		replicationInfo.masterReplId,
		replicationInfo.masterReplOffset))

	emptyRDB, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return nil, err
	}

	RDB := []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRDB), string(emptyRDB)))

	response := make([]byte, 0, len(fullResync)+len(emptyRDB)+3+4)
	response = append(response, fullResync...)
	response = append(response, RDB...)

	fmt.Printf("%s\n", string(response))

	return response, nil
}

func execute(conn net.Conn, query *query) ([]byte, command, error) {
	if query.queryType != Array {
		return nil, UNKNOWN, fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	// Can assume everyhting will be a string for our limited use
	array, err := query.asStringArray()
	if err != nil {
		return nil, UNKNOWN, err
	}

	command := array[0]
	args := array[1:]

	if strings.EqualFold(command, "PING") {
		response := ping()
		return response, PING, nil
	}

	if strings.EqualFold(command, "ECHO") {
		response, err := echo(args)
		return response, ECHO, err
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
		response, err := info(args)
		return response, INFO, err
	}

	if strings.EqualFold(command, "REPLCONF") {
		response, command, err := replconf(conn, args)
		return response, command, err
	}

	if strings.EqualFold(command, "PSYNC") {
		response, err := psync(conn, args)
		return response, PSYNC, err
	}

	return nil, UNKNOWN, fmt.Errorf("%w unknown command '%s'", ErrRespSimpleError, command)
}
