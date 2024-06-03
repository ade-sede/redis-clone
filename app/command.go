package main

import (
	"encoding/hex"
	"fmt"
	"net"
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

func set(conn net.Conn, args []string) error {
	var expiresAt *time.Time = nil
	var durationMultiplier time.Duration = 0
	var duration int = 0
	var err error

	if len(args) < 2 {
		return ErrRespWrongNumberOfArguments
	}

	key := args[0]
	value := args[1]
	options := args[2:]

	for i, option := range options {
		expiryDurationOption := isExpiryDurationOption(option)

		if expiryDurationOption != "" {
			if len(options) < i+2 {
				return ErrOutOfBounds
			}

			durationString := options[i+1]

			duration, err = strconv.Atoi(durationString)
			if err != nil {
				return err
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

	conn.Write([]byte("+OK\r\n"))
	return nil
}

func get(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return ErrRespWrongNumberOfArguments
	}

	key := args[0]

	entry, ok := data[key]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return nil
	}

	str, ok := entry.value.(string)
	if !ok {
		return fmt.Errorf("Invalid value type: %T", entry.value)
	}

	if entry.expiresAt != nil && entry.expiresAt.Before(time.Now()) {
		delete(data, key)
		conn.Write([]byte("$-1\r\n"))
		return nil
	}

	conn.Write(encodeBulkString(str))
	return nil
}

func ping(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func echo(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return ErrRespWrongNumberOfArguments
	}
	conn.Write(encodeBulkString(args[0]))
	return nil
}

func info(conn net.Conn, args []string) error {
	requestedSections := 0
	replicationRequested := false

	for _, section := range args {
		requestedSections += 1

		if section == "replication" {
			replicationRequested = true
		} else if section == "all" {
			replicationRequested = true
		} else {
			return fmt.Errorf("%w unsupported info section: %s", ErrRespSimpleError, section)
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

		conn.Write(encodeBulkString(response))
		return nil
	}

	panic("Unreachable code")
}

func replconf(conn net.Conn, args []string) error {
	existingReplica, err := replicationInfo.findReplica(conn)
	if err != nil {
		return err
	}

	for i, arg := range args {
		if arg == "listening-port" {
			if len(args) < i+2 {
				return ErrOutOfBounds
			}

			port, err := strconv.Atoi(args[i+1])
			if err != nil {
				return err
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
				return ErrOutOfBounds
			}

			newCapa := args[i+1]

			if existingReplica == nil {
				return fmt.Errorf("Can't add a capability, no replica registered for %s", conn.RemoteAddr().String())
			}

			existingReplica.capabilites = append(existingReplica.capabilites, newCapa)
		}
	}

	conn.Write([]byte("+OK\r\n"))
	return nil
}

func psync(conn net.Conn, args []string) error {
	existingReplica, err := replicationInfo.findReplica(conn)
	if err != nil {
		return err
	}

	if existingReplica == nil {
		return fmt.Errorf("No replica registered for %s", conn.RemoteAddr().String())
	}

	args = nil
	response := fmt.Sprintf("FULLRESYNC %s %d",
		replicationInfo.masterReplId,
		replicationInfo.masterReplOffset)

	conn.Write(encodeBulkString(response))

	emptyRDB, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return err
	}

	_, err = conn.Write(append([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))), emptyRDB...))
	if err != nil {
		return err
	}

	return nil
}

func getCallerIp(conn net.Conn) (string, error) {
	addr := conn.RemoteAddr()
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", fmt.Errorf("Error parsing IP address %s", host)
	}

	if ip.IsLoopback() {
		return "localhost", nil
	}

	return ip.String(), nil
}

func execute(conn net.Conn, query *query) (mustPropagateToReplicas bool, err error) {
	if query.queryType != Array {
		return false, fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	// Can assume everyhting will be a string for our limited use
	array, err := query.asStringArray()
	if err != nil {
		return false, err
	}

	command := array[0]
	args := array[1:]

	if strings.EqualFold(command, "PING") {
		ping(conn)
		return false, nil
	}

	if strings.EqualFold(command, "ECHO") {
		err = echo(conn, args)
		return false, err
	}

	if strings.EqualFold(command, "SET") {
		err = set(conn, args)
		return true, err
	}

	if strings.EqualFold(command, "GET") {
		err = get(conn, args)
		return false, err
	}

	if strings.EqualFold(command, "INFO") {
		err = info(conn, args)
		return false, err
	}

	if strings.EqualFold(command, "REPLCONF") {
		err = replconf(conn, args)
		return false, err
	}

	if strings.EqualFold(command, "PSYNC") {
		err = psync(conn, args)
		return false, err
	}

	return false, fmt.Errorf("%w unknown command '%s'", ErrRespSimpleError, command)
}
