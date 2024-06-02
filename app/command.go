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

func set(conn net.Conn, args []*query) error {
	var expiresAt *time.Time = nil
	var durationMultiplier time.Duration = 0
	var duration int = 0

	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments\r\n"))
		return nil
	}

	key, err := args[0].asString()
	if err != nil {
		return err
	}

	value, err := args[1].asString()
	if err != nil {
		return err
	}

	options := args[2:]

	for i, option := range options {
		option, err := option.asString()
		if err != nil {
			return err
		}

		expiryDurationOption := isExpiryDurationOption(option)

		if expiryDurationOption != "" {
			if len(options) < i+2 {
				return ErrOutOfBounds
			}

			durationString, err := options[i+1].asString()
			if err != nil {
				return err
			}

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

func get(conn net.Conn, args []*query) error {
	if len(args) != 1 {
		conn.Write([]byte("-ERR wrong number of arguments\r\n"))
		return nil
	}

	key, err := args[0].asString()
	if err != nil {
		return err
	}

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

func echo(conn net.Conn, args []*query) error {
	if len(args) != 1 {
		conn.Write([]byte("-ERR wrong number of arguments\r\n"))
		return nil
	}

	if args[0].queryType != BulkString {
		conn.Write([]byte("-ERR invalid argument type\r\n"))
		return nil
	}

	bulkString, err := args[0].asString()
	if err != nil {
		return err
	}

	conn.Write(encodeBulkString(bulkString))
	return nil
}

func info(conn net.Conn, args []*query) error {
	requestedSections := 0
	replicationRequested := false

	for _, option := range args {
		requestedSections += 1

		optionName, err := option.asString()
		if err != nil {
			return err
		}

		if optionName == "replication" {
			replicationRequested = true
		} else {
			response := fmt.Sprintf("-ERR unsupported info section: %s\r\n", optionName)
			conn.Write([]byte(response))
			return nil
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

func replconf(conn net.Conn, args []*query) error {
	callerIp, err := getCallerIp(conn)
	if err != nil {
		return err
	}

	existingReplica, err := replicationInfo.findReplica(callerIp)
	if err != nil {
		return err
	}

	for i, arg := range args {
		str, err := arg.asString()
		if err != nil {
			return err
		}

		if str == "listening-port" {
			if len(args) < i+2 {
				return ErrOutOfBounds
			}

			port, err := args[i+1].asString()
			if err != nil {
				return err
			}

			if existingReplica == nil {
				newReplica := replica{
					host:        fmt.Sprintf("%s:%s", callerIp, port),
					capabilites: make([]string, 0),
				}

				replicationInfo.replicas = append(replicationInfo.replicas, newReplica)
			} else {
				existingReplica.host = fmt.Sprintf("%s:%s", callerIp, port)
			}
		}

		if str == "capa" {
			if len(args) < i+2 {
				return ErrOutOfBounds
			}

			newCapa, err := args[i+1].asString()
			if err != nil {
				return err
			}

			if existingReplica == nil {
				return fmt.Errorf("Can't add a capability, no replica registered for %s", callerIp)
			}

			existingReplica.capabilites = append(existingReplica.capabilites, newCapa)
		}
	}

	conn.Write([]byte("+OK\r\n"))
	return nil
}

func psync(conn net.Conn, args []*query) error {
	callerIp, err := getCallerIp(conn)
	if err != nil {
		return err
	}

	existingReplica, err := replicationInfo.findReplica(callerIp)
	if err != nil {
		return err
	}

	if existingReplica == nil {
		return fmt.Errorf("No replica registered for %s", callerIp)
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

func execute(conn net.Conn, query *query) error {
	if query.queryType != Array {
		return fmt.Errorf("Can't execute of query type: %d. Only Arrays are supported at this time (type %d)", query.queryType, Array)
	}

	array, err := query.asArray()
	if err != nil {
		return err
	}

	command, err := array[0].asString()
	if err != nil {
		return err
	}

	args := array[1:]

	if strings.EqualFold(command, "PING") {
		ping(conn)
		return nil
	}

	if strings.EqualFold(command, "ECHO") {
		return echo(conn, args)
	}

	if strings.EqualFold(command, "SET") {
		return set(conn, args)
	}

	if strings.EqualFold(command, "GET") {
		return get(conn, args)
	}

	if strings.EqualFold(command, "INFO") {
		return info(conn, args)
	}

	if strings.EqualFold(command, "REPLCONF") {
		return replconf(conn, args)
	}

	if strings.EqualFold(command, "PSYNC") {
		return psync(conn, args)
	}

	errorResponse := fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
	conn.Write([]byte(errorResponse))
	return nil
}
