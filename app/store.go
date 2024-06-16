package main

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

type stringEntry struct {
	value     string
	expiresAt *time.Time
}

// Each entry in a stream is a kv map
// One stream contains many entries
type stream struct {
	entries []map[string]string
}

type database struct {
	stringStore map[string]stringEntry
	streamStore map[string]stream
}

func initStore() error {
	status.activeDB = 0
	status.databases = make(map[int]database)
	initialDB := database{
		stringStore: make(map[string]stringEntry),
		streamStore: make(map[string]stream),
	}
	status.databases[0] = initialDB

	if status.dbFileName != "" && status.dir != "" {
		return initPersistence()
	}

	return nil
}

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

	status.databases[status.activeDB].stringStore[key] = stringEntry{value: value, expiresAt: expiresAt}

	return []byte("+OK\r\n"), nil
}

func get(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]

	entry, ok := status.databases[status.activeDB].stringStore[key]
	if !ok {
		return []byte("$-1\r\n"), nil
	}

	if entry.expiresAt != nil && entry.expiresAt.Before(time.Now()) {
		delete(status.databases[status.activeDB].stringStore, key)
		return []byte("$-1\r\n"), nil
	}

	return encodeBulkString(entry.value), nil
}

func del(args []string) ([]byte, error) {
	if len(args) < 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	keys := args

	deleted := 0
	for _, key := range keys {
		if _, ok := status.databases[status.activeDB].stringStore[key]; ok {
			delete(status.databases[status.activeDB].stringStore, key)
			deleted++
		}
	}

	return encodeInteger(deleted), nil
}

func selectFunc(args []string) []byte {
	status.activeDB, _ = strconv.Atoi(args[0])
	if _, ok := status.databases[status.activeDB]; !ok {
		newDB := database{
			stringStore: make(map[string]stringEntry),
			streamStore: make(map[string]stream),
		}
		status.databases[status.activeDB] = newDB
	}

	return []byte("+OK\r\n")
}

func keys(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	pattern := args[0]
	if pattern == "*" {
		pattern = ".*"
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)

	for key := range status.databases[status.activeDB].stringStore {
		if regex.MatchString(key) {
			keys = append(keys, key)
		}
	}

	return encodeStringArray(keys), nil
}

func typeFunc(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]

	_, inStringStore := status.databases[status.activeDB].stringStore[key]
	_, inStreamStore := status.databases[status.activeDB].streamStore[key]

	if inStringStore {
		return encodeSimpleString("string"), nil
	}

	if inStreamStore {
		return encodeSimpleString("stream"), nil
	}

	return encodeSimpleString("none"), nil
}

func xadd(args []string) ([]byte, error) {
	var str stream
	entry := make(map[string]string)

	if len(args) < 3 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]
	id := args[1]
	kv := args[2:]

	str, ok := status.databases[status.activeDB].streamStore[key]
	if !ok {
		str = stream{
			entries: make([]map[string]string, 1),
		}
	}

	entry["id"] = id
	for i := 0; i+1 < len(kv); i += 2 {
		entry[kv[i]] = kv[i+1]
	}

	str.entries = append(str.entries, entry)
	status.databases[status.activeDB].streamStore[key] = str

	return encodeBulkString(id), nil
}
