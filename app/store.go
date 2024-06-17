package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type stringEntry struct {
	value     string
	expiresAt *time.Time
}

// A stream is described by a key.
// It contains a list of entries.
// Each entry in a stream is a KV store
type streamEntry struct {
	entries []map[string]string
	lastId  string
}

type database struct {
	stringStore map[string]stringEntry
	streamStore map[string]streamEntry
}

func initStore() error {
	status.activeDB = 0
	status.databases = make(map[int]database)
	initialDB := database{
		stringStore: make(map[string]stringEntry),
		streamStore: make(map[string]streamEntry),
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
			streamStore: make(map[string]streamEntry),
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

func parseStreamEntryId(id string) (int, int, error) {
	milliseconds := -1
	seq := -1

	parts := strings.Split(id, "-")

	milliseconds, err := strconv.Atoi(parts[0])
	if err != nil {
		if parts[0] == "*" {
			milliseconds = -1
		} else {
			return -1, -1, err
		}
	}

	if len(parts) == 2 {
		seq, err = strconv.Atoi(parts[1])
		if err != nil {
			if parts[1] == "*" {
				seq = -1
			} else {
				return milliseconds, -1, err
			}
		}
	}

	return milliseconds, seq, nil
}

// An ID is <milliseconds>-<sequenceNumber>
// ID of later entries must be superior to id of previous entries
// - Higher timestamp
// - If same timestamp, higher seq
// If `sequenceNumber` is `*` it is auto generated
// - If same timestamp, previous sequence number + 1
// - Else 0 (note, 0-0 is illegal. Works out of the box because we use 0-0 as default)
// TODO refactor, clear code and remove comment
func validateStreamEntryId(newId string, lastId string) (string, error) {
	newMs, newSeq, err := parseStreamEntryId(newId)
	if err != nil {
		return "", err
	}

	if newMs == 0 && newSeq == 0 {
		return "", fmt.Errorf("%w The ID specified in XADD must be greater than 0-0", ErrRespSimpleError)
	}

	lastMs, lastSeq, err := parseStreamEntryId(lastId)
	if err != nil {
		return "", err
	}

	if newSeq == -1 {
		if newMs == lastMs {
			newSeq = lastSeq + 1
		} else {
			newSeq = 0
		}
	}

	if newMs < lastMs {
		return "", fmt.Errorf("%w The ID specified in XADD is equal or smaller than the target stream top item", ErrRespSimpleError)
	}

	if newMs == lastMs {
		if newSeq <= lastSeq {
			return "", fmt.Errorf("%w The ID specified in XADD is equal or smaller than the target stream top item", ErrRespSimpleError)
		}
	}

	return fmt.Sprintf("%d-%d", newMs, newSeq), nil
}

func xadd(args []string) ([]byte, error) {
	var stream streamEntry

	entry := make(map[string]string)

	if len(args) < 3 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]
	id := args[1]
	kv := args[2:]

	stream, ok := status.databases[status.activeDB].streamStore[key]
	if !ok {
		stream = streamEntry{
			entries: make([]map[string]string, 1),
			lastId:  "0-0",
		}
	}

	validatedId, err := validateStreamEntryId(id, stream.lastId)
	if err != nil {
		return nil, err
	}

	entry["id"] = validatedId

	for i := 0; i+1 < len(kv); i += 2 {
		entry[kv[i]] = kv[i+1]
	}

	stream.entries = append(stream.entries, entry)
	stream.lastId = validatedId
	status.databases[status.activeDB].streamStore[key] = stream

	return encodeBulkString(validatedId), nil
}
