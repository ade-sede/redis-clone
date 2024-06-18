package main

import (
	"context"
	"fmt"
	"math"
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
	initialDB := database{stringStore: make(map[string]stringEntry),
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

	return encodeRespBulkString(entry.value), nil
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

	return encodeRespInteger(deleted), nil
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

	return encodeRespStringArray(keys), nil
}

func typeFunc(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]

	_, inStringStore := status.databases[status.activeDB].stringStore[key]
	_, inStreamStore := status.databases[status.activeDB].streamStore[key]

	if inStringStore {
		return encodeRespSimpleString("string"), nil
	}

	if inStreamStore {
		return encodeRespSimpleString("stream"), nil
	}

	return encodeRespSimpleString("none"), nil
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
		return "", fmt.Errorf("%w The ID specified in XADD must be greater than 0-0\r\n", ErrRespSimpleError)
	}

	lastMs, lastSeq, err := parseStreamEntryId(lastId)
	if err != nil {
		return "", err
	}

	if newMs == -1 {
		newMs = int(time.Now().UnixMilli())
	}

	if newSeq == -1 {
		if newMs == lastMs {
			newSeq = lastSeq + 1
		} else {
			newSeq = 0
		}
	}

	if newMs < lastMs {
		return "", fmt.Errorf("%w The ID specified in XADD is equal or smaller than the target stream top item\r\n", ErrRespSimpleError)
	}

	if newMs == lastMs {
		if newSeq <= lastSeq {
			return "", fmt.Errorf("%w The ID specified in XADD is equal or smaller than the target stream top item\r\n", ErrRespSimpleError)
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
			entries: make([]map[string]string, 0),
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

	return encodeRespBulkString(validatedId), nil
}

// TODO xrange, xread, and stream type need major refactor
// unreadable code, spaghetti logic
func xrange(args []string) ([]byte, error) {
	if len(args) != 3 {
		return nil, ErrRespWrongNumberOfArguments
	}

	key := args[0]
	start := args[1]
	end := args[2]

	stream, ok := status.databases[status.activeDB].streamStore[key]
	if !ok {
		return []byte("*0\r\n"), nil
	}

	if start == "-" {
		start = "0-0"
	}

	if end == "+" {
		end = fmt.Sprintf("%d-%d", math.MaxInt, math.MaxInt)
	}

	startMs, startSeq, err := parseStreamEntryId(start)
	if err != nil {
		return nil, err
	}

	endMs, endSeq, err := parseStreamEntryId(end)
	if err != nil {
		return nil, err
	}

	if startSeq == -1 {
		startSeq = 0
	}

	if endSeq == -1 {
		endSeq = math.MaxInt
	}

	capturedEntries := make([]map[string]string, 0)

	for _, entry := range stream.entries {
		entryMs, entrySeq, err := parseStreamEntryId(entry["id"])
		if err != nil {
			return nil, err
		}

		if (entryMs > startMs || (entryMs == startMs && entrySeq >= startSeq)) &&
			(entryMs < endMs || (entryMs == endMs && entrySeq <= endSeq)) {
			capturedEntries = append(capturedEntries, entry)
		}

	}

	// Each entry is returned as an array of two elements
	// First element is the ID of the entry as a bulk strings
	// Second element is an array where all keys and values are bulk strings
	//
	// [
	//   [
	//     "1526985054069-0",
	//     [
	//       "temperature",
	//       "36",
	//       "humidity",
	//       "95"
	//     ]
	//   ],
	//   [
	//     "1526985054079-0",
	//     [
	//       "temperature",
	//       "37",
	//       "humidity",
	//       "94"
	//     ]
	//   ],
	// ]

	allRespEncodedEntries := make([][]byte, 0)
	for _, entry := range capturedEntries {
		id := encodeRespBulkString(entry["id"])

		allKVs := make([][]byte, 0)
		for k, v := range entry {
			if k == "id" {
				continue
			}

			key := encodeRespBulkString(k)
			value := encodeRespBulkString(v)

			allKVs = append(allKVs, key)
			allKVs = append(allKVs, value)
		}

		kvArray := encodeRespArray(allKVs)
		respEncodedEntry := encodeRespArray([][]byte{id, kvArray})
		allRespEncodedEntries = append(allRespEncodedEntries, respEncodedEntry)
	}

	response := encodeRespArray(allRespEncodedEntries)
	return response, nil
}

func xread(args []string) ([]byte, error) {
	var blockTimeout time.Duration
	var blocking bool = false

	if len(args) < 3 {
		return nil, ErrRespWrongNumberOfArguments
	}

	var streamArgs []string

	for i, arg := range args {
		if arg == "block" {
			if i+1 >= len(args) {
				return nil, ErrRespWrongNumberOfArguments
			}

			blocking = true
			timeout, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, err
			}

			blockTimeout = time.Duration(timeout) * time.Millisecond
		}
		if arg == "streams" {
			streamArgs = args[i+1:]
			break
		}
	}

	if len(streamArgs)%2 != 0 {
		return nil, ErrRespWrongNumberOfArguments
	}

	allCapturedEntries := make(map[string][]map[string]string)
	resultC := make(chan res)
	errorC := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < len(streamArgs)/2; i++ {
		key := streamArgs[i]
		id := streamArgs[i+(len(streamArgs)/2)]

		go xreadRoutine(ctx, key, id, resultC, errorC, blocking)
	}

	for {
		select {
		case res := <-resultC:
			allCapturedEntries[res.key] = res.entries
			if len(allCapturedEntries) == len(streamArgs)/2 {
				cancel()
				return xreadFormatReturn(allCapturedEntries), nil
			}
		case <-time.After(blockTimeout):
			if blocking {
				cancel()
				// Everything around the return format and conditions
				// is a spaghetti mess becaus I don't actually
				// understand it
				if len(allCapturedEntries) == 0 {
					return []byte("$-1\r\n"), nil
				}
				return xreadFormatReturn(allCapturedEntries), nil
			}
		}
	}
}

type res struct {
	key     string
	entries []map[string]string
}

func xreadRoutine(ctx context.Context, streamKey string, cutoffId string, resultC chan res, errorC chan error, blocking bool) {
	capturedEntries := make([]map[string]string, 0)

	cutoffMs, cutoffSeq, err := parseStreamEntryId(cutoffId)
	if err != nil {
		errorC <- err
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			stream, ok := status.databases[status.activeDB].streamStore[streamKey]
			if !ok {
				errorC <- fmt.Errorf("%w no stream for key %s", ErrRespSimpleError, streamKey)
				return
			}

			for _, entry := range stream.entries {
				entryId := entry["id"]
				entryMs, entrySeq, err := parseStreamEntryId(entryId)
				if err != nil {
					errorC <- err
					return
				}

				if entryMs > cutoffMs || (entryMs == cutoffMs && entrySeq > cutoffSeq) {
					capturedEntries = append(capturedEntries, entry)
				}
			}

			if blocking && len(capturedEntries) == 0 {
				continue
			}

			resultC <- res{
				key:     streamKey,
				entries: capturedEntries,
			}
			return
		}
	}
}

// Output format is complicated
// Variable naming is kind of a mess ... :'(
// [
//
//	[
//	  "stream_key",
//	  [
//	    [
//	      "0-1",
//	      [
//	        "foo",
//	        "bar"
//	      ]
//	    ]
//	  ]
//	],
//	[
//	  "other_stream_key",
//	  [
//	    [
//	      "0-2",
//	      [
//	        "bar",
//	        "baz"
//	      ]
//	    ]
//	  ]
//	]
//
// ]
func xreadFormatReturn(allCapturedEntries map[string][]map[string]string) []byte {
	allStreams := make([][]byte, 0)
	for streamKey, capturedEntries := range allCapturedEntries {
		encodedStreamKey := encodeRespBulkString(streamKey)

		allEncodedEntriesForKey := make([][]byte, 0)
		for _, entry := range capturedEntries {
			encodedId := encodeRespBulkString(entry["id"])

			allKVs := make([][]byte, 0)
			for k, v := range entry {
				if k == "id" {
					continue
				}

				key := encodeRespBulkString(k)
				value := encodeRespBulkString(v)

				allKVs = append(allKVs, key)
				allKVs = append(allKVs, value)
			}

			encodedKvArray := encodeRespArray(allKVs)
			encodedEntry := encodeRespArray([][]byte{encodedId, encodedKvArray})
			allEncodedEntriesForKey = append(allEncodedEntriesForKey, encodedEntry)
		}
		encodedStream := encodeRespArray([][]byte{encodedStreamKey, encodeRespArray(allEncodedEntriesForKey)})
		allStreams = append(allStreams, encodedStream)
	}

	response := encodeRespArray(allStreams)
	return response
}
