package main

import (
	"bytes"
	"fmt"
	"slices"
	"unicode"
)

var (
	ErrRespSimpleError            = fmt.Errorf("-ERR")
	ErrRespWrongNumberOfArguments = fmt.Errorf("%w wrong number of arguments\r\n", ErrRespSimpleError)
	ErrOutOfBounds                = fmt.Errorf("Requested index is out of bounds")
	ErrMissingCRLF                = fmt.Errorf("Missing CRLF")
	ErrPossibleRDBFile            = fmt.Errorf("Possible RDB file")
)

type queryType int

const (
	SimpleString queryType = iota
	SimpleError
	Integer
	BulkString
	Array
)

type query struct {
	queryType queryType
	value     interface{}
	raw       []byte
}

func (q *query) asString() (string, error) {
	if q.queryType != SimpleString && q.queryType != BulkString && q.queryType != SimpleError {
		return "", fmt.Errorf("Expected a type that can be represented as string. One of %d, %d, %d or %d, got %d", SimpleString, BulkString, SimpleError, Integer, q.queryType)
	}

	str, ok := q.value.(string)
	if !ok {
		int, ok := q.value.(int)
		if !ok {
			return "", fmt.Errorf("Expected value to be a string or an int, got %T", q.value)
		}

		return fmt.Sprintf("%d", int), nil
	}

	return str, nil
}

func (q *query) asArray() ([]*query, error) {
	if q.queryType != Array {
		return nil, fmt.Errorf("Expected query type Array %d, got %d", Array, q.queryType)
	}

	array, ok := q.value.([]*query)
	if !ok {
		return nil, fmt.Errorf("Expected value to be an array, got %T", q.value)
	}

	return array, nil
}

func (q *query) asInteger() (int, error) {
	if q.queryType != Integer {
		return 0, fmt.Errorf("Expected query type Integer %d, got %d", Integer, q.queryType)
	}

	i, ok := q.value.(int)
	if !ok {
		return 0, fmt.Errorf("Expected value to be an integer, got %T", q.value)
	}

	return i, nil
}

func (q *query) asStringArray() ([]string, error) {
	strings := make([]string, 0)

	a, err := q.asArray()
	if err != nil {
		return nil, err
	}

	for _, innerQuery := range a {
		s, err := innerQuery.asString()
		if err != nil {
			return nil, err
		}

		strings = append(strings, s)
	}

	return strings, nil
}

func isDigit(b byte) bool {
	return unicode.IsDigit(rune(b))
}

func parseSuffix(buf []byte, offset *int) error {
	suffix, err := extractBytes(buf, offset, 2)
	if err != nil {
		return err
	}

	if !bytes.Equal(suffix, []byte("\r\n")) {
		return ErrMissingCRLF
	}

	return nil
}

func extractBytes(buf []byte, offset *int, size int) ([]byte, error) {
	start := *offset
	end := start + size

	if *offset+size > len(buf) {
		return nil, fmt.Errorf("%w, len = %d, offset = %d, size = %d", ErrOutOfBounds, len(buf), *offset, size)
	}

	*offset = end

	return buf[start:end], nil
}

func atoi(buf []byte, offset *int) (int, error) {
	if *offset >= len(buf) {
		return 0, fmt.Errorf("%w, len = %d, offset = %d", ErrOutOfBounds, len(buf), *offset)
	}

	var n int
	var sign int = 1

	if buf[*offset] == '-' {
		sign = -1
		*offset += 1
	} else if buf[*offset] == '+' {
		sign = 1
		*offset += 1
	}

	for ; *offset < len(buf); *offset += 1 {
		char := buf[*offset]

		if !isDigit(char) {
			break
		}

		n = n*10 + (int(char) - 48)
	}

	n = n * sign

	err := parseSuffix(buf, offset)
	if err != nil {
		return n, err
	}

	return n, nil
}

func parseSimpleString(buf []byte, offset *int) (*query, error) {
	i := slices.Index(buf, byte('\r'))
	if i == -1 {
		return nil, ErrMissingCRLF
	}

	i -= 1

	str := buf[*offset : *offset+i]
	*offset += i

	err := parseSuffix(buf, offset)
	if err != nil {
		return nil, err
	}

	return &query{
		queryType: SimpleString,
		value:     string(str),
	}, nil
}

func parseSimpleError(buf []byte, offset *int) (*query, error) {
	// Simple errors are just like simple strings
	query, err := parseSimpleString(buf, offset)
	if err != nil {
		return nil, err
	}

	query.queryType = SimpleError

	return query, nil
}

func parseBulkString(buf []byte, offset *int) (*query, error) {
	length, err := atoi(buf, offset)
	if err != nil {
		return nil, err
	}

	// Null bulk string
	if length < 0 {
		return &query{
			queryType: BulkString,
			value:     nil,
		}, nil
	}

	data, err := extractBytes(buf, offset, int(length))
	if err != nil {
		return nil, err
	}

	err = parseSuffix(buf, offset)
	if err != nil {
		// RDB Files look just like bulk string
		// The only way to distinguish between the two are:
		// - RDB files don't have a CRLF suffix
		// - RDB files are only expected after a PSYNC command
		// I dont feel like tracking state that accurately so for the
		// moment I plan on ignoring errors matching this specific case
		if err == ErrMissingCRLF {
			return nil, ErrPossibleRDBFile
		}
		return nil, err
	}

	return &query{
		queryType: BulkString,
		value:     string(data),
	}, nil
}

func encodeStringArray(a []string) []byte {
	response := make([]byte, 0)
	prefix := fmt.Sprintf("*%d\r\n", len(a))
	response = append(response, []byte(prefix)...)
	for _, s := range a {
		bulkString := encodeBulkString(s)
		response = append(response, bulkString...)
	}

	return response
}

func encodeBulkString(str string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
}

func encodeSimpleString(str string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", str))
}

func parseArray(buf []byte, offset *int) (*query, error) {
	length, err := atoi(buf, offset)
	if err != nil {
		return nil, err
	}

	// Null array
	if length < 0 {
		return &query{
			queryType: Array,
			value:     nil,
		}, nil
	}

	if length == 0 {
		return &query{
			queryType: Array,
			value:     []*query{},
		}, nil
	}

	arr := make([]*query, 0, length)

	for i := 0; i < length; i++ {
		elem, _, err := parseResp(buf, offset)
		if err != nil {
			return nil, err
		}

		arr = append(arr, elem)
	}

	return &query{
		queryType: Array,
		value:     arr,
	}, nil
}

func parseResp(buf []byte, offset *int) (*query, bool, error) {
	var err error
	var q *query

	start := *offset

	if *offset >= len(buf) {
		return nil, true, fmt.Errorf("%w, len = %d, offset = %d", ErrOutOfBounds, len(buf), *offset)
	}

	switch buf[*offset] {
	case '+':
		*offset += 1
		q, err = parseSimpleString(buf, offset)
		if err != nil {

			return nil, true, err
		}
	case '-':
		*offset += 1
		q, err = parseSimpleError(buf, offset)
		if err != nil {
			return nil, true, err
		}
	case ':':
		*offset += 1
		n, err := atoi(buf, offset)
		if err != nil {
			return nil, true, err
		}

		q = &query{
			queryType: Integer,
			value:     n,
		}
	case '$':
		*offset += 1
		q, err = parseBulkString(buf, offset)
		if err != nil {
			return nil, true, err
		}
	case '*':
		*offset += 1
		q, err = parseArray(buf, offset)
		if err != nil {
			return nil, true, err
		}
	default:
		return nil, true, fmt.Errorf("Unexpected character `%c` at offset %d", buf[*offset], *offset)
	}

	q.raw = buf[start:*offset]

	// Have we read everything there is to read ?
	if *offset >= len(buf) || buf[*offset] == 0 {
		return q, true, nil
	}

	return q, false, nil
}
