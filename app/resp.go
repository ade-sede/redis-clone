package main

import (
	"bytes"
	"fmt"
	"slices"
	"unicode"
)

var (
	ErrOutOfBounds = fmt.Errorf("Out of bounds")
	ErrMissingCRLF = fmt.Errorf("Missing CRLF")
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
}

func (q *query) asSimpleString() (string, error) {
	if q.queryType != SimpleString {
		return "", fmt.Errorf("Query is not a simple string")
	}

	str, ok := q.value.(string)
	if !ok {
		return "", fmt.Errorf("Value is not a string")
	}

	return str, nil
}

func (q *query) asSimpleError() (string, error) {
	if q.queryType != SimpleError {
		return "", fmt.Errorf("Query is not a simple error")
	}

	str, ok := q.value.(string)
	if !ok {
		return "", fmt.Errorf("Value is not a string")
	}

	return str, nil
}

func (q *query) asBulkString() (string, error) {
	if q.queryType != BulkString {
		return "", fmt.Errorf("Query is not a bulk string")
	}

	str, ok := q.value.(string)
	if !ok {
		return "", fmt.Errorf("Value is not a string")
	}

	return str, nil
}

func (q *query) asArray() ([]*query, error) {
	if q.queryType != Array {
		return nil, fmt.Errorf("Query is not an array")
	}

	array, ok := q.value.([]*query)
	if !ok {
		return nil, fmt.Errorf("Value is not an array of queries")
	}

	return array, nil
}

func (q *query) asInteger() (int, error) {
	if q.queryType != Integer {
		return 0, fmt.Errorf("Query is not an integer")
	}

	i, ok := q.value.(int)
	if !ok {
		return 0, fmt.Errorf("Value is not an integer")
	}

	return i, nil
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
		return nil, ErrOutOfBounds
	}

	*offset = end

	return buf[start:end], nil
}

func atoi(buf []byte, offset *int) (int, error) {
	if *offset >= len(buf) {
		return 0, ErrOutOfBounds
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
		return nil, err
	}

	return &query{
		queryType: BulkString,
		value:     string(data),
	}, nil
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
		elem, err := parseResp(buf, offset)
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

func parseResp(buf []byte, offset *int) (*query, error) {
	if *offset >= len(buf) {
		return nil, ErrOutOfBounds
	}

	switch buf[*offset] {
	case '+':
		*offset += 1
		return parseSimpleString(buf, offset)
	case '-':
		*offset += 1
		return parseSimpleError(buf, offset)
	case ':':
		*offset += 1
		n, err := atoi(buf, offset)
		if err != nil {
			return nil, err
		}

		return &query{
			queryType: Integer,
			value:     n,
		}, nil
	case '$':
		*offset += 1
		return parseBulkString(buf, offset)
	case '*':
		*offset += 1
		return parseArray(buf, offset)
	default:
		return nil, fmt.Errorf("Unknown type")
	}
}
