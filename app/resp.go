package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"unicode"
)

var (
	ErrRespSimpleError            = fmt.Errorf("-ERR")
	ErrRespWrongNumberOfArguments = fmt.Errorf("%w wrong number of arguments\r\n", ErrRespSimpleError)
	ErrOutOfBounds                = fmt.Errorf("Requested index is out of bounds")
	ErrMissingCRLF                = fmt.Errorf("Missing CRLF")
)

type queryType int

const (
	SimpleString queryType = iota
	SimpleError
	Integer
	BulkString
	Array
	RDBFile
)

type query struct {
	queryType queryType
	value     interface{}
}

func (q *query) raw() []byte {
	if q.queryType == SimpleString {
		return encodeSimpleString(q.value.(string))
	} else if q.queryType == SimpleError {
		return encodeSimpleError(q.value.(string))
	} else if q.queryType == Integer {
		return encodeInteger(q.value.(int))
	} else if q.queryType == BulkString {
		return encodeBulkString(q.value.(string))
	} else if q.queryType == Array {
		array := q.value.([]*query)

		response := make([]byte, 0)
		response = append(response, fmt.Sprintf("*%d\r\n", len(array))...)

		for _, innerQuery := range array {
			raw := innerQuery.raw()
			response = append(response, raw...)
		}

		return response
	} else if q.queryType == RDBFile {
		val := q.value.([]byte)

		response := make([]byte, 0)
		response = append(response, fmt.Sprintf("$%d\r\n", len(val))...)
		response = append(response, val...)

		return response
	} else {
		panic("Unsupported query type")
	}

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

func atoi(reader *bufio.Reader) (int, error) {
	var n int

	number, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	withoutCRLF := number[:len(number)-2]

	if withoutCRLF == "" {
		return 0, nil
	}

	n, err = strconv.Atoi(string(withoutCRLF))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func parseSimpleString(reader *bufio.Reader) (*query, error) {
	buf, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	return &query{
		queryType: SimpleString,
		value:     string(buf[:len(buf)-2]),
	}, nil
}

func parseSimpleError(reader *bufio.Reader) (*query, error) {
	query, err := parseSimpleString(reader)
	if err != nil {
		return nil, err
	}

	query.queryType = SimpleError

	return query, nil
}

func parseBulkString(reader *bufio.Reader) (*query, error) {
	length, err := atoi(reader)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		reader.Discard(2)
		return &query{
			queryType: BulkString,
			value:     nil,
		}, nil
	}

	data := make([]byte, length)
	n, err := reader.Read(data)
	if err != nil {
		return nil, err
	}

	if n != length {
		return nil, fmt.Errorf("Expected %d bytes, got %d", length, n)
	}

	data = data[:n]

	suffix, err := reader.Peek(2)
	if !bytes.Equal(suffix, []byte("\r\n")) {
		// RDB files and bulk strings share a similar format
		// Similar prefix, followed by length of content
		// Only difference is there is no CLRF at the end of RDB files,
		// and it starts with the `REDIS` magic string
		if bytes.HasPrefix(data, []byte("REDIS")) {
			return &query{
				queryType: RDBFile,
				value:     data,
			}, nil
		}
	}

	if err != nil {
		return nil, err
	}

	reader.Discard(len(suffix))

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

func encodeSimpleError(str string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", str))
}

func encodeInteger(i int) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

func parseArray(reader *bufio.Reader) (*query, error) {
	length, err := atoi(reader)
	if err != nil {
		return nil, err
	}

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
		elem, err := readResp(reader)
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

func readRespFromBuffer(b []byte) (*query, error) {
	reader := bufio.NewReader(bytes.NewReader(b))
	return readResp(reader)
}

func readRespFromNetwork(conn *connection) (*query, error) {
	reader := bufio.NewReader(conn.handler)
	return readResp(reader)
}

func readResp(reader *bufio.Reader) (*query, error) {
	var q *query
	var prefix byte
	var err error

	prefix, err = reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		q, err = parseSimpleString(reader)
		if err != nil {
			return nil, err
		}
	case '-':
		q, err = parseSimpleError(reader)
		if err != nil {
			return nil, err
		}
	case ':':
		n, err := atoi(reader)
		if err != nil {
			return nil, err
		}

		q = &query{
			queryType: Integer,
			value:     n,
		}
	case '$':
		q, err = parseBulkString(reader)
		if err != nil {
			return nil, err
		}
	case '*':
		q, err = parseArray(reader)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unexpected character `%c`(hex %02x)", prefix, prefix)
	}

	return q, nil
}
