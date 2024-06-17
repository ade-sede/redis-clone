package main

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

type testCase struct {
	name     string
	input    string
	expected interface{}
	wantErr  bool
}

type arrayTestCase struct {
	name     string
	input    string
	expected []interface{}
	wantErr  bool
}

func TestAtoi(t *testing.T) {
	tests := []testCase{
		{"ValidInteger", "123\r\n", 123, false},
		{"InvalidInteger", "abc\r\n", 0, true},
		{"EmptyString", "", 0, true},
		// Required to support the null bulk string
		{"EmptyStringCRLF", "\r\n", 0, false},
		{"MissingCRLF", "123", 0, true},
		{"NegativeInteger", "-456\r\n", -456, false},
		{"NegativeInvalidInteger", "-abc\r\n", 0, true},
		{"NegativeMissingCRLF", "-789", 0, true},
		{"MalformedNegative", "--789", 0, true},
		{"ExplicitPositive", "+123\r\n", 123, false},
		{"MalformedPositive", "+++123\r\n", 0, true},
		{"Zero", "0\r\n", 0, false},
		{"WhitespacesBeforeNumber", "  123", 0, true},
		{"WhitespacesBeforeAndAfterNumber", "  123  ", 0, true},
		{"WhitespacesBetweenMinusAndNumber", "- 123", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, err := atoi(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("atoi() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("atoi() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseSimpleString(t *testing.T) {
	tests := []testCase{
		{"ValidSimpleString", "+hello\r\n", "hello", false},
		{"EmptyString", "", "", true},
		{"PrefixOnly", "+", "", true},
		{"MissingCRLF", "+hello", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, parseError := readResp(reader)

			if (parseError != nil) != tt.wantErr {
				t.Errorf("parseSimpleString() error = %v, wantErr %v", parseError, tt.wantErr)
				return
			}

			if got != nil {
				str, err := got.asString()
				if err != nil {
					t.Errorf("parseSimpleString() error = %v", err)
				}

				if str != tt.expected {
					t.Errorf("parseSimpleString() = %v, want %v", str, tt.expected)
				}
			}
		})
	}
}

func TestParseSimpleError(t *testing.T) {
	tests := []testCase{
		{"ValidSimpleError", "-Error message\r\n", "Error message", false},
		{"EmptyString", "", "", true},
		{"PrefixOnly", "-", "", true},
		{"MissingCRLF", "-Error message", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, parseError := readResp(reader)

			if (parseError != nil) != tt.wantErr {
				t.Errorf("parseSimpleError() error = %v, wantErr %v", parseError, tt.wantErr)
				return
			}

			if got != nil {
				str, err := got.asString()
				if err != nil {
					t.Errorf("parseSimpleError() error = %v", err)
				}

				if str != tt.expected {
					t.Errorf("parseSimpleError() = %v, want %v", str, tt.expected)
				}
			}
		})
	}
}

func TestParseInteger(t *testing.T) {
	tests := []testCase{
		{"ValidInteger", ":123\r\n", 123, false},
		{"EmptyString", "", 0, true},
		{"PrefixOnly", ":", 0, true},
		{"MissingCRLF", ":123", 123, true},
		{"NegativeInteger", ":-456\r\n", -456, false},
		{"ExplicitPositive", ":+9993123\r\n", 9993123, false},
		{"NegativeMissingCRLF", ":-789", -789, true},
		{"MalformedNegative", ":--789\r\n", 0, true},
		{"MalformedPositive", ":++123\r\n", 0, true},
		{"Zero", ":0\r\n", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, parseError := readResp(reader)

			if (parseError != nil) != tt.wantErr {
				t.Errorf("parseInteger() error = %v, wantErr %v", parseError, tt.wantErr)
				return
			}

			if got != nil {
				num, err := got.asInteger()
				if err != nil {
					t.Errorf("parseInteger() error = %v", err)
				}

				if num != tt.expected {
					t.Errorf("parseInteger() = %v, want %v", num, tt.expected)
				}
			}
		})
	}
}

func TestParseBulkString(t *testing.T) {
	tests := []testCase{
		{"ValidBulkString", "$5\r\nhello\r\n", "hello", false},
		{"EmptyString", "", "", true},
		{"PrefixOnly", "$", "", true},
		{"InvalidLength", "$abc\r\nhello\r\n", "", true},
		{"MissingCRLF", "$5\r\nhello", "", true},
		{"EmptyBulkString", "$0\r\n\r\n", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, parseError := readResp(reader)

			if (parseError != nil) != tt.wantErr {
				t.Errorf("parseBulkString() error = %v, wantErr %v", parseError, tt.wantErr)
				return
			}

			if got != nil {
				str, err := got.asString()
				if err != nil {
					t.Errorf("parseBulkString() error = %v", err)
				}

				if str != tt.expected {
					t.Errorf("parseBulkString() = %v, want %v", str, tt.expected)
				}
			}
		})
	}
}

func TestParseArray(t *testing.T) {
	tests := []arrayTestCase{
		{"ValidBulkStringArray", "*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$4\r\nfrom\r\n", []interface{}{"hello", "world", "from"}, false},
		{"ValidIntegerArray", "*3\r\n:1\r\n:2\r\n:3\r\n", []interface{}{1, 2, 3}, false},
		{"ValidMixedArray", "*3\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n", []interface{}{"hello", 2, "world"}, false},
		{"EmptyArray", "*0\r\n", []interface{}{}, false},
		{"EmptyString", "", nil, true},
		{"PrefixOnly", "*", nil, true},
		{"InvalidLength", "*abc\r\n:1\r\n:2\r\n:3\r\n", nil, true},
		{"MissingCRLF", "*3\r\n:1\r\n:2\r\n:3", nil, true},
		{"InvalidInteger", "*3\r\n:1\r\n:2\r\n:abc\r\n", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, parseError := readResp(reader)

			if (parseError != nil) != tt.wantErr {
				t.Errorf("parseArray() error = %v, wantErr %v", parseError, tt.wantErr)
				return
			}

			if got != nil {
				arr, err := got.asArray()
				if err != nil {
					t.Errorf("parseArray() error = %v", err)
				}

				if len(arr) != len(tt.expected) {
					t.Errorf("parseArray() = %v, want %v", arr, tt.expected)
				}

				helperArrayEquality(t, arr, tt.expected)
			}
		})
	}
}

func TestParseNestedArray(t *testing.T) {
	input := "*4\r\n*2\r\n:1\r\n:2\r\n$5\r\nhello\r\n$5\r\nworld\r\n:3\r\n"
	expected := []interface{}{
		[]interface{}{1, 2},
		"hello",
		"world",
		3,
	}

	reader := bufio.NewReader(bytes.NewReader([]byte(input)))
	got, parseError := readResp(reader)

	if parseError != nil {
		t.Errorf("parseNestedArray() error = %v", parseError)
		return
	}

	if got != nil {
		arr, err := got.asArray()
		if err != nil {
			t.Errorf("parseNestedArray() error = %v", err)
		}

		helperArrayEquality(t, arr, expected)
	}
}

func TestSegmentWithSeveralCommands(t *testing.T) {
	input := "$3\r\nSET\r\n$3\r\nkey\r\n"

	reader := bufio.NewReader(bytes.NewReader([]byte(input)))
	query, parseError := readResp(reader)

	if parseError != nil {
		t.Errorf("segmentWithSeveralCommands() error = %v", parseError)
		return
	}

	if query == nil {
		t.Errorf("segmentWithSeveralCommands() query is nil")
	}

	if query.value.(string) != "SET" {
		t.Errorf("segmentWithSeveralCommands() got = %v, want %v", query.value, "SET")
	}

	query, parseError = readResp(reader)
	if parseError != nil {
		t.Errorf("segmentWithSeveralCommands() error = %v", parseError)
		return
	}

	if query == nil {
		t.Errorf("segmentWithSeveralCommands() query is nil")
	}

	if query.value.(string) != "key" {
		t.Errorf("segmentWithSeveralCommands() got = %v, want %v", query.value, "key")
	}

	query, parseError = readResp(reader)

	if query != nil {
		t.Errorf("segmentWithSeveralCommands() query is not nil")
	}
}

func TestParseRdbFile(t *testing.T) {
	input := []byte(fmt.Sprintf("$%d\r\n%s", len(EMPTY_RDB_FILE), string(EMPTY_RDB_FILE)))
	reader := bufio.NewReader(bytes.NewReader(input))
	got, parseError := readResp(reader)

	if parseError != nil {
		t.Errorf("parseRdbFile() error = %v", parseError)
		return
	}

	if got == nil {
		t.Errorf("parseRdbFile() got = %v, want not nil", got)
	}

	if got.queryType != RDBFile {
		t.Errorf("parseRdbFile() got = %v, want %v", got.queryType, RDBFile)
	}

	val, ok := got.value.([]byte)
	if !ok {
		t.Errorf("parseRdbFile() val is not []byte")
	}

	if !bytes.Equal(val, EMPTY_RDB_FILE) {
		t.Errorf("parseRdbFile() got = %v, want %v", val, EMPTY_RDB_FILE)
	}

	if !bytes.Equal(got.raw(), input) {
		t.Errorf("parseRdbFile() got = %v, want %v", got.raw(), input)
	}
}

func TestSegmentWithSeveralCommandsIncludingRDBFile(t *testing.T) {
	input := []byte(fmt.Sprintf("$%d\r\n%s", len(EMPTY_RDB_FILE), string(EMPTY_RDB_FILE)))
	input = append(input, []byte("$3\r\nSET\r\n")...)

	reader := bufio.NewReader(bytes.NewReader(input))

	got, parseError := readResp(reader)

	if parseError != nil {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() error = %v", parseError)
		return
	}

	if got == nil {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() got = %v, want not nil", got)
	}

	if got.queryType != RDBFile {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() got = %v, want %v", got.queryType, RDBFile)
	}

	got, parseError = readResp(reader)
	if parseError != nil {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() error = %v", parseError)
		return
	}

	if got == nil {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() got = %v, want not nil", got)
	}

	if got.value.(string) != "SET" {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() got = %v, want %v", got.value, "SET")
	}

	got, parseError = readResp(reader)
	if got != nil {
		t.Errorf("segmentWithSeveralCommandsIncludingRDBFile() got = %v, want nil", got)
	}
}

func helperArrayEquality(t *testing.T, arr []*query, expected []interface{}) {
	if len(arr) != len(expected) {
		t.Errorf("helperArrayEquality() = %v, want %v", len(arr), len(expected))
	}

	for i, v := range arr {
		if v.queryType == Integer {
			num, err := v.asInteger()
			if err != nil {
				t.Errorf("helperArrayEquality() error = %v", err)
			}

			if num != expected[i] {
				t.Errorf("helperArrayEquality() = %v, want %v", arr, expected)
			}
		} else if v.queryType == BulkString || v.queryType == SimpleError || v.queryType == SimpleString {
			str, err := v.asString()
			if err != nil {
				t.Errorf("helperArrayEquality() error = %v", err)
			}

			if str != expected[i] {
				t.Errorf("helperArrayEquality() = %v, want %v", arr, expected)
			}
		} else if v.queryType == Array {
			nestedArr, err := v.asArray()

			if err != nil {
				t.Errorf("helperArrayEquality() error = %v", err)
			}

			helperArrayEquality(t, nestedArr, expected[i].([]interface{}))
		} else {
			t.Errorf("helperArrayEquality() unexpected query type: %v", v.queryType)
		}
	}
}

var EMPTY_RDB_FILE = generateEmptyRDBFile()

func generateEmptyRDBFile() []byte {
	databases := make(map[int]database)
	db := database{
		stringStore: make(map[string]stringEntry),
		streamStore: make(map[string]streamEntry),
	}

	databases[0] = db

	file, _ := encodeRDBFile(databases)
	return file
}
