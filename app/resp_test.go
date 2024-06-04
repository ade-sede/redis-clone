package main

import (
	"bytes"
	"errors"
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

func TestExtractBytes(t *testing.T) {
	buf := []byte("12345")

	t.Run("HappyPath", func(t *testing.T) {
		offset := 0
		got, err := extractBytes(buf, &offset, 5)

		if err != nil {
			t.Errorf("extractBytes() error = %v, wantErr %v", err, false)
		}

		if !bytes.Equal(got, buf) {
			t.Errorf("extractBytes() got = %v, want %v", got, buf)
		}

		if offset != 5 {
			t.Errorf("extractBytes() offset = %v, want %v", offset, 5)
		}
	})
	t.Run("OutOfBounds", func(t *testing.T) {
		offset := 0
		_, err := extractBytes(buf, &offset, 6)

		if !errors.Is(err, ErrOutOfBounds) {
			t.Errorf("extractBytes() error = %v, wantErr %v", err, ErrOutOfBounds)
		}
	})
}

func TestAtoi(t *testing.T) {
	tests := []testCase{
		{"ValidInteger", "123\r\n", 123, false},
		{"InvalidInteger", "abc\r\n", 0, true},
		{"EmptyString", "", 0, true},
		// Required to support the null bulk string
		{"EmptyStringCRLF", "\r\n", 0, false},
		{"MissingCRLF", "123", 123, true},
		{"NegativeInteger", "-456\r\n", -456, false},
		{"NegativeInvalidInteger", "-abc\r\n", 0, true},
		{"NegativeMissingCRLF", "-789", -789, true},
		{"MalformedNegative", "--789", 0, true},
		{"ExplicitPositive", "+123\r\n", 123, false},
		{"MalformedPositive", "+++123\r\n", 0, true},
		{"Zero", "0\r\n", 0, false},
		{"WhitespacesBeforeNumber", "  123", 0, true},
		{"WhitespacesAfterNumber", "123  ", 123, true},
		{"WhitespacesBeforeAndAfterNumber", "  123  ", 0, true},
		{"WhitespacesBetweenMinusAndNumber", "- 123", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset := 0
			got, err := atoi([]byte(tt.input), &offset)
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
			offset := 0
			got, _, parseError := parseResp([]byte(tt.input), &offset)

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
			offset := 0
			got, _, parseError := parseResp([]byte(tt.input), &offset)

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
			offset := 0
			got, _, parseError := parseResp([]byte(tt.input), &offset)

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
			offset := 0
			got, _, parseError := parseResp([]byte(tt.input), &offset)

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
			offset := 0
			got, _, parseError := parseResp([]byte(tt.input), &offset)

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

	offset := 0
	got, _, parseError := parseResp([]byte(input), &offset)

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

func TestSegmentWithSeveralCommands(t *testing.T) {
	input := "$3\r\nSET\r\n$3\r\nkey\r\n"

	offset := 0
	_, doneReading, parseError := parseResp([]byte(input), &offset)

	if parseError != nil {
		t.Errorf("segmentWithSeveralCommands() error = %v", parseError)
		return
	}

	if doneReading {
		t.Errorf("segmentWithSeveralCommands() doneReading = %v, want %v", doneReading, false)
	}

	_, doneReading, parseError = parseResp([]byte(input), &offset)
	if parseError != nil {
		t.Errorf("segmentWithSeveralCommands() error = %v", parseError)
		return
	}

	if !doneReading {
		t.Errorf("segmentWithSeveralCommands() doneReading = %v, want %v", doneReading, true)
	}
}
