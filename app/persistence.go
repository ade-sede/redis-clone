package main

// Mandatory read
// https://rdb.fnordig.de/file_format.html

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func config(args []string) ([]byte, error) {
	response := make([]string, 0)

	if len(args) < 2 || !strings.EqualFold(args[0], "GET") {
		return nil, fmt.Errorf("%w expected GET subcommand with at least one resource name", ErrRespSimpleError)
	}

	for _, arg := range args[1:] {
		if arg == "dir" {
			response = append(response, "dir")
			response = append(response, status.dir)
		} else if arg == "dbfilename" {
			response = append(response, "dbfilename")
			response = append(response, status.dbFileName)
		} else {
			return nil, fmt.Errorf("%w unsupported config option: %s", ErrRespSimpleError, arg)
		}
	}

	return encodeStringArray(response), nil
}

func readEncodedLength(reader *bufio.Reader) (int, error) {
	length := 0

	b, err := reader.ReadByte()
	if err != nil {
		return -1, err
	}

	if b>>6 == 0b00 {
		length = int(b)
	} else if b>>6 == 0b01 {
		nextByte, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}

		err = binary.Read(
			bytes.NewReader([]byte{b & 0b001111, nextByte}),
			binary.BigEndian,
			&length,
		)
		if err != nil {
			return -1, err
		}
	} else if b>>6 == 0b10 {
		err := binary.Read(reader, binary.BigEndian, &length)
		if err != nil {
			return -1, err
		}
	}

	return length, nil
}

func readEncodedString(reader *bufio.Reader) (string, error) {
	encodedIntegerSize := 0

	b, err := reader.Peek(1)
	if err != nil {
		return "", err
	}

	if b[0] == 0xC0 {
		encodedIntegerSize = 1
	} else if b[0] == 0xC1 {
		encodedIntegerSize = 2
	} else if b[0] == 0xC2 {
		encodedIntegerSize = 4
	} else if b[0] == 0xC3 {
		return "", fmt.Errorf("LZF decompression is not supported")
	}

	if encodedIntegerSize > 0 {
		reader.Discard(1)

		number := 0
		buf := make([]byte, encodedIntegerSize)
		n, err := reader.Read(buf)
		if err != nil {
			return "", err
		}

		if n != encodedIntegerSize {
			return "", fmt.Errorf("Expected %d bytes, got %d", encodedIntegerSize, n)
		}

		if encodedIntegerSize == 1 {
			number = int(buf[0])
		} else if encodedIntegerSize == 2 {
			number = int(binary.LittleEndian.Uint16(buf))
		} else if encodedIntegerSize == 4 {
			number = int(binary.LittleEndian.Uint32(buf))
		}

		return strconv.Itoa(int(number)), nil
	}

	length, err := readEncodedLength(reader)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length)
	n, err := reader.Read(buf)
	if err != nil {
		return "", err
	}

	if n != length {
		return "", fmt.Errorf("Expected %d bytes, got %d", length, n)
	}

	return string(buf), nil
}

func readAuxiliaryField(reader *bufio.Reader) (key string, value string, err error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", "", err
	}

	if prefix != 0xFA {
		return "", "", fmt.Errorf("Expected 0xFA, got %02x", prefix)
	}

	key, err = readEncodedString(reader)
	if err != nil {
		return "", "", err
	}

	value, err = readEncodedString(reader)
	if err != nil {
		return "", "", err
	}

	return key, value, nil
}

func readDbSelector(reader *bufio.Reader) (int, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return -1, err
	}

	if prefix != 0xFE {
		return -1, fmt.Errorf("Expected 0xFE, got %02x", prefix)
	}

	return readEncodedLength(reader)
}

func readResizeDBSection(reader *bufio.Reader) error {
	prefix, err := reader.ReadByte()
	if err != nil {
		return err
	}

	if prefix != 0xFB {
		return fmt.Errorf("Expected 0xFB, got %02x", prefix)
	}

	// hash table size
	_, err = readEncodedLength(reader)
	if err != nil {
		return err
	}

	// expiry hash table size
	_, err = readEncodedLength(reader)
	if err != nil {
		return err
	}

	return nil
}

func readDatabaseEntry(reader *bufio.Reader) (string, *entry, error) {
	var expiresAt *time.Time

	b, err := reader.Peek(1)
	if err != nil {
		return "", nil, err
	}

	if b[0] == 0xFD { // expiry timestamp in seconds, 4 bytes unsigned int
		reader.Discard(1)
		expiryTime := make([]byte, 4)
		n, err := reader.Read(expiryTime)
		if err != nil {
			return "", nil, err
		}

		if n != 4 {
			return "", nil, fmt.Errorf("Expected 4 bytes, got %d", n)
		}

		expirySeconds := binary.LittleEndian.Uint32(expiryTime)
		tmp := time.Unix(int64(expirySeconds), 0)
		expiresAt = &tmp
	}

	if b[0] == 0xFC { // expiry timestamp in milliseconds, 8 bytes unsigned long
		reader.Discard(1)

		expiryTime := make([]byte, 8)
		n, err := reader.Read(expiryTime)
		if err != nil {
			return "", nil, err
		}

		if n != 8 {
			return "", nil, fmt.Errorf("Expected 8 bytes, got %d", n)
		}

		expiryMilliseconds := binary.LittleEndian.Uint64(expiryTime)
		tmp := time.Unix(0, int64(expiryMilliseconds)*int64(time.Millisecond))
		expiresAt = &tmp
	}

	valueType, err := reader.ReadByte()
	if err != nil {
		return "", nil, err
	}

	if valueType != 0x00 {
		return "", nil, fmt.Errorf("Expected 0x00, got %02x. Unsupported value type", valueType)

	}

	key, err := readEncodedString(reader)
	if err != nil {
		return "", nil, err
	}

	value, err := readEncodedString(reader)
	if err != nil {
		return "", nil, err
	}

	return key, &entry{
		value:     value,
		expiresAt: expiresAt,
	}, nil
}

func readDatabaseSection(reader *bufio.Reader) (int, map[string]entry, error) {
	databaseNumber := -1
	store := make(map[string]entry)

	databaseNumber, err := readDbSelector(reader)
	if err != nil {
		return databaseNumber, store, err
	}

	for {
		b, err := reader.Peek(1)
		if err != nil {
			return databaseNumber, store, err
		}

		if b[0] == 0xFF || b[0] == 0xFE {
			break
		}

		if b[0] == 0xFB {
			err := readResizeDBSection(reader)
			if err != nil {
				return databaseNumber, store, err
			}
		} else {
			key, entry, err := readDatabaseEntry(reader)
			if err != nil {
				return databaseNumber, store, err
			}

			store[key] = *entry
		}
	}

	return databaseNumber, store, nil
}

func readRDBFile(reader *bufio.Reader) error {
	metadata := make(map[string]string)

	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	if err != nil {
		return err
	}

	if n != 5 || !bytes.Equal([]byte("REDIS"), buf) {
		return fmt.Errorf("Expected magic string `REDIS`, got %s", string(buf))
	}

	versionNumber := make([]byte, 4)
	n, err = reader.Read(versionNumber)
	if err != nil {
		return err
	}

	if n != 4 {
		return fmt.Errorf("Expected version number to be 4 digit long, got %d", n)
	}

	for {
		b, err := reader.Peek(1)
		if err != nil {
			return err
		}

		if b[0] == 0xFA {
			key, value, err := readAuxiliaryField(reader)
			if err != nil {
				return err
			}

			metadata[key] = value
		} else if b[0] == 0xFE {
			dbNumber, db, err := readDatabaseSection(reader)
			if err != nil {
				return err
			}

			status.store[dbNumber] = db
		} else if b[0] == 0xFF {
			// TODO checksum
			return nil
		} else {
			return fmt.Errorf("Unexpected byte: %02x", b[0])
		}
	}
}

func initPersistence() error {
	fileName := fmt.Sprintf("%s/%s", status.dir, status.dbFileName)
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	err = readRDBFile(reader)
	return err
}
