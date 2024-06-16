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

var ErrMissingRDBFile = fmt.Errorf("RDB file not found")

func initPersistence() error {
	fileName := fmt.Sprintf("%s/%s", status.dir, status.dbFileName)
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrMissingRDBFile, fileName)
		}
		return err
	}

	reader := bufio.NewReader(file)
	err = readRDBFile(reader)
	return err
}

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

func readDatabaseEntry(reader *bufio.Reader) (string, *stringEntry, error) {
	var expiresAt *time.Time

	b, err := reader.Peek(1)
	if err != nil {
		return "", nil, err
	}

	// TODO discard expired entries

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

	return key, &stringEntry{
		value:     value,
		expiresAt: expiresAt,
	}, nil
}

func readDatabaseSection(reader *bufio.Reader) (int, map[string]stringEntry, error) {
	databaseNumber := -1
	stringStore := make(map[string]stringEntry)

	databaseNumber, err := readDbSelector(reader)
	if err != nil {
		return databaseNumber, stringStore, err
	}

	for {
		b, err := reader.Peek(1)
		if err != nil {
			return databaseNumber, stringStore, err
		}

		if b[0] == 0xFF || b[0] == 0xFE {
			break
		}

		if b[0] == 0xFB {
			err := readResizeDBSection(reader)
			if err != nil {
				return databaseNumber, stringStore, err
			}
		} else {
			key, stringEntry, err := readDatabaseEntry(reader)
			if err != nil {
				return databaseNumber, stringStore, err
			}

			stringStore[key] = *stringEntry
		}
	}

	return databaseNumber, stringStore, nil
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
			dbNumber, stringStore, err := readDatabaseSection(reader)
			if err != nil {
				return err
			}

			currentDB := status.databases[dbNumber]
			currentDB.stringStore = stringStore
			status.databases[dbNumber] = currentDB
		} else if b[0] == 0xFF {
			// TODO: checksum
			reader.Discard(1)
			return nil
		} else {
			return fmt.Errorf("Unexpected byte: %02x", b[0])
		}
	}
}

func encodeRDBString(s string) []byte {
	buf := make([]byte, 0)
	length := len(s)

	if length > 63 {
		panic("String length is too long. Only the simplest string encoding is enabled at the moment.")
	}

	buf = append(buf, byte(length))
	buf = append(buf, []byte(s)...)

	return buf
}

func encodeRDBFile(store map[int]database) ([]byte, error) {
	buf := make([]byte, 0)

	buf = append(buf, []byte("REDIS")...)
	buf = append(buf, []byte("0009")...)

	buf = append(buf, []byte{0xFA}...)
	buf = append(buf, encodeRDBString("redis-version")...)
	buf = append(buf, encodeRDBString("ade-sede's custom redis")...)

	for dbNumber, db := range store {
		if dbNumber > 10 || dbNumber < 0 {
			return nil, fmt.Errorf("Invalid database number: %d. Only support [0:10]", dbNumber)
		}

		if len(db.stringStore) == 0 {
			continue
		}

		buf = append(buf, []byte{0xFE}...)
		buf = append(buf, byte(dbNumber))

		for key, entry := range db.stringStore {
			if entry.expiresAt != nil && entry.expiresAt.After(time.Now()) {
				timestamp := make([]byte, 8)

				buf = append(buf, []byte{0xFC}...)
				binary.LittleEndian.PutUint64(timestamp, uint64(entry.expiresAt.Unix()))
				buf = append(buf, timestamp...)
			}

			buf = append(buf, []byte{0x00}...)
			buf = append(buf, encodeRDBString(key)...)
			buf = append(buf, encodeRDBString(entry.value)...)
		}
	}

	buf = append(buf, []byte{0xFF}...)
	checksum := crc64(buf)
	buf = binary.LittleEndian.AppendUint64(buf, checksum)
	return buf, nil
}

func save() ([]byte, error) {
	if status.dbFileName == "" || status.dir == "" {
		return nil, fmt.Errorf("Database file name or directory is not set")
	}

	fileName := fmt.Sprintf("%s/%s", status.dir, status.dbFileName)
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}

	buf, err := encodeRDBFile(status.databases)
	if err != nil {
		return nil, err
	}

	_, err = file.Write(buf)
	if err != nil {
		return nil, err
	}

	return encodeSimpleString("OK"), nil
}
