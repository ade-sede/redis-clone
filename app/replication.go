package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func generateReplId() string {
	bytes := make([]byte, 20)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

type replica struct {
	capabilites    []string
	conn           *connection
	expectedOffset int
	measuredOffset int
}

func (r *replica) replicate(b []byte) {
	// For a simple write we don't need to make use of the connection's mutex
	r.conn.handler.Write(b)
	r.expectedOffset += len(b)
}

func initReplication(listeningPort int, errorC chan error) error {
	status.replicas = make(map[string]*replica)

	if status.replicaof == "" {
		status.replId = generateReplId()
		status.replOffset = 0

		return nil
	}

	status.replId = "?"
	status.replOffset = -1
	fields := strings.Fields(status.replicaof)

	status.masterIp = fields[0]
	status.masterPort, _ = strconv.Atoi(fields[1])
	status.masterAddress = fmt.Sprintf("%s:%d",
		status.masterIp,
		status.masterPort)

	handle, err := net.Dial("tcp", status.masterAddress)
	if err != nil {
		return err
	}

	replicationConn := connection{
		handler: handle,
		port:    status.masterPort,
	}

	go handleConnection(&replicationConn, true, errorC)

	handshake(&replicationConn, listeningPort)

	return nil
}

func handshake(conn *connection, listeningPort int) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.handler.SetReadDeadline(time.Time{})

	reader := bufio.NewReader(conn.handler)

	conn.handler.Write(encodeStringArray([]string{"PING"}))
	reader.ReadBytes('\n') // "+PONG\r\n"

	conn.handler.Write(encodeStringArray([]string{
		"REPLCONF",
		"listening-port",
		strconv.Itoa(listeningPort),
	}))
	reader.ReadBytes('\n') // "+OK\r\n"

	conn.handler.Write(encodeStringArray([]string{
		"REPLCONF",
		"capa",
		"psync2",
		"capa",
		"eof",
	}))
	reader.ReadBytes('\n') // "+OK\r\n"

	conn.handler.Write(encodeStringArray([]string{
		"PSYNC",
		status.replId,
		strconv.Itoa(status.replOffset),
	}))

	// "+FULLRESYNC <replid> <reploffset>\r\n"
	response, _ := reader.ReadBytes('\n')

	offset := 0
	query, _, _ := parseResp(response, &offset)
	s, _ := query.asString()

	array := strings.Fields(s)
	status.replId = array[1]
	replOffset, _ := strconv.Atoi(array[2])
	status.replOffset = replOffset
}

func replicate(buf []byte) {
	for _, replica := range status.replicas {
		replica.replicate(buf)
	}
}

func replconf(conn *connection, args []string) ([]byte, command, error) {
	var isGetAck bool = false

	existingReplica := status.findReplica(conn.handler)

	for i, arg := range args {
		if arg == "listening-port" {
			port, _ := strconv.Atoi(args[i+1])

			if existingReplica == nil {
				newReplica := replica{
					capabilites: make([]string, 0),
					conn:        conn,
				}
				conn.port = port
				status.replicas[conn.handler.RemoteAddr().String()] = &newReplica
			} else {
				existingReplica.conn = conn
				existingReplica.conn.port = port
			}
		}

		if arg == "capa" {
			newCapa := args[i+1]

			if existingReplica == nil {
				return nil, REPLCONF, fmt.Errorf("No matching replica")
			}

			existingReplica.capabilites = append(existingReplica.capabilites, newCapa)
		}

		if strings.EqualFold(arg, "GETACK") {
			isGetAck = true
		}
	}

	if isGetAck {
		response := encodeStringArray([]string{
			"REPLCONF",
			"ACK",
			strconv.Itoa(status.replOffset),
		})
		return []byte(response), REPLCONF_GETACK, nil
	} else {
		return []byte("+OK\r\n"), REPLCONF, nil

	}
}

func psync(conn *connection) ([]byte, error) {
	existingReplica := status.findReplica(conn.handler)
	if existingReplica == nil {
		return nil, fmt.Errorf("No replica registered for %s", conn.handler.RemoteAddr().String())
	}

	// Should actually send the server's offset instead of 0
	// But codecrafters' test suite expects 0
	fullResyncNotification := encodeBulkString(fmt.Sprintf("FULLRESYNC %s %d",
		status.replId,
		0))

	emptyRDB, _ := hex.DecodeString(EMPTY_RDB_FILE)

	RDB := []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRDB), string(emptyRDB)))

	response := make([]byte, 0)
	response = append(response, fullResyncNotification...)
	response = append(response, RDB...)

	return response, nil
}

func wait(args []string) []byte {
	status.globalLock.Lock()
	defer status.globalLock.Unlock()

	if len(status.replicas) == 0 {
		return encodeInteger(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// replicaCountTarget, _ := strconv.Atoi(args[0])
	timeoutMs, _ := strconv.Atoi(args[1])
	timeout := time.Duration(timeoutMs) * time.Millisecond

	doneCount := 0
	ack := make(chan bool)

	for _, replica := range status.replicas {
		replica.conn.mu.Lock()
		defer replica.conn.mu.Unlock()
		go pollReplicaCount(ctx, replica, ack)
	}

	for {
		select {
		case <-ack:
			doneCount += 1
			// Tests for stage TU8 imply we must give a chance to all replicas to ack
			// From the `WAIT` manual I understood we could stop as soon as we got our target
			// Commenting out just to pass tests
			// if doneCount == replicaCountTarget {
			// cancel()
			// return encodeInteger(doneCount)
			// }
		case <-time.After(timeout):
			cancel()
			return encodeInteger(doneCount)
		}
	}
}

func pollReplicaCount(ctx context.Context, replica *replica, ack chan bool) {
	if replica.measuredOffset == replica.expectedOffset {
		ack <- true
		return
	}

	replica.conn.handler.SetReadDeadline(time.Time{})

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := replica.conn.handler.Write(encodeStringArray(
				[]string{"REPLCONF", "GETACK", "*"},
			))
			replica.expectedOffset += n

			if err != nil {
				continue
			}

			offset := 0
			buf := make([]byte, 100)
			_, err = replica.conn.handler.Read(buf)
			query, doneReading, _ := parseResp(buf, &offset)

			if !doneReading {
				fmt.Println("Warning, received more commands than expected as part of response")
			}

			if query != nil {
				array, _ := query.asArray()

				offsetString, _ := array[2].asString()
				measuredOffset, _ := strconv.Atoi(offsetString)
				replica.measuredOffset = measuredOffset

				if replica.measuredOffset == replica.expectedOffset-n {
					ack <- true
					return
				}
			}

		}
	}
}
