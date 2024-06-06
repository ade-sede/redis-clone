package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type connection struct {
	port    int
	handler net.Conn
	mu      sync.Mutex
}

type instanceStatus struct {
	globalLock    sync.Mutex
	replId        string
	replOffset    int
	replicas      map[string]*replica // indexed by conn.RemoteAddr().String()
	replicaof     string              // "<IP> <PORT>"
	masterAddress string              // "<IP>:<PORT>"
	masterIp      string
	masterPort    int
}

func (status *instanceStatus) findReplica(conn net.Conn) *replica {
	remoteAddr := conn.RemoteAddr().String()

	replica, ok := status.replicas[remoteAddr]
	if !ok {
		return nil
	}

	return replica
}

var status instanceStatus

type entry struct {
	value     any
	expiresAt *time.Time
}

var data map[string]entry

func main() {
	data = make(map[string]entry)
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	port := flag.Int("port", 6379, "port to listen to")
	flag.StringVar(&status.replicaof, "replicaof", "", "address and port of redis instance to follow")
	flag.Parse()

	replicationConnection, err := initReplication(*port)
	if err != nil {
		errorLogger.Fatalln(err)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		errorLogger.Fatalln(fmt.Errorf("Failed to start instance: err = %w", err))
	}
	defer l.Close()

	errorChannel := make(chan error, 100)

	go func() {
		for err := range errorChannel {
			errorLogger.Println(err)
		}
	}()

	if replicationConnection != nil {
		go handleConnection(replicationConnection, true, errorChannel)
	}

	for {
		handler, err := l.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			errorLogger.Println(fmt.Errorf("Error accepting TCP connection: err = %w", err))
			continue
		}

		status.globalLock.Lock()

		conn := connection{
			handler: handler,
			port:    handler.RemoteAddr().(*net.TCPAddr).Port,
		}

		go handleConnection(&conn, false, errorChannel)
		status.globalLock.Unlock()
	}

}

func readParse(conn *connection) ([]*query, error) {
	buf := make([]byte, 4096)
	queries := make([]*query, 0)
	offset := 0

	// This way we do not block the lock, we only check whether or not someone is blocking it
	status.globalLock.Lock()
	status.globalLock.Unlock()

	conn.mu.Lock()
	conn.handler.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	n, err := conn.handler.Read(buf)
	conn.mu.Unlock()

	if err != nil {
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			return []*query{}, nil
		} else if err == io.EOF {
			return nil, nil
		} else {
			conn.handler.Close()
			return nil, err
		}
	}

	for {
		query, doneReading, err := parseResp(buf[:n], &offset)
		if err != nil {
			return nil, fmt.Errorf("Error parsing buf: `%s`. %w", string(buf[:n]), err)
		}

		if query != nil && query.queryType != RDBFile {
			queries = append(queries, query)
		}

		if doneReading {
			break
		}
	}

	return queries, nil
}

func handleConnection(conn *connection, toFollower bool, errorChannel chan error) {
	for {
		queries, err := readParse(conn)
		if err != nil {
			errorChannel <- err
			return
		}

		for _, query := range queries {
			response, command, err := execute(conn, query)
			if err != nil {
				if !toFollower && errors.Is(err, ErrRespSimpleError) {
					conn.handler.Write([]byte(err.Error()))
				}

				errorChannel <- fmt.Errorf("Error executing the command: err = %w", err)
				return
			}

			if response != nil {
				if !toFollower {
					conn.handler.Write(response)
				} else if toFollower && command == REPLCONF_GETACK {
					conn.handler.Write(response)
				}
			}

			status.replOffset += len(query.raw)

			if command == SET {
				go replicate(query.raw)
			}
		}
	}
}
