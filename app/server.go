package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

type entry struct {
	value     any
	expiresAt *time.Time
}

var data map[string]entry

func main() {
	data = make(map[string]entry)
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	port := flag.Int("port", 6379, "port to listen to")
	flag.StringVar(&replicationInfo.replicaof, "replicaof", "", "address and port of redis instance to follow")
	flag.Parse()

	replicationConnection, err := initReplication(*port)
	if err != nil {
		errorLogger.Fatalln(err)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		errorLogger.Fatalln(fmt.Errorf("Failed to start server: err = %w", err))
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
		conn, err := l.Accept()
		if err != nil {
			errorLogger.Println(fmt.Errorf("Error accepting TCP connection: err = %w", err))
			continue
		}

		go handleConnection(conn, false, errorChannel)
	}

}

func mustPropagateToReplicas(command command, isReplicationChannel bool) bool {
	if isReplicationChannel {
		return false
	}

	if command == SET {
		return true
	}

	return false
}

func handleConnection(conn net.Conn, isReplicationChannel bool, errorChannel chan error) {
	fmt.Println("New TCP connection from: ", conn.RemoteAddr().String())
	for {
		// Note:
		// One TCP segment may contain several queries. We need to
		// parse and execute them one by one, which is why we have so
		// many nested loops.

		// Another possibility is that one command could be so big it
		// does not hold within one segment. Currently unsupported
		buf := make([]byte, 4096)
		_, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				errorChannel <- fmt.Errorf("Error reading from TCP connection: err = %w", err)
			}
			conn.Close()
			return
		}

		fmt.Println("Received: ", string(buf))
		offset := 0

		for {
			query, err := parseResp(buf, &offset)
			if err != nil && err != ErrPossibleRDBFile {
				errorChannel <- fmt.Errorf("Error parsing query. buf = %s, offset = %d, err = %w", string(buf), offset, err)
				return
			}

			if query != nil {
				response, command, err := execute(conn, query)
				if err != nil {
					if errors.Is(err, ErrRespSimpleError) {
						conn.Write([]byte(err.Error()))
					}

					errorChannel <- fmt.Errorf("Error executing the command: err = %w", err)
					return
				}

				if response != nil && !isReplicationChannel {
					_, err = conn.Write(response)
					if err != nil {
						errorChannel <- fmt.Errorf("Error writing to TCP connection: err = %w", err)
						return
					}
				}

				if mustPropagateToReplicas(command, isReplicationChannel) {
					for _, replica := range replicationInfo.replicas {
						go func() {
							_, err := replica.conn.Write(query.raw)
							if err != nil {
								errorChannel <- fmt.Errorf("Error propagating to replica: err = %w", err)
							}
						}()
					}
				}
			}

			// If we are done reading all commands inside the 4096 byte
			// segment, we can go back to listening for messages
			if offset >= len(buf) || buf[offset] == 0 {
				break
			}
		}

	}
}
