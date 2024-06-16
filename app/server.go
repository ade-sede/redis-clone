package main

import (
	"bufio"
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
	dir           string
	dbFileName    string

	// One redis instance can host several databases
	// Each database has several stores.
	// One per data type.
	activeDB  int
	databases map[int]database
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

func main() {
	errorC := make(chan error, 100)
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	port := flag.Int("port", 6379, "port to listen to")
	flag.StringVar(&status.replicaof, "replicaof", "", "address and port of redis instance to follow")
	flag.StringVar(&status.dir, "dir", "", "directory to store the database")
	flag.StringVar(&status.dbFileName, "dbfilename", "dump.rdb", "name of the database file")
	flag.Parse()

	err := initStore()
	if err != nil {
		if errors.Is(err, ErrMissingRDBFile) {
			errorLogger.Println(err)
		} else {
			errorLogger.Fatalln(err)
		}
	}

	err = initReplication(*port, errorC)
	if err != nil {
		errorLogger.Fatalln(err)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		errorLogger.Fatalln(fmt.Errorf("Failed to start instance: err = %w", err))
	}
	defer l.Close()

	go func() {
		for err := range errorC {
			errorLogger.Println(err)
		}
	}()

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

		go handleConnection(&conn, false, errorC)
		status.globalLock.Unlock()
	}

}

func handleConnection(conn *connection, connectionToMaster bool, errorC chan error) {
	reader := bufio.NewReader(conn.handler)

	for {
		status.globalLock.Lock()
		status.globalLock.Unlock()

		conn.mu.Lock()
		conn.handler.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		query, err := readResp(reader)
		conn.mu.Unlock()

		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			} else if err == io.EOF {
				continue
			} else {
				conn.handler.Close()
				errorC <- err
				return
			}
		}

		response, command, err := execute(conn, query)
		if err != nil {
			if !connectionToMaster && errors.Is(err, ErrRespSimpleError) {
				conn.handler.Write([]byte(err.Error()))
			}

			errorC <- fmt.Errorf("Error executing the command: err = %w", err)
			continue
		}

		if response != nil {
			if !connectionToMaster {
				conn.handler.Write(response)
			} else if connectionToMaster && command == REPLCONF_GETACK {
				conn.handler.Write(response)
			}
		}

		if query.queryType != RDBFile {
			rawQuery := query.raw()
			status.replOffset += len(rawQuery)

			if command == SET || command == DEL {
				go replicate(rawQuery)
			}
		}
	}
}
