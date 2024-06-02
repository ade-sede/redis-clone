package main

import (
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

	_, err := initReplication(*port)
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

	for {
		conn, err := l.Accept()
		if err != nil {
			errorLogger.Println(fmt.Errorf("Error accepting TCP connection: err = %w", err))
			continue
		}

		go handleConnection(conn, errorChannel)
	}

}

func handleConnection(conn net.Conn, errorChannel chan error) {
	var callerAddress string

	fmt.Println("New TCP connection from: ", conn.RemoteAddr().String())
	caller := conn.RemoteAddr()
	host, _, err := net.SplitHostPort(caller.String())
	if err != nil {
		errorChannel <- err
		return
	}
	ip := net.ParseIP(host)
	if ip == nil {
		errorChannel <- fmt.Errorf("Error parsing IP address %s", host)
		return
	}
	if ip.IsLoopback() {
		callerAddress = "localhost"
	} else {
		callerAddress = ip.String()
	}

	for {
		buf := make([]byte, 1024)
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
		query, err := parseResp(buf, &offset)
		if err != nil {
			errorChannel <- fmt.Errorf("Error parsing query. buf = %s, offset = %d, err = %w", string(buf), offset, err)
			return
		}

		response, err := execute(query, callerAddress)
		if err != nil {
			errorChannel <- fmt.Errorf("Error executing the command: err = %w", err)
			return
		}

		_, err = conn.Write(response)
		if err != nil {
			errorChannel <- fmt.Errorf("Error writing to TCP connection: err = %w", err)
			return
		}
	}
}
