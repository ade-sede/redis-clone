package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func handleConnection(conn net.Conn, errorChannel chan error) {
	fmt.Println("New TCP connection from: ", conn.RemoteAddr().String())
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

		response, err := execute(query)
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

func main() {
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	data = make(map[string]entry)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
