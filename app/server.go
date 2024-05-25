package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func handleConnection(conn net.Conn, errorChannel chan error) {
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				errorChannel <- fmt.Errorf("EOF: Remote client closed connection")
			} else {
				errorChannel <- fmt.Errorf("Error reading from connection: %s", err.Error())
			}
			conn.Close()
			return
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			errorChannel <- fmt.Errorf("Error writing to connection: %s", err.Error())
			return
		}
	}
}

func main() {
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		errorLogger.Println("Failed to bind to port 6379")
		os.Exit(1)
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
			errorLogger.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn, errorChannel)
	}

}
