package main

import (
	"io"
	"log"
	"net"
	"os"
)

func main() {
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		errorLogger.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		errorLogger.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				errorLogger.Println("EOF: Remote client closed connection")
			} else {
				errorLogger.Println("Error reading from connection: ", err.Error())
			}
			os.Exit(1)
		}

		conn.Write([]byte("+PONG\r\n"))
	}
}
