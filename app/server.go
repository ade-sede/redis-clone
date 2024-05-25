package main

import (
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

	conn.Write([]byte("+PONG\r\n"))
}
