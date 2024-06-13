package main

import (
	"fmt"
	"strings"
)

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
