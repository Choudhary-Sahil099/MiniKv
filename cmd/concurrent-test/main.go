package main

import (
	"fmt"
	"net"
)

func main() {

	conn, err := net.Dial("tcp", "localhost:5001")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	command := "REPL_SET apple green 2026-07-19T00:15:00+05:30 NodeB=1\n"

	_, err = conn.Write([]byte(command))
	if err != nil {
		panic(err)
	}

	buffer := make([]byte, 1024)

	n, err := conn.Read(buffer)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(buffer[:n]))
}