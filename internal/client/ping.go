package client

import (
	"bufio"
	"net"
)

func Ping(address string) error {

	conn, err := net.Dial(
		"tcp",
		address,
	)

	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.Write(
		[]byte("PING\n"),
	)

	if err != nil {
		return err
	}

	_, err = bufio.NewReader(conn).
		ReadString('\n')

	return err
}