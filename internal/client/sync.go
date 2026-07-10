package client

import (
	"bufio"
	"net"
)

func RequestDump(
	address string,
) ([]byte, error) {

	conn, err := net.Dial(
		"tcp",
		address,
	)

	if err != nil {
		return nil, err
	}

	defer conn.Close()

	_, err = conn.Write(
		[]byte("DUMP\n"),
	)

	if err != nil {
		return nil, err
	}

	response, err := bufio.NewReader(conn).
		ReadBytes('\n')

	if err != nil {
		return nil, err
	}

	return response, nil
}
