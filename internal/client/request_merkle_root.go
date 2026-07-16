package client

import (
	"bufio"
	"net"
	"strings"
)

func RequestMerkleRoot(
	address string,
) (string, error) {

	conn, err := net.Dial(
		"tcp",
		address,
	)

	if err != nil {
		return "", err
	}

	defer conn.Close()

	_, err = conn.Write(
		[]byte("MERKLE_ROOT\n"),
	)

	if err != nil {
		return "", err
	}

	response, err := bufio.NewReader(conn).
		ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSpace(response), nil
}