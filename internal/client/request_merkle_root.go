package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

func RequestMerkleRoot(address string) (string, error) {
	conn, err := net.DialTimeout(
		"tcp",
		address,
		dialTimeout,
	)
	if err != nil {
		return "", fmt.Errorf(
			"connecting to %s for Merkle root failed: %w",
			address,
			err,
		)
	}

	defer conn.Close()

	if err := conn.SetDeadline(
		time.Now().Add(ioTimeout),
	); err != nil {
		return "", fmt.Errorf(
			"setting Merkle root deadline failed: %w",
			err,
		)
	}

	_, err = conn.Write([]byte("MERKLE_ROOT\n"))
	if err != nil {
		return "", fmt.Errorf(
			"sending Merkle root request to %s failed: %w",
			address,
			err,
		)
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf(
			"reading Merkle root response from %s failed: %w",
			address,
			err,
		)
	}

	return strings.TrimSpace(response), nil
}
