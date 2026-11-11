package client

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func RequestDump(address string) ([]byte, error) {
	conn, err := net.DialTimeout(
		"tcp",
		address,
		dialTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"connecting to %s for dump failed: %w",
			address,
			err,
		)
	}

	defer conn.Close()

	if err := conn.SetDeadline(
		time.Now().Add(ioTimeout),
	); err != nil {
		return nil, fmt.Errorf(
			"setting dump deadline failed: %w",
			err,
		)
	}

	_, err = conn.Write([]byte("DUMP\n"))
	if err != nil {
		return nil, fmt.Errorf(
			"sending dump request to %s failed: %w",
			address,
			err,
		)
	}

	response, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf(
			"reading dump response from %s failed: %w",
			address,
			err,
		)
	}

	return response, nil
}
