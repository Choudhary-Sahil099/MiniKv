package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

const pingTimeout = 2 * time.Second // limit setup

func Ping(address string) error {
	conn, err := net.DialTimeout(
		"tcp",
		address,
		dialTimeout,
	)
	if err != nil {
		return fmt.Errorf("ping connection to %s failed: %w", address, err)
	}

	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(pingTimeout)); err != nil {
		return fmt.Errorf("setting ping deadline failed: %w", err)
	}

	if _, err := conn.Write([]byte("PING\n")); err != nil {
		return fmt.Errorf("sending ping to %s failed: %w", address, err)
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("receiving ping response from %s failed: %w", address, err)
	}

	if strings.TrimSpace(response) != "PONG" {
		return fmt.Errorf(
			"unexpected ping response from %s: %q",
			address,
			response,
		)
	}

	return nil
}
