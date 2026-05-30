package client

import (
	"strings"
)

func ForwardCommand(
	address string,
	command string,
) (string, error) {

	pc, err := GetConnection(address)

	if err != nil {
		return "", err
	}

	// Only one goroutine can use this connection
	// at a time.
	pc.Mu.Lock()
	defer pc.Mu.Unlock()

	_, err = pc.Conn.Write(
		[]byte("FORWARDED " + command + "\n"),
	)

	if err != nil {

		RemoveConnection(address)

		return "", err
	}

	response, err := pc.Reader.
		ReadString('\n')

	if err != nil {

		RemoveConnection(address)

		return "", err
	}

	return strings.TrimSpace(
		response,
	), nil
}