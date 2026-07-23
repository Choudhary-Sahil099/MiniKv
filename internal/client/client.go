package client

import (
	"go.uber.org/zap"
	"minikv/internal/logger"
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

	// Only one goroutine can use this connection at a time.
	pc.Mu.Lock()
	defer pc.Mu.Unlock()

	logger.Log.Info(
		"sending command",
		zap.String("address", address),
		zap.String("command", command),
	)

	cleanCommand := strings.TrimSpace(command)

	_, err = pc.Conn.Write(
		[]byte("FORWARDED " + cleanCommand + "\n"),
	)

	if err != nil {

		RemoveConnection(address)

		return "", err
	}

	response, err := pc.Reader.
		ReadString('\n')


	logger.Log.Info(
		"received response",
		zap.String("address", address),
		zap.String("response", strings.TrimSpace(response)),
	)
	if err != nil {

		RemoveConnection(address)

		return "", err
	}
	logger.Log.Info(
		"forward command result",
		zap.String("command", command),
		zap.String("response", strings.TrimSpace(response)),
	)
	return strings.TrimSpace(
		response,
	), nil
}
