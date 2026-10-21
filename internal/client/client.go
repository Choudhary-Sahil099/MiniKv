package client

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"minikv/internal/logger"
)

func ForwardCommand(
	address string,
	command string,
) (string, error) {

	pool := getPeerPool(address)

	pc, err := pool.acquire()
	if err != nil {
		return "", err
	}

	logger.Log.Info(
		"sending command",
		zap.String("address", address),
		zap.String("command", command),
	)

	pc.conn.SetDeadline(time.Now().Add(ioTimeout))

	cleanCommand := strings.TrimSpace(command)

	_, err = pc.conn.Write(
		[]byte("FORWARDED " + cleanCommand + "\n"),
	)

	if err != nil {
		pool.discard(pc)
		pool.flushIdle()
		return "", err
	}

	response, err := pc.reader.ReadString('\n')

	if err != nil {
		pool.discard(pc)
		pool.flushIdle()
		return "", err
	}

	pc.conn.SetDeadline(time.Time{})
	pool.release(pc)

	response = strings.TrimSpace(response)

	logger.Log.Info(
		"forward command result",
		zap.String("command", command),
		zap.String("response", response),
	)

	return response, nil
}
