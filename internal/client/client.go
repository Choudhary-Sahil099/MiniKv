package client

import (
	"io"
	"strings"
	"time"

	"go.uber.org/zap"
	"minikv/internal/logger"
)

/*
purspose of the file is that it connects the nodes between themselves
*/
func ForwardCommand(
	address string,
	command string,
) (string, error) {
	// acquire a pool connection
	pool := getPeerPool(address)

	pc, err := pool.acquire()
	if err != nil {
		return "", err
	}
	// never return a potentially unhealty connection to the pool
	discard := func() {
		pool.discard(pc)
		pool.flushIdle()
	}
	cleanCommand := strings.TrimSpace(command)
	// Log only the command type; key/value payloads may contain application data.
	commandName := "UNKNOWN"
	if fields := strings.Fields(cleanCommand); len(fields) > 0 {
		commandName = strings.ToUpper(fields[0])
	}
	logger.Log.Debug(
		"sending peer command",
		zap.String("address", address),
		zap.String("command", commandName),
	)
	if err := pc.conn.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		discard()
		return "", err
	}

	payload := []byte("FORWARDED " + cleanCommand + "\n")

	for len(payload) > 0 {
		n, writeErr := pc.conn.Write(payload)

		if writeErr != nil {
			discard()
			return "", writeErr
		}

		if n == 0 {
			discard()
			return "", io.ErrShortWrite
		}

		payload = payload[n:]
	}

	response, err := pc.reader.ReadString('\n')

	if err != nil {
		pool.discard(pc)
		return "", err
	}

	if err := pc.conn.SetDeadline(time.Time{}); err != nil {
		discard()
		return "", err
	}
	pool.release(pc)
	response = strings.TrimSpace(response)

	logger.Log.Debug(
		"peer command completed",
		zap.String("address", address),
		zap.String("command", commandName),
	)

	return response, nil
}

// Retry failed read-forwarding operations
func ForwardReadCommand(address string, command string) (string, error) {
	commandName := "UNKNOWN"
	if fields := strings.Fields(command); len(fields) > 0 {
		commandName = strings.ToUpper(fields[0])
	}
	response, err := ForwardCommand(address, command)
	if err == nil {
		return response, nil
	}

	logger.Log.Warn(
		"read forwarding failed; retrying with a fresh connection",
		zap.String("address", address),
		zap.String("command", commandName),
		zap.Error(err),
	)

	return ForwardCommand(address, command)
}
