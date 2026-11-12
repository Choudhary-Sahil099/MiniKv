package network

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"

	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

const (
	maxCommandBytes        = 1 << 20  // 1 MiB
	maxResponseBytes       = 64 << 20 // 64 MiB; DUMP can be substantially larger than a command.
	connectionReadTimeout  = 2 * time.Minute
	connectionWriteTimeout = 10 * time.Second
)

var errCommandTooLarge = errors.New("network: command exceeds maximum size")

func HandleConnection(
	conn net.Conn,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	g *gossip.Gossip,
	handoff *handoff.Manager,
) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(connectionReadTimeout)); err != nil {
			logger.Log.Debug("setting connection read deadline failed", zap.Error(err))
			return
		}
		message, err := readCommandLine(reader, maxCommandBytes)
		if err != nil {
			if errors.Is(err, errCommandTooLarge) {
				_ = conn.SetWriteDeadline(time.Now().Add(connectionWriteTimeout))
				_, _ = writer.WriteString("ERR command too large\n")
				_ = writer.Flush()
				logger.Log.Warn("oversized command rejected", zap.String("node", nodeID), zap.String("remote", conn.RemoteAddr().String()))
			}
			if !errors.Is(err, io.EOF) {
				logger.Log.Debug("connection read ended", zap.String("node", nodeID), zap.Error(err))
			}
			return
		}

		commandParts := strings.Fields(message)
		if len(commandParts) == 0 {
			response := ProcessCommand(message, nodeID, store, wal, ring, false, g, handoff)
			if err := writeResponse(conn, writer, response); err != nil {
				return
			}
			continue
		}
		commandName := strings.ToUpper(commandParts[0])
		isForwarded := false
		if strings.HasPrefix(message, "FORWARDED ") {
			isForwarded = true
			message = strings.TrimPrefix(message, "FORWARDED ")
			commandParts = strings.Fields(message)
			if len(commandParts) == 0 {
				if err := writeResponse(conn, writer, "Empty command"); err != nil {
					return
				}
				continue
			}
			commandName = strings.ToUpper(commandParts[0])
		}

		logger.Log.Debug("processing TCP command", zap.String("node", nodeID), zap.String("command", commandName))
		response := ProcessCommand(message, nodeID, store, wal, ring, isForwarded, g, handoff)
		if err := writeResponse(conn, writer, response); err != nil {
			logger.Log.Debug("connection response failed", zap.String("node", nodeID), zap.Error(err))
			return
		}
	}
}

func readCommandLine(reader *bufio.Reader, maxBytes int) (string, error) {
	var command strings.Builder
	for {
		fragment, err := reader.ReadSlice('\n')
		if command.Len()+len(fragment) > maxBytes {
			return "", errCommandTooLarge
		}
		_, _ = command.Write(fragment)
		if err == nil {
			return strings.TrimSuffix(command.String(), "\n"), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}

func writeResponse(conn net.Conn, writer *bufio.Writer, response string) error {
	if len(response) > maxResponseBytes {
		response = "ERR response too large"
	}
	if err := conn.SetWriteDeadline(time.Now().Add(connectionWriteTimeout)); err != nil {
		return err
	}
	if _, err := writer.WriteString(response + "\n"); err != nil {
		return err
	}
	return writer.Flush()
}
