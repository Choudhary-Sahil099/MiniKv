package handlers

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"minikv/internal/logger"
	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func HandleReplSet(
	ctx *common.CommandContext,
	command string,
) string {

	key, valueData, timestamp, serializedClock, ok := parseVersionedSetCommand(command)
	if !ok {
		return "Usage: REPL_SET key value timestamp vectorclock"
	}
	unlock := ctx.Store.LockKey(key)
	defer unlock()

	logger.Log.Debug(
		"REPL_SET received",
		zap.String("node", ctx.NodeID),
		zap.String("command", "REPL_SET"),
	)

	incomingTime, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return "Invalid timestamp"
	}
	incomingClock, err := vectorclock.Parse(serializedClock)
	if err != nil {
		return "Invalid vector clock"
	}
	currentValue, exists := ctx.Store.GetValue(key)
	if exists {

		comparison := vectorclock.Compare(
			incomingClock,
			currentValue.Clock,
		)

		logger.Log.Debug(
			"vector clock comparison",
			zap.String("key", key),
			zap.String("incoming", incomingClock.Serialize()),
			zap.String("current", currentValue.Clock.Serialize()),
			zap.String("result", comparison.String()),
		)

		switch comparison {

		case vectorclock.Before:
			return "IGNORED_CAUSALLY_OLDER"

		case vectorclock.Equal:
			if !incomingTime.After(currentValue.CreatedAt) {
				return "DUPLICATE_VERSION"
			}

		case vectorclock.After:

		case vectorclock.Concurrent:

			logger.Log.Warn(
				"concurrent write detected",
				zap.String("key", key),
			)

			if !incomingTime.After(currentValue.CreatedAt) {
				return "IGNORED_CONCURRENT_VERSION"
			}
		}
	}

	err = ctx.WAL.Write(command)
	if err != nil {
		return "WAL write failed"
	}
	value := storage.Value{
		Data:      valueData,
		CreatedAt: incomingTime,
		Clock:     incomingClock,
	}
	if !ctx.Store.MergeIfNewer(key, value) {
		return "IGNORED_CAUSALLY_OLDER"
	}

	return "REPLICATED"
}

func parseVersionedSetCommand(command string) (key, value, timestamp, clock string, ok bool) {
	trimmed := strings.TrimSpace(command)
	fields := strings.Fields(trimmed)
	if len(fields) < 5 {
		return "", "", "", "", false
	}
	metadata := " " + fields[len(fields)-2] + " " + fields[len(fields)-1]
	metadataIndex := strings.LastIndex(trimmed, metadata)
	if metadataIndex < 0 {
		return "", "", "", "", false
	}
	prefix := strings.TrimSpace(trimmed[:metadataIndex])
	firstSeparator := strings.IndexFunc(prefix, func(r rune) bool { return r == ' ' || r == '\t' })
	if firstSeparator < 0 {
		return "", "", "", "", false
	}
	rest := strings.TrimLeft(prefix[firstSeparator:], " \t")
	keySeparator := strings.IndexFunc(rest, func(r rune) bool { return r == ' ' || r == '\t' })
	if keySeparator <= 0 {
		return "", "", "", "", false
	}
	key = rest[:keySeparator]
	value = strings.TrimSpace(rest[keySeparator:])
	if value == "" {
		return "", "", "", "", false
	}
	return key, value, fields[len(fields)-2], fields[len(fields)-1], true
}
