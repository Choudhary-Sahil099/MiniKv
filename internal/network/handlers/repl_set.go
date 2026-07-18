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

	parts := strings.Fields(command)

	logger.Log.Info(
		"REPL_SET received",
		zap.String("node", ctx.NodeID),
		zap.String("key", parts[1]),
		zap.String("value", parts[2]),
	)

	if len(parts) != 5 {
		return "Usage: REPL_SET key value timestamp vectorclock"
	}

	incomingTime, err := time.Parse(
		time.RFC3339Nano,
		parts[3],
	)
	incomingClock := vectorclock.Deserialize(parts[4])
	if err != nil {
		return "Invalid timestamp"
	}
	currentValue, exists := ctx.Store.GetValue(parts[1])
	if exists {

		comparison := vectorclock.Compare(
			incomingClock,
			currentValue.Clock,
		)

		logger.Log.Info(
			"vector clock comparison",
			zap.String("key", parts[1]),
			zap.String("result", comparison.String()),
		)
	}
	if exists && !incomingTime.After(currentValue.CreatedAt) {
		return "IGNORED_OLDER_VERSION"
	}

	err = ctx.WAL.Write(command)
	if err != nil {
		return "WAL write failed"
	}
	value := storage.Value{
		Data:      parts[2],
		CreatedAt: incomingTime,
		Clock:     incomingClock,
	}
	ctx.Store.SetValueWithTimestamp(
		parts[1],
		value,
	)

	return "REPLICATED"

}
