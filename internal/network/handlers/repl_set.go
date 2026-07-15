package handlers

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"minikv/internal/logger"
	"minikv/internal/network/common"
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

	if len(parts) != 4 {
		return "Usage: REPL_SET key value timestamp"
	}

	incomingTime, err := time.Parse(
		time.RFC3339Nano,
		parts[3],
	)

	if err != nil {
		return "Invalid timestamp"
	}
	currentValue, exists := ctx.Store.GetValue(parts[1])
	if exists && !incomingTime.After(currentValue.CreatedAt) {
		return "IGNORED_OLDER_VERSION"
	}

	err = ctx.WAL.Write(command)
	if err != nil {
		return "WAL write failed"
	}
	ctx.Store.SetWithTimestamp(
		parts[1],
		parts[2],
		incomingTime,
	)

	return "REPLICATED"

}
