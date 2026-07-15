package handlers

import (
	"encoding/json"
	"go.uber.org/zap"
	"minikv/internal/logger"
	"minikv/internal/network/common"
)

func HandleDump(
	ctx *common.CommandContext,
) string {

	data := ctx.Store.Export()

	bytes, err := json.Marshal(data)

	if err != nil {

		logger.Log.Error(
			"dump marshal failed",
			zap.Error(err),
		)

		return "DUMP_FAILED"
	}

	return string(bytes) + "\n"
}
