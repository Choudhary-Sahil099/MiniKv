package handlers

import (
	"strings"
	"minikv/internal/network/common"
)

func HandleReplDelete(
	ctx *common.CommandContext,
	command string,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: REPL_DEL key"
	}

	err := ctx.WAL.Write(command)
	if err != nil {
		return "WAL write failed"
	}

	ctx.Store.Delete(parts[1])

	return "DELETED"
}
