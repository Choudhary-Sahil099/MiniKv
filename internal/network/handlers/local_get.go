package handlers

import (
	"minikv/internal/network/common"
	"strings"
)

func HandleLocalGet(
	ctx *common.CommandContext,
	command string,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: LOCAL_GET key"
	}

	value, exists := ctx.Store.Get(parts[1])

	if !exists {
		return "NOT_FOUND"
	}

	return value
}
