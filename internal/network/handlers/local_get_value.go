package handlers

import (
	"encoding/json"
	"strings"
	"minikv/internal/network/common"
)

func HandleLocalGetValue(
	ctx *common.CommandContext,
	command string,

) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: LOCAL_GET_VALUE key"
	}

	value, exists := ctx.Store.GetValue(parts[1])

	if !exists {
		return "NOT_FOUND"
	}

	bytes, err := json.Marshal(value)

	if err != nil {
		return "ERROR"
	}

	return string(bytes)
}
