package handlers

import (
	"encoding/json"
	"minikv/internal/network/common"
	"strings"
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
