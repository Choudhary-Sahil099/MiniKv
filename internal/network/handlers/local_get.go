package handlers

import (
	"fmt"
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

	value, exists := ctx.Store.GetValue(parts[1])

	if !exists {
		return "LOCAL_NOT_FOUND"
	}

	return fmt.Sprintf(
		"Value=%s Timestamp=%s Clock=%s",
		value.Data,
		value.CreatedAt.Format("15:04:05"),
		value.Clock.Serialize(),
	)
}