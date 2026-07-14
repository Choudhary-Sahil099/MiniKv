package handlers

import "minikv/internal/network/common"

func HandlePing(
	ctx *common.CommandContext,
) string {

	return "PONG"
}