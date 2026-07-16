package handlers

import (
	"minikv/internal/merkle"
	"minikv/internal/network/common"
)

func HandleMerkleRoot(
	ctx *common.CommandContext,
) string {

	tree := merkle.Build(
		ctx.Store.Export(),
	)

	return tree.RootHash()
}