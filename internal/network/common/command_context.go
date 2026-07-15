package common

import (
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

type CommandContext struct {
	NodeID         string
	Store          *storage.Store
	WAL            *wal.WAL
	Ring           *hashring.HashRing
	Gossip         *gossip.Gossip
	HandoffManager *handoff.Manager
	IsForwarded    bool
}
