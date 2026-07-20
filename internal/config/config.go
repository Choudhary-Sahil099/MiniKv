package config

import "time"

const (
	VirtualNodes      = 3
	ReplicationFactor = 3

	WriteQuorum = 2
	ReadQuorum  = 2

	SnapshotInterval    = 1 * time.Second
	AntiEntropyInterval = 30 * time.Second
	HeartbeatInterval   = 5 * time.Second
)
