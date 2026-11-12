package handlers

import (
	"strings"
	"time"

	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func HandleReplDelete(ctx *common.CommandContext, command string) string {
	parts := strings.Fields(command)
	if len(parts) == 2 || len(parts) == 4 {
		unlock := ctx.Store.LockKey(parts[1])
		defer unlock()
	}
	if len(parts) == 2 {
		// Compatibility with pre-tombstone WAL/replication records.
		if err := ctx.WAL.Write(command); err != nil {
			return "WAL write failed"
		}
		ctx.Store.Delete(parts[1])
		return "DELETED"
	}
	if len(parts) != 4 {
		return "Usage: REPL_DEL key timestamp vectorclock"
	}

	incomingTime, err := time.Parse(time.RFC3339Nano, parts[2])
	if err != nil {
		return "Invalid timestamp"
	}
	incomingClock, err := vectorclock.Parse(parts[3])
	if err != nil {
		return "Invalid vector clock"
	}

	currentValue, exists := ctx.Store.GetValue(parts[1])
	if exists {
		relation := vectorclock.Compare(incomingClock, currentValue.Clock)
		switch relation {
		case vectorclock.Before:
			return "IGNORED_CAUSALLY_OLDER"
		case vectorclock.Equal:
			if !incomingTime.After(currentValue.CreatedAt) {
				if currentValue.Deleted {
					return "DUPLICATE_VERSION"
				}
				return "IGNORED_CONFLICTING_EQUAL_VERSION"
			}
		case vectorclock.Concurrent:
			if !incomingTime.After(currentValue.CreatedAt) {
				return "IGNORED_CONCURRENT_VERSION"
			}
		}
	}

	if err := ctx.WAL.Write(command); err != nil {
		return "WAL write failed"
	}
	if !ctx.Store.MergeIfNewer(parts[1], storage.Value{
		CreatedAt: incomingTime,
		Clock:     incomingClock,
		Deleted:   true,
	}) {
		return "IGNORED_CAUSALLY_OLDER"
	}
	return "DELETED"
}
