package handlers

import (
	"path/filepath"
	"testing"
	"time"

	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/wal"
)

func newReplicationTestContext(t *testing.T) (*common.CommandContext, func()) {
	t.Helper()
	store := storage.NewStore()
	log, err := wal.NewWAL(filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	return &common.CommandContext{NodeID: "NodeB", Store: store, WAL: log}, func() {
		if err := log.Close(); err != nil {
			t.Errorf("WAL Close() error = %v", err)
		}
	}
}

func TestHandleReplSetAcceptsLaterTimestampForEqualClock(t *testing.T) {
	ctx, cleanup := newReplicationTestContext(t)
	defer cleanup()

	clock := make(vectorclock.VectorClock)
	clock["NodeA"] = 1
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx.Store.SetValue("key", storage.Value{Data: "old", CreatedAt: older, Clock: clock.Copy()})
	newer := older.Add(time.Second)
	command := "REPL_SET key new-value " + newer.Format(time.RFC3339Nano) + " " + clock.Serialize()

	if response := HandleReplSet(ctx, command); response != "REPLICATED" {
		t.Fatalf("HandleReplSet() = %q, want REPLICATED", response)
	}
	got, _ := ctx.Store.GetValue("key")
	if got.Data != "new-value" || !got.CreatedAt.Equal(newer) {
		t.Fatalf("later equal-clock value not applied: %+v", got)
	}
}

func TestHandleReplDeleteAcceptsLaterTimestampForEqualClock(t *testing.T) {
	ctx, cleanup := newReplicationTestContext(t)
	defer cleanup()

	clock := make(vectorclock.VectorClock)
	clock["NodeA"] = 1
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx.Store.SetValue("key", storage.Value{Data: "old", CreatedAt: older, Clock: clock.Copy()})
	newer := older.Add(time.Second)
	command := "REPL_DEL key " + newer.Format(time.RFC3339Nano) + " " + clock.Serialize()

	if response := HandleReplDelete(ctx, command); response != "DELETED" {
		t.Fatalf("HandleReplDelete() = %q, want DELETED", response)
	}
	got, exists := ctx.Store.GetValue("key")
	if !exists || !got.Deleted || !got.CreatedAt.Equal(newer) {
		t.Fatalf("later equal-clock tombstone not applied: %+v, exists=%v", got, exists)
	}
}
