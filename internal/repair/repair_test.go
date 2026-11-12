package repair

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/wal"
)

func TestApplyLocalRepairPersistsVersionBeforeApplying(t *testing.T) {
	base := filepath.Join(t.TempDir(), "node")
	log, err := wal.NewWAL(base)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	store := storage.NewStore()
	clock := make(vectorclock.VectorClock)
	clock["NodeB"] = 3
	candidate := storage.Value{Data: "repaired value", CreatedAt: time.Now().UTC(), Clock: clock}

	if !applyLocalRepair(store, log, "key", candidate) {
		t.Fatal("expected repair to be applied")
	}
	got, ok := store.GetValue("key")
	if !ok || got.Data != candidate.Data || got.Clock.Serialize() != candidate.Clock.Serialize() {
		t.Fatalf("unexpected repaired value: %+v, exists=%v", got, ok)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("repair was not persisted to WAL")
	}
}

func TestApplyLocalRepairDoesNotReplaceNewerVersion(t *testing.T) {
	base := filepath.Join(t.TempDir(), "node")
	log, err := wal.NewWAL(base)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	store := storage.NewStore()
	currentClock := make(vectorclock.VectorClock)
	currentClock["NodeA"] = 5
	store.SetValue("key", storage.Value{Data: "current", CreatedAt: time.Now(), Clock: currentClock})

	olderClock := make(vectorclock.VectorClock)
	olderClock["NodeA"] = 4
	if applyLocalRepair(store, log, "key", storage.Value{Data: "stale", CreatedAt: time.Now().Add(time.Hour), Clock: olderClock}) {
		t.Fatal("older causal version should not be applied")
	}
	got, _ := store.Get("key")
	if got != "current" {
		t.Fatalf("stale repair overwrote current value: %q", got)
	}
}

func TestApplyLocalRepairPersistsTombstone(t *testing.T) {
	base := filepath.Join(t.TempDir(), "node")
	log, err := wal.NewWAL(base)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	store := storage.NewStore()
	clock := make(vectorclock.VectorClock)
	clock["NodeA"] = 2
	candidate := storage.Value{CreatedAt: time.Now().UTC(), Clock: clock, Deleted: true}
	if !applyLocalRepair(store, log, "key", candidate) {
		t.Fatal("expected tombstone repair to be applied")
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("tombstoned key must not be visible")
	}
	value, ok := store.GetValue("key")
	if !ok || !value.Deleted {
		t.Fatalf("tombstone metadata missing: %+v", value)
	}
}
