package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"minikv/internal/storage"
)

func TestSave(t *testing.T) {
	store := storage.NewStore()
	store.Set("apple", "red")
	store.Set("banana", "yellow")

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	if err := Save(store, path); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}
}

func TestSaveEmptyStore(t *testing.T) {
	store := storage.NewStore()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	if err := Save(store, path); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}
}

func TestSaveInvalidPath(t *testing.T) {
	store := storage.NewStore()

	err := Save(store, "/invalid/path/snapshot.json")
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestLoad(t *testing.T) {
	store := storage.NewStore()
	store.Set("apple", "red")
	store.Set("banana", "yellow")

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	if err := Save(store, path); err != nil {
		t.Fatal(err)
	}

	newStore := storage.NewStore()

	if err := Load(newStore, path); err != nil {
		t.Fatal(err)
	}

	v, ok := newStore.Get("apple")
	if !ok || v != "red" {
		t.Fatalf("expected apple=red")
	}

	v, ok = newStore.Get("banana")
	if !ok || v != "yellow" {
		t.Fatalf("expected banana=yellow")
	}
}

func TestLoadMissingFile(t *testing.T) {
	store := storage.NewStore()

	err := Load(store, "does_not_exist.json")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	err := os.WriteFile(path, []byte("{invalid json"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()

	err = Load(store, path)
	if err == nil {
		t.Fatal("expected JSON parsing error")
	}
}

func TestImportSnapshotBytes(t *testing.T) {
	store := storage.NewStore()

	dump := map[string]storage.Value{
		"apple": {
			Data:      "red",
			CreatedAt: time.Now(),
		},
	}

	data, err := json.Marshal(dump)
	if err != nil {
		t.Fatal(err)
	}

	err = ImportSnapshotBytes(store, data)
	if err != nil {
		t.Fatal(err)
	}

	v, ok := store.Get("apple")
	if !ok || v != "red" {
		t.Fatalf("expected apple=red, got %q", v)
	}
}

func TestImportSnapshotBytesInvalidJSON(t *testing.T) {
	store := storage.NewStore()

	err := ImportSnapshotBytes(
		store,
		[]byte("{bad json"),
	)

	if err == nil {
		t.Fatal("expected JSON error")
	}
}