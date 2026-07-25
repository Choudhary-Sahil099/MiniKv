package wal

import (
	"os"
	"path/filepath"
	"testing"
	"minikv/internal/storage"
)

func createTempWAL(t *testing.T) (*WAL, string) {
	t.Helper()

	dir := t.TempDir()
	base := filepath.Join(dir, "wal")

	w, err := NewWAL(base)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}

	return w, base
}

func TestNewWAL(t *testing.T) {

	w, base := createTempWAL(t)
	defer w.Close()

	path := base + ".000001.log"

	if _, err := os.Stat(path); err != nil {
		t.Fatal("wal file not created")
	}
}

func TestWrite(t *testing.T) {

	w, base := createTempWAL(t)
	defer w.Close()

	if err := w.Write("SET a 10"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "SET a 10\n" {
		t.Fatal("unexpected wal contents")
	}
}

func TestMultipleWrites(t *testing.T) {

	w, base := createTempWAL(t)
	defer w.Close()

	w.Write("SET a 1")
	w.Write("SET b 2")
	w.Write("DEL a")

	data, _ := os.ReadFile(base + ".000001.log")

	expected := "SET a 1\nSET b 2\nDEL a\n"

	if string(data) != expected {
		t.Fatal("write order incorrect")
	}
}

func TestRotate(t *testing.T) {

	w, base := createTempWAL(t)
	defer w.Close()

	seq, err := w.Rotate()

	if err != nil {
		t.Fatal(err)
	}

	if seq != 1 {
		t.Fatal("expected rotated sequence 1")
	}

	if _, err := os.Stat(base + ".000002.log"); err != nil {
		t.Fatal("new segment not created")
	}
}

func TestPurgeOlderThan(t *testing.T) {

	w, base := createTempWAL(t)

	w.Rotate()
	w.Rotate()

	if err := w.PurgeOlderThan(2); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(base + ".000001.log"); !os.IsNotExist(err) {
		t.Fatal("segment 1 should be removed")
	}

	if _, err := os.Stat(base + ".000002.log"); !os.IsNotExist(err) {
		t.Fatal("segment 2 should be removed")
	}

	if _, err := os.Stat(base + ".000003.log"); err != nil {
		t.Fatal("latest segment should remain")
	}

	w.Close()
}

func TestRecoverSingleSet(t *testing.T) {

	w, base := createTempWAL(t)

	w.Write("SET key value")
	w.Close()

	store := storage.NewStore()

	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}

	val, ok := store.Get("key")

	if !ok {
		t.Fatal("missing recovered key")
	}

	if val != "value" {
		t.Fatal("incorrect recovered value")
	}
}

func TestRecoverDelete(t *testing.T) {

	w, base := createTempWAL(t)

	w.Write("SET x hello")
	w.Write("DEL x")

	w.Close()

	store := storage.NewStore()

	Recover(store, base)

	if _, ok := store.Get("x"); ok {
		t.Fatal("delete not replayed")
	}
}

func TestRecoverMultipleSegments(t *testing.T) {

	w, base := createTempWAL(t)

	w.Write("SET a 1")
	w.Rotate()

	w.Write("SET b 2")
	w.Rotate()

	w.Write("SET c 3")

	w.Close()

	store := storage.NewStore()

	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}

	tests := []string{"a", "b", "c"}

	for _, k := range tests {

		if _, ok := store.Get(k); !ok {
			t.Fatalf("%s missing", k)
		}
	}
}


func TestRecoverIgnoresInvalidLines(t *testing.T) {

	dir := t.TempDir()

	base := filepath.Join(dir, "wal")

	os.WriteFile(
		base+".000001.log",
		[]byte("INVALID\nSET a 1\nBAD DATA\n"),
		0644,
	)

	store := storage.NewStore()

	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}

	if v, ok := store.Get("a"); !ok || v != "1" {
		t.Fatal("valid command not replayed")
	}
}

func TestRecoverMissingDirectory(t *testing.T) {

	store := storage.NewStore()

	err := Recover(
		store,
		filepath.Join(t.TempDir(), "missing", "wal"),
	)

	if err != nil {
		t.Fatal(err)
	}
}