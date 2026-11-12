package wal

import (
	"fmt"
	"minikv/internal/storage"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestConcurrentWritesPersistAllRecords(t *testing.T) {
	w, base := createTempWAL(t)

	const workers = 10
	const writesPerWorker = 20
	const totalWrites = workers * writesPerWorker

	var wg sync.WaitGroup
	errCh := make(chan error, totalWrites)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for i := 0; i < writesPerWorker; i++ {
				entry := fmt.Sprintf(
					"SET key-%d-%d value-%d-%d",
					id, i, id, i,
				)

				if err := w.Write(entry); err != nil {
					errCh <- err
				}
			}
		}(workerID)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent WAL write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	data, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}

	lines := strings.Split(
		strings.TrimSpace(string(data)),
		"\n",
	)

	if len(lines) != totalWrites {
		t.Fatalf(
			"expected %d records, got %d",
			totalWrites,
			len(lines),
		)
	}

	seen := make(map[string]bool, totalWrites)

	for _, line := range lines {
		if seen[line] {
			t.Errorf("duplicate WAL record: %s", line)
		}
		seen[line] = true
	}
}

func TestWriteAfterCloseReturnsError(t *testing.T) {
	w, _ := createTempWAL(t)

	if err := w.Write("SET a 1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Write("SET b 2"); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteIsDurableWhenItReturns(t *testing.T) {
	w, base := createTempWAL(t)
	defer w.Close()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entry := fmt.Sprintf("SET k%d v%d\n", i, i)
			if err := w.Write(entry); err != nil {
				t.Error(err)
				return
			}
			data, err := os.ReadFile(base + ".000001.log")
			if err != nil {
				t.Error(err)
				return
			}
			if !strings.Contains(string(data), fmt.Sprintf("SET k%d v%d\n", i, i)) {
				t.Errorf("record %d not on disk after Write returned", i)
			}
		}(i)
	}
	wg.Wait()
}

func TestRecoverSetWithSpaces(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	if err := os.WriteFile(base+".000001.log", []byte("SET description hello distributed world\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get("description")
	if !ok || got != "hello distributed world" {
		t.Fatalf("recovered value = %q, exists = %v", got, ok)
	}
}

func TestRecoverReplicaSetWithSpacesAndMetadata(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	createdAt := time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC)
	line := fmt.Sprintf("REPL_SET description hello distributed world %s NodeB=7\n", createdAt.Format(time.RFC3339Nano))
	if err := os.WriteFile(base+".000001.log", []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	got, ok := store.GetValue("description")
	if !ok {
		t.Fatal("replicated value was not recovered")
	}
	if got.Data != "hello distributed world" {
		t.Fatalf("recovered data = %q", got.Data)
	}
	if !got.CreatedAt.Equal(createdAt) {
		t.Fatalf("recovered timestamp = %s, want %s", got.CreatedAt, createdAt)
	}
	if got.Clock["NodeB"] != 7 {
		t.Fatalf("recovered vector clock = %v", got.Clock)
	}
}

func TestRecoverReplicaDelete(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	if err := os.WriteFile(base+".000001.log", []byte("SET key value\nREPL_DEL key\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("replicated delete was not recovered")
	}
}

func TestRecoverSetV2RestoresMetadataAndSpaces(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	createdAt := time.Date(2026, 11, 12, 11, 45, 0, 0, time.UTC)
	line := fmt.Sprintf("SET_V2 description hello from MiniKV %s NodeA=4\n", createdAt.Format(time.RFC3339Nano))
	if err := os.WriteFile(base+".000001.log", []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	got, ok := store.GetValue("description")
	if !ok {
		t.Fatal("SET_V2 value was not recovered")
	}
	if got.Data != "hello from MiniKV" || !got.CreatedAt.Equal(createdAt) || got.Clock["NodeA"] != 4 {
		t.Fatalf("unexpected recovered value: %+v", got)
	}
}

func TestRecoverVersionedDeleteTombstone(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	createdAt := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	contents := fmt.Sprintf("SET_V2 key value %s NodeA=1\nDEL_V2 key %s NodeA=2\n", createdAt.Format(time.RFC3339Nano), createdAt.Add(time.Second).Format(time.RFC3339Nano))
	if err := os.WriteFile(base+".000001.log", []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("deleted key should not be visible after recovery")
	}
	value, ok := store.GetValue("key")
	if !ok || !value.Deleted || value.Clock["NodeA"] != 2 {
		t.Fatalf("expected recovered tombstone with NodeA=2, got %+v, exists=%v", value, ok)
	}
}

func TestReopenWALAppendsToLatestSegment(t *testing.T) {
	w, base := createTempWAL(t)
	if err := w.Write("SET first one"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	w, err := NewWAL(base)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	if err := w.Write("SET second two"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(base + ".000002.log"); !os.IsNotExist(err) {
		t.Fatalf("restart unexpectedly created segment 2: err=%v", err)
	}
	data, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "SET first one\nSET second two\n" {
		t.Fatalf("unexpected reopened WAL contents: %q", data)
	}
}

func TestRecoverTruncatesIncompleteFinalRecord(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal")
	path := base + ".000001.log"
	original := "SET complete value\nSET partial value"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if value, ok := store.Get("complete"); !ok || value != "value" {
		t.Fatalf("complete record was not recovered: value=%q exists=%v", value, ok)
	}
	if _, ok := store.Get("partial"); ok {
		t.Fatal("incomplete final record must not be replayed")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "SET complete value\n" {
		t.Fatalf("incomplete tail was not truncated: %q", data)
	}
}

func TestAutomaticRotationBySegmentSize(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal")
	w, err := NewWALWithSegmentSize(base, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []string{"SET a 1", "SET b 2", "SET c 3"} {
		if err := w.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	first, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(base + ".000002.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 16 || string(first) != "SET a 1\nSET b 2\n" {
		t.Fatalf("unexpected first segment (%d bytes): %q", len(first), first)
	}
	if string(second) != "SET c 3\n" {
		t.Fatalf("unexpected second segment: %q", second)
	}
}

func TestWALRejectsRecordLargerThanSegmentLimit(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal")
	w, err := NewWALWithSegmentSize(base, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Write("SET key value"); err != ErrRecordTooLarge {
		t.Fatalf("Write() error = %v, want ErrRecordTooLarge", err)
	}
}

func TestPurgeRefusesActiveSegment(t *testing.T) {
	w, _ := createTempWAL(t)
	defer w.Close()
	if err := w.PurgeOlderThan(1); err == nil {
		t.Fatal("expected purge of active segment to be rejected")
	}
}

func TestRecoverVersionedReplicaValueAndTombstone(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal")
	w, err := NewWAL(base)
	if err != nil {
		t.Fatal(err)
	}
	clock := "NodeA=2,NodeB=1"
	timestamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339Nano)
	if err := w.Write("REPL_SET key value with spaces " + timestamp + " " + clock); err != nil {
		t.Fatal(err)
	}
	if err := w.Write("REPL_DEL deleted " + timestamp + " " + clock); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	store := storage.NewStore()
	if err := Recover(store, base); err != nil {
		t.Fatal(err)
	}
	value, ok := store.GetValue("key")
	if !ok || value.Data != "value with spaces" || value.Clock.Serialize() != clock {
		t.Fatalf("replica value metadata not recovered: %+v, exists=%v", value, ok)
	}
	tombstone, ok := store.GetValue("deleted")
	if !ok || !tombstone.Deleted || tombstone.Clock.Serialize() != clock {
		t.Fatalf("replica tombstone not recovered: %+v, exists=%v", tombstone, ok)
	}
}

func TestRecoverRejectsMalformedKnownRecord(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal")
	if err := os.WriteFile(base+".000001.log", []byte("SET key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Recover(storage.NewStore(), base); err == nil {
		t.Fatal("expected malformed SET record to stop recovery")
	}
}

func TestRecoverRejectsIncompleteTailInNonFinalSegment(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal")
	if err := os.WriteFile(base+".000001.log", []byte("SET key value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".000002.log", []byte("SET other value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Recover(storage.NewStore(), base); err == nil {
		t.Fatal("expected incomplete record in a non-final segment to fail recovery")
	}
	data, err := os.ReadFile(base + ".000001.log")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "SET key value" {
		t.Fatalf("recovery modified a non-final corrupt segment: %q", data)
	}
}
