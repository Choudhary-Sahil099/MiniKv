package storage

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"minikv/internal/vectorclock"
)

func TestNewStore(t *testing.T) {
	store := NewStore()

	if store == nil {
		t.Fatal("store is nil")
	}

	if store.Len() != 0 {
		t.Fatal("new store should be empty")
	}
}
func TestSetAndGet(t *testing.T) {
	store := NewStore()

	store.Set("name", "Sahil")

	value, ok := store.Get("name")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Sahil" {
		t.Fatalf("expected Sahil got %s", value)
	}
}
func TestGetMissingKey(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("missing")

	if ok {
		t.Fatal("expected missing key")
	}
}
func TestOverwriteValue(t *testing.T) {
	store := NewStore()

	store.Set("x", "1")
	store.Set("x", "2")

	value, _ := store.Get("x")

	if value != "2" {
		t.Fatalf("expected 2 got %s", value)
	}
}
func TestDelete(t *testing.T) {
	store := NewStore()

	store.Set("a", "10")

	store.Delete("a")

	_, ok := store.Get("a")

	if ok {
		t.Fatal("key should have been deleted")
	}
}
func TestDeleteMissingKey(t *testing.T) {
	store := NewStore()

	store.Delete("unknown")

	if len(store.Export()) != 0 {
		t.Fatal("store should still be empty")
	}
}
func TestSetValuePreservesMetadata(t *testing.T) {

	store := NewStore()

	ts := time.Now()

	value := Value{
		Data:      "hello",
		CreatedAt: ts,
	}

	store.SetValue("k", value)

	got, ok := store.GetValue("k")

	if !ok {
		t.Fatal("missing value")
	}

	if got.Data != "hello" {
		t.Fatal("wrong data")
	}

	if !got.CreatedAt.Equal(ts) {
		t.Fatal("timestamp changed")
	}
}
func TestExportReturnsCopy(t *testing.T) {

	store := NewStore()

	store.Set("x", "1")

	exported := store.Export()

	exported["x"] = Value{
		Data: "999",
	}

	value, _ := store.Get("x")

	if value != "1" {
		t.Fatal("export should not modify store")
	}
}
func TestImport(t *testing.T) {

	store := NewStore()

	data := map[string]Value{
		"a": {Data: "1"},
		"b": {Data: "2"},
	}

	store.Import(data)

	value, ok := store.Get("a")

	if !ok {
		t.Fatal("expected imported key")
	}

	if value != "1" {
		t.Fatalf("expected 1 got %s", value)
	}
}
func TestImportReplacesStore(t *testing.T) {

	store := NewStore()

	store.Set("old", "value")

	newData := map[string]Value{
		"new": {Data: "fresh"},
	}

	store.Import(newData)

	_, ok := store.Get("old")

	if ok {
		t.Fatal("old data should be gone")
	}

	value, ok := store.Get("new")

	if !ok {
		t.Fatal("expected new key")
	}

	if value != "fresh" {
		t.Fatal("new data missing")
	}
}
func TestConcurrentSet(t *testing.T) {

	store := NewStore()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			store.Set(strconv.Itoa(i), "value")
		}(i)
	}

	wg.Wait()

	if len(store.Export()) != 100 {
		t.Fatal("missing values")
	}
}
func TestConcurrentGet(t *testing.T) {
	store := NewStore()
	store.Set("key", "value")

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			value, ok := store.Get("key")

			if !ok || value != "value" {
				t.Error("unexpected read")
			}
		}()
	}

	wg.Wait()
}
func TestConcurrentSetAndGet(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {

		wg.Add(2)

		go func(i int) {
			defer wg.Done()
			store.Set(strconv.Itoa(i), "value")
		}(i)

		go func(i int) {
			defer wg.Done()
			store.Get(strconv.Itoa(i))
		}(i)
	}

	wg.Wait()
}

func TestTombstoneHidesValueButRetainsVersion(t *testing.T) {
	store := NewStore()
	clock := make(vectorclock.VectorClock)
	clock.Increment("NodeA")
	store.SetValue("key", Value{Data: "old", Clock: clock})

	tombstoneClock := clock.Copy()
	tombstoneClock.Increment("NodeA")
	store.SetTombstone("key", Value{Clock: tombstoneClock, Deleted: true})

	if _, ok := store.Get("key"); ok {
		t.Fatal("tombstoned key must not be visible through Get")
	}
	got, ok := store.GetValue("key")
	if !ok || !got.Deleted || got.Clock["NodeA"] != 2 {
		t.Fatalf("tombstone metadata was not retained: %+v, exists=%v", got, ok)
	}
	if _, ok := store.Export()["key"]; !ok {
		t.Fatal("tombstone must be included in exports")
	}
}

func TestLockKeySerializesSameKeyUpdates(t *testing.T) {
	store := NewStore()
	const workers = 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := store.LockKey("shared")
			defer unlock()

			current, exists := store.GetValue("shared")
			clock := make(vectorclock.VectorClock)
			if exists {
				clock = current.Clock.Copy()
			}
			clock.Increment("NodeA")
			store.SetValue("shared", Value{Data: "value", Clock: clock})
		}()
	}
	wg.Wait()

	value, exists := store.GetValue("shared")
	if !exists {
		t.Fatal("expected shared key to exist")
	}
	if value.Clock["NodeA"] != workers {
		t.Fatalf("expected vector clock counter %d, got %d", workers, value.Clock["NodeA"])
	}
}

func TestMergeIfNewerAppliesLaterEqualClockVersion(t *testing.T) {
	store := NewStore()
	clock := make(vectorclock.VectorClock)
	clock["NodeA"] = 2
	olderTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newerTime := olderTime.Add(time.Second)
	store.SetValue("key", Value{Data: "old", CreatedAt: olderTime, Clock: clock.Copy()})

	if !store.MergeIfNewer("key", Value{Data: "new", CreatedAt: newerTime, Clock: clock.Copy()}) {
		t.Fatal("later equal-clock version should be applied")
	}
	got, ok := store.GetValue("key")
	if !ok || got.Data != "new" || !got.CreatedAt.Equal(newerTime) {
		t.Fatalf("later equal-clock version not stored: %+v, exists=%v", got, ok)
	}
}

func TestMergeIfNewerRejectsEarlierEqualClockVersion(t *testing.T) {
	store := NewStore()
	clock := make(vectorclock.VectorClock)
	clock["NodeA"] = 2
	newerTime := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	olderTime := newerTime.Add(-time.Second)
	store.SetValue("key", Value{Data: "new", CreatedAt: newerTime, Clock: clock.Copy()})

	if store.MergeIfNewer("key", Value{Data: "old", CreatedAt: olderTime, Clock: clock.Copy()}) {
		t.Fatal("earlier equal-clock version must not replace current value")
	}
	got, _ := store.Get("key")
	if got != "new" {
		t.Fatalf("current value changed unexpectedly: %q", got)
	}
}

func TestGetValueReturnsIndependentVectorClock(t *testing.T) {
	store := NewStore()

	clock := make(vectorclock.VectorClock)
	clock.Increment("NodeA")

	store.SetValue("key", Value{
		Data:  "value",
		Clock: clock,
	})

	got, exists := store.GetValue("key")
	if !exists {
		t.Fatal("expected key to exist")
	}

	got.Clock.Increment("NodeA")

	stored, _ := store.GetValue("key")
	if stored.Clock["NodeA"] != 1 {
		t.Fatalf("external mutation changed stored clock: got %d, want 1",
			stored.Clock["NodeA"])
	}
}

func TestSetValueCopiesIncomingVectorClock(t *testing.T) {
	store := NewStore()

	clock := make(vectorclock.VectorClock)
	clock.Increment("NodeA")

	store.SetValue("key", Value{
		Data:  "value",
		Clock: clock,
	})

	clock.Increment("NodeA")

	stored, _ := store.GetValue("key")
	if stored.Clock["NodeA"] != 1 {
		t.Fatalf("input clock mutation changed stored clock: got %d, want 1",
			stored.Clock["NodeA"])
	}
}

func TestExportDeepCopiesVectorClocks(t *testing.T) {
	store := NewStore()

	clock := make(vectorclock.VectorClock)
	clock.Increment("NodeA")

	store.SetValue("key", Value{
		Data:  "value",
		Clock: clock,
	})

	exported := store.Export()
	value := exported["key"]
	value.Clock.Increment("NodeA")
	exported["key"] = value

	stored, _ := store.GetValue("key")
	if stored.Clock["NodeA"] != 1 {
		t.Fatalf("export mutation changed stored clock: got %d, want 1",
			stored.Clock["NodeA"])
	}
}

func TestImportDeepCopiesVectorClocks(t *testing.T) {
	store := NewStore()

	clock := make(vectorclock.VectorClock)
	clock.Increment("NodeA")

	input := map[string]Value{
		"key": {
			Data:  "value",
			Clock: clock,
		},
	}

	store.Import(input)

	clock.Increment("NodeA")

	stored, _ := store.GetValue("key")
	if stored.Clock["NodeA"] != 1 {
		t.Fatalf("input mutation changed imported clock: got %d, want 1",
			stored.Clock["NodeA"])
	}
}
