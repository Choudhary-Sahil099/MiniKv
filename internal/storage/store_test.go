package storage

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	store := NewStore()

	if store == nil {
		t.Fatal("store is nil")
	}

	if len(store.data) != 0 {
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