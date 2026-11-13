package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"minikv/internal/vectorclock"
)

func benchmarkValue(i int) Value {
	clock := make(vectorclock.VectorClock)
	clock["node-a"] = 10
	clock["node-b"] = 5

	return Value{
		Data:      fmt.Sprintf("value-%d", i),
		CreatedAt: time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC),
		Clock:     clock,
	}
}

func BenchmarkStoreSetParallel(b *testing.B) {
	store := NewStore()
	b.ReportAllocs()
	b.ResetTimer()

	var counter uint64
	var mu sync.Mutex

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			counter++
			i := counter
			mu.Unlock()

			store.Set(fmt.Sprintf("key-%d", i), "benchmark-value")
		}
	})
}

func BenchmarkStoreGetParallel(b *testing.B) {
	store := NewStore()
	for i := 0; i < 10000; i++ {
		store.SetValue(fmt.Sprintf("key-%d", i), benchmarkValue(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	var counter uint64
	var mu sync.Mutex

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			counter++
			i := counter % 10000
			mu.Unlock()

			store.GetValue(fmt.Sprintf("key-%d", i))
		}
	})
}

func BenchmarkStoreExport(b *testing.B) {
	store := NewStore()
	for i := 0; i < 10000; i++ {
		store.SetValue(fmt.Sprintf("key-%d", i), benchmarkValue(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Export()
	}
}
