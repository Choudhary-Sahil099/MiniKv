package merkle

import (
	"fmt"
	"testing"
	"time"

	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func benchmarkMerkleData(size int) map[string]storage.Value {
	data := make(map[string]storage.Value, size)

	for i := 0; i < size; i++ {
		clock := make(vectorclock.VectorClock)
		clock["node-a"] = 10
		clock["node-b"] = 5
		clock["node-c"] = 3

		key := fmt.Sprintf("key-%08d", i)

		data[key] = storage.Value{
			Data:      fmt.Sprintf("benchmark-value-%08d", i),
			CreatedAt: time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC),
			Clock:     clock,
		}
	}

	return data
}

func BenchmarkHashLeaf(b *testing.B) {
	value := benchmarkMerkleData(1)["key-00000000"]

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HashLeaf("key-00000000", value)
	}
}

func BenchmarkHashInternal(b *testing.B) {
	left := make([]byte, 32)
	right := make([]byte, 32)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HashInternal(left, right)
	}
}

func BenchmarkMerkleBuild(b *testing.B) {
	for _, size := range []int{10000, 100000} {
		b.Run(fmt.Sprintf("Entries_%d", size), func(b *testing.B) {
			data := benchmarkMerkleData(size)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				Build(data)
			}
		})
	}
}
