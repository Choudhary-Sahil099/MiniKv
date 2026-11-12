package snapshot

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func benchmarkSnapshotData(size int) map[string]storage.Value {
	data := make(map[string]storage.Value, size)

	for i := 0; i < size; i++ {
		clock := make(vectorclock.VectorClock)
		clock["node-a"] = 10
		clock["node-b"] = 5

		key := fmt.Sprintf("key-%08d", i)

		data[key] = storage.Value{
			Data:      fmt.Sprintf("benchmark-value-%08d", i),
			CreatedAt: time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC),
			Clock:     clock,
		}
	}

	return data
}

func BenchmarkSnapshotSerialization(b *testing.B) {
	data := benchmarkSnapshotData(10000)

	b.Run("CompactJSON", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("IndentedJSON", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := json.MarshalIndent(data, "", "  "); err != nil {
				b.Fatal(err)
			}
		}
	})
}
