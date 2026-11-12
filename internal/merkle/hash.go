package merkle

import (
	"crypto/sha256"
	"encoding/json"

	"minikv/internal/storage"
)

func HashLeaf(key string, value storage.Value) []byte {
	// Include version metadata and tombstone state. Hashing only the payload
	// could hide causal-version divergence from anti-entropy.
	canonical := struct {
		Key       string `json:"key"`
		Data      string `json:"data"`
		CreatedAt string `json:"created_at"`
		Clock     string `json:"clock"`
		Deleted   bool   `json:"deleted"`
	}{
		Key:       key,
		Data:      value.Data,
		CreatedAt: value.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		Clock:     value.Clock.Serialize(),
		Deleted:   value.Deleted,
	}
	encoded, _ := json.Marshal(canonical)
	hash := sha256.Sum256(encoded)
	return hash[:]
}

func HashInternal(left []byte, right []byte) []byte {
	combined := make([]byte, 0, len(left)+len(right))
	combined = append(combined, left...)
	combined = append(combined, right...)
	hash := sha256.Sum256(combined)
	return hash[:]
}
