package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"minikv/internal/storage"
)

func writeField(h hash.Hash, value []byte) {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(value)))

	_, _ = h.Write(length[:n])
	_, _ = h.Write(value)
}

func HashLeaf(key string, value storage.Value) []byte {
	h := sha256.New()

	// Domain separation and hash format version.
	_, _ = h.Write([]byte("minikv:leaf:v2"))

	writeField(h, []byte(key))
	writeField(h, []byte(value.Data))
	writeField(h, []byte(value.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")))
	writeField(h, []byte(value.Clock.Serialize()))

	if value.Deleted {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}

	return h.Sum(nil)
}

func HashInternal(left, right []byte) []byte {
	h := sha256.New()

	_, _ = h.Write([]byte("minikv:internal:v2"))
	writeField(h, left)
	writeField(h, right)

	return h.Sum(nil)
}
