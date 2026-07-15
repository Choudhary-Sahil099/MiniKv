package merkle

import (
	"crypto/sha256"
)

func HashLeaf(
	key string,
	value string,
) []byte {

	hash := sha256.Sum256(
		[]byte(key + ":" + value),
	)

	return hash[:]
}

func HashInternal(
	left []byte,
	right []byte,
) []byte {

	combined := append(left, right...)

	hash := sha256.Sum256(combined)

	return hash[:]
}