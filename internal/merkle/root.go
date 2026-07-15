package merkle

import "encoding/hex"

func (t *Tree) RootHash() string {

	if t.Root == nil {
		return ""
	}

	return hex.EncodeToString(t.Root.Hash)
}