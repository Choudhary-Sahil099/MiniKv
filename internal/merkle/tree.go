package merkle

import (
	"sort"

	"minikv/internal/storage"
)

type Tree struct {
	Root  *Node
	arena [][]Node
}

func Build(data map[string]storage.Value) *Tree {
	if len(data) == 0 {
		return &Tree{}
	}

	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	tree := &Tree{
		arena: make([][]Node, 0),
	}

	// Allocate all leaves in one contiguous block.
	leaves := make([]Node, len(keys))
	level := make([]*Node, len(keys))

	for i, key := range keys {
		value := data[key]

		leaves[i] = Node{
			Hash:     HashLeaf(key, value),
			Key:      key,
			StartKey: key,
			EndKey:   key,
		}

		level[i] = &leaves[i]
	}

	tree.arena = append(tree.arena, leaves)

	// Build parent levels using contiguous node allocations.
	for len(level) > 1 {
		parentCount := (len(level) + 1) / 2

		parents := make([]Node, parentCount)
		nextLevel := make([]*Node, parentCount)

		for i := 0; i < len(level); i += 2 {
			parentIndex := i / 2

			left := level[i]
			right := left

			if i+1 < len(level) {
				right = level[i+1]
			}

			parents[parentIndex] = Node{
				Hash:  HashInternal(left.Hash, right.Hash),
				Left:  left,
				Right: right,

				StartKey: left.StartKey,
				EndKey:   right.EndKey,
			}

			nextLevel[parentIndex] = &parents[parentIndex]
		}

		tree.arena = append(tree.arena, parents)
		level = nextLevel
	}

	tree.Root = level[0]

	return tree
}
