package merkle

import (
	"minikv/internal/storage"
	"sort"
)

type Tree struct {
	Root *Node
}

func Build(
	data map[string]storage.Value,
) *Tree {

	keys := make([]string, 0, len(data))

	for key := range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	leaves := make([]*Node, 0, len(keys))

	for _, key := range keys {

		value := data[key]

		leaf := &Node{
			Hash: HashLeaf(
				key,
				value.Data,
			),
			Key: key,

			StartKey: key,
			EndKey:   key,
		}

		leaves = append(
			leaves,
			leaf,
		)
	}

	nodes := leaves
	if len(nodes) == 0 {
		return &Tree{}
	}
	for len(nodes) > 1 {

		nextLevel := make([]*Node, 0, (len(nodes)+1)/2)

		for i := 0; i < len(nodes); i += 2 {

			left := nodes[i]
			right := left

			if i+1 < len(nodes) {
				right = nodes[i+1]
			}

			parent := &Node{
				Hash: HashInternal(
					left.Hash,
					right.Hash,
				),

				Left:  left,
				Right: right,

				StartKey: left.StartKey,
				EndKey:   right.EndKey,
			}

			nextLevel = append(
				nextLevel,
				parent,
			)
		}

		nodes = nextLevel
	}

	return &Tree{
		Root: nodes[0],
	}
}
