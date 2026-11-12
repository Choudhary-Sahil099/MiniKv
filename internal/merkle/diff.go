package merkle

import "sort"

func collectLeaves(node *Node, leaves map[string]*Node) {
	if node == nil {
		return
	}

	if node.Left == nil && node.Right == nil {
		leaves[node.Key] = node
		return
	}

	collectLeaves(node.Left, leaves)
	collectLeaves(node.Right, leaves)
}

func (t *Tree) Differences(other *Tree) []*Node {
	if t == nil || t.Root == nil {
		if other == nil || other.Root == nil {
			return nil
		}

		var differences []*Node
		collectLeaves(other.Root, make(map[string]*Node))
		leaves := make(map[string]*Node)
		collectLeaves(other.Root, leaves)

		for _, leaf := range leaves {
			differences = append(differences, leaf)
		}

		sort.Slice(differences, func(i, j int) bool {
			return differences[i].Key < differences[j].Key
		})

		return differences
	}

	if other == nil || other.Root == nil {
		var differences []*Node
		leaves := make(map[string]*Node)
		collectLeaves(t.Root, leaves)

		for _, leaf := range leaves {
			differences = append(differences, leaf)
		}

		sort.Slice(differences, func(i, j int) bool {
			return differences[i].Key < differences[j].Key
		})

		return differences
	}

	localLeaves := make(map[string]*Node)
	remoteLeaves := make(map[string]*Node)

	collectLeaves(t.Root, localLeaves)
	collectLeaves(other.Root, remoteLeaves)

	keys := make(map[string]struct{}, len(localLeaves)+len(remoteLeaves))

	for key := range localLeaves {
		keys[key] = struct{}{}
	}

	for key := range remoteLeaves {
		keys[key] = struct{}{}
	}

	orderedKeys := make([]string, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)

	var differences []*Node

	for _, key := range orderedKeys {
		local, localExists := localLeaves[key]
		remote, remoteExists := remoteLeaves[key]

		if !localExists {
			differences = append(differences, remote)
			continue
		}

		if !remoteExists || string(local.Hash) != string(remote.Hash) {
			differences = append(differences, local)
		}
	}

	return differences
}
