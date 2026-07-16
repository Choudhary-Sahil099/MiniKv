package merkle

func findSubtree(
	node *Node,
	startKey string,
	endKey string,
) *Node {

	if node == nil {
		return nil
	}

	if node.StartKey == startKey &&
		node.EndKey == endKey {

		return node
	}

	left := findSubtree(
		node.Left,
		startKey,
		endKey,
	)

	if left != nil {
		return left
	}

	return findSubtree(
		node.Right,
		startKey,
		endKey,
	)
}


func (t *Tree) FindSubtree(
	startKey string,
	endKey string,
) *Node {

	return findSubtree(
		t.Root,
		startKey,
		endKey,
	)
}
