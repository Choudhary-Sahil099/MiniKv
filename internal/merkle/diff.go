package merkle

func (t *Tree) Differences(
	other *Tree,
) []*Node {

	if t == nil ||
		other == nil ||
		t.Root == nil ||
		other.Root == nil {

		return nil
	}

	var differences []*Node

	seen := make(map[string]bool)

	CompareNodes(
		t.Root,
		other.Root,
		&differences,
		seen,
	)

	return differences
}
