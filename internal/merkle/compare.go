package merkle

func CompareNodes(
	local *Node,
	remote *Node,
	differences *[]*Node,
	seen map[string]bool,
) {

	if local == nil || remote == nil {
		return
	}

	// check if the subtreees are identical or not
	if string(local.Hash) == string(remote.Hash) {
		return
	}

	// hit the bottom
	if local.Left == nil &&
		local.Right == nil {

		if !seen[local.Key] {
			*differences = append(
				*differences,
				local,
			)

			seen[local.Key] = true
		}

		return
	}

	CompareNodes(
		local.Left,
		remote.Left,
		differences,
		seen,
	)

	CompareNodes(
		local.Right,
		remote.Right,
		differences,
		seen,
	)
}
