package vectorclock

type Comparison int

const (
	Before Comparison = iota
	After
	Equal
	Concurrent
)

func Compare(
	a VectorClock,
	b VectorClock,
) Comparison {

	aLess := false
	aGreater := false

	nodes := make(map[string]struct{})

	for node := range a {
		nodes[node] = struct{}{}
	}

	for node := range b {
		nodes[node] = struct{}{}
	}

	for node := range nodes {

		if a[node] < b[node] {
			aLess = true
		}

		if a[node] > b[node] {
			aGreater = true
		}
	}

	switch {

	case !aLess && !aGreater:
		return Equal

	case aLess && !aGreater:
		return Before

	case !aLess && aGreater:
		return After

	default:
		return Concurrent
	}
}