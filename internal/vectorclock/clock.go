package vectorclock

type VectorClock map[string]int

func (vc *VectorClock) Increment(nodeID string) {
	if *vc == nil {
		*vc = make(VectorClock)
	}
	(*vc)[nodeID]++
}

func (vc VectorClock) Copy() VectorClock {
	copyClock := make(VectorClock)
	for node, counter := range vc {
		copyClock[node] = counter
	}
	return copyClock
}