package hashring

import (
	"hash/crc32"
	"minikv/internal/cluster"
	"sort"
	"strconv"
)

type HashRing struct {
	nodes map[uint32]cluster.Node
	keys  []uint32

	virtualNodes      int
	replicationFactor int
}

func NewHashRing(
	virtualNodes int,
	replicationFactor int,
) *HashRing {

	return &HashRing{
		nodes: make(map[uint32]cluster.Node),
		keys:  []uint32{},

		virtualNodes:      virtualNodes,
		replicationFactor: replicationFactor,
	}
}
func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (h *HashRing) AddNode(node cluster.Node) {

	for i := 0; i < h.virtualNodes; i++ {

		virtualNode := node.ID + "#" + strconv.Itoa(i)

		hash := hashKey(virtualNode)

		h.nodes[hash] = node

		h.keys = append(h.keys, hash)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *HashRing) GetNode(key string) cluster.Node {
	if len(h.keys) == 0 {
		return cluster.Node{}
	}
	hash := hashKey(key)
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}
	nodeHash := h.keys[idx]
	return h.nodes[nodeHash]
}

func (h *HashRing) GetReplicaNode(key string) cluster.Node {

	if len(h.keys) == 0 {
		return cluster.Node{}
	}

	hash := hashKey(key)

	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})

	if idx == len(h.keys) {
		idx = 0
	}

	primaryHash := h.keys[idx]

	primaryNode := h.nodes[primaryHash]

	replicaIdx := (idx + 1) % len(h.keys)

	replicaHash := h.keys[replicaIdx]

	replicaNode := h.nodes[replicaHash]

	// avoid same node replica -->  important for the Replication process
	if replicaNode.ID == primaryNode.ID {

		replicaIdx = (replicaIdx + 1) % len(h.keys)

		replicaHash = h.keys[replicaIdx]

		replicaNode = h.nodes[replicaHash]
	}

	return replicaNode
}
func (h *HashRing) GetReplicaNodes(
	key string,
) []cluster.Node {
	replicationFactor := h.replicationFactor

	if len(h.keys) == 0 || replicationFactor <= 1 {
		return nil
	}

	hash := hashKey(key)

	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})

	if idx == len(h.keys) {
		idx = 0
	}

	owner := h.nodes[h.keys[idx]]

	replicas := []cluster.Node{}

	seen := make(map[string]bool)
	seen[owner.ID] = true

	current := idx

	for len(replicas) < replicationFactor-1 {

		current = (current + 1) % len(h.keys)

		node := h.nodes[h.keys[current]]

		if seen[node.ID] {
			continue
		}

		replicas = append(replicas, node)
		seen[node.ID] = true

		// Safety check
		if len(replicas) == replicationFactor-1 {
			break
		}
	}

	return replicas
}
