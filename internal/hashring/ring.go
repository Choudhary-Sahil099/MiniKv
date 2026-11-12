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
	replicas := h.GetReplicaNodes(key)
	if len(replicas) == 0 {
		return cluster.Node{}
	}
	return replicas[0]
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

	// Walk at most one full ring. If the configured replication factor is
	// larger than the number of distinct physical nodes, return the replicas
	// that actually exist instead of looping forever.
	for visited := 0; visited < len(h.keys) && len(replicas) < replicationFactor-1; visited++ {
		current = (current + 1) % len(h.keys)

		node := h.nodes[h.keys[current]]
		if seen[node.ID] {
			continue
		}

		replicas = append(replicas, node)
		seen[node.ID] = true
	}

	return replicas
}
