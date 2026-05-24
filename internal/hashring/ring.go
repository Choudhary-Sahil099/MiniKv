package hashring

import (
	"hash/crc32"
	"minikv/internal/cluster"
	"sort"
	"strconv"
)

type HashRing struct {
	nodes    map[uint32]cluster.Node
	keys     []uint32
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodes:    make(map[uint32]cluster.Node),
		keys:     []uint32{},
		replicas: replicas,
	}
}
func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (h *HashRing) AddNode(node cluster.Node) {

	for i := 0; i < h.replicas; i++ {

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
