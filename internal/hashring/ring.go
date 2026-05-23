package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashRing struct {
	nodes    map[uint32]string
	keys     []uint32
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodes:    make(map[uint32]string),
		keys:     []uint32{},
		replicas: replicas,
	}
}
func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (h *HashRing) AddNode(node string) {

	for i := 0; i < h.replicas; i++ {

		virtualNode := node + "#" + strconv.Itoa(i)

		hash := hashKey(virtualNode)

		h.nodes[hash] = node

		h.keys = append(h.keys, hash)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}
func (h *HashRing) GetNode(key string) string {
	if len(h.keys) == 0 {
		return ""
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
