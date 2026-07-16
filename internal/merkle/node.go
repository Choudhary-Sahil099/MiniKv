package merkle

type Node struct {
	Hash []byte

	Left  *Node
	Right *Node

	Key string

	StartKey string
	EndKey   string
}