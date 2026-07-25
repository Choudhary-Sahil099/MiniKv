package merkle

import (
	"minikv/internal/storage"
	"testing"
)

func createData() map[string]storage.Value {
	return map[string]storage.Value{
		"a": {Data: "1"},
		"b": {Data: "2"},
		"c": {Data: "3"},
	}
}

func TestBuildEmptyTree(t *testing.T) {

	tree := Build(map[string]storage.Value{})

	if tree == nil {
		t.Fatal("tree is nil")
	}

	if tree.Root != nil {
		t.Fatal("empty tree should have nil root")
	}
}

func TestBuildSingleNode(t *testing.T) {

	data := map[string]storage.Value{
		"a": {Data: "1"},
	}

	tree := Build(data)

	if tree.Root == nil {
		t.Fatal("root should not be nil")
	}

	if tree.Root.Key != "a" {
		t.Fatal("incorrect key")
	}
}

func TestRootHashDeterministic(t *testing.T) {

	data1 := map[string]storage.Value{
		"a": {Data: "1"},
		"b": {Data: "2"},
		"c": {Data: "3"},
	}

	data2 := map[string]storage.Value{
		"c": {Data: "3"},
		"a": {Data: "1"},
		"b": {Data: "2"},
	}

	tree1 := Build(data1)
	tree2 := Build(data2)

	if tree1.RootHash() != tree2.RootHash() {
		t.Fatal("root hash should be deterministic")
	}
}

func TestDifferentDataDifferentRoot(t *testing.T) {

	data1 := createData()

	data2 := createData()
	data2["b"] = storage.Value{Data: "100"}

	tree1 := Build(data1)
	tree2 := Build(data2)

	if tree1.RootHash() == tree2.RootHash() {
		t.Fatal("different data produced same root")
	}
}

func TestIdenticalTreesHaveNoDifferences(t *testing.T) {

	tree1 := Build(createData())
	tree2 := Build(createData())

	diff := tree1.Differences(tree2)

	if len(diff) != 0 {
		t.Fatal("identical trees should have no differences")
	}
}

func TestSingleDifference(t *testing.T) {

	data1 := createData()

	data2 := createData()
	data2["b"] = storage.Value{Data: "999"}

	tree1 := Build(data1)
	tree2 := Build(data2)

	diff := tree1.Differences(tree2)

	if len(diff) != 1 {
		t.Fatalf("expected 1 difference got %d", len(diff))
	}

	if diff[0].Key != "b" {
		t.Fatalf("expected key b got %s", diff[0].Key)
	}
}

func TestMultipleDifferences(t *testing.T) {

	data1 := createData()

	data2 := createData()
	data2["a"] = storage.Value{Data: "10"}
	data2["c"] = storage.Value{Data: "30"}

	tree1 := Build(data1)
	tree2 := Build(data2)

	diff := tree1.Differences(tree2)

	if len(diff) != 2 {
		t.Fatalf("expected 2 differences got %d", len(diff))
	}
}

func TestFindSubtree(t *testing.T) {

	tree := Build(createData())

	node := tree.FindSubtree("a", "b")

	if node == nil {
		t.Fatal("expected subtree")
	}

	if node.StartKey != "a" {
		t.Fatalf("expected start key a got %s", node.StartKey)
	}

	if node.EndKey != "b" {
		t.Fatalf("expected end key b got %s", node.EndKey)
	}
}

func TestFindSubtreeMissing(t *testing.T) {

	tree := Build(createData())

	node := tree.FindSubtree("x", "z")

	if node != nil {
		t.Fatal("expected nil subtree")
	}
}
func TestRootHashEmptyTree(t *testing.T) {

	tree := Build(map[string]storage.Value{})

	if tree.RootHash() != "" {
		t.Fatal("empty tree should return empty root hash")
	}
}