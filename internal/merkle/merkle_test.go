package merkle

import (
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"testing"
	"time"
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

func TestRootHashIncludesVersionMetadata(t *testing.T) {
	clockA := make(vectorclock.VectorClock)
	clockA["NodeA"] = 1
	clockB := make(vectorclock.VectorClock)
	clockB["NodeA"] = 2
	createdAt := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

	base := map[string]storage.Value{"key": {Data: "same", CreatedAt: createdAt, Clock: clockA}}
	newerClock := map[string]storage.Value{"key": {Data: "same", CreatedAt: createdAt, Clock: clockB}}
	tombstone := map[string]storage.Value{"key": {Data: "same", CreatedAt: createdAt, Clock: clockA, Deleted: true}}

	baseHash := Build(base).RootHash()
	if baseHash == Build(newerClock).RootHash() {
		t.Fatal("different vector clocks must produce different Merkle roots")
	}
	if baseHash == Build(tombstone).RootHash() {
		t.Fatal("tombstone state must affect the Merkle root")
	}
}

func TestDifferencesDetectLocalOnlyKey(t *testing.T) {
	local := createData()
	local["d"] = storage.Value{Data: "4"}

	remote := createData()

	diff := Build(local).Differences(Build(remote))

	if len(diff) != 1 {
		t.Fatalf("expected 1 difference, got %d", len(diff))
	}

	if diff[0].Key != "d" {
		t.Fatalf("expected key d, got %s", diff[0].Key)
	}
}

func TestDifferencesDetectRemoteOnlyKey(t *testing.T) {
	local := createData()

	remote := createData()
	remote["d"] = storage.Value{Data: "4"}

	diff := Build(local).Differences(Build(remote))

	if len(diff) != 1 {
		t.Fatalf("expected 1 difference, got %d", len(diff))
	}

	if diff[0].Key != "d" {
		t.Fatalf("expected key d, got %s", diff[0].Key)
	}
}

func TestDifferencesDetectDifferentTreeSizes(t *testing.T) {
	local := map[string]storage.Value{
		"a": {Data: "1"},
		"b": {Data: "2"},
	}

	remote := map[string]storage.Value{
		"a": {Data: "1"},
		"b": {Data: "2"},
		"c": {Data: "3"},
		"d": {Data: "4"},
	}

	diff := Build(local).Differences(Build(remote))

	if len(diff) != 2 {
		t.Fatalf("expected 2 differences, got %d", len(diff))
	}
}
