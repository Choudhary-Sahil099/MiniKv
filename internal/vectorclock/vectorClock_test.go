package vectorclock

import "testing"

func TestIncrement(t *testing.T) {

	var vc VectorClock

	vc.Increment("NodeA")

	if vc["NodeA"] != 1 {
		t.Fatalf("expected 1 got %d", vc["NodeA"])
	}

	vc.Increment("NodeA")

	if vc["NodeA"] != 2 {
		t.Fatalf("expected 2 got %d", vc["NodeA"])
	}
}

func TestCopy(t *testing.T) {

	vc := VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	copyVC := vc.Copy()

	copyVC["NodeA"] = 100

	if vc["NodeA"] != 1 {
		t.Fatal("copy modified original")
	}
}

func TestCompareEqual(t *testing.T) {

	a := VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	b := VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	if Compare(a, b) != Equal {
		t.Fatal("expected Equal")
	}
}

func TestCompareBefore(t *testing.T) {

	a := VectorClock{
		"NodeA": 1,
	}

	b := VectorClock{
		"NodeA": 2,
	}

	if Compare(a, b) != Before {
		t.Fatal("expected Before")
	}
}

func TestCompareAfter(t *testing.T) {

	a := VectorClock{
		"NodeA": 5,
	}

	b := VectorClock{
		"NodeA": 3,
	}

	if Compare(a, b) != After {
		t.Fatal("expected After")
	}
}

func TestCompareConcurrent(t *testing.T) {

	a := VectorClock{
		"NodeA": 2,
		"NodeB": 1,
	}

	b := VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	if Compare(a, b) != Concurrent {
		t.Fatal("expected Concurrent")
	}
}

func TestSerialize(t *testing.T) {

	vc := VectorClock{
		"NodeB": 2,
		"NodeA": 1,
	}

	expected := "NodeA=1,NodeB=2"

	if vc.Serialize() != expected {
		t.Fatalf("expected %s got %s", expected, vc.Serialize())
	}
}

func TestSerializeEmpty(t *testing.T) {

	vc := VectorClock{}

	if vc.Serialize() != "" {
		t.Fatal("expected empty string")
	}
}

func TestDeserialize(t *testing.T) {

	vc := Deserialize("NodeA=1,NodeB=2")

	if vc["NodeA"] != 1 {
		t.Fatal("incorrect NodeA")
	}

	if vc["NodeB"] != 2 {
		t.Fatal("incorrect NodeB")
	}
}

func TestDeserializeEmpty(t *testing.T) {

	vc := Deserialize("")

	if len(vc) != 0 {
		t.Fatal("expected empty clock")
	}
}

func TestDeserializeInvalid(t *testing.T) {

	vc := Deserialize("NodeA=abc,wrongformat")

	if len(vc) != 0 {
		t.Fatal("invalid entries should be ignored")
	}
}

func TestComparisonString(t *testing.T) {

	tests := []struct {
		comp Comparison
		want string
	}{
		{Before, "Before"},
		{After, "After"},
		{Equal, "Equal"},
		{Concurrent, "Concurrent"},
	}

	for _, tt := range tests {

		if tt.comp.String() != tt.want {
			t.Fatalf("expected %s got %s", tt.want, tt.comp.String())
		}
	}
}
func TestCompareMissingNodes(t *testing.T) {

	a := VectorClock{
		"NodeA": 1,
	}

	b := VectorClock{
		"NodeA": 1,
		"NodeB": 1,
	}

	if Compare(a, b) != Before {
		t.Fatal("missing node should be treated as counter 0")
	}
}