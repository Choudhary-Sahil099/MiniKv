package handoff

import (
	"minikv/internal/logger"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m := NewManager()

	if m == nil {
		t.Fatal("expected manager to be created")
	}

	if len(m.PendingHints()) != 0 {
		t.Fatal("expected no pending hints")
	}
}

func TestAddHint(t *testing.T) {
	m := NewManager()

	h := Hint{
		TargetNode: "node1",
		Command:    "SET key value",
		CreatedAt:  time.Now(),
	}

	m.AddHint(h)

	hints := m.PendingHints()

	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}

	if hints[0].TargetNode != h.TargetNode {
		t.Fatal("target node mismatch")
	}

	if hints[0].Command != h.Command {
		t.Fatal("command mismatch")
	}
}

func TestPendingHintsReturnsCopy(t *testing.T) {
	m := NewManager()

	m.AddHint(Hint{
		TargetNode: "node1",
		Command:    "SET a 1",
		CreatedAt:  time.Now(),
	})

	hints := m.PendingHints()

	hints[0].Command = "MODIFIED"

	newHints := m.PendingHints()

	if newHints[0].Command != "SET a 1" {
		t.Fatal("PendingHints should return a copy, not the original slice")
	}
}

func TestRemoveHint(t *testing.T) {
	m := NewManager()

	m.AddHint(Hint{
		TargetNode: "node1",
		Command:    "SET a 1",
		CreatedAt:  time.Now(),
	})

	m.AddHint(Hint{
		TargetNode: "node2",
		Command:    "SET b 2",
		CreatedAt:  time.Now(),
	})

	m.RemoveHint(0)

	hints := m.PendingHints()

	if len(hints) != 1 {
		t.Fatalf("expected 1 hint remaining, got %d", len(hints))
	}

	if hints[0].TargetNode != "node2" {
		t.Fatal("wrong hint removed")
	}
}

func TestRemoveHintInvalidIndex(t *testing.T) {
	m := NewManager()

	m.AddHint(Hint{
		TargetNode: "node1",
		Command:    "SET a 1",
		CreatedAt:  time.Now(),
	})

	m.RemoveHint(-1)
	m.RemoveHint(10)

	hints := m.PendingHints()

	if len(hints) != 1 {
		t.Fatal("invalid index should not remove hints")
	}
}

func TestInit(t *testing.T) {
	logger.Init()

	if logger.Log == nil {
		t.Fatal("logger should not be nil")
	}
}

func TestRemoveHintEntryRemovesMatchingEntriesAfterEarlierRemoval(t *testing.T) {
	m := NewManager()
	first := Hint{TargetNode: "node1", Command: "REPL_SET a 1", CreatedAt: time.Unix(1, 0)}
	second := Hint{TargetNode: "node2", Command: "REPL_SET b 2", CreatedAt: time.Unix(2, 0)}
	third := Hint{TargetNode: "node3", Command: "REPL_SET c 3", CreatedAt: time.Unix(3, 0)}

	m.AddHint(first)
	m.AddHint(second)
	m.AddHint(third)

	if !m.RemoveHintEntry(first) {
		t.Fatal("expected first hint to be removed")
	}
	if !m.RemoveHintEntry(second) {
		t.Fatal("expected second hint to be removed")
	}

	remaining := m.PendingHints()
	if len(remaining) != 1 || remaining[0] != third {
		t.Fatalf("expected only third hint to remain, got %+v", remaining)
	}
}
