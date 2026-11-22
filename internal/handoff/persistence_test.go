package handoff

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistentHintsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node1.hints.json")
	first, err := NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	hint := Hint{TargetNode: "node2", Command: "REPL_SET a 1", CreatedAt: time.Unix(123, 0).UTC()}
	if err := first.AddHint(hint); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	pending := restarted.PendingHints()
	if len(pending) != 1 || pending[0] != hint {
		t.Fatalf("hint lost on restart: %+v", pending)
	}
	removed, err := restarted.RemoveHintEntryDurable(hint)
	if err != nil || !removed {
		t.Fatalf("durable removal failed: removed=%v err=%v", removed, err)
	}
	secondRestart, err := NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondRestart.PendingHints()) != 0 {
		t.Fatal("replayed hint returned after restart")
	}
}

func TestCorruptedHintFileFailsStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.hints.json")
	if err := os.WriteFile(path, []byte("{invalid JSON"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPersistentManager(path); err == nil {
		t.Fatal("corrupt persistence must fail startup")
	}
}

func TestFailedHintPersistenceDoesNotPublishHint(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewPersistentManager(filepath.Join(blocker, "hints.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddHint(Hint{TargetNode: "node2", Command: "REPL_SET a 1", CreatedAt: time.Now()}); err == nil {
		t.Fatal("expected persistence failure")
	}
	if got := len(manager.PendingHints()); got != 0 {
		t.Fatalf("hint published despite failed persistence: %d", got)
	}
}
