package handoff

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// NewPersistentManager restores previously saved hints. Invalid or unreadable
// files fail startup rather than silently discarding pending repairs.
func NewPersistentManager(path string) (*Manager, error) {
	if path == "" {
		return nil, fmt.Errorf("hint persistence path is empty")
	}
	manager := &Manager{hints: []Hint{}, persistencePath: path}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return manager, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read hints %q: %w", path, err)
	}
	var hints []Hint
	if err := json.Unmarshal(data, &hints); err != nil {
		return nil, fmt.Errorf("decode hints %q: %w", path, err)
	}
	if hints == nil {
		return nil, fmt.Errorf("hint file %q contains null", path)
	}
	for i, hint := range hints {
		if hint.TargetNode == "" || hint.Command == "" {
			return nil, fmt.Errorf("invalid persisted hint %d in %q", i, path)
		}
	}
	manager.hints = hints
	return manager, nil
}

// saveLocked writes the proposed state atomically before publishing it in RAM.
// The rename is made durable by syncing the containing directory where supported.
// The caller must hold m.mu.
func (m *Manager) saveLocked(next []Hint) error {
	if m.persistencePath == "" {
		return nil
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode hints: %w", err)
	}
	dir := filepath.Dir(m.persistencePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create hint directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".hints-*")
	if err != nil {
		return fmt.Errorf("create temporary hint file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return fmt.Errorf("write hints: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync hints: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close hints: %w", err)
	}
	if err := os.Rename(tmpName, m.persistencePath); err != nil {
		return fmt.Errorf("commit hints: %w", err)
	}
	// Directory sync is best-effort across OS/filesystems (notably Windows).
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open hints directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		// Many filesystems / Windows do not support syncing directories.
		// The renamed hint file exists; don't return an error that suggests
		// the in-memory state can safely remain unchanged.
		return nil
	}
	return nil
}
