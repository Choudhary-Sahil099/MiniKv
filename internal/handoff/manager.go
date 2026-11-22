package handoff

import (
	"fmt"
	"sync"

	"go.uber.org/zap"

	"minikv/internal/logger"
)

type Manager struct {
	hints           []Hint
	mu              sync.Mutex
	persistencePath string
}

func NewManager() *Manager {
	return &Manager{
		hints: []Hint{},
	}
}
func (m *Manager) AddHint(h Hint) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	next := append(append([]Hint(nil), m.hints...), h)
	if err := m.saveLocked(next); err != nil {
		return fmt.Errorf("persist added hint: %w", err)
	}
	m.hints = next

	logger.Log.Debug(
		"hint stored",
		zap.String("target", h.TargetNode),
	)
	return nil
}
func (m *Manager) PendingHints() []Hint {

	m.mu.Lock()
	defer m.mu.Unlock()

	copyHints := make(
		[]Hint,
		len(m.hints),
	)

	copy(
		copyHints,
		m.hints,
	)

	return copyHints
}
func (m *Manager) RemoveHint(index int) {
	if err := m.RemoveHintAt(index); err != nil {
		logger.Log.Error("failed to durably remove hint", zap.Error(err))
	}
}

func (m *Manager) RemoveHintAt(index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index < 0 || index >= len(m.hints) {
		return nil
	}
	next := make([]Hint, 0, len(m.hints)-1)
	next = append(next, m.hints[:index]...)
	next = append(next, m.hints[index+1:]...)
	if err := m.saveLocked(next); err != nil {
		return err
	}
	m.hints = next
	logger.Log.Info("hint removed", zap.Int("remaining", len(m.hints)))
	return nil
}

// RemoveHintEntry removes the matching hint by value rather than by its
// position in a snapshot. This remains correct when several hints are
// successfully replayed during the same pass.
func (m *Manager) RemoveHintEntry(target Hint) bool {
	removed, err := m.RemoveHintEntryDurable(target)
	if err != nil {
		logger.Log.Error("failed to durably remove hint", zap.Error(err))
	}
	return removed
}

// RemoveHintEntryDurable reports persistence failures instead of silently
// losing the pending hint, allowing replay to try again later.
func (m *Manager) RemoveHintEntryDurable(target Hint) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, hint := range m.hints {
		if hint.TargetNode == target.TargetNode &&
			hint.Command == target.Command && hint.CreatedAt.Equal(target.CreatedAt) {
			next := make([]Hint, 0, len(m.hints)-1)
			next = append(next, m.hints[:i]...)
			next = append(next, m.hints[i+1:]...)
			if err := m.saveLocked(next); err != nil {
				return false, err
			}
			m.hints = next
			logger.Log.Info("hint removed", zap.Int("remaining", len(m.hints)))
			return true, nil
		}
	}
	return false, nil
}
