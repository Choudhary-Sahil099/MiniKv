package handoff

import (
	"sync"

	"go.uber.org/zap"

	"minikv/internal/logger"
)

type Manager struct {
	hints []Hint
	mu    sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		hints: []Hint{},
	}
}
func (m *Manager) AddHint(h Hint) {

	m.mu.Lock()
	defer m.mu.Unlock()

	m.hints = append(m.hints, h)

	logger.Log.Debug(
		"hint stored",
		zap.String("target", h.TargetNode),
	)
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

	m.mu.Lock()
	defer m.mu.Unlock()

	if index < 0 || index >= len(m.hints) {
		return
	}

	m.hints = append(
		m.hints[:index],
		m.hints[index+1:]...,
	)
	logger.Log.Info(
		"hint removed",
		zap.Int("remaining", len(m.hints)),
	)
}

// RemoveHintEntry removes the matching hint by value rather than by its
// position in a snapshot. This remains correct when several hints are
// successfully replayed during the same pass.
func (m *Manager) RemoveHintEntry(target Hint) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, hint := range m.hints {
		if hint.TargetNode == target.TargetNode &&
			hint.Command == target.Command &&
			hint.CreatedAt.Equal(target.CreatedAt) {
			m.hints = append(m.hints[:i], m.hints[i+1:]...)
			logger.Log.Info(
				"hint removed",
				zap.Int("remaining", len(m.hints)),
			)
			return true
		}
	}
	return false
}
