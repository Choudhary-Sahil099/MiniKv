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

	logger.Log.Info(
		"hint stored",
		zap.String("target", h.TargetNode),
		zap.String("command", h.Command),
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