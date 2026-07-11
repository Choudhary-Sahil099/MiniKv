package handoff

import "sync"

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

	m.hints = append(
		m.hints,
		h,
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