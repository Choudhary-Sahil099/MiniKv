package storage
import "time"
import "sync"

type Store struct {
	data map[string]Value
	mu   sync.RWMutex
}
func NewStore() *Store {
	return &Store{
		data: make(map[string]Value),
	}
}
func (s *Store) Set(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock() // Only one writer allowed

	s.data[key] = Value{
	Data: value,
	CreatedAt: time.Now(),
}
}
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	return value.Data, exists
}
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}