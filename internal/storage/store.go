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
		Data:      value,
		CreatedAt: time.Now(), // this creates the timeStamp
	}
}

func (s *Store) SetValue(key string, value Value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// this is the timeStamp version of the anti_entropy  and is used to preserve the time stamp
func (s *Store) SetWithTimestamp(
	key string,
	value string,
	createdAt time.Time,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = Value{
		Data:      value,
		CreatedAt: createdAt,
	}
}

func (s *Store) SetValueWithTimestamp(
	key string,
	value Value,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	return value.Data, exists
}
func (s *Store) GetValue(
	key string,
) (Value, bool) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]

	return value, exists
}
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
func (s *Store) Export() map[string]Value {

	s.mu.RLock()
	defer s.mu.RUnlock()

	copyData := make(map[string]Value)

	for k, v := range s.data {
		copyData[k] = v
	}

	return copyData
}
func (s *Store) Import(data map[string]Value) {

    s.mu.Lock()
    defer s.mu.Unlock()

    newData := make(map[string]Value, len(data))

    for k, v := range data {
        newData[k] = v
    }

    s.data = newData
}
