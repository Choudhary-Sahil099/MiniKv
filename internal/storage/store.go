package storage

import (
	"hash/crc32"
	"sync"
	"time"

	"minikv/internal/vectorclock"
)

type Store struct {
	data     map[string]Value
	mu       sync.RWMutex
	keyLocks [256]sync.Mutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Value),
	}
}

// LockKey serializes read-modify-write operations for the same key without
// blocking unrelated keys. The fixed stripe array bounds lock memory usage.
func (s *Store) LockKey(key string) func() {
	index := crc32.ChecksumIEEE([]byte(key)) % uint32(len(s.keyLocks))
	lock := &s.keyLocks[index]
	lock.Lock()
	return lock.Unlock
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

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	if !exists || value.Deleted {
		return "", false
	}
	return value.Data, true
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

// SetTombstone stores a versioned deletion. Tombstones remain in the store so
// replication and anti-entropy can prevent older values from resurrecting.
func (s *Store) SetTombstone(key string, value Value) {
	value.Deleted = true
	s.SetValue(key, value)
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

func (s *Store) MergeIfNewer(key string, candidate Value) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists := s.data[key]
	if !exists {
		s.data[key] = candidate
		return true
	}

	relation := vectorclock.Compare(candidate.Clock, current.Clock)

	switch relation {
	case vectorclock.After:
		s.data[key] = candidate
		return true

	case vectorclock.Concurrent, vectorclock.Equal:
		// Concurrent versions use the project's timestamp-based conflict
		// policy. Equal clocks can also carry a later timestamp after a
		// repair/replay, so apply the same deterministic tie-breaker here.
		if candidate.CreatedAt.After(current.CreatedAt) {
			s.data[key] = candidate
			return true
		}
	}

	return false
}
