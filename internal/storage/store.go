package storage

import (
	"sync"
	"time"

	"minikv/internal/vectorclock"
)

const (
	shardCount   = 64
	keyLockCount = 256
)

type shard struct {
	mu   sync.RWMutex
	data map[string]Value
}

type Store struct {
	shards   [shardCount]shard
	keyLocks [keyLockCount]sync.Mutex
}

func cloneValue(value Value) Value {
	value.Clock = value.Clock.Copy()
	return value
}

func NewStore() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].data = make(map[string]Value)
	}
	return s
}

func hashKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	return h
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[hashKey(key)&(shardCount-1)]
}

func (s *Store) LockKey(key string) func() {
	lock := &s.keyLocks[hashKey(key)&(keyLockCount-1)]
	lock.Lock()
	return lock.Unlock
}

func (s *Store) Set(key string, value string) {
	v := Value{
		Data:      value,
		CreatedAt: time.Now(), // this creates the timeStamp
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.data[key] = v
	sh.mu.Unlock()
}

func (s *Store) SetValue(key string, value Value) {
	value = cloneValue(value)

	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.data[key] = value
	sh.mu.Unlock()
}

// this is the timeStamp version of the anti_entropy  and is used to preserve the time stamp

func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	value, exists := sh.data[key]
	if !exists || value.Deleted {
		return "", false
	}
	return value.Data, true
}

func (s *Store) GetValue(
	key string,
) (Value, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	value, exists := sh.data[key]
	if !exists {
		return Value{}, false
	}

	return cloneValue(value), true
}

func (s *Store) Delete(key string) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	delete(sh.data, key)
	sh.mu.Unlock()
}

func (s *Store) SetTombstone(key string, value Value) {
	value.Deleted = true
	s.SetValue(key, value)
}

func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

func (s *Store) Export() map[string]Value {
	copyData := make(map[string]Value, s.Len())

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, value := range sh.data {
			copyData[key] = cloneValue(value)
		}
		sh.mu.RUnlock()
	}

	return copyData
}

func (s *Store) Range(fn func(key string, value Value) bool) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, value := range sh.data {
			if !fn(key, cloneValue(value)) {
				sh.mu.RUnlock()
				return
			}
		}
		sh.mu.RUnlock()
	}
}

func (s *Store) Import(data map[string]Value) {
	var fresh [shardCount]map[string]Value
	perShard := len(data)/shardCount + 1
	for i := range fresh {
		fresh[i] = make(map[string]Value, perShard)
	}
	for key, value := range data {
		fresh[hashKey(key)&(shardCount-1)][key] = cloneValue(value)
	}

	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	for i := range s.shards {
		s.shards[i].data = fresh[i]
	}
	for i := range s.shards {
		s.shards[i].mu.Unlock()
	}
}

func (s *Store) MergeIfNewer(key string, candidate Value) bool {
	candidate = cloneValue(candidate)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	current, exists := sh.data[key]
	if !exists {
		sh.data[key] = candidate
		return true
	}

	relation := vectorclock.Compare(candidate.Clock, current.Clock)

	switch relation {
	case vectorclock.After:
		sh.data[key] = candidate
		return true

	case vectorclock.Concurrent, vectorclock.Equal:
		if candidate.CreatedAt.After(current.CreatedAt) {
			sh.data[key] = candidate
			return true
		}
	}

	return false
}
