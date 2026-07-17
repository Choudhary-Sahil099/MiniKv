package storage

import (
	"minikv/internal/vectorclock"
	"time"
)

type Value struct {
	Data      string
	CreatedAt time.Time
	Clock     vectorclock.VectorClock
}
