package network

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

const defaultMaxConnections = 1024

type Server struct {
	Address string
	NodeID  string
	Store   *storage.Store
	WAL     *wal.WAL
	Ring    *hashring.HashRing
	Gossip  *gossip.Gossip
	handoff *handoff.Manager

	// MaxConnections bounds accepted connections and their handler goroutines.
	// A value <= 0 uses defaultMaxConnections.
	MaxConnections int

	mu       sync.Mutex
	listener net.Listener
	active   map[net.Conn]struct{}
	workers  sync.WaitGroup
}

func NewServer(
	address string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	g *gossip.Gossip,
	handoff *handoff.Manager,
) *Server {
	return &Server{
		Address:        address,
		NodeID:         nodeID,
		Store:          store,
		WAL:            wal,
		Ring:           ring,
		Gossip:         g,
		handoff:        handoff,
		MaxConnections: defaultMaxConnections,
		active:         make(map[net.Conn]struct{}),
	}
}

// Start preserves the original blocking API. Applications that need controlled
// shutdown should use StartContext and cancel its context.
func (s *Server) Start() error {
	return s.StartContext(context.Background())
}

func (s *Server) StartContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("network server: nil context")
	}

	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		_ = listener.Close()
		return errors.New("network server: already started")
	}
	s.listener = listener
	s.mu.Unlock()

	logger.Log.Info("MiniKV TCP server listening", zap.String("address", s.Address), zap.String("node", s.NodeID))

	maxConnections := s.MaxConnections
	if maxConnections <= 0 {
		maxConnections = defaultMaxConnections
	}
	connectionSlots := make(chan struct{}, maxConnections)
	watcherDone := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
			s.closeActiveConnections()
		case <-watcherDone:
		}
	}()

	var acceptErr error
	backoff := 5 * time.Millisecond
acceptLoop:
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			if temporary, ok := err.(net.Error); ok && temporary.Temporary() {
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					break acceptLoop
				case <-timer.C:
				}
				if backoff < time.Second {
					backoff *= 2
				}
				continue
			}
			acceptErr = err
			logger.Log.Error("TCP accept failed", zap.String("node", s.NodeID), zap.Error(acceptErr))
			break
		}
		backoff = 5 * time.Millisecond

		select {
		case connectionSlots <- struct{}{}:
		default:
			logger.Log.Warn("connection limit reached; rejecting connection", zap.String("node", s.NodeID), zap.Int("limit", maxConnections))
			_ = conn.Close()
			continue
		}

		s.mu.Lock()
		if ctx.Err() != nil {
			s.mu.Unlock()
			<-connectionSlots
			_ = conn.Close()
			break
		}
		s.active[conn] = struct{}{}
		s.workers.Add(1)
		s.mu.Unlock()

		go func(conn net.Conn) {
			defer s.workers.Done()
			defer func() {
				s.mu.Lock()
				delete(s.active, conn)
				s.mu.Unlock()
				<-connectionSlots
			}()
			HandleConnection(conn, s.NodeID, s.Store, s.WAL, s.Ring, s.Gossip, s.handoff)
		}(conn)
	}

	close(watcherDone)
	_ = listener.Close()
	s.closeActiveConnections()
	s.workers.Wait()

	s.mu.Lock()
	s.listener = nil
	s.mu.Unlock()

	if ctx.Err() != nil {
		return nil
	}
	return acceptErr
}

func (s *Server) closeActiveConnections() {
	s.mu.Lock()
	connections := make([]net.Conn, 0, len(s.active))
	for conn := range s.active {
		connections = append(connections, conn)
	}
	s.mu.Unlock()

	for _, conn := range connections {
		_ = conn.Close()
	}
}
