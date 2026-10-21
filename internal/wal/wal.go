package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var ErrClosed = errors.New("wal: closed")
const maxBatch = 1024

type writeRequest struct {
	data string
	done chan error
}

type WAL struct {
	activeFile *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	basePath   string
	currentSeq int

	reqs      chan writeRequest
	closeMu   sync.RWMutex 
	closed    bool
	loopDone  chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewWAL(basePath string) (*WAL, error) {
	w := &WAL{
		basePath: basePath,
	}

	dir := filepath.Dir(basePath)
	baseName := filepath.Base(basePath)
	prefix := baseName + "."
	maxSeq := 0

	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) ||
			!strings.HasSuffix(name, ".log") {
			continue
		}

		seqText := strings.TrimSuffix(
			strings.TrimPrefix(name, prefix),
			".log",
		)

		seq, err := strconv.Atoi(seqText)
		if err != nil || seq < 1 {
			continue
		}

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	w.currentSeq = maxSeq + 1

	if err := w.openCurrentSegment(); err != nil {
		return nil, err
	}

	w.reqs = make(chan writeRequest, maxBatch)
	w.loopDone = make(chan struct{})
	go w.commitLoop()

	return w, nil
}

func (w *WAL) openCurrentSegment() error {
	path := fmt.Sprintf("%s.%06d.log", w.basePath, w.currentSeq)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}
	w.activeFile = file
	w.writer = bufio.NewWriterSize(
		file,
		64*1024,
	)
	return nil
}

func (w *WAL) Write(entry string) error {
	entry = strings.TrimRight(entry, "\r\n")

	req := writeRequest{data: entry, done: make(chan error, 1)}

	w.closeMu.RLock()
	if w.closed {
		w.closeMu.RUnlock()
		return ErrClosed
	}
	w.reqs <- req
	w.closeMu.RUnlock()

	return <-req.done
}
func (w *WAL) commitLoop() {
	defer close(w.loopDone)

	batch := make([]writeRequest, 0, maxBatch)

	for first := range w.reqs {
		batch = append(batch[:0], first)

	collect:
		for len(batch) < maxBatch {
			select {
			case r, ok := <-w.reqs:
				if !ok {
					break collect
				}
				batch = append(batch, r)
			default:
				break collect
			}
		}

		err := w.commit(batch)

		for _, r := range batch {
			r.done <- err
		}
	}
}

func (w *WAL) commit(batch []writeRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, r := range batch {
		if _, err := w.writer.WriteString(r.data); err != nil {
			return err
		}
		if err := w.writer.WriteByte('\n'); err != nil {
			return err
		}
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.activeFile.Sync()
}

func (w *WAL) Rotate() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return 0, err
	}

	if err := w.activeFile.Sync(); err != nil {
		return 0, err
	}

	if err := w.activeFile.Close(); err != nil {
		return 0, err
	}

	oldSeq := w.currentSeq
	w.currentSeq++

	if err := w.openCurrentSegment(); err != nil {
		return 0, err
	}

	return oldSeq, nil
}

func (w *WAL) PurgeOlderThan(seq int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 1; i <= seq; i++ {
		path := fmt.Sprintf("%s.%06d.log", w.basePath, i)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func (w *WAL) Close() error {
	w.closeOnce.Do(func() {
		w.closeMu.Lock()
		w.closed = true
		close(w.reqs)
		w.closeMu.Unlock()

		<-w.loopDone

		w.mu.Lock()
		defer w.mu.Unlock()

		if err := w.writer.Flush(); err != nil {
			w.closeErr = err
			return
		}
		if err := w.activeFile.Sync(); err != nil {
			w.closeErr = err
			return
		}
		w.closeErr = w.activeFile.Close()
	})
	return w.closeErr
}
