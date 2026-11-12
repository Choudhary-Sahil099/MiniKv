package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrClosed         = errors.New("wal: closed")
	ErrRecordTooLarge = errors.New("wal: record exceeds configured segment size")
)

const (
	maxBatch            = 1024
	maxBatchBytes       = 4 << 20
	DefaultSegmentBytes = 64 << 20
	walBufferBytes      = 64 << 10
)

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
	activeSize int64
	maxBytes   int64
	writeErr   error

	reqs      chan writeRequest
	closeMu   sync.RWMutex
	closed    bool
	loopDone  chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewWAL(basePath string) (*WAL, error) {
	return NewWALWithSegmentSize(basePath, DefaultSegmentBytes)
}

// NewWALWithSegmentSize opens the newest segment for append. Call Recover
// before constructing a WAL during process startup so an incomplete tail is
// repaired before new records are appended.
func NewWALWithSegmentSize(basePath string, maxSegmentBytes int64) (*WAL, error) {
	if strings.TrimSpace(basePath) == "" {
		return nil, errors.New("wal: base path must not be empty")
	}
	if maxSegmentBytes <= 0 {
		return nil, errors.New("wal: segment size must be positive")
	}

	w := &WAL{basePath: basePath, maxBytes: maxSegmentBytes}
	dir := filepath.Dir(basePath)
	baseName := filepath.Base(basePath)
	prefix := baseName + "."
	maxSeq := 0

	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read WAL directory %q: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}
		seqText := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
		seq, parseErr := strconv.Atoi(seqText)
		if parseErr != nil || seq < 1 {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	// Reuse the latest recovered segment. Starting a new segment on every
	// process restart wastes files and breaks size-based segment lifecycle.
	w.currentSeq = maxSeq
	if w.currentSeq == 0 {
		w.currentSeq = 1
	}
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
		return fmt.Errorf("create WAL directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open WAL segment %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat WAL segment %q: %w", path, err)
	}
	w.activeFile = file
	w.activeSize = info.Size()
	w.writer = bufio.NewWriterSize(file, walBufferBytes)
	return nil
}

func (w *WAL) Write(entry string) error {
	entry = strings.TrimRight(entry, "\r\n")
	if entry == "" {
		return errors.New("wal: empty record")
	}
	if int64(len(entry)+1) > w.maxBytes {
		return ErrRecordTooLarge
	}
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
	batchLimit := int64(maxBatchBytes)
	if w.maxBytes < batchLimit {
		batchLimit = w.maxBytes
	}
	var pending writeRequest
	hasPending := false

	for {
		var first writeRequest
		if hasPending {
			first = pending
			hasPending = false
		} else {
			var ok bool
			first, ok = <-w.reqs
			if !ok {
				return
			}
		}

		batch = append(batch[:0], first)
		batchBytes := int64(len(first.data) + 1)
	collect:
		for len(batch) < maxBatch {
			select {
			case request, ok := <-w.reqs:
				if !ok {
					break collect
				}
				requestBytes := int64(len(request.data) + 1)
				if batchBytes+requestBytes > batchLimit {
					pending = request
					hasPending = true
					break collect
				}
				batch = append(batch, request)
				batchBytes += requestBytes
			default:
				break collect
			}
		}

		err := w.commit(batch)
		for _, request := range batch {
			request.done <- err
		}
	}
}

func (w *WAL) commit(batch []writeRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return w.writeErr
	}

	batchBytes := int64(0)
	for _, request := range batch {
		batchBytes += int64(len(request.data) + 1)
	}
	if w.activeSize > 0 && w.activeSize+batchBytes > w.maxBytes {
		if _, err := w.rotateLocked(); err != nil {
			return err
		}
	}

	startOffset, err := w.activeFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek WAL segment: %w", err)
	}
	for _, request := range batch {
		if _, err := w.writer.WriteString(request.data); err != nil {
			return w.rollbackBatch(startOffset, err)
		}
		if err := w.writer.WriteByte('\n'); err != nil {
			return w.rollbackBatch(startOffset, err)
		}
	}
	if err := w.writer.Flush(); err != nil {
		return w.rollbackBatch(startOffset, err)
	}
	if err := w.activeFile.Sync(); err != nil {
		return w.rollbackBatch(startOffset, err)
	}
	w.activeSize += batchBytes
	return nil
}

func (w *WAL) rollbackBatch(startOffset int64, cause error) error {
	// Discard buffered bytes and restore the segment to the exact boundary
	// preceding this batch. If rollback itself fails, poison the WAL so later
	// requests cannot append behind an uncertain/corrupt tail.
	w.writer.Reset(w.activeFile)
	if err := w.activeFile.Truncate(startOffset); err != nil {
		w.writeErr = errors.Join(cause, fmt.Errorf("truncate failed WAL batch: %w", err))
		return w.writeErr
	}
	if _, err := w.activeFile.Seek(0, io.SeekEnd); err != nil {
		w.writeErr = errors.Join(cause, fmt.Errorf("seek after WAL rollback: %w", err))
		return w.writeErr
	}
	if err := w.activeFile.Sync(); err != nil {
		w.writeErr = errors.Join(cause, fmt.Errorf("sync after WAL rollback: %w", err))
		return w.writeErr
	}
	w.activeSize = startOffset
	w.writer = bufio.NewWriterSize(w.activeFile, walBufferBytes)
	return cause
}

func (w *WAL) Rotate() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.rotateLocked()
}

func (w *WAL) rotateLocked() (int, error) {
	if err := w.writer.Flush(); err != nil {
		return 0, fmt.Errorf("flush WAL before rotation: %w", err)
	}
	if err := w.activeFile.Sync(); err != nil {
		return 0, fmt.Errorf("sync WAL before rotation: %w", err)
	}

	oldSeq := w.currentSeq
	nextSeq := oldSeq + 1
	nextPath := fmt.Sprintf("%s.%06d.log", w.basePath, nextSeq)
	nextFile, err := os.OpenFile(nextPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("open next WAL segment %q: %w", nextPath, err)
	}
	if err := w.activeFile.Close(); err != nil {
		_ = nextFile.Close()
		_ = os.Remove(nextPath)
		return 0, fmt.Errorf("close previous WAL segment: %w", err)
	}

	w.currentSeq = nextSeq
	w.activeFile = nextFile
	w.activeSize = 0
	w.writer = bufio.NewWriterSize(nextFile, walBufferBytes)
	return oldSeq, nil
}

func (w *WAL) PurgeOlderThan(seq int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq >= w.currentSeq {
		return fmt.Errorf("wal: refusing to purge active segment %d or newer", w.currentSeq)
	}
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
		var flushErr, syncErr, closeErr error
		if w.writer != nil {
			flushErr = w.writer.Flush()
		}
		if w.activeFile != nil {
			syncErr = w.activeFile.Sync()
			closeErr = w.activeFile.Close()
		}
		w.closeErr = errors.Join(w.writeErr, flushErr, syncErr, closeErr)
	})
	return w.closeErr
}
