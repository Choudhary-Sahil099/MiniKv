package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"strconv"
	"strings"
)

type WAL struct {
	activeFile *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	basePath   string
	currentSeq int
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

    // fresh segment
    w.currentSeq = maxSeq + 1

    if err := w.openCurrentSegment(); err != nil {
        return nil, err
    }

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
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.writer.WriteString(entry + "\n"); err != nil {
		return err
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
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.activeFile.Sync(); err != nil {
		return err
	}

	return w.activeFile.Close()
}