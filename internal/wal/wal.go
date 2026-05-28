package wal

import (
	"os"
	"bufio"
	"time"
	"sync"
)

type WAL struct {
	file *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}
func NewWAL(path string) (*WAL, error) {

	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		writer: bufio.NewWriterSize(
			file,
			64*1024,
		),
	}, nil
}
func (w *WAL) Write(entry string) error {

	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.writer.WriteString(
		entry + "\n",
	)

	return err
}
func (w *WAL) StartFlushLoop() {

	ticker := time.NewTicker(
		10 * time.Millisecond,
	)

	for range ticker.C {

		w.mu.Lock()

		w.writer.Flush()
		w.file.Sync()

		w.mu.Unlock()
	}
}