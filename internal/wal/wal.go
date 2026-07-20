package wal

import (
	"bufio"
	"os"
	"sync"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
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
		path: path,
		writer: bufio.NewWriterSize(
			file,
			64*1024,
		),
	}, nil
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

	return w.file.Sync()
}

func (w *WAL) Truncate() error {

	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.writer.Flush()

	if err != nil {
		return err
	}

	err = w.file.Sync()

	if err != nil {
		return err
	}

	err = w.file.Close()

	if err != nil {
		return err
	}

	file, err := os.OpenFile(
		w.path,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)

	if err != nil {
		return err
	}

	w.file = file

	w.writer = bufio.NewWriterSize(
		file,
		64*1024,
	)

	return nil
}
func (w *WAL) Close() error {

    w.mu.Lock()
    defer w.mu.Unlock()

    if err := w.writer.Flush(); err != nil {
        return err
    }

    if err := w.file.Sync(); err != nil {
        return err
    }

    return w.file.Close()
}