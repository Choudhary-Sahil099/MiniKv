package wal

import (
	"os"
)

type WAL struct {
	file *os.File
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
	}, nil
}
func (w *WAL) Write(entry string) error {

	_, err := w.file.WriteString(entry + "\n")

	if err != nil {
		return err
	}

	return w.file.Sync()
}