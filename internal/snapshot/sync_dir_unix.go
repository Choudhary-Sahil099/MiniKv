//go:build !windows

package snapshot

import "os"

// Persist the parent directory entry after snapshot rename.
func syncSnapshotDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
