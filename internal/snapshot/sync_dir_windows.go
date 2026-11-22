//go:build windows

package snapshot

// Windows requires platform-specific APIs for durable directory metadata.
// This fallback does not guarantee rename survival after sudden power loss.
func syncSnapshotDirectory(dir string) error { return nil }
