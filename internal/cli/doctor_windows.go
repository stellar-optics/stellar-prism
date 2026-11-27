//go:build windows

package cli

import (
	"fmt"
	"os"
	"path/filepath"
)

func checkSystemFileDescriptorLimits() (string, bool, error) {
	// On Windows, handle limits are system-managed rather than governed by POSIX rlimit.
	return "file descriptors (Windows OS managed handle limits)", true, nil
}

func checkDirectoryFileLock(dir string) error {
	lockPath := filepath.Join(dir, ".doctor_lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("lock file already exists: %w", err)
		}
		return fmt.Errorf("creating lock file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing lock file: %w", err)
	}
	if err := os.Remove(lockPath); err != nil {
		return fmt.Errorf("removing lock file: %w", err)
	}
	return nil
}
