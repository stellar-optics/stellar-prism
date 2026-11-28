//go:build !windows

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func checkSystemFileDescriptorLimits() (string, bool, error) {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return "", false, fmt.Errorf("unable to read rlimit: %w", err)
	}

	soft := rlimit.Cur
	hard := rlimit.Max

	if soft < 1024 {
		return fmt.Sprintf("file descriptors soft limit %d is low (recommended >= 1024, hard: %d)", soft, hard), false, nil
	}
	return fmt.Sprintf("file descriptors (soft: %d, hard: %d)", soft, hard), true, nil
}

func checkDirectoryFileLock(dir string) error {
	lockPath := filepath.Join(dir, ".doctor_lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("opening lock file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(lockPath)
	}()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("advisory file lock failed: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("unlocking file: %w", err)
	}
	return nil
}
