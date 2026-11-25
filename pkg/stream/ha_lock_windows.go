//go:build windows

package stream

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// Windows byte-range locks are mandatory, not advisory: while one handle
// holds a lock over a range, every other handle — even one opened by the same
// process — is refused reads and writes of that range. Locking the file's
// content would therefore stop a standby from reading the lease record the
// leader holds, and stop the leader from rewriting the record it holds itself
// through a second handle.
//
// The lock is instead taken over a single byte far beyond any real end of
// file. Locking past EOF is explicitly allowed, it never overlaps the bytes
// prism reads and writes, and every instance agrees on the same byte, so it
// behaves exactly like the advisory whole-file lock flock gives on Unix.
const (
	lockOffsetLow  uint32 = 0xFFFFFFFE
	lockOffsetHigh uint32 = 0x7FFFFFFF
	lockLength     uint32 = 1
)

// lockFile takes a lock on f with LockFileEx.
//
// The semantics match the Unix implementation: an exclusive lock is held for
// the whole of a leader's term, a shared or exclusive lock guards the short
// critical sections around the lease record, and a non-blocking request that
// finds the lock taken fails at once with ErrLeaseHeld so that a standby can
// poll for the lease instead of waiting on it.
//
// The lock is released by the kernel when the handle is closed — including
// when the owning process is killed — which is what lets a standby take over
// as soon as the leader dies.
func lockFile(f *os.File, exclusive, blocking bool) error {
	var flags uint32
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	if !blocking {
		flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	}
	ol := windows.Overlapped{Offset: lockOffsetLow, OffsetHigh: lockOffsetHigh}
	err := windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, lockLength, 0, &ol)
	if err == nil {
		return nil
	}
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
		return ErrLeaseHeld
	}
	return fmt.Errorf("locking %s: %w", f.Name(), err)
}

// unlockFile releases the lock taken by lockFile.
func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	ol := windows.Overlapped{Offset: lockOffsetLow, OffsetHigh: lockOffsetHigh}
	if err := windows.UnlockFileEx(windows.Handle(f.Fd()), 0, lockLength, 0, &ol); err != nil {
		if errors.Is(err, windows.ERROR_NOT_LOCKED) {
			return nil
		}
		return fmt.Errorf("unlocking %s: %w", f.Name(), err)
	}
	return nil
}
