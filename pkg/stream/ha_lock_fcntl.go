//go:build solaris || illumos || aix

package stream

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes a POSIX record lock over the whole of f with fcntl.
//
// These systems have no flock(2), so the lease is built on fcntl(F_SETLK)
// instead. The kernel releases the lock when the owning process exits, which
// is what lets a standby take over as soon as the leader dies.
//
// POSIX record locks belong to the process, not the descriptor: two
// descriptors in the same process never conflict. That is harmless here,
// because every prism process holds at most one election, and the lock is
// only ever contended between processes.
func lockFile(f *os.File, exclusive, blocking bool) error {
	lk := unix.Flock_t{
		Type:   unix.F_RDLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0, // zero means "to the end of the file, however long"
	}
	if exclusive {
		lk.Type = unix.F_WRLCK
	}
	cmd := unix.F_SETLK
	if blocking {
		cmd = unix.F_SETLKW
	}
	if err := unix.FcntlFlock(f.Fd(), cmd, &lk); err != nil {
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EACCES) {
			return ErrLeaseHeld
		}
		return fmt.Errorf("locking %s: %w", f.Name(), err)
	}
	return nil
}

// unlockFile releases the record lock on f.
func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	lk := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart}
	if err := unix.FcntlFlock(f.Fd(), unix.F_SETLK, &lk); err != nil {
		return fmt.Errorf("unlocking %s: %w", f.Name(), err)
	}
	return nil
}
