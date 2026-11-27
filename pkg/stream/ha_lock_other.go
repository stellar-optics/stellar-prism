//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || windows || solaris || illumos || aix)

package stream

import (
	"fmt"
	"os"
	"runtime"
)

// lockFile reports that advisory locking is unavailable.
//
// The lease is built on an advisory lock: without one, two instances could
// both believe they are the leader and both stream. Failing loudly is the
// only safe answer — the alternative, pretending the lease works, would
// produce exactly the duplicate events the lease exists to prevent.
func lockFile(f *os.File, exclusive, blocking bool) error {
	return fmt.Errorf("leader election: advisory file locking is not supported on %s", runtime.GOOS)
}

// unlockFile does nothing: no lock was ever taken.
func unlockFile(f *os.File) error { return nil }
