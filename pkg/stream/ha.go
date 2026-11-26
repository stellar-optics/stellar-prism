package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultHAHeartbeat is how often the leader renews its lease when no other
// interval is configured.
//
// Five seconds is a compromise: short enough that a failed leader is
// replaced quickly, long enough that a briefly busy process does not lose
// the lease it is working on.
const DefaultHAHeartbeat = 5 * time.Second

// DefaultHATTL is how long a lease stays valid after its last renewal.
//
// It is three heartbeats. Long enough that a slow renewal does not hand the
// stream to a standby, short enough that a leader which really died is
// replaced within seconds.
const DefaultHATTL = 15 * time.Second

// DefaultHAStandbyPoll is how often a standby asks for the lease when no
// other interval is configured.
//
// The request is a non-blocking lock attempt — a single system call that
// fails at once while the leader is alive — so polling often costs nothing,
// and it is what bounds failover: when the leader process dies the kernel
// drops its lock, and the standby holds the lease within one poll.
const DefaultHAStandbyPoll = 200 * time.Millisecond

// ErrLeaseHeld is returned when the lease is held by another instance.
var ErrLeaseHeld = errors.New("lease: held by another instance")

// LeaseRecord is what the lease file says about its current holder.
//
// Epoch is the fencing token: it increases every time the lease changes
// hands, so a leader that was slow, partitioned or paused can tell that
// somebody else has taken over and step down instead of continuing to emit
// records alongside the new leader.
type LeaseRecord struct {
	// Holder identifies the instance that holds the lease.
	Holder string `json:"holder"`
	// Epoch counts how many times the lease has changed hands.
	Epoch uint64 `json:"epoch"`
	// AcquiredAt is when the holder took the lease.
	AcquiredAt time.Time `json:"acquired_at"`
	// RenewedAt is when the holder last renewed it.
	RenewedAt time.Time `json:"renewed_at"`
	// ExpiresAt is when the lease stops being valid without a renewal.
	ExpiresAt time.Time `json:"expires_at"`
}

// Expired reports whether the lease is no longer valid at now.
func (r LeaseRecord) Expired(now time.Time) bool {
	return !r.ExpiresAt.After(now)
}

// LeaderElectionOptions configures a LeaderElection.
type LeaderElectionOptions struct {
	// Path is the lease file. It is the file the advisory lock is taken on,
	// so it must live on a file system that supports locking — a shared NFS
	// mount, for instance, so every instance sees the same lock.
	//
	// The record of who holds the lease lives beside it, in <path>.lease.
	Path string
	// Holder identifies this instance in the lease record. It defaults to
	// the hostname and process id.
	Holder string
	// Heartbeat is how often the leader renews the lease. Zero uses
	// DefaultHAHeartbeat.
	Heartbeat time.Duration
	// StandbyPoll is how often a standby asks for the lease. Zero uses
	// DefaultHAStandbyPoll. It is capped at Heartbeat: a standby never polls
	// less often than the leader renews.
	StandbyPoll time.Duration
	// TTL is how long the lease stays valid without a renewal. Zero uses
	// DefaultHATTL. A TTL shorter than twice the heartbeat would hand the
	// lease away while the leader was still working on it, so it is raised.
	TTL time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// OnChange, when set, is called whenever this instance gains or loses
	// the lease.
	OnChange func(leader bool)
	// OnError, when set, is called when a lease operation fails.
	OnError func(error)
}

// LeaderElection implements active-passive leader election over a file lease.
//
// Only the leader streams. A standby keeps asking for the lease and takes
// over when the leader lets it: either by exiting, which releases the
// advisory lock immediately, or by ceasing to renew, which lets the lease
// expire.
//
// Two mechanisms cooperate. The advisory lock on the lease file is held for
// the whole of a leader's term, and the kernel releases it the moment the
// process dies — so a crashed leader is replaced at once. The record beside
// it carries an expiry and an epoch, which covers the case the lock cannot:
// a leader that is alive but partitioned, and so neither renews nor exits.
// Its lease expires, a standby fences it by claiming a higher epoch, and the
// old leader notices on its next renewal that it has been replaced and
// stops.
//
// The election owns one goroutine, the heartbeat, which renews the lease
// while this instance leads and takes over the lease when it does not.
type LeaderElection struct {
	opts  LeaderElectionOptions
	clock Clock

	mu     sync.Mutex
	leader bool
	epoch  uint64
	// term is the advisory lock held for the duration of a term. It is nil
	// while this instance is not the leader.
	term *os.File
	// changed is closed and replaced whenever the leadership state changes,
	// so a waiter can be woken without polling.
	changed chan struct{}

	cancel context.CancelFunc
	done   chan struct{}
}

// NewLeaderElection returns an election configured by opts.
//
// The lease file's directory must already exist: creating a directory tree
// behind the operator's back would hide a typo in the path until the first
// attempt to take the lease.
func NewLeaderElection(opts LeaderElectionOptions) (*LeaderElection, error) {
	if opts.Path == "" {
		return nil, errors.New("leader election: a lease file path is required")
	}
	dir := filepath.Dir(opts.Path)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("leader election directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("leader election directory %s is not a directory", dir)
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = DefaultHAHeartbeat
	}
	if opts.TTL <= 0 {
		opts.TTL = DefaultHATTL
	}
	if opts.StandbyPoll <= 0 {
		opts.StandbyPoll = DefaultHAStandbyPoll
	}
	if opts.StandbyPoll > opts.Heartbeat {
		opts.StandbyPoll = opts.Heartbeat
	}
	if opts.TTL < 2*opts.Heartbeat {
		// A lease that expires before the leader could renew it would be
		// handed away while the leader was still working on it.
		opts.TTL = 2 * opts.Heartbeat
	}
	if opts.Holder == "" {
		opts.Holder = defaultHolderID()
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	return &LeaderElection{
		opts:    opts,
		clock:   opts.Clock,
		changed: make(chan struct{}),
	}, nil
}

// defaultHolderID names this instance in the lease record.
func defaultHolderID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "prism"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// Path returns the lease file the election locks.
func (e *LeaderElection) Path() string { return e.opts.Path }

// StandbyPoll returns how often a standby asks for the lease.
func (e *LeaderElection) StandbyPoll() time.Duration { return e.opts.StandbyPoll }

// Holder returns the instance identity written into the lease record.
func (e *LeaderElection) Holder() string { return e.opts.Holder }

// Start begins the heartbeat and returns immediately.
//
// The heartbeat renews the lease while this instance leads, and tries to
// take it when it does not. Starting an election twice is an error.
func (e *LeaderElection) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	e.mu.Lock()
	if e.done != nil {
		e.mu.Unlock()
		return errors.New("leader election: already started")
	}
	loopCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.done = make(chan struct{})
	done := e.done
	e.mu.Unlock()

	go func() {
		defer close(done)
		e.heartbeat(loopCtx)
	}()
	return nil
}

// Close stops the heartbeat and gives up the lease.
//
// It is safe to call more than once, and safe on an election that was never
// started. Releasing the lease marks it expired, so a standby can take over
// immediately instead of waiting for the TTL.
func (e *LeaderElection) Close() error {
	e.mu.Lock()
	cancel := e.cancel
	done := e.done
	e.cancel = nil
	e.done = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return e.Release(context.Background())
}

// heartbeat renews the lease, or takes it, until the context is cancelled.
//
// The leader renews every Heartbeat. A standby runs a non-blocking poll loop
// at the faster StandbyPoll interval: each pass is one lock attempt that
// fails immediately while the leader lives, so the standby never blocks on
// the lock, stays responsive to cancellation, and takes the lease within one
// poll of the leader's lock being released.
func (e *LeaderElection) heartbeat(ctx context.Context) {
	for {
		wait := e.opts.StandbyPoll
		if e.IsLeader() {
			wait = e.opts.Heartbeat
			renewed, err := e.Renew(ctx)
			if err != nil {
				e.reportError(err)
			}
			if !renewed {
				// The lease was taken from us: another instance is the
				// leader now, and this one is a standby again.
				e.reportError(errLeaseFenced)
			}
		} else {
			acquired, err := e.Acquire(ctx)
			if err != nil {
				e.reportError(err)
			}
			if acquired {
				// Leading now: the next pass is a renewal.
				wait = e.opts.Heartbeat
			}
		}

		if err := e.clock.Sleep(ctx, wait); err != nil {
			return
		}
	}
}

// IsLeader reports whether this instance currently holds the lease.
func (e *LeaderElection) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader
}

// Epoch returns the epoch of the term this instance holds, or zero when it
// holds none.
//
// The epoch is the fencing token: it increases every time the lease changes
// hands, so a caller can tell that it is leading a different term than it
// was before, and that whatever it was doing belongs to a term that has
// ended.
func (e *LeaderElection) Epoch() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.epoch
}

// Record returns the lease record as it is written on disk.
//
// The second result is false when no lease has been taken yet.
func (e *LeaderElection) Record() (LeaseRecord, bool) {
	record, err := e.withRecordLock(func() (LeaseRecord, error) {
		return e.readRecordLocked()
	})
	if err != nil {
		return LeaseRecord{}, false
	}
	if record.Holder == "" {
		return LeaseRecord{}, false
	}
	return record, true
}

// Acquire tries to take the lease.
//
// It returns true when this instance is the leader, and false when another
// instance holds a live lease. A lease that has expired is fenced: the
// election claims a higher epoch, which makes the old leader step down at
// its next renewal, and the lock is taken as soon as the old holder
// releases it.
func (e *LeaderElection) Acquire(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	e.mu.Lock()
	if e.leader {
		e.mu.Unlock()
		return true, nil
	}

	term, err := openLeaseFile(e.opts.Path)
	if err != nil {
		e.mu.Unlock()
		return false, err
	}
	if err := lockFile(term, true, false); err != nil {
		_ = term.Close()
		if !errors.Is(err, ErrLeaseHeld) {
			e.mu.Unlock()
			return false, err
		}
		// Somebody holds the term. Their lease may still be live, in which
		// case this instance is a standby; if it has expired, the lease is
		// fenced and the holder is expected to step down.
		_, claimErr := e.claimExpiredLease()
		e.mu.Unlock()
		if claimErr != nil {
			return false, claimErr
		}
		return false, nil
	}

	// The term is ours. Publishing the record with a fresh epoch makes any
	// previous holder's next renewal fail, so exactly one instance is ever
	// told it is the leader.
	record, err := e.publishRecord()
	if err != nil {
		// The term is given back, so the next candidate does not have
		// to wait for it to expire.
		unlockErr := unlockFile(term)
		closeErr := term.Close()
		e.mu.Unlock()
		return false, errors.Join(err, unlockErr, closeErr)
	}
	e.term = term
	e.epoch = record.Epoch
	changed := e.setLeaderLocked(true)
	e.mu.Unlock()

	if changed {
		e.notify(true)
	}
	return true, nil
}

// Renew extends the lease.
//
// It returns false when the lease was taken from this instance, which
// happens when a standby fenced it while it was not renewing.
func (e *LeaderElection) Renew(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	e.mu.Lock()
	if !e.leader {
		e.mu.Unlock()
		return false, nil
	}

	now := e.clock.Now()
	_, err := e.withRecordLock(func() (LeaseRecord, error) {
		current, err := e.readRecordLocked()
		if err != nil {
			return LeaseRecord{}, err
		}
		if current.Epoch > e.epoch {
			// Somebody fenced us. The lease is theirs now.
			return LeaseRecord{}, errLeaseFenced
		}
		renewed := LeaseRecord{
			Holder:     e.opts.Holder,
			Epoch:      e.epoch,
			AcquiredAt: current.AcquiredAt,
			RenewedAt:  now,
			ExpiresAt:  now.Add(e.opts.TTL),
		}
		if err := e.writeRecordLocked(renewed); err != nil {
			return LeaseRecord{}, err
		}
		return renewed, nil
	})
	if errors.Is(err, errLeaseFenced) {
		demoteErr := e.demoteLocked()
		e.mu.Unlock()
		e.notify(false)
		if demoteErr != nil {
			e.reportError(demoteErr)
		}
		return false, nil
	}
	e.mu.Unlock()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Release gives up the lease.
//
// The lease is marked expired and the lock is released, so a standby can
// take over at once rather than waiting for the TTL.
func (e *LeaderElection) Release(ctx context.Context) error {
	e.mu.Lock()
	if !e.leader {
		e.mu.Unlock()
		return nil
	}
	err := e.demoteLocked()
	e.mu.Unlock()
	e.notify(false)
	return err
}

// demoteLocked gives up the lease. The caller holds the lock.
//
// The errors are collected rather than returned one at a time: the lease is
// given up whatever happens, and a caller that could not write the record
// still needs the lock released.
func (e *LeaderElection) demoteLocked() error {
	var errs []error
	if e.term != nil {
		// Marking the lease expired tells the next candidate that the
		// term is over, so it does not have to wait out the TTL.
		_, err := e.withRecordLock(func() (LeaseRecord, error) {
			current, err := e.readRecordLocked()
			if err != nil {
				return LeaseRecord{}, err
			}
			expired := current
			expired.ExpiresAt = e.clock.Now()
			if err := e.writeRecordLocked(expired); err != nil {
				return LeaseRecord{}, err
			}
			return expired, nil
		})
		errs = append(errs, err)
		errs = append(errs, unlockFile(e.term))
		errs = append(errs, e.term.Close())
		e.term = nil
	}
	e.leader = false
	e.epoch = 0
	e.signalLocked()
	return errors.Join(errs...)
}

// Ensure blocks until this instance holds the lease.
//
// It returns false when the context ends first, which is how a standby
// learns that it is being asked to stop. When no heartbeat is running the
// call acquires the lease itself and keeps asking for it, so a caller that
// never started the election still makes progress.
func (e *LeaderElection) Ensure(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		e.mu.Lock()
		leader := e.leader
		changed := e.changed
		started := e.done != nil
		e.mu.Unlock()

		if leader {
			return true, nil
		}
		if !started {
			// Nothing is renewing the lease on this instance's behalf,
			// so the waiting is ours to do: ask for the lease, and ask
			// again until it is free.
			acquired, err := e.Acquire(ctx)
			if err != nil {
				if ctx.Err() != nil {
					// The caller stopped asking between two polls; that
					// is the same answer as a wait that was cut short.
					return false, nil //nolint:nilerr // the wait ended because the caller stopped asking
				}
				return false, err
			}
			if acquired {
				return true, nil
			}
			if err := e.clock.Sleep(ctx, e.opts.StandbyPoll); err != nil {
				return false, nil //nolint:nilerr // the wait ended because the caller stopped asking
			}
			continue
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false, nil
		}
	}
}

// setLeaderLocked updates the leadership flag and reports whether it
// changed. The caller holds the lock.
func (e *LeaderElection) setLeaderLocked(leader bool) bool {
	if e.leader == leader {
		return false
	}
	e.leader = leader
	e.signalLocked()
	return true
}

// signalLocked wakes the waiters. The caller holds the lock.
func (e *LeaderElection) signalLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

// notify tells the caller about a change of leadership.
func (e *LeaderElection) notify(leader bool) {
	if e.opts.OnChange != nil {
		e.opts.OnChange(leader)
	}
}

// reportError tells the caller about a lease failure.
func (e *LeaderElection) reportError(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if e.opts.OnError != nil {
		e.opts.OnError(err)
	}
}

// errLeaseFenced reports that the lease was taken from this instance.
var errLeaseFenced = errors.New("lease: fenced by a newer epoch")

// claimExpiredLease fences an expired lease and reports whether it did.
//
// The claim writes a higher epoch, which the old holder sees on its next
// renewal. It does not take the lock: the old holder still holds it until
// it notices, so the caller retries.
func (e *LeaderElection) claimExpiredLease() (bool, error) {
	now := e.clock.Now()
	claimed := false
	_, err := e.withRecordLock(func() (LeaseRecord, error) {
		current, err := e.readRecordLocked()
		if err != nil {
			return LeaseRecord{}, err
		}
		if !current.Expired(now) {
			return current, nil
		}
		next := LeaseRecord{
			Holder:     e.opts.Holder,
			Epoch:      current.Epoch + 1,
			AcquiredAt: now,
			RenewedAt:  now,
			ExpiresAt:  now.Add(e.opts.TTL),
		}
		if err := e.writeRecordLocked(next); err != nil {
			return LeaseRecord{}, err
		}
		claimed = true
		return next, nil
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// publishRecord writes the lease record for a new term and returns it.
func (e *LeaderElection) publishRecord() (LeaseRecord, error) {
	now := e.clock.Now()
	return e.withRecordLock(func() (LeaseRecord, error) {
		current, err := e.readRecordLocked()
		if err != nil {
			return LeaseRecord{}, err
		}
		next := LeaseRecord{
			Holder:     e.opts.Holder,
			Epoch:      current.Epoch + 1,
			AcquiredAt: now,
			RenewedAt:  now,
			ExpiresAt:  now.Add(e.opts.TTL),
		}
		if err := e.writeRecordLocked(next); err != nil {
			return LeaseRecord{}, err
		}
		return next, nil
	})
}

// leaseRecordPath returns the path of the record that sits beside the lease
// file.
func leaseRecordPath(path string) string { return path + ".lease" }

// openLeaseFile opens the file the advisory lock is taken on.
func openLeaseFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening lease file %s: %w", path, err)
	}
	return f, nil
}

// withRecordLock runs fn while holding the lock on the lease record.
//
// The record is a separate file from the lock, so a standby can read the
// holder and its expiry while the leader holds the term.
func (e *LeaderElection) withRecordLock(fn func() (LeaseRecord, error)) (record LeaseRecord, err error) {
	path := leaseRecordPath(e.opts.Path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return LeaseRecord{}, fmt.Errorf("opening lease record %s: %w", path, err)
	}
	defer func() {
		err = errors.Join(err, unlockFile(f), f.Close())
	}()

	if err := lockFile(f, true, true); err != nil {
		return LeaseRecord{}, err
	}
	return fn()
}

// readRecordLocked reads the lease record. The caller holds the record lock.
func (e *LeaderElection) readRecordLocked() (LeaseRecord, error) {
	path := leaseRecordPath(e.opts.Path)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LeaseRecord{}, nil
		}
		return LeaseRecord{}, fmt.Errorf("reading lease record %s: %w", path, err)
	}
	if len(b) == 0 {
		return LeaseRecord{}, nil
	}
	var record LeaseRecord
	if err := json.Unmarshal(b, &record); err != nil {
		// A record that cannot be read is treated as no record at all: the
		// lease is then expired, which is the safe direction.
		return LeaseRecord{}, nil //nolint:nilerr // an unreadable record is treated as no record
	}
	return record, nil
}

// writeRecordLocked writes the lease record. The caller holds the record
// lock.
func (e *LeaderElection) writeRecordLocked(record LeaseRecord) error {
	path := leaseRecordPath(e.opts.Path)
	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding lease record: %w", err)
	}
	b = append(b, '\n')

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("writing lease record %s: %w", path, err)
	}
	if _, err := f.Write(b); err != nil {
		closeErr := f.Close()
		return errors.Join(fmt.Errorf("writing lease record %s: %w", path, err), closeErr)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing lease record %s: %w", path, err)
	}
	return nil
}
