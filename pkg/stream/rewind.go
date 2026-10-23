package stream

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Rewinding is the opposite of advancing: the cursor is stepped back to an
// earlier position so the records in between are processed again.
//
// The rules live here rather than in the CLI so that a library user and the
// command line agree on what "step back" means: how far, from where, and
// what happens when the step reaches past the start of the chain.

const (
	// NominalLedgerInterval is roughly how long a ledger takes to close. It
	// converts a wall-clock rewind distance into ledgers.
	//
	// The conversion is an approximation. Stellar's ledger close time is
	// close to five seconds but not exactly five seconds, so a rewind
	// expressed as a duration lands near the requested point rather than
	// exactly on it.
	NominalLedgerInterval = 5 * time.Second

	// FirstLedger is the earliest ledger a rewind can land on. Ledger
	// sequences start at one, so a rewind that reaches past the beginning of
	// the chain is clamped here rather than at zero, which every stream
	// reads as "unset".
	FirstLedger = 1
)

// RewindUnit names the unit a rewind distance is expressed in.
type RewindUnit string

const (
	// RewindLedgers counts ledgers.
	RewindLedgers RewindUnit = "ledgers"
	// RewindDuration counts wall-clock time, converted to ledgers with
	// NominalLedgerInterval.
	RewindDuration RewindUnit = "duration"
)

// RewindDistance is how far back a rewind reaches.
//
// Exactly one of LedgerCount or Duration carries the distance; Unit says
// which. The zero value is not a valid distance: use ParseRewind to build
// one from a user-supplied string.
type RewindDistance struct {
	// LedgerCount is the number of ledgers to step back. It is used when
	// Unit is RewindLedgers.
	LedgerCount uint32
	// Duration is a wall-clock distance to step back. It is used when Unit
	// is RewindDuration.
	Duration time.Duration
	// Unit says which of the two fields above carries the distance.
	Unit RewindUnit
}

// ParseRewind parses a rewind distance as written by a user.
//
// A bare number is a count of ledgers ("500"), an "l" suffix says so
// explicitly ("500l"), and a duration suffix is a wall-clock distance
// ("45s", "30m", "2h", "1h30m", "7d"). Durations are converted to ledgers
// with NominalLedgerInterval, so they land near the requested point rather
// than exactly on it.
func ParseRewind(value string) (RewindDistance, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return RewindDistance{}, fmt.Errorf("a rewind distance is required")
	}

	distance, err := parseRewind(raw, value)
	if err != nil {
		return RewindDistance{}, err
	}
	// Validate here so a caller never has to check the distance twice, and
	// so a zero or negative distance fails at the point it is written
	// rather than at the point it is used.
	if _, err := distance.Ledgers(); err != nil {
		return RewindDistance{}, fmt.Errorf("invalid rewind distance %q: %w", value, err)
	}
	return distance, nil
}

// parseRewind builds a distance from an already-trimmed value.
func parseRewind(raw, original string) (RewindDistance, error) {
	lower := strings.ToLower(raw)
	if number, ok := strings.CutSuffix(lower, "l"); ok {
		return ledgerDistance(number, original)
	}
	if number, ok := strings.CutSuffix(lower, "d"); ok {
		days, err := strconv.ParseUint(strings.TrimSpace(number), 10, 32)
		if err != nil {
			return RewindDistance{}, fmt.Errorf(
				"invalid rewind distance %q: expected a number of days", original)
		}
		return RewindDistance{
			Duration: time.Duration(days) * 24 * time.Hour,
			Unit:     RewindDuration,
		}, nil
	}
	if duration, err := time.ParseDuration(raw); err == nil {
		return RewindDistance{Duration: duration, Unit: RewindDuration}, nil
	}
	return ledgerDistance(raw, original)
}

// ledgerDistance builds a ledger-count distance from the numeric part of a
// rewind value.
func ledgerDistance(number, original string) (RewindDistance, error) {
	count, err := strconv.ParseUint(strings.TrimSpace(number), 10, 32)
	if err != nil {
		return RewindDistance{}, fmt.Errorf(
			"invalid rewind distance %q: expected a number of ledgers (500) or a duration (30m)",
			original)
	}
	if count == 0 {
		return RewindDistance{}, fmt.Errorf(
			"invalid rewind distance %q: a rewind must step back at least one ledger", original)
	}
	return RewindDistance{LedgerCount: uint32(count), Unit: RewindLedgers}, nil
}

// Ledgers converts the distance into a number of ledgers.
//
// A duration is converted with NominalLedgerInterval and rounded down, but
// never to zero: a distance shorter than one ledger still steps back one
// ledger, because stepping back nothing is not a rewind.
func (d RewindDistance) Ledgers() (uint32, error) {
	switch d.Unit {
	case RewindLedgers:
		if d.LedgerCount == 0 {
			return 0, fmt.Errorf("a rewind distance must be at least one ledger")
		}
		return d.LedgerCount, nil
	case RewindDuration:
		if d.Duration <= 0 {
			return 0, fmt.Errorf("a rewind distance must be positive")
		}
		// A duration is at most a few hundred years, which is fewer ledgers
		// than a uint32 can hold, so the conversion cannot overflow.
		ledgers := uint64(d.Duration / NominalLedgerInterval)
		if ledgers < 1 {
			ledgers = 1
		}
		return uint32(ledgers), nil
	default:
		return 0, fmt.Errorf("unknown rewind unit %q", d.Unit)
	}
}

// String renders the distance the way it would be written on a command line.
func (d RewindDistance) String() string {
	switch d.Unit {
	case RewindLedgers:
		return strconv.FormatUint(uint64(d.LedgerCount), 10) + "l"
	case RewindDuration:
		return d.Duration.String()
	default:
		return "0l"
	}
}

// RewindTarget is where a rewind lands.
type RewindTarget struct {
	// Ledger is the ledger the stream should resume from.
	Ledger uint32
	// Anchor is the position the step was measured from.
	Anchor uint32
	// Back is the number of ledgers actually stepped back. It is less than
	// the requested distance when the step was clamped.
	Back uint32
	// Clamped reports that the step reached past the first ledger and was
	// clamped to it.
	Clamped bool
}

// Target computes where a rewind of this distance lands.
//
// The anchor is the position the step is measured from: the ledger a stream
// reached, or the server's head for a fresh run. A step that would reach
// before the first ledger is clamped to it rather than rejected, because
// "as far back as the chain goes" is a request, not a mistake.
func (d RewindDistance) Target(anchor uint32) (RewindTarget, error) {
	back, err := d.Ledgers()
	if err != nil {
		return RewindTarget{}, err
	}
	if anchor <= FirstLedger {
		return RewindTarget{
			Ledger:  FirstLedger,
			Anchor:  anchor,
			Back:    0,
			Clamped: true,
		}, nil
	}
	if back >= anchor {
		return RewindTarget{
			Ledger:  FirstLedger,
			Anchor:  anchor,
			Back:    anchor - FirstLedger,
			Clamped: true,
		}, nil
	}
	return RewindTarget{
		Ledger: anchor - back,
		Anchor: anchor,
		Back:   back,
	}, nil
}

// RewindRequest asks a stream to step back to an earlier position.
type RewindRequest struct {
	// Distance is how far back to step.
	Distance RewindDistance
	// Anchor is the position the step is measured from. Zero means the
	// stream's own position, which only the tailer knows.
	Anchor uint32
}

// RewindController coordinates a rewind request with a running stream.
//
// A stream's cursor is the position it has reached, and only the tailer's
// goroutine may move it. The controller is the seam between whoever asks
// for the rewind — a command-line flag, a signal handler — and the tailer,
// which applies a pending request before its next fetch.
//
// The zero value is ready to use, and a RewindController is safe for
// concurrent use.
type RewindController struct {
	mu      sync.Mutex
	pending *RewindRequest
	taken   int
}

// NewRewindController returns a controller ready to use.
func NewRewindController() *RewindController { return &RewindController{} }

// Request asks the stream to step back.
//
// A pending request is replaced rather than queued: an operator asking to
// rewind twice wants the latest intent, not two passes over the same
// records.
func (c *RewindController) Request(req RewindRequest) error {
	if _, err := req.Distance.Ledgers(); err != nil {
		return fmt.Errorf("rewind request: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = &req
	return nil
}

// Pending reports whether a rewind is waiting to be applied.
func (c *RewindController) Pending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending != nil
}

// Take removes the pending request, if there is one, and returns it.
//
// The tailer calls it, and the tailer is the only goroutine that moves a
// cursor, so a request is applied exactly once.
func (c *RewindController) Take() (RewindRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil {
		return RewindRequest{}, false
	}
	req := *c.pending
	c.pending = nil
	c.taken++
	return req, true
}

// Taken reports how many requests have been taken from the controller.
func (c *RewindController) Taken() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.taken
}
