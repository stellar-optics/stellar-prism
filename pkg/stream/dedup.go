package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// DefaultDedupWindow is the number of event fingerprints remembered by
// --dedup-window when a caller asks for deduplication without naming a size.
//
// One page of events is 200 records, and a re-poll after a reconnect replays
// at most the last page, so a window significantly larger than a page keeps
// the whole replay inside memory while staying fixed and small.
const DefaultDedupWindow = 4096

// Dedup is a fixed-capacity, memory-bounded deduplicator: a ring buffer of
// recently seen keys backed by a set for O(1) lookups.
//
// The ring bounds memory by count, not by elapsed time, which is deliberate.
// A time window would make the memory ceiling depend on traffic, and the
// failure mode of a hot contract would be unbounded growth — exactly what a
// streaming tool must avoid. When the buffer is full, the oldest key is
// evicted before the new one is inserted, so Dedup never holds more than its
// capacity.
//
// See docs/architecture/deduplication_internals.md for fingerprint and
// content-hash computation details, memory sizing formulas, and window
// selection guidelines.
//
// Dedup is not safe for concurrent use. It is driven by a single tailer
// goroutine, and a sink wrapper is emitted from that same goroutine.
type Dedup struct {
	// capacity is the maximum number of keys retained. Zero (or negative)
	// disables deduplication entirely: every key is allowed and nothing is
	// remembered.
	capacity int
	// ring stores the keys in insertion order. The slot at next is the
	// oldest key once the ring has wrapped.
	ring []string
	// seen answers "was this key recorded?" without scanning the ring.
	seen map[string]struct{}
	// next is the slot the next insertion overwrites.
	next int
	// dropped counts duplicate keys refused since construction.
	dropped int
}

// NewDedup returns a Dedup retaining at most capacity keys.
//
// A non-positive capacity returns a usable, disabled Dedup rather than nil,
// so callers can treat deduplication as a flag without a nil check. A zero
// DefaultDedupWindow is not substituted here: choosing a default is the
// caller's decision, and silently enabling deduplication would change
// existing output.
func NewDedup(capacity int) *Dedup {
	if capacity <= 0 {
		return &Dedup{}
	}
	return &Dedup{
		capacity: capacity,
		ring:     make([]string, capacity),
		seen:     make(map[string]struct{}, capacity),
	}
}

// Cap reports the maximum number of keys the Dedup retains. A disabled Dedup
// reports zero.
func (d *Dedup) Cap() int { return d.capacity }

// Len reports how many keys are currently retained.
func (d *Dedup) Len() int {
	if d == nil || d.seen == nil {
		return 0
	}
	return len(d.seen)
}

// Dropped reports how many duplicate keys have been refused.
func (d *Dedup) Dropped() int {
	if d == nil {
		return 0
	}
	return d.dropped
}

// Allow reports whether key is new, and records it when it is.
//
// A key that is already retained returns false and does not disturb its
// insertion position: a replay is the same item, not a newer one. A disabled
// Dedup always returns true.
func (d *Dedup) Allow(key string) bool {
	if d == nil || d.capacity == 0 {
		return true
	}

	if _, ok := d.seen[key]; ok {
		d.dropped++
		return false
	}

	// The slot about to be overwritten holds the oldest key, so evict it
	// before inserting. This is what keeps the set and the ring in step and
	// memory bounded.
	if old := d.ring[d.next]; old != "" {
		delete(d.seen, old)
	}
	d.ring[d.next] = key
	d.seen[key] = struct{}{}
	d.next = (d.next + 1) % d.capacity
	return true
}

// Contains reports whether key is currently retained. It is a diagnostic
// helper; output filtering should use Allow so the window keeps advancing.
func (d *Dedup) Contains(key string) bool {
	if d == nil || d.seen == nil {
		return false
	}
	_, ok := d.seen[key]
	return ok
}

// Reset forgets every retained key while keeping the configured capacity.
func (d *Dedup) Reset() {
	if d == nil {
		return
	}
	if d.seen != nil {
		clear(d.seen)
	}
	clear(d.ring)
	d.next = 0
}

// EventKey derives the de-duplication fingerprint of an event record.
//
// The server's event ID is the identity: a re-poll after a reconnect replays
// the same ID, which is exactly the duplicate to suppress. ID is preferred
// over the topic/value payload because two genuinely distinct events can be
// byte-identical — a contract may emit the same topics and value twice in one
// operation — while their IDs are still different.
//
// A record with no ID (a hand-built source, or a test) falls back to a
// content fingerprint over the event's locator and payload, so such a stream
// still de-duplicates repeated deliveries. The fallback is deliberately
// scoped to records that cannot offer a stable identity.
func EventKey(r Record) string {
	if r.ID != "" {
		return string(r.Kind) + ":id:" + r.ID
	}
	if r.Event == nil {
		return ""
	}

	e := r.Event
	var b strings.Builder
	b.Grow(128)
	b.WriteString("ev:content:")
	b.WriteString(strconv.FormatUint(uint64(r.Sequence), 10))
	b.WriteByte(':')
	b.WriteString(e.ContractID)
	b.WriteByte(':')
	b.WriteString(e.TxHash)
	b.WriteByte(':')
	b.WriteString(strconv.FormatUint(uint64(e.TxIndex), 10))
	b.WriteByte(':')
	b.WriteString(strconv.FormatUint(uint64(e.OpIndex), 10))
	b.WriteByte(':')
	b.WriteString(e.Type)

	// Hash the payload so the retained key is fixed-size regardless of event
	// size; a window of large events would otherwise be bounded by count but
	// not by bytes.
	h := sha256.New()
	for _, topic := range e.TopicXDR {
		_, _ = h.Write([]byte(topic))
		_, _ = h.Write([]byte{0})
	}
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(e.ValueXDR))
	b.WriteByte(':')
	b.WriteString(hex.EncodeToString(h.Sum(nil)))
	return b.String()
}

// DedupSink wraps a Sink and suppresses duplicate event fingerprints.
//
// It sits on the output seam, not inside the tailer, for the same reason
// PatternSink does: the tailer's guarantee is about not losing, skipping or
// repeating records from the *stream*, while this wrapper is about collapsing
// equivalent records that a re-poll produced. Dropping one here still counts
// as delivered, so the cursor advances past it exactly once.
type DedupSink struct {
	inner Sink
	dedup *Dedup
}

// NewDedupSink returns a Sink that emits each event fingerprint at most once
// in a window of capacity. A non-positive capacity returns a wrapper that
// passes every record through, so the caller does not need to special-case a
// disabled flag.
func NewDedupSink(inner Sink, capacity int) *DedupSink {
	return &DedupSink{inner: inner, dedup: NewDedup(capacity)}
}

// Emit implements Sink.
func (s *DedupSink) Emit(r Record) error {
	if key := EventKey(r); key != "" && !s.dedup.Allow(key) {
		return nil
	}
	return s.inner.Emit(r)
}

// EmitBatch implements BatchSink, preserving per-record de-duplication and
// forwarding the survivors in order.
func (s *DedupSink) EmitBatch(records []Record) error {
	survivors := make([]Record, 0, len(records))
	for _, r := range records {
		if key := EventKey(r); key == "" || s.dedup.Allow(key) {
			survivors = append(survivors, r)
		}
	}
	if len(survivors) == 0 {
		return nil
	}
	if batch, ok := s.inner.(BatchSink); ok {
		return batch.EmitBatch(survivors)
	}
	for _, r := range survivors {
		if err := s.inner.Emit(r); err != nil {
			return err
		}
	}
	return nil
}

// Flush implements Sink.
func (s *DedupSink) Flush() error { return s.inner.Flush() }

// Dropped reports how many duplicate events this sink has suppressed.
func (s *DedupSink) Dropped() int { return s.dedup.Dropped() }

// String describes the window for diagnostics.
func (s *DedupSink) String() string {
	return fmt.Sprintf("dedup(window=%d)", s.dedup.Cap())
}
