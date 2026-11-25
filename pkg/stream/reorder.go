package stream

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
)

// DefaultMaxResequenceWindow is the default maximum number of ledgers ahead of
// the next expected ledger that ResequenceBuffer will buffer before reporting
// a missing ledger.
const DefaultMaxResequenceWindow uint32 = 64

// ErrMissingLedger is returned when ResequenceBuffer's buffered ledger span
// exceeds its configured maximum window without receiving the next expected
// consecutive ledger.
var ErrMissingLedger = errors.New("missing ledger in resequence buffer")

// ReorderBuffer holds the records belonging to the active ledger until a
// record from a later ledger proves that the active ledger is complete.
//
// RPC replicas can return pages with a small amount of intra-ledger skew. A
// high-water ID is therefore not safe to apply until the ledger has closed;
// keeping only that one ledger bounded the memory cost while allowing a
// deterministic sort by ledger, transaction, operation and event position.
type ReorderBuffer struct {
	pending []Record
	seen    map[string]struct{}
}

// NewReorderBuffer returns an empty sliding ledger reorder buffer.
func NewReorderBuffer() *ReorderBuffer {
	return &ReorderBuffer{seen: make(map[string]struct{})}
}

// Add accepts records and returns records from ledgers that are now known to
// be complete. Records from the highest ledger remain buffered.
func (b *ReorderBuffer) Add(records []Record) []Record {
	if b.seen == nil {
		b.seen = make(map[string]struct{})
	}
	maxLedger := uint32(0)
	haveLedger := false
	for _, r := range b.pending {
		if !haveLedger || r.Sequence > maxLedger {
			maxLedger = r.Sequence
			haveLedger = true
		}
	}
	for _, r := range records {
		// An absent ID cannot identify a replay. Leave content-based
		// deduplication to downstream sinks in that case.
		if r.ID != "" {
			if _, ok := b.seen[r.ID]; ok {
				continue
			}
			b.seen[r.ID] = struct{}{}
		}
		b.pending = append(b.pending, r)
		if !haveLedger || r.Sequence > maxLedger {
			maxLedger = r.Sequence
			haveLedger = true
		}
	}
	if !haveLedger {
		return nil
	}

	ready := make([]Record, 0, len(b.pending))
	keep := b.pending[:0]
	for _, r := range b.pending {
		if r.Sequence < maxLedger {
			ready = append(ready, r)
			delete(b.seen, r.ID)
		} else {
			keep = append(keep, r)
		}
	}
	b.pending = keep
	sortRecords(ready)
	return ready
}

// Flush returns all records still held by the active ledger, in stream order.
func (b *ReorderBuffer) Flush() []Record {
	ready := append([]Record(nil), b.pending...)
	b.pending = b.pending[:0]
	for _, r := range ready {
		delete(b.seen, r.ID)
	}
	sortRecords(ready)
	return ready
}

func sortRecords(records []Record) {
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
		if a.Kind == KindEvent && b.Kind == KindEvent && a.Event != nil && b.Event != nil {
			if a.Event.TxIndex != b.Event.TxIndex {
				return a.Event.TxIndex < b.Event.TxIndex
			}
			if a.Event.OpIndex != b.Event.OpIndex {
				return a.Event.OpIndex < b.Event.OpIndex
			}
			if a.Event.EventIndex != b.Event.EventIndex {
				return a.Event.EventIndex < b.Event.EventIndex
			}
		}
		if a.Kind == KindOperation && b.Kind == KindOperation && a.Operation != nil && b.Operation != nil {
			if a.Operation.TxIndex != b.Operation.TxIndex {
				return a.Operation.TxIndex < b.Operation.TxIndex
			}
			return a.Operation.Index < b.Operation.Index
		}
		if a.Kind == KindTransaction && b.Kind == KindTransaction && a.Transaction != nil && b.Transaction != nil && a.Transaction.Index != b.Transaction.Index {
			return a.Transaction.Index < b.Transaction.Index
		}
		return a.ID < b.ID
	})
}

func recordLess(a, b Record) bool {
	if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	if a.Kind == KindEvent && b.Kind == KindEvent && a.Event != nil && b.Event != nil {
		if a.Event.TxIndex != b.Event.TxIndex {
			return a.Event.TxIndex < b.Event.TxIndex
		}
		if a.Event.OpIndex != b.Event.OpIndex {
			return a.Event.OpIndex < b.Event.OpIndex
		}
		if a.Event.EventIndex != b.Event.EventIndex {
			return a.Event.EventIndex < b.Event.EventIndex
		}
	}
	if a.Kind == KindOperation && b.Kind == KindOperation && a.Operation != nil && b.Operation != nil {
		if a.Operation.TxIndex != b.Operation.TxIndex {
			return a.Operation.TxIndex < b.Operation.TxIndex
		}
		return a.Operation.Index < b.Operation.Index
	}
	if a.Kind == KindTransaction && b.Kind == KindTransaction && a.Transaction != nil && b.Transaction != nil && a.Transaction.Index != b.Transaction.Index {
		return a.Transaction.Index < b.Transaction.Index
	}
	return a.ID < b.ID
}

type recordMinHeap []Record

func (h recordMinHeap) Len() int           { return len(h) }
func (h recordMinHeap) Less(i, j int) bool { return recordLess(h[i], h[j]) }
func (h recordMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *recordMinHeap) Push(x any)        { *h = append(*h, x.(Record)) }
func (h *recordMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// ResequenceBuffer is a min-heap priority queue ordered by
// (ledgerSeq, txIndex, eventIndex) that buffers out-of-order worker pages
// until the consecutive next ledger is available.
type ResequenceBuffer struct {
	h          recordMinHeap
	seen       map[string]struct{}
	nextLedger uint32
	haveNext   bool
	maxWindow  uint32
	maxSeen    uint32
}

// NewResequenceBuffer returns a ResequenceBuffer expecting startLedger next
// (or the minimum ledger of the first pushed batch if startLedger is 0).
// maxWindow bounds how many ledgers ahead may be buffered before flagging a
// missing ledger; 0 defaults to DefaultMaxResequenceWindow.
func NewResequenceBuffer(startLedger, maxWindow uint32) *ResequenceBuffer {
	if maxWindow == 0 {
		maxWindow = DefaultMaxResequenceWindow
	}
	return &ResequenceBuffer{
		seen:       make(map[string]struct{}),
		nextLedger: startLedger,
		haveNext:   startLedger > 0,
		maxWindow:  maxWindow,
	}
}

// Push inserts records into the min-heap and returns all consecutive ledgers
// starting from the next expected ledger in strictly ascending order.
// Returns ErrMissingLedger if buffered records exceed maxWindow ahead of the
// next expected ledger.
func (b *ResequenceBuffer) Push(records []Record) ([]Record, error) {
	if b.seen == nil {
		b.seen = make(map[string]struct{})
	}
	for _, r := range records {
		if r.ID != "" {
			if _, ok := b.seen[r.ID]; ok {
				continue
			}
			b.seen[r.ID] = struct{}{}
		}
		if r.Sequence > b.maxSeen {
			b.maxSeen = r.Sequence
		}
		heap.Push(&b.h, r)
	}
	if b.h.Len() == 0 {
		return nil, nil
	}
	if !b.haveNext {
		b.nextLedger = b.h[0].Sequence
		b.haveNext = true
	}

	var ready []Record
	for b.h.Len() > 0 {
		top := b.h[0]
		if top.Sequence < b.nextLedger {
			r := heap.Pop(&b.h).(Record)
			delete(b.seen, r.ID)
			ready = append(ready, r)
			continue
		}
		if top.Sequence == b.nextLedger {
			for b.h.Len() > 0 && b.h[0].Sequence == b.nextLedger {
				r := heap.Pop(&b.h).(Record)
				delete(b.seen, r.ID)
				ready = append(ready, r)
			}
			b.nextLedger++
			continue
		}
		break
	}

	if b.h.Len() > 0 && b.maxSeen > b.nextLedger && b.maxSeen-b.nextLedger > b.maxWindow {
		return ready, fmt.Errorf(
			"%w: expected ledger %d, buffered up to %d (window %d)",
			ErrMissingLedger, b.nextLedger, b.maxSeen, b.maxWindow,
		)
	}
	return ready, nil
}

// Flush drains all remaining buffered records in strictly ascending order.
func (b *ResequenceBuffer) Flush() []Record {
	if b.h.Len() == 0 {
		return nil
	}
	ready := make([]Record, 0, b.h.Len())
	for b.h.Len() > 0 {
		r := heap.Pop(&b.h).(Record)
		delete(b.seen, r.ID)
		ready = append(ready, r)
	}
	return ready
}

// Len returns the number of records currently buffered.
func (b *ResequenceBuffer) Len() int {
	return b.h.Len()
}
