package stream

import (
	"context"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultLedgerPageLimit is the page size requested for ledger streams.
// Ledger pages are much heavier than event pages, so the default is smaller.
const DefaultLedgerPageLimit = 50

// LedgerQuery describes which ledgers to stream.
type LedgerQuery struct {
	// Consensus enables signed-value diagnostics and consecutive close deltas.
	Consensus bool
	// ExpectedValidators are expected value signers, not a quorum definition.
	ExpectedValidators []string
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
}

// LedgerSource streams ledger close metadata from RPC.
type LedgerSource struct {
	client           rpc.Client
	query            LedgerQuery
	previousSequence uint32
	previousTime     uint64
	cached           map[uint32]*ConsensusMetrics
}

// NewLedgerSource returns a Source over the given RPC client.
func NewLedgerSource(client rpc.Client, q LedgerQuery) *LedgerSource {
	if q.PageLimit == 0 {
		q.PageLimit = DefaultLedgerPageLimit
	}
	return &LedgerSource{client: client, query: q}
}

// Name implements Source.
func (s *LedgerSource) Name() string { return "ledgers" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *LedgerSource) Rewind(ledger uint32) {
	s.query.StartLedger = ledger
	s.previousSequence = 0
	s.cached = nil
}

// CursorLedger implements Source. The getLedgers cursor is the ledger
// sequence in decimal.
func (s *LedgerSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// Fetch implements Source.
//
// As with events, the server treats a cursor and a start ledger as
// alternatives, so the first call uses StartLedger and later ones the cursor.
func (s *LedgerSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	req := protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: s.query.PageLimit,
		},
	}
	if cursor == "" {
		req.StartLedger = s.query.StartLedger
	} else {
		req.Pagination.Cursor = cursor
	}

	resp, err := s.client.GetLedgers(ctx, req)
	if err != nil {
		return Batch{}, err
	}

	batch := Batch{
		Records:         make([]Record, 0, len(resp.Ledgers)),
		NextCursor:      resp.Cursor,
		Latest:          resp.LatestLedger,
		LatestCloseTime: unixCloseTime(resp.LatestLedgerCloseTime),
		Oldest:          resp.OldestLedger,
		Full:            uint(len(resp.Ledgers)) >= s.query.PageLimit,
	}
	for _, l := range resp.Ledgers {
		r := ledgerRecord(l)
		if s.query.Consensus {
			m, ok := s.cached[l.Sequence]
			if !ok {
				m = consensusMetrics(l.LedgerHeader)
				if m.Error == "" {
					if s.previousSequence != 0 && s.previousSequence+1 == l.Sequence {
						delta := int64(m.CloseTime) - int64(s.previousTime)
						m.CloseDeltaSeconds = &delta
						if delta > 10 {
							m.Warnings = append(m.Warnings, "consensus latency spike: close delta exceeds 10s")
						}
						if delta < 0 {
							m.Warnings = append(m.Warnings, "ledger close time moved backwards")
						}
					}
					for _, expected := range s.query.ExpectedValidators {
						found := false
						for _, signer := range m.ValueSigners {
							if expected == signer {
								found = true
							}
						}
						if !found {
							m.Warnings = append(m.Warnings, "expected value signer not observed: "+expected)
						}
					}
					if l.Sequence > s.previousSequence {
						s.previousSequence = l.Sequence
						s.previousTime = m.CloseTime
					}
				}
			}
			r.Close.Consensus = m
		}
		batch.Records = append(batch.Records, r)
	}
	if s.query.Consensus {
		s.cached = make(map[uint32]*ConsensusMetrics, len(batch.Records))
		for _, r := range batch.Records {
			s.cached[r.Sequence] = r.Close.Consensus
		}
	}
	return batch, nil
}

// ledgerRecord converts an RPC ledger into a neutral Record.
//
// The ID is the zero-padded sequence, which keeps the lexicographic ordering
// the tailer relies on for de-duplication. Ten digits comfortably covers the
// uint32 sequence space.
func ledgerRecord(l protocol.LedgerInfo) Record {
	return Record{
		ID:       fmt.Sprintf("%010d", l.Sequence),
		Sequence: l.Sequence,
		Time:     time.Unix(l.LedgerCloseTime, 0).UTC(),
		Kind:     KindLedger,
		Close: &LedgerClose{
			Hash:        l.Hash,
			HeaderXDR:   l.LedgerHeader,
			MetadataXDR: l.LedgerMetadata,
		},
	}
}
