package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AggMetric represents a metric to compute in the aggregation.
type AggMetric struct {
	Name  string
	Field string
}

// AggregateSource wraps a Source and emits aggregated metrics over time windows.
type AggregateSource struct {
	source  Source
	window  time.Duration
	metrics []aggMetric

	mu      sync.Mutex
	buckets map[uint64]*aggBucket
}

type aggMetric = AggMetric

type aggBucket struct {
	windowStart time.Time
	windowEnd   time.Time
	data        map[string]*contractAgg
}

type contractAgg struct {
	contractID string
	count      int
	sums       map[string]float64
	counts     map[string]int
	mins       map[string]float64
	maxs       map[string]float64
	uniqueSets map[string]map[string]struct{}
}

func NewAggregateSource(source Source, window time.Duration, metrics []aggMetric) *AggregateSource {
	return &AggregateSource{
		source:  source,
		window:  window,
		metrics: metrics,
		buckets: make(map[uint64]*aggBucket),
	}
}

func (a *AggregateSource) Name() string {
	return "aggregate(" + a.source.Name() + ")"
}

func (a *AggregateSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	// Fetch from underlying source
	batch, err := a.source.Fetch(ctx, cursor)
	if err != nil {
		return batch, err
	}

	// Process records into buckets
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, r := range batch.Records {
		if r.Kind != KindEvent || r.Event == nil {
			continue
		}
		win := a.windowKey(r.Time)
		bucket := a.getBucket(win)
		contract := r.Event.ContractID
		ca := bucket.getContract(contract)

		// Extract field values
		for _, m := range a.metrics {
			switch m.Name {
			case "count":
				ca.count++
			case "sum", "avg", "min", "max":
				val := a.extractNumber(r, m.Field)
				if val != nil {
					ca.addNumber(m.Name, m.Field, *val)
				}
			case "unique_count":
				val := a.extractString(r, m.Field)
				if val != "" {
					ca.addUnique(m.Field, val)
				}
			}
		}
	}

	// Check for completed windows and emit aggregate records
	now := time.Now().UTC()
	var completedWins []uint64
	for win := range a.buckets {
		winEnd := a.windowEnd(win)
		if winEnd.Before(now) || winEnd.Equal(now) {
			completedWins = append(completedWins, win)
		}
	}

	var aggRecords []Record
	for _, win := range completedWins {
		bucket := a.buckets[win]
		for _, ca := range bucket.data {
			for _, m := range a.metrics {
				var value any
				switch m.Name {
				case "count":
					value = ca.count
				case "sum":
					value = ca.sums[m.Field]
				case "avg":
					if ca.counts[m.Field] > 0 {
						value = ca.sums[m.Field] / float64(ca.counts[m.Field])
					}
				case "min":
					value = ca.mins[m.Field]
				case "max":
					value = ca.maxs[m.Field]
				case "unique_count":
					if ca.uniqueSets[m.Field] != nil {
						value = len(ca.uniqueSets[m.Field])
					}
				}
				if value != nil {
					id := fmt.Sprintf("%s-%d-%s-%s", ca.contractID, win, m.Name, m.Field)
					aggRecords = append(aggRecords, Record{
						ID:       id,
						Sequence: uint32(bucket.windowStart.Unix()),
						Time:     bucket.windowStart,
						Kind:     KindEvent, // Reuse KindEvent for aggregate records
						Event: &Event{
							ContractID: ca.contractID,
							Type:       "aggregate",
							TopicXDR:   []string{fmt.Sprintf("%d", win), m.Name, m.Field},
							ValueXDR:   fmt.Sprintf("%v", value),
						},
					})
				}
			}
		}
		delete(a.buckets, win)
	}

	// If no aggregate records ready, return empty batch but keep cursor
	if len(aggRecords) == 0 {
		return Batch{
			Records:    []Record{},
			NextCursor: batch.NextCursor,
			Latest:     batch.Latest, LatestCloseTime: batch.LatestCloseTime,
			Oldest: batch.Oldest,
			Full:   batch.Full,
		}, nil
	}

	return Batch{
		Records:    aggRecords,
		NextCursor: batch.NextCursor,
		Latest:     batch.Latest, LatestCloseTime: batch.LatestCloseTime,
		Oldest: batch.Oldest,
		Full:   batch.Full,
	}, nil
}

func (a *AggregateSource) windowKey(t time.Time) uint64 {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return uint64(t.UnixNano() / int64(a.window))
}

func (a *AggregateSource) windowEnd(win uint64) time.Time {
	return time.Unix(0, int64((win+1)*uint64(a.window))).UTC()
}

func (a *AggregateSource) getBucket(win uint64) *aggBucket {
	if b, ok := a.buckets[win]; ok {
		return b
	}
	start := time.Unix(0, int64(win)*int64(a.window)).UTC()
	end := time.Unix(0, int64(win+1)*int64(a.window)).UTC()
	b := &aggBucket{
		windowStart: start,
		windowEnd:   end,
		data:        make(map[string]*contractAgg),
	}
	a.buckets[win] = b
	return b
}

func (b *aggBucket) getContract(contractID string) *contractAgg {
	if ca, ok := b.data[contractID]; ok {
		return ca
	}
	ca := &contractAgg{
		contractID: contractID,
		sums:       make(map[string]float64),
		counts:     make(map[string]int),
		mins:       make(map[string]float64),
		maxs:       make(map[string]float64),
		uniqueSets: make(map[string]map[string]struct{}),
	}
	b.data[contractID] = ca
	return ca
}

func (ca *contractAgg) addNumber(op, field string, val float64) {
	switch op {
	case "sum":
		ca.sums[field] += val
		ca.counts[field]++
	case "avg":
		ca.sums[field] += val
		ca.counts[field]++
	case "min":
		if ca.counts[field] == 0 || val < ca.mins[field] {
			ca.mins[field] = val
		}
		ca.counts[field]++
	case "max":
		if ca.counts[field] == 0 || val > ca.maxs[field] {
			ca.maxs[field] = val
		}
		ca.counts[field]++
	}
}

func (ca *contractAgg) addUnique(field, val string) {
	if ca.uniqueSets[field] == nil {
		ca.uniqueSets[field] = make(map[string]struct{})
	}
	ca.uniqueSets[field][val] = struct{}{}
}

func (a *AggregateSource) extractNumber(r Record, field string) *float64 {
	// Simple field extraction - in real implementation this would use the same
	// expression evaluation as filter.go
	if r.Event == nil {
		return nil
	}
	// For now, try to parse valueXDR as a number
	// This is a placeholder - real implementation would decode and navigate
	return nil
}

func (a *AggregateSource) extractString(r Record, field string) string {
	if r.Event == nil {
		return ""
	}
	// Placeholder for string extraction
	return ""
}

func (a *AggregateSource) CursorLedger(cursor string) (uint32, bool) {
	return a.source.CursorLedger(cursor)
}
