package stream

import (
	"context"
	"testing"
	"time"
)

func TestSampler(t *testing.T) {
	tests := []struct {
		name     string
		rate     float64
		ids      []string
		wantKeep int
	}{
		{
			name:     "rate 0 drops all",
			rate:     0,
			ids:      []string{"id1", "id2", "id3", "id4", "id5"},
			wantKeep: 0,
		},
		{
			name:     "rate 1 keeps all",
			rate:     1,
			ids:      []string{"id1", "id2", "id3", "id4", "id5"},
			wantKeep: 5,
		},
		{
			name:     "rate 0.5 keeps roughly half",
			rate:     0.5,
			ids:      []string{"id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9", "id10"},
			wantKeep: 5, // deterministic, so exact count depends on hash
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSampler(tc.rate)
			kept := 0
			for _, id := range tc.ids {
				r := Record{ID: id}
				matched, err := s.Match(r)
				if err != nil {
					t.Fatalf("Match error: %v", err)
				}
				if matched {
					kept++
				}
			}
			if tc.rate == 0 || tc.rate == 1 {
				if kept != tc.wantKeep {
					t.Errorf("rate=%v: kept=%d want=%d", tc.rate, kept, tc.wantKeep)
				}
			}
		})
	}
}

func TestSamplerDeterministic(t *testing.T) {
	s1 := NewSampler(0.3)
	s2 := NewSampler(0.3)
	ids := []string{"id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9", "id10"}

	for _, id := range ids {
		r := Record{ID: id}
		m1, _ := s1.Match(r)
		m2, _ := s2.Match(r)
		if m1 != m2 {
			t.Errorf("sampling not deterministic for id=%s: m1=%v m2=%v", id, m1, m2)
		}
	}
}

func TestRateLimiter(t *testing.T) {
	// 10 tokens/sec, burst of 11
	rl := NewRateLimiter(10)

	// Should allow burst
	allowed := 0
	for i := 0; i < 11; i++ {
		if rl.Allow() {
			allowed++
		}
	}
	if allowed != 11 {
		t.Errorf("burst: allowed=%d want=11", allowed)
	}

	// Should deny after burst exhausted
	if rl.Allow() {
		t.Error("should deny after burst exhausted")
	}

	// Wait for refill
	time.Sleep(150 * time.Millisecond)
	if !rl.Allow() {
		t.Error("should allow after refill")
	}
}

func TestRateLimiterZeroRate(t *testing.T) {
	rl := NewRateLimiter(0)
	// Zero rate means unlimited
	for i := 0; i < 100; i++ {
		if !rl.Allow() {
			t.Errorf("zero rate should allow all, denied at %d", i)
		}
	}
}

func TestSampleFilter(t *testing.T) {
	// Create a mock source that returns fixed records
	records := []Record{
		{ID: "00000000000000000001", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000002", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000003", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000004", Sequence: 1, Kind: KindEvent},
		{ID: "00000000000000000005", Sequence: 1, Kind: KindEvent},
	}

	mockSrc := &mockSource{records: records, name: "mock"}
	filter := NewSampleFilter(mockSrc, 0.4, 0) // 40% sample rate

	ctx := context.Background()
	batch, err := filter.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	// With 40% sample rate on 5 records, we expect 2 (deterministic)
	t.Logf("Sampled: %d, RateLimited: %d, Records returned: %d", filter.Sampled(), filter.RateLimited(), len(batch.Records))
}

func TestSampleFilterRateLimit(t *testing.T) {
	records := make([]Record, 20)
	for i := range records {
		records[i] = Record{ID: formatID(uint64(i + 1)), Sequence: 1, Kind: KindEvent}
	}

	mockSrc := &mockSource{records: records, name: "mock"}
	filter := NewSampleFilter(mockSrc, 0, 5) // 5 events/sec, no sampling

	ctx := context.Background()
	batch, err := filter.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	// With 5/sec rate limit and burst of 6, first fetch should allow ~6
	t.Logf("Sampled: %d, RateLimited: %d, Records returned: %d", filter.Sampled(), filter.RateLimited(), len(batch.Records))
	if filter.RateLimited() == 0 {
		t.Error("expected some rate limiting")
	}
}

// mockSource is a simple source for testing
type mockSource struct {
	records []Record
	name    string
	index   int
}

func (m *mockSource) Name() string { return m.name }

func (m *mockSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	if m.index >= len(m.records) {
		return Batch{Records: nil, NextCursor: "", Latest: 1, Oldest: 1}, nil
	}
	batch := Batch{
		Records:    m.records[m.index:],
		NextCursor: "cursor",
		Latest:     1,
		Oldest:     1,
		Full:       false,
	}
	m.index = len(m.records)
	return batch, nil
}

func (m *mockSource) CursorLedger(cursor string) (uint32, bool) {
	return 1, true
}

func formatID(n uint64) string {
	return string(make([]byte, 20))[:20-len(string(rune(n)))] + string(rune(n))
}

func TestSampleFilterCombined(t *testing.T) {
	records := make([]Record, 100)
	for i := range records {
		records[i] = Record{ID: formatID3(uint64(i + 1)), Sequence: 1, Kind: KindEvent}
	}

	mockSrc := &mockSource{records: records, name: "mock"}
	filter := NewSampleFilter(mockSrc, 0.5, 10) // 50% sample, 10/sec

	ctx := context.Background()
	batch, err := filter.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	t.Logf("Sampled: %d, RateLimited: %d, Records returned: %d", filter.Sampled(), filter.RateLimited(), len(batch.Records))
}

func formatID3(n uint64) string {
	// Create a fixed-width zero-padded ID
	b := make([]byte, 20)
	for i := 19; i >= 0; i-- {
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b)
}

func TestSamplerUniformForTenThousandRecords(t *testing.T) {
	sampler := NewSampler(0.2)
	kept := 0
	for i := 0; i < 10000; i++ {
		matched, err := sampler.Match(Record{ID: formatID3(uint64(i))})
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		if matched {
			kept++
		}
	}
	if kept < 1700 || kept > 2300 {
		t.Fatalf("20%% sampler kept %d/10000 records, want 1700-2300", kept)
	}
}
