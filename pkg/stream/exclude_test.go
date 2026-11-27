package stream

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExcludeFilter(t *testing.T) {
	records := []Record{
		{ID: "00000000000000000001", Sequence: 1, Kind: KindEvent, Event: &Event{ContractID: "CAAA", TopicXDR: []string{"AAAAAQ=="}, ValueXDR: "AAAAAA=="}},
		{ID: "00000000000000000002", Sequence: 1, Kind: KindEvent, Event: &Event{ContractID: "CBBB", TopicXDR: []string{"AAAABg=="}, ValueXDR: "AAAAAA=="}},
		{ID: "00000000000000000003", Sequence: 1, Kind: KindEvent, Event: &Event{ContractID: "CAAA", TopicXDR: []string{"AAAAAQ=="}, ValueXDR: "AAAAAA=="}},
		{ID: "00000000000000000004", Sequence: 1, Kind: KindEvent, Event: &Event{ContractID: "CCCC", TopicXDR: []string{"AAAACg=="}, ValueXDR: "AAAAAA=="}},
	}

	mockSrc := &mockSource{records: records, name: "mock"}

	// Test exclude by contract
	filter, err := NewExcludeFilter(mockSrc, nil, []string{"CBBB"})
	if err != nil {
		t.Fatalf("NewExcludeFilter error: %v", err)
	}

	ctx := context.Background()
	batch, err := filter.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if len(batch.Records) != 3 {
		t.Errorf("expected 3 records after excluding CBBB, got %d", len(batch.Records))
	}
	for _, r := range batch.Records {
		if r.Event.ContractID == "CBBB" {
			t.Errorf("should have excluded CBBB")
		}
	}
	if filter.Excluded() != 1 {
		t.Errorf("expected 1 excluded, got %d", filter.Excluded())
	}

	// Test exclude by topic (base64)
	mockSrc2 := &mockSource{records: records, name: "mock2"}
	filter2, err := NewExcludeFilter(mockSrc2, []string{"AAAAAQ=="}, nil)
	if err != nil {
		t.Fatalf("NewExcludeFilter error: %v", err)
	}

	batch2, err := filter2.Fetch(ctx, "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if len(batch2.Records) != 2 {
		t.Errorf("expected 2 records after excluding AAAAAQ==, got %d", len(batch2.Records))
	}
	if filter2.Excluded() != 2 {
		t.Errorf("expected 2 excluded, got %d", filter2.Excluded())
	}
}

func TestExcludeFilterSymbol(t *testing.T) {
	symbol, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("transfer"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := xdr.MarshalBase64(symbol)
	if err != nil {
		t.Fatal(err)
	}
	source := &mockSource{records: []Record{
		{ID: "excluded", Kind: KindEvent, Event: &Event{TopicXDR: []string{encoded}}},
		{ID: "kept", Kind: KindEvent, Event: &Event{TopicXDR: []string{"AAAAAQ=="}}},
	}, name: "mock"}
	filter, err := NewExcludeFilter(source, []string{"transfer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := filter.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 || batch.Records[0].ID != "kept" {
		t.Fatalf("records = %+v, want only the non-matching topic", batch.Records)
	}
	if filter.Excluded() != 1 {
		t.Fatalf("excluded = %d, want 1", filter.Excluded())
	}
}

func TestBlacklistFilterComposesAfterWhitelist(t *testing.T) {
	symbol, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("transfer"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := xdr.MarshalBase64(symbol)
	if err != nil {
		t.Fatal(err)
	}
	whitelist, err := CompileExpression(`contract == "CAAA"`)
	if err != nil {
		t.Fatal(err)
	}
	blacklist, err := NewBlacklistFilter([]string{"transfer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	filters := FilterSet{whitelist, blacklist}

	tests := []struct {
		name string
		rec  Record
		want bool
	}{
		{
			name: "whitelisted but blacklisted",
			rec:  Record{Kind: KindEvent, Event: &Event{ContractID: "CAAA", TopicXDR: []string{encoded}}},
			want: false,
		},
		{
			name: "not whitelisted",
			rec:  Record{Kind: KindEvent, Event: &Event{ContractID: "CBBB", TopicXDR: []string{encoded}}},
			want: false,
		},
		{
			name: "whitelisted and not blacklisted",
			rec:  Record{Kind: KindEvent, Event: &Event{ContractID: "CAAA", TopicXDR: []string{"AAAAAQ=="}}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := filters.Match(tt.rec)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}
