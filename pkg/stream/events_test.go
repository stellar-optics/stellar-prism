package stream

import (
	"context"
	"reflect"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func TestSplitContractIDsRespectsServerLimit(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	groups := splitContractIDs(ids)
	want := [][]string{{"a", "b", "c", "d", "e"}, {"f", "g", "h", "i", "j"}, {"k"}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups %v, want %v", groups, want)
	}
}

func TestEventIndexMatchesCursorParsing(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want uint32
	}{
		{name: "valid", id: (protocol.Cursor{Ledger: 1, Event: 7}).String(), want: 7},
		{name: "invalid TOID", id: "invalid-7"},
		{name: "invalid event", id: "1-invalid"},
		{name: "extra separator", id: "1-7-extra"},
		{name: "missing separator", id: "17"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := eventIndex(test.id); got != test.want {
				t.Errorf("eventIndex(%q) = %d, want %d", test.id, got, test.want)
			}
		})
	}
}

func TestRecordBufferPoolClearsSlotsWithoutMutatingRecords(t *testing.T) {
	records := getRecordBuffer(1)
	event := &Event{ContractID: "contract"}
	records = append(records, Record{ID: "record", Event: event})

	putRecordBuffer(records)
	if event.ContractID != "contract" {
		t.Fatal("recycling the record buffer mutated the retained event")
	}

	reused := getRecordBuffer(1)
	defer putRecordBuffer(reused)
	if len(reused) != 0 {
		t.Fatalf("reused buffer length = %d, want 0", len(reused))
	}
	if cap(reused) > DefaultPageLimit {
		t.Fatalf("reused buffer capacity = %d, want at most %d", cap(reused), DefaultPageLimit)
	}
}

func TestEventIDSetPoolClearsEntries(t *testing.T) {
	seen := getEventIDSet()
	seen["record"] = struct{}{}
	putEventIDSet(seen)

	reused := getEventIDSet()
	defer putEventIDSet(reused)
	if len(reused) != 0 {
		t.Fatalf("reused ID set length = %d, want 0", len(reused))
	}
}

type eventPoolClient struct {
	rpc.Client
	calls int
}

func (c *eventPoolClient) GetEvents(context.Context, protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	c.calls++
	if c.calls == 1 {
		cursor := (protocol.Cursor{Ledger: 1}).String()
		return protocol.GetEventsResponse{
			Events: []protocol.EventInfo{{
				ID:         cursor,
				Ledger:     1,
				EventType:  protocol.EventTypeContract,
				ContractID: "contract",
				TopicXDR:   []string{"AAAAAQ=="},
				ValueXDR:   "AAAAAgAAAAE=",
			}},
			Cursor: cursor,
		}, nil
	}
	return protocol.GetEventsResponse{}, nil
}

func TestTailerRecordBufferReusePreservesDeliveredRecords(t *testing.T) {
	client := &eventPoolClient{}
	source, err := NewEventSource(client, EventQuery{PageLimit: 1})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	var delivered []Record
	sink := SinkFunc(func(record Record) error {
		delivered = append(delivered, record)
		return nil
	})
	tailer := NewTailer(source, sink, Options{})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered %d records, want 1", len(delivered))
	}
	if delivered[0].Event == nil || delivered[0].Event.ContractID != "contract" {
		t.Fatalf("delivered event = %+v, want retained contract event", delivered[0].Event)
	}
}
