package sink

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
	prismpb "github.com/stellar-optics/stellar-prism/proto"
)

// startGRPCSink serves a GRPCSink on a loopback port and returns a client.
func startGRPCSink(t *testing.T) (*GRPCSink, *grpc.Server, prismpb.PrismStreamClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := NewGRPCSink()
	server := grpc.NewServer()
	prismpb.RegisterPrismStreamServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s, server, prismpb.NewPrismStreamClient(conn)
}

// waitForSubscribers blocks until n event subscribers are registered, so a
// test does not emit before the stream handler is listening.
func waitForSubscribers(t *testing.T, s *GRPCSink, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := len(s.events)
		s.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d subscribers", n)
}

func grpcTestEvent(id, contract string, ledger uint32) stream.Record {
	return stream.Record{
		ID:       id,
		Sequence: ledger,
		Time:     time.UnixMilli(1_700_000_000_000).UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			Type:       "contract",
			ContractID: contract,
			TopicXDR:   []string{"AAAADwAAAAh0cmFuc2Zlcg=="},
			ValueXDR:   "AAAAAQ==",
		},
	}
}

func TestGRPCSinkStreamsEventsAndFiltersByContract(t *testing.T) {
	t.Parallel()

	s, _, client := startGRPCSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	all, err := client.StreamEvents(ctx, &prismpb.EventRequest{})
	if err != nil {
		t.Fatalf("StreamEvents(all): %v", err)
	}
	only, err := client.StreamEvents(ctx, &prismpb.EventRequest{ContractId: "CB"})
	if err != nil {
		t.Fatalf("StreamEvents(CB): %v", err)
	}
	waitForSubscribers(t, s, 2)

	for _, r := range []stream.Record{
		grpcTestEvent("0000000010-0000000001", "CA", 10),
		grpcTestEvent("0000000010-0000000002", "CB", 10),
	} {
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}

	for _, want := range []string{"0000000010-0000000001", "0000000010-0000000002"} {
		msg, err := all.Recv()
		if err != nil {
			t.Fatalf("all.Recv: %v", err)
		}
		if msg.GetId() != want {
			t.Fatalf("all got id %q, want %q", msg.GetId(), want)
		}
	}

	msg, err := only.Recv()
	if err != nil {
		t.Fatalf("only.Recv: %v", err)
	}
	if msg.GetId() != "0000000010-0000000002" || msg.GetLedger() != 10 ||
		msg.GetClosedAtUnixMs() != 1_700_000_000_000 {
		t.Fatalf("unexpected filtered message: %+v", msg)
	}
	// record_json is the documented `--json --raw` NDJSON line.
	var rec map[string]any
	if err := json.Unmarshal(msg.GetRecordJson(), &rec); err != nil {
		t.Fatalf("record_json is not JSON: %v", err)
	}
	if rec["kind"] != "event" || rec["contract"] != "CB" || rec["valueXdr"] != "AAAAAQ==" {
		t.Fatalf("unexpected record_json: %s", msg.GetRecordJson())
	}
}

// TestGRPCSinkCloseEndsActiveStreams guards the serve shutdown path: closing
// the sink must end every handler so GracefulStop can return while a client
// is still connected.
func TestGRPCSinkCloseEndsActiveStreams(t *testing.T) {
	t.Parallel()

	s, server, client := startGRPCSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := client.StreamEvents(ctx, &prismpb.EventRequest{})
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	waitForSubscribers(t, s, 1)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := events.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Close = %v, want io.EOF", err)
	}

	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop did not return after the sink was closed")
	}
}
