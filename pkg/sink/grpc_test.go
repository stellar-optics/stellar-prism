package sink_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	prismpb "github.com/stellar-optics/stellar-prism/proto"
)

func TestGRPCSinkStreamsEvents(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(1 << 20)
	grpcSink := sink.NewGRPCSink()
	server := grpc.NewServer()
	prismpb.RegisterPrismStreamServer(server, grpcSink)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		_ = grpcSink.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer conn.Close()

	stream, err := prismpb.NewPrismStreamClient(conn).StreamEvents(ctx, &prismpb.EventRequest{})
	if err != nil {
		t.Fatalf("StreamEvents() error = %v", err)
	}

	record := eventRecord()
	type result struct {
		resp *prismpb.EventResponse
		err  error
	}
	results := make(chan result, 1)
	go func() {
		resp, err := stream.Recv()
		results <- result{resp: resp, err: err}
	}()
	for i := 0; i < 100; i++ {
		if err := grpcSink.Emit(record); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("Recv() error = %v", got.err)
			}
			if got.resp.GetId() != record.ID || got.resp.GetLedger() != record.Sequence {
				t.Fatalf("response = %+v, want id %q ledger %d", got.resp, record.ID, record.Sequence)
			}
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for a streamed event")
}
