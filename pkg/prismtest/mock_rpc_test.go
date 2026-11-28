package prismtest_test

import (
	"context"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/prismtest"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func TestMockRPCServer(t *testing.T) {
	mock := prismtest.NewMockRPC()
	mock.SetLedgers(prismtest.MakeLedgers(100, 10))

	server := prismtest.NewMockServer(mock)
	defer server.Close()

	client := rpc.New(server.URL, 5*time.Second)

	// Test getLatestLedger
	resp, err := client.GetLatestLedger(context.Background())
	if err != nil {
		t.Fatalf("GetLatestLedger() error = %v", err)
	}
	if resp.Sequence != 109 {
		t.Errorf("Sequence = %d, want 109", resp.Sequence)
	}

	// Test getHealth
	healthResp, err := client.GetHealth(context.Background())
	if err != nil {
		t.Fatalf("GetHealth() error = %v", err)
	}
	if healthResp.Status != "healthy" {
		t.Errorf("Status = %q, want healthy", healthResp.Status)
	}

	// Test getLedgers
	ledgersResp, err := client.GetLedgers(context.Background(), protocol.GetLedgersRequest{StartLedger: 100})
	if err != nil {
		t.Fatalf("GetLedgers() error = %v", err)
	}
	if len(ledgersResp.Ledgers) == 0 {
		t.Error("expected non-empty ledgers response")
	}

	// Test 500 error simulation
	server.Simulate500(1)
	_, err = client.GetLatestLedger(context.Background())
	if err == nil {
		t.Error("expected error on simulated 500, got nil")
	}

	// Test rate limit 429 simulation
	server.SimulateRateLimit(1)
	_, err = client.GetLatestLedger(context.Background())
	if err == nil {
		t.Error("expected error on simulated 429, got nil")
	}

	// Test retention gap simulation
	mock.SimulateGap(10, 60)
	evResp, err := client.GetEvents(context.Background(), protocol.GetEventsRequest{StartLedger: 10})
	if err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if evResp.OldestLedger != 60 {
		t.Errorf("OldestLedger = %d, want 60", evResp.OldestLedger)
	}
}
