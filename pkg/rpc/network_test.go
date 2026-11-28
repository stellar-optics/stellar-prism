package rpc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

type networkClient struct{ passphrase string }

func (c networkClient) GetNetwork(context.Context) (protocol.GetNetworkResponse, error) {
	return protocol.GetNetworkResponse{Passphrase: c.passphrase}, nil
}

func TestValidateNetwork(t *testing.T) {
	if err := ValidateNetwork(t.Context(), networkClient{network.TestNetworkPassphrase}, "testnet"); err != nil {
		t.Fatal(err)
	}
	err := ValidateNetwork(t.Context(), networkClient{network.PublicNetworkPassphrase}, "testnet")
	if err == nil || !strings.Contains(err.Error(), "network mismatch") {
		t.Fatalf("error = %v", err)
	}
}

func TestExpectedPassphraseRejectsUnknownPreset(t *testing.T) {
	if _, err := ExpectedPassphrase("other"); err == nil {
		t.Fatal("expected error")
	}
}

type mockEndpointClient struct {
	sequence uint32
	status   string
	err      error
}

func (m *mockEndpointClient) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	if m.err != nil {
		return protocol.GetEventsResponse{}, m.err
	}
	return protocol.GetEventsResponse{}, nil
}

func (m *mockEndpointClient) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return nil, nil
}

func (m *mockEndpointClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	if m.err != nil {
		return protocol.GetLedgersResponse{}, m.err
	}
	return protocol.GetLedgersResponse{}, nil
}

func (m *mockEndpointClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	if m.err != nil {
		return protocol.GetTransactionResponse{}, m.err
	}
	return protocol.GetTransactionResponse{}, nil
}

func (m *mockEndpointClient) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	if m.err != nil {
		return protocol.GetTransactionsResponse{}, m.err
	}
	return protocol.GetTransactionsResponse{}, nil
}

func (m *mockEndpointClient) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	if m.err != nil {
		return protocol.SimulateTransactionResponse{}, m.err
	}
	return protocol.SimulateTransactionResponse{}, nil
}

func (m *mockEndpointClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	if m.err != nil {
		return protocol.GetLatestLedgerResponse{}, m.err
	}
	return protocol.GetLatestLedgerResponse{Sequence: m.sequence}, nil
}

func (m *mockEndpointClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	if m.err != nil {
		return protocol.GetHealthResponse{}, m.err
	}
	return protocol.GetHealthResponse{Status: m.status, LatestLedger: m.sequence}, nil
}

func TestFailoverRpcClient(t *testing.T) {
	clientA := &mockEndpointClient{err: fmt.Errorf("HTTP 500 Internal Server Error"), status: "unhealthy"}
	clientB := &mockEndpointClient{sequence: 12345, status: "healthy"}

	pool, err := NewFailoverRpcClient([]string{"http://rpc1.local", "http://rpc2.local"}, 5*time.Second,
		WithFailoverHealthInterval(0), // disable automatic loop in test, test manual probe
		WithFailoverClientFactory(func(u string) Client {
			if u == "http://rpc1.local" {
				return clientA
			}
			return clientB
		}),
	)
	if err != nil {
		t.Fatalf("NewFailoverRpcClient error: %v", err)
	}
	defer pool.Close()

	// Initial call should fail on clientA and seamlessly succeed on clientB
	resp, err := pool.GetLatestLedger(context.Background())
	if err != nil {
		t.Fatalf("GetLatestLedger failover failed: %v", err)
	}
	if resp.Sequence != 12345 {
		t.Errorf("expected sequence 12345 from secondary, got %d", resp.Sequence)
	}

	endpoints := pool.Endpoints()
	if endpoints[0].Healthy {
		t.Error("expected primary endpoint to be marked unhealthy after 500 error")
	}
	if !endpoints[1].Healthy {
		t.Error("expected secondary endpoint to remain healthy")
	}

	// Now recover clientA
	clientA.err = nil
	clientA.sequence = 12346
	clientA.status = "healthy"

	// Probe endpoints to simulate background recovery
	pool.ProbeEndpoints()

	endpoints = pool.Endpoints()
	if !endpoints[0].Healthy {
		t.Error("expected primary endpoint to be restored to healthy after recovery")
	}
}
