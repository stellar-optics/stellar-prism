package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

type doctorFakeClient struct {
	fakerpc.Server
	healthResp protocol.GetHealthResponse
	healthErr  error
	netResp    protocol.GetNetworkResponse
	netErr     error
}

func (c *doctorFakeClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	if c.healthErr != nil {
		return protocol.GetHealthResponse{}, c.healthErr
	}
	return c.healthResp, nil
}

func (c *doctorFakeClient) GetNetwork(ctx context.Context) (protocol.GetNetworkResponse, error) {
	if c.netErr != nil {
		return protocol.GetNetworkResponse{}, c.netErr
	}
	return c.netResp, nil
}

func (c *doctorFakeClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	return protocol.GetLatestLedgerResponse{
		Sequence: 500,
	}, nil
}

func withDoctorFake(t *testing.T, client *doctorFakeClient) {
	t.Helper()
	orig := newClient
	newClient = func(urls []string, timeout time.Duration) (rpc.Client, error) {
		return client, nil
	}
	t.Cleanup(func() {
		newClient = orig
	})
}

func TestDoctorCommandAllHealthy(t *testing.T) {
	tempDir := t.TempDir()
	stateDir := filepath.Join(tempDir, "state_test")

	client := &doctorFakeClient{
		healthResp: protocol.GetHealthResponse{
			Status:       rpc.HealthStatusHealthy,
			LatestLedger: 500,
		},
		netResp: protocol.GetNetworkResponse{
			Passphrase:      network.TestNetworkPassphrase,
			ProtocolVersion: 22,
		},
	}
	withDoctorFake(t, client)

	stdout, stderr, err := run(t, "doctor",
		"--state-file", stateDir,
		"--network", "testnet",
		"--color", "never",
	)
	if err != nil {
		t.Fatalf("run(doctor) unexpected error: %v (stderr: %s)", err, stderr)
	}

	for _, want := range []string{
		"[✓] RPC Node",
		"healthy",
		"latest ledger: 500",
		"protocol: 22",
		"network: testnet",
		"[!] Sinks: no external sinks configured",
		"[✓] State Directory",
		"readable, writable",
		"[✓] System Limits",
		"All required checks passed.",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout missing expected substring %q\nFull output:\n%s", want, stdout)
		}
	}
}

func TestDoctorCommandNetworkMismatchFails(t *testing.T) {
	tempDir := t.TempDir()
	stateDir := filepath.Join(tempDir, "state_test")

	client := &doctorFakeClient{
		healthResp: protocol.GetHealthResponse{
			Status:       rpc.HealthStatusHealthy,
			LatestLedger: 500,
		},
		netResp: protocol.GetNetworkResponse{
			Passphrase:      network.PublicNetworkPassphrase, // Mismatch against testnet
			ProtocolVersion: 22,
		},
	}
	withDoctorFake(t, client)

	stdout, _, err := run(t, "doctor",
		"--state-file", stateDir,
		"--network", "testnet",
		"--color", "never",
	)
	if err == nil {
		t.Fatalf("run(doctor) expected error on network mismatch, got nil")
	}

	for _, want := range []string{
		"[✗] RPC Node",
		"network passphrase mismatch",
		"Doctor detected 1 or more critical issues.",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout missing expected substring %q\nFull output:\n%s", want, stdout)
		}
	}
}

func TestDoctorCommandRPCUnhealthyFails(t *testing.T) {
	tempDir := t.TempDir()
	stateDir := filepath.Join(tempDir, "state_test")

	client := &doctorFakeClient{
		healthResp: protocol.GetHealthResponse{
			Status: "degraded",
		},
		healthErr: fmt.Errorf("node lagging"),
	}
	withDoctorFake(t, client)

	stdout, _, err := run(t, "doctor",
		"--state-file", stateDir,
		"--color", "never",
	)
	if err == nil {
		t.Fatalf("run(doctor) expected error on unhealthy RPC, got nil")
	}

	if !strings.Contains(stdout, "[✗] RPC Node") {
		t.Errorf("expected [✗] RPC Node failure in stdout:\n%s", stdout)
	}
}

func TestDoctorCommandConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test-prism.yaml")
	configContent := `
rpc-url: https://soroban-testnet.stellar.org
timescale-dsn: postgres://fake:fake@127.0.0.1:5432/nonexistent?sslmode=disable
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o600); err != nil {
		t.Fatal(err)
	}

	client := &doctorFakeClient{
		healthResp: protocol.GetHealthResponse{
			Status:       rpc.HealthStatusHealthy,
			LatestLedger: 100,
		},
		netResp: protocol.GetNetworkResponse{
			Passphrase:      network.TestNetworkPassphrase,
			ProtocolVersion: 22,
		},
	}
	withDoctorFake(t, client)

	stdout, _, err := run(t, "doctor",
		"--config", configPath,
		"--network", "testnet",
		"--color", "never",
	)
	// Because the fake PostgreSQL connection should fail to connect to 127.0.0.1:5432
	if err == nil {
		t.Fatalf("expected error due to failing postgres sink connection, got nil")
	}

	if !strings.Contains(stdout, "Sink (PostgreSQL/TimescaleDB)") {
		t.Errorf("expected PostgreSQL sink to be tested from config file:\n%s", stdout)
	}
}
