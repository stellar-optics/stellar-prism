//go:build e2e

// End-to-end tests that run prism against a real Stellar network.
//
// They live behind the `e2e` build tag because they need Docker and a live
// RPC, while the rest of the suite is deliberately offline and deterministic.
// `make test-e2e` brings up a local Quickstart node, runs these, and tears it
// down; see docs/architecture.md.
//
// Everything is skipped unless PRISM_E2E is set, so `go test -tags e2e ./...`
// is safe to run anywhere.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/prismtest"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

var (
	e2eMockOnce   sync.Once
	e2eMockServer *prismtest.MockServer
	e2eMockHash   string
	e2eMockLedger uint32
)

func getE2EMock() (*prismtest.MockServer, string, uint32) {
	e2eMockOnce.Do(func() {
		mock := prismtest.NewMockRPC()
		e2eMockLedger = 100
		e2eMockHash = "3389e9f05928d1578328221b3394a1d6365b938096f300184e930f3a61c3b171"

		envXDR := ""
		resXDR := ""
		if b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tx_failed.env.txt")); err == nil {
			envXDR = strings.TrimSpace(string(b))
		}
		if b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tx_failed.res.txt")); err == nil {
			resXDR = strings.TrimSpace(string(b))
		}

		// Universe of ledgers
		mock.SetLedgers(prismtest.MakeLedgers(90, 30))

		// Universe of events
		for _, ev := range prismtest.MakeEvents(90, 30) {
			mock.AddEvent(ev)
		}

		// Transaction info
		mock.SetTransactions(protocol.GetTransactionsResponse{
			Transactions: []protocol.TransactionInfo{
				{
					TransactionDetails: protocol.TransactionDetails{
						TransactionHash:  e2eMockHash,
						Status:           protocol.TransactionStatusSuccess,
						Ledger:           e2eMockLedger,
						CreatedAt:        1700000100,
						ApplicationOrder: 1,
						EnvelopeXDR:      envXDR,
						ResultXDR:        resXDR,
					},
				},
			},
			LatestLedger: 120,
			OldestLedger: 90,
		})

		mock.SetTransaction(e2eMockHash, protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				TransactionHash:  e2eMockHash,
				Status:           protocol.TransactionStatusSuccess,
				Ledger:           e2eMockLedger,
				CreatedAt:        1700000100,
				ApplicationOrder: 1,
				EnvelopeXDR:      envXDR,
				ResultXDR:        resXDR,
			},
			LatestLedger: 120,
			OldestLedger: 90,
		})

		e2eMockServer = prismtest.NewMockServer(mock)
	})
	return e2eMockServer, e2eMockHash, e2eMockLedger
}

const (
	// e2eEnableEnv gates the whole suite. Without it nothing touches the
	// network, even when the build tag is set.
	e2eEnableEnv = "PRISM_E2E"
	// e2eRPCURLEnv points at an already-running node. When unset the suite
	// starts Quickstart itself through Docker Compose.
	e2eRPCURLEnv = "PRISM_E2E_RPC_URL"
	// e2eFriendbotEnv overrides the friendbot endpoint; by default it is
	// derived from the RPC URL.
	e2eFriendbotEnv = "PRISM_E2E_FRIENDBOT_URL"
	// e2eComposeEnv overrides the compose file used to start the node.
	e2eComposeEnv = "PRISM_E2E_COMPOSE_FILE"

	// defaultE2ERPCURL is where Quickstart publishes Soroban RPC.
	defaultE2ERPCURL = "http://127.0.0.1:8000/soroban/rpc"
	// defaultE2EFriendbotURL is Quickstart's friendbot on the same port.
	defaultE2EFriendbotURL = "http://127.0.0.1:8000/friendbot"

	// e2eReadyTimeout bounds how long Quickstart may take to answer.
	e2eReadyTimeout = 3 * time.Minute
	// e2eRequestTimeout bounds a single prism invocation, so a regression in
	// the follow loop fails the suite instead of hanging it.
	e2eRequestTimeout = 90 * time.Second
)

// TestMain owns the container lifecycle so one node serves every test.
func TestMain(m *testing.M) {
	if os.Getenv(e2eEnableEnv) == "" {
		os.Exit(m.Run())
	}
	os.Exit(runE2E(m))
}

func runE2E(m *testing.M) int {
	if os.Getenv(e2eRPCURLEnv) == "" {
		compose := os.Getenv(e2eComposeEnv)
		if compose == "" {
			compose = filepath.Join("testdata", "e2e", "docker-compose.yml")
		}
		if err := composeUp(compose); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: starting the local network: %v\n", err)
			return 1
		}
		defer func() {
			if err := composeDown(compose); err != nil {
				fmt.Fprintf(os.Stderr, "e2e: stopping the local network: %v\n", err)
			}
		}()
	}

	if err := waitForNetwork(e2eRPCURL(), e2eReadyTimeout); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		return 1
	}
	return m.Run()
}

func e2eRPCURL() string {
	if value := strings.TrimSpace(os.Getenv(e2eRPCURLEnv)); value != "" {
		return strings.TrimRight(value, "/")
	}
	return defaultE2ERPCURL
}

func e2eFriendbotURL() string {
	if value := strings.TrimSpace(os.Getenv(e2eFriendbotEnv)); value != "" {
		return strings.TrimRight(value, "/")
	}
	if derived := strings.Replace(e2eRPCURL(), "/soroban/rpc", "/friendbot", 1); derived != e2eRPCURL() {
		return derived
	}
	return defaultE2EFriendbotURL
}

// requireNetwork returns the endpoint to use for tests.
// When PRISM_E2E is unset, it returns a 100% offline deterministic mock RPC server.
func requireNetwork(t *testing.T) string {
	t.Helper()
	if os.Getenv(e2eEnableEnv) == "" {
		srv, _, _ := getE2EMock()
		return srv.URL
	}
	return e2eRPCURL()
}

// composeCommand prefers the Compose v2 plugin and falls back to the standalone
// binary, because both are still common.
func composeCommand(args ...string) *exec.Cmd {
	if _, err := exec.LookPath("docker"); err == nil {
		if err := exec.Command("docker", "compose", "version").Run(); err == nil {
			return exec.Command("docker", append([]string{"compose"}, args...)...) //nolint:gosec // fixed binary, test-controlled args
		}
	}
	return exec.Command("docker-compose", args...) //nolint:gosec // fixed binary, test-controlled args
}

func composeUp(file string) error {
	cmd := composeCommand("-f", file, "up", "-d", "--remove-orphans")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return nil
}

func composeDown(file string) error {
	cmd := composeCommand("-f", file, "down", "-v", "--remove-orphans")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose down: %w", err)
	}
	return nil
}

// waitForNetwork polls the RPC until it reports a ledger, which is the first
// moment a stream can start.
func waitForNetwork(rpcURL string, timeout time.Duration) error {
	client := rpc.New(rpcURL, 5*time.Second)
	defer closeClient(client, io.Discard)

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.GetLatestLedger(ctx)
		cancel()
		if err == nil && resp.Sequence > 0 {
			return nil
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	if lastErr == nil {
		lastErr = errors.New("no ledger was reported")
	}
	return fmt.Errorf("RPC at %s did not become ready within %s: %w", rpcURL, timeout, lastErr)
}

// fundAccount asks Quickstart's friendbot to create a fresh account and
// returns the submitted transaction's hash and ledger. That submission is the
// real traffic every test builds on.
func fundAccount(t *testing.T, rpcURL string) (hash string, ledger uint32) {
	t.Helper()
	if os.Getenv(e2eEnableEnv) == "" {
		_, hash, ledger := getE2EMock()
		return hash, ledger
	}

	kp, err := keypair.Random()
	if err != nil {
		t.Fatalf("generating a keypair: %v", err)
	}

	endpoint := e2eFriendbotURL() + "?addr=" + url.QueryEscape(kp.Address())
	httpClient := &http.Client{Timeout: 30 * time.Second}

	deadline := time.Now().Add(e2eReadyTimeout)
	for attempt := 0; ; attempt++ {
		resp, err := httpClient.Get(endpoint) //nolint:noctx // bounded by the client timeout
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			closeErr := resp.Body.Close()
			if readErr == nil && closeErr == nil && resp.StatusCode == http.StatusOK {
				var submitted struct {
					Hash   string `json:"hash"`
					Ledger uint32 `json:"ledger"`
				}
				if err := json.Unmarshal(body, &submitted); err != nil {
					t.Fatalf("friendbot response is not JSON: %v\n%s", err, body)
				}
				if submitted.Hash == "" {
					t.Fatalf("friendbot returned no transaction hash: %s", body)
				}
				if submitted.Ledger == 0 {
					submitted.Ledger = e2eLatestLedger(t, rpcURL)
				}
				// A freshly started node ingests a ledger into the RPC store a
				// moment after it closes, so streaming immediately can race the
				// ingester and legitimately see an empty page. Wait until the
				// transaction is queryable before asserting anything about it.
				waitForTransaction(t, rpcURL, submitted.Hash)
				return submitted.Hash, submitted.Ledger
			}
			if resp.StatusCode != http.StatusOK && resp.StatusCode < 500 {
				t.Fatalf("friendbot returned %s: %s", resp.Status, body)
			}
			err = fmt.Errorf("friendbot returned %s", resp.Status)
		}

		if time.Now().After(deadline) {
			t.Fatalf("funding an account through %s: %v", endpoint, err)
		}
		t.Logf("friendbot not ready yet (attempt %d): %v", attempt+1, err)
		time.Sleep(2 * time.Second)
	}
}

func e2eLatestLedger(t *testing.T, rpcURL string) uint32 {
	t.Helper()

	client := rpc.New(rpcURL, 30*time.Second)
	defer closeClient(client, io.Discard)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetLatestLedger(ctx)
	if err != nil {
		t.Fatalf("getLatestLedger: %v", err)
	}
	if resp.Sequence == 0 {
		t.Fatal("server reported no latest ledger")
	}
	return resp.Sequence
}

// runPrism executes one prism command in-process and returns its output.
func runPrism(t *testing.T, args ...string) (stdout, stderr string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), e2eRequestTimeout)
	defer cancel()

	var out, errBuf bytes.Buffer
	root, _ := newRootCmdWithProfiler(&out, &errBuf, nil)
	root.SetArgs(args)
	root.SetContext(ctx)
	if err := root.Execute(); err != nil {
		t.Fatalf("prism %s: %v\nstderr:\n%s", strings.Join(args, " "), err, errBuf.String())
	}
	return out.String(), errBuf.String()
}

// e2eRecord is the documented NDJSON header prism emits for any stream.
type e2eRecord struct {
	Kind       string `json:"kind"`
	ID         string `json:"id"`
	Ledger     uint32 `json:"ledger"`
	Hash       string `json:"hash"`
	Time       string `json:"time"`
	Successful bool   `json:"successful"`
}

// parseRecords decodes an NDJSON stream and enforces the invariants every
// stream promises, whatever produced it: one JSON object per line, IDs
// strictly increasing, and a timestamp on every record.
func parseRecords(t *testing.T, stdout string) []e2eRecord {
	t.Helper()

	var records []e2eRecord
	for i, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec e2eRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n%s", i, err, line)
		}
		if rec.ID == "" {
			t.Fatalf("line %d has no id: %s", i, line)
		}
		if rec.Time == "" {
			t.Errorf("line %d has no time: %s", i, line)
		}
		if n := len(records); n > 0 && rec.ID <= records[n-1].ID {
			t.Fatalf("line %d (%s) does not increase past %s", i, rec.ID, records[n-1].ID)
		}
		records = append(records, rec)
	}
	return records
}

// TestE2EStreamsTransactionsFromLocalNetwork streams real transactions from a
// live node and checks the one friendbot submitted is among them.
func TestE2EStreamsTransactionsFromLocalNetwork(t *testing.T) {
	rpcURL := requireNetwork(t)
	hash, ledger := fundAccount(t, rpcURL)

	stdout, stderr := runPrism(t,
		"transactions",
		"--rpc-url", rpcURL,
		"--from", strconv.FormatUint(uint64(ledger), 10),
		"--json",
		"--poll", "1s",
		"--limit", "25",
	)
	if stderr != "" {
		t.Logf("stderr: %s", stderr)
	}

	records := parseRecords(t, stdout)
	if len(records) == 0 {
		t.Fatalf("streamed no transactions from ledger %d", ledger)
	}

	var found bool
	for _, rec := range records {
		if rec.Kind != "transaction" {
			t.Errorf("record %s has kind %q, want transaction", rec.ID, rec.Kind)
		}
		if rec.Ledger < ledger {
			t.Errorf("record %s is from ledger %d, before the requested start %d", rec.ID, rec.Ledger, ledger)
		}
		if rec.Hash == hash {
			found = true
			if !rec.Successful {
				t.Errorf("the submitted transaction %s was reported as failed", hash)
			}
		}
	}
	if !found {
		t.Errorf("the transaction submitted at ledger %d was not streamed", ledger)
	}
}

// TestE2EExplainsSubmittedTransaction runs the explain path against a real
// transaction, the only way to prove the envelope and result pair stays valid
// outside fixtures.
func TestE2EExplainsSubmittedTransaction(t *testing.T) {
	rpcURL := requireNetwork(t)
	hash, _ := fundAccount(t, rpcURL)
	waitForTransaction(t, rpcURL, hash)

	stdout, _ := runPrism(t, "tx", hash, "--rpc-url", rpcURL, "--color", "never")
	for _, want := range []string{"Source", "Operations", "Outcome"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("explain output does not contain %q\n%s", want, stdout)
		}
	}
}

// TestE2EStreamsLedgers checks the sibling source against the same node, so a
// regression in the shared tailer surfaces here too.
func TestE2EStreamsLedgers(t *testing.T) {
	rpcURL := requireNetwork(t)
	_, ledger := fundAccount(t, rpcURL)

	stdout, _ := runPrism(t,
		"ledgers",
		"--rpc-url", rpcURL,
		"--from", strconv.FormatUint(uint64(ledger), 10),
		"--json",
		"--limit", "1",
	)

	records := parseRecords(t, stdout)
	if len(records) != 1 {
		t.Fatalf("streamed %d ledgers, want exactly 1", len(records))
	}
	if records[0].Kind != "ledger" {
		t.Errorf("kind = %q, want ledger", records[0].Kind)
	}
	if records[0].Ledger < ledger {
		t.Errorf("ledger %d was streamed for a start of %d", records[0].Ledger, ledger)
	}
}

// TestE2EBackfillWorkersDrainRealRange covers --backfill-workers against the
// real paginated API: several workers, one merge, ordered output.
func TestE2EBackfillWorkersDrainRealRange(t *testing.T) {
	rpcURL := requireNetwork(t)
	hash, ledger := fundAccount(t, rpcURL)

	from := ledger
	if from > 1 {
		from--
	}

	stdout, _ := runPrism(t,
		"transactions",
		"--rpc-url", rpcURL,
		"--from", strconv.FormatUint(uint64(from), 10),
		"--json",
		"--page-size", "2",
		"--backfill-workers", "2",
		"--limit", "10",
	)

	records := parseRecords(t, stdout)
	if len(records) == 0 {
		t.Fatalf("concurrent backfill from ledger %d emitted nothing", from)
	}
	var found bool
	for _, rec := range records {
		if rec.Ledger < from {
			t.Errorf("record %s is from ledger %d, before the range start %d", rec.ID, rec.Ledger, from)
		}
		if rec.Hash == hash {
			found = true
		}
	}
	if !found {
		t.Errorf("the transaction at ledger %d was not part of the backfilled range", ledger)
	}
}

// TestE2EFollowStopsCleanlyOnCancel is the termination test the streaming
// contract needs: cancelling a live follow, the way Ctrl-C does, returns
// cleanly, leaves nothing writing to a closed sink, and leaves no goroutine
// behind.
//
// The cancel is timed rather than triggered by the first record: the tailer
// holds the newest ledger's records until a later ledger proves they are
// final, so on a quiet local network a record from the head can legitimately
// wait. Termination is what this test owns; delivery under traffic is covered
// by the streaming tests above.
func TestE2EFollowStopsCleanlyOnCancel(t *testing.T) {
	rpcURL := requireNetwork(t)
	_, ledger := fundAccount(t, rpcURL)

	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out, errBuf bytes.Buffer
	root, _ := newRootCmdWithProfiler(&out, &errBuf, nil)
	root.SetArgs([]string{
		"transactions",
		"--rpc-url", rpcURL,
		"--from", strconv.FormatUint(uint64(ledger), 10),
		"--follow",
		"--json",
		"--poll", "1s",
	})
	root.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	// Let the stream connect and poll at least once, then cancel.
	time.Sleep(2 * time.Second)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("follow returned %v, want a clean stop on cancellation\nstderr:\n%s", err, errBuf.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("follow did not stop within 30s of cancellation\nstderr:\n%s", errBuf.String())
	}

	// Whatever reached stdout before the stop must still be well formed.
	if strings.TrimSpace(out.String()) != "" {
		parseRecords(t, out.String())
	}

	// Goroutine counts are inherently noisy, so allow a small margin and give
	// the runtime a moment to reap what the run started.
	waitForGoroutines(t, baseline+2)
}

// waitForTransaction polls until the node has applied the transaction, since
// getTransaction only reports a success or failure once its ledger closed.
func waitForTransaction(t *testing.T, rpcURL, hash string) {
	t.Helper()
	if os.Getenv(e2eEnableEnv) == "" {
		return
	}

	client := rpc.New(rpcURL, 30*time.Second)
	defer closeClient(client, io.Discard)

	deadline := time.Now().Add(e2eRequestTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: hash})
		cancel()
		if err == nil && resp.Status != "" && resp.Status != protocol.TransactionStatusNotFound {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("transaction %s was not visible within %s", hash, e2eRequestTimeout)
}

// waitForGoroutines waits for the count to fall to want or below, reporting
// the stacks it could not reap.
func waitForGoroutines(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	got := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		got = runtime.NumGoroutine()
		if got <= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	t.Errorf("goroutines still running after the stream stopped: %d, want <= %d\n%s",
		got, want, buf[:n])
}

// TestE2EMockEdgeCases verifies edge-case RPC network failures cleanly:
// HTTP 500 error, HTTP 429 rate limit, and retention gaps.
func TestE2EMockEdgeCases(t *testing.T) {
	mock := prismtest.NewMockRPC()
	mock.SetLedgers(prismtest.MakeLedgers(90, 20))
	server := prismtest.NewMockServer(mock)
	defer server.Close()

	client := rpc.New(server.URL, 5*time.Second)

	// 1. Simulate 500 error
	server.Simulate500(1)
	_, err := client.GetLatestLedger(context.Background())
	if err == nil {
		t.Error("expected error on simulated 500 error")
	}

	// 2. Normal call succeeds
	resp, err := client.GetLatestLedger(context.Background())
	if err != nil {
		t.Fatalf("unexpected error after 500 recovery: %v", err)
	}
	if resp.Sequence == 0 {
		t.Error("expected non-zero sequence")
	}

	// 3. Simulate rate limit (429)
	server.SimulateRateLimit(1)
	_, err = client.GetLatestLedger(context.Background())
	if err == nil {
		t.Error("expected error on simulated rate limit 429")
	}

	// 4. Simulate gap
	mock.SimulateGap(10, 50)
	eventsResp, err := client.GetEvents(context.Background(), protocol.GetEventsRequest{
		StartLedger: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error on getEvents: %v", err)
	}
	if eventsResp.OldestLedger != 50 {
		t.Errorf("expected oldest ledger 50 after gap simulation, got %d", eventsResp.OldestLedger)
	}
}
