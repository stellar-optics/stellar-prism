package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// withOpenCursorStore swaps the cursor-store opener for one that needs no
// database, restoring it when the test finishes — the same seam newClient
// gives the RPC layer.
func withOpenCursorStore(t *testing.T, fn func(ctx context.Context, dsn string) (stream.CursorStore, func() error, error)) {
	t.Helper()
	original := openCursorStore
	openCursorStore = fn
	t.Cleanup(func() { openCursorStore = original })
}

// cursorStore is a minimal in-memory CursorStore that records which keys the
// command looked up and wrote under.
type cursorStore struct {
	mu       sync.Mutex
	state    stream.CursorState
	saved    bool
	loadKeys []string
	saveKeys []string
}

func (s *cursorStore) Load(_ context.Context, key string) (stream.CursorState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadKeys = append(s.loadKeys, key)
	if !s.saved {
		return stream.CursorState{}, fmt.Errorf("cursor %q: %w", key, stream.ErrNoCursor)
	}
	return s.state, nil
}

func (s *cursorStore) Save(_ context.Context, key string, state stream.CursorState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveKeys = append(s.saveKeys, key)
	s.state, s.saved = state, true
	return nil
}

func (s *cursorStore) keys() (loads, saves []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.loadKeys...), append([]string(nil), s.saveKeys...)
}

// TestPGFlagsAreDocumented checks the assignment's "--help strings" half: the
// flags must be discoverable on every streaming command, with enough wording
// to understand what they do without reading the source.
func TestPGFlagsAreDocumented(t *testing.T) {
	for _, cmd := range []string{"events", "ledgers", "transactions"} {
		t.Run(cmd, func(t *testing.T) {
			stdout, _, err := run(t, cmd, "--help")
			if err != nil {
				t.Fatalf("Execute(%s --help) error = %v", cmd, err)
			}
			for _, want := range []string{
				"--pg-dsn",
				"--state-pg",
				"--pg-cursor-key",
				"PRISM_PG_DSN",
				"resumes where",
			} {
				if !strings.Contains(stdout, want) {
					t.Errorf("%s --help does not mention %q", cmd, want)
				}
			}
		})
	}
}

// TestEventsCommandLongExplainsDurability covers the prose half of the
// documentation requirement, not just the flag table.
func TestEventsCommandLongExplainsDurability(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute(events --help) error = %v", err)
	}
	for _, want := range []string{"--pg-dsn", "committed to PostgreSQL", "resumes where it stopped"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("events --help does not mention %q", want)
		}
	}
}

// TestCursorStoreIsNotOpenedWithoutADsn exercises the production opener
// directly: with no DSN it must hand back a disabled store and never touch a
// driver, which is what keeps the default path offline.
func TestCursorStoreIsNotOpenedWithoutADsn(t *testing.T) {
	store, closeStore, err := openCursorStore(context.Background(), "")
	if err != nil {
		t.Fatalf("openCursorStore(\"\") error = %v", err)
	}
	if store != nil {
		t.Error("openCursorStore(\"\") returned a store, want persistence to stay disabled")
	}
	if closeStore != nil {
		t.Error("openCursorStore(\"\") returned a closer, want nil")
	}
}

// TestCursorStoreReceivesTheResolvedDsn pins the resolution order: flag
// first, then the environment, and nothing at all when neither is set.
func TestCursorStoreReceivesTheResolvedDsn(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  string
		want string
	}{
		{name: "flag wins", args: []string{"events", "--from", "100", "--pg-dsn", "postgres://flag"}, env: "postgres://env", want: "postgres://flag"},
		{name: "environment is used when no flag", args: []string{"events", "--from", "100"}, env: "postgres://env", want: "postgres://env"},
		{name: "disabled by default", args: []string{"events", "--from", "100"}, env: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envPGDSN, tc.env)
			srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2), fakerpc.WithPageLimit(10))
			withFake(t, srv)

			var got string
			called := 0
			withOpenCursorStore(t, func(_ context.Context, dsn string) (stream.CursorStore, func() error, error) {
				called++
				got = dsn
				return nil, nil, nil
			})

			if _, _, err := run(t, tc.args...); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if called != 1 {
				t.Fatalf("openCursorStore called %d time(s), want 1", called)
			}
			if got != tc.want {
				t.Errorf("dsn = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCursorStoreFailureFailsTheRun: an operator who asked for durability
// must find out immediately if it cannot be provided, rather than running
// without it in silence.
func TestCursorStoreFailureFailsTheRun(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	wantErr := errors.New("permission denied for schema public")
	withOpenCursorStore(t, func(context.Context, string) (stream.CursorStore, func() error, error) {
		return nil, nil, wantErr
	})

	_, _, err := run(t, "events", "--from", "100", "--pg-dsn", "postgres://somewhere")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute() error = %v, want it to wrap %v", err, wantErr)
	}
}

// TestCursorStoreIsClosedOnExit proves the connection pool is released when
// the command ends, including the success path where nothing else would
// complain.
func TestCursorStoreIsClosedOnExit(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	closed := 0
	withOpenCursorStore(t, func(context.Context, string) (stream.CursorStore, func() error, error) {
		return &cursorStore{}, func() error {
			closed++
			return nil
		}, nil
	})

	if _, _, err := run(t, "events", "--from", "100", "--pg-dsn", "postgres://somewhere"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if closed != 1 {
		t.Errorf("closer ran %d time(s), want exactly once", closed)
	}
}

// TestEventsResumeAcrossInvocations is the CLI-level integration test: two
// separate runs of the real command, sharing only the store, must produce one
// continuous stream — the first run's records plus the next ones, with no
// repetition and nothing skipped.
func TestEventsResumeAcrossInvocations(t *testing.T) {
	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))
	withFake(t, srv)

	store := &cursorStore{}
	withOpenCursorStore(t, func(context.Context, string) (stream.CursorStore, func() error, error) {
		return store, func() error { return nil }, nil
	})

	first, _, err := run(t, "events", "--from", "100", "--limit", "3", "--json")
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, _, err := run(t, "events", "--from", "100", "--limit", "5", "--json")
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}

	got := append(idsFromNDJSON(t, first), idsFromNDJSON(t, second)...)

	want := make([]string, 0, 8)
	for _, e := range events[:8] {
		want = append(want, e.ID)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d record(s) across two runs, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	seen := make(map[string]bool, len(got))
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %q, want %q — the second run did not resume where the first stopped", i, got[i], want[i])
		}
		if seen[got[i]] {
			t.Errorf("record %q was emitted twice", got[i])
		}
		seen[got[i]] = true
	}

	loads, saves := store.keys()
	if len(loads) != 2 {
		t.Errorf("cursor row was loaded %d time(s), want once per invocation", len(loads))
	}
	if len(saves) == 0 {
		t.Error("no position was ever persisted")
	}
	for _, key := range append(loads, saves...) {
		if key == "" {
			t.Error("an empty cursor key was used; every stream must name its own row")
		}
	}
}

// TestCursorKeyFlagOverridesTheDerivedKey: --pg-cursor-key is the operator's
// way to pin a row by name, and it must win over the derivation.
func TestCursorKeyFlagOverridesTheDerivedKey(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	store := &cursorStore{}
	withOpenCursorStore(t, func(context.Context, string) (stream.CursorStore, func() error, error) {
		return store, func() error { return nil }, nil
	})

	if _, _, err := run(t, "events", "--from", "100", "--limit", "2", "--json",
		"--pg-cursor-key", "my-pinned-row"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	loads, saves := store.keys()
	for _, key := range append(loads, saves...) {
		if key != "my-pinned-row" {
			t.Errorf("row name = %q, want the --pg-cursor-key value", key)
		}
	}
}

// TestGapGuidanceMentionsTheCursorRow covers the stalled-cursor trap: with a
// store enabled, a gap must say what happens to the stale row, because
// restarting the same command would gap again.
func TestGapGuidanceMentionsTheCursorRow(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 40),
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{1: {OldestLedger: 9000000}}),
	)
	withFake(t, srv)
	withOpenCursorStore(t, func(context.Context, string) (stream.CursorStore, func() error, error) {
		return &cursorStore{}, func() error { return nil }, nil
	})

	_, stderr, err := run(t, "events", "--from", "5000", "--json",
		"--page-size", "5", "--pg-dsn", "postgres://somewhere")
	if err == nil {
		t.Fatal("Execute() error = nil, want a gap failure")
	}
	code, _ := ExitCode(err)
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	for _, want := range []string{"gap", "missed", "cursor row"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q\n%s", want, stderr)
		}
	}
}

// TestCursorKeyDerivation pins the row-naming rules:
//   - the same stream always derives the same key;
//   - a stream that differs in any selecting filter derives a different one;
//   - parts cannot re-spell one another, so two genuinely different streams
//     can never be handed the same row.
func TestCursorKeyDerivation(t *testing.T) {
	base := cursorKey("events", fromKeyPart(100), "contract=CA")
	if base != cursorKey("events", fromKeyPart(100), "contract=CA") {
		t.Error("the same stream did not derive a stable key")
	}
	if !strings.HasPrefix(base, "events/") || len(base) != len("events/")+16 {
		t.Errorf("key = %q, want events/<16 hex characters>", base)
	}

	tests := []struct {
		name  string
		parts []string
	}{
		{name: "another contract", parts: []string{fromKeyPart(100), "contract=CB"}},
		{name: "another start ledger", parts: []string{fromKeyPart(101), "contract=CA"}},
		{name: "a second contract", parts: []string{fromKeyPart(100), "contract=CA", "contract=CB"}},
		{name: "a topic instead", parts: []string{fromKeyPart(100), "contract=CA", "topic=AAAAAQ==,*"}},
		{name: "a type filter", parts: []string{fromKeyPart(100), "contract=CA", "type=system"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if key := cursorKey("events", tc.parts...); key == base {
				t.Errorf("key = %q, want it to differ from the base stream", key)
			}
		})
	}

	// Field tags matter: without them, "one contract plus one topic" and "two
	// contracts" would join to the same string and share a row.
	contractPlusTopic := cursorKey("events", fromKeyPart(100), "contract=X", "topic=Y")
	twoContracts := cursorKey("events", fromKeyPart(100), "contract=X", "contract=Y")
	if contractPlusTopic == twoContracts {
		t.Error("part boundaries are not preserved; different streams could share a row")
	}

	if got, same := cursorKey("ledgers", fromKeyPart(100), "contract=CA"), base; got == same {
		t.Error("different commands derived the same key")
	}
}

func idsFromNDJSON(t *testing.T, out string) []string {
	t.Helper()

	var ids []string
	for _, line := range nonEmptyLines(out) {
		var rec struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %q is not valid JSON: %v", line, err)
		}
		ids = append(ids, rec.ID)
	}
	return ids
}
