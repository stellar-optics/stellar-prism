package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// defaultPruneAfter is the --prune-after default, in the flag's own syntax.
const defaultPruneAfter = "7d"

// stateFlags holds the options that select and bound a stream's partition of
// a shared state file.
type stateFlags struct {
	// streamID names this stream's partition in --state-file. Empty means
	// "derive one from the command, its arguments and its filters".
	streamID string
	// pruneAfter is the raw --prune-after value ("7d", "36h", "0").
	pruneAfter string
	// retention is pruneAfter parsed. Zero keeps the whole history.
	retention time.Duration
	// operational lists the flags streamFlags.register added. They shape
	// how a stream is delivered, not which records it selects, so they are
	// left out of a derived stream ID.
	operational map[string]bool
}

// flagNames returns the names of the flags cmd defines so far.
func flagNames(cmd *cobra.Command) map[string]bool {
	names := map[string]bool{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) { names[f.Name] = true })
	return names
}

// registerStateFlags adds --stream-id and --prune-after. It is called at the
// end of streamFlags.register with the flags that existed before register
// began, so every flag register added — and only those — is recorded as
// operational, whatever order a command registers its own filters in.
func (s *streamFlags) registerStateFlags(cmd *cobra.Command, before map[string]bool) {
	cmd.Flags().StringVar(&s.state.streamID, "stream-id", "",
		"name this stream's partition in --state-file, so several prism processes can share one file "+
			"(default: a hash of the command, its arguments and its filters)")
	cmd.Flags().StringVar(&s.state.pruneAfter, "prune-after", defaultPruneAfter,
		`drop --state-file history checkpoints older than this, e.g. "7d" or "36h"; the latest checkpoint and the resume cursor are always kept (0 keeps all)`)

	s.state.operational = map[string]bool{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if !before[f.Name] {
			s.state.operational[f.Name] = true
		}
	})
}

// validateStateFlags checks --stream-id and --prune-after.
func (s *streamFlags) validateStateFlags(cmd *cobra.Command) error {
	retention, err := parseRetention(s.state.pruneAfter)
	if err != nil {
		return fmt.Errorf("invalid --prune-after: %w", err)
	}
	s.state.retention = retention
	if s.state.streamID != "" {
		if err := stream.ValidateStreamID(s.state.streamID); err != nil {
			return fmt.Errorf("invalid --stream-id: %w", err)
		}
	}
	if s.stateFile == "" {
		for _, name := range []string{"stream-id", "prune-after"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s requires --state-file", name)
			}
		}
	}
	return nil
}

// parseRetention parses a retention window: a Go duration ("36h", "90m"), a
// whole number of days ("7d"), or "0" for no limit.
func parseRetention(value string) (time.Duration, error) {
	v := strings.TrimSpace(strings.ToLower(value))
	if v == "" || v == "0" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(v, "d"); ok {
		n, err := strconv.ParseUint(strings.TrimSpace(days), 10, 32)
		if err != nil {
			return 0, fmt.Errorf("%q: expected a whole number of days, such as 7d", value)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%q: expected a duration such as 7d or 36h", value)
	}
	if d < 0 {
		return 0, fmt.Errorf("%q must not be negative", value)
	}
	return d, nil
}

// resolveStreamID returns the partition this run reads and writes in a
// shared state file: --stream-id when given, otherwise one derived from the
// command.
//
// The derived ID is a deterministic hash of the command path, the network,
// the positional arguments and every command-specific flag that was set —
// the filters. Operational flags (output format, sinks, --from, --follow,
// the state flags themselves, ...) are left out: they change how records are
// delivered or where a run starts, not which stream it is, so a run resumed
// with a different --limit or output file still finds its own cursor.
func (s *streamFlags) resolveStreamID(cmd *cobra.Command, network string) string {
	if s.state.streamID != "" {
		return s.state.streamID
	}
	return deriveStreamID(cmd, network, s.state.operational)
}

// deriveStreamID implements the default of resolveStreamID.
func deriveStreamID(cmd *cobra.Command, network string, operational map[string]bool) string {
	parts := []string{"cmd=" + cmd.CommandPath(), "network=" + network}
	for _, arg := range cmd.Flags().Args() {
		parts = append(parts, "arg="+arg)
	}
	var filters []string
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if !f.Changed || operational[f.Name] {
			return
		}
		filters = append(filters, "flag="+f.Name+"="+f.Value.String())
	})
	sort.Strings(filters)
	parts = append(parts, filters...)

	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return cmd.Name() + "/" + hex.EncodeToString(sum[:8])
}
