package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestRetryBackoffFlags(t *testing.T) {
	t.Parallel()
	def := stream.DefaultBackoff()
	tests := []struct {
		name        string
		args        []string
		wantErr     string
		wantInitial time.Duration
		wantMax     time.Duration
	}{
		{name: "defaults match the library", wantInitial: def.Initial, wantMax: def.Max},
		{name: "both tuned", args: []string{"--retry-backoff", "2s", "--retry-backoff-max", "2m"}, wantInitial: 2 * time.Second, wantMax: 2 * time.Minute},
		{name: "initial equal to max", args: []string{"--retry-backoff", "5s", "--retry-backoff-max", "5s"}, wantInitial: 5 * time.Second, wantMax: 5 * time.Second},
		{name: "zero initial uses the default", args: []string{"--retry-backoff", "0s"}, wantInitial: def.Initial, wantMax: def.Max},
		{name: "negative rejected", args: []string{"--retry-backoff", "-1s"}, wantErr: "must not be negative"},
		{name: "max below initial rejected", args: []string{"--retry-backoff", "10s", "--retry-backoff-max", "1s"}, wantErr: "must not be less than --retry-backoff"},
		{name: "initial above the default max needs a larger max", args: []string{"--retry-backoff", "1m"}, wantErr: "must not be less than --retry-backoff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sf streamFlags
			cmd := &cobra.Command{Use: "events"}
			sf.register(cmd, 100)
			if err := cmd.ParseFlags(tc.args); err != nil {
				t.Fatalf("parse: %v", err)
			}
			err := sf.validate(cmd)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("validate error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			b := sf.backoff()
			if b.Initial != tc.wantInitial || b.Max != tc.wantMax {
				t.Fatalf("backoff = %s..%s, want %s..%s", b.Initial, b.Max, tc.wantInitial, tc.wantMax)
			}
			if !b.Jitter || b.Factor != def.Factor {
				t.Fatalf("backoff jitter/factor = %v/%v, want the library's %v/%v", b.Jitter, b.Factor, def.Jitter, def.Factor)
			}
		})
	}
}

func TestRetryBackoffUnsetUsesTailerDefault(t *testing.T) {
	t.Parallel()
	var sf streamFlags
	if got := sf.backoff(); got.Initial != 0 || got.Max != 0 {
		t.Fatalf("backoff() on unset flags = %+v, want the zero value so the tailer applies its default", got)
	}
}
