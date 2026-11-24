package cli

import (
	"bytes"
	"io"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func TestParseColorMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    colorMode
		wantErr bool
	}{
		{name: "auto", input: "auto", want: colorAuto},
		{name: "always", input: "always", want: colorAlways},
		{name: "never", input: "never", want: colorNever},
		{name: "empty defaults to auto", input: "", want: colorAuto},
		{name: "unknown", input: "sometimes", wantErr: true},
		{name: "case sensitive", input: "Always", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseColorMode(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("parseColorMode() error = nil, want a validation failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseColorMode() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("parseColorMode(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestColorModeEnabled(t *testing.T) {
	var buf bytes.Buffer

	t.Run("always overrides NO_COLOR", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		if !colorAlways.enabled(&buf) {
			t.Error("--color=always should colour even when NO_COLOR is set")
		}
	})

	t.Run("never always disables", func(t *testing.T) {
		if colorNever.enabled(&buf) {
			t.Error("--color=never enabled colour")
		}
	})

	t.Run("auto honours NO_COLOR", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		if colorAuto.enabled(&buf) {
			t.Error("--color=auto ignored NO_COLOR")
		}
	})

	t.Run("auto suppresses colour for a non-terminal", func(t *testing.T) {
		if colorAuto.enabled(&buf) {
			t.Error("--color=auto coloured a bytes.Buffer")
		}
	})
}

func TestColorModePaletteIsNoOpForNever(t *testing.T) {
	t.Parallel()
	palette := colorNever.palette(io.Discard)
	if palette.Reset != "" || palette.Key != "" {
		t.Errorf("colorNever returned a non-empty palette: %+v", palette)
	}
}

func TestInvalidColorIsRejectedBeforeRunning(t *testing.T) {
	_, _, err := run(t, "version", "--color", "rainbow")
	if err == nil {
		t.Fatal("Execute() error = nil, want an invalid colour failure")
	}
}

func TestNoColorEnvironmentSuppressesAutoOutput(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(120, 2))
	withFake(t, srv)
	t.Setenv("NO_COLOR", "1")

	stdout, _, err := run(t, "events", "--from", "120", "--color", "auto")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if bytes.Contains([]byte(stdout), []byte("\x1b[")) {
		t.Error("NO_COLOR did not suppress auto colour")
	}
}

func TestHighlightTextAndNever(t *testing.T) {
	got, err := HighlightText("LIQUIDATION GABCD", "LIQUIDATION|GABCD", true)
	if err != nil {
		t.Fatalf("HighlightText: %v", err)
	}
	if !bytes.Contains([]byte(got), []byte("\x1b[1;93mLIQUIDATION\x1b[0m")) {
		t.Fatalf("LIQUIDATION was not highlighted: %q", got)
	}
	got, err = HighlightText("LIQUIDATION", "LIQUIDATION", false)
	if err != nil {
		t.Fatalf("HighlightText disabled: %v", err)
	}
	if got != "LIQUIDATION" {
		t.Fatalf("disabled highlighting changed text: %q", got)
	}
}
