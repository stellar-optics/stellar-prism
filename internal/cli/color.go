package cli

import (
	"fmt"
	"io"
	"os"
	"regexp"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"
)

// colorMode is the parsed value of --color.
//
// The three values match stellar-xdr-lens so the two tools behave as one
// family. An empty value is treated as "auto" so a programmatic caller that
// leaves the flag unset gets the same behaviour as the command line.
type colorMode string

const (
	colorAuto   colorMode = "auto"
	colorAlways colorMode = "always"
	colorNever  colorMode = "never"
)

// parseColorMode validates a --color value.
//
// The error names the accepted values, because a typo in a colour flag is
// otherwise indistinguishable from "colours are off on this terminal".
func parseColorMode(value string) (colorMode, error) {
	switch colorMode(value) {
	case colorAuto, "":
		return colorAuto, nil
	case colorAlways:
		return colorAlways, nil
	case colorNever:
		return colorNever, nil
	default:
		return "", fmt.Errorf(`invalid --color %q: want "auto", "always" or "never"`, value)
	}
}

// palette returns the palette to use for w.
//
// "always" is an explicit request from the operator and therefore wins over
// NO_COLOR; "never" is equally explicit. Only "auto" consults the environment
// and the writer, which is where the https://no-color.org convention applies.
func (m colorMode) palette(w io.Writer) lensformat.Palette {
	switch m {
	case colorAlways:
		return lensformat.ColorPalette
	case colorNever:
		return lensformat.NoColor
	default:
		return lensformat.PaletteFor(w, string(colorAuto))
	}
}

// enabled reports whether the mode produces ANSI escape sequences for w.
func (m colorMode) enabled(w io.Writer) bool {
	return m.palette(w).Reset != ""
}

type TerminalHighlighter struct {
	re      *regexp.Regexp
	enabled bool
}

func CompileHighlighter(pattern string) (*TerminalHighlighter, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return &TerminalHighlighter{re: re, enabled: true}, nil
}

func NewTerminalHighlighter(pattern string) (*TerminalHighlighter, error) {
	return CompileHighlighter(pattern)
}

func (h *TerminalHighlighter) SetEnabled(enabled bool) {
	h.enabled = enabled
}

func (h *TerminalHighlighter) Highlight(text string) string {
	if h == nil || h.re == nil || !h.enabled {
		return text
	}
	return h.re.ReplaceAllStringFunc(text, func(match string) string {
		return "\x1b[1;93m" + match + "\x1b[0m"
	})
}

func HighlightText(text, pattern string, enabled bool) (string, error) {
	h, err := CompileHighlighter(pattern)
	if err != nil {
		return "", err
	}
	h.SetEnabled(enabled)
	return h.Highlight(text), nil
}

type highlightWriter struct {
	w io.Writer
	h *TerminalHighlighter
}

func (w *highlightWriter) Write(p []byte) (int, error) {
	_, err := io.WriteString(w.w, w.h.Highlight(string(p)))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func NewHighlightWriter(w io.Writer, pattern string, mode colorMode) (io.Writer, error) {
	if w == nil || pattern == "" || mode == colorNever {
		return w, nil
	}
	if mode != colorAlways && os.Getenv("NO_COLOR") != "" {
		return w, nil
	}
	if mode == colorAuto && !isTerminalWriter(w) {
		return w, nil
	}
	h, err := CompileHighlighter(pattern)
	if err != nil {
		return nil, err
	}
	return &highlightWriter{w: w, h: h}, nil
}
