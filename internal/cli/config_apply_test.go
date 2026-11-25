package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// toyFlags is the resolved state of the toy command tree used below.
type toyFlags struct {
	rpcTimeout time.Duration
	color      string
	pageSize   uint
	follow     bool
	topics     []string
	types      []string
}

// newToyTree builds a root with one persistent flag and an "events" child
// with local flags of each shape the CLI uses, and returns the child.
func newToyTree(t *testing.T, f *toyFlags, args ...string) *cobra.Command {
	t.Helper()
	root := &cobra.Command{Use: "prism"}
	root.PersistentFlags().DurationVar(&f.rpcTimeout, "rpc-timeout", 30*time.Second, "")
	root.PersistentFlags().StringVar(&f.color, "color", "auto", "")
	events := &cobra.Command{Use: "events", RunE: func(*cobra.Command, []string) error { return nil }}
	events.Flags().UintVar(&f.pageSize, "page-size", 200, "")
	events.Flags().BoolVarP(&f.follow, "follow", "f", false, "")
	events.Flags().StringArrayVar(&f.topics, "topic", nil, "")
	events.Flags().StringSliceVar(&f.types, "type", nil, "")
	root.AddCommand(events)

	cmd, rest, err := root.Find(append([]string{"events"}, args...))
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if err := cmd.ParseFlags(rest); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cmd
}

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func envFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestEnvVarForFlag(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"rpc-url":       "PRISM_RPC_URL",
		"page-size":     "PRISM_PAGE_SIZE",
		"sink-es-index": "PRISM_SINK_ES_INDEX",
		"follow":        "PRISM_FOLLOW",
	}
	for flag, want := range tests {
		if got := EnvVarForFlag(flag); got != want {
			t.Errorf("EnvVarForFlag(%q) = %q, want %q", flag, got, want)
		}
	}
}

func TestApplyConfigSourcesPrecedence(t *testing.T) {
	t.Parallel()

	const file = `
page-size: 10
rpc-timeout: 5s
events:
  page-size: 20
`
	tests := []struct {
		name     string
		args     []string
		env      map[string]string
		file     string
		wantPage uint
		wantTO   time.Duration
	}{
		{name: "defaults when nothing is configured", wantPage: 200, wantTO: 30 * time.Second},
		{name: "top-level file key", file: "page-size: 10\n", wantPage: 10, wantTO: 30 * time.Second},
		{name: "command section beats top level", file: file, wantPage: 20, wantTO: 5 * time.Second},
		{name: "environment beats file", file: file, env: map[string]string{"PRISM_PAGE_SIZE": "30"}, wantPage: 30, wantTO: 5 * time.Second},
		{name: "flag beats environment", file: file, args: []string{"--page-size", "40"}, env: map[string]string{"PRISM_PAGE_SIZE": "30"}, wantPage: 40, wantTO: 5 * time.Second},
		{name: "blank environment value is ignored", file: file, env: map[string]string{"PRISM_PAGE_SIZE": "  "}, wantPage: 20, wantTO: 5 * time.Second},
		{name: "persistent flag from environment", env: map[string]string{"PRISM_RPC_TIMEOUT": "2s"}, wantPage: 200, wantTO: 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			src := configSources{
				userFile:    filepath.Join(dir, "absent.yaml"),
				projectFile: filepath.Join(dir, "absent-project.yaml"),
				lookupEnv:   envFrom(tc.env),
			}
			if tc.file != "" {
				src.projectFile = writeFile(t, dir, ".prism.yaml", tc.file)
			}
			var f toyFlags
			cmd := newToyTree(t, &f, tc.args...)
			if err := applyConfigSources(cmd, src); err != nil {
				t.Fatalf("applyConfigSources: %v", err)
			}
			if f.pageSize != tc.wantPage {
				t.Errorf("page-size = %d, want %d", f.pageSize, tc.wantPage)
			}
			if f.rpcTimeout != tc.wantTO {
				t.Errorf("rpc-timeout = %s, want %s", f.rpcTimeout, tc.wantTO)
			}
		})
	}
}

func TestApplyConfigSourcesMarksFlagsChanged(t *testing.T) {
	t.Parallel()
	var f toyFlags
	cmd := newToyTree(t, &f)
	src := configSources{lookupEnv: envFrom(map[string]string{"PRISM_FOLLOW": "true"})}
	if err := applyConfigSources(cmd, src); err != nil {
		t.Fatal(err)
	}
	if !f.follow || !cmd.Flags().Changed("follow") {
		t.Fatalf("follow = %v, changed = %v; want both true", f.follow, cmd.Flags().Changed("follow"))
	}
	if cmd.Flags().Changed("page-size") {
		t.Fatal("an unconfigured flag must not be marked as changed")
	}
}

func TestApplyConfigSourcesProjectOverridesUser(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := configSources{
		userFile:    writeFile(t, dir, "config.yaml", "page-size: 11\ncolor: never\n"),
		projectFile: writeFile(t, dir, ".prism.yaml", "page-size: 22\n"),
		lookupEnv:   envFrom(nil),
	}
	var f toyFlags
	cmd := newToyTree(t, &f)
	if err := applyConfigSources(cmd, src); err != nil {
		t.Fatal(err)
	}
	if f.pageSize != 22 {
		t.Errorf("page-size = %d, want the project value 22", f.pageSize)
	}
	if f.color != "never" {
		t.Errorf("color = %q, want the user value kept where the project is silent", f.color)
	}
}

func TestApplyConfigSourcesExplicitFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	explicit := writeFile(t, dir, "ci.yaml", "page-size: 7\n")
	src := configSources{
		explicit:    explicit,
		userFile:    writeFile(t, dir, "config.yaml", "color: never\n"),
		projectFile: writeFile(t, dir, ".prism.yaml", "page-size: 99\n"),
		lookupEnv:   envFrom(nil),
	}
	var f toyFlags
	cmd := newToyTree(t, &f)
	if err := applyConfigSources(cmd, src); err != nil {
		t.Fatal(err)
	}
	if f.pageSize != 7 || f.color != "auto" {
		t.Fatalf("page-size = %d, color = %q; want only the explicit file applied (7, auto)", f.pageSize, f.color)
	}

	src.explicit = filepath.Join(dir, "missing.yaml")
	var g toyFlags
	if err := applyConfigSources(newToyTree(t, &g), src); err == nil {
		t.Fatal("a missing explicit --config file must be an error")
	}
}

func TestApplyConfigSourcesRepeatableFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		file       string
		env        map[string]string
		wantTopics []string
		wantTypes  []string
	}{
		{
			name:       "yaml lists set one value per item",
			file:       "events:\n  topic: ['AAAAAQ==,*', 'AAAAAg==']\n  type: [contract, system]\n",
			wantTopics: []string{"AAAAAQ==,*", "AAAAAg=="},
			wantTypes:  []string{"contract", "system"},
		},
		{
			name:       "environment splits on newlines and keeps commas",
			env:        map[string]string{"PRISM_TOPIC": "AAAAAQ==,*\nAAAAAg==,**\n", "PRISM_TYPE": "contract,diagnostic"},
			wantTopics: []string{"AAAAAQ==,*", "AAAAAg==,**"},
			wantTypes:  []string{"contract", "diagnostic"},
		},
		{
			name:       "a scalar yaml value sets a single entry",
			file:       "topic: 'AAAAAQ==,*'\n",
			wantTopics: []string{"AAAAAQ==,*"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			src := configSources{lookupEnv: envFrom(tc.env)}
			if tc.file != "" {
				src.projectFile = writeFile(t, dir, ".prism.yaml", tc.file)
			}
			var f toyFlags
			cmd := newToyTree(t, &f)
			if err := applyConfigSources(cmd, src); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.topics, tc.wantTopics) {
				t.Errorf("topics = %q, want %q", f.topics, tc.wantTopics)
			}
			if !reflect.DeepEqual(f.types, tc.wantTypes) {
				t.Errorf("types = %q, want %q", f.types, tc.wantTypes)
			}
		})
	}
}

func TestApplyConfigSourcesErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		file    string
		env     map[string]string
		wantErr string
	}{
		{name: "unparseable value from file", file: "page-size: lots\n", wantErr: `config key "page-size"`},
		{name: "unparseable value from environment", env: map[string]string{"PRISM_RPC_TIMEOUT": "soon"}, wantErr: "$PRISM_RPC_TIMEOUT"},
		{name: "list for a scalar flag", file: "page-size: [1, 2]\n", wantErr: "takes a single value"},
		{name: "mapping for a flag", file: "events:\n  page-size:\n    a: 1\n", wantErr: "mapping"},
		{name: "malformed yaml", file: "page-size: [\n", wantErr: "reading config file"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			src := configSources{lookupEnv: envFrom(tc.env)}
			if tc.file != "" {
				src.projectFile = writeFile(t, dir, ".prism.yaml", tc.file)
			}
			var f toyFlags
			err := applyConfigSources(newToyTree(t, &f), src)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestCommandSection(t *testing.T) {
	t.Parallel()
	root := &cobra.Command{Use: "prism"}
	completion := &cobra.Command{Use: "completion"}
	bash := &cobra.Command{Use: "bash"}
	completion.AddCommand(bash)
	root.AddCommand(completion)

	tests := map[*cobra.Command]string{root: "", completion: "completion", bash: "completion.bash"}
	for cmd, want := range tests {
		if got := commandSection(cmd); got != want {
			t.Errorf("commandSection(%s) = %q, want %q", cmd.Name(), got, want)
		}
	}
}

// TestRootAppliesConfigFile proves the real root command consumes --config:
// an invalid colour in the file must be rejected by the same validation that
// rejects it on the command line.
func TestRootAppliesConfigFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := writeFile(t, dir, "prism.yaml", "color: sometimes\n")

	var stdout, stderr bytes.Buffer
	err := Execute([]string{"version", "--config", path}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "sometimes") {
		t.Fatalf("error = %v, want the config file's invalid colour rejected", err)
	}

	good := writeFile(t, dir, "good.yaml", "color: never\n")
	stdout.Reset()
	if err := Execute([]string{"version", "--config", good}, &stdout, &stderr); err != nil {
		t.Fatalf("valid config file: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != Version {
		t.Fatalf("stdout = %q, want the version", stdout.String())
	}
}
