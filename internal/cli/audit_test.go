package cli

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

// writeAuditTrail runs a stream with --sink-audit and returns the path of
// the trail it wrote.
func writeAuditTrail(t *testing.T, args ...string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	srv := fakerpc.NewServer(fakerpc.MakeEvents(12000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	full := append([]string{"events", "--from", "12000", "--json", "--sink-audit", path}, args...)
	if _, _, err := run(t, full...); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	return path
}

func TestAuditSinkWritesAVerifiableTrail(t *testing.T) {
	path := writeAuditTrail(t)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the audit trail was not written: %v", err)
	}

	stdout, _, err := run(t, "audit-verify", path)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "the trail is intact") {
		t.Errorf("verification does not report an intact trail\n%s", stdout)
	}
	if !strings.Contains(stdout, "Records: 3") {
		t.Errorf("verification does not report the audited records\n%s", stdout)
	}
}

func TestAuditVerifyFindsTampering(t *testing.T) {
	path := writeAuditTrail(t)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	// Change a record without touching the hashes that were written with
	// it, which is what a tampered log looks like.
	tampered := strings.Replace(string(data), `"recordId":"`, `"recordId":"tampered-`, 1)
	if tampered == string(data) {
		t.Fatal("the trail holds no record to tamper with")
	}
	if err := os.WriteFile(path, []byte(tampered), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}

	stdout, _, err := run(t, "audit-verify", path)
	if err == nil {
		t.Fatal("Execute() error = nil, want the tampering to be reported")
	}
	if !strings.Contains(stdout, "[modified]") {
		t.Errorf("verification does not name the modified record\n%s", stdout)
	}
	if !strings.Contains(stdout, "the trail was tampered with") {
		t.Errorf("verification does not report the tampering\n%s", stdout)
	}
}

func TestAuditVerifyChecksTheExpectedKey(t *testing.T) {
	path := writeAuditTrail(t)

	other, err := os.CreateTemp(t.TempDir(), "other*.pub")
	if err != nil {
		t.Fatalf("creating the public key: %v", err)
	}
	if _, err := other.WriteString("-----BEGIN PUBLIC KEY-----\nnot a key\n-----END PUBLIC KEY-----\n"); err != nil {
		t.Fatalf("writing the public key: %v", err)
	}
	if err := other.Close(); err != nil {
		t.Fatalf("closing the public key: %v", err)
	}

	if _, _, err := run(t, "audit-verify", "--public-key", other.Name(), path); err == nil {
		t.Fatal("Execute() error = nil, want the unreadable key to be reported")
	}
}

func TestAuditVerifyPrintsTheSigningKey(t *testing.T) {
	path := writeAuditTrail(t)

	stdout, stderr, err := run(t, "audit-verify", "--print-public-key", path)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "BEGIN PUBLIC KEY") {
		t.Errorf("the signing key was not printed\n%s", stdout)
	}
	// The key is printed on its own, so it can be redirected into a file
	// the verifier reads back.
	key, err := sink.ParseAuditPublicKey([]byte(stdout))
	if err != nil {
		t.Fatalf("the printed key is not a usable public key: %v\n%s", err, stdout)
	}
	if !strings.Contains(stderr, "the trail is intact") {
		t.Errorf("the report does not go beside the key\n%s", stderr)
	}

	keyFile := filepath.Join(t.TempDir(), "prism.pub")
	if err := os.WriteFile(keyFile, []byte(stdout), 0o644); err != nil {
		t.Fatalf("writing %s: %v", keyFile, err)
	}
	if _, _, err := run(t, "audit-verify", "--public-key", keyFile, path); err != nil {
		t.Fatalf("Execute() error = %v, want the pinned key to verify", err)
	}
	if len(key) != ed25519.PublicKeySize {
		t.Errorf("the printed key is %d bytes, want %d", len(key), ed25519.PublicKeySize)
	}
}

func TestAuditSinkFlagValidation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(12100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "key without a trail",
			args: []string{"events", "--from", "12100", "--sink-audit-key", "/tmp/key"},
		},
		{
			name: "checkpoint without a trail",
			args: []string{"events", "--from", "12100", "--sink-audit-checkpoint", "10"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestAuditSinkRejectsTheOutputFile(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(12200, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "out.ndjson")
	args := []string{"events", "--from", "12200", "--json", "--output-file", path, "--sink-audit", path}
	if _, _, err := run(t, args...); err == nil {
		t.Fatal("Execute() error = nil, want the audit trail to differ from the output file")
	}
}
