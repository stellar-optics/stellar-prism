package sink_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// auditContent mirrors the content the chain hash covers. It is written out
// here on purpose: the test recomputes the hash of the documented formula
// rather than calling into the sink, so a change to the formula cannot make
// the test agree with itself.
type auditContent struct {
	Seq        uint32          `json:"seq"`
	Ledger     uint32          `json:"ledger"`
	Time       string          `json:"time"`
	RecordID   string          `json:"recordId"`
	RecordKind string          `json:"recordKind"`
	Record     json.RawMessage `json:"record"`
}

// auditRecords returns the records a trail is built from.
func auditRecords() []stream.Record {
	records := []stream.Record{eventRecord()}
	for i := 0; i < 4; i++ {
		records = append(records, stream.Record{
			ID:       "000000010" + string(rune('a'+i)),
			Sequence: uint32(101 + i),
			Time:     time.Unix(1700000100+int64(i), 0).UTC(),
			Kind:     stream.KindLedger,
			Close:    &stream.LedgerClose{Hash: "deadbeef"},
		})
	}
	return records
}

// writeAuditTrail writes a trail and returns its path.
func writeAuditTrail(t *testing.T, path string, records []stream.Record, opts sink.AuditOptions) string {
	t.Helper()

	opts.Path = path
	trail, err := sink.NewAuditSink(opts)
	if err != nil {
		t.Fatalf("NewAuditSink() error = %v", err)
	}
	for _, record := range records {
		if err := trail.Emit(record); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if err := trail.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return path
}

// readAuditLines returns the lines of a trail.
func readAuditLines(t *testing.T, path string) []string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// writeAuditLines writes lines back to a trail.
func writeAuditLines(t *testing.T, path string, lines []string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// entryType returns the type of an audit entry.
func entryType(t *testing.T, line string) string {
	t.Helper()

	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("entry is not valid JSON: %v\n%s", err, line)
	}
	return envelope.Type
}

// auditAnomalyTypes returns the types of the anomalies a report holds.
func auditAnomalyTypes(report *sink.AuditVerification) []string {
	types := make([]string, 0, len(report.Anomalies))
	for _, anomaly := range report.Anomalies {
		types = append(types, anomaly.Type)
	}
	return types
}

// hasAnomaly reports whether a report holds an anomaly of a kind.
func hasAnomaly(report *sink.AuditVerification, kind string) bool {
	for _, anomaly := range report.Anomalies {
		if anomaly.Type == kind {
			return true
		}
	}
	return false
}

func TestAuditTrailVerifies(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 2})

	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(readAuditLines(t, path), "\n")), sink.AuditVerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("VerifyAuditLog() reported anomalies: %v", auditAnomalyTypes(report))
	}
	if report.Records != 5 {
		t.Errorf("records = %d, want 5", report.Records)
	}
	if report.Checkpoints == 0 {
		t.Error("checkpoints = 0, want the trail to carry signed checkpoints")
	}
	if report.LedgerMin != 100 || report.LedgerMax != 104 {
		t.Errorf("ledger range = %d - %d, want 100 - 104", report.LedgerMin, report.LedgerMax)
	}
	if report.PublicKey == "" {
		t.Error("public key is empty, want the trail to name the key it is signed with")
	}
}

func TestAuditChainFollowsTheDocumentedFormula(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 3})

	lines := readAuditLines(t, path)

	var header sink.AuditHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("header is not valid JSON: %v", err)
	}
	previous, err := hex.DecodeString(header.Hash)
	if err != nil {
		t.Fatalf("header hash is not valid hex: %v", err)
	}

	records := 0
	for _, line := range lines[1:] {
		if entryType(t, line) != sink.AuditEntryTypeRecord {
			continue
		}
		var record sink.AuditRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record is not valid JSON: %v", err)
		}

		// Hash_n = SHA256(Hash_{n-1} + Record_n).
		body, err := json.Marshal(auditContent{
			Seq:        record.Seq,
			Ledger:     record.Ledger,
			Time:       record.Time,
			RecordID:   record.RecordID,
			RecordKind: record.RecordKind,
			Record:     record.Record,
		})
		if err != nil {
			t.Fatalf("encoding record content: %v", err)
		}
		digest := sha256.New()
		digest.Write(previous)
		digest.Write(body)
		want := hex.EncodeToString(digest.Sum(nil))

		if record.Hash != want {
			t.Errorf("record %d hash = %s, want %s", record.Seq, record.Hash, want)
		}
		if record.PrevHash != hex.EncodeToString(previous) {
			t.Errorf("record %d previous hash = %s, want %s", record.Seq, record.PrevHash, hex.EncodeToString(previous))
		}
		previous, err = hex.DecodeString(record.Hash)
		if err != nil {
			t.Fatalf("record hash is not valid hex: %v", err)
		}
		records++
	}
	if records != 5 {
		t.Errorf("records = %d, want 5", records)
	}
}

func TestAuditTrailDetectsTampering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tamper   func(t *testing.T, lines []string) []string
		wantType string
	}{
		{
			name: "modified record",
			tamper: func(t *testing.T, lines []string) []string {
				t.Helper()
				for i, line := range lines {
					if entryType(t, line) != sink.AuditEntryTypeRecord {
						continue
					}
					lines[i] = strings.Replace(line, `"recordId":"`, `"recordId":"tampered-`, 1)
					return lines
				}
				t.Fatal("the trail holds no record to modify")
				return nil
			},
			wantType: sink.AuditAnomalyModified,
		},
		{
			name: "deleted record",
			tamper: func(t *testing.T, lines []string) []string {
				t.Helper()
				seen := 0
				for i, line := range lines {
					if entryType(t, line) != sink.AuditEntryTypeRecord {
						continue
					}
					seen++
					if seen == 2 {
						return append(lines[:i:i], lines[i+1:]...)
					}
				}
				t.Fatal("the trail holds no record to delete")
				return nil
			},
			wantType: sink.AuditAnomalyDeleted,
		},
		{
			name: "inserted record",
			tamper: func(t *testing.T, lines []string) []string {
				t.Helper()
				for i, line := range lines {
					if entryType(t, line) != sink.AuditEntryTypeRecord {
						continue
					}
					return append(lines[:i+1:i+1], append([]string{line}, lines[i+1:]...)...)
				}
				t.Fatal("the trail holds no record to insert")
				return nil
			},
			wantType: sink.AuditAnomalyInserted,
		},
		{
			name: "forged checkpoint",
			tamper: func(t *testing.T, lines []string) []string {
				t.Helper()
				for i, line := range lines {
					if entryType(t, line) != sink.AuditEntryTypeCheckpoint {
						continue
					}
					var checkpoint map[string]json.RawMessage
					if err := json.Unmarshal([]byte(line), &checkpoint); err != nil {
						t.Fatalf("checkpoint is not valid JSON: %v", err)
					}
					checkpoint["hash"] = json.RawMessage(`"` + strings.Repeat("0", 64) + `"`)
					body, err := json.Marshal(checkpoint)
					if err != nil {
						t.Fatalf("encoding checkpoint: %v", err)
					}
					lines[i] = string(body)
					return lines
				}
				t.Fatal("the trail holds no checkpoint to forge")
				return nil
			},
			wantType: sink.AuditAnomalySignature,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "audit.log")
			writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 2})

			lines := tc.tamper(t, readAuditLines(t, path))
			writeAuditLines(t, path, lines)

			report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(lines, "\n")), sink.AuditVerifyOptions{})
			if err != nil {
				t.Fatalf("VerifyAuditLog() error = %v", err)
			}
			if report.OK() {
				t.Fatal("VerifyAuditLog() reported no anomalies, want the tampering to be found")
			}
			if !hasAnomaly(report, tc.wantType) {
				t.Errorf("anomalies = %v, want one of type %q", auditAnomalyTypes(report), tc.wantType)
			}
		})
	}
}

// TestAuditTrailDetectsRehashedTampering covers the case an attacker cares
// most about: a record was changed and the whole chain was recomputed, so
// the hashes agree with each other again. The signed checkpoint does not.
func TestAuditTrailDetectsRehashedTampering(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 2})

	lines := readAuditLines(t, path)
	for i, line := range lines {
		if entryType(t, line) != sink.AuditEntryTypeRecord {
			continue
		}
		lines[i] = strings.Replace(line, `"recordId":"`, `"recordId":"tampered-`, 1)
		break
	}
	lines = rechainAuditTrail(t, lines)
	writeAuditLines(t, path, lines)

	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(lines, "\n")), sink.AuditVerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if report.OK() {
		t.Fatal("VerifyAuditLog() reported no anomalies, want the re-hashed tampering to be found")
	}
	if !hasAnomaly(report, sink.AuditAnomalySignature) {
		t.Errorf("anomalies = %v, want a bad signature", auditAnomalyTypes(report))
	}
}

// rechainAuditTrail recomputes every hash of a trail, the way an attacker
// who edited a record and wanted the chain to look intact would.
func rechainAuditTrail(t *testing.T, lines []string) []string {
	t.Helper()

	var header sink.AuditHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("header is not valid JSON: %v", err)
	}
	previous, err := hex.DecodeString(header.Hash)
	if err != nil {
		t.Fatalf("header hash is not valid hex: %v", err)
	}

	out := append([]string(nil), lines...)
	for i, line := range out {
		switch entryType(t, line) {
		case sink.AuditEntryTypeRecord:
			var record sink.AuditRecord
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("record is not valid JSON: %v", err)
			}
			body, err := json.Marshal(auditContent{
				Seq:        record.Seq,
				Ledger:     record.Ledger,
				Time:       record.Time,
				RecordID:   record.RecordID,
				RecordKind: record.RecordKind,
				Record:     record.Record,
			})
			if err != nil {
				t.Fatalf("encoding record content: %v", err)
			}
			digest := sha256.New()
			digest.Write(previous)
			digest.Write(body)
			hash := digest.Sum(nil)

			var entry map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("record is not valid JSON: %v", err)
			}
			entry["prevHash"] = json.RawMessage(`"` + hex.EncodeToString(previous) + `"`)
			entry["hash"] = json.RawMessage(`"` + hex.EncodeToString(hash) + `"`)
			encoded, err := json.Marshal(entry)
			if err != nil {
				t.Fatalf("encoding record: %v", err)
			}
			out[i] = string(encoded)
			previous = hash
		case sink.AuditEntryTypeCheckpoint:
			var checkpoint map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &checkpoint); err != nil {
				t.Fatalf("checkpoint is not valid JSON: %v", err)
			}
			checkpoint["hash"] = json.RawMessage(`"` + hex.EncodeToString(previous) + `"`)
			encoded, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatalf("encoding checkpoint: %v", err)
			}
			out[i] = string(encoded)
		}
	}
	return out
}

func TestAuditTrailContinuesAcrossRuns(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	records := auditRecords()

	writeAuditTrail(t, path, records[:2], sink.AuditOptions{CheckpointRecords: 1})
	writeAuditTrail(t, path, records[2:], sink.AuditOptions{CheckpointRecords: 1})

	lines := readAuditLines(t, path)
	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(lines, "\n")), sink.AuditVerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("VerifyAuditLog() reported anomalies: %v", auditAnomalyTypes(report))
	}
	if report.Records != len(records) {
		t.Errorf("records = %d, want %d", report.Records, len(records))
	}
}

func TestAuditTrailRefusesAnotherKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	writeAuditTrail(t, path, auditRecords()[:1], sink.AuditOptions{})

	other, err := sink.NewAuditSink(sink.AuditOptions{Path: path, KeyPath: filepath.Join(dir, "other.key")})
	if err == nil {
		_ = other.Close()
		t.Fatal("NewAuditSink() error = nil, want the trail to refuse another key")
	}
	if !strings.Contains(err.Error(), "different signing key") {
		t.Errorf("NewAuditSink() error = %v, want it to name the signing key", err)
	}
}

func TestAuditTrailDropsAPartialLine(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords()[:2], sink.AuditOptions{CheckpointRecords: 1})

	// An interrupted run leaves a half-written entry behind.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	if _, err := file.WriteString(`{"type":"audit_rec`); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("closing %s: %v", path, err)
	}

	writeAuditTrail(t, path, auditRecords()[2:], sink.AuditOptions{CheckpointRecords: 1})

	lines := readAuditLines(t, path)
	for _, line := range lines {
		if strings.Contains(line, "audit_rec\"") && !strings.HasSuffix(line, "}") {
			t.Fatalf("the trail still holds a partial entry: %s", line)
		}
	}

	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(lines, "\n")), sink.AuditVerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("VerifyAuditLog() reported anomalies: %v", auditAnomalyTypes(report))
	}
	if report.Records != len(auditRecords()) {
		t.Errorf("records = %d, want %d", report.Records, len(auditRecords()))
	}
}

func TestAuditSinkCheckpointsEveryRecord(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 0})

	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(readAuditLines(t, path), "\n")), sink.AuditVerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("VerifyAuditLog() reported anomalies: %v", auditAnomalyTypes(report))
	}
	if report.Checkpoints != report.Records {
		t.Errorf("checkpoints = %d, want one per record (%d)", report.Checkpoints, report.Records)
	}
}

func TestAuditSinkWritesTheSignedCheckpoint(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{CheckpointRecords: 1})

	lines := readAuditLines(t, path)

	var header sink.AuditHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("header is not valid JSON: %v", err)
	}
	public, err := base64.StdEncoding.DecodeString(header.PublicKey)
	if err != nil {
		t.Fatalf("public key is not valid base64: %v", err)
	}

	found := false
	for _, line := range lines {
		if entryType(t, line) != sink.AuditEntryTypeCheckpoint {
			continue
		}
		found = true
		var checkpoint sink.AuditCheckpoint
		if err := json.Unmarshal([]byte(line), &checkpoint); err != nil {
			t.Fatalf("checkpoint is not valid JSON: %v", err)
		}
		if checkpoint.Ledger == 0 {
			t.Error("checkpoint has no ledger, want the ledger it covers")
		}
		hash, err := hex.DecodeString(checkpoint.Hash)
		if err != nil {
			t.Fatalf("checkpoint hash is not valid hex: %v", err)
		}
		signature, err := base64.StdEncoding.DecodeString(checkpoint.Signature)
		if err != nil {
			t.Fatalf("checkpoint signature is not valid base64: %v", err)
		}
		if !ed25519.Verify(ed25519.PublicKey(public), hash, signature) {
			t.Errorf("checkpoint %d does not verify against the key the header names", checkpoint.Seq)
		}
	}
	if !found {
		t.Fatal("the trail holds no checkpoint")
	}
}

func TestAuditSinkReportsAnUnexpectedKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords(), sink.AuditOptions{})

	otherPublic, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	report, err := sink.VerifyAuditLog(strings.NewReader(strings.Join(readAuditLines(t, path), "\n")),
		sink.AuditVerifyOptions{PublicKey: otherPublic})
	if err != nil {
		t.Fatalf("VerifyAuditLog() error = %v", err)
	}
	if report.OK() {
		t.Fatal("VerifyAuditLog() reported no anomalies, want the key mismatch to be found")
	}
	if !hasAnomaly(report, sink.AuditAnomalyKeyMismatch) {
		t.Errorf("anomalies = %v, want a key mismatch", auditAnomalyTypes(report))
	}
}

func TestAuditSinkValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.AuditOptions
	}{
		{name: "missing path", opts: sink.AuditOptions{}},
		{name: "blank path", opts: sink.AuditOptions{Path: "   "}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := sink.NewAuditSink(tc.opts); err == nil {
				t.Fatal("NewAuditSink() error = nil, want a validation failure")
			}
		})
	}
}

func TestAuditSinkRejectsAnUnusableKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	if err := os.WriteFile(keyPath, []byte("not a key"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", keyPath, err)
	}

	if _, err := sink.NewAuditSink(sink.AuditOptions{Path: filepath.Join(dir, "audit.log"), KeyPath: keyPath}); err == nil {
		t.Fatal("NewAuditSink() error = nil, want the unusable key to be reported")
	}
}

func TestAuditSinkRefusesToEmitAfterClose(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	trail, err := sink.NewAuditSink(sink.AuditOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAuditSink() error = %v", err)
	}
	if err := trail.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := trail.Close(); err != nil {
		t.Fatalf("Close() error = %v, want a second close to be harmless", err)
	}

	if err := trail.Emit(eventRecord()); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Emit() error = %v, want it to wrap os.ErrClosed", err)
	}
}

func TestAuditSinkCreatesTheSigningKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit.log")
	writeAuditTrail(t, path, auditRecords()[:1], sink.AuditOptions{})

	info, err := os.Stat(path + ".key")
	if err != nil {
		t.Fatalf("the signing key was not created: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("signing key mode = %o, want 600", mode)
	}
}
