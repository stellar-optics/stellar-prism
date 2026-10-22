package sink

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// The anomalies a verification can report. Every one of them names the kind
// of tampering that was found, so an auditor can tell a modified record from
// a deleted one.
const (
	// AuditAnomalyMalformed means a line could not be read as an entry.
	AuditAnomalyMalformed = "malformed"
	// AuditAnomalyModified means a record's contents no longer match the
	// hash that was written with them.
	AuditAnomalyModified = "modified"
	// AuditAnomalyDeleted means records are missing from the chain.
	AuditAnomalyDeleted = "deleted"
	// AuditAnomalyInserted means a record was inserted or replayed.
	AuditAnomalyInserted = "inserted"
	// AuditAnomalyChainBreak means an entry does not link to the one before
	// it.
	AuditAnomalyChainBreak = "chain_break"
	// AuditAnomalyCheckpoint means a signed checkpoint no longer matches
	// the chain it covers.
	AuditAnomalyCheckpoint = "checkpoint_mismatch"
	// AuditAnomalySignature means a checkpoint signature does not verify.
	AuditAnomalySignature = "bad_signature"
	// AuditAnomalyHeader means the header was changed or is unsupported.
	AuditAnomalyHeader = "header_mismatch"
	// AuditAnomalyKeyMismatch means the trail is signed with a different
	// key than the one the reader expected.
	AuditAnomalyKeyMismatch = "key_mismatch"
)

// The severities an anomaly can carry. Only an error makes a verification
// fail; a warning is reported but does not condemn the trail.
const (
	// AuditSeverityError marks an anomaly that fails the verification.
	AuditSeverityError = "error"
	// AuditSeverityWarning marks an anomaly that is reported but does not
	// fail the verification.
	AuditSeverityWarning = "warning"
)

// AuditAnomaly is one problem found in an audit log.
type AuditAnomaly struct {
	// Type names the kind of tampering, one of the AuditAnomaly constants.
	Type string
	// Line is the 1-indexed line of the log the problem was found on.
	Line int
	// Seq is the chain sequence of the entry, when it has one.
	Seq uint32
	// Message explains the problem.
	Message string
	// Severity is AuditSeverityError or AuditSeverityWarning.
	Severity string
}

// AuditVerification is the result of verifying an audit log.
type AuditVerification struct {
	// Records is the number of audited records the log holds.
	Records int
	// Checkpoints is the number of signed checkpoints the log holds.
	Checkpoints int
	// LedgerMin and LedgerMax span the ledgers the log covers.
	LedgerMin uint32
	LedgerMax uint32
	// FinalHash is the chain hash the log ends on.
	FinalHash string
	// PublicKey is the base64 encoded key the checkpoints are signed with.
	PublicKey string
	// Anomalies holds every problem that was found.
	Anomalies []AuditAnomaly
}

// OK reports whether the log verified without an error.
//
// A warning does not fail the verification: a trail without a checkpoint is
// still a chain, it just cannot prove who wrote it.
func (v *AuditVerification) OK() bool {
	if v == nil {
		return false
	}
	for _, anomaly := range v.Anomalies {
		if anomaly.Severity == AuditSeverityError {
			return false
		}
	}
	return true
}

// AuditVerifyOptions configures a verification.
type AuditVerifyOptions struct {
	// PublicKey is the Ed25519 public key the trail is expected to be
	// signed with. Empty accepts whatever key the header names.
	PublicKey ed25519.PublicKey
}

// VerifyAuditLog reads an audit log and checks it end to end.
//
// It recomputes the hash chain from the header, checks that every record
// links to the one before it, checks that every signed checkpoint still
// matches the chain it covers and still verifies against the public key.
//
// The returned report names every problem it found, so a modified, deleted
// or inserted line is reported with the line it is on. A nil error means the
// log could be read; the report says whether it verified.
func VerifyAuditLog(r io.Reader, opts AuditVerifyOptions) (*AuditVerification, error) {
	if r == nil {
		return nil, fmt.Errorf("audit: reader is required")
	}

	v := &auditVerifier{
		result:      &AuditVerification{},
		expectedKey: opts.PublicKey,
	}

	reader := bufio.NewReader(r)
	lineNo := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
				v.processLine(lineNo, trimmed)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("audit: reading log: %w", readErr)
		}
	}

	if v.header == nil {
		v.add(AuditAnomalyMalformed, 0, 0, "the log has no header")
	} else if !v.sawCheckpoint {
		v.warn(AuditAnomalyHeader, 1, 0, "the log holds no signed checkpoint")
	}
	if v.result.Records == 0 {
		v.warn(AuditAnomalyHeader, 0, 0, "the log holds no records")
	}

	v.result.FinalHash = hex.EncodeToString(v.chain)
	return v.result, nil
}

// auditVerifier holds the state of one verification.
type auditVerifier struct {
	result      *AuditVerification
	expectedKey ed25519.PublicKey

	header        *AuditHeader
	publicKey     ed25519.PublicKey
	chain         []byte
	records       uint32
	sawHeader     bool
	sawCheckpoint bool
	haveLedger    bool
}

// add records an anomaly.
func (v *auditVerifier) add(kind string, line int, seq uint32, message string) {
	v.result.Anomalies = append(v.result.Anomalies, AuditAnomaly{
		Type:     kind,
		Line:     line,
		Seq:      seq,
		Message:  message,
		Severity: AuditSeverityError,
	})
}

// warn records an anomaly that does not fail the verification.
func (v *auditVerifier) warn(kind string, line int, seq uint32, message string) {
	v.result.Anomalies = append(v.result.Anomalies, AuditAnomaly{
		Type:     kind,
		Line:     line,
		Seq:      seq,
		Message:  message,
		Severity: AuditSeverityWarning,
	})
}

// processLine dispatches one entry of the log.
func (v *auditVerifier) processLine(lineNo int, line []byte) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the entry is not valid JSON: %v", err))
		return
	}

	switch envelope.Type {
	case AuditEntryTypeHeader:
		v.processHeader(lineNo, line)
	case AuditEntryTypeRecord:
		v.processRecord(lineNo, line)
	case AuditEntryTypeCheckpoint:
		v.processCheckpoint(lineNo, line)
	default:
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the entry has unknown type %q", envelope.Type))
	}
}

// processHeader checks the header and starts the chain at the genesis hash.
func (v *auditVerifier) processHeader(lineNo int, line []byte) {
	if v.sawHeader {
		v.add(AuditAnomalyInserted, lineNo, 0, "the log holds a second header")
		return
	}
	v.sawHeader = true

	var header AuditHeader
	if err := json.Unmarshal(line, &header); err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the header is not a valid audit header: %v", err))
		return
	}
	v.header = &header
	v.result.PublicKey = header.PublicKey

	if header.Version != AuditVersion {
		v.add(AuditAnomalyHeader, lineNo, 0, fmt.Sprintf("the log is version %d, want %d", header.Version, AuditVersion))
	}
	if header.Chain != AuditChainAlgorithm {
		v.add(AuditAnomalyHeader, lineNo, 0, fmt.Sprintf("the log chains with %q, want %q", header.Chain, AuditChainAlgorithm))
	}

	body, err := json.Marshal(header.content())
	if err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the header cannot be hashed: %v", err))
		return
	}
	genesis := sha256.Sum256(body)
	if hex.EncodeToString(genesis[:]) != header.Hash {
		v.add(AuditAnomalyHeader, lineNo, 0, "the header hash does not match the header: the header was modified")
	}

	// The chain continues from the hash the log carries, so the records
	// that follow are still checked against each other.
	stored, err := hex.DecodeString(header.Hash)
	if err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, "the header hash is not a valid hash")
	} else {
		v.chain = stored
	}

	key, err := base64.StdEncoding.DecodeString(header.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		v.add(AuditAnomalyHeader, lineNo, 0, "the header does not carry a readable public key")
		return
	}
	v.publicKey = ed25519.PublicKey(key)
	if v.expectedKey != nil && !bytes.Equal(v.expectedKey, v.publicKey) {
		v.add(AuditAnomalyKeyMismatch, lineNo, 0, "the trail is signed with a different key than the one expected")
	}
}

// processRecord checks one audited record.
func (v *auditVerifier) processRecord(lineNo int, line []byte) {
	var record AuditRecord
	if err := json.Unmarshal(line, &record); err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the entry is not a valid audit record: %v", err))
		return
	}
	v.result.Records++

	expected := v.records + 1
	switch {
	case record.Seq > expected:
		v.add(AuditAnomalyDeleted, lineNo, record.Seq,
			fmt.Sprintf("expected record %d, found %d: %d record(s) were deleted", expected, record.Seq, record.Seq-expected))
	case record.Seq < expected:
		v.add(AuditAnomalyInserted, lineNo, record.Seq,
			fmt.Sprintf("expected record %d, found %d: a record was inserted or replayed", expected, record.Seq))
	}

	if record.PrevHash != hex.EncodeToString(v.chain) {
		v.add(AuditAnomalyChainBreak, lineNo, record.Seq,
			"the previous hash does not match the entry before it: the chain was broken")
	}

	// The record's own hash must match its contents. It is computed over
	// the hash the record names, so a record that was edited is reported
	// even when the chain around it was rebuilt.
	prev, err := hex.DecodeString(record.PrevHash)
	if err != nil {
		v.add(AuditAnomalyMalformed, lineNo, record.Seq, "the previous hash is not a valid hash")
		prev = nil
	}
	computed, err := auditChainHash(prev, record.content())
	switch {
	case err != nil:
		v.add(AuditAnomalyMalformed, lineNo, record.Seq, fmt.Sprintf("the record cannot be hashed: %v", err))
	case hex.EncodeToString(computed) != record.Hash:
		v.add(AuditAnomalyModified, lineNo, record.Seq,
			"the record hash does not match its contents: the record was modified")
	}

	// The chain continues with the hash the log carries, so one damaged
	// record is reported once instead of marking every later record.
	stored, err := hex.DecodeString(record.Hash)
	if err != nil {
		v.add(AuditAnomalyMalformed, lineNo, record.Seq, "the record hash is not a valid hash")
	} else {
		v.chain = stored
	}
	v.records = record.Seq

	if !v.haveLedger || record.Ledger < v.result.LedgerMin {
		v.result.LedgerMin = record.Ledger
	}
	if !v.haveLedger || record.Ledger > v.result.LedgerMax {
		v.result.LedgerMax = record.Ledger
	}
	v.haveLedger = true
}

// processCheckpoint checks one signed checkpoint.
func (v *auditVerifier) processCheckpoint(lineNo int, line []byte) {
	var checkpoint AuditCheckpoint
	if err := json.Unmarshal(line, &checkpoint); err != nil {
		v.add(AuditAnomalyMalformed, lineNo, 0, fmt.Sprintf("the entry is not a valid audit checkpoint: %v", err))
		return
	}
	v.sawCheckpoint = true
	v.result.Checkpoints++

	if checkpoint.Seq != v.records {
		v.add(AuditAnomalyCheckpoint, lineNo, checkpoint.Seq,
			fmt.Sprintf("the checkpoint covers %d records but %d were read", checkpoint.Seq, v.records))
	}
	if checkpoint.Hash != hex.EncodeToString(v.chain) {
		v.add(AuditAnomalyCheckpoint, lineNo, checkpoint.Seq,
			"the checkpoint hash does not match the chain: records were removed or changed")
	}

	hash, err := hex.DecodeString(checkpoint.Hash)
	if err != nil {
		v.add(AuditAnomalyMalformed, lineNo, checkpoint.Seq, "the checkpoint hash is not a valid hash")
		return
	}
	if v.publicKey == nil {
		v.warn(AuditAnomalySignature, lineNo, checkpoint.Seq,
			"the checkpoint cannot be verified: the trail carries no public key")
		return
	}
	signature, err := base64.StdEncoding.DecodeString(checkpoint.Signature)
	if err != nil {
		v.add(AuditAnomalySignature, lineNo, checkpoint.Seq, "the checkpoint signature is not valid base64")
		return
	}
	if !ed25519.Verify(v.publicKey, hash, signature) {
		v.add(AuditAnomalySignature, lineNo, checkpoint.Seq,
			"the checkpoint signature does not verify: the checkpoint was forged or changed")
	}
}
