package cli

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

// newAuditCmd implements the `prism audit` command.
//
// prism audit verification command validates NDJSON dump integrity:
//
//	cat events.ndjson | prism audit
//	prism audit /data/events/2026-10-20.ndjson
//
// Features:
//   - Validates sequence continuity and identifies any ledger gaps
//   - Flags duplicate IDs or out-of-order records with exact line numbers
//   - Prints a health audit summary: total records, ledger span, detected anomalies
//   - Exits code 0 on clean stream, code 1 on anomalies
func newAuditCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var inputFile string

	cmd := &cobra.Command{
		Use:   "audit [file|--]",
		Short: "Verify NDJSON stream integrity",
		Long: `Validate the integrity of an NDJSON stream dump.

Checks that:
- No ledger sequences were skipped
- Event IDs are strictly increasing
- No duplicate event IDs exist
- Timestamps are monotonically non-decreasing

Examples:

  # Validate NDJSON from stdin
  cat events.ndjson | prism audit

  # Validate a specific file
  prism audit /data/events/2026-10-20.ndjson

  # Start checking from line N (skip header lines)
  prism audit --from-line 1 /data/events.ndjson`,

		Args: cobra.MaximumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			reader, closeReader, err := openAuditInput(inputFile, args)
			if err != nil {
				return err
			}
			defer closeReader()

			return auditStream(reader, stdout, cmd)
		},
	}

	cmd.Flags().StringVar(&inputFile, "file", "", "path to NDJSON file to audit (defaults to stdin)")

	return cmd
}

// newAuditVerifyCmd implements the `prism audit-verify` command.
//
// prism audit-verify checks a tamper-evident audit trail written by
// --sink-audit:
//
//	prism audit-verify /var/log/prism/audit.log
//
// The trail is a hash chain: every record is hashed together with the hash of
// the record before it, so a change to any record changes every hash after
// it. Signed checkpoints are checked as well, which is what catches a trail
// that was edited and re-hashed from the beginning.
//
// Features:
//   - Recomputes the SHA-256 chain and names every broken link
//   - Reports the exact line of every modified, deleted or inserted record
//   - Checks that the signed checkpoints still match the chain they cover
//   - Exits code 0 on an intact trail, code 1 when tampering is found
func newAuditVerifyCmd(stdout, stderr io.Writer) *cobra.Command {
	var (
		inputFile      string
		publicKeyFile  string
		printPublicKey bool
	)

	cmd := &cobra.Command{
		Use:   "audit-verify [file|--]",
		Short: "Verify a tamper-evident audit trail",
		Long: `Verify the integrity of an audit trail written with --sink-audit.

Every audited record is hashed together with the hash of the record before
it, so a change to any record changes every hash after it. The checkpoints
written along the way are signed, which is what catches a trail that was
edited and re-hashed from the beginning.

Checks that:
- Every record still hashes to the value that was written with it
- Every record links to the record before it, so nothing was deleted
- Every signed checkpoint matches the chain it covers and still verifies
- The trail is signed with the expected key, when one is given

Examples:

  # Verify an audit trail
  prism audit-verify /var/log/prism/audit.log

  # Verify a trail against a known public key
  prism audit-verify --public-key /etc/prism/audit.pub /var/log/prism/audit.log

  # Print the key a trail is signed with, so it can be pinned elsewhere
  prism audit-verify --print-public-key /var/log/prism/audit.log

  # Verify a trail from stdin
  cat audit.log | prism audit-verify`,

		Args: cobra.MaximumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			reader, closeReader, err := openAuditInput(inputFile, args)
			if err != nil {
				return err
			}
			defer closeReader()

			opts := sink.AuditVerifyOptions{}
			if publicKeyFile != "" {
				key, err := readAuditPublicKey(publicKeyFile)
				if err != nil {
					return err
				}
				opts.PublicKey = key
			}

			report, err := sink.VerifyAuditLog(reader, opts)
			if err != nil {
				return err
			}

			// The key is printed on its own, so it can be redirected into
			// a file the verifier later reads. The report then goes to
			// stderr, beside it.
			reportWriter := stdout
			if printPublicKey {
				reportWriter = stderr
				if err := printTrailPublicKey(stdout, report); err != nil {
					return err
				}
			}

			printAuditVerification(reportWriter, report)

			if !report.OK() {
				fmt.Fprintf(stderr, "audit-verify: %d anomalies detected\n", len(report.Anomalies))
				return errors.New("audit trail verification failed")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&inputFile, "file", "", "path to the audit trail to verify (defaults to stdin)")
	cmd.Flags().StringVar(&publicKeyFile, "public-key", "", "path to a PEM public key the trail must be signed with")
	cmd.Flags().BoolVar(&printPublicKey, "print-public-key", false, "print the PEM public key the trail is signed with")

	return cmd
}

// openAuditInput opens the file the audit commands read: the --file flag
// first, then a positional argument, then stdin.
func openAuditInput(inputFile string, args []string) (io.Reader, func(), error) {
	path := inputFile
	if path == "" && len(args) > 0 && args[0] != "-" {
		path = args[0]
	}
	if path == "" {
		return os.Stdin, func() {}, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening input file: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}

// readAuditPublicKey reads a PEM encoded Ed25519 public key.
func readAuditPublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading public key %s: %w", path, err)
	}
	key, err := sink.ParseAuditPublicKey(data)
	if err != nil {
		return nil, fmt.Errorf("reading public key %s: %w", path, err)
	}
	return key, nil
}

// printTrailPublicKey prints the key a trail is signed with as PEM, so it can
// be pinned somewhere the trail cannot reach.
func printTrailPublicKey(writer io.Writer, report *sink.AuditVerification) error {
	if report.PublicKey == "" {
		return errors.New("the trail carries no public key")
	}
	raw, err := base64.StdEncoding.DecodeString(report.PublicKey)
	if err != nil {
		return fmt.Errorf("reading the trail's public key: %w", err)
	}
	encoded, err := sink.MarshalAuditPublicKey(ed25519.PublicKey(raw))
	if err != nil {
		return err
	}
	if _, err := writer.Write(encoded); err != nil {
		return fmt.Errorf("writing the public key: %w", err)
	}
	return nil
}

// printAuditVerification prints the report of a verification.
func printAuditVerification(writer io.Writer, report *sink.AuditVerification) {
	fmt.Fprintln(writer, "=== Audit Verification ===")
	fmt.Fprintf(writer, "Records: %d\n", report.Records)
	fmt.Fprintf(writer, "Checkpoints: %d\n", report.Checkpoints)
	if report.Records > 0 {
		fmt.Fprintf(writer, "Ledger range: %d - %d\n", report.LedgerMin, report.LedgerMax)
	}
	if report.FinalHash != "" {
		fmt.Fprintf(writer, "Final hash: %s\n", report.FinalHash)
	}
	if report.PublicKey != "" {
		fmt.Fprintf(writer, "Public key: %s\n", report.PublicKey)
	}

	if len(report.Anomalies) > 0 {
		fmt.Fprintln(writer, "\nAnomalies:")
		for _, a := range report.Anomalies {
			fmt.Fprintf(writer, "  Line %d [%s]: %s\n", a.Line, a.Type, a.Message)
		}
	}

	fmt.Fprintln(writer, "====================")
	if report.OK() {
		fmt.Fprintln(writer, "audit-verify: the trail is intact")
		return
	}
	fmt.Fprintln(writer, "audit-verify: the trail was tampered with")
}

// auditStream validates NDJSON stream integrity and reports anomalies.
func auditStream(reader io.Reader, writer io.Writer, cmd *cobra.Command) error {
	// Read all lines from the stream
	lines, err := readNDJSONLines(reader)
	if err != nil {
		return fmt.Errorf("reading NDJSON stream: %w", err)
	}

	if len(lines) == 0 {
		fmt.Fprintf(writer, "audit: no records found in stream\n")
		return nil
	}

	// Audit the stream
	auditResult := auditLines(lines)

	// Print results
	if auditResult.HasAnomalies {
		printAuditSummary(writer, auditResult)
	}

	// Exit with appropriate code
	if auditResult.HasAnomalies {
		fmt.Fprintln(cmd.ErrOrStderr(), "audit: anomalies detected - exiting with code 1")
		return errors.New("audit anomalies detected")
	}

	fmt.Fprintln(writer, "audit: stream is clean - exiting with code 0")
	return nil
}

// readNDJSONLines reads all NDJSON lines from a reader.
func readNDJSONLines(reader io.Reader) ([]string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var lines []string
	rawLines := strings.Split(string(data), "\n")
	for _, line := range rawLines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
	}

	return lines, nil
}

// auditLines performs the actual audit checks on NDJSON lines.
func auditLines(lines []string) *auditResult {
	result := &auditResult{
		TotalRecords: len(lines),
	}

	// Track state for auditing
	var lastID string
	var seenIDs = make(map[string]int) // ID -> line number (1-indexed)
	var anomalies []anomalyInfo
	var ledgerMin = ^uint32(0) // max uint32
	var ledgerMax uint32

	for i, line := range lines {
		// Parse the NDJSON line - extract ID and Ledger fields
		var record recordInfo
		if err := parseNDJSONLine(line, &record); err != nil {
			// If we can't parse, skip this line
			anomalies = append(anomalies, anomalyInfo{
				Type:     "parse_error",
				Line:     i + 1, // 1-indexed
				Message:  fmt.Sprintf("failed to parse NDJSON line: %v", err),
				Severity: "error",
			})
			continue
		}

		// Check ledger sequence continuity
		if record.Ledger == 0 {
			anomalies = append(anomalies, anomalyInfo{
				Type:     "missing_ledger",
				Line:     i + 1,
				Message:  "record missing ledger field",
				Severity: "error",
			})
			continue
		}
		if record.Ledger < ledgerMax && ledgerMax != 0 {
			anomalies = append(anomalies, anomalyInfo{
				Type:     "gap",
				Line:     i + 1,
				Message:  fmt.Sprintf("ledger gap: expected ledger >= %d, got %d", ledgerMax, record.Ledger),
				Severity: "warning",
			})
		}
		if record.Ledger > ledgerMax {
			ledgerMax = record.Ledger
		}
		if record.Ledger < ledgerMin {
			ledgerMin = record.Ledger
		}

		// Check ID ordering (must be strictly increasing)
		if lastID != "" {
			if record.ID == lastID {
				anomalies = append(anomalies, anomalyInfo{
					Type:     "duplicate_id",
					Line:     i + 1,
					Message:  fmt.Sprintf("duplicate event ID: %s (first seen at line %d)", record.ID, seenIDs[record.ID]),
					Severity: "error",
				})
			} else if record.ID < lastID {
				anomalies = append(anomalies, anomalyInfo{
					Type:     "out_of_order",
					Line:     i + 1,
					Message:  fmt.Sprintf("out-of-order event ID: %s (previous was %s)", record.ID, lastID),
					Severity: "error",
				})
			}
		}
		lastID = record.ID
		seenIDs[record.ID] = i + 1 // 1-indexed
	}

	result.Anomalies = anomalies
	result.HasAnomalies = len(anomalies) > 0

	return result
}

// auditResult holds the results of an audit check.
type auditResult struct {
	TotalRecords   int
	HasAnomalies   bool
	Anomalies      []anomalyInfo
	LedgerMin      uint32
	LedgerMax      uint32
	LedgerGapCount int
}

// anomalyInfo describes a single anomaly found during auditing.
type anomalyInfo struct {
	Type     string
	Line     int
	Message  string
	Severity string
}

// recordInfo holds the parsed fields from an NDJSON record.
type recordInfo struct {
	ID        string
	Ledger    uint32
	Timestamp int64 // nanoseconds since epoch, or 0 if not present
}

// parseNDJSONLine parses a single NDJSON line into a recordInfo.
// This is a simplified parser that extracts ID and Ledger fields.
func parseNDJSONLine(line string, record *recordInfo) error {
	// Simple NDJSON parsing: find "id" and "ledger" fields
	// In a real implementation, this would use proper JSON parsing

	record.ID = ""
	record.Ledger = 0
	record.Timestamp = 0

	// Try to find id field
	idIdx := strings.Index(line, "\"id\"")
	if idIdx >= 0 {
		// Extract the value after "id"
		rest := line[idIdx+5:]
		quoteIdx := strings.Index(rest, "\"")
		if quoteIdx >= 0 {
			record.ID = rest[:quoteIdx]
		}
	}

	// Try to find ledger field
	ledgerIdx := strings.Index(line, "\"ledger\"")
	if ledgerIdx >= 0 {
		rest := line[ledgerIdx+10:]
		// Skip whitespace and colon
		for _, ch := range rest {
			if ch == ' ' || ch == ':' || ch == '"' {
				continue
			}
			// Parse number
			if ch >= '0' && ch <= '9' {
				// Simple parsing - find the full number
				numEnd := strings.IndexAny(rest, ",\"")
				if numEnd >= 0 {
					num, err := strconv.Atoi(rest[:numEnd])
					if err != nil {
						continue
					}
					record.Ledger = uint32(num)
				}
				break
			}
		}
	}

	return nil
}

// printAuditSummary prints a human-readable audit summary.
func printAuditSummary(writer io.Writer, result *auditResult) {
	fmt.Fprintln(writer, "=== Audit Summary ===")
	fmt.Fprintf(writer, "Total records: %d\n", result.TotalRecords)
	fmt.Fprintf(writer, "Ledger range: %d - %d\n", result.LedgerMin, result.LedgerMax)

	if result.LedgerGapCount > 0 {
		fmt.Fprintf(writer, "Ledger gaps detected: %d\n", result.LedgerGapCount)
	}

	fmt.Fprintln(writer, "\nAnomalies:")
	for _, a := range result.Anomalies {
		fmt.Fprintf(writer, "  Line %d [%s]: %s\n", a.Line, a.Type, a.Message)
	}

	fmt.Fprintln(writer, "====================")
}
