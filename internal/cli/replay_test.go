package cli_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

func createSampleLedgerCloseMeta(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{byte(seq >> 24), byte(seq >> 16), byte(seq >> 8), byte(seq)},
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(1700000000 + int64(seq)*5),
					},
				},
			},
		},
	}
}

func createArchiveFixture(t *testing.T, archiveDir string, chk uint32, ledgers []uint32) {
	t.Helper()

	dir1 := fmt.Sprintf("%02x", uint8(chk>>24))
	dir2 := fmt.Sprintf("%02x", uint8(chk>>16))
	dir3 := fmt.Sprintf("%02x", uint8(chk>>8))

	chkDir := filepath.Join(archiveDir, "history", dir1, dir2, dir3)
	if err := os.MkdirAll(chkDir, 0o755); err != nil {
		t.Fatalf("creating archive fixture dir: %v", err)
	}

	filePath := filepath.Join(chkDir, fmt.Sprintf("history-%08x.xdr.gz", chk))
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("creating fixture file %s: %v", filePath, err)
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	for _, seq := range ledgers {
		meta := createSampleLedgerCloseMeta(seq)
		if err := xdr.MarshalFramed(gz, meta); err != nil {
			t.Fatalf("writing framed ledger meta %d: %v", seq, err)
		}
	}
}

func TestReplayCommandOfflineIntegration(t *testing.T) {
	archiveDir := t.TempDir()

	// Checkpoint 63 covers ledgers 0-63
	createArchiveFixture(t, archiveDir, 63, []uint32{60, 61, 62, 63})
	// Checkpoint 127 covers ledgers 64-127
	createArchiveFixture(t, archiveDir, 127, []uint32{64, 65, 66})

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	args := []string{
		"replay",
		"--archive-url", archiveDir,
		"--start-ledger", "61",
		"--end-ledger", "65",
		"--json",
	}

	err := cli.Execute(args, &stdout, &stderr)
	if err != nil {
		t.Fatalf("Execute(replay) error = %v, stderr = %s", err, stderr.String())
	}

	out := stdout.String()
	// Should contain ledgers 61, 62, 63, 64, 65
	for _, wantSeq := range []string{"0000000061", "0000000062", "0000000063", "0000000064", "0000000065"} {
		if !strings.Contains(out, wantSeq) {
			t.Errorf("stdout does not contain ledger %s:\n%s", wantSeq, out)
		}
	}

	// Ledger 60 and 66 should be outside boundaries
	if strings.Contains(out, "0000000060") {
		t.Errorf("stdout contains ledger 60 which is below start boundary:\n%s", out)
	}
	if strings.Contains(out, "0000000066") {
		t.Errorf("stdout contains ledger 66 which is above end boundary:\n%s", out)
	}
}

func TestReplayCommandValidatesBoundaries(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	// start > end
	err := cli.Execute([]string{
		"replay",
		"--archive-url", t.TempDir(),
		"--start-ledger", "100",
		"--end-ledger", "50",
	}, &stdout, &stderr)

	if err == nil {
		t.Fatal("Execute() error = nil, want boundary validation failure")
	}
	if !strings.Contains(err.Error(), "cannot be less than") {
		t.Errorf("error = %q, want boundary error", err)
	}
}

func TestReplayCommandSinkFanOutParquet(t *testing.T) {
	archiveDir := t.TempDir()
	parquetDir := t.TempDir()

	createArchiveFixture(t, archiveDir, 63, []uint32{10, 11, 12})

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	args := []string{
		"replay",
		"--archive-url", archiveDir,
		"--start-ledger", "10",
		"--end-ledger", "12",
		"--sink-parquet-dir", parquetDir,
	}

	err := cli.Execute(args, &stdout, &stderr)
	if err != nil {
		t.Fatalf("Execute(replay to parquet) error = %v, stderr = %s", err, stderr.String())
	}

	// Verify part files were written to parquetDir
	entries, err := os.ReadDir(parquetDir)
	if err != nil {
		t.Fatalf("reading parquet dir: %v", err)
	}
	if len(entries) == 0 {
		t.Errorf("expected parquet files in %s, found 0", parquetDir)
	}
}
