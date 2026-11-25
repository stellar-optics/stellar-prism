package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// replayFlags holds options specific to the `prism replay` command.
type replayFlags struct {
	archiveURL  string
	startLedger uint32
	endLedger   uint32
	concurrency int
	asJSON      bool
	raw         bool
	sinks       sinkFlags
}

// newReplayCmd builds the `prism replay` command.
func newReplayCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var rf replayFlags

	cmd := &cobra.Command{
		Use:   "replay --archive-url <url> --start-ledger <N> --end-ledger <M> [sink_flags]",
		Short: "Stream historical ledgers from Stellar history archive buckets",
		Long: `Download and parse historical ledger close metadata directly from Stellar
history archive buckets (GZ/XDR files) offline, without contacting live RPC nodes.

Historical records can be streamed to stdout or fanned out into any configured sink
(Parquet, ClickHouse, PostgreSQL, Kafka, etc.).`,
		Example: `  # Replay a range of ledgers to stdout as JSON
  prism replay --archive-url https://history.stellar.org/prd/core-live/core_live_001 \
    --start-ledger 1000 --end-ledger 2000 --json

  # Replay into partitioned Parquet files offline
  prism replay --archive-url file:///data/stellar-archive \
    --start-ledger 50000 --end-ledger 60000 --sink-parquet-dir ./parquet_history

  # Replay directly into ClickHouse
  prism replay --archive-url https://history.stellar.org/prd/core-live/core_live_001 \
    --start-ledger 100000 --end-ledger 200000 --clickhouse-addr 127.0.0.1:9000`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := rf.validate(cmd); err != nil {
				return err
			}
			return runReplay(cmd.Context(), g, &rf, stdout, stderr)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&rf.archiveURL, "archive-url", "", "base URL or filesystem path of Stellar history archive")
	flags.Uint32Var(&rf.startLedger, "start-ledger", 0, "start ledger sequence (inclusive)")
	flags.Uint32Var(&rf.endLedger, "end-ledger", 0, "end ledger sequence (inclusive)")
	flags.IntVar(&rf.concurrency, "concurrency", 4, "concurrent download workers")
	flags.BoolVar(&rf.asJSON, "json", false, "emit NDJSON, one record per line")
	flags.BoolVar(&rf.raw, "raw", false, "leave XDR base64-encoded instead of decoding it")

	rf.sinks.register(cmd)
	return cmd
}

// NewReplayCmd returns the replay command for external callers.
func NewReplayCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	return newReplayCmd(g, stdout, stderr)
}

func (rf *replayFlags) validate(cmd *cobra.Command) error {
	if strings.TrimSpace(rf.archiveURL) == "" {
		return fmt.Errorf("--archive-url is required")
	}
	if rf.startLedger == 0 {
		return fmt.Errorf("--start-ledger must be greater than 0")
	}
	if rf.endLedger == 0 {
		return fmt.Errorf("--end-ledger must be greater than 0")
	}
	if rf.endLedger < rf.startLedger {
		return fmt.Errorf("--end-ledger (%d) cannot be less than --start-ledger (%d)", rf.endLedger, rf.startLedger)
	}
	if rf.concurrency <= 0 {
		rf.concurrency = 1
	}
	return rf.sinks.validate(cmd)
}

// runReplay coordinates concurrent archive file downloading, decompression,
// parsing, and feeding historical records into configured sinks in sequential order.
func runReplay(ctx context.Context, g *globalFlags, rf *replayFlags, stdout, stderr io.Writer) error {
	colorMode := "auto"
	if g != nil && g.color != "" {
		colorMode = g.color
	}

	var baseSink stream.Sink
	if rf.asJSON {
		baseSink = sink.NewNDJSON(stdout, !rf.raw)
	} else {
		baseSink = sink.NewHuman(stdout, lensformat.PaletteFor(stdout, colorMode), rf.raw)
	}

	outSink, err := rf.sinks.addTo(baseSink, !rf.raw, func(e error) {
		fmt.Fprintf(stderr, "prism replay: sink async error: %v\n", e)
	})
	if err != nil {
		return err
	}
	defer func() {
		if closer, ok := outSink.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	// Calculate 64-ledger checkpoints covering [startLedger, endLedger].
	// In Stellar history archives, checkpoint ledger is (seq | 0x3f).
	startChk := rf.startLedger | 0x3f
	endChk := rf.endLedger | 0x3f

	var checkpoints []uint32
	for chk := startChk; chk <= endChk; chk += 64 {
		checkpoints = append(checkpoints, chk)
	}

	type checkpointResult struct {
		checkpoint uint32
		metas      []xdr.LedgerCloseMeta
		err        error
	}

	resultsChan := make(chan checkpointResult, len(checkpoints))
	sem := make(chan struct{}, rf.concurrency)
	var wg sync.WaitGroup

	httpClient := &http.Client{Timeout: 30 * time.Second}

	for _, chk := range checkpoints {
		wg.Add(1)
		go func(chkSeq uint32) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				resultsChan <- checkpointResult{checkpoint: chkSeq, err: ctx.Err()}
				return
			}

			metas, err := fetchAndParseCheckpoint(ctx, httpClient, rf.archiveURL, chkSeq)
			resultsChan <- checkpointResult{checkpoint: chkSeq, metas: metas, err: err}
		}(chk)
	}

	// Close results channel once all workers finish
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	byCheckpoint := make(map[uint32][]xdr.LedgerCloseMeta)
	for res := range resultsChan {
		if res.err != nil {
			return fmt.Errorf("archive checkpoint 0x%08x: %w", res.checkpoint, res.err)
		}
		byCheckpoint[res.checkpoint] = res.metas
	}

	// Flatten and sort ledgers to ensure strictly sequential ingestion
	var allMetas []xdr.LedgerCloseMeta
	for _, chk := range checkpoints {
		metas := byCheckpoint[chk]
		for _, m := range metas {
			seq := m.LedgerSequence()
			if seq >= rf.startLedger && seq <= rf.endLedger {
				allMetas = append(allMetas, m)
			}
		}
	}

	sort.Slice(allMetas, func(i, j int) bool {
		return allMetas[i].LedgerSequence() < allMetas[j].LedgerSequence()
	})

	for _, m := range allMetas {
		record, err := ledgerCloseMetaToRecord(m)
		if err != nil {
			return err
		}
		if err := outSink.Emit(record); err != nil {
			return fmt.Errorf("emitting ledger %d to sink: %w", record.Sequence, err)
		}
	}

	if err := outSink.Flush(); err != nil {
		return fmt.Errorf("flushing sink: %w", err)
	}
	return nil
}

// checkpointRelativePaths returns candidate relative archive paths for a checkpoint.
func checkpointRelativePaths(chk uint32) []string {
	dir1 := fmt.Sprintf("%02x", uint8(chk>>24))
	dir2 := fmt.Sprintf("%02x", uint8(chk>>16))
	dir3 := fmt.Sprintf("%02x", uint8(chk>>8))

	return []string{
		fmt.Sprintf("history/%s/%s/%s/history-%08x.xdr.gz", dir1, dir2, dir3, chk),
		fmt.Sprintf("ledger/%s/%s/%s/ledger-%08x.xdr.gz", dir1, dir2, dir3, chk),
		fmt.Sprintf("%s/%s/%s/history-%08x.xdr.gz", dir1, dir2, dir3, chk),
		fmt.Sprintf("history-%08x.xdr.gz", chk),
	}
}

func fetchAndParseCheckpoint(ctx context.Context, client *http.Client, baseURL string, chk uint32) ([]xdr.LedgerCloseMeta, error) {
	relPaths := checkpointRelativePaths(chk)
	var lastErr error

	for _, rel := range relPaths {
		r, err := openArchiveResource(ctx, client, baseURL, rel)
		if err != nil {
			lastErr = err
			continue
		}
		defer r.Close()

		metas, err := parseLedgerCloseMetaStream(r)
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", rel, err)
		}
		return metas, nil
	}
	return nil, fmt.Errorf("checkpoint %08x not found in archive (%w)", chk, lastErr)
}

func openArchiveResource(ctx context.Context, client *http.Client, baseURL, relPath string) (io.ReadCloser, error) {
	// Check if baseURL is an HTTP(S) URL
	if strings.HasPrefix(baseURL, "http://") || strings.HasPrefix(baseURL, "https://") {
		targetURL := strings.TrimRight(baseURL, "/") + "/" + relPath
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
		}
		return resp.Body, nil
	}

	// Local filesystem path (either file:// or raw path)
	localDir := baseURL
	if strings.HasPrefix(localDir, "file://") {
		u, err := url.Parse(localDir)
		if err == nil {
			localDir = u.Path
		} else {
			localDir = strings.TrimPrefix(localDir, "file://")
		}
	}

	fullPath := filepath.Join(localDir, relPath)
	return os.Open(fullPath)
}

func parseLedgerCloseMetaStream(r io.Reader) ([]xdr.LedgerCloseMeta, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("decompressing gzip: %w", err)
	}
	defer gz.Close()

	// Read all decompressed bytes
	decompressed, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("reading decompressed data: %w", err)
	}

	if len(decompressed) == 0 {
		return nil, nil
	}

	// Try framed decoding first
	streamReader := io.NopCloser(bytes.NewReader(decompressed))
	xdrStream := xdr.NewStream(streamReader)
	var metas []xdr.LedgerCloseMeta

	for {
		var meta xdr.LedgerCloseMeta
		err := xdrStream.ReadOne(&meta)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// If framed parsing failed on first element, fallback to unframed decoding
			if len(metas) == 0 {
				return parseUnframedLedgerCloseMeta(decompressed)
			}
			return nil, fmt.Errorf("reading framed XDR: %w", err)
		}
		metas = append(metas, meta)
	}

	if len(metas) > 0 {
		return metas, nil
	}
	return parseUnframedLedgerCloseMeta(decompressed)
}

func parseUnframedLedgerCloseMeta(b []byte) ([]xdr.LedgerCloseMeta, error) {
	reader := bytes.NewReader(b)
	var metas []xdr.LedgerCloseMeta
	for reader.Len() > 0 {
		var meta xdr.LedgerCloseMeta
		_, err := xdr.Unmarshal(reader, &meta)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("unframed XDR unmarshal: %w", err)
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

func ledgerCloseMetaToRecord(m xdr.LedgerCloseMeta) (stream.Record, error) {
	seq := m.LedgerSequence()
	closedAt := m.ClosedAt()
	headerXDR, err := xdr.MarshalBase64(m.LedgerHeaderHistoryEntry())
	if err != nil {
		return stream.Record{}, fmt.Errorf("marshaling header XDR for ledger %d: %w", seq, err)
	}
	metaXDR, err := xdr.MarshalBase64(m)
	if err != nil {
		return stream.Record{}, fmt.Errorf("marshaling meta XDR for ledger %d: %w", seq, err)
	}
	lh := m.LedgerHash()
	hash := hex.EncodeToString(lh[:])

	return stream.Record{
		ID:       fmt.Sprintf("%010d", seq),
		Sequence: seq,
		Time:     closedAt,
		Kind:     stream.KindLedger,
		Close: &stream.LedgerClose{
			Hash:        hash,
			HeaderXDR:   headerXDR,
			MetadataXDR: metaXDR,
		},
	}, nil
}
