package sink_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// scValU32 is a real, decodable ScVal carrying the u32 value 7.
const scValU32 = "AAAAAwAAAAc="

func eventRecord() stream.Record {
	return stream.Record{
		ID:       "0000000429496729600-0000000000",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			Type:       "contract",
			ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			TxHash:     "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
			TxIndex:    1,
			OpIndex:    0,
			TopicXDR:   []string{scValU32},
			ValueXDR:   scValU32,
		},
	}
}

func ledgerRecord() stream.Record {
	return stream.Record{
		ID:       "0000000100",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindLedger,
		Close: &stream.LedgerClose{
			Hash: "deadbeef",
		},
	}
}

func TestNDJSONShapeIsStable(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := sink.NewNDJSON(&buf, true)
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Exactly one line: NDJSON framing is the whole contract.
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("emitted %d lines for one record, want 1:\n%s", len(lines), buf.String())
	}

	var rec struct {
		Kind     string            `json:"kind"`
		ID       string            `json:"id"`
		Ledger   uint32            `json:"ledger"`
		Time     string            `json:"time"`
		Type     string            `json:"type"`
		Contract string            `json:"contract"`
		Tx       string            `json:"tx"`
		Topics   []json.RawMessage `json:"topics"`
		Value    json.RawMessage   `json:"value"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, lines[0])
	}

	if rec.Kind != "event" {
		t.Errorf("kind = %q, want event", rec.Kind)
	}
	if rec.Ledger != 100 {
		t.Errorf("ledger = %d, want 100", rec.Ledger)
	}
	if rec.Time != "2023-11-14T22:15:00Z" {
		t.Errorf("time = %q, want an RFC3339 timestamp", rec.Time)
	}
	if len(rec.Topics) != 1 {
		t.Fatalf("got %d topics, want 1", len(rec.Topics))
	}

	// Decoded payloads must carry lens's {type, value} envelope so jq
	// expressions written against `lens decode --json` work unchanged.
	var env struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		t.Fatalf("value is not a lens envelope: %v\n%s", err, rec.Value)
	}
	if env.Type != "ScVal" {
		t.Errorf("decoded value type = %q, want ScVal", env.Type)
	}
}

func TestNDJSONRawPreservesBase64(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := sink.NewNDJSON(&buf, false)
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	_ = s.Flush()

	out := buf.String()
	if !strings.Contains(out, `"valueXdr":"`+scValU32+`"`) {
		t.Errorf("raw mode did not preserve the base64 payload\n%s", out)
	}
	if strings.Contains(out, `"value":{`) {
		t.Errorf("raw mode decoded the payload anyway\n%s", out)
	}
}

// TestNDJSONSurvivesUndecodablePayload covers the rule that one bad value
// must not terminate a long-running stream.
func TestNDJSONSurvivesUndecodablePayload(t *testing.T) {
	t.Parallel()

	r := eventRecord()
	r.Event.ValueXDR = "!!!not base64!!!"

	var buf bytes.Buffer
	s := sink.NewNDJSON(&buf, true)
	if err := s.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v, want the record to be emitted anyway", err)
	}
	_ = s.Flush()

	var rec struct {
		Value struct {
			XDR   string `json:"xdr"`
			Error string `json:"error"`
		} `json:"value"`
	}
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec.Value.XDR == "" || rec.Value.Error == "" {
		t.Errorf("undecodable payload did not report both the raw XDR and the reason\n%s", buf.String())
	}
}

func TestNDJSONEmitsOneLinePerRecord(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := sink.NewNDJSON(&buf, true)
	for i := range 5 {
		r := eventRecord()
		r.ID = string(rune('a' + i))
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	_ = s.Flush()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("got %d lines for 5 records, want 5", len(lines))
	}
	for i, l := range lines {
		if !json.Valid([]byte(l)) {
			t.Errorf("line %d is not valid JSON: %s", i, l)
		}
	}
}

func TestNDJSONLedgerRecord(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := sink.NewNDJSON(&buf, true)
	if err := s.Emit(ledgerRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	_ = s.Flush()

	var rec struct {
		Kind   string `json:"kind"`
		Ledger uint32 `json:"ledger"`
		Hash   string `json:"hash"`
	}
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if rec.Kind != "ledger" || rec.Hash != "deadbeef" {
		t.Errorf("got kind=%q hash=%q, want kind=ledger hash=deadbeef", rec.Kind, rec.Hash)
	}
}

// TestHumanRendersPayloadThroughLens is the check that prism and lens agree
// on how a value looks, which is the point of depending on lens at all.
func TestHumanRendersPayloadThroughLens(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := sink.NewHuman(&buf, lensformat.NoColor, false)
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	_ = s.Flush()
	out := buf.String()

	for _, want := range []string{
		"ledger 100",
		"contract",
		"topic[0]",
		"value",
		"ScVal",           // the lens type header
		"ScValTypeScvU32", // the lens enum rendering
	} {
		if !strings.Contains(out, want) {
			t.Errorf("human output does not contain %q\n%s", want, out)
		}
	}
	// The tree must come from lens's TreeFormatter, box characters and all.
	if !strings.Contains(out, "└─") && !strings.Contains(out, "├─") {
		t.Errorf("payload was not rendered as a lens tree\n%s", out)
	}
}

func TestHumanCompactOmitsPayload(t *testing.T) {
	t.Parallel()

	var full, compact bytes.Buffer
	if err := sink.NewHuman(&full, lensformat.NoColor, false).Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := sink.NewHuman(&compact, lensformat.NoColor, true).Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if compact.Len() >= full.Len() {
		t.Errorf("compact output (%d bytes) is not shorter than full (%d bytes)",
			compact.Len(), full.Len())
	}
	if strings.Contains(compact.String(), "ScVal") {
		t.Errorf("compact output still rendered the payload\n%s", compact.String())
	}
	if !strings.Contains(compact.String(), "ledger 100") {
		t.Error("compact output dropped the header line too")
	}
}

func TestHumanColorHonoursPalette(t *testing.T) {
	t.Parallel()

	var colored, plain bytes.Buffer
	if err := sink.NewHuman(&colored, lensformat.ColorPalette, true).Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := sink.NewHuman(&plain, lensformat.NoColor, true).Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if !strings.Contains(colored.String(), "\x1b[") {
		t.Error("colour palette produced no escape sequences")
	}
	if strings.Contains(plain.String(), "\x1b[") {
		t.Error("NoColor palette produced escape sequences")
	}
}

// TestHumanSurvivesUndecodablePayload mirrors the NDJSON case: a malformed
// value is reported inline, not fatal.
func TestHumanSurvivesUndecodablePayload(t *testing.T) {
	t.Parallel()

	r := eventRecord()
	r.Event.ValueXDR = "!!!not base64!!!"

	var buf bytes.Buffer
	if err := sink.NewHuman(&buf, lensformat.NoColor, false).Emit(r); err != nil {
		t.Fatalf("Emit() error = %v, want the record to render anyway", err)
	}
	if !strings.Contains(buf.String(), "undecodable") {
		t.Errorf("undecodable payload was not reported\n%s", buf.String())
	}
}

func TestSinksRejectUnknownKind(t *testing.T) {
	t.Parallel()

	bad := stream.Record{ID: "x", Kind: stream.Kind("nonsense")}

	var buf bytes.Buffer
	if err := sink.NewNDJSON(&buf, true).Emit(bad); err == nil {
		t.Error("NDJSON.Emit(unknown kind) error = nil, want an error")
	}
	buf.Reset()
	if err := sink.NewHuman(&buf, lensformat.NoColor, false).Emit(bad); err == nil {
		t.Error("Human.Emit(unknown kind) error = nil, want an error")
	}
}
