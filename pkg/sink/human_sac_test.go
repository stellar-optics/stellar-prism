package sink_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func humanSACB64(t *testing.T, value xdr.ScVal) string {
	t.Helper()
	encoded, err := xdr.MarshalBase64(&value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func humanSACB64Topics(t *testing.T, values []xdr.ScVal) []string {
	t.Helper()
	topics := make([]string, 0, len(values))
	for _, value := range values {
		topics = append(topics, humanSACB64(t, value))
	}
	return topics
}

func humanSACSymbol(t *testing.T, name string) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func humanSACString(t *testing.T, name string) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvString, xdr.ScString(name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func humanSACAccount(t *testing.T, seed byte) (xdr.ScVal, string) {
	t.Helper()
	var key xdr.Uint256
	key[31] = seed
	account := xdr.AccountId(xdr.PublicKey{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &key})
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &account}
	value, err := xdr.NewScVal(xdr.ScValTypeScvAddress, address)
	if err != nil {
		t.Fatal(err)
	}
	label, err := address.String()
	if err != nil {
		t.Fatal(err)
	}
	return value, label
}

func humanSACI128(t *testing.T, amount xdr.Uint64) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Lo: amount})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func humanSACRecord(t *testing.T, topics []xdr.ScVal, value xdr.ScVal) stream.Record {
	t.Helper()
	record := eventRecord()
	record.Event.TopicXDR = humanSACB64Topics(t, topics)
	record.Event.ValueXDR = humanSACB64(t, value)
	record.Event.SAC = nil
	return record
}

func renderHumanSAC(t *testing.T, record stream.Record, compact bool, palette lensformat.Palette) string {
	t.Helper()
	var buf bytes.Buffer
	if err := sink.NewHuman(&buf, palette, compact).Emit(record); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	return buf.String()
}

func TestHumanRendersSACTokenBadgeInBothModes(t *testing.T) {
	from, fromStr := humanSACAccount(t, 1)
	to, toStr := humanSACAccount(t, 2)
	creditAsset := humanSACString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	nativeAsset := humanSACString(t, "native")
	hundred := humanSACI128(t, 1_000_000_000)
	authorized, err := xdr.NewScVal(xdr.ScValTypeScvBool, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		topics  []xdr.ScVal
		value   xdr.ScVal
		summary string
	}{
		{
			name:    "transfer",
			topics:  []xdr.ScVal{humanSACSymbol(t, "transfer"), from, to, creditAsset},
			value:   hundred,
			summary: fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", fromStr, toStr),
		},
		{
			name:    "mint",
			topics:  []xdr.ScVal{humanSACSymbol(t, "mint"), to, nativeAsset},
			value:   hundred,
			summary: fmt.Sprintf("mint: %s (100.00 XLM)", toStr),
		},
		{
			name:    "clawback",
			topics:  []xdr.ScVal{humanSACSymbol(t, "clawback"), from, creditAsset},
			value:   hundred,
			summary: fmt.Sprintf("clawback: %s (100.00 USDC)", fromStr),
		},
		{
			name:    "burn",
			topics:  []xdr.ScVal{humanSACSymbol(t, "burn"), from, creditAsset},
			value:   hundred,
			summary: fmt.Sprintf("burn: %s (100.00 USDC)", fromStr),
		},
		{
			name:    "set_authorized",
			topics:  []xdr.ScVal{humanSACSymbol(t, "set_authorized"), to, creditAsset},
			value:   authorized,
			summary: fmt.Sprintf("set_authorized: %s (authorized)", toStr),
		},
	}

	for _, test := range tests {
		for _, compact := range []bool{false, true} {
			name := test.name + " full"
			if compact {
				name = test.name + " compact"
			}
			t.Run(name, func(t *testing.T) {
				out := renderHumanSAC(t, humanSACRecord(t, test.topics, test.value), compact, lensformat.NoColor)
				if !strings.Contains(out, "[token]") {
					t.Errorf("output lacks the token badge:\n%s", out)
				}
				if !strings.Contains(out, test.summary) {
					t.Errorf("output lacks %q:\n%s", test.summary, out)
				}
				if !strings.Contains(out, "ledger 100") {
					t.Errorf("output dropped the event header:\n%s", out)
				}
				if compact {
					if strings.Contains(out, "ScVal") || strings.Contains(out, "topic[0]") {
						t.Errorf("compact output still rendered the payload tree:\n%s", out)
					}
				} else if !strings.Contains(out, "topic[0]") || !strings.Contains(out, "ScVal") {
					t.Errorf("full output dropped the decoded payload tree:\n%s", out)
				}
			})
		}
	}
}

func TestHumanUsesPreDecodedSACEvent(t *testing.T) {
	record := eventRecord()
	_, fromStr := humanSACAccount(t, 1)
	_, toStr := humanSACAccount(t, 2)
	record.Event.SAC = &stream.SACEvent{
		Action: stream.SACTransfer,
		Asset:  "USDC",
		From:   fromStr,
		To:     toStr,
		Amount: big.NewInt(1_000_000_000),
	}

	out := renderHumanSAC(t, record, true, lensformat.NoColor)
	want := fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", fromStr, toStr)
	if !strings.Contains(out, want) {
		t.Fatalf("pre-decoded SAC event was not rendered:\n%s", out)
	}
}

func TestHumanFallsBackForMalformedAndNonSACEvents(t *testing.T) {
	from, _ := humanSACAccount(t, 1)
	to, _ := humanSACAccount(t, 2)
	amount := humanSACI128(t, 1)
	asset := humanSACString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")

	tests := []struct {
		name   string
		topics []string
		value  string
	}{
		{"non-SAC symbol", humanSACB64Topics(t, []xdr.ScVal{humanSACSymbol(t, "swap"), from, to, asset}), humanSACB64(t, amount)},
		{"malformed base64", []string{"!!!not base64!!!"}, "!!!not base64!!!"},
		{"short topics", humanSACB64Topics(t, []xdr.ScVal{humanSACSymbol(t, "transfer")}), humanSACB64(t, amount)},
		{"wrong value type", humanSACB64Topics(t, []xdr.ScVal{humanSACSymbol(t, "transfer"), from, to, asset}), humanSACB64(t, humanSACSymbol(t, "amount"))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := eventRecord()
			record.Event.TopicXDR = test.topics
			record.Event.ValueXDR = test.value
			out := renderHumanSAC(t, record, false, lensformat.NoColor)
			if strings.Contains(out, "[token]") || strings.Contains(out, "transfer: ") {
				t.Errorf("non-SAC event got a token badge:\n%s", out)
			}
			if !strings.Contains(out, "ledger 100") || !strings.Contains(out, "contract") {
				t.Errorf("event header was not rendered:\n%s", out)
			}
		})
	}
}

func TestHumanSACBadgeHonoursPalette(t *testing.T) {
	from, fromStr := humanSACAccount(t, 1)
	to, toStr := humanSACAccount(t, 2)
	asset := humanSACString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	record := humanSACRecord(t,
		[]xdr.ScVal{humanSACSymbol(t, "transfer"), from, to, asset},
		humanSACI128(t, 1_000_000_000))

	colored := renderHumanSAC(t, record, true, lensformat.ColorPalette)
	plain := renderHumanSAC(t, record, true, lensformat.NoColor)
	want := fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", fromStr, toStr)
	if !strings.Contains(colored, "\x1b[") || !strings.Contains(colored, "[token]") || !strings.Contains(colored, want) {
		t.Fatalf("colored output lost the badge or summary:\n%q", colored)
	}
	if strings.Contains(plain, "\x1b[") || !strings.Contains(plain, want) {
		t.Fatalf("plain output has unexpected escapes or lost the summary:\n%q", plain)
	}
}

func TestSACDecodingDoesNotChangeNDJSONSchema(t *testing.T) {
	from, _ := humanSACAccount(t, 1)
	to, _ := humanSACAccount(t, 2)
	asset := humanSACString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	record := humanSACRecord(t,
		[]xdr.ScVal{humanSACSymbol(t, "transfer"), from, to, asset},
		humanSACI128(t, 1_000_000_000))
	record.Event.SAC = stream.DecodeSAC(record.Event.TopicXDR, record.Event.ValueXDR)
	if record.Event.SAC == nil {
		t.Fatal("test event did not decode as SAC")
	}

	var buf bytes.Buffer
	if err := sink.NewNDJSON(&buf, false).Emit(record); err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"sac", "SAC", "sacEvent"} {
		if _, ok := line[key]; ok {
			t.Errorf("NDJSON gained a %q key: %v", key, line)
		}
	}
	if len(record.Event.TopicXDR) != 4 || record.Event.ValueXDR == "" {
		t.Fatal("test record lost its raw XDR")
	}
}
