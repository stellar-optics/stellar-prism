package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func setMaxContractSize(t *testing.T, srv *fakerpc.Server, limit uint32) {
	t.Helper()

	size := xdr.Uint32(limit)
	entry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:      xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
				ContractMaxSizeBytes: &size,
			},
		},
	}
	data, err := xdr.MarshalBase64(entry)
	if err != nil {
		t.Fatalf("marshalling config setting: %v", err)
	}
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeConfigSetting,
		ConfigSetting: &xdr.LedgerKeyConfigSetting{
			ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
		},
	}
	keyXDR, err := xdr.MarshalBase64(key)
	if err != nil {
		t.Fatalf("marshalling config setting key: %v", err)
	}
	srv.SetLedgerEntries(map[string]protocol.LedgerEntryResult{
		keyXDR: {DataXDR: data},
	})
}

func TestFetchMaxContractSizeBytes(t *testing.T) {
	srv := fakerpc.NewServer(nil)
	setMaxContractSize(t, srv, 65536)

	got, err := fetchMaxContractSizeBytes(context.Background(), srv)
	if err != nil {
		t.Fatalf("FetchMaxContractSizeBytes() error = %v", err)
	}
	if got != 65536 {
		t.Fatalf("FetchMaxContractSizeBytes() = %d, want 65536", got)
	}
}

func TestWASMCommandReportsSizeAgainstLimit(t *testing.T) {
	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9500, "abc", protocol.TransactionStatusSuccess, cliUploadEnvelope(t, wasm), ""),
	})
	setMaxContractSize(t, srv, 10)
	withFake(t, srv)

	stdout, _, err := run(t, "wasm", "--from", "9500", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1\n%s", len(lines), stdout)
	}
	var rec struct {
		WASMSizeLimitBytes uint32  `json:"wasmSizeLimitBytes"`
		WASMSizePercent    float64 `json:"wasmSizePercent"`
		WASMSizeWarning    bool    `json:"wasmSizeWarning"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, lines[0])
	}
	if rec.WASMSizeLimitBytes != 10 || rec.WASMSizePercent != 80 || !rec.WASMSizeWarning {
		t.Fatalf("size report = %+v, want limit 10, 80%%, warning", rec)
	}
}

func TestFetchMaxContractSizeBytesFailures(t *testing.T) {
	t.Run("missing capability", func(t *testing.T) {
		_, err := fetchMaxContractSizeBytes(context.Background(), streamOnlyClient{})
		if err == nil || !strings.Contains(err.Error(), "getLedgerEntries") {
			t.Fatalf("error = %v, want a missing-capability failure", err)
		}
	})

	t.Run("no config setting", func(t *testing.T) {
		_, err := fetchMaxContractSizeBytes(context.Background(), fakerpc.NewServer(nil))
		if err == nil || !strings.Contains(err.Error(), "CONFIG_SETTING_CONTRACT_MAX_SIZE_BYTES") {
			t.Fatalf("error = %v, want a missing-setting failure", err)
		}
	})

	t.Run("malformed entry", func(t *testing.T) {
		srv := fakerpc.NewServer(nil)
		key := xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.LedgerKeyConfigSetting{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
			},
		}
		keyXDR, err := xdr.MarshalBase64(key)
		if err != nil {
			t.Fatalf("marshalling key: %v", err)
		}
		srv.SetLedgerEntries(map[string]protocol.LedgerEntryResult{
			keyXDR: {DataXDR: "not-xdr"},
		})
		if _, err := fetchMaxContractSizeBytes(context.Background(), srv); err == nil {
			t.Fatal("FetchMaxContractSizeBytes() error = nil, want a decode failure")
		}
	})
}

func TestFormatWASMSize(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		limit   uint32
		percent float64
		want    string
	}{
		{name: "no limit", size: 1234, want: "1234 bytes"},
		{name: "acceptance example", size: 46_284, limit: 65_536, percent: 70.6, want: "45.2 KB / 64.0 KB (70.6%)"},
		{name: "warning", size: 52_428, limit: 65_536, percent: 80.0, want: "51.2 KB / 64.0 KB (80.0%)"},
		{name: "exceeded", size: 65_536, limit: 65_536, percent: 100.0, want: "64.0 KB / 64.0 KB (100.0%)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatWASMSize(tc.size, tc.limit, tc.percent); got != tc.want {
				t.Fatalf("FormatWASMSize() = %q, want %q", got, tc.want)
			}
		})
	}
}
