package stream

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func TestSplitContractIDsRespectsServerLimit(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	groups := splitContractIDs(ids)
	want := [][]string{{"a", "b", "c", "d", "e"}, {"f", "g", "h", "i", "j"}, {"k"}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups %v, want %v", groups, want)
	}
}

func sacTestB64(t *testing.T, value xdr.ScVal) string {
	t.Helper()
	encoded, err := xdr.MarshalBase64(&value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func sacTestB64Topics(t *testing.T, values []xdr.ScVal) []string {
	t.Helper()
	topics := make([]string, 0, len(values))
	for _, value := range values {
		topics = append(topics, sacTestB64(t, value))
	}
	return topics
}

func sacTestSymbol(t *testing.T, name string) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestString(t *testing.T, name string) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvString, xdr.ScString(name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestAccount(t *testing.T, seed byte) xdr.ScVal {
	t.Helper()
	var key xdr.Uint256
	key[31] = seed
	account := xdr.AccountId(xdr.PublicKey{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &key})
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &account}
	value, err := xdr.NewScVal(xdr.ScValTypeScvAddress, address)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestContract(t *testing.T, seed byte) xdr.ScVal {
	t.Helper()
	var id xdr.ContractId
	id[31] = seed
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id}
	value, err := xdr.NewScVal(xdr.ScValTypeScvAddress, address)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestAddressString(t *testing.T, value xdr.ScVal) string {
	t.Helper()
	address, ok := scValAddress(value)
	if !ok {
		t.Fatal("test address did not decode")
	}
	return address
}

func sacTestI128(t *testing.T, parts xdr.Int128Parts) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvI128, parts)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestBool(t *testing.T, authorized bool) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvBool, authorized)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestVoid(t *testing.T) xdr.ScVal {
	t.Helper()
	value, err := xdr.NewScVal(xdr.ScValTypeScvVoid, nil)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sacTestMuxedMap(t *testing.T, amount xdr.ScVal, id xdr.Uint64) xdr.ScVal {
	t.Helper()
	muxed, err := xdr.NewScVal(xdr.ScValTypeScvU64, id)
	if err != nil {
		t.Fatal(err)
	}
	entries := xdr.ScMap{
		{Key: sacTestSymbol(t, "amount"), Val: amount},
		{Key: sacTestSymbol(t, "to_muxed_id"), Val: muxed},
	}
	value, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type sacWant struct {
	action      SACAction
	asset       string
	from        string
	to          string
	account     string
	admin       string
	amount      string
	authorized  *bool
	muxedID     *uint64
	summary     string
	amountEmpty bool
}

func assertSAC(t *testing.T, got *SACEvent, want sacWant) {
	t.Helper()
	if got == nil {
		t.Fatal("DecodeSAC() = nil, want a SAC event")
	}
	if got.Action != want.action || got.Asset != want.asset || got.From != want.from ||
		got.To != want.to || got.Account != want.account || got.Admin != want.admin {
		t.Errorf("decoded identities = %+v, want action=%q asset=%q from=%q to=%q account=%q admin=%q",
			got, want.action, want.asset, want.from, want.to, want.account, want.admin)
	}
	if want.amountEmpty {
		if got.Amount != nil {
			t.Errorf("Amount = %v, want nil", got.Amount)
		}
	} else if got.Amount == nil || got.AmountString() != want.amount {
		t.Errorf("amount = %v, want %q", got.Amount, want.amount)
	}
	if !reflect.DeepEqual(got.Authorized, want.authorized) {
		t.Errorf("Authorized = %v, want %v", got.Authorized, want.authorized)
	}
	if !reflect.DeepEqual(got.MuxedID, want.muxedID) {
		t.Errorf("MuxedID = %v, want %v", got.MuxedID, want.muxedID)
	}
	if got.Summary() != want.summary {
		t.Errorf("Summary() = %q, want %q", got.Summary(), want.summary)
	}
}

func TestDecodeSACEventShapes(t *testing.T) {
	accountA := sacTestAccount(t, 1)
	accountB := sacTestAccount(t, 2)
	accountC := sacTestContract(t, 3)
	accountAStr := sacTestAddressString(t, accountA)
	accountBStr := sacTestAddressString(t, accountB)
	accountCStr := sacTestAddressString(t, accountC)
	if accountAStr == accountBStr || accountAStr == accountCStr || accountBStr == accountCStr {
		t.Fatal("test addresses are not distinct")
	}

	creditAsset := sacTestString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	nativeAsset := sacTestString(t, "native")
	hundred := sacTestI128(t, xdr.Int128Parts{Lo: 1_000_000_000})
	zero := sacTestI128(t, xdr.Int128Parts{})
	oneStroop := sacTestI128(t, xdr.Int128Parts{Lo: 1})
	large := sacTestI128(t, xdr.Int128Parts{Hi: 1})
	negative := sacTestI128(t, xdr.Int128Parts{Hi: -1, Lo: xdr.Uint64(^uint64(0))})
	authorizedTrue, authorizedFalse := true, false
	muxedID := uint64(987654321)

	tests := []struct {
		name   string
		topics []xdr.ScVal
		value  xdr.ScVal
		want   sacWant
	}{
		{
			name:   "modern transfer with credit asset",
			topics: []xdr.ScVal{sacTestSymbol(t, "transfer"), accountA, accountB, creditAsset},
			value:  hundred,
			want: sacWant{
				action: SACTransfer, asset: "USDC", from: accountAStr, to: accountBStr,
				amount: "100.0000000", summary: fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", accountAStr, accountBStr),
			},
		},
		{
			name:   "legacy transfer without asset topic",
			topics: []xdr.ScVal{sacTestSymbol(t, "transfer"), accountA, accountB},
			value:  oneStroop,
			want: sacWant{
				action: SACTransfer, from: accountAStr, to: accountBStr,
				amount: "0.0000001", summary: fmt.Sprintf("transfer: %s -> %s (0.00)", accountAStr, accountBStr),
			},
		},
		{
			name:   "transfer between contract and account with native asset",
			topics: []xdr.ScVal{sacTestSymbol(t, "transfer"), accountC, accountA, nativeAsset},
			value:  zero,
			want: sacWant{
				action: SACTransfer, asset: "XLM", from: accountCStr, to: accountAStr,
				amount: "0.0000000", summary: fmt.Sprintf("transfer: %s -> %s (0.00 XLM)", accountCStr, accountAStr),
			},
		},
		{
			name:   "transfer with to_muxed_id map payload",
			topics: []xdr.ScVal{sacTestSymbol(t, "transfer"), accountA, accountC, creditAsset},
			value:  sacTestMuxedMap(t, hundred, xdr.Uint64(muxedID)),
			want: sacWant{
				action: SACTransfer, asset: "USDC", from: accountAStr, to: accountCStr,
				amount: "100.0000000", muxedID: &muxedID,
				summary: fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", accountAStr, accountCStr),
			},
		},
		{
			name:   "modern mint",
			topics: []xdr.ScVal{sacTestSymbol(t, "mint"), accountB, nativeAsset},
			value:  hundred,
			want: sacWant{
				action: SACMint, asset: "XLM", to: accountBStr, account: accountBStr,
				amount: "100.0000000", summary: fmt.Sprintf("mint: %s (100.00 XLM)", accountBStr),
			},
		},
		{
			name:   "legacy mint with admin topic",
			topics: []xdr.ScVal{sacTestSymbol(t, "mint"), accountA, accountB},
			value:  oneStroop,
			want: sacWant{
				action: SACMint, to: accountBStr, account: accountBStr, admin: accountAStr,
				amount: "0.0000001", summary: fmt.Sprintf("mint: %s (0.00)", accountBStr),
			},
		},
		{
			name:   "modern clawback",
			topics: []xdr.ScVal{sacTestSymbol(t, "clawback"), accountA, creditAsset},
			value:  hundred,
			want: sacWant{
				action: SACClawback, asset: "USDC", from: accountAStr, account: accountAStr,
				amount: "100.0000000", summary: fmt.Sprintf("clawback: %s (100.00 USDC)", accountAStr),
			},
		},
		{
			name:   "legacy clawback with admin topic",
			topics: []xdr.ScVal{sacTestSymbol(t, "clawback"), accountA, accountC, creditAsset},
			value:  zero,
			want: sacWant{
				action: SACClawback, asset: "USDC", from: accountCStr, account: accountCStr, admin: accountAStr,
				amount: "0.0000000", summary: fmt.Sprintf("clawback: %s (0.00 USDC)", accountCStr),
			},
		},
		{
			name:   "burn with large i128 amount",
			topics: []xdr.ScVal{sacTestSymbol(t, "burn"), accountB, creditAsset},
			value:  large,
			want: sacWant{
				action: SACBurn, asset: "USDC", from: accountBStr, account: accountBStr,
				amount:  "1844674407370.9551616",
				summary: fmt.Sprintf("burn: %s (1844674407370.95 USDC)", accountBStr),
			},
		},
		{
			name:   "legacy burn with zero amount",
			topics: []xdr.ScVal{sacTestSymbol(t, "burn"), accountC},
			value:  zero,
			want: sacWant{
				action: SACBurn, from: accountCStr, account: accountCStr,
				amount: "0.0000000", summary: fmt.Sprintf("burn: %s (0.00)", accountCStr),
			},
		},
		{
			name:   "burn with negative i128 amount",
			topics: []xdr.ScVal{sacTestSymbol(t, "burn"), accountA, nativeAsset},
			value:  negative,
			want: sacWant{
				action: SACBurn, asset: "XLM", from: accountAStr, account: accountAStr,
				amount: "-0.0000001", summary: fmt.Sprintf("burn: %s (-0.00 XLM)", accountAStr),
			},
		},
		{
			name:   "modern set_authorized",
			topics: []xdr.ScVal{sacTestSymbol(t, "set_authorized"), accountB, creditAsset},
			value:  sacTestBool(t, true),
			want: sacWant{
				action: SACSetAuthorized, asset: "USDC", to: accountBStr, account: accountBStr,
				authorized: &authorizedTrue, amountEmpty: true,
				summary: fmt.Sprintf("set_authorized: %s (authorized)", accountBStr),
			},
		},
		{
			name:   "legacy set_authorized with admin topic",
			topics: []xdr.ScVal{sacTestSymbol(t, "set_authorized"), accountA, accountC, nativeAsset},
			value:  sacTestBool(t, false),
			want: sacWant{
				action: SACSetAuthorized, asset: "XLM", to: accountCStr, account: accountCStr, admin: accountAStr,
				authorized: &authorizedFalse, amountEmpty: true,
				summary: fmt.Sprintf("set_authorized: %s (deauthorized)", accountCStr),
			},
		},
		{
			name:   "set_authorized with flag in topic",
			topics: []xdr.ScVal{sacTestSymbol(t, "set_authorized"), accountB, sacTestBool(t, true), nativeAsset},
			value:  sacTestVoid(t),
			want: sacWant{
				action: SACSetAuthorized, asset: "XLM", to: accountBStr, account: accountBStr,
				authorized: &authorizedTrue, amountEmpty: true,
				summary: fmt.Sprintf("set_authorized: %s (authorized)", accountBStr),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := DecodeSAC(sacTestB64Topics(t, test.topics), sacTestB64(t, test.value))
			assertSAC(t, got, test.want)
		})
	}
}

func TestEventIndexMatchesCursorParsing(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want uint32
	}{
		{name: "valid", id: (protocol.Cursor{Ledger: 1, Event: 7}).String(), want: 7},
		{name: "invalid TOID", id: "invalid-7"},
		{name: "invalid event", id: "1-invalid"},
		{name: "extra separator", id: "1-7-extra"},
		{name: "missing separator", id: "17"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := eventIndex(test.id); got != test.want {
				t.Errorf("eventIndex(%q) = %d, want %d", test.id, got, test.want)
			}
		})
	}
}

func TestDecodeSACRejectsMalformedAndNonSACEvents(t *testing.T) {
	account := sacTestAccount(t, 1)
	accountStr := sacTestAddressString(t, account)
	creditAsset := sacTestString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	transferTopics := []xdr.ScVal{sacTestSymbol(t, "transfer"), account, account, creditAsset}
	amount := sacTestI128(t, xdr.Int128Parts{Lo: 1})
	badIntValue := xdr.Int64(7)
	badInt, err := xdr.MarshalBase64(&badIntValue)
	if err != nil {
		t.Fatal(err)
	}
	mapWithoutAmount, err := xdr.NewScVal(xdr.ScValTypeScvMap, &xdr.ScMap{
		{Key: sacTestSymbol(t, "to_muxed_id"), Val: sacTestI128(t, xdr.Int128Parts{Lo: 1})},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		topics []string
		value  string
	}{
		{"no topics", nil, sacTestB64(t, amount)},
		{"invalid base64 topic", []string{"!!!not base64!!!"}, sacTestB64(t, amount)},
		{"invalid base64 value", sacTestB64Topics(t, transferTopics), "!!!not base64!!!"},
		{"topic is not an ScVal", []string{badInt}, sacTestB64(t, amount)},
		{"symbol topic has wrong type", sacTestB64Topics(t, []xdr.ScVal{sacTestString(t, "transfer"), account, account}), sacTestB64(t, amount)},
		{"short topic list", []string{sacTestB64(t, sacTestSymbol(t, "transfer"))}, sacTestB64(t, amount)},
		{"too many topics", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "transfer"), account, account, account, creditAsset}), sacTestB64(t, amount)},
		{"non-SAC symbol", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "swap"), account, account, creditAsset}), sacTestB64(t, amount)},
		{"non-address topic", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "transfer"), sacTestSymbol(t, "from"), account, creditAsset}), sacTestB64(t, amount)},
		{"invalid asset topic", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "transfer"), account, account, sacTestString(t, "USDC")}), sacTestB64(t, amount)},
		{"wrong value type", sacTestB64Topics(t, transferTopics), sacTestB64(t, sacTestSymbol(t, "amount"))},
		{"map payload without amount", sacTestB64Topics(t, transferTopics), sacTestB64(t, mapWithoutAmount)},
		{"set_authorized without boolean", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "set_authorized"), account, creditAsset}), sacTestB64(t, amount)},
		{"set_authorized with two flags", sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "set_authorized"), account, sacTestBool(t, true), sacTestBool(t, false), creditAsset}), sacTestB64(t, sacTestVoid(t))},
		{"muxed id with wrong type", sacTestB64Topics(t, transferTopics), sacTestB64(t, func() xdr.ScVal {
			entries := xdr.ScMap{
				{Key: sacTestSymbol(t, "amount"), Val: amount},
				{Key: sacTestSymbol(t, "to_muxed_id"), Val: sacTestSymbol(t, "42")},
			}
			value, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}())},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DecodeSAC(test.topics, test.value); got != nil {
				t.Fatalf("DecodeSAC() = %+v, want nil for account %s", got, accountStr)
			}
		})
	}
}

func TestEventRecordDecodesSACAndKeepsRawXDR(t *testing.T) {
	account := sacTestAccount(t, 1)
	accountStr := sacTestAddressString(t, account)
	creditAsset := sacTestString(t, "USDC:GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	topics := sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "transfer"), account, account, creditAsset})
	value := sacTestB64(t, sacTestI128(t, xdr.Int128Parts{Lo: 1_000_000_000}))
	info := protocol.EventInfo{
		ID:         "0000000100-0000000000",
		Ledger:     100,
		EventType:  "contract",
		TopicXDR:   topics,
		ValueXDR:   value,
		ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
	}

	record := eventRecord(info)
	if record.Event.SAC == nil {
		t.Fatal("eventRecord() did not decode a SAC event")
	}
	if record.Event.SAC.Action != SACTransfer || record.Event.SAC.Asset != "USDC" {
		t.Errorf("SAC = %+v, want a USDC transfer", record.Event.SAC)
	}
	if record.Event.SAC.Summary() != fmt.Sprintf("transfer: %s -> %s (100.00 USDC)", accountStr, accountStr) {
		t.Errorf("summary = %q", record.Event.SAC.Summary())
	}
	if !reflect.DeepEqual(record.Event.TopicXDR, topics) || record.Event.ValueXDR != value {
		t.Errorf("eventRecord() changed the raw XDR: topics=%v value=%q", record.Event.TopicXDR, record.Event.ValueXDR)
	}

	info.TopicXDR = sacTestB64Topics(t, []xdr.ScVal{sacTestSymbol(t, "swap"), account, account})
	if got := eventRecord(info).Event.SAC; got != nil {
		t.Errorf("non-SAC eventRecord() SAC = %+v, want nil", got)
	}
}

func TestFormatSACAmountUsesFixedPointArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		amount   *big.Int
		decimals int
		want     string
	}{
		{"zero", big.NewInt(0), SACDecimals, "0.0000000"},
		{"one stroop", big.NewInt(1), SACDecimals, "0.0000001"},
		{"hundred", big.NewInt(1_000_000_000), SACDecimals, "100.0000000"},
		{"two decimals", big.NewInt(1_005_000_000), 2, "100.50"},
		{"negative", big.NewInt(-1), SACDecimals, "-0.0000001"},
		{"large", new(big.Int).Lsh(big.NewInt(1), 64), SACDecimals, "1844674407370.9551616"},
		{"no fraction", big.NewInt(42), 0, "0"},
		{"one token no fraction", big.NewInt(10_000_000), 0, "1"},
		{"extra precision", big.NewInt(1_000_000_000), 9, "100.000000000"},
		{"nil", nil, SACDecimals, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := FormatSACAmount(test.amount, test.decimals); got != test.want {
				t.Errorf("FormatSACAmount(%v, %d) = %q, want %q", test.amount, test.decimals, got, test.want)
			}
		})
	}
}

func TestRecordBufferPoolClearsSlotsWithoutMutatingRecords(t *testing.T) {
	records := getRecordBuffer(1)
	event := &Event{ContractID: "contract"}
	records = append(records, Record{ID: "record", Event: event})

	putRecordBuffer(records)
	if event.ContractID != "contract" {
		t.Fatal("recycling the record buffer mutated the retained event")
	}

	reused := getRecordBuffer(1)
	defer putRecordBuffer(reused)
	if len(reused) != 0 {
		t.Fatalf("reused buffer length = %d, want 0", len(reused))
	}
	if cap(reused) > DefaultPageLimit {
		t.Fatalf("reused buffer capacity = %d, want at most %d", cap(reused), DefaultPageLimit)
	}
}

func TestEventIDSetPoolClearsEntries(t *testing.T) {
	seen := getEventIDSet()
	seen["record"] = struct{}{}
	putEventIDSet(seen)

	reused := getEventIDSet()
	defer putEventIDSet(reused)
	if len(reused) != 0 {
		t.Fatalf("reused ID set length = %d, want 0", len(reused))
	}
}

type eventPoolClient struct {
	rpc.Client
	calls int
}

func (c *eventPoolClient) GetEvents(context.Context, protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	c.calls++
	if c.calls == 1 {
		cursor := (protocol.Cursor{Ledger: 1}).String()
		return protocol.GetEventsResponse{
			Events: []protocol.EventInfo{{
				ID:         cursor,
				Ledger:     1,
				EventType:  protocol.EventTypeContract,
				ContractID: "contract",
				TopicXDR:   []string{"AAAAAQ=="},
				ValueXDR:   "AAAAAgAAAAE=",
			}},
			Cursor: cursor,
		}, nil
	}
	return protocol.GetEventsResponse{}, nil
}

func TestTailerRecordBufferReusePreservesDeliveredRecords(t *testing.T) {
	client := &eventPoolClient{}
	source, err := NewEventSource(client, EventQuery{PageLimit: 1})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	var delivered []Record
	sink := SinkFunc(func(record Record) error {
		delivered = append(delivered, record)
		return nil
	})
	tailer := NewTailer(source, sink, Options{})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered %d records, want 1", len(delivered))
	}
	if delivered[0].Event == nil || delivered[0].Event.ContractID != "contract" {
		t.Fatalf("delivered event = %+v, want retained contract event", delivered[0].Event)
	}
}
