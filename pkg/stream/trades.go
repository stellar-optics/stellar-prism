package stream

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

type TradeQuery struct {
	StartLedger uint32
	PageLimit   uint
	Pair        string
}
type TradeSource struct {
	tx   *TransactionSource
	pair string
}

func NewTradeSource(client rpc.Client, q TradeQuery) (*TradeSource, error) {
	if client == nil {
		return nil, fmt.Errorf("trade source: rpc client is required")
	}
	pair := ""
	if strings.TrimSpace(q.Pair) != "" {
		parts := strings.Split(q.Pair, "/")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("trade source: pair must be ASSET/ASSET")
		}
		pair = strings.ToUpper(strings.TrimSpace(parts[0])) + "/" + strings.ToUpper(strings.TrimSpace(parts[1]))
	}
	return &TradeSource{tx: NewTransactionSource(client, TransactionQuery{StartLedger: q.StartLedger, PageLimit: q.PageLimit}), pair: pair}, nil
}
func (s *TradeSource) Name() string                         { return "trades" }
func (s *TradeSource) Rewind(ledger uint32)                 { s.tx.Rewind(ledger) }
func (s *TradeSource) CursorLedger(c string) (uint32, bool) { return s.tx.CursorLedger(c) }
func (s *TradeSource) Fetch(ctx context.Context, c string) (Batch, error) {
	b, err := s.tx.Fetch(ctx, c)
	if err != nil {
		return Batch{}, err
	}
	out := Batch{Records: []Record{}, NextCursor: b.NextCursor, Latest: b.Latest, LatestCloseTime: b.LatestCloseTime, Oldest: b.Oldest, Full: b.Full}
	for _, r := range b.Records {
		t := r.Transaction
		if t == nil || !t.Successful || t.EnvelopeXDR == "" || t.ResultXDR == "" {
			continue
		}
		trades, err := extractPathPaymentTrades(t, r.Sequence, r.Time)
		if err != nil {
			return Batch{}, fmt.Errorf("trade source: transaction %s: %w", t.Hash, err)
		}
		for _, trade := range trades {
			if s.matches(trade) {
				out.Records = append(out.Records, trade)
			}
		}
	}
	return out, nil
}
func (s *TradeSource) matches(t Record) bool {
	if s.pair == "" {
		return true
	}
	parts := strings.Split(s.pair, "/")
	return assetMatches(t.Trade.AssetIn, parts[0]) && assetMatches(t.Trade.AssetOut, parts[1]) || assetMatches(t.Trade.AssetIn, parts[1]) && assetMatches(t.Trade.AssetOut, parts[0])
}

func assetMatches(label, want string) bool {
	return strings.EqualFold(label, want) || strings.HasPrefix(strings.ToUpper(label), strings.ToUpper(want)+":")
}

func extractPathPaymentTrades(tx *Transaction, ledger uint32, closeTime time.Time) ([]Record, error) {
	var env xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env); err != nil {
		return nil, fmt.Errorf("decoding envelope: %w", err)
	}
	var result xdr.TransactionResult
	if err := xdr.SafeUnmarshalBase64(tx.ResultXDR, &result); err != nil {
		return nil, fmt.Errorf("decoding result: %w", err)
	}
	results := result.Result.Results
	if result.Result.InnerResultPair != nil && result.Result.InnerResultPair.Result.Result.Results != nil {
		results = result.Result.InnerResultPair.Result.Result.Results
	}
	if results == nil {
		return nil, nil
	}
	ops, err := envelopeOperations(&env)
	if err != nil {
		return nil, err
	}
	var out []Record
	for i, op := range ops {
		if i >= len(*results) {
			break
		}
		or := (*results)[i]
		if or.Code != xdr.OperationResultCodeOpInner || or.Tr == nil {
			continue
		}
		trade := &Trade{Protocol: "classic", TxHash: tx.Hash, OpIndex: uint32(i)}
		if transaction, ok := envelopeTransaction(env); ok {
			source := transaction.SourceAccount
			if op.SourceAccount != nil {
				source = *op.SourceAccount
			}
			trade.Taker = muxedAccountAddress(source)
			trade.Trader = trade.Taker
		}
		switch op.Body.Type {
		case xdr.OperationTypePathPaymentStrictReceive:
			p := op.Body.PathPaymentStrictReceiveOp
			res := or.Tr.PathPaymentStrictReceiveResult
			if p == nil || res == nil || res.Success == nil {
				continue
			}
			trade.Operation = "path_payment_strict_receive"
			trade.AssetIn = assetName(p.SendAsset)
			trade.AssetOut = assetName(p.DestAsset)
			trade.AmountOut = stroops(res.Success.Last.Amount)
			var sent xdr.Int64
			for _, claim := range res.Success.Offers {
				if claim.OrderBook != nil {
					sent += claim.OrderBook.AmountSold
					if trade.Maker == "" {
						trade.Maker = accountAddress(claim.OrderBook.SellerId)
					}
				} else if claim.LiquidityPool != nil {
					sent += claim.LiquidityPool.AmountSold
				} else if claim.V0 != nil {
					sent += claim.V0.AmountSold
				}
			}
			trade.AmountIn = stroops(sent)
		case xdr.OperationTypePathPaymentStrictSend:
			p := op.Body.PathPaymentStrictSendOp
			res := or.Tr.PathPaymentStrictSendResult
			if p == nil || res == nil || res.Success == nil {
				continue
			}
			trade.Operation = "path_payment_strict_send"
			trade.AssetIn = assetName(p.SendAsset)
			trade.AssetOut = assetName(p.DestAsset)
			trade.AmountIn = stroops(p.SendAmount)
			trade.AmountOut = stroops(res.Success.Last.Amount)
			for _, claim := range res.Success.Offers {
				if claim.OrderBook != nil && trade.Maker == "" {
					trade.Maker = accountAddress(claim.OrderBook.SellerId)
				}
			}
		default:
			continue
		}
		in, inErr := strconv.ParseFloat(trade.AmountIn, 64)
		outAmount, outErr := strconv.ParseFloat(trade.AmountOut, 64)
		if inErr == nil && outErr == nil && in > 0 {
			trade.Price = strconv.FormatFloat(outAmount/in, 'f', 8, 64)
		}
		out = append(out, Record{ID: fmt.Sprintf("%010d-%06d-%06d", ledger, tx.Index, i), Sequence: ledger, Time: closeTime, Kind: KindTrade, Trade: trade})
	}
	if tx.ResultMetaXDR != "" {
		var meta xdr.TransactionMeta
		if xdr.SafeUnmarshalBase64(tx.ResultMetaXDR, &meta) == nil {
			for i, event := range sorobanContractEvents(&meta) {
				trade, ok := sorobanSwapTrade(event, tx.Hash, uint32(i))
				if !ok {
					continue
				}
				out = append(out, Record{ID: fmt.Sprintf("%010d-%06d-%06d", ledger, tx.Index, len(ops)+i), Sequence: ledger, Time: closeTime, Kind: KindTrade, Trade: trade})
			}
		}
	}
	return out, nil
}

func sorobanContractEvents(meta *xdr.TransactionMeta) []xdr.ContractEvent {
	var events []xdr.ContractEvent
	if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
		events = append(events, meta.V3.SorobanMeta.Events...)
	}
	if meta.V == 4 && meta.V4 != nil {
		for _, event := range meta.V4.Events {
			events = append(events, event.Event)
		}
	}
	return events
}

// sorobanSwapTrade recognizes the common Soroban swap event shape:
// (symbol("swap"), trader, asset_in, asset_out), with [amount_in, amount_out]
// in the event value. Contract event schemas are application-defined, so
// unknown layouts are left untouched instead of being guessed.
func sorobanSwapTrade(event xdr.ContractEvent, hash string, index uint32) (*Trade, bool) {
	if event.Type != xdr.ContractEventTypeContract || event.Body.V0 == nil {
		return nil, false
	}
	topics := event.Body.V0.Topics
	if len(topics) < 4 {
		return nil, false
	}
	name, ok := scValName(topics[0])
	if !ok || (!strings.EqualFold(name, "swap") && !strings.EqualFold(name, "swapped")) {
		return nil, false
	}
	trader, ok := scValAddress(topics[1])
	if !ok {
		return nil, false
	}
	assetIn, ok := scValAddress(topics[2])
	if !ok {
		return nil, false
	}
	assetOut, ok := scValAddress(topics[3])
	if !ok {
		return nil, false
	}
	values, ok := event.Body.V0.Data.GetVec()
	if !ok || values == nil || len(*values) < 2 {
		return nil, false
	}
	in, ok := scValInteger((*values)[0])
	if !ok {
		return nil, false
	}
	out, ok := scValInteger((*values)[1])
	if !ok {
		return nil, false
	}
	trade := &Trade{Protocol: "soroban", TxHash: hash, Trader: trader, Taker: trader, Operation: "swap", AssetIn: assetIn, AmountIn: in.String(), AmountUnit: "raw token units", AssetOut: assetOut, AmountOut: out.String(), OpIndex: index}
	if in.Sign() > 0 {
		ratio := new(big.Rat).SetFrac(out, in)
		trade.Price = ratio.FloatString(8)
	}
	return trade, true
}

func scValName(value xdr.ScVal) (string, bool) {
	if v, ok := value.GetSym(); ok {
		return string(v), true
	}
	if v, ok := value.GetStr(); ok {
		return string(v), true
	}
	return "", false
}
func scValAddress(value xdr.ScVal) (string, bool) {
	address, ok := value.GetAddress()
	if !ok {
		return "", false
	}
	if contract, ok := address.GetContractId(); ok {
		v, err := strkey.Encode(strkey.VersionByteContract, contract[:])
		return v, err == nil
	}
	if account, ok := address.GetAccountId(); ok {
		return accountAddress(account), accountAddress(account) != ""
	}
	return "", false
}
func scValInteger(value xdr.ScVal) (*big.Int, bool) {
	switch value.Type {
	case xdr.ScValTypeScvI32:
		if value.I32 != nil {
			return big.NewInt(int64(*value.I32)), true
		}
	case xdr.ScValTypeScvU32:
		if value.U32 != nil {
			return new(big.Int).SetUint64(uint64(*value.U32)), true
		}
	case xdr.ScValTypeScvI64:
		if value.I64 != nil {
			return big.NewInt(int64(*value.I64)), true
		}
	case xdr.ScValTypeScvU64:
		if value.U64 != nil {
			return new(big.Int).SetUint64(uint64(*value.U64)), true
		}
	case xdr.ScValTypeScvI128:
		if value.I128 != nil {
			hi := big.NewInt(int64(value.I128.Hi))
			hi.Lsh(hi, 64)
			return hi.Add(hi, new(big.Int).SetUint64(uint64(value.I128.Lo))), true
		}
	case xdr.ScValTypeScvU128:
		if value.U128 != nil {
			hi := new(big.Int).SetUint64(uint64(value.U128.Hi))
			hi.Lsh(hi, 64)
			return hi.Add(hi, new(big.Int).SetUint64(uint64(value.U128.Lo))), true
		}
	}
	return nil, false
}
func assetName(a xdr.Asset) string {
	switch a.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "XLM"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return a.String()
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return a.String()
	}
	return "unknown"
}
func accountAddress(a xdr.AccountId) string {
	key, ok := a.GetEd25519()
	if !ok {
		return ""
	}
	address, err := strkey.Encode(strkey.VersionByteAccountID, key[:])
	if err != nil {
		return ""
	}
	return address
}
func muxedAccountAddress(a xdr.MuxedAccount) string {
	if key, ok := a.GetEd25519(); ok {
		address, err := strkey.Encode(strkey.VersionByteAccountID, key[:])
		if err == nil {
			return address
		}
	}
	if muxed, ok := a.GetMed25519(); ok {
		address, err := strkey.Encode(strkey.VersionByteAccountID, muxed.Ed25519[:])
		if err == nil {
			return address
		}
	}
	return ""
}
func stroops(v xdr.Int64) string { return fmt.Sprintf("%.7f", float64(v)/10000000) }
