package stream

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// OperationQuery controls the operation stream filters.
type OperationQuery struct {
	StartLedger    uint32
	PageLimit      uint
	Types          []string
	SourceAccounts []string
}

// OperationSource deconstructs transaction envelopes into operation records.
type OperationSource struct {
	tx    *TransactionSource
	query OperationQuery
}

// NewOperationSource returns an operation source over the given RPC client.
func NewOperationSource(client rpc.Client, q OperationQuery) (*OperationSource, error) {
	if client == nil {
		return nil, fmt.Errorf("operations source: rpc client is required")
	}
	for i := range q.Types {
		q.Types[i] = normalizeOperationType(q.Types[i])
		if _, ok := operationTypeByName[q.Types[i]]; !ok {
			return nil, fmt.Errorf("unknown operation type %q", q.Types[i])
		}
	}
	return &OperationSource{
		tx:    NewTransactionSource(client, TransactionQuery{StartLedger: q.StartLedger, PageLimit: q.PageLimit}),
		query: q,
	}, nil
}

func (s *OperationSource) Name() string { return "operations" }

func (s *OperationSource) Rewind(ledger uint32) { s.tx.Rewind(ledger) }

func (s *OperationSource) CursorLedger(cursor string) (uint32, bool) {
	return s.tx.CursorLedger(cursor)
}

func (s *OperationSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := s.tx.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}
	out := Batch{NextCursor: batch.NextCursor, Latest: batch.Latest, LatestCloseTime: batch.LatestCloseTime, Oldest: batch.Oldest, Full: batch.Full}
	for _, record := range batch.Records {
		if record.Transaction == nil || record.Transaction.EnvelopeXDR == "" {
			continue
		}
		operations, err := s.extract(record)
		if err != nil {
			return Batch{}, fmt.Errorf("transaction %s: %w", record.Transaction.Hash, err)
		}
		out.Records = append(out.Records, operations...)
	}
	sortRecords(out.Records)
	return out, nil
}

func (s *OperationSource) extract(record Record) ([]Record, error) {
	var envelope xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(record.Transaction.EnvelopeXDR, &envelope); err != nil {
		return nil, fmt.Errorf("decoding envelope: %w", err)
	}
	operations, err := envelopeOperations(&envelope)
	if err != nil {
		return nil, err
	}
	results, err := operationResults(record.Transaction.ResultXDR)
	if err != nil {
		return nil, fmt.Errorf("decoding result: %w", err)
	}
	txSource := envelope.SourceAccount()
	defaultSource := txSource.Address()
	out := make([]Record, 0, len(operations))
	for index := range operations {
		op := &operations[index]
		name := operationTypeName(op.Body.Type)
		if len(s.query.Types) > 0 && !contains(s.query.Types, name) {
			continue
		}
		source := defaultSource
		if op.SourceAccount != nil {
			source = op.SourceAccount.Address()
		}
		if len(s.query.SourceAccounts) > 0 && !contains(s.query.SourceAccounts, source) {
			continue
		}
		parameters, err := xdr.MarshalBase64(op.Body)
		if err != nil {
			return nil, fmt.Errorf("operation %d parameters: %w", index, err)
		}
		operation := &Operation{
			Index:           uint32(index),
			TxIndex:         record.Transaction.Index,
			Type:            name,
			SourceAccount:   source,
			ParametersXDR:   parameters,
			TransactionHash: record.Transaction.Hash,
		}
		if index < len(results) {
			operation.ResultCode = results[index].Code.String()
			operation.ResultXDR, err = xdr.MarshalBase64(results[index])
			if err != nil {
				return nil, fmt.Errorf("operation %d result: %w", index, err)
			}
		}
		out = append(out, Record{
			ID:          fmt.Sprintf("%010d-%04d-%02d", record.Sequence, record.Transaction.Index, index),
			Sequence:    record.Sequence,
			Time:        record.Time,
			Kind:        KindOperation,
			Operation:   operation,
			Transaction: &Transaction{Hash: record.Transaction.Hash, Index: record.Transaction.Index},
		})
	}
	return out, nil
}

func operationResults(encoded string) ([]xdr.OperationResult, error) {
	if encoded == "" {
		return nil, nil
	}
	var result xdr.TransactionResult
	if err := xdr.SafeUnmarshalBase64(encoded, &result); err != nil {
		return nil, err
	}
	if result.Result.InnerResultPair != nil {
		return operationResultsFromInner(result.Result.InnerResultPair.Result)
	}
	return operationResultsFromResult(result)
}

func operationResultsFromInner(result xdr.InnerTransactionResult) ([]xdr.OperationResult, error) {
	if result.Result.Results == nil {
		return nil, nil
	}
	return *result.Result.Results, nil
}

func operationResultsFromResult(result xdr.TransactionResult) ([]xdr.OperationResult, error) {
	if result.Result.Results == nil {
		return nil, nil
	}
	return *result.Result.Results, nil
}

var operationTypeByName = map[string]xdr.OperationType{
	"create_account":                   xdr.OperationTypeCreateAccount,
	"payment":                          xdr.OperationTypePayment,
	"path_payment_strict_receive":      xdr.OperationTypePathPaymentStrictReceive,
	"manage_sell_offer":                xdr.OperationTypeManageSellOffer,
	"create_passive_sell_offer":        xdr.OperationTypeCreatePassiveSellOffer,
	"set_options":                      xdr.OperationTypeSetOptions,
	"change_trust":                     xdr.OperationTypeChangeTrust,
	"allow_trust":                      xdr.OperationTypeAllowTrust,
	"account_merge":                    xdr.OperationTypeAccountMerge,
	"inflation":                        xdr.OperationTypeInflation,
	"manage_data":                      xdr.OperationTypeManageData,
	"bump_sequence":                    xdr.OperationTypeBumpSequence,
	"manage_buy_offer":                 xdr.OperationTypeManageBuyOffer,
	"path_payment_strict_send":         xdr.OperationTypePathPaymentStrictSend,
	"create_claimable_balance":         xdr.OperationTypeCreateClaimableBalance,
	"claim_claimable_balance":          xdr.OperationTypeClaimClaimableBalance,
	"begin_sponsoring_future_reserves": xdr.OperationTypeBeginSponsoringFutureReserves,
	"end_sponsoring_future_reserves":   xdr.OperationTypeEndSponsoringFutureReserves,
	"revoke_sponsorship":               xdr.OperationTypeRevokeSponsorship,
	"clawback":                         xdr.OperationTypeClawback,
	"clawback_claimable_balance":       xdr.OperationTypeClawbackClaimableBalance,
	"set_trustline_flags":              xdr.OperationTypeSetTrustLineFlags,
	"liquidity_pool_deposit":           xdr.OperationTypeLiquidityPoolDeposit,
	"liquidity_pool_withdraw":          xdr.OperationTypeLiquidityPoolWithdraw,
	"invoke_host_function":             xdr.OperationTypeInvokeHostFunction,
	"extend_footprint_ttl":             xdr.OperationTypeExtendFootprintTtl,
	"restore_footprint":                xdr.OperationTypeRestoreFootprint,
}

func normalizeOperationType(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "OperationType")
	value = strings.TrimPrefix(value, "operationtype")
	if strings.Contains(value, "_") || strings.ToUpper(value) == value {
		return strings.ToLower(value)
	}
	var b strings.Builder
	for i, r := range value {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return strings.TrimPrefix(b.String(), "_")
}

func operationTypeName(value xdr.OperationType) string {
	for name, candidate := range operationTypeByName {
		if value == candidate {
			return name
		}
	}
	return normalizeOperationType(value.String())
}
