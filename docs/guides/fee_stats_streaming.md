# Streaming fee congestion and base reserve metrics

`prism fee-stats` emits one record per closed ledger describing Stellar's fee
market: the network base fee and base reserve, a percentile breakdown of what
transactions bid and actually paid, how full the ledger was, and whether it
closed under surge pricing. Wallets and trading systems use it to price
transactions dynamically, paying the base fee when the network is quiet and
bidding just enough when it is busy.

- [Quick start](#quick-start)
- [How Stellar fees work, briefly](#how-stellar-fees-work-briefly)
- [Payload schema](#payload-schema)
- [Percentile methodology](#percentile-methodology)
- [Surge pricing indicators](#surge-pricing-indicators)
- [Recommended fee recipes](#recommended-fee-recipes)
- [Operational notes](#operational-notes)

The derivation lives in
[`pkg/stream/fee_stats.go`](../../pkg/stream/fee_stats.go) (`FeeStatsSource`,
`summarizeFeeMarket`, `surgePricing`) and the schema in
[`pkg/stream/record.go`](../../pkg/stream/record.go) (`FeeStats`,
`FeeDistribution`).

## Quick start

```console
# Follow the fee market, human-readable
$ prism fee-stats --follow
2026-11-25T14:35:13Z  ledger 3966812  base fee 100 stroops  base reserve 5000000 stroops  fee pool 1873456123456  coins 1054439020873472865  avg fee 10/50 8412.60/2210.30  tx 212  Soroban resource/bid 4183271/6950110  inclusion fee/op p50/p90/p99 1200/1200/1350  capacity 100%  surge yes (capacity+fee)

# NDJSON for programs
$ prism fee-stats --follow --json

# Mainnet, through your own endpoint
$ prism fee-stats --follow --json --network mainnet --rpc-url https://rpc.example.com
```

`fee-stats` takes all the [shared streaming
flags](../cli/command_reference.md#shared-streaming-flags), so its output can
go to Kafka, a webhook, Prometheus or a file like any other stream.

## How Stellar fees work, briefly

A Stellar transaction's fee has up to two parts:

- **Inclusion fee.** What the transaction offers, per operation, for a place
  in the ledger. The protocol minimum is the ledger's *base fee*, currently
  100 stroops per operation (1 XLM = 10,000,000 stroops).
- **Resource fee** (Soroban only). Payment for CPU instructions, ledger
  reads and writes, events and rent, estimated by simulating the
  transaction. It is not part of the inclusion auction.

The envelope's `fee` field is the **maximum** a transaction is willing to pay.
What it is actually **charged** depends on demand:

- **No congestion.** Everything submitted fits in the ledger. Every included
  transaction is charged exactly the base fee per operation, whatever it
  bid.
- **Surge pricing.** More is submitted than fits. Validators include the
  highest bidders and charge each of them the *lowest accepted bid* per
  operation, so the price of inclusion rises above the base fee. Classic and
  Soroban transactions compete in separate lanes, each with its own
  capacity.

A fee-bump transaction pays for its inner transaction's operations plus one
for the wrapper.

## Payload schema

One record per ledger, with `--json`:

```json
{
  "kind": "fee_stats",
  "id": "0003966812",
  "ledger": 3966812,
  "time": "2026-11-25T14:35:13Z",
  "baseFee": 100,
  "baseReserve": 5000000,
  "feePool": 1873456123456,
  "coinsInCirculation": 1054439020873472865,
  "averageFee10": 8412.6,
  "averageFee50": 2210.3,
  "transactions": 212,
  "sorobanResourceFee": 4183271,
  "sorobanFeeBid": 6950110,
  "operations": 1037,
  "classicOperations": 1000,
  "maxTxSetSize": 1000,
  "capacityUsage": 1,
  "inclusionFeeCharged": { "min": 1200, "mode": 1200, "p10": 1200, "p50": 1200, "p90": 1200, "p99": 1350, "max": 1350 },
  "inclusionFeeBid":     { "min": 1200, "mode": 2000, "p10": 1500, "p50": 2000, "p90": 10000, "p99": 50000, "max": 100000 },
  "surgePricing": true,
  "surgeReason": "capacity+fee"
}
```

All amounts are in stroops.

### Ledger header fields

| Field | Type | Meaning |
|---|---|---|
| `ledger` | uint32 | Ledger sequence |
| `time` | RFC 3339 | Ledger close time |
| `baseFee` | uint32 | Minimum inclusion fee per operation |
| `baseReserve` | uint32 | Reserve unit: an account must hold `(2 + subentries) × baseReserve` |
| `feePool` | int64 | Fees collected and held by the network |
| `coinsInCirculation` | int64 | Total lumens in existence |
| `maxTxSetSize` | uint32 | Classic capacity of the ledger, in operations |

### Fee distribution fields

| Field | Type | Meaning |
|---|---|---|
| `inclusionFeeCharged` | distribution | Per-operation inclusion fee each transaction was actually charged |
| `inclusionFeeBid` | distribution | Per-operation maximum inclusion fee each transaction offered |
| `.min` | int64 | Lowest value |
| `.mode` | int64 | Most frequent value; on a tie, the lowest of the tied values |
| `.p10`, `.p50`, `.p90`, `.p99` | int64 | Nearest-rank percentiles; `p50` is the median |
| `.max` | int64 | Highest value. `inclusionFeeBid.max` is the highest bid in the ledger |

Both distributions are omitted for a ledger with no transactions.

### Capacity and surge fields

| Field | Type | Meaning |
|---|---|---|
| `transactions` | uint32 | Transactions in the ledger with a readable result |
| `operations` | uint32 | Operations across those transactions, counting a fee-bump wrapper as one |
| `classicOperations` | uint32 | The part of `operations` outside Soroban transactions |
| `capacityUsage` | float | `classicOperations / maxTxSetSize`; `1` means the classic lane was full |
| `surgePricing` | bool | The ledger closed under surge pricing (see [below](#surge-pricing-indicators)) |
| `surgeReason` | string | `capacity`, `fee`, or `capacity+fee`; omitted when not surging |

### Totals and rolling averages

| Field | Type | Meaning |
|---|---|---|
| `sorobanResourceFee` | int64 | Sum of Soroban resource fees charged in the ledger |
| `sorobanFeeBid` | int64 | Sum of Soroban resource fees declared in envelopes |
| `averageFee10`, `averageFee50` | float | Mean *total* fee charged per transaction over the last 10 and 50 ledgers this run has seen |

The averages include Soroban resource fees and are measured per transaction,
not per operation. They describe overall spending and are a poor guide for
bidding; use the inclusion-fee distributions to set bids. The rolling window
starts empty, so the averages cover fewer ledgers until the run has seen 10
or 50 of them.

## Percentile methodology

For every ledger, prism fetches the header and the ledger's transactions and
builds the two distributions as follows.

1. **One sample per transaction.** Each transaction contributes exactly one
   value to each distribution, whatever its size, because the fee market
   ranks transactions by their per-operation bid. A transaction whose
   envelope cannot be decoded is left out.

2. **Operation count.** `ops` is the number of operations, plus one for a
   fee-bump wrapper.

3. **Remove Soroban resource fees.** For a Soroban transaction:

   ```text
   inclusion charged = feeCharged − resource fee charged     (from the result and metadata)
   inclusion bid     = envelope fee − declared resourceFee   (from sorobanData)
   ```

   Classic transactions have no resource fee, so their inclusion fee is the
   whole fee. Removing the resource part makes classic and Soroban
   transactions comparable: both are priced per operation for inclusion.

4. **Per operation.** Divide by `ops` and round down, as the network does:

   ```text
   charged per op = inclusion charged / ops
   bid per op     = inclusion bid / ops
   ```

5. **Nearest-rank percentiles.** Sort the n values ascending. The p-th
   percentile is the value at 1-based rank `⌈p/100 × n⌉`. It is always an
   observed value, never an interpolation, so it is a fee some transaction
   really paid or bid.

   Worked example, with charged-per-op values from 6 transactions:

   ```text
   sorted:  100  100  100  200  500  1000     n = 6
   p10  rank ⌈0.6⌉ = 1  → 100
   p50  rank ⌈3.0⌉ = 3  → 100
   p90  rank ⌈5.4⌉ = 6  → 1000
   p99  rank ⌈5.94⌉ = 6 → 1000
   mode                 → 100  (three times)
   ```

   With few transactions, the upper percentiles collapse onto `max`. Treat
   p90 and p99 as meaningful only when `transactions` is reasonably large,
   or aggregate over several ledgers (see the [recipes](#recommended-fee-recipes)).

### Reading the two distributions together

- `inclusionFeeCharged` is the **clearing price**: what it actually cost to
  get in. Outside a surge it sits flat at `baseFee`.
- `inclusionFeeBid` is **willingness to pay**. Wallets that bid generously
  push it up even in a quiet ledger, so a high bid percentile alone does not
  mean congestion.
- When `inclusionFeeCharged.min` rises above `baseFee`, the auction is
  binding: the cheapest included transaction paid more than the minimum.

## Surge pricing indicators

RPC does not expose validators' transaction queues, so prism cannot observe
queue pressure directly. Instead it infers surge pricing from evidence that
every closed ledger carries: how full the ledger got relative to its capacity,
and what the cheapest included transaction paid.
`surgePricing` is `true` when either trigger fires:

| Trigger | `surgeReason` | Condition | Why it indicates a surge |
|---|---|---|---|
| Capacity | `capacity` | `classicOperations ≥ maxTxSetSize` (`capacityUsage ≥ 1`) | The classic lane is full, so transactions competed for space and some were left in the queue. |
| Fee | `fee` | `inclusionFeeCharged.min > baseFee` | Without a surge, every included transaction is charged exactly the base fee. The cheapest one paying more is only possible when bids compete. |

When both fire, `surgeReason` is `capacity+fee`.

### Limits of the inference

- **Soroban lane capacity is not in the header.** Soroban limits
  (transaction count, instructions, read and write bytes per ledger) are
  network configuration settings, not header fields, so the capacity trigger
  covers only the classic lane. A Soroban-only surge is caught by the fee
  trigger when every included transaction paid above the base fee.
- **A full ledger can clear at the base fee.** If the lowest accepted bid was
  the base fee, a full classic lane fires `capacity` but not `fee`. That is
  correct: there was competition for space, even though the clearing price
  stayed at the minimum.
- **Per-ledger, not predictive.** The flag describes the ledger that just
  closed. Surges start and end within seconds, so pricing recipes look at a
  short window of recent ledgers (next section).
- **Large ledgers.** prism reads up to 1,000 transactions per ledger. A
  ledger with more is summarised from the first 1,000.

## Recommended fee recipes

These recipes compute the **inclusion fee per operation** to bid. The fee to
put in the envelope is then:

```text
envelope fee = per-op bid × operations  (+1 operation for a fee bump)
             + Soroban resource fee     (from simulateTransaction / prism simulate; 0 for classic)
```

### Recipe 1: shell, one line per ledger

```sh
prism fee-stats --follow --json | jq -r --unbuffered '
  if .surgePricing
  then "ledger \(.ledger) surge (\(.surgeReason)): bid \(.inclusionFeeCharged.p90 * 1.1 | ceil) stroops/op"
  else "ledger \(.ledger) normal: bid \(.baseFee) stroops/op"
  end'
```

This bids the base fee in a calm market, and 10% over the 90th percentile of
what was actually charged during a surge.

### Recipe 2: Go, a rolling-window fee advisor

Considering several ledgers smooths out a single odd ledger and reacts as
soon as a surge appears. This advisor keeps the last five ledgers (about 30
seconds), returns the base fee unless one of them surged, and otherwise bids
a chosen percentile of recent clearing prices plus headroom, capped by a
ceiling:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// FeeAdvisor turns a stream of per-ledger fee stats into a recommended
// inclusion fee per operation, in stroops.
type FeeAdvisor struct {
	Window   int     // recent ledgers to consider, e.g. 5 (~30 s)
	Headroom float64 // multiplier over recent winners during a surge, e.g. 1.1
	Ceiling  int64   // cap per operation; 0 means no cap

	recent []stream.FeeStats
}

// Observe records one ledger's stats.
func (a *FeeAdvisor) Observe(s stream.FeeStats) {
	a.recent = append(a.recent, s)
	if w := max(a.Window, 1); len(a.recent) > w {
		a.recent = a.recent[len(a.recent)-w:]
	}
}

// Recommend returns the inclusion fee per operation to bid. percentile is
// 50, 90 or 99: roughly the share of recent transactions the bid should
// match or beat. Outside a surge the base fee is enough, because every
// included transaction is charged the base fee anyway.
func (a *FeeAdvisor) Recommend(percentile int) int64 {
	if len(a.recent) == 0 {
		return 100 // the network minimum, until the first ledger arrives
	}
	base := int64(a.recent[len(a.recent)-1].BaseFee)

	surging := false
	var target int64
	for _, s := range a.recent {
		surging = surging || s.SurgePricing
		if d := s.InclusionFeeCharged; d != nil {
			target = max(target, pick(d, percentile))
		}
	}
	if !surging {
		return base
	}

	headroom := max(a.Headroom, 1)
	bid := max(int64(math.Ceil(float64(target)*headroom)), base)
	if a.Ceiling > 0 {
		bid = min(bid, a.Ceiling)
	}
	return bid
}

func pick(d *stream.FeeDistribution, percentile int) int64 {
	switch {
	case percentile >= 99:
		return d.P99
	case percentile >= 90:
		return d.P90
	default:
		return d.P50
	}
}

// TotalFee is the envelope fee for a transaction with ops operations: the
// per-operation bid times the operation count, plus the Soroban resource fee
// from simulation (zero for classic transactions).
func TotalFee(perOp int64, ops int, sorobanResourceFee int64) int64 {
	return perOp*int64(ops) + sorobanResourceFee
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := rpc.New(rpc.DefaultURL, 30*time.Second)
	src, err := stream.NewFeeStatsSource(client, stream.LedgerQuery{PageLimit: stream.DefaultLedgerPageLimit})
	if err != nil {
		log.Fatal(err)
	}

	advisor := &FeeAdvisor{Window: 5, Headroom: 1.1, Ceiling: 100_000}
	out := stream.SinkFunc(func(r stream.Record) error {
		if r.FeeStats == nil {
			return nil
		}
		advisor.Observe(*r.FeeStats)
		fmt.Printf("ledger %d  surge=%-5v  standard %d  fast %d  urgent %d stroops/op\n",
			r.Sequence, r.FeeStats.SurgePricing,
			advisor.Recommend(50), advisor.Recommend(90), advisor.Recommend(99))
		return nil
	})

	tailer := stream.NewTailer(src, out, stream.Options{Follow: true})
	if err := tailer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

Run it with `go run .` in a module that requires
`github.com/stellar-optics/stellar-prism`. A wallet would keep one advisor
alive in a background goroutine, guarding it with a mutex, and call
`TotalFee(advisor.Recommend(90), len(ops), simulatedResourceFee)` when it
builds a transaction.

Choosing the tier:

| Tier | Percentile | Use for |
|---|---|---|
| Standard | p50 | Not time-sensitive; may wait a few ledgers during a surge |
| Fast | p90 | Interactive payments; usually included in the next ledger |
| Urgent | p99 | Liquidations, arbitrage, anything that must land now |

### Recipe 3: feed a service without writing Go

Publish the stream once and let every client read the latest value:

```console
# Live fan-out to browsers or backend services over Server-Sent Events
prism fee-stats --follow --serve-sse 127.0.0.1:8081
curl -N http://127.0.0.1:8081/events

# Or into Kafka for many consumers
prism fee-stats --follow --kafka-brokers kafka:9092 --kafka-topic stellar-fee-stats
```

### Guard rails for any recipe

- **Always set a ceiling.** During extreme surges, p99 can be orders of
  magnitude above the base fee. Cap what you are willing to pay, and fail or
  queue the transaction above it.
- **The bid is a maximum.** You are charged the clearing price, not your bid,
  so modest headroom costs little when the market is calm.
- **Fall back safely.** If the stream stalls (check `--heartbeat` and
  `/healthz` with `--health-port`), fall back to the base fee times a fixed
  multiplier rather than to a stale surge price.
- **Fee-bump to rescue a stuck transaction.** If a transaction misses its
  window during a surge, re-bid with a fee bump at a higher tier rather than
  resubmitting.

## Operational notes

- **RPC cost.** Each ledger costs one `getTransactions` call on top of the
  ledger fetch. With the default `--page-size 50`, catching up from `--from`
  issues many requests; tune with `--page-size`, `--adaptive` and the retry
  flags described in [troubleshooting](../troubleshooting/streaming_gaps_and_lag.md).
- **History.** `--from` reaches back only as far as the RPC's retention
  window. For long-term fee history, archive the stream as it runs
  (`--output-file`, `--cloud-bucket`, `--sink-parquet-dir`).
- **Base reserve changes** are rare network upgrades. Alert on a change in
  `baseReserve` to update minimum-balance calculations
  (`(2 + subentries) × baseReserve`) without hard-coding them.
- **Ordering and resumption** follow the same guarantees as every prism
  stream: records arrive in ledger order, and `--state-file` or `--pg-dsn`
  resume where a previous run stopped.
