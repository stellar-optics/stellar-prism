# Multi-Stream Aggregation and Ledger Throughput Analytics

Streaming analytics pipelines often summarize raw Soroban contract events and ledger activity into time-bounded rollups—measuring event throughput per contract, active callers, or resource fee expenditure. `stellar-prism` provides `prism aggregate` for windowed contract-event rollups alongside `prism fee-stats` and `prism transactions` for ledger-wide throughput and fee analytics.

For implementation details, see [`internal/cli/aggregate.go`](../../internal/cli/aggregate.go) and [`pkg/stream/aggregate.go`](../../pkg/stream/aggregate.go).

---

## Aggregation Windows

`prism aggregate` requires at least one `--contract`, a `--window` duration, and one or more `--metric` definitions:

```sh
# Count events per watched contract every 1 minute
prism aggregate --contract CDLZF... --contract CBGSB... --window 1m --metric count --follow

# Aggregate over 5-minute windows with JSON output
prism aggregate --contract CDLZF... --window 5m --metric count --follow --json

# Hourly rollup starting from a specific ledger
prism aggregate --contract CDLZF... --from 3966000 --window 1h --metric count --follow
```

### Supported `--window` Options

`parseWindow` in [`internal/cli/aggregate.go`](../../internal/cli/aggregate.go) parses `<integer><unit>` durations where `<unit>` is:

| Suffix | Unit | Common Examples |
|---|---|---|
| `s` | Seconds | `10s`, `30s` |
| `m` | Minutes | `1m`, `5m`, `15m` |
| `h` | Hours | `1h`, `6h` |

Up to 5 `--contract` filters can be combined with `--topic` and `--type` (`contract`, `system`, `diagnostic`), plus standard streaming flags such as `--from`, `--follow`, `--limit`, `--state-file`, `--resume`, `--health-port`, and `--ha-lease-file`.

### Tumbling vs. Sliding Window Calculations

Time-windowed streaming analytics typically use one of two window geometries:

| Property | Tumbling Window (`prism aggregate`) | Sliding Window |
|---|---|---|
| **Geometry** | Fixed-size, contiguous, **non-overlapping** intervals of duration $W$ | Overlapping intervals of length $W$ advancing every slide step $S < W$ |
| **Bucket assignment** | Each event belongs to **exactly one** window | Each event contributes to $\lceil W / S \rceil$ concurrent windows |
| **Epoch boundary math** | $\text{win} = \lfloor t / W \rfloor$, span $[\text{win}\cdot W,\; (\text{win}+1)\cdot W)$ | Windows $[t_0 + kS,\; t_0 + kS + W)$ for integer steps $k$ |
| **Memory overhead** | $O(\text{contracts} \times \text{active buckets})$ | $O(\frac{W}{S} \times \text{contracts})$ — scales with overlap factor |
| **Use case** | Periodic rollups, billing/accounting intervals, clean time-series bars | Smooth moving averages (`fee-stats` 10/50-ledger windows), spike smoothing |

`AggregateSource` implements **epoch-aligned tumbling windows**:

1. For each incoming `KindEvent` record with ledger close timestamp $t$ (falling back to current UTC time if zero), `windowKey(t)` assigns the record to bucket:
   $$\text{win} = \left\lfloor \frac{t_{\text{unix\_ns}}}{W_{\text{ns}}} \right\rfloor$$
   with `windowStart = win * W` and `windowEnd = (win + 1) * W`.
2. Records accumulate per `(win, ContractID)` inside `aggBucket.data`.
3. On each poll cycle, any bucket whose `windowEnd <= time.Now().UTC()` is sealed, converted into aggregate `Record` items (one per contract and metric), emitted downstream, and deleted from memory (`delete(a.buckets, win)`).
4. If no window has closed yet during a poll, `AggregateSource` returns an empty batch while preserving `NextCursor` so the `Tailer` cursor continues advancing without re-polling past events.

For moving-window smoothing over ledger fees without double-counting, `prism fee-stats` separately computes 10-ledger and 50-ledger rolling averages (`averageFee10`, `averageFee50`).

---

## Aggregated Metrics

`prism aggregate` parses `--metric` using `parseMetrics` and groups results by `ContractID` within each completed window:

| Metric flag | Definition | Notes |
|---|---|---|
| `count` | Total matched events emitted by the contract in the window: $N_c = \sum \mathbf{1}$ | Active in `AggregateSource`; increments per matched `KindEvent`. |
| `sum:<field>` | Sum of a numeric field across events in the window: $\sum x_i$ | Requires a field specifier (e.g., `sum:value.amount`). |
| `avg:<field>` | Arithmetic mean over events with numeric field values: $\frac{\sum x_i}{N}$ | Computed as `sums[field] / counts[field]` when `counts[field] > 0`. |
| `min:<field>` | Minimum observed value of `<field>` in the window | Tracked per contract and field. |
| `max:<field>` | Maximum observed value of `<field>` in the window | Tracked per contract and field. |
| `unique_count:<field>` | Cardinality of distinct string values for `<field>`: $\lvert \{v_i\} \rvert$ | Maintained via per-field set `uniqueSets[field]`. |

> [!NOTE]
> In the current `pkg/stream/aggregate.go` implementation, `count` is fully active, while `extractNumber` (`sum`, `avg`, `min`, `max`) and `extractString` (`unique_count`) are stubbed pending decoded ScVal field-path navigation. For production transaction-level unique account counts and fee expenditure today, combine `prism aggregate` with the `prism transactions` and `prism fee-stats` pipelines below.

### 1. Event Frequency by Contract ID

Watch up to 5 contracts simultaneously to compare invocation and event throughput per window:

```sh
prism aggregate \
  --contract CDLZF... \
  --contract CBGSB... \
  --window 1m \
  --metric count \
  --follow --json
```

Each closed 1-minute bucket emits one record per active contract carrying its event count, keyed by `<contractID>-<win>-count-`.

### 2. Distinct Transaction Source Accounts

To measure unique active accounts interacting with your contracts or operations across a stream window:

```sh
# Stream transactions with source/fee-payer attribution and count unique accounts per minute
prism transactions --from 3966000 --follow --json \
  | jq -r '[.time[0:16], .feeAccount] | @tsv' \
  | sort -u | cut -f1 | uniq -c

# Or filter to specific operation types via prism operations
prism operations --type invoke_host_function --follow --json \
  | jq -r '.sourceAccount'
```

Within `prism aggregate`, `unique_count:<field>` is designed to compute per-contract unique field cardinalities (such as `--metric "unique_count:value.from"`) directly in-memory once field extraction is enabled.

### 3. Total Soroban Fee Expenditure and Ledger Throughput

Monitor Soroban resource fees, ledger transaction counts, and rolling fee averages across closed ledgers with `prism fee-stats`:

```sh
# Live ledger fee market and rolling 10- and 50-ledger transaction fee averages
prism fee-stats --follow

# Emit NDJSON fee-market snapshots for time-window summation (e.g., per-minute Soroban fee expenditure)
prism fee-stats --follow --json \
  | jq -c '{ledger, time, transactions, sorobanResourceFee, sorobanFeeBid, averageFee10, averageFee50}'
```

| `fee-stats` field | Meaning |
|---|---|
| `transactions` | Number of transactions included in the ledger |
| `sorobanResourceFee` | Total Soroban resource fees charged in the ledger (stroops) |
| `sorobanFeeBid` | Total Soroban fee bids submitted in the ledger (stroops) |
| `averageFee10` / `averageFee50` | Rolling 10-ledger and 50-ledger average transaction fees |
| `baseFee` / `feePool` | Network base fee and current fee pool balance |

---

## Output Formats

`prism aggregate` supports terminal human output and periodic JSON/NDJSON streaming:

### Terminal View (Default)

Running `prism aggregate` without `--json` renders each emitted window summary through `sink.NewHuman`:

```sh
prism aggregate --contract CDLZF... --window 1m --metric count --follow --stats-bar
```

- Each closed window prints a line containing the window timestamp (`windowStart`), sequence, contract ID, metric tags (`TopicXDR`: `[windowKey, metricName, field]`), and aggregated value (`ValueXDR`).
- Adding `--stats-bar` (with `--follow`) displays a live stderr progress line showing emitted records, poll cadence, and stream lag.

### Periodic JSON Stream (`--json` / `--format ndjson`)

Passing `--json` (or `--format ndjson`) configures `sink.NewNDJSON`, emitting one JSON object per `(window, contract, metric)` as each tumbling window closes:

- `kind`: `"event"`
- `id`: `"<contractID>-<windowKey>-<metricName>-<field>"`
- `ledger`: `uint32(windowStart.Unix())`
- `time`: RFC 3339 UTC timestamp of `windowStart`
- `type`: `"aggregate"`
- `contract`: Contract strkey (`C...`)
- `topics` / `value`: Metric identifier tuple `[windowKey, metricName, field]` and computed metric value

For downstream analytics, pipe `prism aggregate --json` or `prism fee-stats --json` directly into `jq`, `grep`, or a time-series database sink.
