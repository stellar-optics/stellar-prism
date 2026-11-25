# Event Deduplication and Ring-Buffer Cache Sizing

`stellar-prism` suppresses duplicate event deliveries across Soroban RPC re-polls, transient network retries, and multi-endpoint failovers without letting heap memory grow with stream duration or contract traffic.

This guide explains the two-tier deduplication design, how event keys and content hashes are computed in [`pkg/stream/dedup.go`](../../pkg/stream/dedup.go), exact memory-sizing formulas for `--dedup-window`, and how to choose window sizes under Stellar's finality model.

---

## Architecture Overview

Deduplication in `stellar-prism` operates at two complementary layers:

1. **Tailer high-water mark ([`pkg/stream/tail.go`](../../pkg/stream/tail.go))**: Always active. Every `stream.Record` carries a fixed-width, zero-padded `ID` (for Soroban RPC events, `<toid>-<eventIndex>`, e.g., `0017037362169118720-0000000000`). The tailer keeps the highest emitted `lastID` and suppresses any incoming record with `r.ID <= t.lastID` in $O(1)$ memory, counting it under `Stats.Duplicates`.
2. **Sink-seam ring buffer ([`pkg/stream/dedup.go`](../../pkg/stream/dedup.go))**: Enabled by default with `--dedup-window 10000` on `prism events` (disabled when set to `0`). `stream.DedupSink` wraps the output sink and retains the last `N` seen event keys in a bounded ring buffer backed by an $O(1)$ lookup set.

`DedupSink` sits on the **output seam** (between `Tailer` and downstream sinks, after client-side `--topic-pattern` filtering when configured) rather than inside `Tailer` itself:

- Unmatched records rejected by client-side pattern filters do not consume slots in the deduplication ring.
- When `DedupSink` drops a duplicate, `Emit` returns `nil`, so `Tailer` still considers the record delivered and advances the durable cursor past the replayed page.
- Suppressed records are counted by `DedupSink.Dropped()` and folded into the final `--stats` summary alongside the tailer's high-water-mark duplicates.

---

## Deduplication Mechanics

### Fingerprint and Hash Computation (`EventKey`)

`stream.EventKey(r Record)` derives the deduplication key using a two-branch strategy designed to distinguish identical payloads emitted by separate events while bounding key length:

#### 1. Primary Path — Server Event Identity (`r.ID != ""`)

For standard Soroban RPC event streams (and any `Record` with a non-empty `ID`), `EventKey` uses the record kind and server-assigned ID directly without hashing:

```text
<Kind>:id:<Record.ID>
```

For example:

```text
event:id:0017037362169118720-0000000000
```

**Why `r.ID` is preferred over payload hashing:** A single contract invocation can legitimately emit two events in the same operation with byte-identical `TopicXDR` and `ValueXDR` payloads (for example, two identical `transfer` or `mint` notifications). Hashing only the payload would wrongly collapse the second event, whereas the Soroban RPC `ID` encodes the ledger/transaction/operation/event position and stays unique.

#### 2. Content-Hash Fallback Path (`r.ID == ""` and `r.Event != nil`)

When a source produces an event record without a server `ID` (such as synthetic streams, custom `Source` implementations, or tests), `EventKey` builds a composite locator prefix combined with a SHA-256 digest of the XDR payload:

```text
ev:content:<Sequence>:<ContractID>:<TxHash>:<TxIndex>:<OpIndex>:<Type>:<sha256_hex>
```

Where:

| Component | Source field | Purpose |
|---|---|---|
| `Sequence` | `r.Sequence` | Ledger sequence number (`uint32`) |
| `ContractID` | `e.ContractID` | Strkey contract address (`C...`) |
| `TxHash` | `e.TxHash` | Hex transaction hash |
| `TxIndex` | `e.TxIndex` | Zero-based transaction index within the ledger |
| `OpIndex` | `e.OpIndex` | Zero-based operation index within the transaction |
| `Type` | `e.Type` | `"contract"`, `"system"`, or `"diagnostic"` |
| `sha256_hex` | `SHA-256(TopicXDR, ValueXDR)` | 64-character lowercase hex digest |

The SHA-256 digest is computed incrementally over the raw base64 strings:

```text
SHA256(
  TopicXDR[0] || 0x00 ||
  TopicXDR[1] || 0x00 ||
  ...
  TopicXDR[k-1] || 0x00 ||
  0x00 ||
  ValueXDR
)
```

Separating topics with `0x00` bytes prevents boundary collisions (for example, `["ab", "c"]` vs. `["a", "bc"]`), and hashing the variable-length XDR payload keeps every retained key bounded in bytes even if contracts emit large `ScVal` maps or vectors.

Records that have neither an `ID` nor an `Event` payload return `""` and pass through unfiltered.

### Ring-Buffer Eviction (`Dedup`)

`stream.Dedup` maintains a fixed-capacity sliding window of **`N` event fingerprints** (not elapsed wall-clock time or ledger count):

```go
type Dedup struct {
    capacity int                 // max retained keys; <= 0 disables dedup
    ring     []string            // circular insertion-order buffer of length capacity
    seen     map[string]struct{} // O(1) membership index
    next     int                 // next slot to overwrite, wraps modulo capacity
    dropped  int                 // cumulative duplicate refusals
}
```

**Why count-bounded instead of ledger- or time-bounded:** Bounding the cache by time or ledger count makes heap usage proportional to peak event throughput. A sudden traffic spike on a high-volume contract could exhaust memory. Bounding by fingerprint count `N` guarantees a hard, predictable memory ceiling regardless of traffic rate.

When `Allow(key)` runs on the tailer's single emission goroutine:

1. **Disabled check:** If `d == nil` or `d.capacity == 0`, return `true` immediately without allocating.
2. **Duplicate check:** Look up `key` in `d.seen`. If found, increment `d.dropped` and return `false`. Importantly, a duplicate hit **does not** refresh or move `key` in `d.ring`—a replayed record retains its original insertion age.
3. **Eviction on wrap:** Inspect `old := d.ring[d.next]`. Once the ring has filled (`old != ""`), remove `old` from `d.seen` before overwriting the slot.
4. **Insertion:** Store `d.ring[d.next] = key`, insert `d.seen[key] = struct{}{}`, and advance `d.next = (d.next + 1) % d.capacity`.

For batched sinks (`EmitBatch`), `DedupSink` filters all incoming records through `Allow` in order into a `survivors` slice and forwards non-empty survivors to `BatchSink.EmitBatch` (or falls back to per-record `Emit`).

---

## Memory Footprint Calculations

### Steady-State Heap Formula

For `--dedup-window N` (`N > 0`) on a 64-bit Go runtime, `NewDedup(N)` retains up to `N` keys across three structures:

1. **Ring buffer backing array (`ring []string`)**: Each Go `string` header is 16 bytes (pointer + length):
   $$M_{\text{ring}}(N) = 16 \cdot N \text{ bytes}$$

2. **Retained key string bytes (`S_alloc`)**: Because `ring[i]` and the map key share the same backing bytes (Go does not copy string data on `d.seen[key] = struct{}{}`), each distinct key pays its string heap allocation once:
   - **Primary RPC ID key** (`event:id:0017037362169118720-0000000000`, ~39 bytes): rounds to the 48-byte Go allocator size class ($S_{\text{alloc}} \approx 48\text{ B}$).
   - **Content-hash fallback key** (`ev:content:...:<sha256>`, ~215 bytes): rounds to the 224-byte Go allocator size class ($S_{\text{alloc}} \approx 224\text{ B}$).

3. **Hash set (`seen map[string]struct{}`)**: With zero-byte `struct{}` values, Go maps store 16-byte string headers plus control/tophash metadata across power-of-two bucket/group tables. Accounting for load factor (~75–87.5%) and power-of-two growth slack, map storage averages $B_{\text{map}} \approx 24\text{–}36\text{ bytes/entry}$.

Combining all three terms gives the steady-state retained memory formula:

$$M_{\text{total}}(N) = N \cdot \left(16 + S_{\text{alloc}} + B_{\text{map}}\right)$$

Using conservative planning constants ($B_{\text{map}} = 36\text{ B}$):

- **Soroban RPC ID keys** ($S_{\text{alloc}} = 48\text{ B}$):
  $$M_{\text{id}}(N) \approx 100 \cdot N \text{ bytes} \quad (\approx 0.10\text{ KB per entry})$$
- **Content-hash fallback keys** ($S_{\text{alloc}} = 224\text{ B}$):
  $$M_{\text{content}}(N) \approx 276 \cdot N \text{ bytes} \quad (\approx 0.27\text{ KB per entry})$$

*(Note: During initial stream warm-up, transient per-event allocations from `EventKey` string construction also occur; the built-in `BenchmarkDedupSinkFiftyThousandEvents` in `pkg/stream/bench_test.go` measures ~423 bytes/event total transient allocation across 50,000 events with the default cache size, which is reclaimed by GC.)*

### Table 1: Direct `--dedup-window` Memory Footprint

| `--dedup-window` (`N`) | Ring Array (`16·N`) | Map Index (`~36·N`) | Total RAM (RPC ID Keys, `~100 B/entry`) | Total RAM (Content Hash, `~276 B/entry`) | Operational Role |
|---|---|---|---|---|---|
| `0` | `0 B` | `0 B` | `0 B` | `0 B` | Disabled; relies solely on `Tailer.lastID` |
| `100` | `1.6 KB` | `3.5 KB` | `~9.8 KB` | `~27.0 KB` | Minimal single-batch replay guard |
| `1,000` | `15.6 KB` | `35.2 KB` | `~97.7 KB` | `~269.5 KB` | Light contract stream (~5 full RPC pages) |
| `4,096` | `64.0 KB` | `144.0 KB` | `~400.0 KB` | `~1.10 MB` | Moderate replay window (~20 RPC pages) |
| `10,000` (`DefaultDedupWindow`) | `156.3 KB` | `351.6 KB` | `~976.6 KB` | `~2.63 MB` | Default multi-page replay window |
| `50,000` | `781.3 KB` | `1.72 MB` | `~4.77 MB` | `~13.16 MB` | High-throughput DEX / bridge indexing |
| `100,000` | `1.53 MB` | `3.43 MB` | `~9.54 MB` | `~26.32 MB` | Multi-hour replay cushion at moderate rate |
| `1,000,000` | `15.26 MB` | `34.33 MB` | `~95.37 MB` | `~263.21 MB` | Upper practical in-process ceiling |

### Translating Target Ledger Depth to `--dedup-window`

Because `--dedup-window` bounds **event count** rather than ledger count, size `N` from your desired ledger coverage window $L$ and your filter's average matched events per ledger $\bar{E}_{\text{ledger}}$:

$$N = L \times \bar{E}_{\text{ledger}}$$

At Stellar's ~5-second ledger close cadence:
- **100 ledgers** spans **~8.3 minutes**
- **1,000 ledgers** spans **~1.4 hours**
- **10,000 ledgers** spans **~13.9 hours**

### Table 2: Ledger-Span Sizing Matrix (`100` vs. `10,000` Ledgers)

The table below shows the required `--dedup-window` and resulting steady-state RAM (using RPC ID keys at `~100 B/entry`) for different event rates:

| Target Ledger Span ($L$) | Approximate Wall Time | Low Traffic (`1 event/ledger`) | Moderate Traffic (`10 events/ledger`) | Hot Contract (`100 events/ledger`) |
|---|---|---|---|---|
| **100 ledgers** | ~8.3 min | `N = 100` (`~9.8 KB`) | `N = 1,000` (`~97.7 KB`) | `N = 10,000` (`~976.6 KB`) |
| **1,000 ledgers** | ~1.4 hr | `N = 1,000` (`~97.7 KB`) | `N = 10,000` (`~976.6 KB`) | `N = 100,000` (`~9.54 MB`) |
| **10,000 ledgers** | ~13.9 hr | `N = 10,000` (`~976.6 KB`) | `N = 100,000` (`~9.54 MB`) | `N = 1,000,000` (`~95.37 MB`) |

For high-throughput streams needing coverage across 10,000+ ledgers, note how `100 events/ledger` requires `N = 1,000,000` (~95 MB) in-memory. At that scale, keeping `--dedup-window` sized for reconnect/failover overlap (`4,096`–`50,000`) and enforcing downstream idempotency keyed on `Record.ID` (e.g., PostgreSQL primary keys or SQS FIFO `MessageDeduplicationId`) is more memory-efficient than sizing an in-memory ring for an entire day.

---

## Re-org Resilience and Network Finality Guidelines

### Stellar Finality vs. Duplicate Delivery Sources

Stellar uses the **Stellar Consensus Protocol (SCP)**, which provides **deterministic single-ledger finality**: once a ledger closes, it is final and cannot undergo probabilistic chain reorganizations (unlike proof-of-work or longest-chain networks).

As a result, `stellar-prism` does not need a deep "re-org rollback buffer" to undo orphaned blocks. Instead, duplicate event deliveries arise from transport and RPC topology effects:

1. **Mid-pagination HTTP retries and reconnects:** A dropped connection forces `Tailer` to re-poll with its unchanged cursor. `Tailer.lastID` catches ordered ID replays automatically; `DedupSink` adds protection when custom/ID-less sources or overlapping responses are involved.
2. **Multi-endpoint RPC failover (`stream.Pool`):** If the primary RPC node errors or stalls and `stream.Pool` switches to a secondary RPC node, the secondary node may be a few ledgers behind or serve slightly different boundary pages around the failover point.
3. **High-availability standby takeover (`--ha-lease-file`):** When a passive standby promotes to leader, it resumes from the last committed `--state-file` checkpoint, which may re-poll up to one page of already-emitted events.
4. **Watchlist or multi-chunk queries:** Splitting more than `MaxContractIDs` watched contracts across multiple `getEvents` requests merges records before emission.

### Recommended Sizing by Deployment Profile

| Scenario | Recommended `--dedup-window` | Rationale |
|---|---|---|
| Single RPC endpoint, standard `--follow` | `10,000` (default) | `Tailer.lastID` already prevents ordered in-run page replays; the cache also protects against duplicate IDs across overlapping responses. |
| Multi-endpoint `--rpc-url` pool failover | `10000`–`50000` | Covers ~100–500 ledgers of moderate traffic (~1–5 MB) across lagging backup RPC nodes during failover. |
| Active-passive HA (`--ha-lease-file`) | `4096`–`10000` | Note: `Dedup` state is in-memory and starts empty on standby promotion; pair a modest `--dedup-window` with `--state-file` + downstream `id`-keyed upserts. |
| Custom / hand-built sources without `Record.ID` | `>= 2 × batch_size` (e.g. `4096`) | Activates the `ev:content:...:<sha256>` fallback fingerprint so replayed pages without IDs are still suppressed. |
| Intentional reprocessing with `--rewind` / `SIGUSR2` | `10,000` (default) | The CLI clears the sink cache when a rewind is applied, so the requested historical records can be delivered again. |
