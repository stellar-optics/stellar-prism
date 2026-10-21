# Architecture

How prism fits together, and where to make the two changes contributors most
often want to make:

- [Adding a stream source](#adding-a-stream-source)
- [Adding an output sink](#adding-an-output-sink)

## The shape of the problem

Soroban RPC is **poll-only**. There is no subscription method, no websocket,
no server push. So "streaming" means polling on a cursor, and every hard part
of this project follows from that:

- a dropped connection must not skip records;
- a server that replays a page must not produce duplicates;
- a stream that falls behind retention must not silently lose data;
- a slow consumer must not cause unbounded memory growth.

Everything below is in service of those four.

## The pipeline

```
        ┌──────────┐   Batch    ┌────────┐   Record   ┌──────┐
RPC ───►│  Source  ├───────────►│ Tailer ├───────────►│ Sink ├───► stdout
        └──────────┘            └────────┘            └──────┘
         events.go               tail.go               ndjson.go
         ledgers.go            cursor, dedup,          human.go
         transactions.go       backoff, gaps           file_rotator.go
         storage_changes.go                          (via lens)
         wasm_events.go
              ▲
              │ (optional)
         stream.Pool  — round-robin + failover over several RPC URLs
```

One goroutine, no channels. The tailer fetches a page, emits its records
synchronously, advances the cursor, and repeats. Concurrency would buy nothing
— the bottleneck is the network — and would cost determinism in tests.

## Package layout

```
cmd/prism/            main; maps errors to exit codes
  gendocs.go          man page and Markdown generator
internal/cli/         cobra commands — thin wrappers over pkg/stream
  color.go            --color parsing and NO_COLOR handling
  completion.go       bash/zsh/fish/powershell script generation
  abi_resolve.go      contract WASM lookup and embedded-spec parsing
internal/fakerpc/     scriptable in-memory RPC for tests
pkg/rpc/              the Client interface + SDK adapter
pkg/stream/
  record.go           Record: the neutral item every source produces
  source.go           Source and Sink interfaces, GapError   ← the seams
  tail.go             the state machine
  events.go           contract events source
  ledgers.go          ledger close source
  transactions.go     transaction source
  storage_changes.go  contract storage mutations (from tx meta)
  wasm_events.go      WASM uploads and contract deployments
  pool.go             multi-endpoint failover RPC client
  drainer.go          concurrent historical backfill + ordered merge
  batcher.go          count/time batching sink wrapper
  gap_recovery.go     GapStrategy, Rewinder and gap handling
  context.go          TaskGroup and cancellation helpers
  clock.go            Clock and Backoff, both injectable
pkg/sink/
  ndjson.go           NDJSON, one object per line
  human.go            terminal output, payloads via lens
  file_rotator.go     size/time rotating file sink, optional gzip
```

## The state machine

```go
cursor = ""            // empty means "start where the Source was configured"
lastID = ""            // high-water mark

for {
    batch, err := fetchWithRetry(cursor)   // retries the SAME cursor
    if gap(cursor, batch.Oldest) { return GapError }

    for r := range batch.Records {
        if r.ID <= lastID { skip }          // replay or reorder
        sink.Emit(r); lastID = r.ID
    }

    cursor = batch.NextCursor               // advance only after a clean emit
    if batch.Full { continue }              // more waiting; drain at network speed
    if !follow { return }
    clock.Sleep(pollInterval)
}
```

Four details carry the weight:

**1. Retry never advances the cursor.** `fetchWithRetry` loops on the same
cursor value. This is why a dropped connection cannot cause a gap, and it is
why `Source.Fetch` must be idempotent — a Source that mutated internal state
on a failed call would break the guarantee.

**2. IDs are fixed-width and zero-padded.** RPC event IDs look like
`0017037362169118720-0000000000`, so *lexicographic order equals chronological
order*. De-duplication is therefore one string comparison against the
high-water mark, with no parsing and no per-ID bookkeeping. `LedgerSource`
preserves the property by synthesising `%010d` from the sequence. **A new
Source must preserve it too** — this is the single most important contract in
the codebase.

**3. The cursor advances only after every record in a page is emitted.** If a
sink fails mid-page the tailer returns with the cursor untouched, so a caller
that retries resumes from before the failure rather than past it.

**4. `Full` drives backfill speed.** A page that hit the server's limit means
more data is already waiting, so the tailer refetches immediately instead of
sleeping. Backfilling a day of history runs at network speed; a caught-up
stream polls once per ledger close.

### Cursors and the mutual-exclusion rule

The RPC rejects a request carrying both a ledger range and a cursor
(`"ledger ranges and cursor cannot both be set"`). So the first `Fetch` uses
`StartLedger` and every subsequent one uses the cursor alone. The fake
enforces the same rule, so a regression here fails in tests rather than only
in production.

### Gap detection

Records can be lost exactly one way: falling behind the RPC's retention window
(roughly 24 hours of events). The server reports `OldestLedger` on every
response, so the condition is directly observable:

```go
if cursorLedger < batch.Oldest { return &GapError{...} }
```

By default prism reports the missed range and exits 2. It does **not**
silently resume from the oldest available ledger — a stream with an
unannounced hole is worse than one that stops, especially for anything
auditing contract activity.

`--gap-strategy resume` opts into a different policy. The tailer calls
`Rewind(batch.Oldest)` on sources that implement `Rewinder`, clears the
cursor, and fetches again from the oldest retained ledger. The skipped range
is still announced: it reaches `Options.OnGap`, the CLI prints it on stderr,
and `Stats.Gaps` / `Stats.Recovered` count it. `--max-gap-recoveries` bounds
how many times a stream may skip ahead. A source without `Rewinder` cannot
resume and fails exactly as it would under the default policy.

### Backpressure

`Sink.Emit` is synchronous. A slow consumer (`prism … | jq | something-slow`)
blocks the write, which blocks the poll loop. That is correct backpressure
with no queue to grow. It is also why both sinks flush per record: a tail that
buffers looks like a tail that has hung.

`--batch` changes the unit of work, not the backpressure: `Batcher` buffers
records until `--batch-size` is reached or `--batch-timeout` elapses, then
hands the whole slice to the sink. It uses a single timer, not a goroutine,
and `Close` stops the timer and flushes the remainder, so Ctrl-C cannot leave
a buffered record behind. Sinks that implement `BatchSink` (both built-ins do)
flush once per batch; other sinks are driven with one `Emit` per record.

### Parallel historical backfill

`--backfill-workers N` (with an explicit `--from`) uses `Drainer` instead of
the sequential tailer for the historical phase. It probes the source once to
learn the retention bounds, splits the ledger range into N contiguous chunks,
and gives each worker its own `Source` and cursor. Workers stop at their chunk
boundary, so no ledger is fetched twice.

Records from the workers are merged by ID through a small heap before reaching
the sink. That preserves the same ordering and de-duplication guarantee as the
sequential tailer without buffering the whole backfill in memory: at most one
page per worker is live at a time. A full page is fetched ahead by a worker
while the merge consumes the previous one, so fetching is genuinely
concurrent. After the historical phase the CLI either exits (`--follow` is
off) or starts a normal tailer at `Until()+1`.

If `--from` predates the retention window, the probe handles the gap according
to `--gap-strategy`. A gap discovered inside a worker is handled the same way,
or returned as a `*GapError` under the default policy.

### Cancellation and goroutine ownership

`TaskGroup` in `context.go` is the one place that starts concurrent workers.
The first non-cancellation error cancels every sibling; `Wait` blocks until
all workers have returned and then cancels the shared context. A caller
cancellation is a clean stop, not an error. `Drainer.Run` waits for every
worker on every path, including sink failure and limit exhaustion, so a
terminated backfill cannot leak goroutines or leave a worker writing to a
closed sink.

### What is deliberately *not* handled

**Chain reorganisations.** Stellar ledgers are final on close — SCP gives
immediate finality, and there is no reorg. Code to handle rewritten history
would be untestable and would imply a hazard that does not exist. The
realistic adjacent failure is a load-balanced RPC pool serving from a lagging
backend, which reports an *older* `LatestLedger`; that case is tested.

## Multi-endpoint failover (`pkg/stream/pool.go`)

`--rpc-url` may be repeated (or `SOROBAN_RPC_URL` may be comma-separated).
With more than one URL, `internal/cli` builds a `stream.Pool` instead of a
single `rpc.Client`, so every command gains failover without knowing about it.

- Calls are distributed **round-robin** across healthy endpoints.
- A retryable failure marks the endpoint unhealthy and puts it in a cooldown
  (default 30s); the same call is retried on the next endpoint.
- Permanent errors (`rpc.Permanent`) stop failover immediately — retrying a
  malformed cursor on another backend cannot help.
- When **every** endpoint is cooling down, the one closest to recovery is
  still tried: a stale health mark is more likely than a total outage, and the
  call itself is the authoritative check.
- A background goroutine probes unhealthy endpoints (`GetLatestLedger`) and
  restores them as they recover. `Close` cancels the probe and waits, so a
  Pool never leaks a goroutine.

Options: `WithHealthInterval` (0 disables the probe), `WithHealthTimeout`,
`WithFailureCooldown`, `WithClientFactory` (tests inject fakes).

## Derived transaction streams

Soroban RPC has no dedicated endpoints for storage mutations or deployments.
Both streams therefore wrap `TransactionSource` and decode each transaction's
envelope / result metadata, so pagination, cursors and the tailer's delivery
guarantee apply unchanged.

### Contract storage changes (`prism storage`)

`StorageChangeSource` decodes `ResultMetaXDR` and emits one record per
contract-related ledger entry mutation: `contract_data`, `contract_code` or
`ttl`, with change type `created` / `updated` / `removed` / `restored`.
Filters (`--contract`, `--change`, `--entry`) are validated up front so a
typo fails immediately rather than streaming nothing.

### WASM uploads and deployments (`prism wasm`)

`WASMSource` decodes each envelope's `InvokeHostFunction` operations and
emits `upload` (sha256 of the WASM bytes) or `create` (contract ID derived
from the network passphrase). Failed transactions are skipped unless
`--include-failed` is set. Contract IDs use the same derivation as the
network, so `--network-passphrase` must match the chain being streamed
(defaults to testnet).

Both sources synthesise fixed-width zero-padded IDs (`%010d-%06d-…`) from
the ledger sequence, preserving the lexicographic-order contract the tailer
depends on.

## Rotating file sink (`--output-file` + `--rotate-*`)

Every streaming command accepts `--output-file`. When set, the chosen format
(NDJSON or human) is built on top of a `sink.RotatingWriter`:

| Flag | Meaning |
|---|---|
| `--rotate-size` | rotate before a write would exceed N bytes |
| `--rotate-interval` | rotate once the active file has been open this long |
| `--rotate-compress` | gzip each rotated file |
| `--rotate-keep` | keep at most N rotated files (0 keeps all) |

Rotated files are named `<path>.<UTC timestamp>` (plus `.gz` when
compressed). Rotation is synchronous; the active file is reopened before
pruning, so a failed prune cannot stop the stream. `--rotate-*` without
`--output-file` is rejected.

## Testing the guarantee

`internal/fakerpc` is a scriptable in-memory RPC. Tests describe failures
(`{2: {Err: ErrDropped}}`, `{1: {ReplayLast: true}}`) rather than assembling
responses by hand, and the fake serves from a fixed universe using real cursor
semantics, so a test cannot accidentally pass against behaviour the real
server would never produce.

Every scenario funnels through one helper asserting three invariants:

1. **IDs strictly increasing** — ordering survived.
2. **No duplicates** — de-duplication worked.
3. **Set equality with the expected range** — *this is what proves no gap.*
   Counting records would not.

Covered: clean streams, drops at every position, consecutive drops, replayed
pages, out-of-order pages, retention overrun with both gap policies, lagging
backends, permanent errors, retry exhaustion, cancellation mid-follow, sink
failure, full-page draining, concurrent backfill, batcher count/time/explicit
flushes, and task-group cancellation. Gap recovery is also unit-tested
without a network through the `Rewinder` seam.

Time is injected via `Clock`, so there are no real sleeps and no wall-clock
flakiness. Follow-loop tests carry a sleep budget that cancels the context, so
a regression that stops the tailer terminating **fails** rather than hanging
CI — a distinction worth the few extra lines.

### Mutation-checked

The invariants were verified by breaking the code on purpose:

| Mutation | Result |
|---|---|
| De-duplication disabled | `TestTailSuppressesReplayedPages` and `TestTailRejectsOutOfOrderPages` fail with exact diagnostics |
| Gap detection disabled | `TestTailDetectsRetentionGap` fails in 0.00s |

If you change the state machine, re-run that exercise. A test suite that
cannot fail is not protecting anything.

## Relationship to stellar-xdr-lens

prism does **no XDR decoding of its own**. Every payload goes through
[`lens`](https://github.com/stellar-optics/stellar-xdr-lens):

- `sink/human.go` renders payloads with lens's `TreeFormatter`, so an ScVal in
  a prism stream is identical to the same ScVal under `lens decode`. Only the
  header line is prism's.
- `sink/ndjson.go` embeds lens's `{type, value}` envelope, so `jq` expressions
  transfer between the two tools.
- `cli/tx.go` is a thin wrapper over `lens.ExplainPair`.
- Even topic filters are validated by decoding through `lens.DecodeAs`, so a
  malformed segment produces lens's error wording.

Shared CLI conventions: `--json`, `--color auto|always|never`, and the same
exit-code meanings.

## Colour control

`--color` accepts exactly `auto`, `always` and `never`, and an invalid value is
rejected before any command runs. `auto` colours only when the destination is a
terminal, `TERM` is not `dumb`, and `NO_COLOR` is unset, following
[no-color.org](https://no-color.org). `always` is an explicit operator request
and therefore overrides `NO_COLOR`; `never` always wins in the other direction.
The palette itself comes from lens, so prism and lens colour values identically.

## Contract ABI resolution

`prism abi <contract-id>` resolves a contract's spec without trusting a local
file. The flow is two `getLedgerEntries` calls:

1. The contract's persistent instance entry names its `ContractExecutable`.
   For user contracts that is a WASM hash; the Stellar asset contract has no
   embedded spec and is reported as such.
2. The contract-code entry for that hash carries the WASM bytecode.

The bytecode's `contractspecv0` custom section is a concatenated stream of
`ScSpecEntry` XDR values. `abi_resolve.go` walks the WASM section framing,
decodes each entry with the XDR stream decoder, and groups functions, structs,
unions, enums, error enums and events. Function parameters and return types are
rendered in Soroban type syntax, and `--function` plus repeated `--arg` decodes
actual base64 `ScVal` values through lens against the named parameters.

`getLedgerEntries` is an optional capability (`rpc.LedgerEntriesClient`), not
part of the streaming `rpc.Client` interface. That keeps the four-method core
small and avoids forcing stream-only fakes to implement a method they never
use. Only the `abi` command takes this path; every streaming command remains
independent of it.

## Completions and generated documentation

`prism completion <shell>` is pure command-tree traversal, so it makes no
network calls. Command constructors register fixed value completions for
enumerated flags (`--color`, `--type`, `--gap-strategy`); a new enum flag should
call `registerFlagCompletion` next to its flag definition.

`prism gendocs <man|markdown>` uses Cobra's document generators. It is attached
to the root command from package main so `internal/cli` does not depend on the
documentation packages. The generator disables Cobra's automatic timestamp tag
so rerunning it does not dirty version control.

## Adding a stream source

Implement `stream.Source`:

```go
type Source interface {
    Name() string
    Fetch(ctx context.Context, cursor string) (Batch, error)
    CursorLedger(cursor string) (uint32, bool)
}
```

Three rules, all load-bearing:

1. Records in a `Batch` are in ascending ID order.
2. `Record.ID` is fixed-width and zero-padded — see above.
3. `Fetch` is idempotent for a given cursor, because retries reuse it.

Return `Oldest` from whatever the server reports, or gap detection silently
becomes a no-op for your source. Then wire it into a command; the tailer and
every sink work unchanged. `ledgers.go` is about 100 lines and is the shortest
worked example.

## Adding an output sink

Implement `stream.Sink`:

```go
type Sink interface {
    Emit(r Record) error
    Flush() error
}
```

Flush per record if the sink is for live viewing. Decode XDR through `lens`
rather than reaching for `xdr` directly, so output stays consistent with the
rest of the family — and report an undecodable payload inline rather than
returning an error, so one malformed value cannot kill a long-running tail.

## Adding an RPC method

`pkg/rpc.Client` has four methods. Every one added must also be implemented in
`internal/fakerpc`, so prefer composing the existing calls. The interface is
small on purpose: it is the boundary that keeps the entire streaming core
testable without a network.
