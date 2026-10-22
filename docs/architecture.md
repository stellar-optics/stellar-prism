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

        ┌────────┐  OnPage: the position each delivered page reached
        │ Tailer ├───┐
        └────────┘   │  ┌──────────┐  append  ┌──────────────────┐
                      └─►│  Pruner  ├─────────►│ cursor journal   │  --state-file
                         └──────────┘  prune   └──────────────────┘
                         pruning.go            cursor_store.go
```

One goroutine, no channels. The tailer fetches a page, emits its records
synchronously, advances the cursor, and repeats. Concurrency would buy nothing
— the bottleneck is the network — and would cost determinism in tests. The
pruner is the one addition: it owns a single background goroutine, and the
command joins it before it exits.

## Package layout

```
cmd/prism/            main; maps errors to exit codes
  gendocs.go          man page and Markdown generator
internal/cli/         cobra commands — thin wrappers over pkg/stream
  color.go            --color parsing and NO_COLOR handling
  completion.go       bash/zsh/fish/powershell script generation
  abi_resolve.go      contract WASM lookup and embedded-spec parsing
  state.go            the cursor journal, the pruner, and the resume point
  runtime.go          the health server, the lease and the state key source
  syncwriter.go       serialised stderr, for the diagnostics several goroutines write
internal/fakerpc/     scriptable in-memory RPC for tests
pkg/rpc/              the Client interface + SDK adapter
  health.go           periodic getHealth and ledger latency evaluation
pkg/stream/
  record.go           Record: the neutral item every source produces
  source.go           Source and Sink interfaces, GapError   ← the seams
  tail.go             the state machine
  events.go           contract events source
  ledgers.go          ledger close source
  transactions.go     transaction source and opt-in classification
  tx_status.go        status/failure decoding and the filtered tx source
  traps.go            Soroban trap predicate and trap source
  feebump.go          fee-bump classification and fee-account filters
  dedup.go            bounded ring-buffer de-duplication for re-polls
  storage_changes.go  contract storage mutations (from tx meta)
  wasm_events.go      WASM uploads and contract deployments
  operations.go        operation-level transaction demux
  footprint.go         same-ledger Soroban footprint conflicts
  simulate.go          live transaction simulation estimates
  reorder.go            intra-ledger response reordering buffer
  pool.go             multi-endpoint failover RPC client
  drainer.go          concurrent historical backfill + ordered merge
  batcher.go          count/time batching sink wrapper
  gap_recovery.go     GapStrategy, Rewinder and gap handling
  pruning.go          CursorMetadata, MetadataStore, Pruner
  cursor_store.go     file-backed cursor metadata journal
  ha.go               lease-based leader election (active-passive)
  ha_lock_unix.go     flock(2) primitives for the lease
  health.go           the liveness/readiness probe server and its monitor
  crypto_state.go     AES-256-GCM encrypted cursor journal
  kms.go              AWS and GCP key management clients
  context.go          TaskGroup and cancellation helpers
  clock.go            Clock and Backoff, both injectable
  topic_pattern.go    client-side topic matching over decoded text
  demux.go            per-contract fan-out, queues + one goroutine each
  time_window.go      --since → approximate start ledger
  backoff.go          AdaptivePoller for --adaptive follow waits
pkg/sink/
  ndjson.go           NDJSON, one object per line
  human.go            terminal output, payloads via lens
  file_rotator.go     size/time rotating file sink, optional gzip
  dlq.go              dead letter queue for malformed or undeliverable records
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

### --since: relative windows, offline and approximate

`--since 10m` converts a relative window into a start ledger by dividing the
window by `RoughLedgerCloseInterval` (5s, in `time_window.go`) and rounding
**up**, so a window never starts *later* than asked. The conversion is pure
arithmetic on the server's reported head: one RPC call (`getLatestLedger`, the
same one a run with no `--from` makes anyway), no second call to measure the
real cadence. The ledger rate varies with network load, so the result is
approximate by design — streams needing an exact ledger use `--from`. Very
long windows clamp to ledger 1, where the server's own retention error takes
over.

### Client-side topic patterns

The server-side `--topic` filters are constrained to base64 ScVal segments
with `*`/`**` wildcards, because that is all the RPC understands. `--topic-pattern`
adds glob, prefix and regular-expression matching over the **decoded** topic
text, applied client-side in `topic_pattern.go` by a `PatternSink` that wraps
the output sink. The text it matches against is derived from the same lens
envelope `--json` prints, so a pattern tuned against `--json` output works
unchanged here — and filtering lives at the sink seam rather than inside the
tailer, so the delivery guarantee and cursor mechanics are untouched by
output shaping.

### Per-contract fan-out

`--fanout-dir` routes one stream to one NDJSON file per contract while the
terminal keeps the combined output. The routing lives in `stream.Demux`
(demux.go): an intake queue, a single dispatcher goroutine, and one queue plus
goroutine per subscribed contract. Records are routed by `Event.ContractID`;
each subscriber's queue holds a small buffer and overflows by counting drops
rather than wedging the other contracts. `Close` after the tailer stops
drains everything the caller accepted, closes each queue from the dispatcher
—the only closer, after its last send — flushes, and waits on every goroutine,
so a fanout run terminates with no leaked goroutines and a joined error.

### Re-poll de-duplication

`--dedup-window N` wraps the event sink in `stream.DedupSink`, which
suppresses a record whose fingerprint was already emitted within the last `N`
keys. The window is a fixed-size ring (`dedup.go`): the oldest key is evicted
when a new one arrives, so memory is bounded by count rather than by traffic
or elapsed time, and a hot contract cannot grow it without bound. A zero
window disables the wrapper entirely.

The fingerprint prefers the server's event ID, because a re-poll after a
reconnect replays the same ID while two genuinely distinct events can carry
byte-identical topics and values. Records without an ID fall back to a
content hash over the event locator and payload. De-duplication lives at the
sink seam, like pattern filtering, so the cursor still advances across a
suppressed record and the tailer's delivery guarantee is unchanged.

### Adaptive polling

`--adaptive` replaces the fixed `--poll` wait with an `AdaptivePoller`
(backoff.go) whose only signal is how many new ledgers closed during the last
wait: at least one means the wait matched or beat the ledger cadence, so
shorten; zero means mostly dead time, so lengthen. With no cadence parameter
at all that rule converges on oscillating around the real close rate, tracking
a network it is never told about — and the stretchy bounds (`--min-poll`-less
defaults 1s..30s) keep the invariant: never faster than Min, never slower
than Max. Jitter spreads each returned wait over its lower half so several
prism instances do not poll in phase. The tuner is one number in one
goroutine, so its tests run deterministically — no wall-clock flakiness.
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
worker on every path, including sink failure and limit exhaustion, soa terminated backfill cannot leak goroutines or leave a worker writing to a
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

The pool's probe is a liveness check: it asks an unhealthy endpoint for its
latest ledger and restores it when the call succeeds. It is not the latency
checker described below. The two share the word "health" because both answer
"is this endpoint usable?", but they run for different reasons and on different
schedules.

## RPC health checking (`pkg/rpc/health.go`)

`prism health` evaluates an endpoint with a single `getHealth` call. It is
useful as a CI gate, a cron probe, or a manual check when a stream looks stuck:

```console
$ prism health
2026-10-21T12:00:00Z  healthy    status=healthy  latest=500  oldest=490  ledger-latency=1.2s  probe-latency=4ms  retention=17280
```

A check fails for any of four observable reasons:

| Condition | Error | Meaning |
|---|---|---|
| `getHealth` returns an error | `rpc.ErrHealthCheckFailed` | the endpoint could not answer |
| `status` is not `"healthy"` | `rpc.ErrUnhealthyStatus` | the server says it is not serving normally |
| required fields are missing or inconsistent | `rpc.ErrInvalidHealthResponse` | the response cannot be evaluated |
| latest ledger close time is too old | `rpc.ErrStaleLedger` | the endpoint has fallen behind the chain |

The report carries the metrics the decision used:

| Metric | Source | Meaning |
|---|---|---|
| `LedgerLatency` | `now - latestLedgerCloseTime` | staleness of the newest ledger |
| `ProbeLatency` | duration of the `getHealth` call | RPC round-trip time |
| `LatestLedger`, `OldestLedger`, `LedgerRetentionWindow` | `getHealth` | the retention bounds a stream would inherit |
| `ConsecutiveFailures` | the checker | how many checks in a row have failed |

`getHealth` is exposed as the optional `rpc.HealthClient` interface rather than
as a method on `rpc.Client`. The streaming core never calls it, and widening
the streaming interface would force every existing `Client` implementation and
test double to grow a method it does not use. `SDKClient` and `stream.Pool`
both implement it, so `--rpc-url` may be repeated and a failed endpoint fails
over exactly like a streamed call.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--follow` / `-f` | off | keep checking instead of exiting after one probe |
| `--interval` | `30s` | delay between checks under `--follow` |
| `--health-timeout` | `5s` | bound on a single `getHealth` request |
| `--max-ledger-age` | `60s` | staleness limit for the latest ledger |
| `--fail-after` | `0` | with `--follow`, exit 1 after N consecutive failures (0 keeps watching) |
| `--json` | off | one JSON object per check, one per line |

One probe exits 0 when healthy and 1 when not. `--follow` resets the failure
count on a healthy result; `--fail-after` is what turns a watch into a gate.
Ctrl-C is a clean stop and exits 0, because an operator stopping a watch is not
an endpoint failure. The command validates every flag before opening a client,
so a bad interval never reaches the network.

### Goroutine ownership

The checker owns at most one goroutine. `Start` launches it and returns;
`Stop` cancels it and **waits** for it to return. Every `Stop` waits, not only
the first, so a second caller cannot report the checker stopped while a probe
is still in flight. `Run` is the `Start`/`Stop` pair for a blocking command,
and `defer Stop` handles the early-return paths. A one-shot `Check` starts no
goroutine at all.

The loop checks immediately, then on a ticker. It checks the context before
each probe, and suppresses the observer when cancellation races with a probe:
Ctrl-C must not print a spurious "unhealthy" line. The CLI joins the checker
before it returns, so an interrupted `prism health --follow` leaves nothing
running.

### Determinism and offline tests

`NewHealthChecker` takes an injected `HealthClient` and an injectable clock
(`WithHealthClock`); it never constructs a client or contacts an endpoint by
itself. Unit tests therefore run with no network and no real sleeps:

- `pkg/rpc/health_test.go` covers every classification branch, the latency
  arithmetic, failure reset, context cancellation, observer delivery, and both
  `Stop` invariants (join an in-flight probe, idempotent stop).
- `internal/cli/health_test.go` exercises the command end to end against
  `internal/fakerpc`: healthy JSON, human output, stale and unhealthy exits,
  `--follow --fail-after`, clean cancellation, flag validation before any RPC
  call, and a client without the optional capability.
- `fakerpc.WithHealthResponse`, `WithHealthError` and `WithHealthScript` stage
  real response sequences, including a final entry that repeats so a periodic
  test terminates without timing guesswork.

Mutation-checked, the same way as the streaming invariants:

| Mutation | Result |
|---|---|
| Staleness check disabled | `TestHealthCheckerClassifiesFailures/stale_ledger` fails: `Check() = healthy, want a failure` |
| `Stop` returns without joining the loop | `TestHealthCheckerStopWaitsForInFlightProbe` fails: `Stop() returned while a probe was still in flight` |
| Consecutive failures never reset | `TestHealthCheckerResetsFailuresAfterSuccess` fails: `recovery = healthy=true failures=2, want healthy and 0` |

### Compatibility

The feature is additive. `rpc.Client`, every existing command and every
existing flag are unchanged. New exported surface:

| Added | Where |
|---|---|
| `rpc.HealthClient` | `pkg/rpc/health.go` |
| `rpc.HealthChecker`, `rpc.NewHealthChecker`, `rpc.HealthReport`, `rpc.HealthOption` | `pkg/rpc/health.go` |
| `rpc.WithHealthInterval`, `WithHealthTimeout`, `WithMaxLedgerAge`, `WithHealthClock`, `WithHealthObserver` | `pkg/rpc/health.go` |
| `rpc.DefaultHealthCheckInterval`, `DefaultHealthCheckTimeout`, `DefaultMaxLedgerAge` | `pkg/rpc/health.go` |
| `rpc.ErrHealthUnsupported`, `ErrHealthCheckFailed`, `ErrUnhealthyStatus`, `ErrStaleLedger`, `ErrInvalidHealthResponse`, `ErrInvalidHealthConfig`, `ErrHealthCheckerStarted` | `pkg/rpc/health.go` |
| `SDKClient.GetHealth`, `Pool.GetHealth` | `pkg/rpc/client.go`, `pkg/stream/pool.go` |
| `--follow`, `--interval`, `--health-timeout`, `--max-ledger-age`, `--fail-after`, `--json` | `prism health` |
| `fakerpc.WithHealthResponse`, `WithHealthError`, `WithHealthScript`, `fakerpc.HealthBehaviour` | `internal/fakerpc` |

## Active-passive high availability (`pkg/stream/ha.go`)

A single prism instance is a single point of failure, and two instances
streaming the same contract emit the same records twice and fight over the
cursor file. `--ha-lease-file` makes a pair **active-passive**: the leader
streams, the standby waits and takes over.

Two mechanisms cooperate, because each covers a failure the other cannot.

- **An advisory lock on the lease file** (`flock(2)`) is held for the whole of
  a leader's term. The kernel drops it the moment the process dies, so a
  crashed leader is replaced at once — no TTL to wait out.
- **A lease record beside it** (`<path>.lease`) carries the holder, an
  expiry and an **epoch**. It covers the case the lock cannot: a leader that
  is alive but partitioned, and so neither renews nor exits. Its lease
  expires, a standby fences it by claiming a higher epoch, and the old leader
  notices on its next renewal that it has been replaced and stops.

The epoch is the fencing token. It increases every time the lease changes
hands, so a leader that was slow, paused or partitioned can tell that its
term has ended and step down instead of continuing to emit alongside the new
leader. The tailer compares the epoch on every poll: a change means the
pages queued so far describe work the previous leader did, so they are
dropped rather than committed, and the stream rewinds to the cursor the
previous leader committed.

```
  leader                              standby
  ──────                              ───────
  flock(lease)  ──► held
  write record {holder, epoch: 7, expires}
                    renew every --ha-heartbeat
                                         flock(lease)  ──► busy, lease live
                                         ...lease expires...
                                         write record {holder, epoch: 8}
  renew ──► epoch 8 > 7, fenced
  release the term                     flock(lease)  ──► held
                                       write record {holder, epoch: 9}
```

The lease file must be on a file system that supports advisory locking (a
shared NFS mount, for instance) so every instance sees the same lock. On a
platform without `flock`, `lockFile` returns an error rather than pretending
the lease works — the alternative would produce exactly the duplicate events
the lease exists to prevent.

A standby is alive but not ready: it is waiting for the lease, not serving.
The health monitor is told about the change through the election's `OnChange`
callback, so `/healthz` keeps answering 200 while `/readyz` answers 503.

## Liveness and readiness probes (`pkg/stream/health.go`)

`--health-port` serves the three endpoints a container orchestrator asks
for. The state behind them is a `HealthMonitor`, fed by the tailer:

| Endpoint | Answers |
|---|---|
| `/healthz` | 200 while the process is running and still making progress; 503 when it is stuck |
| `/readyz` | 200 when the RPC is answering, the stream has not gapped, and the lag is within `--max-lag`; 503 otherwise |
| `/metrics` | The same state in the Prometheus exposition format |

The tailer reports progress after every successful fetch through
`Options.OnProgress`, so the lag is the real distance between the stream's
position and the server's head rather than a guess. A failed fetch marks the
endpoint unreachable; a detected gap marks the stream gapped. Both make the
stream not ready, because neither is a state a consumer should be sent into.

Liveness is a staleness check: the monitor records when it was last updated,
and a process that has not reported for `StaleAfter` (30s by default) is
considered stuck. A standby is exempt — it is waiting for the lease, not
wedged.

## Graceful drain (`Tailer.Drain`)

On SIGTERM or Ctrl-C, prism stops scheduling new RPC calls and then gives
the sinks and the state store a bounded window to finish:

```sh
prism events --follow --drain-timeout 15s
```

`Tailer.Drain(ctx, timeout)` writes out the records still held in the reorder
buffer, flushes the sink and reports the pages that were delivered, which is
what commits the final cursor. The timeout bounds the whole operation, and a
drain that overruns it is reported rather than waited out: a process that is
about to be killed cannot be helped by a longer flush.

Two details make the drain safe rather than merely best-effort:

- **Only one flush runs at a time.** `flushOutput` admits one flush through a
  gate; a flush that overran its deadline is abandoned, and a second one
  waits for it rather than starting alongside it. A sink that buffers would
  otherwise write the same batch twice.
- **The cursor is committed only for delivered pages.** A page that was
  partly emitted is not reported, so a drain that fails half-way leaves the
  cursor where it was and the next run re-emits the records that were not
  delivered.

The CLI runs the drain inside its own deadline and joins any error with the
run's own, so a drain failure is reported rather than swallowed.

## Encrypted state files (`pkg/stream/crypto_state.go`)

The cursor journal holds stream metadata, and on a shared file system or in a
cloud bucket that is worth encrypting. `--state-passphrase-env` (or
`--state-passphrase-file`, or a key management service) makes prism write an
encrypted journal instead.

The file is a header line followed by one envelope per entry:

```
{"version":1,"alg":"aes-256-gcm","key":{"kdf":"pbkdf2-hmac-sha256", ...}}
{"version":1,"nonce":"...","ciphertext":"..."}
```

Each entry is sealed with authenticated **AES-256-GCM** under a fresh random
nonce, with a fixed label as additional authenticated data so a ciphertext
cannot be moved from one file into another. A file that was tampered with,
truncated or written with a different key fails the authentication tag and is
rejected with a clear error rather than being read as if it were intact.

The key comes from exactly one place, decided by the flags and validated
before anything is fetched. A passphrase is stretched into a key with
PBKDF2-HMAC-SHA256 over a random salt, and the salt and iteration count are
recorded in the header so the file stays readable by a future version; a key
management service generates the data key and keeps the wrapped form, so the
plaintext key never reaches the disk.

| Source | Flag |
|---|---|
| Environment variable | `--state-passphrase-env NAME` |
| File | `--state-passphrase-file PATH` |
| AWS KMS | `--state-kms-provider aws --state-kms-key-id ARN` |
| GCP KMS | `--state-kms-provider gcp --state-kms-key-id NAME` |

Passing more than one source is an error rather than a silently ignored flag:
an operator who believes the file is encrypted with a passphrase must never
end up with a file encrypted with something else.

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

### Operation streams (`prism operations`)

`OperationSource` decodes each transaction envelope and emits one record per
operation with the deterministic ID `%010d-%04d-%02d`. It carries the operation
type, effective source account, operation-body XDR and matching operation
result, and supports type and source-account filters.

### Footprint conflicts (`--detect-conflicts`)

The tailer's reorder buffer closes a ledger before `FootprintAnalyzer` examines
its Soroban transaction footprints. A key is reported when transactions share
it and at least one access is read-write. The same conflict is attached to
each participating transaction, allowing both human warnings and structured
NDJSON consumers to identify hot keys and transaction sets.

### Simulation streams (`prism simulate`)

`SimulationSource` polls the latest ledger, calls `simulateTransaction` once
for each newly observed head, decodes Soroban resource estimates, and compares
successive return values through stellar-xdr-lens before emitting changed
fields.

### Transaction classification (`prism transactions`)

`TransactionSource` accepts `TransactionQuery.Classify`, which fills four
derived fields on each record by decoding the envelope and result through
lens: `Status` (`success` or `failed`), `FeeAccount`, `FeeBump` and
`Failure`. Classification is opt-in because it costs two XDR decodes per
record; the filtered streams enable it automatically.

`Failure` keeps every level at which the protocol reports a reason, because
the most specific level is not always the one a consumer wants:

- `Detail` is the transaction-level code (`tx_failed`, `tx_bad_seq`, …).
- `Inner` is the inner transaction's code for a fee-bump wrapper, which is
  almost always the real cause.
- `Operation` is the first failing operation, with its own decoded code. A
  Soroban host function that trapped or panicked appears here as
  `invoke_host_function_trapped`, and `Failure.Trapped()` reports it.

`TransactionFilterSource` wraps a classified `TransactionSource` and applies
a status set plus zero or more `TransactionFilter` predicates after each page
is fetched. Filtering therefore advances the cursor across suppressed
records: `--status failed`, `--trapped`, `--fee-bump` and `--fee-account` are
views of one stream, not separate streams that could skip history. `traps.go`
exposes `TrappedFilter`/`NewTrapSource` for contract executions that trapped,
which is distinct from a malformed invocation or an exceeded resource limit.
`feebump.go` exposes `FeeBumpFilter`, `FeeAccountFilter` (which normalises a
muxed address to its underlying account) and the `FeeBump` classification,
which keeps the wrapper's fee payer, inner source, inner hash, fee bids and
signature counts separate from the inner transaction.

`prism transactions` always classifies, so the JSON line carries `status`,
`feeAccount`, and `failure`/`feeBump` objects when they apply. The human sink
names the failed operation and prints "contract execution trapped or
panicked" for a trap.

## Cursor metadata and background pruning (`pkg/stream/pruning.go`)

A long-running stream produces one checkpoint per page, and a page per ledger
close. Left alone that journal grows without bound: a week of following a
contract is hundreds of thousands of lines of metadata nobody asked for.

`--state-file` makes the cursor durable. After every page that was emitted
**in full**, prism records the position it reached, and a background pruner
keeps the file from growing:

| Flag | Meaning |
|---|---|
| `--state-file <path>` | the journal: one JSON checkpoint per line |
| `--resume` | start this run from the last recorded position |
| `--prune-interval` | how often the background pruner compacts the file |
| `--prune-keep` | how many checkpoints to keep per stream |
| `--prune-max-age` | how old a checkpoint may be before it is dropped |

The pieces:

| Type | Role |
|---|---|
| `stream.CursorMetadata` | one checkpoint: key, cursor, ledger, timestamp |
| `stream.MetadataStore` | the seam: append, latest, prune |
| `stream.FileMetadataStore` | the file-backed journal |
| `stream.Pruner` | the background compactor |
| `stream.Options.OnPage` | how the tailer reports a delivered page |
| `stream.Options.StartCursor` | how a tailer resumes |
| `stream.DrainerConfig.OnPage` | how a concurrent backfill reports a delivered page |

**The checkpoint is written after the emit, not before.** `Options.OnPage`
fires once a page has been delivered in full. Pages held in the ledger reorder
buffer delay their checkpoints, including subsequent empty filtered pages;
`Tailer.Cursor()` returns the delivered position, not the ahead-of-output fetch
cursor. A bounded follow drains its buffered suffix when the available pages
are exhausted and enough records are available to satisfy `--limit`, without
waiting for another ledger. A page cut short by `--limit`,
or one the sink rejected, produces no checkpoint, so a recorded position can
never point past records that were not delivered. The callback also skips a
page that returned nothing and left the cursor where it was: that says
nothing new, and reporting it would fill the journal with duplicates.

**A concurrent backfill reports its position too.** `--backfill-workers`
drains a historical range with several workers before the follow phase
begins, so it is the phase that produces the most pages. `DrainerConfig.OnPage`
carries the same `stream.Page` the tailer uses: the workers cover disjoint
ledger ranges and their pages are merged by record ID, so the pages are
delivered in ascending order and each reported position is further along than
the last. A page the `--limit` cut short never reaches the callback.

**The pruner owns one goroutine, and joins it.** `Pruner.Start` begins the
loop; `Pruner.Close` cancels it and waits for it to return. The CLI closes
the pruner before the command exits — including on Ctrl-C — so an
interrupted stream leaves nothing running behind it. A failed pass is
reported on stderr and the loop continues: a file that cannot be compacted
right now is no reason to stop streaming records.

**Every `Close` waits, not only the first one.** `Pruner.Close` is safe to
call more than once, and each caller waits for the goroutine to be joined. A
second `Close` that returned early would report the pruner as stopped when
it is not, and a caller that trusted that report could exit with the
goroutine still alive.

**Diagnostics are serialised.** The pruner reports what it compacted on
stderr, and so does the stream — from a different goroutine. `runStream`
wraps stderr in a mutex-guarded writer, so those lines cannot interleave
mid-sentence and a test's stderr buffer cannot race. `fmt.Fprintf` formats
into its own buffer and hands the result over in a single `Write`, so one
lock acquisition covers one whole line.

**The final compaction happens even on Ctrl-C.** When the command returns, the
pruner is closed and one last pass runs against a context detached from the
command's, so an interrupted run still leaves a compacted journal behind. A
failure there is reported as the command's error rather than swallowed.

**Rewrites are atomic.** The pruner writes the compacted journal to a
temporary file in the same directory and renames it over the old one. A
reader sees either the whole old file or the whole new one, and a crash
mid-rewrite leaves the previous file intact. The file's permissions are
carried over.

**The prune pass streams.** It counts the entries per key in one pass and
rewrites in a second, keeping only the newest `--prune-keep` per key in
memory. The journal may be large; the pruner's memory footprint is not a
function of its size.

**A resumed stream is still gap-checked.** A recorded cursor is a real
position, and the server may have pruned past it. Resuming from a stale
checkpoint reports the missed range and exits 2, exactly like any other gap.

**Resume is opt-in, and it is not exactly-once.** It narrows the restart
window from "the whole run" to "one record"; a process killed between the
emit and the write still re-emits. The README keeps saying so.

**Migration notes.** Nothing existing changed: no CLI flag was renamed or
removed, and no exported type lost a field or changed a signature. The
feature adds to the public surface:

| Added | Where |
|---|---|
| `stream.CursorMetadata`, `stream.MetadataStore`, `stream.PrunePolicy`, `stream.PruneStats`, `stream.PruneOptions`, `stream.PrunerStats` | `pkg/stream/pruning.go` |
| `stream.MetadataStore`, `stream.PrunePolicy`, `stream.PruneStats`, `stream.FileMetadataStore` | `pkg/stream/cursor_store.go` |
| `stream.Page` | `pkg/stream/tail.go` |
| `stream.Options.OnPage`, `stream.Options.StartCursor` | `pkg/stream/tail.go` |
| `stream.DrainerConfig.OnPage` | `pkg/stream/drainer.go` |
| `--state-file`, `--resume`, `--prune-interval`, `--prune-keep`, `--prune-max-age` | every streaming command |
| `sink.DLQ`, `sink.DLQEntry`, `sink.DLQOptions`, `sink.DLQStats`, `sink.NewDLQ`, `sink.ValidateRecord` | `pkg/sink/dlq.go` |
| `--dlq-file` | every streaming command |

`stream.Pruner` and `stream.FileMetadataStore` are new types rather than
changes to existing ones, so a caller that ignores them compiles and behaves
exactly as before. The one behavioural change visible to an existing caller
is that a streaming command now reads `--state-file` before its first fetch;
with no `--state-file` nothing is opened and nothing is written.

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

## Dead letter queue (`--dlq-file`)

A long-running tail must not die because one record is malformed or one write
fails. `--dlq-file` wraps the chosen sink in `sink.DLQ`, which records the
failure and keeps the stream running:

```
                      ┌──────────────┐
   Tailer ── Record ─►│ sink.DLQ     ├──► chosen sink ──► stdout / --output-file
                      │              │
                      │   failure    ├──► --dlq-file (NDJSON, append)
                      └──────────────┘
```

The wrapper implements both `stream.Sink` and `stream.BatchSink`, so every
streaming command gains it without knowing it exists.

| Field | Meaning |
|---|---|
| `kind` | always `"dlq"`, so a queue file is distinguishable from stream NDJSON |
| `stage` | `validation`, `delivery` or `flush` |
| `error` | the failure that prevented delivery |
| `failedAt` | UTC timestamp, injectable through `DLQOptions.Now` |
| `record` | the original record, preserved field-for-field; absent for `flush` |

Two classes of failure are captured:

- **Validation.** `sink.ValidateRecord` checks the structural contract every
  sink relies on: a non-empty ID, a known `Kind`, and a payload pointer that
  matches that kind. It deliberately does *not* reject XDR that lens cannot
  decode — the built-in sinks already report that inline, and treating a
  display problem as a delivery failure would be wrong.
- **Delivery.** If the wrapped sink's `Emit` returns an error, the record is
  written to the queue and the error is swallowed. Only a failure to write the
  dead letter itself is returned, because at that point the record would vanish
  with no trace. A primary `Flush` failure is captured as a `flush` entry for
  the same reason: the per-record failures are already accounted for, and
  failing the command at shutdown would defeat the queue's purpose.

A captured record counts as delivered, so the tailer advances its cursor past
it. The queue is the durable copy: replay its `record` field through a sink if
the primary output must be complete. This is deliberate — blocking the cursor
on a record the primary can never write would stall the stream forever — and
it is why the queue is append-only and flushed per entry.

`EmitBatch` validates and delivers one record at a time. A batch sink can fail
part-way through a batch, after some records were written, so forwarding the
whole batch again would duplicate them and forwarding nothing would lose them.
Per-record isolation is what makes every failure attributable; the trade-off
is that a DLQ-wrapped run does not amortise the wrapped sink's flushes. The
batcher still controls when the tailer hands records over.

The DLQ starts no goroutines. `Close` flushes the queue, closes the wrapped
sink when it implements `io.Closer`, and closes the queue writer (when
`CloseWriter` is set) — so `Ctrl-C`, a failed run and a normal exit all leave
nothing open behind. It is idempotent, and each `Close` performs the flush it
needs rather than returning early.

`--dlq-file` is append-only and must differ from `--output-file`. Without
`--dlq-file` the sink chain is unchanged: nothing is opened and no wrapper is
inserted.

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

The cursor metadata store is tested the same way: appends and rewrites
against a real file in a temporary directory, a pruner that keeps running
after a failure, a pruner whose goroutine is joined rather than abandoned,
and a tailer that resumes from a recorded cursor and is still gap-checked
when it does. `internal/cli` runs the whole thing end to end against
`internal/fakerpc`: a run that records checkpoints, a file that stays
bounded while the stream runs, and a second run that picks up where the
first stopped.

The same guarantees also hold through the seams this file describes below:
adaptive poll waits, `--since` window arithmetic, client-side topic
patterns, and demux routing under concurrent submitters — all checked with
`go test -race`.

The dead letter queue is covered at both levels. `pkg/sink` tests every
validation branch, delivery and flush failures (including a writer that
fails like a full disk), batch attribution, and idempotent close against a
real file. `internal/cli` runs `runStream` with a source that emits one valid
record and one malformed record, then asserts that the valid record reached
stdout, the malformed one reached `--dlq-file`, and the command exited
successfully — the end-to-end shape of "a bad record does not crash a tail".

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
| Checkpoint written before the page was emitted | `TestTailDoesNotReportAPartiallyEmittedPage` and `TestTailDoesNotReportAPageTheSinkRejected` fail |
| Pruner goroutine abandoned instead of joined | `TestPrunerCloseJoinsItsGoroutine` fails: `Close()` returned while the pruner was still running |
| Second `Close` returning without joining the goroutine | `TestPrunerConcurrentCloseWaitsForTheGoroutine` fails: `Close() returned (<nil>) while the pruner was still running` |
| Diagnostics written to stderr without serialisation | `TestSyncWriterSerialisesConcurrentWrites` fails with a `DATA RACE` report from the race detector |
| Prune keeping the oldest entries | `TestFileMetadataStorePruneKeepsTheNewestPerKey` fails |
| Final compaction at shutdown removed | `TestEventsCommandPrunesTheStateFile` fails: 10 checkpoints left instead of 2 |
| DLQ swallowing a failure without recording it | `TestDLQCapturesDeliveryFailure` fails: no dead letter on disk |
| DLQ returning a captured delivery failure | `TestDLQCapturesDeliveryFailure` fails: `Emit()` returned the error |
| DLQ closing without flushing | `TestDLQClosesWriterAndPrimaryIdempotently` fails: the queue file is empty |

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
part of the streaming `rpc.Client` interface. That keeps the core interface
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

### Built-in sinks

Every streaming command accepts the same optional output flags. They are off
unless set, so the default path still touches nothing but RPC and stdout. When
one or more are enabled, `pkg/sink.MultiSink` delivers each record to the
normal output and every extra sink. It records acknowledgements per sink, so a
retry does not repeat a delivery that already succeeded; any unresolved
failure ends the stream rather than silently dropping records. `Close` flushes buffered
batches, stops timers and closes listeners and sockets, so a finished stream
leaves nothing running.

All extra sinks reuse `sink.MarshalRecord`, so the JSON object is byte-for-byte
what `--json` writes to stdout. With `--raw` the base64 XDR fields are kept
instead of decoded values.

#### Webhook (`--webhook-url`, `pkg/sink/webhook.go`)

Batches records into one request body:

```json
{"delivery":"1700000123456789-1","sentAt":"2023-11-14T22:15:23Z",
 "records":[{"kind":"event","id":"..."}]}
```

Headers: `Content-Type: application/json`, `X-Prism-Delivery` (the idempotency
key), `X-Prism-Timestamp` (Unix seconds) and, when a secret is configured,
`X-Prism-Signature: sha256=<hex>`. The signature is
`HMAC-SHA256(secret, "<timestamp>.<body>")`; receivers must verify it before
trusting a payload. The secret comes from `--webhook-secret` or
`PRISM_WEBHOOK_SECRET`.

Batching flushes at `--webhook-batch-size` records or `--webhook-timeout`
after the first buffered record, and on exit. Failures retry with exponential
backoff from 100ms to 5s, ±20% jitter, honouring `Retry-After`; a 4xx other
than 408/425/429 is treated as permanent. A failed batch stays buffered, so a
later flush or close retries it: delivery is at-least-once, not exactly-once.

#### Object storage (`--cloud-*`, `pkg/sink/cloud_storage.go`)

Records are accumulated in memory and uploaded as one object when the chunk
reaches `--cloud-batch-records`, `--cloud-batch-bytes`, or
`--cloud-batch-timeout`. The key is derived only from the records:

```
<prefix>/<YYYY>/<MM>/<DD>/ledger-<start>-<end>/<firstID>-<lastID>.ndjson[.gz]
```

A retry therefore overwrites the same object instead of duplicating a chunk,
and a backfill lands in the partition of the ledger's close date rather than
the upload date. `--cloud-compress` (default on) gzips the body and sets
`application/gzip`; otherwise it is `application/x-ndjson`. One record larger
than the byte cap is uploaded alone rather than split.

The `ObjectStore` interface is the seam, so chunking is tested with an
in-memory store and no network:

- **S3** (`pkg/sink/s3.go`): SigV4-signed `PUT` using
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optional
  `AWS_SESSION_TOKEN`. Path-style addressing by default, `--cloud-region`
  defaults to `us-east-1`. The signer is checked against AWS's published PUT
  example in the unit tests.
- **GCS** (`pkg/sink/gcs.go`): the JSON API's simple media upload (`POST
  /upload/storage/v1/b/<bucket>/o?uploadType=media&name=<key>`) with a bearer
  token from `GOOGLE_OAUTH_ACCESS_TOKEN`.

Uploads retry like the webhook and keep the chunk buffered on failure.

#### Prometheus (`--metrics-addr`, `pkg/sink/prometheus.go`)

Serves `/metrics` on the configured address with:

| Metric | Type | Meaning |
|---|---|---|
| `prism_records_total{kind}` | counter | records emitted |
| `prism_events_total{type}` | counter | Soroban events by type |
| `prism_errors_total{stage}` | counter | `rpc` retries, `gap` recoveries, `sink` failures |
| `prism_ledger_latency_seconds` | histogram | ledger close to emission, default buckets |
| `prism_ledger_sequence` | gauge | highest ledger emitted |

The exposition text is rendered directly rather than through the Prometheus
client, keeping the CLI dependency-free. The server binds before the stream
starts; `:0` chooses a port, which the CLI prints on stderr. Shutdown waits for
in-flight scrapes and leaves no goroutine behind.

#### Syslog and journald (`--syslog-address`, `pkg/sink/syslog.go`)

`--syslog-format rfc5424` writes one newline-terminated message per record:

```
<134>1 2023-11-14T22:15:23.000Z host prism - event [prism@32473 kind="event" ledger="100" id="..."] {"kind":"event",...}
```

Severity follows the record: failed transactions and deployments are warnings
(4), diagnostic events are debug (7), everything else is informational (6).
Structured-data values escape `\`, `"` and `]` per RFC 5424.

`--syslog-format journald` writes the journald native protocol instead: a
`NAME=value` block with `PRIORITY`, `SYSLOG_IDENTIFIER`, `SYSLOG_FACILITY`,
`PRISM_KIND`, `PRISM_LEDGER`, `PRISM_RECORD_ID`, `PRISM_LEDGER_TIME` and
`MESSAGE`, terminated by a blank line. Use `unixgram:///run/systemd/journal/socket`
as the address. `DialSyslog` also accepts `unix://`, `udp://`, `tcp://` and a
bare socket path; `--syslog-facility` accepts any POSIX facility name and
defaults to `local0`.

#### Elasticsearch/OpenSearch (`--sink-es-url`, `pkg/sink/elasticsearch.go`)

Records are written through the bulk API in NDJSON pairs: an `index` action
followed by the same JSON document emitted by `--json`. The record ID is the
Elasticsearch document ID, so retrying a buffered batch overwrites the same
documents rather than creating duplicates. `--sink-es-index` is the base name;
the actual index is `<base>-YYYY.MM.DD`, derived from each record's UTC close
time. `--sink-es-batch-size` and `--sink-es-flush-interval` bound the in-memory
batch. Authenticate with `--sink-es-api-key` (or `PRISM_ES_API_KEY`) or basic
credentials.

#### NATS and JetStream (`--sink-nats`, `pkg/sink/nats.go`)

Each record is published as the regular `--json` document to a subject rendered
from `--nats-subject`; the template may use `.ContractID`, `.Topic0`, `.ID` and
`.Ledger`. The default is `stellar.events.{{.ContractID}}`. A rendered subject
must contain nonempty publish tokens. Core NATS flushes after each publish;
`--nats-jetstream` waits for a JetStream publish acknowledgement.

#### ZeroMQ PUB (`--sink-zmq`, `pkg/sink/zeromq.go`)

The ZeroMQ sink connects a PUB socket to `--sink-zmq` and sends each record as
a multipart `[topic, JSON]` message. `--zmq-topic` uses the same template data
as `--nats-subject`, which gives subscribers an efficient prefix-filterable
first frame. PUB delivery is intentionally best-effort: a slow or disconnected
subscriber can miss messages under ZeroMQ's high-water-mark semantics. Use
NATS JetStream when publisher acknowledgements are required.

#### Audit trail (`--sink-audit`, `pkg/sink/audit_log.go`)

The audit sink writes a trail that shows whether it was changed after the fact.
Every record is hashed together with the hash of the record before it:

```
Hash_n = SHA256(Hash_{n-1} + Record_n)
```

The chain starts at a genesis hash computed over the header, so the header
cannot be swapped for another one without breaking every record that follows.
Each record is written as one JSON line holding the sequence number, the
ledger, the record id and kind, the previous hash, the new hash and the record
itself — the same document `--json` writes, so an auditor reads the trail
without a second decoder.

Editing, deleting or inserting a line changes every hash after it, and the
verifier names the line of each anomaly. On top of the chain, a checkpoint is
written every `--sink-audit-checkpoint` records (100 by default, 0 writes one
after every record) and signed with an Ed25519 key:

```
Signature = Ed25519(SigningKey, Hash_n)
```

The signature covers the chain hash at that point, so a trail that was edited
and re-hashed from the beginning still fails to verify against the checkpoints
written before the edit. The signing key is the PEM private key at
`--sink-audit-key`, or `<audit file>.key` when the flag is left out; it is
created exclusively with mode `0600` the first time it is needed, so two
processes cannot each write a key and leave the trail signed by one of them.

The trail is append-only across runs: a new sink reads the header, checks it
against the signing key and continues the chain from the last entry. A log
signed with a different key is refused rather than appended to. A final line
left half-written by an interrupted run is truncated before the chain
continues, so an audit log is never left with a dangling entry.

`prism audit-verify` (`internal/cli/audit.go`) recomputes the chain, checks
every signed checkpoint against the chain it covers and against the public key
in the header, and reports each anomaly with its line:

| Anomaly | Meaning |
|---|---|
| `modified` | the record hash does not match the record |
| `deleted` | the sequence numbers skip ahead |
| `inserted` | the sequence numbers go back, or the log holds a second header |
| `chain_break` | the record does not link to the entry before it |
| `checkpoint_mismatch` | the checkpoint does not match the chain it covers |
| `bad_signature` | the checkpoint signature does not verify |
| `key_mismatch` | the trail is signed with another key than the one expected |
| `header_mismatch` | the header was changed or is not supported |

The command exits 0 when the trail is intact and 1 when it was tampered with.
`--public-key` pins the key the trail must be signed with, and
`--print-public-key` prints the key the trail carries, so it can be pinned
somewhere the trail cannot reach.

#### Configuration rules

Secrets are read from the environment wherever possible, because command-line
flags are visible in process listings. `--webhook-secret` exists for trusted
shells; the cloud adapters only read credentials from the environment.

`--batch` composes with these sinks: the batcher wraps the fanout, so stdout
and every destination receive whole batches. The webhook and cloud sinks also
cap their own in-memory batches, so a large stream cannot grow memory without
bound even when `--batch` is not used.

## Adding an RPC method

`pkg/rpc.Client` has a small set of methods. Every one added must also be implemented in
`internal/fakerpc`, so prefer composing the existing calls. The interface is
small on purpose: it is the boundary that keeps the entire streaming core
testable without a network.

A method used by only one command belongs in its own optional interface, as
`LedgerEntriesClient` and `HealthClient` do. That keeps stream-only fakes small
and makes a missing capability an explicit, testable error rather than a
compile-time burden on every implementation.
