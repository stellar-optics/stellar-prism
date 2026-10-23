# Roadmap

An honest account of where this is going. Dates are intentions, not commitments.

## Where things stand (v0.1)

Working and tested:

- `prism events` — contract, topic and type filters; one-shot or `--follow`
- `prism transactions` — `getTransactions` with SEP-35 cursor continuity,
  gap detection and classification; `--status`, `--trapped`, `--fee-bump`,
  `--fee-account` and `--memo` are views of the same stream
- `prism ledgers` — ledger close metadata
- `prism tx` — fetch and explain, through the lens explain path
- `prism abi` — resolve a contract's WASM via RPC and render its embedded spec
- `prism completion` and `prism gendocs` — shell completion and reference docs
- `--color auto|always|never` honoring `NO_COLOR`
- NDJSON on everything, flushed per record or per `--batch`
- Optional webhook, S3/GCS, Prometheus and syslog/journald sinks, composed
  with stdout through a fanout
- `--from` backfill, draining full pages at network speed, optionally with
  `--backfill-workers`
- `--since 10m` — relative windows converted to approximate start ledgers
- Reconnection with jittered backoff; retry never advances the cursor
- `--adaptive` — poll waits tuned from observed ledger close frequency
- Client-side `--topic-pattern` — glob, prefix and regex over decoded topics
- `--fanout-dir` — one NDJSON file per contract via the streaming demux
- Gap detection against the server's retention window: exit 2 by default, or
  `--gap-strategy resume` with the skipped range reported
- Optional cursor journal (`--state-file`) with background pruning, and
  `--resume` to continue a stream from the last recorded position
- `--stats` — a run summary on exit: records processed, duplicates dropped,
  retries and elapsed time
- `--heartbeat 30s` — a line on stderr after every idle interval during
  `--follow`, so a quiet contract is distinguishable from a hung process
- Streaming core fully tested with no network, and mutation-checked
- An opt-in end-to-end suite that runs prism against a local Quickstart
  network (`make test-e2e`), including clean shutdown with no leaked goroutines

Known limits, stated plainly:

- **Event payloads still render as the XDR wire shape.** `prism abi` can
  resolve and display the spec, including decoding standalone `--arg` values,
  but the live event stream does not yet apply struct/field names
  automatically. See below.
- **No exactly-once across restarts.** `--state-file` and `--resume` narrow
  the restart window to a single record, but a process killed between
  emitting and recording still re-emits on restart.
- **Polling, not push.** Soroban RPC has no subscription API, so latency is
  bounded by `--poll` (one ledger close by default).
- **~24-hour history.** RPC retention bounds how far `--from` can reach.
- **The library API is not frozen.** Expect breaking changes before v1.0.

## Near term

### Durable cursors

Landed as `--state-file` and `--resume`: prism records the position after
every page it delivered in full, and the next run can start from it. The
journal is compacted by a background pruner, so it does not grow without
bound however long the stream runs.

What is still missing is true exactly-once. That needs a transaction spanning
the sink and the cursor store, which a CLI writing to stdout cannot have, so
the README keeps saying so.

Two smaller things could follow: recording the position per *ledger* rather
than per page, and letting several streams share one state file without
keying it by filter set.

Until it exists, `--rewind` is the manual escape hatch: it steps a run back
so recent records are reprocessed, by ledger count or by duration.

### Contract-spec-aware decoding

The one place `stellar events` is clearly better. Given a contract's
`ScSpecEntry` definitions, render events with real field and variant names
instead of the wire shape.

Needs a way to obtain the spec (fetch the WASM via `getLedgerEntries` and
cache it, or accept a local file) and belongs in `lens` rather than `prism`,
since `lens decode` would benefit identically. Probably the highest-value item
here, and the largest.

### Better `--follow` ergonomics

- a heartbeat line on stderr when a follow is idle, so a quiet contract is
  distinguishable from a hung process (the `--since` and `--stats` halves of
  this list have already shipped — see them in the working list above)

## Later, and less certain

### Horizon as a second backend

Horizon offers SSE streaming for classic operations, which would genuinely be
push rather than polling. It is a different API shape and a different data
model, so it would be a second `Source` family rather than a drop-in.

## Explicitly not planned

- **Indexing.** prism can write records to PostgreSQL/TimescaleDB or Parquet,
  but it does not index, query or serve them. Mercury and Zephyr remain the
  hosted indexers; prism's sinks are for the store you already run.
- **Transaction submission or signing.** prism reads. It never handles secret
  keys, and adding that would make an inspection tool security-sensitive.
- **Reimplementing XDR decoding.** That is
  [stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens)'s job,
  and duplicating it would let the two tools disagree about what a value means.
- **Reorg handling.** Stellar ledgers are final on close. Code for a hazard
  that cannot occur is code that cannot be tested.
- **Competing with `stellar events` on one-shot queries.** Where the official
  CLI does the job better, prism should point at it.

## Contributing to the roadmap

Adding a source or a sink is designed to be a one-file change — see
[architecture.md](architecture.md). If you want something here sooner, open an
issue describing what you were trying to debug and where prism fell short;
concrete cases are far more useful than feature requests.
