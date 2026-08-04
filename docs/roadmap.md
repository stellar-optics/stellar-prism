# Roadmap

An honest account of where this is going. Dates are intentions, not commitments.

## Where things stand (v0.1)

Working and tested:

- `prism events` — contract, topic and type filters; one-shot or `--follow`
- `prism ledgers` — ledger close metadata
- `prism tx` — fetch and explain, through the lens explain path
- NDJSON on everything, flushed per record
- `--from` backfill, draining full pages at network speed
- Reconnection with jittered backoff; retry never advances the cursor
- Gap detection against the server's retention window, exiting 2
- Streaming core fully tested with no network, and mutation-checked

Known limits, stated plainly:

- **No contract-spec decoding.** Payloads render as the XDR wire shape.
  `stellar events` decodes against the contract's spec and is better for
  reading values today. See below.
- **No durable cursor.** Exactly-once across restarts is not offered; a
  process killed between emitting and advancing will re-emit on restart.
- **Polling, not push.** Soroban RPC has no subscription API, so latency is
  bounded by `--poll` (one ledger close by default).
- **~24-hour history.** RPC retention bounds how far `--from` can reach.
- **The library API is not frozen.** Expect breaking changes before v1.0.

## Near term

### Durable cursors

`--state-file` persisting the cursor after each successful emit, so a restart
resumes where it left off. This narrows the restart window from "the whole run"
to "one record" and makes prism usable as a long-lived ingestion process.

It cannot reach true exactly-once — that needs a transaction spanning the sink
and the cursor store, which a CLI writing to stdout cannot have — and the
README should keep saying so.

### Contract-spec-aware decoding

The one place `stellar events` is clearly better. Given a contract's
`ScSpecEntry` definitions, render events with real field and variant names
instead of the wire shape.

Needs a way to obtain the spec (fetch the WASM via `getLedgerEntries` and
cache it, or accept a local file) and belongs in `lens` rather than `prism`,
since `lens decode` would benefit identically. Probably the highest-value item
here, and the largest.

### Transaction and operation streams

`prism txs --follow` over `getTransactions`, which is already a paginated
cursor API of the same shape. Mostly a new `Source`; the state machine and
sinks are unchanged. A good first substantial contribution.

### Better `--follow` ergonomics

- `--since 10m` as an alternative to `--from <ledger>`
- a heartbeat line on stderr when a follow is idle, so a quiet contract is
  distinguishable from a hung process
- `--stats` on exit, reporting records emitted, duplicates suppressed and
  retries

## Later, and less certain

### Alternative sinks

The `Sink` seam makes these small: a file rotator, a webhook poster, an
S3/GCS writer. Worth adding when someone actually needs one rather than
speculatively — each adds configuration surface and failure modes.

### Horizon as a second backend

Horizon offers SSE streaming for classic operations, which would genuinely be
push rather than polling. It is a different API shape and a different data
model, so it would be a second `Source` family rather than a drop-in.

### Prometheus metrics

Only if prism becomes something people run as a daemon. A CLI that exits does
not need a metrics endpoint.

## Explicitly not planned

- **Indexing or storage.** prism streams and exits. Mercury and Zephyr are
  hosted indexers; if you need queryable history, use one of those.
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
