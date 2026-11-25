# Glossary

Terms used in prism's streaming, indexing, and event-routing code.

## Streaming

- **Adaptive polling** — Adjusting the wait between RPC requests based on
  observed ledger activity, rather than using one fixed interval.
  ([`backoff.go`](../../pkg/stream/backoff.go))
- **At-least-once delivery** — A record may be emitted again after a restart
  if delivery succeeds but its cursor is not persisted; consumers can use the
  record ID to make processing idempotent.
  ([`tail.go`](../../pkg/stream/tail.go))
- **Backpressure** — A slow sink slows polling because the tailer emits records
  synchronously and does not build an unbounded in-memory queue.
  ([`source.go`](../../pkg/stream/source.go))
- **Batch (page)** — One ordered set of records fetched from a source, with
  cursor and retention-bound metadata used by the tailer.
  ([`source.go`](../../pkg/stream/source.go))
- **Cursor (polling cursor)** — An opaque token, or a ledger sequence for
  sources without tokens, identifying the position from which the next fetch
  resumes. A source interprets its own cursor.
  ([`source.go`](../../pkg/stream/source.go))
- **De-duplication high-water mark** — The greatest record ID already emitted;
  replayed or reordered records at or below it are suppressed.
  ([`dedup.go`](../../pkg/stream/dedup.go))
- **Gap** — A range of ledgers the source can no longer return because the
  server's retention floor has moved past the stream's cursor, commonly after
  prolonged downtime.
  ([`source.go`](../../pkg/stream/source.go))
- **Gap strategy** — The configured response to a retention gap: fail (the
  default) or resume at the oldest retained ledger and report the skipped
  range.
  ([`gap_recovery.go`](../../pkg/stream/gap_recovery.go))
- **Polling** — Repeatedly fetching pages from Soroban RPC; prism calls this
  streaming because Soroban RPC does not provide a subscription API.
  ([`tail.go`](../../pkg/stream/tail.go))
- **Pruning** — Removing old cursor-journal checkpoints according to retention
  limits while keeping recent positions available for resume.
  ([`pruning.go`](../../pkg/stream/pruning.go))
- **Rewind** — Moving a running stream's fetch position backward to
  intentionally replay records.
  ([`rewind.go`](../../pkg/stream/rewind.go))
- **Source** — An upstream adapter that fetches ordered record batches and
  interprets its cursor.
  ([`source.go`](../../pkg/stream/source.go))
- **Tailer** — The core loop that polls pages, checks for gaps and duplicate
  records, advances and persists the cursor after delivery, and invokes the
  sink.
  ([`tail.go`](../../pkg/stream/tail.go))
- **Durable cursor / cursor journal** — Persisted stream position used to
  resume after a process restart. The journal stores checkpoints; it does not
  make delivery exactly-once across crashes.
  ([`cursor_store.go`](../../pkg/stream/cursor_store.go))

## Events and routing

- **Event topic** — An ordered ScVal in a Soroban contract event used to
  classify it. Topics may be symbols or other ScVal values, including
  32-byte values; prism retains their base64-encoded XDR representation.
  ([`record.go`](../../pkg/stream/record.go))
- **Topic filter** — A server-side event query constraint; a topic pattern is
  a separate client-side match against decoded topic text.
  ([`events.go`](../../pkg/stream/events.go),
  [`topic_pattern.go`](../../pkg/stream/topic_pattern.go))
- **Fan-out** — Sending each record to multiple sinks, in order; an error from
  any sink is returned rather than silently dropping that delivery.
  ([`fanout.go`](../../pkg/sink/fanout.go))
- **Dead Letter Queue (DLQ)** — NDJSON quarantine output for malformed records
  or records a wrapped sink cannot deliver, preserving the original record
  and failure stage for inspection or replay.
  ([`dlq.go`](../../pkg/sink/dlq.go))
- **Sink** — A destination that consumes records and flushes buffered output,
  such as stdout, a database, or a message broker.
  ([`source.go`](../../pkg/stream/source.go))
- **Per-contract demultiplexing** — Routing event records to separate
  per-contract queues and workers.
  ([`demux.go`](../../pkg/stream/demux.go))

## Indexing and storage

- **Hypertable** — A TimescaleDB table partitioned into time-series chunks;
  prism's Timescale sink partitions records by ledger when the extension is
  available.
  ([`timescale.go`](../../pkg/sink/timescale.go))
- **Backfill** — Fetching historical pages in sequence; a full page is fetched
  again immediately so available history drains without a poll-interval wait.
  ([`tail.go`](../../pkg/stream/tail.go))
- **Record ID** — A fixed-width, zero-padded identifier whose lexical order
  matches stream order, enabling simple duplicate suppression.
  ([`record.go`](../../pkg/stream/record.go))
