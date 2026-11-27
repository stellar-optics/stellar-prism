# Troubleshooting streaming gaps and RPC lag

An operator's runbook for the three ways a long-running prism stream goes
wrong:

1. [It stops with a **gap**](#1-gap-errors-exit-code-2): records were pruned
   before prism read them.
2. [It **falls behind** the chain head](#2-lag-behind-the-chain-head): the
   stream is alive but late.
3. [The RPC provider **throttles or errors**](#3-upstream-http-5xx-and-429-errors):
   HTTP 429 or 5xx responses slow it down or stop it.

Each section starts with the symptom you will see, then gives a diagnostic
workflow and the fix. Flags are described in full in the
[command reference](../cli/command_reference.md), and every flag can also be
set from a config file or a `PRISM_*` variable
([config options](../cli/config_options.md)).

## Background: why gaps and lag happen

Soroban RPC is poll-only and keeps a **rolling retention window** of recent
ledgers. Everything older is pruned and cannot be fetched from that node
again. prism follows a cursor through that window:

```text
          pruned               retention window                head
 ──────────────────────┬─────────────────────────────────────────┬──►
                       │ oldestLedger                latestLedger│
                       │          ▲ your cursor                  │
                       │          └── lag ──────────────────────►│
```

- **Lag** is the distance from your cursor to `latestLedger`. It grows when
  prism, or the sink behind it, processes ledgers more slowly than the network
  closes them (about one every 5–6 seconds).
- **A gap** happens when `oldestLedger` overtakes your cursor. That takes
  either a consumer that was **offline** longer than the window, or one that
  **lagged** by more than the window. The records in between are gone from
  that node.

The window is set by the RPC operator. Recent `stellar-rpc` releases default
to about **7 days** (120,960 ledgers) of history. Older releases, and many
hosted providers, keep only about **24 hours** (17,280 ledgers) of events.
Measure your provider rather than assuming:

```console
$ prism health --json | jq '{oldestLedger, latestLedger, ledgerRetentionWindow, oldestLedgerCloseTime}'
{
  "oldestLedger": 3845700,
  "latestLedger": 3966820,
  "ledgerRetentionWindow": 121120,
  "oldestLedgerCloseTime": "2026-11-18T10:12:03Z"
}
```

`ledgerRetentionWindow` × ~5.5 s is roughly how long a consumer can be away
before a restart gaps. With 121,120 ledgers that is about 7.7 days.

prism never passes over a gap silently. It detects one by comparing its
cursor with the `oldestLedger` the server reports on every page, and stops
with exit code 2 (see `stream.GapError` in
[`pkg/stream/source.go`](../../pkg/stream/source.go)).

---

## 1. Gap errors (exit code 2)

### Symptom

```text
prism: gap in events stream: resumed at ledger 3801200 but the server has pruned everything before ledger 3845700, so ledgers 3801200-3845699 were missed
prism: restart with --from 3845700 to resume from the oldest data the server still has
```

The process exits with **status 2**, which is distinct from an ordinary
failure (1), so a supervisor or CI job can branch on it.

### Diagnose

1. **Confirm it is a retention gap and not a misconfiguration.** Compare the
   ledgers in the message with the provider's current window:

   ```console
   prism health --json | jq '{oldestLedger, ledgerRetentionWindow}'
   ```

   If the "resumed at" ledger is below `oldestLedger`, it is a genuine gap.

2. **Find out why the cursor fell out of the window.**
   - *The consumer was offline.* Check how long the process was stopped:
     supervisor logs, the timestamp of the last checkpoint in the state
     file, or the row in the PostgreSQL cursor table (`prism_cursors`).
   - *The consumer was running but lagging.* If `prism_stream_lag` (see
     [section 2](#2-lag-behind-the-chain-head)) climbed steadily before the
     gap, this is a throughput problem. Fix it first, or the gap will recur.
   - *The provider changed.* A new `--rpc-url`, or a provider that shortened
     its retention, moves `oldestLedger` without warning. Pools mixing
     providers with different windows behave like the shortest one.

3. **Size the hole.** Missed ledgers = `Oldest − From` from the message.
   At ~5.5 s per ledger, 44,500 ledgers is roughly 68 hours of data.

### Recover

Pick one path, depending on whether you need the missing records.

#### Path A: accept the loss and re-anchor

Use this when the missed records do not matter, or when you will backfill
them separately (Path B).

How you re-anchor depends on where the cursor lives:

| Cursor store | Re-anchor with |
|---|---|
| None | `prism events ... --from <oldest>` |
| Cursor journal (`--state-file x.jsonl`, or with `--resume`) | Start once with `--from <oldest>`, which overrides the recorded cursor. Later restarts with `--resume` continue from the new position. |
| Single-position file (plain `--state-file`) | `--from` is ignored while the file holds a position, so reposition the file: `prism rewind --state-file x.json --to-ledger <oldest>`. A backup is written alongside. |
| PostgreSQL (`--pg-dsn`) | Start with `--from <oldest>`. `--from` is part of the derived cursor row key, so this starts a fresh row; delete the stale row that prism names on stderr. Keep passing the same `--from` on later restarts so they find the new row, or set a stable `--pg-cursor-key` instead. |

```console
# Journal example: re-anchor at the oldest retained ledger, keep following
prism events --contract CDLZF... --follow \
  --state-file /var/lib/prism/events.jsonl --resume --from 3845700
```

To make future gaps self-healing instead of fatal, opt in to automatic
recovery:

```console
prism events --contract CDLZF... --follow \
  --gap-strategy resume --max-gap-recoveries 3
```

With `--gap-strategy resume`, prism logs the missed range and skips to the
oldest retained ledger:

```text
prism: gap in events stream: ... so ledgers 3801200-3845699 were missed; recovering by resuming at ledger 3845700
```

`--max-gap-recoveries` puts a ceiling on how often that may happen before
prism exits 2 anyway, so a persistent throughput problem cannot hide behind
repeated skips. Every recovery is also counted in
`prism_errors_total{stage="gap"}` when `--metrics-addr` is set. Alert on it.

> `resume` trades completeness for availability. Leave the default `fail`
> on streams where a silent hole would be worse than downtime, such as
> accounting, compliance or audit trails.

#### Path B: backfill the missing range from a provider that still has it

prism itself reads only RPC, so "archive replay" means pointing a one-off
prism run at a source with a longer window:

- a full-history or long-retention RPC provider;
- your own `stellar-rpc` configured with a larger `HISTORY_RETENTION_WINDOW`;
- for analytics workloads, Stellar's public data lake or BigQuery datasets,
  queried outside prism.

1. **Check the archive provider actually has the range:**

   ```console
   prism health --rpc-url https://archive.example.com --json | jq .oldestLedger
   # must be <= the "resumed at" ledger from the gap message
   ```

2. **Backfill without `--follow`, into the same destination.** Without
   `--follow` the run drains to that provider's head and exits.
   `--backfill-workers` fetches the range in parallel:

   ```console
   prism events --contract CDLZF... \
     --rpc-url https://archive.example.com \
     --from 3801200 --backfill-workers 4 \
     --json --output-file ./backfill-3801200.ndjson
   ```

   Use a different `--state-file`, or none, so the backfill cannot move the
   live stream's cursor.

3. **Restart the live stream** with Path A.

4. **De-duplicate the overlap downstream.** The backfill and the live stream
   overlap around `<oldest>`. Every record carries a fixed-width,
   lexicographically ordered `id`, so de-duplicate on it: a primary key, an
   idempotent upsert, or `jq -s 'unique_by(.id)[]'` for files.

5. **Verify the de-duplicated result is continuous:**

   ```console
   cat backfill-3801200.ndjson live.ndjson | jq -c -s 'unique_by(.id)[]' | prism audit
   ```

   `prism audit` flags duplicate or non-increasing IDs, ledgers that go
   backwards and timestamps that go backwards. It cannot see a ledger that
   produced no records, so also check that the backfill's first and last
   `ledger` cover the missed range from the gap message:

   ```console
   jq -s '{first: .[0].ledger, last: .[-1].ledger}' backfill-3801200.ndjson
   ```

#### Path C: replay a window that is still retained

`prism rewind` and `--rewind` re-emit records **inside** the retention
window, for example after a sink bug corrupted the last hour of output. They
cannot bring back pruned data: rewinding below `oldestLedger` produces the
same gap again.

```console
prism events --contract CDLZF... --follow --rewind 30m      # one-off replay
prism rewind --state-file ./events.state.json --back 720     # reposition a single-position file
```

### Prevent

- **Alert on lag long before it reaches the window.** Page at a fraction of
  `ledgerRetentionWindow` (see [section 2](#2-lag-behind-the-chain-head)).
- **Keep downtime well inside the window.** If maintenance can take a day,
  a 24-hour provider is not enough.
- **Run an active-passive pair** (`--ha-lease-file`) so a standby takes over
  from the committed cursor when the leader dies.
- **Record the cursor durably** (`--state-file` or `--pg-dsn`), so a restart
  resumes exactly where it stopped instead of re-anchoring at the head.

---

## 2. Lag behind the chain head

### Symptom

- Records arrive minutes or hours after their ledger closed.
- `/readyz` returns 503 when `--max-lag` is set.
- `prism_stream_lag` grows steadily instead of hovering near zero.

### Monitor

Two endpoints expose lag. Enable one or both:

```console
prism events --contract CDLZF... --follow \
  --health-port 8090 --max-lag 60 \
  --metrics-addr 127.0.0.1:9090
```

| Metric | Endpoint | Meaning |
|---|---|---|
| `prism_stream_lag` | `--health-port` → `/metrics` | Ledgers between the newest one prism has emitted and the server's head |
| `prism_stream_current_ledger` / `prism_stream_latest_ledger` | `--health-port` | The two sides of that difference |
| `prism_health_ready` | `--health-port` | 0 when lag exceeds `--max-lag`, or the stream is stuck |
| `prism_ledger_latency_seconds` | `--metrics-addr` → `/metrics` | Histogram of the delay between a ledger closing and prism emitting its records |
| `prism_errors_total{stage}` | `--metrics-addr` | `rpc`, `sink` and `gap` errors |

prism does not publish a gauge named `prism_ledger_lag_seconds`. Use either
of these instead:

```promql
# Lag in seconds, from the ledger gap (about 5.5 s per ledger)
prism_stream_lag * 5.5

# Lag in seconds, from end-to-end emit latency (p95 over 5 minutes)
histogram_quantile(0.95, sum by (le) (rate(prism_ledger_latency_seconds_bucket[5m])))
```

Example alerts, for a provider with a 7-day window:

```yaml
groups:
  - name: prism
    rules:
      - alert: PrismLagging
        expr: prism_stream_lag > 120            # ~11 minutes behind
        for: 10m
      - alert: PrismGapRisk
        expr: prism_stream_lag > 30000          # ~2 days: a quarter of a 7-day window
        for: 5m
        labels: { severity: page }
      - alert: PrismGapRecovered
        expr: increase(prism_errors_total{stage="gap"}[1h]) > 0
```

For a quick look without Prometheus:

```console
curl -s localhost:8090/metrics | grep -E 'prism_stream_(lag|current|latest)'
prism events ... --follow --stats-bar     # live ledger timing on stderr (terminal only)
prism events ... --follow --stats         # summary on exit: records, duplicates, retries, elapsed
```

### Diagnose: is it the fetch or the sink?

prism runs one polling loop, and each sink is called synchronously from it.
A slow sink therefore slows polling instead of growing an unbounded queue.
Lag always comes from one of two places, and the fix depends on which.

1. **Take the sinks out of the picture.** Run the same command with
   `--dry-run`, which applies every filter and reports matches without
   writing to the configured sinks, and compare throughput with `--stats`:

   ```console
   prism events --contract CDLZF... --from 3960000 --limit 50000 --stats --dry-run
   prism events --contract CDLZF... --from 3960000 --limit 50000 --stats --kafka-brokers ... --kafka-topic ...
   ```

   - Both runs are slow → **fetch-bound**. Go to step 2.
   - Only the second is slow → **sink-bound**. Go to *Fix a slow sink*.

2. **Check the RPC itself.** `prism health` reports probe latency. A wire
   log shows each request's duration and status:

   ```console
   prism health --json | jq '{probeLatencyMs, ledgerLatencyMs}'
   prism events ... --limit 2000 --wire-log ./rpc.ndjson
   ```

   High probe latency or frequent retries on stderr point to the provider
   (see [section 3](#3-upstream-http-5xx-and-429-errors)). Fast responses
   that each return a full page mean prism is making too many round trips
   (see *Fix a fetch bottleneck*).

### Fix a fetch bottleneck

| Action | Flag | Why it helps |
|---|---|---|
| Fetch more per call | `--page-size` (events default 200; ledgers and fee-stats 50; most others 100) | Fewer round trips per ledger. The server caps the size, so raise it until pages stop growing. |
| Parallel catch-up | `--backfill-workers N` with `--from` | Historical pages are fetched by N workers, then delivered in order |
| Poll sooner when caught up | `--poll 2s`, or `--adaptive` | Cuts idle wait, so freshness rather than throughput |
| Filter server-side | `--contract`, `--topic`, `--type` | Less data fetched and decoded than with client-side `--filter-expr` / `--topic-pattern` |
| Skip decoding | `--raw`, `--compact` | Leaves XDR base64 when the consumer decodes it anyway |
| Closer or faster RPC | `--rpc-url` | Latency is paid on every page |

### Fix a slow sink

| Action | Flag |
|---|---|
| Batch stdout / file writes | `--batch --batch-size 500 --batch-timeout 250ms` |
| Larger webhook batches | `--webhook-batch-size`, `--webhook-timeout` |
| Larger Elasticsearch bulk requests | `--sink-es-batch-size`, `--sink-es-flush-interval` |
| Larger object-store chunks | `--cloud-batch-records`, `--cloud-batch-bytes`, `--cloud-batch-timeout` |
| Do not block on one bad record | `--dlq-file` with `--max-record-retries` |
| Lighter payloads | `--raw`, or `--compact` for human output |

**Parallel sinks.** Within one process, sinks run in sequence on the polling
loop, which preserves ordering and the delivery guarantee. To add real
parallelism, use one of these:

- **Fan out downstream.** Publish once to a broker (`--kafka-topic`,
  `--sink-nats`, `--amqp-queue`) and let consumer groups do the slow work in
  parallel. prism then only pays for the fast publish.
- **Shard the stream across processes.** Split contracts between several
  prism instances, each with its own `--contract` set and its own
  `--state-file` or `--pg-cursor-key`. Kafka's `--kafka-partition-by contract`
  keeps each contract ordered within its shard.
- **Write a buffered sink.** A custom sink can absorb bursts with a bounded
  queue and a background writer. See
  [authoring custom sinks](../contributing/authoring_custom_sinks.md).

---

## 3. Upstream HTTP 5xx and 429 errors

### Symptom

Lines like these on stderr, while the stream stalls between them:

```text
prism: RPC call failed (rpc getEvents: 429 Too Many Requests); retrying in 1.73s (attempt 3)
prism: RPC call failed (rpc getEvents: 503 Service Unavailable); retrying in 7.9s (attempt 5)
```

With `--max-retries N`, the stream exits 1 after N consecutive failures:

```text
prism: events stream: giving up after 5 consecutive failures: ...
```

### How prism retries

- **What is retried:** connection errors, timeouts, and HTTP 429, 502, 503
  and 504 (and the equivalent JSON-RPC error text). Anything else, such as a
  malformed request or a 4xx other than 429, stops the stream immediately,
  because retrying would not help.
- **The cursor never moves on a retry**, so nothing is skipped or duplicated
  however many attempts it takes.
- **Schedule:** wait `--retry-backoff` (default 500ms), doubling after each
  consecutive failure up to `--retry-backoff-max` (default 30s). Each wait is
  jittered over its lower half, so a fleet of instances does not retry in
  lockstep. One success resets the schedule.
- **How long:** `--max-retries 0` (the default) retries forever. Any other
  value gives up after that many consecutive failures, with exit code 1.
- prism does **not** read `Retry-After` from RPC responses; the backoff
  flags are the control. (The webhook *sink* does honour `Retry-After` from
  your endpoint.)

### Diagnose

1. **Tell throttling from outage.** 429 means you are over the provider's
   rate limit. 502/503/504 mean the provider, or a load balancer in front of
   it, is unhealthy. Count each kind:

   ```console
   prism events ... --follow 2>&1 >/dev/null | grep -o 'RPC call failed ([^)]*)' | sort | uniq -c
   ```

2. **Measure your request rate.** A wire log records every request with its
   status:

   ```console
   prism events ... --follow --wire-log ./rpc.ndjson
   jq -r 'select(.status) | .status' rpc.ndjson | sort | uniq -c
   ```

   The wire log redacts `Authorization`, cookies and credential-bearing query
   parameters, so it is safe to attach to a support ticket. Use
   `--wire-log-max-body -1` to drop bodies entirely.

3. **Track it continuously** with `prism_errors_total{stage="rpc"}` from
   `--metrics-addr`.

4. **Check the provider directly:**

   ```console
   prism health --rpc-url https://rpc.example.com --follow --interval 10s --fail-after 3
   ```

### Fix throttling (429)

Reduce how many requests you make, then make the retries gentler:

```console
prism events --contract CDLZF... --follow \
  --page-size 1000 \
  --adaptive \
  --retry-backoff 2s --retry-backoff-max 2m
```

- **Fewer requests per ledger:** raise `--page-size`, and filter server-side
  with `--contract`, `--topic` and `--type`.
- **Fewer idle polls:** `--adaptive` tunes the poll interval to the observed
  ledger cadence, between 1 s and 30 s. A fixed `--poll 6s` roughly matches
  the close rate.
- **Gentler retries:** a larger `--retry-backoff` and `--retry-backoff-max`
  let a rate-limit window reset instead of hammering it.
- **Consolidate:** several prism processes watching the same contracts cost
  several times the quota. One process publishing to a broker
  (`--kafka-topic`, `--sink-nats`), or `prism serve` for live fan-out, makes
  one set of RPC calls for all consumers.
- **Authenticate:** many providers grant a higher quota to keyed requests.
  Pass the key as a header: `export PRISM_HEADERS="Authorization: Bearer $KEY"`.

### Fix provider errors (5xx)

- **Fail over:** repeat `--rpc-url` to build a pool. prism rotates between
  endpoints and moves away from one that fails:

  ```console
  prism events ... --follow \
    --rpc-url https://rpc-a.example.com --rpc-url https://rpc-b.example.com
  ```

  Pool members should have similar retention windows (see
  [section 1](#1-gap-errors-exit-code-2)).
- **Ride out short outages:** keep `--max-retries 0` so prism waits for the
  provider. Raise `--retry-backoff-max` if each attempt adds load to a
  struggling node.
- **Fail fast under a supervisor:** set `--max-retries` (for example `10`)
  so the process exits 1 and your orchestrator restarts it or alerts. With a
  durable cursor, the restart resumes without loss.
- **Long calls timing out:** large pages over a slow link can exceed
  `--rpc-timeout` (default 30s). Raise it, or lower `--page-size`.

### Tuning reference

| Goal | Flags |
|---|---|
| Survive long outages without exiting | `--max-retries 0 --retry-backoff-max 2m` |
| Exit quickly so a supervisor takes over | `--max-retries 5 --retry-backoff 500ms --retry-backoff-max 5s` |
| Stay under a strict rate limit | `--adaptive --page-size 1000 --retry-backoff 2s --retry-backoff-max 2m` |
| Lowest freshness latency | `--poll 1s` or `--adaptive`, a nearby `--rpc-url`, small `--batch-timeout` |

The same settings in `.prism.yaml`:

```yaml
max-retries: 0
retry-backoff: 2s
retry-backoff-max: 2m
adaptive: true
rpc-url:
  - https://rpc-a.example.com
  - https://rpc-b.example.com
```

---

## Quick reference

| Symptom | First check | Likely fix |
|---|---|---|
| Exit 2, "gap in ... stream" | `prism health --json` → `oldestLedger` | Re-anchor (Path A), backfill (Path B), fix the lag that caused it |
| `/readyz` 503, lag climbing | `--dry-run` vs normal throughput | Larger `--page-size` / `--backfill-workers`, or batch and fan out the sink |
| "RPC call failed (... 429 ...)" | Request rate in `--wire-log` | `--adaptive`, larger `--page-size`, larger `--retry-backoff*`, an API key header |
| "RPC call failed (... 503 ...)" | `prism health --follow` | Repeat `--rpc-url` to fail over; tune `--max-retries` for your supervisor |
| Exit 1, "giving up after N consecutive failures" | stderr just before it | Raise `--max-retries`, or let the supervisor restart with a durable cursor |
