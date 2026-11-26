# RPC pooling, health checks, and failover

A single Soroban RPC endpoint is a single point of failure: rate limits (HTTP
429), maintenance windows, and latency spikes all interrupt a stream. When more
than one `--rpc-url` is configured, `prism` replaces the single RPC client with
[`stream.Pool`](../../pkg/stream/pool.go), an `rpc.Client` that spreads calls
over the endpoints and fails over automatically.

The pool is described briefly in
[`docs/architecture.md`](../architecture.md#multi-endpoint-failover-pkgstreampoolgo).
This guide covers the operator-facing mechanics. Rate-limit troubleshooting is
also in
[streaming gaps and lag](../troubleshooting/streaming_gaps_and_lag.md).

- [Configure an endpoint pool](#configure-an-endpoint-pool)
- [Topology and routing](#topology-and-routing)
- [Health checks and recovery](#health-checks-and-recovery)
- [Rate limits and HTTP 429](#rate-limits-and-http-429)
- [Observability and runbook](#observability-and-runbook)
- [Unsupported strategies](#unsupported-strategies)

---

## Configure an endpoint pool

| Input | Example | Notes |
|---|---|---|
| `--rpc-url <url>` | `--rpc-url https://rpc1.example.com --rpc-url https://rpc2.example.com` | Repeatable persistent flag; order is preserved |
| `SOROBAN_RPC_URL` | `http://rpc1:8000,http://rpc2:8000,http://rpc3:8000` | Comma-separated fallback when no flag is set |
| Config file | `rpc-url: [...]` | YAML list applied by the generic config loader |
| `--rpc-timeout <duration>` | `--rpc-timeout 30s` | Timeout for one RPC call |
| `--header "Key: Value"` | `--header "Authorization: Bearer ..."` | Outbound RPC header; repeatable |
| `--network <name>` | `--network mainnet` | Passphrase checked through the pooled `GetNetwork` |

```sh
prism events \
  --follow \
  --network mainnet \
  --rpc-url https://rpc1.example.com \
  --rpc-url https://rpc2.example.com \
  --rpc-url https://rpc3.example.com \
  --contract CDLZF...CYSC
```

```sh
SOROBAN_RPC_URL='http://rpc1:8000,http://rpc2:8000,http://rpc3:8000' \
prism events --follow --network testnet
```

> [!NOTE]
> `--rpc-urls` is **not** a flag in this version. Use repeated `--rpc-url`, a
> comma-separated `SOROBAN_RPC_URL`, or a YAML `rpc-url` list. With exactly one
> resolved URL, `prism` uses a plain client and there is no pool, no background
> probe, and no failover.

`NewPool` trims whitespace, skips blank entries, drops duplicates, and requires
at least one unique URL. The URL list is resolved at startup; changing flags or
the environment requires a restart. CLI flags take precedence over the
`PRISM_<FLAG>` environment variable, which takes precedence over the config
file. Those resolved flags are followed by `SOROBAN_RPC_URL` and finally the
built-in default (`https://soroban-testnet.stellar.org`).

---

## Topology and routing

The pool keeps one SDK client per endpoint and implements the full `rpc.Client`
interface (`GetEvents`, `GetEventsBatch`, `GetLedgers`, `GetTransaction`,
`GetTransactions`, `SimulateTransaction`, `GetLatestLedger`).

- **Round-robin selection.** `pick` starts at a rotating `next` index and
  advances after each attempt. This is the only strategy available in the CLI
  and in `stream.Pool`; there is no `priority-failover` mode or strategy flag.
  Listing a preferred endpoint first does not make it sticky — subsequent
  calls still rotate.
- **Failover on retryable errors.** If an endpoint returns a retryable error,
  the same call is retried on the next endpoint, up to one attempt per
  endpoint. Batch calls are retried whole, preserving record order.
- **Permanent errors stop immediately.** `rpc.Permanent` errors (for example a
  malformed cursor) and context cancellation are returned without trying
  other endpoints, because another backend cannot fix them.
- **All endpoints cooling down.** The pool still tries the endpoint whose
  cooldown expires first. A stale health mark is more likely than a total
  outage, and the request itself is the authoritative check.
- **Aggregated errors.** If every endpoint fails, the returned error names each
  endpoint so logs identify the affected backends.

---

## Health checks and recovery

When an endpoint fails with a retryable error, it is marked unhealthy and its
failure count increases. Healthy calls clear the mark, the failure count, and
the cooldown.

A background goroutine runs every **30 seconds** (per-probe timeout **5
seconds**) and probes unhealthy endpoints with **`GetLatestLedger`**. A
successful probe restores the endpoint to rotation; a failed probe repeats the
backoff. `Close` cancels the probe and waits for it, so a pool never leaks a
goroutine.

Recovery delay uses exponential backoff with jitter:

| Setting | Default |
|---|---|
| Initial delay | 500 ms |
| Maximum delay | 30 s |
| Factor | 2 |
| Jitter | On; actual delay is `[d/2, d]` after each doubling |

> [!NOTE]
> The Go API exposes `WithHealthInterval` (zero disables the probe),
> `WithHealthTimeout`, `WithBackoff`, `WithFailureCooldown`, and
> `WithClientFactory`, but the CLI exposes **none** of them. Endpoint selection
> currently uses the exponential backoff delay; do not rely on
> `WithFailureCooldown` for a fixed cool-down period.

`GetHealth` is implemented by the pool for the separate `prism health` command.
That command uses `rpc.HealthChecker`, which evaluates `getHealth` status and
ledger staleness; the pool's own rotation does not inspect the `getHealth`
status or `Retry-After`. These are different health systems:

```sh
# Health command: GetHealth, retention and max-ledger-age checks
prism health \
  --rpc-url https://rpc1.example.com \
  --rpc-url https://rpc2.example.com \
  --interval 30s \
  --health-timeout 5s \
  --max-ledger-age 60s
```

> [!NOTE]
> There is no explicit circuit breaker (open / half-open / closed states or
> threshold flags). Endpoint cooldown plus background probing is the closest
> implemented behavior.

---

## Rate limits and HTTP 429

`rpc.IsRetryable` classifies rate-limit errors by their text, including
`"429"` and `"too many requests"`. Such an error is treated like any other
retryable failure:

1. The endpoint that returned 429 is marked unhealthy and cooled down.
2. The in-flight call is retried on the next endpoint immediately, without
   waiting for the pool's recovery backoff.
3. If every endpoint is rate-limited, the aggregated error is returned to the
   stream tailer, which applies its normal `--retry-backoff` /
   `--retry-backoff-max` schedule.

There is no typed 429 handler, no `Retry-After` parsing, and no 429-specific
metric in `prism`. To confirm that a rate limit caused a failover, record the
exact RPC exchange:

```sh
prism events \
  --follow \
  --rpc-url https://rpc1.example.com \
  --rpc-url https://rpc2.example.com \
  --wire-log /var/log/prism/rpc.ndjson
```

The wire log redacts `Authorization` and other credential headers.

---

## Observability and runbook

Go callers can inspect `Pool.Healthy()`, `Pool.URLs()`, and call
`Pool.HealthCheck(ctx)` manually. The CLI reports health separately through
`prism health` and emits the aggregated endpoint errors on failure.

A practical runbook:

1. **One endpoint returns 429.** Leave it in the pool: the next call fails
   over. Check its upstream rate-limit metrics and keep the other endpoints
   healthy.
2. **One endpoint is down for maintenance.** Remove it from `--rpc-url` (or
   `SOROBAN_RPC_URL`) and restart the stream. There is no hot reload of the URL
   list.
3. **All endpoints fail.** Inspect the aggregated error and the wire log.
   Check network passphrase consistency (`--network`), because a passphrase
   mismatch is permanent and cannot be failovered.
4. **Endpoint recovers.** The background probe restores it within roughly one
   30-second interval, subject to backoff. Verify with `prism health`.
5. **Latency spike without errors.** The pool does not fail over on slow but
   successful calls. Reduce `--rpc-timeout` if you need faster cutover, and
   remember it applies to every call.

---

## Unsupported strategies

The following are often expected from a "pool" but are **not implemented** in
this version:

- `--rpc-urls` (use repeated `--rpc-url` or comma-separated
  `SOROBAN_RPC_URL`).
- `priority-failover` or any strategy-selection flag; routing is round-robin.
- `getHealth`-driven pool membership; the pool probes with `GetLatestLedger`.
- Circuit-breaker states, thresholds, or half-open probing.
- A fixed 30-second cooldown controlled by `WithFailureCooldown` (selection
  uses exponential backoff).
- CLI-configurable pool health interval, probe timeout, or backoff.
- Dedicated 429 handling, `Retry-After` parsing, or per-endpoint rate-limit
  metrics.

See [`pkg/stream/pool.go`](../../pkg/stream/pool.go) for the authoritative
implementation and [`pkg/rpc/client.go`](../../pkg/rpc/client.go) for retryable
error classification.
