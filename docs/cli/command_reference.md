# prism command reference

Every `prism` command, its arguments and flags, with defaults and example
invocations. Configuration files and environment variables are covered in
[config_options.md](config_options.md).

The flag tables on this page are generated from the CLI's own `--help`
output, so names, types and defaults match the binary. When you add or change
a flag, update this page in the same pull request; `prism <command> --help` is
always the final word.

- [Conventions](#conventions)
- [Global flags](#global-flags)
- [Shared streaming flags](#shared-streaming-flags)
- Streaming commands:
  [`events`](#prism-events) ·
  [`serve`](#prism-serve) ·
  [`transactions`](#prism-transactions) ·
  [`ledgers`](#prism-ledgers) ·
  [`operations`](#prism-operations) ·
  [`storage`](#prism-storage) ·
  [`contracts`](#prism-contracts) ·
  [`wasm`](#prism-wasm) ·
  [`trades`](#prism-trades) ·
  [`fee-stats`](#prism-fee-stats) ·
  [`simulate`](#prism-simulate) ·
  [`aggregate`](#prism-aggregate)
- One-shot and offline commands:
  [`tx`](#prism-tx) ·
  [`diff`](#prism-diff) ·
  [`abi`](#prism-abi) ·
  [`generate-types`](#prism-generate-types) ·
  [`health`](#prism-health) ·
  [`audit`](#prism-audit) ·
  [`audit-verify`](#prism-audit-verify) ·
  [`rewind`](#prism-rewind) ·
  [`completion`](#prism-completion) ·
  [`version`](#prism-version)
- [Configuration (`config`)](#configuration-config)
- [Exit codes](#exit-codes)
- [Known help-text discrepancies](#known-help-text-discrepancies)

## Conventions

```text
prism [global flags] <command> [arguments] [flags]
```

- **Flag order does not matter.** Global flags may appear before or after the
  command name.
- **Types.** `duration` takes Go durations (`500ms`, `30s`, `5m`, `1h30m`).
  `stringArray` flags are repeatable and keep each value whole, commas
  included (`--topic 'AAAAAQ==,*'`). `strings` flags are repeatable *and*
  split on commas (`--type contract,system` equals
  `--type contract --type system`). Booleans are set by naming them
  (`--follow`) or explicitly (`--follow=false`).
- **Defaults.** A dash (—) in the *Default* column means the zero value:
  empty, `0` or `false`, which for most flags means "off".
- **Every flag can be configured.** Any flag in this document can also be
  set with a `PRISM_<FLAG_NAME>` environment variable or a key in a config
  file. A flag on the command line always wins. See
  [config_options.md](config_options.md).
- **stdout is data, stderr is diagnostics.** Records go to stdout (or a
  sink); retries, gap notices, heartbeats and summaries go to stderr, so a
  piped NDJSON stream is never contaminated.

## Global flags

Accepted by every command.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--rpc-url` | stringArray | `$SOROBAN_RPC_URL, else https://soroban-testnet.stellar.org` | Soroban RPC endpoint; repeat for a failover pool |
| `--rpc-timeout` | duration | `30s` | timeout for a single RPC call |
| `--header`, `-H` | stringArray | — | custom HTTP header ("Key: Value") attached to RPC requests; repeat for multiple headers (env $PRISM_HEADERS) |
| `--network` | string | `"testnet"` | expected Stellar network: testnet, mainnet, futurenet, or standalone |
| `--insecure-skip-network-check` | bool | — | skip RPC network passphrase validation |
| `--config` | string | — | explicit config file to load instead of the defaults (&lt;user config dir&gt;/prism/config.yaml, then .prism.yaml in the current directory); see docs/cli/config_options.md |
| `--color` | string | `"auto"` | when to colourise output: "auto", "always" or "never" |
| `--wire-log` | string | — | write a debug NDJSON log of RPC requests and responses to this file, with Authorization and other credential headers redacted ("-" writes to stderr) |
| `--wire-log-max-body` | int | — | maximum bytes of each body recorded by --wire-log (0 uses 1 MiB, -1 disables body capture) |
| `--pprof-port` | int | — | serve Go runtime profiles on http://127.0.0.1:PORT/debug/pprof/ (0 disables) |
| `--serve-sse` | string | — | broadcast SSE at this listen address (/events); retains 1024 records for replay |
| `--sse` | string | — | alias for --serve-sse |
| `--ws` | string | — | broadcast WebSocket events at this listen address (/events or /ws) |
| `--serve-grpc` | string | — | serve gRPC PrismStream at this listen address |

Notes:

- With no `--rpc-url`, prism uses `$SOROBAN_RPC_URL` (comma-separated for a
  pool), then the public testnet endpoint. Repeating `--rpc-url` builds a
  round-robin failover pool.
- `--network` is checked against the endpoint's network passphrase before
  streaming, so pointing a mainnet config at a testnet RPC fails fast.
- `--serve-sse`, `--ws` and `--serve-grpc` broadcast the records of whatever
  streaming command is running, in addition to its normal output.
  [`prism serve`](#prism-serve) is the dedicated command for that.

```console
# Mainnet through a two-node failover pool, with an API key header
prism events --network mainnet \
  --rpc-url https://rpc-a.example.com --rpc-url https://rpc-b.example.com \
  -H "Authorization: Bearer $RPC_TOKEN" \
  --contract CDLZF... --follow

# Record the raw RPC exchanges (credentials redacted) while debugging
prism ledgers --limit 3 --wire-log ./rpc.ndjson
```

## Shared streaming flags

The streaming commands — `events`, `serve`, `transactions`, `ledgers`,
`operations`, `storage`, `contracts`, `wasm`, `trades`, `fee-stats`,
`simulate` and `aggregate` — share the flags below. Each command's own
section lists only the flags that are specific to it, plus its default
`--page-size`, which differs per command.

### Position, polling and lifecycle

| Flag | Type | Default | Description |
|---|---|---|---|
| `--follow`, `-f` | bool | — | keep streaming as new ledgers close, like tail -f |
| `--from` | uint32 | — | start from this ledger and continue forward (default: the latest ledger) |
| `--since` | duration | — | start about this long ago, e.g. --since 10m; converted to an approximate start ledger (mutually exclusive with --from) |
| `--rewind` | string | — | step back from the start point and reprocess those records: a ledger count (500) or a duration (30m); while following, SIGUSR2 steps back again |
| `--limit`, `-n` | int | — | stop after this many records (0 means unlimited) |
| `--page-size` | uint | `200` | how many records to request per RPC call |
| `--poll` | duration | `5s` | how long to wait between polls once caught up |
| `--adaptive` | bool | — | tune the wait between polls from how many new ledgers close each interval (with --follow) |
| `--heartbeat` | duration | — | print a line on stderr after this long waiting in silence (with --follow; 0 disables) |
| `--backfill-workers` | int | `1` | fetch historical pages concurrently with this many workers when --from is set (1 means sequential) |
| `--drain-timeout` | duration | `15s` | how long a shutdown may take to flush records and commit the final cursor |

- Without `--follow`, a command drains what the server currently has and
  exits; with it, the command keeps polling as ledgers close.
- `--from` and `--since` are mutually exclusive. Neither can reach further
  back than the RPC's retention window.
- `--rewind 500` steps back 500 ledgers from the start point; `--rewind 30m`
  steps back roughly 30 minutes' worth. While following, sending `SIGUSR2`
  steps back again by the same distance.

```console
prism events --contract CDLZF... --since 10m --follow
prism transactions --from 1234567 --limit 100000 --backfill-workers 4 --json > txs.ndjson
prism events --contract CDLZF... --rewind 500 --follow
```

### RPC retries and gaps

| Flag | Type | Default | Description |
|---|---|---|---|
| `--max-retries` | int | — | give up after this many consecutive RPC failures (0 means never give up) |
| `--retry-backoff` | duration | `500ms` | wait before the first retry of a failed RPC call; each further retry doubles it, with jitter (0 uses the default) |
| `--retry-backoff-max` | duration | `30s` | longest wait between retries of a failed RPC call (0 uses the default) |
| `--gap-strategy` | string | `"fail"` | what to do when retention pruned records: "fail" (exit 2) or "resume" from the oldest retained ledger |
| `--max-gap-recoveries` | int | — | limit how many times --gap-strategy resume may skip ahead (0 means no limit) |

A failed RPC call is retried with the cursor unchanged, waiting
`--retry-backoff`, then doubling up to `--retry-backoff-max`, with jitter.
Only transient failures (connection errors, timeouts, HTTP 429/502/503/504)
are retried. A gap — records pruned before prism read them — exits with
status 2 under the default `--gap-strategy fail`. See
[the troubleshooting guide](../troubleshooting/streaming_gaps_and_lag.md).

### Output format and destination

| Flag | Type | Default | Description |
|---|---|---|---|
| `--json` | bool | — | emit NDJSON, one record per line |
| `--format` | string | `is human-readable; --json remains supported` | output format: csv or tsv |
| `--csv-fields` | string | — | comma-separated CSV/TSV columns: timestamp,ledger,tx_hash,contract_id,topics,value |
| `--tsv` | bool | — | write tab-separated output (equivalent to --format tsv) |
| `--raw` | bool | — | leave XDR base64-encoded instead of decoding it |
| `--compact` | bool | — | print only the header line for each record, omitting decoded payloads |
| `--output-file` | string | — | append records to this file instead of stdout |
| `--rotate-size` | int | — | rotate the output file before a write would exceed this many bytes (0 disables) |
| `--rotate-interval` | duration | — | rotate the output file after this long (0 disables) |
| `--rotate-keep` | int | — | keep at most this many rotated files (0 keeps all) |
| `--rotate-compress` | bool | — | gzip rotated output files |
| `--batch` | bool | — | buffer records and flush them in batches |
| `--batch-size` | int | `100` | flush a batch after this many records (0 means only the timeout triggers) |
| `--batch-timeout` | duration | `250ms` | flush a batch after this long (0 means only the record count triggers) |
| `--quiet` | bool | — | suppress diagnostic output on stderr; only emit final exit code |
| `--stats` | bool | — | print a run summary on exit: records processed, duplicates dropped, retries and elapsed time |
| `--stats-bar` | bool | — | show live ledger timing and ingest statistics on stderr (with --follow and a terminal stdout) |
| `--dry-run` | bool | — | validate filters and report matches without writing to configured sinks |

```console
# NDJSON into jq
prism events --contract CDLZF... --follow --json | jq -r '.contract'

# CSV with chosen columns
prism events --contract CDLZF... --format csv --csv-fields ledger,tx_hash,topics

# Append to a file, rotating at 100 MiB and keeping 10 gzipped files
prism ledgers --follow --json --output-file /var/log/prism/ledgers.ndjson \
  --rotate-size 104857600 --rotate-keep 10 --rotate-compress
```

### Durable cursors and state

| Flag | Type | Default | Description |
|---|---|---|---|
| `--state-file` | string | — | record cursor metadata in this file, so a later run can resume from it |
| `--resume` | bool | — | resume from the last cursor recorded in --state-file |
| `--prune-interval` | duration | `5m0s` | how often to compact the cursor metadata file in the background (0 compacts once) |
| `--prune-keep` | int | `100` | keep at most this many cursor metadata entries per stream (0 keeps all) |
| `--prune-max-age` | duration | — | drop cursor metadata entries older than this (0 keeps entries regardless of age) |
| `--state-passphrase-env` | string | — | encrypt the state file with a passphrase read from this environment variable |
| `--state-passphrase-file` | string | — | encrypt the state file with a passphrase read from this file |
| `--state-kms-provider` | string | — | encrypt the state file with a key management service: "aws" or "gcp" |
| `--state-kms-key-id` | string | — | the key to use in the key management service (an ARN for AWS, a resource name for GCP) |
| `--state-kms-region` | string | `us-east-1, else $AWS_REGION` | region of the AWS key |
| `--state-kms-token-env` | string | `"GOOGLE_OAUTH_ACCESS_TOKEN"` | environment variable holding the OAuth token used to reach the GCP key service |
| `--pg-dsn` | string | `$PRISM_PG_DSN; empty disables persistence` | PostgreSQL DSN for a durable cursor, so a restart resumes where this run stopped |
| `--pg-cursor-key` | string | — | cursor row to save and resume under (default: &lt;command&gt;/&lt;hash of --from and filters, so each distinct stream resumes itself) |

`--state-file` writes one of two formats:

- **Cursor journal**: used when the path ends in `.jsonl`, or with
  `--resume`, `--prune-max-age`, state encryption or `--ha-lease-file`. It
  holds one checkpoint per delivered page, is compacted by the background
  pruner, and is resumed with `--resume`. An explicit `--from` or `--since`
  wins over the recorded cursor.
- **Single-position file**: any other `--state-file`. It holds only the
  latest position and is resumed automatically on the next run. Here the
  saved position wins: `--from` is ignored, with a notice. This is the
  format [`prism rewind`](#prism-rewind) edits.

```console
# Journal: record the position as the stream runs, then continue from it
prism events --contract CDLZF... --follow --state-file ~/.cache/prism/events.jsonl
prism events --contract CDLZF... --follow --state-file ~/.cache/prism/events.jsonl --resume

# Resume from PostgreSQL instead of a file
PRISM_PG_DSN=postgres://prism@db/prism prism transactions --follow
```

### High availability, health and metrics

| Flag | Type | Default | Description |
|---|---|---|---|
| `--ha-lease-file` | string | — | run as an active-passive pair, taking this lease file as the lock; only the leader streams |
| `--ha-heartbeat` | duration | `5s` | how often the leader renews the lease |
| `--ha-ttl` | duration | `15s` | how long the lease stays valid without a renewal |
| `--health-port` | int | — | serve /healthz, /readyz and /metrics on this port (0 disables the health server) |
| `--max-lag` | uint32 | — | ledgers the stream may be behind before /readyz reports a failure (0 means no limit) |
| `--metrics-addr` | string | — | serve Prometheus metrics at /metrics on this address, e.g. 127.0.0.1:9090 |

`--health-port` serves `/healthz`, `/readyz` and a `/metrics` endpoint with
`prism_stream_lag` (ledgers behind the server) and related gauges.
`--metrics-addr` serves the sink-side Prometheus metrics, including the
`prism_ledger_latency_seconds` histogram.

### Dead-letter queue

| Flag | Type | Default | Description |
|---|---|---|---|
| `--dlq-file` | string | — | append malformed or undeliverable records to this file as NDJSON instead of stopping the stream |
| `--max-record-retries` | int | — | retry a failed record this many times before writing it to the dead-letter queue |

### Additional sinks

Each sink is off unless its flags are set. Several may be combined; every
record is delivered to the normal output and to each configured sink, and a
sink failure stops the stream rather than dropping records.

**Kafka and AMQP**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--kafka-brokers` | strings | — | Kafka broker addresses (comma-separated); requires --kafka-topic |
| `--kafka-topic` | string | — | publish records as JSON to this Kafka topic instead of stdout; requires --kafka-brokers |
| `--kafka-partition-by` | string | `"contract"` | Kafka message key: "contract" keeps one contract's records ordered on one partition, "tx" keys by transaction hash, "record" keys by record ID |
| `--amqp-url` | string | — | AMQP 0-9-1 broker URL, e.g. amqp://guest:guest@localhost:5672/; requires --amqp-queue |
| `--amqp-queue` | string | — | declare this durable queue and publish records to it instead of stdout; requires --amqp-url |
| `--amqp-exchange` | string | — | AMQP exchange to publish to (default: the default exchange, routing by queue name) |
| `--amqp-routing-key` | string | — | AMQP routing key (default: the queue name) |

**Webhook**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--webhook-url` | string | — | POST batches of records as JSON to this HTTP(S) endpoint |
| `--webhook-batch-size` | int | `100` | records per webhook POST |
| `--webhook-timeout` | duration | `1s` | POST a partial webhook batch after this long (0 means only the batch size triggers) |
| `--webhook-max-retries` | int | `3` | retry a failed webhook delivery this many times (-1 disables retries) |
| `--webhook-secret` | string | `$PRISM_WEBHOOK_SECRET` | HMAC-SHA256 secret for X-Prism-Signature |

**Object storage (S3 / GCS)**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--cloud-provider` | string | `"s3"` | object store provider: "s3" or "gcs" |
| `--cloud-bucket` | string | — | object store bucket for archival uploads |
| `--cloud-prefix` | string | `"prism"` | key prefix for archived objects |
| `--cloud-url` | string | `GCS: https://storage.googleapis.com` | object store endpoint for archival uploads |
| `--cloud-region` | string | `us-east-1` | S3 signing region |
| `--cloud-compress` | bool | `true` | gzip archived chunks and name them .ndjson.gz |
| `--cloud-batch-records` | int | `1000` | archive a chunk after this many records |
| `--cloud-batch-bytes` | int | `8388608` | archive a chunk before it exceeds this many bytes |
| `--cloud-batch-timeout` | duration | `30s` | archive a partial chunk after this long (0 disables the timer) |

**Elasticsearch / OpenSearch**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--sink-es-url` | string | — | Elasticsearch or OpenSearch endpoint for bulk ingestion |
| `--sink-es-index` | string | `"stellar-events"` | base Elasticsearch index; records roll over daily |
| `--sink-es-api-key` | string | `$PRISM_ES_API_KEY` | Elasticsearch API key |
| `--sink-es-username` | string | — | Elasticsearch basic-auth username |
| `--sink-es-password` | string | — | Elasticsearch basic-auth password (prefer $PRISM_ES_API_KEY for API keys) |
| `--sink-es-batch-size` | int | `100` | records per Elasticsearch bulk request |
| `--sink-es-flush-interval` | duration | `1s` | flush a partial Elasticsearch bulk request after this interval |

**NATS, ZeroMQ and cloud messaging**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--sink-nats` | string | — | NATS server URL |
| `--nats-subject` | string | `"stellar.events.{{.ContractID}}"` | NATS subject template using .ContractID, .Topic0, .ID or .Ledger |
| `--nats-jetstream` | bool | — | wait for JetStream publish acknowledgements |
| `--sink-zmq` | string | — | ZeroMQ PUB endpoint |
| `--zmq-topic` | string | `"stellar.events.{{.ContractID}}"` | ZeroMQ topic template using .ContractID, .Topic0, .ID or .Ledger |
| `--sink-pubsub-topic` | string | — | Google Pub/Sub topic as projects/PROJECT/topics/TOPIC |
| `--sink-sns-topic` | string | — | AWS SNS topic ARN |
| `--sink-sqs-queue` | string | — | AWS SQS queue URL |
| `--sink-region` | string | — | AWS region (uses SDK defaults when empty) |

**Files: Parquet, syslog and audit trail**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--sink-parquet-dir` | string | — | write partitioned Parquet files under this directory |
| `--syslog-address` | string | — | send records to a syslog socket, e.g. unixgram:///dev/log or udp://host:514 |
| `--syslog-format` | string | `"rfc5424"` | syslog message format: "rfc5424" or "journald" |
| `--syslog-facility` | string | `"local0"` | syslog facility (local0..local7, user, daemon, ...) |
| `--syslog-app` | string | `"prism"` | syslog APP-NAME / SYSLOG_IDENTIFIER |
| `--sink-audit` | string | — | write a tamper-evident audit trail, chained with SHA-256 and signed at checkpoints, to this file |
| `--sink-audit-checkpoint` | int | `100` | write a signed audit checkpoint every this many records (0 writes one after every record) |
| `--sink-audit-key` | string | `<audit file>.key, created if absent` | Ed25519 private key signing the audit checkpoints |

```console
# Publish to Kafka, keyed by contract so each contract stays ordered
prism events --contract CDLZF... --follow \
  --kafka-brokers kafka-1:9092,kafka-2:9092 --kafka-topic soroban-events

# Signed webhook batches (secret from the environment)
PRISM_WEBHOOK_SECRET=s3cr3t prism events --contract CDLZF... --follow \
  --webhook-url https://hooks.example.com/prism --webhook-batch-size 50

# Archive to S3 in gzipped NDJSON chunks
prism transactions --follow --cloud-provider s3 --cloud-bucket my-archive \
  --cloud-prefix prism/txs --cloud-region eu-west-1

# A tamper-evident audit trail, verified later with prism audit-verify
prism events --contract CDLZF... --follow --sink-audit /var/log/prism/audit.log
```

## Streaming commands

### prism events

Stream Soroban contract events, decoded as they arrive.

```text
prism events [flags]
```

Topic filters (`--topic`) are evaluated by the RPC server and use its wildcard
syntax: `*` matches exactly one segment and `**` matches zero or more and must
come last. `--topic-pattern`, `--topic-regex`, `--filter-expr`,
`--exclude-*`, `--sample-rate` and `--max-rate` are applied client-side after
decoding. `--contracts-file` hot-reloads a watchlist; addresses beyond the
RPC's limit of five are queried in coordinated groups.

Default `--page-size`: **200**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--contract` | strings | — | contract ID to watch; repeatable, up to 5 |
| `--contracts-file` | string | — | read and hot-reload contract IDs from this file (one per line) |
| `--dedup-window` | int | — | drop re-polled duplicate events, remembering this many recent fingerprints (0 disables; 4096 covers a page with margin) |
| `--exclude-contract` | strings | — | contract ID to exclude; repeatable |
| `--exclude-topic` | stringArray | — | topic to exclude: base64 ScVal or symbol string (e.g., 'transfer'); repeatable |
| `--fanout-dir` | string | — | write each watched contract's events to its own NDJSON file in this directory, in addition to the main output |
| `--filter-expr` | string | — | client-side boolean expression evaluated against decoded event payloads |
| `--max-rate` | float | — | maximum events per second to emit (0 disables) |
| `--sample-rate` | float | — | fraction of matching events to emit deterministically (0.0 to 1.0; 0 disables) |
| `--topic` | stringArray | — | topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5 |
| `--topic-pattern` | stringArray | — | client-side topic pattern as comma-separated segments over decoded topics: "*", "**", "prefix:TEXT", "re:REGEX", or plain text with "*" and "?" globs; repeatable, up to 5 |
| `--topic-regex` | stringArray | — | client-side topic rule as segment-index:regexp; repeatable and all rules must match |
| `--type` | strings | — | event type to include: contract, system or diagnostic (default: all) |

```console
# Follow one contract
prism events --contract CBGSB... --follow

# Server-side topic filter, last 20 and exit
prism events --contract CBGSB... --topic 'AAAAAQ==,*' --limit 20

# Client-side predicate over decoded payloads
prism events --filter-expr 'value.amount > 1000000000 && topics[0].symbol == "transfer"'

# Hot-reloaded watchlist plus a topic regex
prism events --contracts-file /etc/prism/watchlist.txt --topic-regex '0:^transfer.*' --follow

# Glob-style topic patterns over decoded topics
prism events --contract CBGSB... --topic-pattern 'transfer,*,**' --follow

# One NDJSON file per contract, in addition to stdout
prism events --contract CA... --contract CB... --fanout-dir ./by-contract --follow
```

### prism serve

Serve live contract events over HTTP Server-Sent Events at `/events`, or over
WebSocket at `/events` and `/ws`. It takes the same event filters as
`prism events`, follows by default, and restarts the stream after a panic up
to `--max-restarts` times.

```text
prism serve (--sse ADDR | --ws ADDR | --serve-grpc ADDR) [flags]
```

Default `--page-size`: **200**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--contract` | strings | — | contract ID to watch; repeatable, up to 5 |
| `--contracts-file` | string | — | read and hot-reload contract IDs from this file (one per line) |
| `--dedup-window` | int | — | drop re-polled duplicate events, remembering this many recent fingerprints (0 disables; 4096 covers a page with margin) |
| `--exclude-contract` | strings | — | contract ID to exclude; repeatable |
| `--exclude-topic` | stringArray | — | topic to exclude: base64 ScVal or symbol string (e.g., 'transfer'); repeatable |
| `--fanout-dir` | string | — | write each watched contract's events to its own NDJSON file in this directory, in addition to the main output |
| `--filter-expr` | string | — | client-side boolean expression evaluated against decoded event payloads |
| `--max-rate` | float | — | maximum events per second to emit (0 disables) |
| `--max-restarts` | int | — | maximum stream restarts after panics (0 means unlimited) |
| `--sample-rate` | float | — | fraction of matching events to emit deterministically (0.0 to 1.0; 0 disables) |
| `--topic` | stringArray | — | topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5 |
| `--topic-pattern` | stringArray | — | client-side topic pattern as comma-separated segments over decoded topics: "*", "**", "prefix:TEXT", "re:REGEX", or plain text with "*" and "?" globs; repeatable, up to 5 |
| `--topic-regex` | stringArray | — | client-side topic rule as segment-index:regexp; repeatable and all rules must match |
| `--type` | strings | — | event type to include: contract, system or diagnostic (default: all) |

```console
prism serve --ws 127.0.0.1:8081
prism serve --sse 127.0.0.1:8080 --contract CBGSB...
curl -N http://127.0.0.1:8080/events
```

### prism transactions

Stream transactions as ledgers close. Every record carries a decoded status,
the account that paid the fee and, for a fee-bump wrapper, both the wrapper
and the inner transaction. Failed transactions carry their decoded reason.
Filtering happens after each page is fetched, so the cursor still advances
across suppressed records.

```text
prism transactions [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--detect-conflicts` | bool | — | annotate Soroban transactions with same-ledger read/write footprint conflicts |
| `--fee-account` | strings | — | only emit transactions whose fee was paid by this account; G or M address, repeatable |
| `--fee-bump` | bool | — | only emit transactions wrapped in a fee-bump envelope |
| `--memo` | string | — | include transactions whose sanitized text memo starts with this text |
| `--status` | strings | — | only emit transactions with this status: success or failed (repeatable; default: all) |
| `--trapped` | bool | — | only emit Soroban transactions whose contract execution trapped or panicked |

```console
prism transactions --status failed --follow
prism transactions --trapped --json | jq -r '.failure.operation'
prism transactions --fee-bump --json | jq -r '.feeAccount'
prism transactions --fee-account GBGSB... --follow
prism transactions --detect-conflicts --follow
```

### prism ledgers

Stream ledger close metadata. Ledger metadata is large, so `--compact` is
usually what you want when watching for liveness.

```text
prism ledgers [flags]
```

Default `--page-size`: **50**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--consensus` | bool | — | extract signed consensus value and close delta diagnostics (headers do not prove quorum health) |
| `--expected-validator` | strings | — | expected value signer address; repeatable, requires --consensus |

```console
prism ledgers --follow --compact
prism ledgers --from 1234567 --limit 5 --json
prism ledgers --follow --consensus --expected-validator GABC... --expected-validator GDEF...
```

### prism operations

Deconstruct transactions into one record per operation.

```text
prism operations [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--source-account` | strings | — | operation source account to include; repeatable |
| `--type` | strings | — | operation type to include, such as payment or invoke_host_function |

```console
prism operations --type payment --type invoke_host_function --follow
prism operations --source-account G... --json
```

### prism storage

Stream contract data, contract code and TTL mutations extracted from
transaction metadata. Contract code and TTL entries are not tied to a contract
address, so `--contract` omits them.

```text
prism storage [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--change` | strings | — | change type to include: created, updated, removed or restored (default: all) |
| `--contract` | strings | — | contract ID to watch; repeatable |
| `--entry` | strings | — | entry type to include: contract_data, contract_code or ttl (default: all) |

```console
prism storage --follow
prism storage --contract CBGSB... --change created --change updated --follow
prism storage --entry contract_code --entry ttl --json | jq -r '.wasmHash'
```

### prism contracts

Alias: `contract-lifecycle`. Watch storage changes, restoration and
deployment activity for chosen contracts, including TTL live-until ledgers
and low-TTL warnings.

```text
prism contracts --watch CONTRACT_ID [--watch ...] [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--network-passphrase` | string | `"Test SDF Network ; September 2015"` | network passphrase for contract ID derivation |
| `--watch` | strings | — | contract ID to watch; repeatable |

```console
prism contracts --watch CDLZF... --follow
prism contracts --watch CDLZF... --follow --json
```

### prism wasm

Alias: `deployments`. Stream WASM uploads and contract creations, decoded from
`InvokeHostFunction` operations. Contract IDs are derived from the network
passphrase, so set `--network-passphrase` off testnet. Failed transactions
are skipped unless `--include-failed` is set.

```text
prism wasm [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--include-failed` | bool | — | include uploads and creations from failed transactions |
| `--network-passphrase` | string | `"Test SDF Network ; September 2015"` | network passphrase used to derive contract IDs |

```console
prism wasm --follow
prism wasm --follow --include-failed \
  --network-passphrase "Public Global Stellar Network ; September 2015"
prism wasm --follow --json | jq -r 'select(.action=="create") | .contract'
```

### prism trades

Stream completed classic path-payment executions and Soroban swap events that
use the common swap topic layout (`swap`, trader, asset in, asset out). Soroban
amounts are raw token units, because decimals are contract-specific.

```text
prism trades [flags]
```

Default `--page-size`: **100**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--pair` | string | — | asset pair filter in ASSET/ASSET form |

```console
prism trades --pair XLM/USDC --follow
prism trades --from 1234567 --json
```

### prism fee-stats

Stream one fee-market record per ledger close: base fee and reserve, fee pool,
rolling fee averages, per-operation inclusion fee percentiles, ledger capacity
usage and a surge-pricing flag. The payload and methodology are documented in
[the fee stats guide](../guides/fee_stats_streaming.md).

```text
prism fee-stats [flags]
```

Default `--page-size`: **50**. `fee-stats` has
no flags beyond the shared streaming flags.

```console
prism fee-stats --follow
prism fee-stats --follow --json | jq '{ledger, p90: .inclusionFeeCharged.p90, surge: .surgePricing}'
```

### prism simulate

Simulate a base64 `TransactionEnvelope` at each new ledger head and report
changes to its return value, instructions, memory footprint and minimum
resource fee. See [offline simulation](../guides/offline_simulation.md).

```text
prism simulate --tx-envelope BASE64 [flags]
```

Default `--page-size`: **1**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--tx-envelope` | string | — | base64 TransactionEnvelope to simulate |

```console
prism simulate --tx-envelope AAAA... --follow
prism simulate --tx-envelope AAAA... --json --limit 1
```

### prism aggregate

Aggregate contract events into time-windowed metrics, one record per contract
per window. Windows: `10s`, `30s`, `1m`, `5m`, `15m`, `1h`. Metrics: `count`,
`sum:<field>`, `avg:<field>`, `min:<field>`, `max:<field>` and
`unique_count:<field>`. See
[multi-stream aggregation](../guides/multi_stream_aggregation.md).

```text
prism aggregate --window DURATION --metric METRIC [--metric ...] [flags]
```

Default `--page-size`: **200**.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--contract` | strings | — | contract ID to watch; repeatable, up to 5 |
| `--metric` | stringArray | — | metric to compute: count, sum:&lt;field&gt;, avg:&lt;field&gt;, min:&lt;field&gt;, max:&lt;field&gt;, unique_count:&lt;field&gt;; repeatable |
| `--topic` | stringArray | — | topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5 |
| `--type` | strings | — | event type to include: contract, system or diagnostic (default: all) |
| `--window` | string | — | time window for aggregation (e.g., 10s, 1m, 5m, 1h) |

```console
prism aggregate --contract CBGSB... --window 1m --metric count
prism aggregate --contract CBGSB... --window 5m --metric "sum:value.amount" --metric "avg:value.amount"
prism aggregate --contract CBGSB... --window 1h --metric "unique_count:value.from"
```

## One-shot and offline commands

### prism tx

Fetch a transaction by hash and explain it in plain English: which operation
failed and why, the same explanation `lens explain --result` produces.

```text
prism tx <hash> [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--fail-on-error` | bool | — | exit with a non-zero status when the transaction failed |
| `--json` | bool | — | emit the summary as JSON |
| `--verbose`, `-v` | bool | — | include the underlying XDR result-code constants |

```console
prism tx 3389e9f0a1b2c3...
prism tx 3389e9f0a1b2c3... --json | jq -r '.headline'
prism tx 3389e9f0a1b2c3... --fail-on-error   # exit 2 when the transaction failed
```

### prism diff

Diff two transactions' envelopes, results and metadata as a structural,
colourised diff.

```text
prism diff <txHash1> <txHash2> [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--envelope-only` | bool | — | Compare only the transaction envelopes |
| `--meta-only` | bool | — | Compare only the transaction metadata |

```console
prism diff TXHASH1 TXHASH2
prism diff TXHASH1 TXHASH2 --envelope-only
prism diff TXHASH1 TXHASH2 --meta-only
```

### prism abi

Fetch a contract's WASM and render the Soroban spec embedded in it: functions,
types and events with argument names. Two RPC calls; nothing is cached.

```text
prism abi <contract-id> [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--arg` | stringArray | — | base64 ScVal argument to decode; repeatable and requires --function |
| `--function` | string | — | show only this function and decode any --arg values against it |

```console
prism abi CBGSB...
prism abi CBGSB... --function transfer --arg AAAAAwAAAAc= --arg AAAAAwAAAAo=
```

### prism generate-types

Generate Go structs and unpacking functions for every event type in a
contract's spec.

```text
prism generate-types --contract CONTRACT_ID --out FILE [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--contract` | string | — | contract ID to generate types for (required) |
| `--out` | string | — | output file path (required) |

```console
prism generate-types --contract CDLZF3MA... --out ./pkg/events/events_gen.go
```

### prism health

Probe an RPC endpoint with `getHealth` and report ledger latency and the
retention window. One probe by default (exit 1 when unhealthy), or a watch loop
with `--follow`.

```text
prism health [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--fail-after` | int | — | with --follow, exit after this many consecutive unhealthy checks (0 keeps watching) |
| `--follow`, `-f` | bool | — | keep checking on an interval instead of exiting after one probe |
| `--health-timeout` | duration | `5s` | timeout for a single getHealth request |
| `--interval` | duration | `30s` | how long to wait between checks with --follow |
| `--json` | bool | — | emit one JSON object per check, one per line |
| `--max-ledger-age` | duration | `1m0s` | treat the endpoint as unhealthy when its latest ledger is older than this |

```console
prism health
prism health --json | jq '{latestLedger, oldestLedger, ledgerRetentionWindow}'
prism health --rpc-url $RPC_A --rpc-url $RPC_B --follow --interval 10s --fail-after 3
```

### prism audit

Validate an NDJSON dump: strictly increasing record IDs, no duplicates, and
ledger sequences and timestamps that never go backwards. It cannot detect
ledgers that produced no records. Reads the file named as an argument or by
`--file`, or stdin.

```text
prism audit [FILE | --file FILE] [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--file` | string | — | path to NDJSON file to audit (defaults to stdin) |

```console
prism audit /data/events/2026-10-20.ndjson
cat events.ndjson | prism audit
```

### prism audit-verify

Verify a tamper-evident trail written with `--sink-audit`: record hashes, the
hash chain, and signed checkpoints, optionally against a pinned public key.

```text
prism audit-verify [FILE | --file FILE] [flags]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--file` | string | — | path to the audit trail to verify (defaults to stdin) |
| `--print-public-key` | bool | — | print the PEM public key the trail is signed with |
| `--public-key` | string | — | path to a PEM public key the trail must be signed with |

```console
prism audit-verify /var/log/prism/audit.log
prism audit-verify --public-key /etc/prism/audit.pub /var/log/prism/audit.log
prism audit-verify --print-public-key /var/log/prism/audit.log > audit.pub
```

### prism rewind

Reposition a single-position state file (see
[Durable cursors and state](#durable-cursors-and-state)) so the next run
starts from another ledger. Pass exactly one of `--to-ledger` or `--back`. A
backup of the previous state is written next to the file and its path is
printed. For a cursor journal, choose the position on the streaming command
instead, with `--from`, `--since` or `--rewind`.

```text
prism rewind --state-file FILE (--to-ledger LEDGER | --back N)
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--back` | uint32 | — | number of ledgers to rewind |
| `--state-file` | string | — | state file to rewind |
| `--to-ledger` | uint32 | — | ledger to replay from |

```console
prism events --contract CDLZF... --follow --state-file ./events.state.json   # writes the position
prism rewind --state-file ./events.state.json --back 1000
prism events --contract CDLZF... --follow --state-file ./events.state.json   # replays from 1000 ledgers back

prism rewind --state-file ./events.state.json --to-ledger 3966000
```

A rewind cannot reach data the RPC has already pruned; see
[the troubleshooting guide](../troubleshooting/streaming_gaps_and_lag.md).

### prism completion

Generate a shell completion script, including suggestions for enumerated flag
values such as `--color`, `--type` and `--gap-strategy`.

```text
prism completion bash|zsh|fish|powershell
```

```console
source <(prism completion bash)
prism completion zsh  > "${fpath[1]}/_prism"
prism completion fish > ~/.config/fish/completions/prism.fish
prism completion powershell | Out-String | Invoke-Expression
```

### prism version

Print the prism version.

```console
prism version
```

## Configuration (`config`)

There is no `prism config` subcommand. Configuration is a layer under every
command:

1. flags on the command line,
2. `PRISM_<FLAG_NAME>` environment variables,
3. a config file (`--config FILE`, or `<user config dir>/prism/config.yaml`
   with `./.prism.yaml` merged over it),
4. built-in defaults.

The first source that sets a flag wins. The file format, every key, and the
complete environment variable list are in
[config_options.md](config_options.md).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Error: invalid flags, unreachable RPC after `--max-retries`, sink failure, unhealthy `prism health` |
| 2 | A specific negative outcome: the stream gapped, or `prism tx --fail-on-error` found a failed transaction |

## Known help-text discrepancies

These examples in the built-in `--help` text name flags that do not exist;
use the replacement shown.

| Help text says | Use instead |
|---|---|
| `prism --timescale-dsn ...` (root examples) | no TimescaleDB flag is registered; use `--pg-dsn` for cursors, or a Kafka/webhook sink into your database |
| `prism --parquet-dir ./txs` (root examples) | `--sink-parquet-dir ./txs` |
| `prism audit --from-line 1 ...` | no such flag; filter the input first, e.g. `tail -n +2 events.ndjson \| prism audit` |
