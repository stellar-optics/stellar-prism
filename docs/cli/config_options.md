# prism configuration files and environment variables

Every `prism` flag can be set in three places besides the command line: a
`PRISM_*` environment variable, a per-directory `.prism.yaml`, or a per-user
`config.yaml`. This page explains where each is read from, the order in which
they win, the file format, and every key and variable.

For what each flag *does*, see the [command reference](command_reference.md).

- [Precedence](#precedence)
- [Where config files are read from](#where-config-files-are-read-from)
- [File format](#file-format)
- [Annotated example](#annotated-example)
- [Environment variables](#environment-variables)
- [Key and variable index](#key-and-variable-index)
- [Validation and troubleshooting](#validation-and-troubleshooting)
- [Security notes](#security-notes)

## Precedence

For each flag, the first source that sets it wins:

```text
1. CLI flag              prism events --page-size 500
2. Environment variable  PRISM_PAGE_SIZE=500
3. Config file           a. the command's section     events: { page-size: 500 }
                         b. the top level             page-size: 500
                         (./.prism.yaml is merged over the per-user file;
                          --config FILE replaces both)
4. Built-in default      the "Default" column in the command reference
```

Values from the environment or a file are applied *as if they had been
typed*. They go through the same parsing and validation as the flag, and they
count as "set" for every rule that asks whether a flag was given. For example,
`events: {from: 5, since: 10m}` in a file fails with
`--from and --since are mutually exclusive`, exactly as the two flags would.

A worked example: with this `.prism.yaml`

```yaml
page-size: 100
events:
  page-size: 300
```

| Invocation | Effective `--page-size` | Why |
|---|---|---|
| `prism ledgers` | 100 | top-level key; `ledgers` has no section |
| `prism events` | 300 | the `events` section beats the top level |
| `PRISM_PAGE_SIZE=50 prism events` | 50 | the environment beats the file |
| `PRISM_PAGE_SIZE=50 prism events --page-size 10` | 10 | the flag beats everything |

### Special cases

A few settings have their own variables, which predate the generic
`PRISM_<FLAG>` mapping and keep working. Each is consulted only when nothing
above it in the list has set the value.

| Setting | Full order (first wins) |
|---|---|
| RPC endpoints | `--rpc-url` → `PRISM_RPC_URL` → config `rpc-url` → `SOROBAN_RPC_URL` (comma-separated) → `https://soroban-testnet.stellar.org` |
| RPC headers | `--header` / `PRISM_HEADER` / config `header`, **merged with** `PRISM_HEADERS`; where both name the same header, the flag-side value wins |
| PostgreSQL cursor DSN | `--pg-dsn` → `PRISM_PG_DSN` → config `pg-dsn` |
| Webhook HMAC secret | `--webhook-secret` → `PRISM_WEBHOOK_SECRET` → config `webhook-secret` |
| Elasticsearch API key | `--sink-es-api-key` → `PRISM_SINK_ES_API_KEY` → config `sink-es-api-key` → `PRISM_ES_API_KEY` |
| Colour | `--color` → `PRISM_COLOR` → config `color` → default `auto`, which honours [`NO_COLOR`](https://no-color.org) and whether stdout is a terminal |

## Where config files are read from

| File | Path | Required |
|---|---|---|
| Per-user | `<user config dir>/prism/config.yaml` | no |
| Per-directory | `.prism.yaml` in the current working directory | no |
| Explicit | the path given to `--config` | yes, it must exist |

`<user config dir>` is Go's
[`os.UserConfigDir`](https://pkg.go.dev/os#UserConfigDir):

| OS | Per-user file |
|---|---|
| Linux, BSD | `$XDG_CONFIG_HOME/prism/config.yaml`, usually `~/.config/prism/config.yaml` |
| macOS | `~/Library/Application Support/prism/config.yaml` |
| Windows | `%AppData%\prism\config.yaml` |

Both default files are optional. When both exist, `.prism.yaml` is merged over
the per-user file key by key: a project can override one setting and inherit
the rest. `--config FILE` reads only that file and ignores both defaults,
which makes it the right choice for CI and containers. `--config` itself can
only be given on the command line.

## File format

The file is YAML. **Every key is a flag name without the leading dashes.**

- **Top-level keys** apply to every command that has that flag, and are
  ignored by commands that do not. Use them for global flags (`rpc-url`,
  `network`, `color`, ...) and settings common to all streaming commands
  (`page-size`, `poll`, `state-file`, ...).
- **Command sections** are mappings named after a command (`events:`,
  `transactions:`, `health:`, ...). Their keys apply only to that command
  and beat top-level keys. Aliases use the canonical name: settings for
  `prism deployments` go under `wasm:`, and for `prism contract-lifecycle`
  under `contracts:`.
- **Value types** follow the flag type: strings, numbers, `true`/`false`,
  and durations as strings (`"30s"`, `"5m"`).
- **Repeatable flags** (`stringArray` and `strings`) take a YAML list, one
  element per repetition. A scalar sets a single value. List elements are
  passed whole, so a server-side topic filter keeps its commas.

Because a top-level key reaches every command with that flag name, keep
command-specific settings in a section. `type`, for example, means *event*
type to `events` but *operation* type to `operations`.

## Annotated example

A complete `.prism.yaml` covering RPC endpoints, filtering rules, sink
parameters, durable state and logging:

```yaml
# ── RPC endpoints ─────────────────────────────────────────────────────────
network: mainnet                  # expected network; checked against the RPC
rpc-url:                          # a list builds a round-robin failover pool
  - https://rpc-a.example.com
  - https://rpc-b.example.com
rpc-timeout: 15s                  # per-call timeout
header:                           # extra HTTP headers on every RPC request
  - "X-Client: prism-indexer"     # (keep credentials in PRISM_HEADERS instead)

# ── Settings shared by every streaming command ────────────────────────────
page-size: 200
poll: 5s
adaptive: true                    # tune the poll interval from ledger cadence
max-retries: 0                    # 0 = retry transient RPC errors forever
retry-backoff: 1s                 # first retry wait; doubles each attempt...
retry-backoff-max: 1m             # ...up to this ceiling, with jitter
gap-strategy: fail                # exit 2 on a retention gap (or: resume)
drain-timeout: 15s

# ── Logging and diagnostics ───────────────────────────────────────────────
color: never                      # auto | always | never
quiet: false                      # true silences stderr diagnostics
stats: true                       # print a run summary on exit
heartbeat: 1m                     # stderr line after a minute of silence
# wire-log: /var/log/prism/rpc.ndjson   # raw RPC log, credentials redacted
# wire-log-max-body: 65536

# ── Per-command settings ──────────────────────────────────────────────────
events:
  follow: true
  json: true
  contract:                       # up to 5; use watchlist for more
    - CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
  # Filtering rules
  topic:                          # server-side, base64 ScVals with * and **
    - "AAAADwAAAAh0cmFuc2Zlcg==,*,*"
  topic-pattern:                  # client-side, over decoded topics
    - "transfer,*,**"
  filter-expr: 'value.amount > 1000000000'
  exclude-topic: [approve]
  type: [contract]
  dedup-window: 4096
  # Durable cursor
  state-file: /var/lib/prism/events.jsonl   # cursor journal
  resume: true
  # Sinks: Kafka, plus a signed webhook (secret from PRISM_WEBHOOK_SECRET)
  kafka-brokers: [kafka-1:9092, kafka-2:9092]
  kafka-topic: soroban-events
  kafka-partition-by: contract
  webhook-url: https://hooks.example.com/prism
  webhook-batch-size: 50
  webhook-timeout: 2s
  # Health endpoints for the orchestrator
  health-port: 8090
  max-lag: 10

transactions:
  follow: true
  status: [failed]
  json: true
  output-file: /var/log/prism/failed-txs.ndjson
  rotate-size: 104857600          # 100 MiB
  rotate-keep: 10
  rotate-compress: true

ledgers:
  page-size: 50
  compact: true

health:
  interval: 10s
  max-ledger-age: 30s
  fail-after: 3
```

Then `prism events` runs fully configured, and `prism events --limit 10
--follow=false` takes a quick look with the same filters.

## Environment variables

### The generic mapping

Every flag is read from `PRISM_` followed by the flag name in upper case,
with dashes turned into underscores:

| Flag | Variable |
|---|---|
| `--rpc-url` | `PRISM_RPC_URL` |
| `--page-size` | `PRISM_PAGE_SIZE` |
| `--sink-es-index` | `PRISM_SINK_ES_INDEX` |
| `--follow` | `PRISM_FOLLOW` (`true` / `false`) |

- An empty or all-whitespace value is ignored, as if the variable were unset.
- For repeatable flags, separate values with **newlines**, because several
  of them (topic filters, headers) legitimately contain commas. Flags of type
  `strings`, such as `--type` and `--contract`, also split on commas, just as
  on the command line.
- A variable applies to every command that has the flag. Export
  command-specific settings only in the environment of the process that
  needs them, or put them in a command section of a config file.

```sh
export PRISM_NETWORK=mainnet
export PRISM_RPC_URL=$'https://rpc-a.example.com\nhttps://rpc-b.example.com'
export PRISM_TOPIC=$'AAAADwAAAAh0cmFuc2Zlcg==,*,*\nAAAADwAAAARtaW50,*'
export PRISM_TYPE=contract,system
prism events --follow
```

In a container, the generic mapping makes a config file optional:

```yaml
# docker-compose.yml
services:
  prism:
    image: your-registry/prism:latest   # an image with the prism binary as its entrypoint
    command: ["events"]
    environment:
      PRISM_NETWORK: mainnet
      PRISM_RPC_URL: https://rpc.example.com
      PRISM_CONTRACT: CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
      PRISM_FOLLOW: "true"
      PRISM_JSON: "true"
      PRISM_STATE_FILE: /data/events.jsonl
      PRISM_RESUME: "true"
      PRISM_HEALTH_PORT: "8090"
```

### Other variables prism reads

| Variable | Used for |
|---|---|
| `SOROBAN_RPC_URL` | RPC endpoints, comma-separated, when no `--rpc-url` is configured anywhere else |
| `PRISM_HEADERS` | Extra RPC headers as `Key: Value` entries separated by newlines or commas; merged with `--header` |
| `PRISM_PG_DSN` | PostgreSQL DSN for durable cursors (same as the generic mapping of `--pg-dsn`) |
| `PRISM_WEBHOOK_SECRET` | HMAC-SHA256 secret for `X-Prism-Signature` (same as the generic mapping of `--webhook-secret`) |
| `PRISM_ES_API_KEY` | Elasticsearch API key, used when `--sink-es-api-key` is not set |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Credentials for the S3 archive (`--cloud-provider s3`) and AWS KMS state encryption |
| `AWS_REGION` | Region for AWS KMS when `--state-kms-region` is not set; the SNS/SQS sinks use the AWS SDK's standard credential chain |
| `GOOGLE_OAUTH_ACCESS_TOKEN` | OAuth token for the GCS archive, and for GCP KMS unless `--state-kms-token-env` names another variable |
| *(named by `--state-passphrase-env`)* | Passphrase that encrypts the state file |
| `NO_COLOR` | Disables colour when `--color` is `auto` |
| `PRISM_E2E` | Test suite only: `PRISM_E2E=1` enables the end-to-end tests |

## Key and variable index

Every config key, its environment variable, and the commands that accept
it. Types, defaults and descriptions are in the
[command reference](command_reference.md).

### Global keys

These work at the top level of the file for every command.

| Key | Environment variable | Accepted by |
|---|---|---|
| `color` | `PRISM_COLOR` | all commands |
| `config` | `PRISM_CONFIG` | all commands |
| `header` | `PRISM_HEADER` | all commands |
| `insecure-skip-network-check` | `PRISM_INSECURE_SKIP_NETWORK_CHECK` | all commands |
| `network` | `PRISM_NETWORK` | all commands |
| `pprof-port` | `PRISM_PPROF_PORT` | all commands |
| `rpc-timeout` | `PRISM_RPC_TIMEOUT` | all commands |
| `rpc-url` | `PRISM_RPC_URL` | all commands |
| `serve-grpc` | `PRISM_SERVE_GRPC` | all commands |
| `serve-sse` | `PRISM_SERVE_SSE` | all commands |
| `sse` | `PRISM_SSE` | all commands |
| `wire-log` | `PRISM_WIRE_LOG` | all commands |
| `wire-log-max-body` | `PRISM_WIRE_LOG_MAX_BODY` | all commands |
| `ws` | `PRISM_WS` | all commands |

### Shared streaming keys

Accepted by `events`, `serve`, `transactions`, `ledgers`, `operations`,
`storage`, `contracts`, `wasm`, `trades`, `fee-stats`, `simulate` and
`aggregate`.

| Key | Environment variable | Accepted by |
|---|---|---|
| `adaptive` | `PRISM_ADAPTIVE` | all streaming commands |
| `amqp-exchange` | `PRISM_AMQP_EXCHANGE` | all streaming commands |
| `amqp-queue` | `PRISM_AMQP_QUEUE` | all streaming commands |
| `amqp-routing-key` | `PRISM_AMQP_ROUTING_KEY` | all streaming commands |
| `amqp-url` | `PRISM_AMQP_URL` | all streaming commands |
| `backfill-workers` | `PRISM_BACKFILL_WORKERS` | all streaming commands |
| `batch` | `PRISM_BATCH` | all streaming commands |
| `batch-size` | `PRISM_BATCH_SIZE` | all streaming commands |
| `batch-timeout` | `PRISM_BATCH_TIMEOUT` | all streaming commands |
| `cloud-batch-bytes` | `PRISM_CLOUD_BATCH_BYTES` | all streaming commands |
| `cloud-batch-records` | `PRISM_CLOUD_BATCH_RECORDS` | all streaming commands |
| `cloud-batch-timeout` | `PRISM_CLOUD_BATCH_TIMEOUT` | all streaming commands |
| `cloud-bucket` | `PRISM_CLOUD_BUCKET` | all streaming commands |
| `cloud-compress` | `PRISM_CLOUD_COMPRESS` | all streaming commands |
| `cloud-prefix` | `PRISM_CLOUD_PREFIX` | all streaming commands |
| `cloud-provider` | `PRISM_CLOUD_PROVIDER` | all streaming commands |
| `cloud-region` | `PRISM_CLOUD_REGION` | all streaming commands |
| `cloud-url` | `PRISM_CLOUD_URL` | all streaming commands |
| `compact` | `PRISM_COMPACT` | all streaming commands |
| `csv-fields` | `PRISM_CSV_FIELDS` | all streaming commands |
| `dlq-file` | `PRISM_DLQ_FILE` | all streaming commands |
| `drain-timeout` | `PRISM_DRAIN_TIMEOUT` | all streaming commands |
| `dry-run` | `PRISM_DRY_RUN` | all streaming commands |
| `follow` | `PRISM_FOLLOW` | all streaming commands, `health` |
| `format` | `PRISM_FORMAT` | all streaming commands |
| `from` | `PRISM_FROM` | all streaming commands |
| `gap-strategy` | `PRISM_GAP_STRATEGY` | all streaming commands |
| `ha-heartbeat` | `PRISM_HA_HEARTBEAT` | all streaming commands |
| `ha-lease-file` | `PRISM_HA_LEASE_FILE` | all streaming commands |
| `ha-standby-poll` | `PRISM_HA_STANDBY_POLL` | all streaming commands |
| `ha-ttl` | `PRISM_HA_TTL` | all streaming commands |
| `health-port` | `PRISM_HEALTH_PORT` | all streaming commands |
| `heartbeat` | `PRISM_HEARTBEAT` | all streaming commands |
| `json` | `PRISM_JSON` | all streaming commands, `health`, `tx` |
| `kafka-brokers` | `PRISM_KAFKA_BROKERS` | all streaming commands |
| `kafka-partition-by` | `PRISM_KAFKA_PARTITION_BY` | all streaming commands |
| `kafka-topic` | `PRISM_KAFKA_TOPIC` | all streaming commands |
| `limit` | `PRISM_LIMIT` | all streaming commands |
| `max-gap-recoveries` | `PRISM_MAX_GAP_RECOVERIES` | all streaming commands |
| `max-lag` | `PRISM_MAX_LAG` | all streaming commands |
| `max-record-retries` | `PRISM_MAX_RECORD_RETRIES` | all streaming commands |
| `max-retries` | `PRISM_MAX_RETRIES` | all streaming commands |
| `metrics-addr` | `PRISM_METRICS_ADDR` | all streaming commands |
| `nats-jetstream` | `PRISM_NATS_JETSTREAM` | all streaming commands |
| `nats-subject` | `PRISM_NATS_SUBJECT` | all streaming commands |
| `output-file` | `PRISM_OUTPUT_FILE` | all streaming commands |
| `page-size` | `PRISM_PAGE_SIZE` | all streaming commands |
| `pg-cursor-key` | `PRISM_PG_CURSOR_KEY` | all streaming commands |
| `pg-dsn` | `PRISM_PG_DSN` | all streaming commands |
| `poll` | `PRISM_POLL` | all streaming commands |
| `prune-after` | `PRISM_PRUNE_AFTER` | all streaming commands |
| `prune-interval` | `PRISM_PRUNE_INTERVAL` | all streaming commands |
| `prune-keep` | `PRISM_PRUNE_KEEP` | all streaming commands |
| `prune-max-age` | `PRISM_PRUNE_MAX_AGE` | all streaming commands |
| `quiet` | `PRISM_QUIET` | all streaming commands |
| `raw` | `PRISM_RAW` | all streaming commands |
| `resume` | `PRISM_RESUME` | all streaming commands |
| `retry-backoff` | `PRISM_RETRY_BACKOFF` | all streaming commands |
| `retry-backoff-max` | `PRISM_RETRY_BACKOFF_MAX` | all streaming commands |
| `rewind` | `PRISM_REWIND` | all streaming commands |
| `rotate-compress` | `PRISM_ROTATE_COMPRESS` | all streaming commands |
| `rotate-interval` | `PRISM_ROTATE_INTERVAL` | all streaming commands |
| `rotate-keep` | `PRISM_ROTATE_KEEP` | all streaming commands |
| `rotate-size` | `PRISM_ROTATE_SIZE` | all streaming commands |
| `since` | `PRISM_SINCE` | all streaming commands |
| `sink-audit` | `PRISM_SINK_AUDIT` | all streaming commands |
| `sink-audit-checkpoint` | `PRISM_SINK_AUDIT_CHECKPOINT` | all streaming commands |
| `sink-audit-key` | `PRISM_SINK_AUDIT_KEY` | all streaming commands |
| `sink-es-api-key` | `PRISM_SINK_ES_API_KEY` | all streaming commands |
| `sink-es-batch-size` | `PRISM_SINK_ES_BATCH_SIZE` | all streaming commands |
| `sink-es-flush-interval` | `PRISM_SINK_ES_FLUSH_INTERVAL` | all streaming commands |
| `sink-es-index` | `PRISM_SINK_ES_INDEX` | all streaming commands |
| `sink-es-password` | `PRISM_SINK_ES_PASSWORD` | all streaming commands |
| `sink-es-url` | `PRISM_SINK_ES_URL` | all streaming commands |
| `sink-es-username` | `PRISM_SINK_ES_USERNAME` | all streaming commands |
| `sink-nats` | `PRISM_SINK_NATS` | all streaming commands |
| `sink-parquet-dir` | `PRISM_SINK_PARQUET_DIR` | all streaming commands |
| `sink-pubsub-topic` | `PRISM_SINK_PUBSUB_TOPIC` | all streaming commands |
| `sink-region` | `PRISM_SINK_REGION` | all streaming commands |
| `sink-sns-topic` | `PRISM_SINK_SNS_TOPIC` | all streaming commands |
| `sink-sqs-queue` | `PRISM_SINK_SQS_QUEUE` | all streaming commands |
| `sink-zmq` | `PRISM_SINK_ZMQ` | all streaming commands |
| `state-file` | `PRISM_STATE_FILE` | all streaming commands, `rewind` |
| `state-kms-key-id` | `PRISM_STATE_KMS_KEY_ID` | all streaming commands |
| `state-kms-provider` | `PRISM_STATE_KMS_PROVIDER` | all streaming commands |
| `state-kms-region` | `PRISM_STATE_KMS_REGION` | all streaming commands |
| `state-kms-token-env` | `PRISM_STATE_KMS_TOKEN_ENV` | all streaming commands |
| `state-passphrase-env` | `PRISM_STATE_PASSPHRASE_ENV` | all streaming commands |
| `state-passphrase-file` | `PRISM_STATE_PASSPHRASE_FILE` | all streaming commands |
| `stats` | `PRISM_STATS` | all streaming commands |
| `stats-bar` | `PRISM_STATS_BAR` | all streaming commands |
| `stream-id` | `PRISM_STREAM_ID` | all streaming commands, `rewind` |
| `syslog-address` | `PRISM_SYSLOG_ADDRESS` | all streaming commands |
| `syslog-app` | `PRISM_SYSLOG_APP` | all streaming commands |
| `syslog-facility` | `PRISM_SYSLOG_FACILITY` | all streaming commands |
| `syslog-format` | `PRISM_SYSLOG_FORMAT` | all streaming commands |
| `tsv` | `PRISM_TSV` | all streaming commands |
| `webhook-batch-size` | `PRISM_WEBHOOK_BATCH_SIZE` | all streaming commands |
| `webhook-max-retries` | `PRISM_WEBHOOK_MAX_RETRIES` | all streaming commands |
| `webhook-secret` | `PRISM_WEBHOOK_SECRET` | all streaming commands |
| `webhook-timeout` | `PRISM_WEBHOOK_TIMEOUT` | all streaming commands |
| `webhook-url` | `PRISM_WEBHOOK_URL` | all streaming commands |
| `zmq-topic` | `PRISM_ZMQ_TOPIC` | all streaming commands |

### Command-specific keys

Put these in the matching command section.

| Key | Environment variable | Accepted by |
|---|---|---|
| `arg` | `PRISM_ARG` | `abi` |
| `back` | `PRISM_BACK` | `rewind` |
| `change` | `PRISM_CHANGE` | `storage` |
| `consensus` | `PRISM_CONSENSUS` | `ledgers` |
| `aggregate` | `PRISM_AGGREGATE` | `events`, `serve` |
| `aggregate-interval` | `PRISM_AGGREGATE_INTERVAL` | `events`, `serve` |
| `contract` | `PRISM_CONTRACT` | `aggregate`, `events`, `generate-types`, `serve`, `storage` |
| `contracts-file` | `PRISM_CONTRACTS_FILE` | `events`, `serve` (alias for `watchlist`) |
| `dedup-window` | `PRISM_DEDUP_WINDOW` | `events`, `serve` |
| `detect-conflicts` | `PRISM_DETECT_CONFLICTS` | `transactions` |
| `entry` | `PRISM_ENTRY` | `storage` |
| `envelope-only` | `PRISM_ENVELOPE_ONLY` | `diff` |
| `exclude-contract` | `PRISM_EXCLUDE_CONTRACT` | `events`, `serve` |
| `exclude-topic` | `PRISM_EXCLUDE_TOPIC` | `events`, `serve` |
| `expected-validator` | `PRISM_EXPECTED_VALIDATOR` | `ledgers` |
| `fail-after` | `PRISM_FAIL_AFTER` | `health` |
| `fail-on-error` | `PRISM_FAIL_ON_ERROR` | `tx` |
| `fanout-dir` | `PRISM_FANOUT_DIR` | `events`, `serve` |
| `fee-account` | `PRISM_FEE_ACCOUNT` | `transactions` |
| `fee-bump` | `PRISM_FEE_BUMP` | `transactions` |
| `file` | `PRISM_FILE` | `audit`, `audit-verify` |
| `filter-expr` | `PRISM_FILTER_EXPR` | `events`, `serve` |
| `function` | `PRISM_FUNCTION` | `abi` |
| `health-timeout` | `PRISM_HEALTH_TIMEOUT` | `health` |
| `include-failed` | `PRISM_INCLUDE_FAILED` | `wasm` |
| `interval` | `PRISM_INTERVAL` | `health` |
| `max-ledger-age` | `PRISM_MAX_LEDGER_AGE` | `health` |
| `max-rate` | `PRISM_MAX_RATE` | `events`, `serve` |
| `max-restarts` | `PRISM_MAX_RESTARTS` | `serve` |
| `memo` | `PRISM_MEMO` | `transactions` |
| `meta-only` | `PRISM_META_ONLY` | `diff` |
| `metric` | `PRISM_METRIC` | `aggregate` |
| `network-passphrase` | `PRISM_NETWORK_PASSPHRASE` | `contracts`, `wasm` |
| `out` | `PRISM_OUT` | `generate-types` |
| `pair` | `PRISM_PAIR` | `trades` |
| `print-public-key` | `PRISM_PRINT_PUBLIC_KEY` | `audit-verify` |
| `public-key` | `PRISM_PUBLIC_KEY` | `audit-verify` |
| `sample-rate` | `PRISM_SAMPLE_RATE` | `events`, `serve` |
| `source-account` | `PRISM_SOURCE_ACCOUNT` | `operations` |
| `status` | `PRISM_STATUS` | `transactions` |
| `to-ledger` | `PRISM_TO_LEDGER` | `rewind` |
| `topic` | `PRISM_TOPIC` | `aggregate`, `events`, `serve` |
| `topic-pattern` | `PRISM_TOPIC_PATTERN` | `events`, `serve` |
| `topic-regex` | `PRISM_TOPIC_REGEX` | `events`, `serve` |
| `trapped` | `PRISM_TRAPPED` | `transactions` |
| `tx-envelope` | `PRISM_TX_ENVELOPE` | `simulate` |
| `type` | `PRISM_TYPE` | `aggregate`, `events`, `operations`, `serve` |
| `verbose` | `PRISM_VERBOSE` | `tx` |
| `watch` | `PRISM_WATCH` | `contracts` |
| `watchlist` | `PRISM_WATCHLIST` | `events`, `serve` |
| `window` | `PRISM_WINDOW` | `aggregate`, `events`, `serve` |

## Validation and troubleshooting

- **Bad values fail fast and name their source.** For example:

  ```text
  prism: config key "page-size": invalid value "lots" for --page-size: ...
  prism: $PRISM_RPC_TIMEOUT: invalid value "soon" for --rpc-timeout: ...
  ```

- **A config file that exists but cannot be parsed is an error**, never
  silently skipped. A missing default file is fine; a missing `--config` file
  is an error.
- **A list given for a single-value flag is rejected** (`--page-size takes a
  single value, got 2`), and so is a mapping where a value was expected.
- **Unknown keys are ignored.** A misspelt key (`pagesize:`) has no effect,
  so if a setting does not seem to apply, compare its spelling with the flag
  in `prism <command> --help`.
- **Something set that you did not expect?** Check, in order, the command
  line, `env | grep '^PRISM_'`, `./.prism.yaml`, and the per-user file.
  Running with `--config /dev/null` (`--config NUL` on Windows) isolates the
  environment and the command line from both config files.

## Security notes

- Prefer environment variables, or files outside the repository, for secrets
  such as `webhook-secret`, `sink-es-api-key`, `sink-es-password`,
  `amqp-url` with credentials, and `pg-dsn`. A `.prism.yaml` is usually
  checked in alongside a project.
- Put RPC credentials in `PRISM_HEADERS` rather than in `header:` keys.
  The `--wire-log` debug log redacts the `Authorization` header, cookies
  and credential-bearing query values, but a config file is not redacted.
- Restrict permissions on any file that holds secrets (`chmod 600`).
