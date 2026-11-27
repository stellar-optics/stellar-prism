# Parquet and cloud object storage sinks

Analytics engines read columnar Parquet far faster than NDJSON, especially when
files are partitioned by date and ledger. `stellar-prism` can write local
Parquet part files through an embedded DuckDB instance, and can stream
compressed NDJSON chunks directly to S3-compatible or Google Cloud Storage
buckets.

This guide describes the behavior implemented in
[`pkg/sink/parquet.go`](../../pkg/sink/parquet.go),
[`pkg/sink/cloud_storage.go`](../../pkg/sink/cloud_storage.go),
[`pkg/sink/s3.go`](../../pkg/sink/s3.go), and
[`pkg/sink/gcs.go`](../../pkg/sink/gcs.go). Related material lives in the
[architecture overview](../architecture.md) and the
[CLI reference](../cli/command_reference.md).

- [Flag names: proposed vs. implemented](#flag-names-proposed-vs-implemented)
- [Local Parquet sink](#local-parquet-sink)
- [Schema and record coverage](#schema-and-record-coverage)
- [Building a partitioned Parquet dataset](#building-a-partitioned-parquet-dataset)
- [Cloud object storage](#cloud-object-storage)
- [Querying the data](#querying-the-data)
- [Operational notes](#operational-notes)

---

## Flag names: proposed vs. implemented

Older issue descriptions and examples sometimes use provider-specific flag
names that do not exist in this code. Use the names below.

| Proposed name | Actual CLI name | Notes |
|---|---|---|
| `--parquet-dir` | `--parquet-dir` or `--sink-parquet-dir` | Both names write the same variable; `--sink-parquet-dir` is canonical |
| `--s3-bucket`, `--gcs-bucket` | `--cloud-bucket` | One flag for either provider |
| `--s3-prefix`, `--gcs-prefix` | `--cloud-prefix` | Default `prism` |
| `--s3-region` | `--cloud-region` | S3 only; GCS ignores it |
| `--s3-endpoint`, `--gcs-endpoint` | `--cloud-url` | Required for S3; optional for GCS |
| `SNAPPY` / `GZIP` codecs | `--parquet-codec snappy\|zstd\|gzip\|uncompressed` | Values are lower-cased before validation |

---

## Local Parquet sink

| Flag | Default | Description |
|---|---|---|
| `--sink-parquet-dir <dir>` / `--parquet-dir <dir>` | *(disabled)* | Directory for part files; created with mode `0750` |
| `--parquet-codec <codec>` | `snappy` | `snappy`, `zstd`, `gzip`, or `uncompressed` |
| `--parquet-row-group-size <bytes>` | `134217728` (128 MiB) | Passed to DuckDB as `ROW_GROUP_SIZE_BYTES` |

```sh
prism events \
  --follow \
  --network mainnet \
  --contract CDLZF...CYSC \
  --sink-parquet-dir /var/lib/prism/events-parquet \
  --parquet-codec zstd \
  --parquet-row-group-size 67108864
```

Each flush writes one part file named `<prefix>-NNNNNN.parquet`, by default
`part-000001.parquet`, `part-000002.parquet`, and so on. A flush is triggered by
5000 buffered rows, by a 5-second timer, or explicitly at shutdown; one COPY is
bounded by 30 seconds. These four values are `ParquetConfig` defaults
(`BatchSize`, `FlushInterval`, `FlushTimeout`, `Prefix`) and have **no CLI
flags**.

Writes go to `part-NNNNNN.parquet.tmp` and are atomically renamed, so an
interrupted COPY cannot leave a trusted truncated part. On restart, the sink
scans existing part files and continues numbering instead of overwriting them.
The directory layout is flat: the CLI Parquet sink does **not** emit
`year=.../month=.../ledger_....parquet` paths. See
[Building a partitioned Parquet dataset](#building-a-partitioned-parquet-dataset)
for the supported way to create one.

> [!NOTE]
> `--raw` does not change Parquet output. XDR columns always remain base64
> strings. Use the decoded `--json` output or the lens formatter in a custom
> sink if you need decoded contract values.

---

## Schema and record coverage

All records share one 17-column union table (`prism_records`), not separate
per-kind tables:

| Column | Type | Populated for |
|---|---|---|
| `id` | `VARCHAR NOT NULL` | All records |
| `ledger` | `UBIGINT NOT NULL` | All records |
| `kind` | `VARCHAR NOT NULL` | All records |
| `closed_at` | `TIMESTAMP` | All records; `NULL` when the record time is zero |
| `hash` | `VARCHAR` | Ledger |
| `contract_id` | `VARCHAR` | Event |
| `tx_hash` | `VARCHAR` | Event, transaction |
| `tx_index` | `UINTEGER` | Event, transaction |
| `op_index` | `UINTEGER` | Event |
| `successful` | `BOOLEAN` | Transaction |
| `topics_xdr` | `VARCHAR[]` | Event; `NULL` when there are no topics |
| `value_xdr` | `VARCHAR` | Event |
| `envelope_xdr` | `VARCHAR` | Transaction |
| `result_xdr` | `VARCHAR` | Transaction |
| `result_meta_xdr` | `VARCHAR` | Transaction |
| `header_xdr` | `VARCHAR` | Ledger |
| `metadata_xdr` | `VARCHAR` | Ledger |

Rows are ordered by `id` inside each part file. Empty strings are stored as
`NULL`, so a `WHERE column IS NOT NULL` filter is reliable.

| Record kind | Parquet support |
|---|---|
| `event` | Yes — contract, transaction, topic and value columns |
| `transaction` | Yes — hash, index, success and XDR columns |
| `ledger` | Yes — hash, header and metadata XDR |
| `operation` | **No** — fails with `parquet: unknown record kind "operation"` |
| `storage_change`, `wasm_deployment`, `fee_stats`, `trade`, `simulation` | **No** — same error |

> [!WARNING]
> `prism operations --sink-parquet-dir ...` cannot write records today. Archive
> operation records as NDJSON with the [cloud sink](#cloud-object-storage), or
> use `--output-file`. If you write your own Go caller, `ParquetConfig` has no
> schema hook; the mapping lives in `parquetRow`.

The unused `ParquetSink` type in the same Go file writes a five-column
`id / kind / ledger / time / payload` row under `dt=YYYY-MM-DD/records.parquet`,
but it is exercised only by tests and is **not** reachable from the CLI. Do not
base operational plans on that layout.

---

## Building a partitioned Parquet dataset

The issue's example path
`events/year=2026/month=11/day=23/ledger_54000.parquet` is not emitted by
`prism`. To get a Hive-partitioned dataset, run a one-off DuckDB conversion
over the part files:

```sql
COPY (
  SELECT
    * EXCLUDE (ledger),
    ledger AS ledger_id,
    year(closed_at)  AS year,
    month(closed_at) AS month,
    day(closed_at)   AS day
  FROM read_parquet('/var/lib/prism/events-parquet/*.parquet')
  WHERE kind = 'event'
) TO '/var/lib/prism/analytics/events/' (
  FORMAT PARQUET,
  PARTITION_BY (year, month, day, ledger_id),
  COMPRESSION SNAPPY
);
```

This produces directories such as:

```text
events/year=2026/month=11/day=23/ledger_id=54000/part-0.parquet
```

DuckDB chooses the file names inside each partition; `prism` does not produce
per-ledger filenames. If you need `ledger_54000.parquet` exactly, add a
post-processing rename step in your own pipeline.

For a continuous setup, run the conversion on a schedule over a fixed time
window (for example, every hour for the previous hour) and keep the raw part
files as the source of truth.

---

## Cloud object storage

The CLI exposes one provider-neutral set of flags:

| Flag | Default | Description |
|---|---|---|
| `--cloud-provider s3\|gcs` | `s3` | Destination provider |
| `--cloud-url <url>` | *(GCS: `https://storage.googleapis.com`)* | S3 endpoint; required for S3, optional for GCS |
| `--cloud-bucket <name>` | *(required)* | Destination bucket |
| `--cloud-prefix <prefix>` | `prism` | Key prefix; leading/trailing slashes are trimmed |
| `--cloud-region <region>` | `us-east-1` applied by S3 | S3 signing region; ignored by GCS |
| `--cloud-batch-records <n>` | `1000` | Flush a chunk after this many records |
| `--cloud-batch-bytes <n>` | `8388608` (8 MiB) | Flush before a chunk exceeds this size |
| `--cloud-batch-timeout <duration>` | `30s` | Flush a partial chunk after this long; `0` disables the timer |
| `--cloud-compress` | `true` | Gzip chunks and name them `.ndjson.gz` |

### S3

S3 uploads require a static access key and secret from the environment; there
is no credential flag and no SDK credential chain for this sink:

```sh
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=... \
prism events \
  --follow \
  --network mainnet \
  --cloud-provider s3 \
  --cloud-url https://s3.us-east-1.amazonaws.com \
  --cloud-bucket prism-archive \
  --cloud-prefix events \
  --cloud-region us-east-1 \
  --cloud-compress
```

`S3Store` signs `PUT` requests with SigV4 using path-style keys by default.
`VirtualHosted` is a Go option only. There are no ACL, storage-class, SSE/KMS,
multipart, or checksum flags; configure encryption and lifecycle rules on the
bucket itself.

### GCS

GCS uses a bearer token from `GOOGLE_OAUTH_ACCESS_TOKEN` and the JSON API's
simple media upload. There is no project, service-account JSON, or resumable
upload option:

```sh
GOOGLE_OAUTH_ACCESS_TOKEN=... \
prism events \
  --follow \
  --network testnet \
  --cloud-provider gcs \
  --cloud-bucket prism-archive \
  --cloud-prefix events \
  --cloud-compress
```

A chunk is uploaded as one NDJSON body. Failures keep the chunk buffered and
retry it (three attempts by default, 100 ms to 5 s backoff), so delivery is
at-least-once. The key is derived from the chunk's first and last IDs and
ledger range, so a retry overwrites the same object instead of creating a
duplicate.

Effective key layout:

```text
<prefix>/<YYYY>/<MM>/<DD>/ledger-<first>-<last>/<firstID>-<lastID>.ndjson.gz
```

For example:

```text
events/2026/11/23/ledger-0000054000-0000054999/0000054000-0000000001-0000054999-0000000127.ndjson.gz
```

Cloud chunks are **NDJSON, not Parquet**, and can contain any supported record
kind. Use the local Parquet sink for columnar analytics and the cloud sink for
durable raw archives.

---

## Querying the data

### DuckDB over raw Parquet parts

```sql
SELECT
  id,
  ledger,
  contract_id,
  topics_xdr,
  value_xdr
FROM read_parquet('/var/lib/prism/events-parquet/*.parquet')
WHERE kind = 'event'
  AND ledger BETWEEN 54000 AND 54099
ORDER BY id;
```

### DuckDB over the partitioned dataset

```sql
SELECT id, ledger_id, contract_id, value_xdr
FROM read_parquet('/var/lib/prism/analytics/events/**/*.parquet', hive_partitioning = true)
WHERE year = 2026 AND month = 11 AND day = 23
  AND ledger_id = 54000;
```

### Amazon Athena

After placing the partitioned Parquet dataset in S3, register the partitions:

```sql
CREATE EXTERNAL TABLE prism_events (
  id string,
  kind string,
  closed_at timestamp,
  contract_id string,
  topics_xdr array<string>,
  value_xdr string
)
PARTITIONED BY (year int, month int, day int, ledger_id int)
STORED AS PARQUET
LOCATION 's3://prism-archive/analytics/events/';

MSCK REPAIR TABLE prism_events;

SELECT id, contract_id, value_xdr
FROM prism_events
WHERE year = 2026 AND month = 11 AND ledger_id = 54000;
```

BigQuery and Databricks can consume the same partitioned Parquet layout with
their own external-table definitions.

---

## Operational notes

- Both sinks can be combined with any stream command that registers shared
  sink flags; `prism replay` registers the sink flags too.
- `--cloud-provider gcs` without `--cloud-bucket`, or
  `--cloud-provider s3` without `--cloud-url` and `--cloud-bucket`, is rejected
  at startup.
- There is no built-in object-store rotation beyond chunk size/count/time;
  configure bucket lifecycle policies for retention.
- The Parquet sink creates directories but does not manage disk usage. Monitor
  the part directory and archive or remove old parts after a successful
  conversion.
- For failures that must not stop a long-running stream, configure the
  [dead letter queue](../operations/dead_letter_queue.md).
