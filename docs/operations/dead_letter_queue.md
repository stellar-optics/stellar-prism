# Dead Letter Queue operations

A long-running stream should not stop because one record is malformed or one
sink write fails temporarily. With `--dlq-file`, `prism` captures those failures
as NDJSON lines and keeps streaming. The implementation is
[`pkg/sink/dlq.go`](../../pkg/sink/dlq.go); the CLI wraps the normal sink chain
in it. See also the [CLI reference](../cli/command_reference.md) and
[troubleshooting streaming gaps](../troubleshooting/streaming_gaps_and_lag.md).

- [How the DLQ works](#how-the-dlq-works)
- [Configuration](#configuration)
- [Dead letter file format](#dead-letter-file-format)
- [Monitoring](#monitoring)
- [Replaying dead letters](#replaying-dead-letters)
- [Limits and caveats](#limits-and-caveats)

---

## How the DLQ works

The DLQ wraps the configured primary sink. For each record it:

1. Runs structural validation. An empty `ID`, an unknown `Kind`, or a nil
   payload for a known kind is written to the queue with `stage: "validation"`
   and never reaches the primary sink.
2. Otherwise calls `primary.Emit` up to `1 + --max-record-retries` times. An
   error on the last attempt is written with `stage: "delivery"`.
3. On `Flush`, records a primary flush failure with `stage: "flush"`; that entry
   has no `record` because the failure is not attributable to one record.

A successfully written dead letter is **not** returned as an error, so the
stream (and its cursor) continues. Only a failure to write the dead letter
itself stops the stream, because otherwise the record would be lost without a
trace.

There is no retryable/permanent error classifier and no backoff between DLQ
retries: they are immediate. Do not confuse `--max-record-retries` with
`--retry-backoff` / `--retry-backoff-max`, which control RPC fetch retries.

---

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--dlq-file <path>` | *(disabled)* | Append dead letters to this file as NDJSON; opened with mode `0644` |
| `--max-record-retries <n>` | `0` | Extra `primary.Emit` attempts before quarantine; `0` means one attempt total |
| `--output-file <path>` | *(stdout)* | Primary output; must be a different path from `--dlq-file` |
| `--rotate-size`, `--rotate-interval`, `--rotate-keep`, `--rotate-compress` | `0` / `0` / `0` / `false` | Rotate the **primary output file**, not the DLQ |

```sh
prism events \
  --follow \
  --network mainnet \
  --contract CDLZF...CYSC \
  --json \
  --output-file /var/lib/prism/events.ndjson \
  --dlq-file /var/lib/prism/events.dlq.ndjson \
  --max-record-retries 3 \
  --state-file /var/lib/prism/events.state \
  --resume
```

Validation rules:

- `--max-record-retries` must not be negative and requires `--dlq-file`.
- `--dlq-file` must differ from `--output-file` and from `--sink-audit`.
- The same flags are registered on the other streaming commands (`ledgers`,
  `transactions`, `operations`, `storage`, `wasm`, `trades`, `fee-stats`, ...).
- Environment equivalents follow the generic mapping: `PRISM_DLQ_FILE` and
  `PRISM_MAX_RECORD_RETRIES`.

> [!NOTE]
> `--dlq-max-retries` is **not** a flag in this version. Use
> `--max-record-retries`. There is no configurable DLQ backoff schedule and no
> per-error retry classification.

Every captured failure prints one diagnostic line to stderr:

```text
prism: dead-lettered record 0000000000005400-0000000003 (delivery): webhook: 503 Service Unavailable
```

---

## Dead letter file format

The file is NDJSON: one JSON object per line, appended and flushed immediately.
HTML escaping is disabled, and `failedAt` is UTC RFC 3339 with nanoseconds.

| Key | Type | Meaning |
|---|---|---|
| `kind` | string | Always `"dlq"` |
| `failedAt` | string | Time the failure was captured; omitted when empty |
| `stage` | string | `"validation"`, `"delivery"`, or `"flush"` |
| `error` | string | `err.Error()` from the validation or sink failure |
| `record` | object | The original `stream.Record`; omitted for `stage: "flush"` |

```json
{
  "kind": "dlq",
  "failedAt": "2026-11-25T14:35:13.123456789Z",
  "stage": "delivery",
  "error": "webhook: 503 Service Unavailable",
  "record": {
    "ID": "0000000000005400-0000000003",
    "Sequence": 54000,
    "Time": "2026-11-25T14:35:13Z",
    "Kind": "event",
    "Event": {
      "Type": "contract",
      "ContractID": "CABC...",
      "TxHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "TxIndex": 1,
      "OpIndex": 0,
      "EventIndex": 0,
      "TopicXDR": ["AAAAAQAAAAA="],
      "ValueXDR": "AAAAAQAAAAA=",
      "CallDepth": 0,
      "ParentContractID": "",
      "InvocationPath": null
    },
    "Close": null,
    "Transaction": null,
    "StorageChange": null,
    "WASMDeployment": null,
    "FeeStats": null,
    "Trade": null,
    "Operation": null,
    "Simulation": null
  }
}
```

> [!NOTE]
> The nested record uses Go field names (`ID`, `Sequence`, `Event`, ...) because
> `stream.Record` has no JSON tags. The DLQ wrapper itself uses lowerCamel keys
> (`failedAt`, `kind`, `stage`, `error`, `record`). There is no target-sink-name
> field; for a multi-sink failure the sink index appears only inside `error`.

A flush entry looks like this:

```json
{"kind":"dlq","failedAt":"2026-11-25T14:35:14Z","stage":"flush","error":"audit: checksum mismatch"}
```

The queue is append-only and is flushed after every write, but `dlq.go` does
not call `File.Sync()`. For durable storage, use a filesystem with normal
write-back guarantees and monitor the file size.

---

## Monitoring

Dead letters are not exposed as Prometheus counters or as a CLI summary. Use
the NDJSON file itself:

```sh
# Count failures by stage
jq -r '.stage' /var/lib/prism/events.dlq.ndjson | sort | uniq -c

# Show the most recent failures
jq -c 'select(.kind == "dlq")' /var/lib/prism/events.dlq.ndjson | tail -20

# Alert on delivery failures after a sink was fixed
jq -e 'select(.stage == "delivery" and .failedAt > "2026-11-25T14:40:00Z")' \
  /var/lib/prism/events.dlq.ndjson
```

A monotonically growing `validation` count means a source or filter bug; a
growing `delivery` count means the downstream sink is still failing. Compare
record IDs against the primary output to see whether the same records were
later delivered.

The Go API exposes `DLQ.Stats()` with `Delivered`, `Malformed`, `Failed`, and
`FlushFailures` counters as a snapshot; the CLI does not print them.

---

## Replaying dead letters

The `record` object preserves the original record so it can be re-emitted after
the downstream problem is fixed. There is **no `prism dlq replay` command** in
this version, and `prism replay` reads Stellar history archives, not DLQ files.
A naive `jq -c '.record' file | prism ...` pipe is not supported: stream
commands do not read records from stdin.

Use one of the following two procedures.

### Option 1: re-stream the source ledger range with `prism`

If the failures came from a known archive backfill, re-run the same sink over
the ledger range and deduplicate downstream by record `ID`:

```sh
prism replay \
  --archive-url https://history.stellar.org/prd/core-live/core_live_001/ \
  --start-ledger 54000 \
  --end-ledger 54099 \
  --network mainnet \
  --json \
  --sink-parquet-dir /var/lib/prism/events-parquet
```

This re-emits the whole range, including records that were delivered
successfully the first time. It is simple and uses only built-in flags, but it
requires idempotent sinks or downstream deduplication.

### Option 2: selectively re-emit the saved records

For precise replay, decode `sink.DLQEntry` and hand each non-nil `Record` to a
`stream.Sink`. The following helper writes the records as NDJSON; replace `out`
with the same sink type used by the original run to deliver directly:

```go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

func main() {
	in, err := os.Open("/var/lib/prism/events.dlq.ndjson")
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	out := sink.NewNDJSON(os.Stdout, true)
	defer func() { _ = out.Flush() }()

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	for scanner.Scan() {
		var entry sink.DLQEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			log.Fatalf("bad dead letter: %v", err)
		}
		if entry.Record == nil {
			continue // flush failure, no record to replay
		}
		if err := out.Emit(*entry.Record); err != nil {
			log.Fatalf("replay %s: %v", entry.Record.ID, err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintln(os.Stderr, "replay complete")
}
```

Run it only after the downstream service is healthy, keep the original DLQ file
as the audit trail, and verify that the re-emitted IDs are now present in the
primary sink. Replaying into the same sink that is still failing simply creates
new dead letters.

---

## Limits and caveats

- **No automatic redelivery.** A dead letter is an audit record. Nothing in
  `prism` watches the DLQ and retries it.
- **No rotation.** `--rotate-*` applies to `--output-file` only. Stop the stream
  before archiving a DLQ file; rotating it underneath an open append handle can
  lose entries.
- **No backoff.** Delivery retries are immediate; a slow downstream service
  receives `1 + N` rapid attempts.
- **Structural validation only.** An XDR payload that will not decode is
  handled by the individual sink, not by DLQ validation.
- **Coverage gaps.** `fee_stats`, `trade`, `operation`, and `simulation` records
  are rejected by `ValidateRecord` as unknown kinds and are dead-lettered even
  with `--dlq-file`, so do not rely on the DLQ for those streams today.
- **Aggregation paths.** `events --aggregate` and `prism aggregate` use their
  own output paths and do not construct a DLQ even though they accept the
  flags.
- **Cursor advance.** Because a captured failure returns nil to the stream, a
  durable cursor can advance past a dead letter. Preserve the file and replay it
  explicitly.
