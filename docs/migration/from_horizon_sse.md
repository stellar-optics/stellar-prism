# Migrating from Horizon SSE to Soroban RPC with `stellar-prism`

Many Stellar indexers and backend services were built around Horizon's Server-Sent Events (SSE) endpoints (`/ledgers`, `/transactions`, `/payments`, `/operations`). As Stellar workloads shift to Soroban smart contracts, contract events, footprints, and state mutations are served by **Soroban RPC** rather than Horizon SSE.

This guide compares Horizon SSE with `stellar-prism`, shows side-by-side replacements for common `EventSource` listeners, and explains how to bridge classic Horizon streams when migrating incrementally.

---

## Feature Comparison Matrix

| Capability | Horizon SSE (`/ledgers`, `/transactions`, `/payments`) | `stellar-prism` (Soroban RPC) |
|---|---|---|
| **Transport & delivery model** | Server-pushed HTTP `text/event-stream` (`EventSource`). | Managed cursor polling (`stream.Tailer`) with fixed (`--poll`) or self-tuning (`--adaptive`, 1s–30s) intervals and retry without cursor advancement. |
| **Smart contract event support** | None — Horizon does not stream decoded Soroban contract events. | Native `prism events` with server-side `--contract` and `--topic` filters, client-side `--topic-pattern` / `--filter-expr`, and `stellar-xdr-lens` decoding. |
| **Transaction & operation streams** | Classic ledger, transaction, payment, and operation JSON resources. | `prism ledgers`, `prism transactions` (with `--status`, `--trapped`, `--fee-bump`, `--detect-conflicts`), `prism operations`, `prism trades`, `prism storage`, `prism contracts`. |
| **Multi-sink fanout** | Single HTTP response per consumer; caller must implement fanout. | Built-in sinks and broadcasters: stdout NDJSON, `--fanout-dir` per-contract files, `--kafka-brokers`, `--amqp-url`, `--nats-url`, `--timescale-dsn`, `--parquet-dir`, `--webhook-url`, `--serve-sse`, `--ws`. |
| **Cursor continuity & replay** | Caller persists `paging_token` and reconnects via `Last-Event-ID` / `?cursor=`. | Durable `--state-file` journal with `--resume` and background pruning, `--pg-dsn` PostgreSQL cursor commits, `--rewind` reprocessing, and `--dedup-window` ring buffer. |
| **Retention & gap handling** | Long-term history on full Horizon nodes; SSE reconnects silently if caller misses events. | Soroban RPC retains ~24 hours of history. `prism` explicitly detects retention gaps and exits `2` (`--gap-strategy fail`) or reports skipped ranges (`--gap-strategy resume`). |
| **Offline testing & deterministic replay** | Requires live Horizon instance or custom HTTP mocks. | Offline deterministic test suite via `internal/fakerpc` / `pkg/prismtest` and reproducible `--from` / `--limit` CLI runs. |

### Endpoint Mapping

| Horizon SSE Endpoint | `stellar-prism` Replacement |
|---|---|
| `GET /ledgers?cursor=now` | `prism ledgers --follow --json` |
| `GET /transactions?cursor=now` | `prism transactions --follow --json` |
| `GET /payments?cursor=now` | `prism operations --type payment --follow --json` (or `prism trades --follow --json` for path-payment trades) |
| `GET /operations?cursor=now` | `prism operations --follow --json` |
| *(No Horizon equivalent)* | `prism events --contract CDLZF... --follow --json` |

---

## Side-by-Side Migration Examples

### 1. Replacing an `EventSource` Listener with NDJSON Pipelines

**Before (Node.js `EventSource` listening to Horizon `/transactions`):**

```javascript
const EventSource = require("eventsource");

const url = "https://horizon-testnet.stellar.org/transactions?cursor=now";
const es = new EventSource(url);

es.onmessage = (msg) => {
  const tx = JSON.parse(msg.data);
  if (!tx.successful) {
    console.error("Failed tx:", tx.hash);
    return;
  }
  console.log("Tx:", tx.hash, "ledger:", tx.ledger);
};

es.onerror = (err) => {
  console.error("SSE error, manual reconnect/backoff needed:", err);
};
```

**After (`prism transactions` streaming NDJSON with durable resume and gap detection):**

```sh
# Stream transactions with managed polling, durable cursor state, and NDJSON output
prism transactions \
  --follow \
  --json \
  --state-file ~/.cache/prism/tx-cursor.jsonl \
  --resume \
  | jq -c '{id, ledger, hash, successful, feeAccount, failure}'
```

For Soroban contract events specifically (which Horizon SSE cannot provide):

```sh
prism events \
  --contract CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC \
  --follow \
  --adaptive \
  --dedup-window 4096 \
  --state-file ~/.cache/prism/events-cursor.jsonl \
  --resume \
  --json \
  | your-indexer-binary
```

If downstream consumers still expect an SSE or WebSocket endpoint rather than stdin NDJSON, `prism` can re-broadcast filtered Soroban streams locally with `--serve-sse` (preserving `Last-Event-ID` replay over the last 1,024 records) or `--ws`:

```sh
prism events --contract CDLZF... --follow --serve-sse 127.0.0.1:8080 --ws 127.0.0.1:8081
```

### 2. Publishing Directly to Kafka Instead of Writing Glue Code

**Before:** Application code consumes Horizon SSE, parses JSON, tracks the `paging_token` in a database, handles retries/reconnects, and produces messages into Kafka.

**After (`prism` streams directly to Kafka with per-contract or per-tx partitioning):**

```sh
# Stream Soroban contract events straight into Kafka, keyed by contract ID
prism events \
  --contract CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC \
  --follow \
  --state-file /var/lib/prism/events.state \
  --resume \
  --kafka-brokers broker-1:9092,broker-2:9092 \
  --kafka-topic soroban.contract.events \
  --kafka-partition-by contract

# Stream transactions into Kafka, keyed by transaction hash
prism transactions \
  --follow \
  --state-file /var/lib/prism/tx.state \
  --resume \
  --kafka-brokers broker-1:9092 \
  --kafka-topic stellar.transactions \
  --kafka-partition-by tx
```

Use `--ha-lease-file /shared/nfs/prism.lock` for active-passive failover across two `prism` instances without duplicating downstream Kafka messages.

### 3. Replacing `/payments` Listeners

For classic payment operations (`payment`, `create_account`, `path_payment_strict_receive`, `path_payment_strict_send`):

```sh
# Stream classic payment operations from Soroban RPC transaction envelopes
prism operations \
  --type payment \
  --type path_payment_strict_receive \
  --type path_payment_strict_send \
  --follow \
  --json
```

For normalized classic path payments and Soroban DEX swaps:

```sh
prism trades --pair XLM/USDC --follow --json
```

---

## Bridging Legacy Horizon Streams (`HorizonSSESource`)

During a phased migration—or when your pipeline still needs classic Horizon SSE endpoints alongside Soroban RPC—`stellar-prism` provides a built-in library source [`HorizonSSESource`](../../pkg/stream/horizon_sse.go) in package `stream`:

- Implements `stream.Source` (`Name() == "horizon-sse"`) and `stream.Rewinder` (`Rewind(ledger)`).
- Connects to a Horizon SSE stream using `SSEDecoder`, parses `ledger-sequence` cursors (`CursorLedger`), and normalizes incoming events into `stream.Record` (`KindTransaction`) so they plug into `stream.Tailer` and every `pkg/sink` implementation (NDJSON, Kafka, TimescaleDB, Parquet, webhooks).
- Supports combining classic Horizon streams and Soroban RPC event streams inside a single ordered pipeline via [`stream.NewMultiplexSource`](../../pkg/stream/multiplex.go).

### Go Library Example

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func main() {
	ctx := context.Background()

	// 1. Legacy Horizon SSE bridge for classic payment/operation events
	horizonSrc := stream.NewHorizonSSESource(
		&http.Client{Timeout: 60 * time.Second},
		stream.HorizonSSEQuery{
			HorizonURL:     "https://horizon-testnet.stellar.org",
			EventTypes:     []string{"payment", "operation"},
			StartLedger:    3966000,
			PageLimit:      50,
			ReconnectAfter: 30 * time.Second,
		},
	)

	// 2. Soroban RPC event source for smart contract events
	rpcClient := rpc.New(rpc.DefaultURL, 30*time.Second)
	eventSrc, err := stream.NewEventSource(rpcClient, stream.EventQuery{
		ContractIDs: []string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
		StartLedger: 3966000,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 3. Merge both streams by Record.ID and emit to any prism sink
	merged := stream.NewMultiplexSource(horizonSrc, eventSrc)
	defer merged.Close()

	tailer := stream.NewTailer(merged, sink.NewNDJSON(os.Stdout, true), stream.Options{
		Follow: true,
	})
	if err := tailer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

### Migration Rollout Checklist

1. **Keep historical backfills on Horizon or archives:** Soroban RPC retains roughly 24 hours of ledgers; use Horizon or Stellar archive stores for older lookups and start `prism --from <recent_ledger>` or `--since 1h` for live tailing.
2. **Adopt `prism` cursors and gap policies early:** Enable `--state-file` + `--resume` and decide whether retention overruns should halt (`--gap-strategy fail`, exit code `2`) or skip forward (`--gap-strategy resume`).
3. **Move consumers to `Record.ID` idempotency:** Key downstream database writes or queue deduplication on `id` so restarts never duplicate records.
4. **Retire the Horizon bridge once classic listeners are migrated:** Replace remaining `/transactions` and `/payments` consumers with `prism transactions`, `prism operations`, and `prism events`.
