# gRPC Streaming API and Protobuf Client Integration

`prism serve` can expose its live record stream as a **gRPC server-streaming
service** over HTTP/2, next to (or in place of) its SSE and WebSocket
endpoints. Downstream services in Go, Python, Rust, Java or any other language
with gRPC support can subscribe with a generated client and get records as
soon as prism emits them, with no HTTP polling or SSE parsing.

Implementation:

- [`proto/prism.proto`](../../proto/prism.proto): the service contract
- [`pkg/sink/grpc.go`](../../pkg/sink/grpc.go): `GRPCSink`, which is both a
  prism sink and the `PrismStream` server
- [`internal/cli/serve.go`](../../internal/cli/serve.go): `--serve-grpc`
  wiring (`startBroadcasts`)

---

## Starting the server

```sh
prism serve --serve-grpc 127.0.0.1:50051 --contract CBGSB... --follow
```

`--serve-grpc` takes a listen address. It can be combined with `--sse` and
`--ws`, and every endpoint gets the same records:

```sh
prism serve \
  --serve-grpc 0.0.0.0:50051 \
  --sse 127.0.0.1:8080 \
  --contract CBGSB... --topic 'AAAAAQ==,*' \
  --state-file /var/lib/prism/serve.state --resume
```

`prism serve` reuses everything `prism events` provides: RPC filters
(`--contract`, `--topic`, `--type`), `--state-file`/`--resume`, failover
across multiple `--rpc-url`s, `--dedup-window`, `--health-port`, and panic
restarts with `--max-restarts`. The gRPC listener is bound before streaming
starts, so a bad or occupied address fails straight away.

**Transport security.** The server is started with `grpc.NewServer()` and no
transport credentials, so it speaks **plaintext HTTP/2 (h2c)**. Bind it to
loopback or a private network. For TLS or mTLS, put it behind a proxy that
terminates TLS for gRPC (Envoy, NGINX `grpc_pass`, a service-mesh sidecar).
Server reflection is not enabled. Clients need the `.proto` file.

---

## Protobuf specification

```protobuf
syntax = "proto3";

package prism.v1;

option go_package = "github.com/stellar-optics/stellar-prism/proto;prismpb";

service PrismStream {
  rpc StreamEvents(EventRequest)   returns (stream EventResponse);
  rpc StreamLedgers(LedgerRequest) returns (stream LedgerResponse);
}

message EventRequest   { string contract_id = 1; }
message LedgerRequest  { uint32 from_ledger = 1; }
message EventResponse  { string id = 1; uint32 ledger = 2; int64 closed_at_unix_ms = 3; bytes record_json = 4; }
message LedgerResponse { string id = 1; uint32 ledger = 2; int64 closed_at_unix_ms = 3; bytes record_json = 4; }
```

### `PrismStream` service

| RPC | Request | Streams | Carries |
|---|---|---|---|
| `StreamEvents` | `EventRequest` | `EventResponse` | records with `kind: "event"` |
| `StreamLedgers` | `LedgerRequest` | `LedgerResponse` | records with `kind: "ledger"` |

> **What `prism serve` feeds today.** `serve` runs the events pipeline, so
> only `StreamEvents` gets records from it. `StreamLedgers` is part of the
> contract and is served by `GRPCSink` for any embedding that emits ledger
> records, but against `prism serve` it stays open and sends nothing.
> There is no transactions RPC in `prism.v1` yet. Use `prism transactions
> --json` or the Kafka/NATS sinks for transaction feeds.

### Request messages

| Message | Field | Type | Behaviour |
|---|---|---|---|
| `EventRequest` | `contract_id` | `string` | Empty: every event the server streams. Otherwise: only events whose contract is exactly this strkey (`C...`) |
| `LedgerRequest` | `from_ledger` | `uint32` | Reserved. Subscriptions are live-only (see [Delivery semantics](#delivery-semantics)) and this field is ignored for now |

### Response messages

`EventResponse` and `LedgerResponse` have the same shape:

| Field | Type | Meaning |
|---|---|---|
| `id` | `string` | prism's stable record ID (for events, the RPC event ID, e.g. `0000512340-0000000001`). Sorts in stream order. Use it for deduplication |
| `ledger` | `uint32` | Ledger sequence |
| `closed_at_unix_ms` | `int64` | Ledger close time, Unix milliseconds |
| `record_json` | `bytes` | The complete record, encoded **exactly** like a `prism events --json --raw` NDJSON line (UTF-8 JSON, no trailing newline) |

The typed fields cover what you need for routing and ordering without
parsing JSON. `record_json` carries the full payload. For an event:

```json
{
  "kind": "event",
  "id": "0000512340-0000000001",
  "ledger": 512340,
  "time": "2026-11-20T09:14:05Z",
  "type": "contract",
  "contract": "CBGSB...",
  "tx": "a3f1...",
  "txIndex": 3,
  "opIndex": 0,
  "topicsXdr": ["AAAADwAAAAh0cmFuc2Zlcg==", "AAAAEgAAAAA..."],
  "valueXdr": "AAAACgAAAAAAAAAAAAAAAAAHoSA="
}
```

Topics and values arrive as **base64 XDR `ScVal`s** (`topicsXdr`,
`valueXdr`). The gRPC path skips decoding to keep server-side cost low.
Decode them in the client with an XDR library (`github.com/stellar/go-stellar-sdk/xdr`,
`stellar-sdk` for Python/JS, `stellar-xdr` for Rust), or pipe them through
`lens decode`.

The JSON shape is versioned with the NDJSON output, which is documented and
covered by tests in `pkg/sink`. New fields may be added, so clients should
ignore unknown keys.

---

## Subscription filtering

Filtering happens at three levels. Use the earliest level that can express
what you need:

| Level | Where | What it can match | Cost |
|---|---|---|---|
| 1. RPC filters | `prism serve` flags | `--contract` (≤ 5 per RPC call; more are split across calls), `--topic` segment patterns, `--type`, `--topic-regex`, `--filter-expr` | Records that don't match never leave the RPC or the prism process |
| 2. Per-subscriber | `EventRequest.contract_id` | One exact contract ID | Filtered per client in `GRPCSink.StreamEvents`, after fan-out |
| 3. Client-side | your code | Anything in `record_json` (topics, values, tx hash) | Every record crosses the wire |

### Contract ID filter

Run one `prism serve` that watches every contract any client might need, and
let each client narrow to its own contract:

```sh
prism serve --serve-grpc 127.0.0.1:50051 \
  --contract CAAAA... --contract CBBBB... --contract CCCCC...
```

A client sending `EventRequest{contract_id: "CBBBB..."}` gets only `CBBBB...`
events. A client sending an empty request gets all three.

### Topic selectors

`prism.v1` requests have no topic field. To filter by topic:

- **Server-wide**, with `prism serve --topic`. It uses the same semantics as
  `stellar events --topic`: comma-separated segments, each a base64 `ScVal` or
  `*`, up to 4 segments per pattern, and the flag can be repeated. For example,
  to match `transfer` events with any second and third topic:

  ```sh
  prism serve --serve-grpc 127.0.0.1:50051 --contract CDLZF... \
    --topic 'AAAADwAAAAh0cmFuc2Zlcg==,*,*'
  ```

  `--topic-regex '0:^transfer$'` matches the decoded symbol instead of XDR.
- **Per client**: compare `topicsXdr[0]` in `record_json` with the base64
  of the symbol you want (the client examples below do this). The base64
  encoding of an ScVal is deterministic, so a plain string comparison is
  enough.

To serve different topic sets to different clients with filtering on the
server side, run one `prism serve` per set on different ports.

---

## Delivery semantics

- **Live only.** A subscriber gets records emitted *after* it subscribed.
  There is no replay. For history, backfill with `prism events --from <ledger>
  --json`, then switch to gRPC and drop duplicates by `id`.
- **Bounded per-subscriber queue.** Each subscriber has a 128-record buffer.
  If a client falls that far behind, **new records for that client are
  dropped** rather than blocking the stream. A slow consumer can never stall
  prism or the other subscribers. If you can't afford to lose records, keep
  the receive loop fast (hand records to a local queue) or use a durable sink
  (`--kafka-*`, `--nats-*`, `--pg-dsn`).
- **Ordering.** Records arrive in prism's emit order: ledger order, and within
  a ledger the RPC's event order. Compare `id`s to check.
- **Shutdown.** When prism stops, every stream ends cleanly with status `OK`
  (EOF in clients) after `GracefulStop`. Clients should reconnect with backoff.
  Crash restarts under `--max-restarts` start a fresh listener.

---

## Generating client stubs

The `.proto` file is self-contained. Copy `proto/prism.proto` into your client
project, or reference the repository directly.

| Language | Generator |
|---|---|
| Go | Import `github.com/stellar-optics/stellar-prism/proto` (stubs are checked in), or run `protoc --go_out=. --go-grpc_out=. prism.proto` |
| Python | `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. prism.proto` |
| Rust | `tonic-build` in `build.rs` |
| Java/Kotlin | `protoc-gen-grpc-java` / the `com.google.protobuf` Gradle plugin |
| Node | `@grpc/proto-loader` (dynamic) or `grpc-tools` |

---

## Go client

A complete program: subscribe to one contract, decode the record JSON, filter
by topic on the client, and reconnect with backoff.

```go
// Command prism-grpc-client subscribes to a prism gRPC event stream.
//
//	go run . -addr 127.0.0.1:50051 -contract CBGSB... -topic AAAADwAAAAh0cmFuc2Zlcg==
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	prismpb "github.com/stellar-optics/stellar-prism/proto"
)

// eventRecord is the subset of record_json this client uses. Unknown fields
// are ignored, so new prism fields never break it.
type eventRecord struct {
	ID        string   `json:"id"`
	Ledger    uint32   `json:"ledger"`
	Contract  string   `json:"contract"`
	TxHash    string   `json:"tx"`
	TopicsXDR []string `json:"topicsXdr"`
	ValueXDR  string   `json:"valueXdr"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "prism --serve-grpc address")
	contract := flag.String("contract", "", "only this contract (empty: all)")
	topic := flag.String("topic", "", "only events whose first topic is this base64 ScVal (empty: all)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()
	client := prismpb.NewPrismStreamClient(conn)

	backoff := time.Second
	for ctx.Err() == nil {
		err := subscribe(ctx, client, *contract, *topic)
		if ctx.Err() != nil {
			return
		}
		log.Printf("stream ended: %v; reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func subscribe(ctx context.Context, client prismpb.PrismStreamClient, contract, topic string) error {
	stream, err := client.StreamEvents(ctx, &prismpb.EventRequest{ContractId: contract})
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("server closed the stream")
		}
		if err != nil {
			return err
		}

		var rec eventRecord
		if err := json.Unmarshal(msg.GetRecordJson(), &rec); err != nil {
			log.Printf("skipping %s: %v", msg.GetId(), err)
			continue
		}
		if topic != "" && (len(rec.TopicsXDR) == 0 || rec.TopicsXDR[0] != topic) {
			continue
		}
		closed := time.UnixMilli(msg.GetClosedAtUnixMs()).UTC()
		log.Printf("ledger=%d closed=%s id=%s contract=%s tx=%s topics=%d",
			msg.GetLedger(), closed.Format(time.RFC3339), msg.GetId(),
			rec.Contract, rec.TxHash, len(rec.TopicsXDR))
	}
}
```

`go.mod`:

```
module example.com/prism-grpc-client

go 1.24

require (
	github.com/stellar-optics/stellar-prism v0.1.0 // or a commit pseudo-version
	google.golang.org/grpc v1.75.0
)
```

To decode `valueXdr` into a typed value:

```go
import "github.com/stellar/go-stellar-sdk/xdr"

var v xdr.ScVal
if err := xdr.SafeUnmarshalBase64(rec.ValueXDR, &v); err == nil && v.Type == xdr.ScValTypeScvI128 {
	amount := v.MustI128() // Int128Parts{Hi, Lo}
	_ = amount
}
```

---

## Python client

Install the dependencies and generate the stubs:

```sh
python -m venv .venv && . .venv/bin/activate
pip install grpcio grpcio-tools
curl -LO https://raw.githubusercontent.com/stellar-optics/stellar-prism/main/proto/prism.proto
mkdir -p proto && mv prism.proto proto/
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/prism.proto
# produces proto/prism_pb2.py and proto/prism_pb2_grpc.py
touch proto/__init__.py
```

`client.py`:

```python
"""Subscribe to a prism gRPC event stream.

    python client.py --addr 127.0.0.1:50051 --contract CBGSB... \
        --topic AAAADwAAAAh0cmFuc2Zlcg==
"""
import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone

import grpc

from proto import prism_pb2, prism_pb2_grpc


def subscribe(stub, contract: str, topic: str) -> None:
    request = prism_pb2.EventRequest(contract_id=contract)
    for msg in stub.StreamEvents(request):
        record = json.loads(msg.record_json)
        topics = record.get("topicsXdr", [])
        if topic and (not topics or topics[0] != topic):
            continue
        closed = datetime.fromtimestamp(msg.closed_at_unix_ms / 1000, tz=timezone.utc)
        print(
            f"ledger={msg.ledger} closed={closed.isoformat()} id={msg.id} "
            f"contract={record.get('contract')} tx={record.get('tx')} "
            f"value={record.get('valueXdr')}",
            flush=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    parser.add_argument("--contract", default="", help="only this contract (empty: all)")
    parser.add_argument("--topic", default="", help="first-topic base64 ScVal (empty: all)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

    backoff = 1.0
    while True:
        with grpc.insecure_channel(args.addr) as channel:
            stub = prism_pb2_grpc.PrismStreamStub(channel)
            try:
                subscribe(stub, args.contract, args.topic)
                print("server closed the stream", file=sys.stderr)
                backoff = 1.0
            except grpc.RpcError as err:
                print(f"stream error: {err.code().name}: {err.details()}", file=sys.stderr)
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


if __name__ == "__main__":
    sys.exit(main())
```

To decode `valueXdr`, use `stellar_sdk.xdr.SCVal.from_xdr(record["valueXdr"])`
from the [`stellar-sdk`](https://pypi.org/project/stellar-sdk/) package.

---

## Testing locally

```sh
# Terminal 1: serve testnet events over gRPC
prism serve --network testnet --serve-grpc 127.0.0.1:50051 --follow

# Terminal 2: inspect with grpcurl (no reflection, so pass the proto)
grpcurl -plaintext -import-path proto -proto prism.proto \
  -d '{"contract_id": ""}' 127.0.0.1:50051 prism.v1.PrismStream/StreamEvents
```

grpcurl prints `recordJson` base64-encoded, since it is a `bytes` field.
Pipe it through `jq -r .recordJson | base64 -d` to read it.

---

## Production notes

- **Health:** combine `--serve-grpc` with `--health-port` and point your
  orchestrator's probes at `/readyz`. The gRPC server doesn't implement
  `grpc.health.v1`.
- **Load balancing:** gRPC streams are long-lived HTTP/2 connections, so
  L4 balancers pin a client to one instance. Every prism instance streams the
  same data, which makes that harmless. Use `--ha-lease-file` if only one
  instance should poll RPC.
- **Fan-out cost:** each record is JSON-encoded once per subscriber. With
  hundreds of subscribers, put a broker in between (`--nats-*`, `--kafka-*`)
  and keep a few gRPC consumers.
