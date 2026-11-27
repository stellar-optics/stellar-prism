# Real-time web streaming with SSE and WebSockets

`prism serve` turns any `prism` stream into a local broadcast server. A browser
can subscribe with Server-Sent Events (SSE) at `/events` or with WebSockets at
`/ws` (also `/events`), receiving records as soon as the stream emits them
instead of polling Soroban RPC from the client.

This tutorial is grounded in the implementation:
[`pkg/sink/sse.go`](../../pkg/sink/sse.go),
[`pkg/sink/websocket.go`](../../pkg/sink/websocket.go), the shared
[`pkg/sink/broadcast.go`](../../pkg/sink/broadcast.go) hub, and the
[`prism serve` wiring](../../internal/cli/serve.go). See also the
[CLI reference](../cli/command_reference.md#prism-serve) and the
[architecture overview](../architecture.md).

- [Start the broadcaster](#start-the-broadcaster)
- [SSE: `/events`](#sse-events)
- [WebSocket: `/ws`](#websocket-ws)
- [Choosing a transport](#choosing-a-transport)
- [Operational notes and troubleshooting](#operational-notes-and-troubleshooting)

---

## Start the broadcaster

`serve` reuses every `prism events` filter and flag. It forces `--follow` to
`true`, and at least one listener flag is required:

| Flag | Alias | Description |
|---|---|---|
| `--serve-sse <address>` | `--sse` | Serve SSE at `http://<address>/events`; keeps 1,024 records for replay |
| `--ws <address>` | — | Serve WebSockets at `http://<address>/ws` or `http://<address>/events` |
| `--max-restarts <n>` | — | Restart the broadcast at most `n` times after a panic (not after ordinary errors) |

```sh
# One SSE listener
prism serve --sse 127.0.0.1:8080 --network testnet --contract CABC... --follow

# One WebSocket listener
prism serve --ws 127.0.0.1:8081 --network testnet --contract CABC... --follow

# Both listeners at once; use different ports
prism serve \
  --serve-sse 127.0.0.1:8080 \
  --ws 127.0.0.1:8081 \
  --network mainnet \
  --contract CDLZF...CYSC \
  --follow
```

The listener is bound during startup, so an invalid or occupied address fails
immediately. Records still go to the normal output sink first; each configured
broadcaster receives the same record. `--json` / `--raw` change stdout, but
**broadcast payloads always use the compact NDJSON shape with base64 XDR
fields**, produced by `recordMessage` in
[`broadcast.go`](../../pkg/sink/broadcast.go).

> [!NOTE]
> Filtering is performed by the CLI before a record reaches the broadcaster.
> The HTTP query string is not used by either handler. In particular,
> `http://localhost:8080/events?contract=CABC...&topic=...` does **not** filter
> the SSE feed in this version: add `--contract` / `--topic` to `prism serve`,
> or filter in the browser as shown below.

---

## SSE: `/events`

### Plain JavaScript

```js
const es = new EventSource("http://localhost:8080/events");

es.onmessage = (event) => {
  const record = JSON.parse(event.data);
  console.log(record.id, record.kind, record.contract);
};

es.onerror = () => {
  // EventSource reconnects automatically. Clear local state here if needed.
};
```

An event frame looks like this:

```text
: connected

id: 0000000000005400-0000000003
data: {"kind":"event","id":"0000000000005400-0000000003","ledger":5400,"contract":"CABC...","txIndex":1,"opIndex":0,"topicsXdr":["AAAAAQAAAAA="],"valueXdr":"AAAAAQAAAAA="}

: ping
```

- `id:` is the record's `ID`, which is also present in the JSON `data:` line.
- Only GET is accepted; other methods receive `405`.
- `Content-Type: text/event-stream`, `Cache-Control: no-cache`, and
  `X-Accel-Buffering: no` are set so proxies do not buffer the stream.
- There is no SSE `event:` field and no `retry:` directive. A `: ping` comment
  is sent every 15 seconds to keep idle connections alive.

### Client-side filtering

Because query parameters are ignored, filter the decoded record yourself when
you cannot control the server flags:

```js
const contract = "CABC...";
const topic = "AAAAAQAAAAA="; // base64 ScVal, exactly as in topicsXdr

const es = new EventSource("http://localhost:8080/events");
es.onmessage = (event) => {
  const record = JSON.parse(event.data);
  if (record.kind !== "event") return;
  if (record.contract !== contract) return;
  if (!record.topicsXdr?.includes(topic)) return;
  render(record);
};
```

For a large fan-out, prefer server-side `--contract` and `--topic` so the
records are not sent to clients that will discard them.

### Resuming with `Last-Event-ID`

SSE replay is a bounded, in-memory ring of the latest **1,024** records. The
SSE handler reads the `Last-Event-ID` request header and replays everything
after that ID before switching to live delivery; registration and replay share
one lock, so no record is missed between the two.

- `EventSource` sends `Last-Event-ID` automatically on reconnect after it has
  received an `id:` line. Do not set the header manually in `EventSource`.
- If the ID is not in the ring (or the process restarted), the server answers
  `409 Conflict`. There is no persistent cursor for SSE.
- An empty header starts a live-only subscription.
- Use a durable `--state-file` / `--resume` on the **stream** if you need a
  cursor that survives restarts; that is independent of browser replay.

When you need explicit control over the header and the `409` case, use `fetch`
with a streaming reader instead of `EventSource`:

```js
let lastId = localStorage.getItem("prism-last-id") || "";

async function stream() {
  const headers = lastId ? { "Last-Event-ID": lastId } : {};
  const res = await fetch("http://localhost:8080/events", { headers });

  if (res.status === 409) {
    localStorage.removeItem("prism-last-id");
    lastId = "";
    return stream(); // restart without the expired cursor
  }
  if (!res.ok) throw new Error(`SSE failed: ${res.status}`);

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let boundary;
    while ((boundary = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);

      let data = "";
      for (const line of frame.split("\n")) {
        if (line.startsWith("id: ")) {
          lastId = line.slice(4);
          localStorage.setItem("prism-last-id", lastId);
        } else if (line.startsWith("data: ")) {
          data += line.slice(6);
        }
      }
      if (data) render(JSON.parse(data));
    }
  }
}
```

### React example

```jsx
import { useEffect, useState } from "react";

export function EventFeed({ url = "http://localhost:8080/events" }) {
  const [records, setRecords] = useState([]);

  useEffect(() => {
    const es = new EventSource(url);
    es.onmessage = (event) => {
      const record = JSON.parse(event.data);
      if (record.kind === "event") {
        setRecords((prev) => [...prev.slice(-99), record]);
      }
    };
    return () => es.close(); // always close on unmount
  }, [url]);

  return (
    <ul>
      {records.map((r) => (
        <li key={r.id}>
          {r.contract} — {r.topicsXdr?.join(" / ")}
        </li>
      ))}
    </ul>
  );
}
```

---

## WebSocket: `/ws`

Connect to either `/ws` or `/events` on the `--ws` listener. A new connection
receives **all** records until it sends its first subscription; the first valid
subscription restricts the feed.

### Subscription protocol

Send JSON text messages with two string fields:

```json
{ "subscribe": "contract", "id": "CABC..." }
```

```json
{ "subscribe": "topic", "id": "AAAAAQAAAAA=" }
```

- `subscribe` must be exactly `"contract"` or `"topic"`; `id` must be non-empty.
- An accepted subscription is acknowledged with
  `{"subscribed":"contract","id":"CABC..."}`.
- An invalid message is answered with `{"error":"..."}`. A malformed JSON
  message or wrong JSON types close the socket.
- Up to 128 subscriptions per connection.
- Contract IDs are matched exactly. Topic IDs are **exact base64 ScVal
  segments** from `topicsXdr`, matched in any topic position.
- Subscriptions within a category are ORed; contract and topic categories are
  ANDed. Once any subscription exists, non-event records are not delivered.
- There is **no unsubscribe message** in this version. To change a
  subscription, close the socket and open a new one.

Each delivered event is a WebSocket text message containing the same compact
NDJSON object as SSE, without the `id:` / `data:` wrapper:

```json
{"kind":"event","id":"0000000000005400-0000000003","ledger":5400,"contract":"CABC...","txIndex":1,"opIndex":0,"topicsXdr":["AAAAAQAAAAA="],"valueXdr":"AAAAAQAAAAA="}
```

> [!NOTE]
> WebSocket replay is not available: the sink keeps no history, so
> `Last-Event-ID` and reconnect replay apply only to SSE. The server sends a
> ping every 15 seconds and expects a pong within 45 seconds.

### Plain JavaScript

```js
const ws = new WebSocket("ws://localhost:8081/ws");

ws.onopen = () => {
  ws.send(JSON.stringify({ subscribe: "contract", id: "CABC..." }));
  ws.send(JSON.stringify({ subscribe: "topic", id: "AAAAAQAAAAA=" }));
};

ws.onmessage = (event) => {
  if (event.data.startsWith('{"error"') || event.data.includes('"subscribed"')) {
    console.log("control:", event.data);
    return;
  }
  render(JSON.parse(event.data));
};

ws.onclose = (e) => console.log("closed", e.code, e.reason);
```

### React example

```jsx
import { useEffect, useState } from "react";

export function WebSocketFeed({
  url = "ws://localhost:8081/ws",
  contracts = [],
  topics = [],
}) {
  const [records, setRecords] = useState([]);

  useEffect(() => {
    const ws = new WebSocket(url);
    ws.onopen = () => {
      for (const id of contracts) {
        ws.send(JSON.stringify({ subscribe: "contract", id }));
      }
      for (const id of topics) {
        ws.send(JSON.stringify({ subscribe: "topic", id }));
      }
    };
    ws.onmessage = (event) => {
      if (event.data.includes('"subscribed"') || event.data.includes('"error"')) {
        return;
      }
      const record = JSON.parse(event.data);
      setRecords((prev) => [...prev.slice(-99), record]);
    };
    return () => ws.close();
  }, [url, contracts.join(","), topics.join(",")]);

  return <pre>{records.map((r) => r.id).join("\n")}</pre>;
}
```

---

## Choosing a transport

| | SSE (`/events`) | WebSocket (`/ws`) |
|---|---|---|
| Browser API | `EventSource` | `WebSocket` |
| Reconnect | Automatic in `EventSource` | Implement in the client |
| Missed events | Up to 1,024 replayed via `Last-Event-ID` | None |
| Filtering | CLI flags or client code | Dynamic `subscribe` messages |
| Same-origin | Browser allows cross-origin `EventSource` | Same-origin required by the default upgrader |

Use SSE for dashboards and simple feeds where automatic reconnect and replay
matter. Use WebSockets when the page needs to change subscriptions without
re-establishing an HTTP stream.

---

## Operational notes and troubleshooting

- **No built-in authentication.** Put both listeners behind a reverse proxy or
  bind them to a private interface. The `--header` flag authenticates *outbound*
  RPC requests and does not protect these inbound endpoints.
- **No CORS headers are added.** For a cross-origin SSE page, configure CORS at
  the proxy. A cross-origin WebSocket handshake is rejected (`403`) by the
  default same-origin check.
- **Slow clients are disconnected.** Each subscriber has a bounded queue; when
  it fills, the client is removed. SSE clients can resume with `Last-Event-ID`;
  WebSocket clients must reconnect and subscribe again.
- **`409 Conflict` on reconnect.** The recorded ID has aged out of the 1,024
  record ring or the process restarted. Clear the saved ID and start live.
- **No events after subscribing.** Check that the ID is exact (base64 ScVal for
  topics, strkey for contracts), and remember that contract and topic filters
  are ANDed.
- **`prism: broadcast listening at ...`** is printed for every listener, with
  the bound address; the message always says `/events`, including for the
  WebSocket listener.
- **`--max-restarts`** covers panics in the supervisor, not normal stream
  errors. A failed `--rpc-url` failover is handled inside the stream; see
  [RPC pooling and failover](../guides/rpc_pooling_and_failover.md).
- For sink failures that should not stop the broadcast, use the
  [dead letter queue](../operations/dead_letter_queue.md).
