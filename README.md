# stellar-prism

`tail -f` for Soroban. Stream, filter and decode live Stellar contract events from the terminal.

[![CI](https://github.com/stellar-optics/stellar-prism/actions/workflows/ci.yml/badge.svg)](https://github.com/stellar-optics/stellar-prism/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/stellar-optics/stellar-prism.svg)](https://pkg.go.dev/github.com/stellar-optics/stellar-prism)
[![Go Report Card](https://goreportcard.com/badge/github.com/stellar-optics/stellar-prism)](https://goreportcard.com/report/github.com/stellar-optics/stellar-prism)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Part of [stellar-optics](https://github.com/stellar-optics) — Go tools that make Stellar and Soroban data legible. Sibling project: [stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens), which prism uses for all XDR decoding.

## Why this exists

Watching what a deployed contract is actually doing means polling RPC, getting
back base64 blobs, decoding them one at a time, and losing the thread. There is
no `tail -f` for contract events, so debugging a live contract turns into a
throwaway script every time.

```console
$ prism events --contract CDLZF...CYSC --follow
2026-08-04T14:35:13Z  ledger 3966812  contract  CDLZF…CYSC  tx c9d34…70b3
  topic[0]
    ScVal
    ├─ Type: ScValTypeScvSymbol
    └─ Sym: fee
  topic[1]
    ScVal
    ├─ Type: ScValTypeScvAddress
    └─ Address
       ├─ Type: ScAddressTypeScAddressTypeAccount
       └─ AccountId: GCGA4FBT7GQI3WMYPRJRMRIQJYZMFV7SWKK7UCWBG6KLF4QNJNOZD3ID
  value
    ScVal
    ├─ Type: ScValTypeScvI128
    └─ I128
       ├─ Hi: 0
       └─ Lo: 100
```

That is real output from Stellar testnet, not a mock-up.

### Honest scope: what already exists

The official [`stellar` CLI](https://developers.stellar.org/docs/tools/cli/stellar-cli)
has `stellar events`, and it does two things better than prism: it takes the
same filters, and it decodes event payloads **against the contract's own spec**,
so it can print real event names and typed parameters where prism prints the
wire shape.

**If you want a one-shot look at recent events, use `stellar events`.**

What it does not do is follow. Its `run()` issues a single request, prints, and
returns — there is no polling loop, no cursor advancement, no reconnection, and
`--count` defaults to 10. prism exists for the other half of the problem:

| | `stellar events` | prism |
|---|---|---|
| Contract / topic / type filters | ✅ | ✅ (same RPC, same semantics) |
| One-shot query | ✅ | ✅ |
| **Follow as ledgers close** | ❌ | ✅ |
| **Cursor continuity across polls** | ❌ | ✅ |
| **Reconnect with backoff** | ❌ | ✅ |
| **Adaptive poll rate** | ❌ | ✅ (`--adaptive`) |
| **NDJSON for pipelines** | ❌ | ✅ |
| **Per-contract fan-out** | ❌ | ✅ (`--fanout-dir`) |
| **Gap detection** | ❌ | ✅ |
| **Active-passive HA** | ❌ | ✅ (`--ha-lease-file`) |
| **Health/readiness probes** | ❌ | ✅ (`--health-port`) |
| **Graceful drain on SIGTERM** | ❌ | ✅ (`--drain-timeout`) |
| **Encrypted state file** | ❌ | ✅ (`--state-passphrase-env`) |
| Ledger streaming | ❌ | ✅ |
| Database / Parquet ingestion | ❌ | ✅ |
| Contract-spec-decoded payloads | ✅ **better** | ❌ ([roadmap](docs/roadmap.md)) |

## Install

```sh
go install github.com/stellar-optics/stellar-prism/cmd/prism@latest
```

Requires Go 1.25 or later. Defaults to Stellar **testnet**, so it works with no
configuration; point elsewhere with `--rpc-url` or `SOROBAN_RPC_URL`.

## Usage

### `prism events` — tail contract events

```sh
# Follow one contract as ledgers close
prism events --contract CDLZF...CYSC --follow

# Backfill from a ledger, then keep following
prism events --contract CDLZF...CYSC --from 3966000 --follow

# Start about ten minutes back, without picking a ledger
prism events --contract CDLZF...CYSC --since 10m --follow

# Filter by topic, using the server's wildcard syntax
prism events --topic 'AAAADwAAAANmZWUA,*' --limit 20
```

Topic filters are evaluated by the RPC server with the same semantics as
`stellar events --topic`: `*` matches exactly one segment, `**` matches zero or
more and must come last.

`--since` converts a relative window into an approximate start ledger (ledgers
close roughly every 5s; the window rounds up, so it never starts late). For
topics, `--topic-pattern` matches their **decoded** text rather than base64
XDR, with glob, prefix and regex segments:

```sh
# Any topic whose first segment starts with "transfer" (e.g. a symbol)
prism events --topic-pattern 'prefix:transfer' --limit 20

# Regex over the decoded topic text
prism events --contract CBGSB... --topic-pattern 're:burn.*' --follow

# Watch several contracts and split them into per-contract NDJSON files
prism events --contract CDLZF... --contract CDLZA... --fanout-dir events/ --follow

# Let the follow loop tune its own poll rate from observed ledger closes
prism events --contract CDLZF... --follow --adaptive
```

`--fanout-dir` writes one NDJSON file per watched contract under the
directory (streamed through a per-contract demux with bounded queues), while
the terminal keeps the combined stream. `--adaptive` replaces the fixed
`--poll` wait with a tuner whose only signal is how many ledgers closed
during the last wait, so it tracks the network's own cadence between 1s and
30s, jittered so several instances do not poll in phase.

### Batched output and parallel backfill

Batching trades a little latency for fewer sink writes. It is opt-in:

```sh
# Flush when 500 records accumulate or after 250ms, whichever comes first
prism events --from 3966000 --follow --json --batch \
  --batch-size 500 --batch-timeout 250ms
```

Historical backfill can fetch pages concurrently with `--backfill-workers`:

```sh
# Drain 24 hours of history with four overlapping fetch loops
prism events --from 3966000 --limit 10000 --json --backfill-workers 4
```

Workers fetch disjoint ledger ranges, then records are merged by ID before
they reach stdout, so output order and de-duplication are unchanged.

### Reprocessing with `--rewind`

`--rewind` steps the start point back so the records in between are processed
again — the manual way to reprocess what a stream just missed:

```sh
# Reprocess the last 500 ledgers, then keep following
prism events --contract CDLZF...CYSC --rewind 500 --follow

# Or by time: 30 minutes' worth of ledgers
prism events --contract CDLZF...CYSC --rewind 30m --follow
```

The step is measured from `--from` when it is given, and from the server's
head otherwise. While a stream is following, `SIGUSR2` asks for the same
step again, which reprocesses the records between the old and the new
position.

A rewind re-emits the records it steps over, on purpose: that is what
reprocessing means. A rewind that reaches past the server's ~24-hour
retention window is reported as a gap and exits 2, like any other gap.

### NDJSON for pipelines

`--json` emits one object per line, flushed after each record (or batch), so a
live stream pipes straight into `jq`:

```console
$ prism events --limit 2 --json
{"kind":"event","id":"0017037362169118720-0000000000","ledger":3966820,"time":"2026-08-04T14:35:53Z","type":"contract","contract":"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC","tx":"2766b62c…","txIndex":0,"opIndex":0,"topics":[{"type":"ScVal","value":{"Sym":"fee","Type":"ScValTypeScvSymbol"}},{"type":"ScVal","value":{"Address":{"AccountId":"GCPC5WU57GAV5IZCW7D2IYVEELAAWGN6GUKJW64MJQYIRNWDDO7VFY6I","Type":"ScAddressTypeScAddressTypeAccount"},"Type":"ScValTypeScvAddress"}}],"value":{"type":"ScVal","value":{"I128":{"Hi":0,"Lo":100},"Type":"ScValTypeScvI128"}}}
```

Decoded payloads carry the same `{type, value}` envelope that
`lens decode --json` emits, so a `jq` expression written against lens works
here unchanged:

```console
$ prism events --limit 4 --json | jq -r '"\(.ledger)  \(.contract[0:8])…  \(.value.value.Type)"'
3966820  CDLZFC3S…  ScValTypeScvI128
3966820  CDLZFC3S…  ScValTypeScvI128
3966820  CDLZFC3S…  ScValTypeScvI128
3966820  CDLZFC3S…  ScValTypeScvI128
```

### Resuming a stream from where it stopped

`--state-file` records the position the stream reached after every page, so
the next run can pick the stream up where the last one stopped:

```sh
# Record the position as the stream runs
prism events --contract CDLZF... --follow --state-file ~/.cache/prism/events.jsonl

# Continue from the last recorded position
prism events --contract CDLZF... --follow \
  --state-file ~/.cache/prism/events.jsonl --resume
```

The file is a journal: one checkpoint per delivered page. A background pruner
compacts it while the stream runs, so it stays small however long you follow.
`--prune-keep` sets how many checkpoints to keep (100 by default),
`--prune-max-age` drops the ones that are too old, and `--prune-interval` sets
how often the compaction happens.

A checkpoint is written only after a page has been delivered *in full*, so a
recorded position never points past records the run did not emit. The same
holds for a concurrent backfill (`--backfill-workers`): the drain reports the
position it reached, and a page the `--limit` cut short is not reported at all.

This narrows the restart window from "the whole run" to "one record". It is
not exactly-once — see [What prism guarantees](#what-prism-guarantees).

### Running two instances without duplicating records

A single prism instance is a single point of failure, and two instances
streaming the same contract produce duplicate downstream events and fight
over the cursor file. `--ha-lease-file` makes the pair **active-passive**: one
instance holds the lease and streams, the other waits and takes over if the
leader stops.

```sh
# Both instances share the lease file; only the leader streams
prism events --contract CDLZF... --follow \
  --ha-lease-file /shared/nfs/prism.lock --ha-heartbeat 5s
```

The lease is a file lock plus a record of who holds it, kept beside the lock
in `<path>.lease`:

- The leader holds an exclusive `flock` on the lease file for its whole term,
  and renews the lease every `--ha-heartbeat` (5s by default).
- A standby watches for the lease to expire and takes it over. `--ha-ttl`
  (15s by default) is how long a lease stays valid without a renewal; it must
  be at least twice the heartbeat.
- A crashed leader is replaced at once, because the kernel drops the lock when
  the process dies. A leader that is alive but partitioned is replaced when
  its lease expires.
- Every handover bumps an **epoch**, the fencing token. A leader that was
  fenced notices on its next renewal and stops, rather than continuing to
  emit alongside the new leader.
- A new leader resumes from the cursor the previous one committed, so the
  takeover continues the stream instead of starting a second one.

The lease file must live on a file system that supports advisory locking — a
shared NFS mount, for instance — so every instance sees the same lock. On a
platform without `flock`, prism refuses to start rather than pretending the
lease works.

A standby is alive but **not ready**: it is waiting for the lease, not
serving, and it must not be sent traffic.

### Health and readiness probes

`--health-port` serves the endpoints a container orchestrator asks for:

```sh
prism events --contract CDLZF... --follow --health-port 8086 --max-lag 100
```

| Endpoint | Answers |
|---|---|
| `/healthz` | 200 while the process is running and making progress; 503 when the stream is stuck |
| `/readyz` | 200 when the RPC is answering, the stream has not gapped, and the lag is within `--max-lag`; 503 otherwise |
| `/metrics` | The same state in the Prometheus exposition format |

`--max-lag` is how many ledgers the stream may be behind the server's head
before it stops being ready; 0 means no limit. The lag is measured from the
stream's own progress, so it is the real distance to the head and not a guess.

A standby answers `/healthz` with 200 — it is waiting, not stuck — and
`/readyz` with 503, so the orchestrator keeps traffic away from it.

### Graceful drain

Kubernetes gives a pod a grace period (30s by default) after SIGTERM before
it sends SIGKILL. prism uses it:

```sh
prism events --contract CDLZF... --follow --drain-timeout 15s
```

On SIGTERM or Ctrl-C, prism stops scheduling new RPC calls, then gives the
sinks and the state store up to `--drain-timeout` (15s by default) to flush
the records still in memory and commit the final cursor. It exits 0 when
every flush confirmed success.

A drain that overruns its deadline is reported rather than waited out: a
process that is about to be killed cannot be helped by a longer flush. The
records that were not delivered are re-emitted on the next run, because the
cursor is only committed for pages that were actually delivered.

### Encrypting the state file

A cursor journal holds stream metadata, and on a shared file system or in a
cloud bucket that is worth encrypting:

```sh
prism events --contract CDLZF... --follow \
  --state-file state.enc --state-passphrase-env PRISM_STATE_KEY
```

The state file is encrypted with authenticated **AES-256-GCM**, and every
update uses a fresh nonce. A file that was tampered with, truncated or
written with a different key is rejected with a clear authentication error
rather than being read as if it were intact.

The key comes from exactly one place:

| Flag | Key source |
|---|---|
| `--state-passphrase-env` | A passphrase in an environment variable |
| `--state-passphrase-file` | A passphrase in a file |
| `--state-kms-provider aws` + `--state-kms-key-id` | An AWS KMS key |
| `--state-kms-provider gcp` + `--state-kms-key-id` | A GCP KMS key |

With a key management service the data key is generated and wrapped by the
service, so the plaintext key never reaches the disk. Passing more than one
key source is an error rather than a silently ignored flag: an operator who
believes the file is encrypted with a passphrase must never end up with a
file encrypted with something else.

### `prism transactions` — successes, failures, traps and fee bumps

Every transaction is classified as it streams: `status`, the account that
actually paid the fee, and — for a fee-bump wrapper — the wrapper and inner
transaction details side by side. Failed transactions carry a decoded reason
naming the failing operation.

```sh
# Only failures, with the decoded reason
prism transactions --status failed --follow

# Soroban contract executions that trapped or panicked
prism transactions --trapped --json | jq -r '.failure.operation.summary'

# Fee-bump wrappers, and who paid for them
prism transactions --fee-bump --json | jq -r '.feeAccount'

# Everything a particular account paid the fee for
prism transactions --fee-account GBGSB... --follow
```

`--trapped` is narrower than `--status failed`: it selects only host
functions that trapped or panicked, not malformed invocations or exceeded
resource limits. `--fee-account` accepts a G or M address and matches the
effective payer, so a muxed fee source resolves to its underlying account.

The filters are applied after each page is fetched, so the cursor still
advances across suppressed transactions — a filtered stream is a view of the
transaction stream rather than a second stream that could skip history.

Pagination uses the RPC's own SEP-35 paging token, so `--from` hands over to
`--follow` at exactly the next transaction rather than at the next ledger. The
page size is capped at 200 by the server (the default is 100, because each
transaction carries three XDR blobs), and a page that stops short of the head
keeps draining: the server limits how many ledgers one call scans, so a short
page is not the end of a backfill.

```console
$ prism transactions --status failed --limit 1 --json
{"kind":"transaction","id":"0003966820-0001","ledger":3966820,"hash":"…","index":1,"successful":false,"status":"failed","feeAccount":"GBGSB…","failure":{"code":"tx_failed","summary":"The transaction was well-formed and authorised, but at least one of its operations failed.","operation":{"index":0,"type":"invoke_host_function","code":"invoke_host_function_trapped","summary":"The contract trapped: it panicked, or explicitly returned an error."}}}
```

Event streams can additionally drop re-polled duplicates with
`--dedup-window`: a bounded ring remembers the last N event fingerprints and
suppresses a replay without growing with traffic. It is off by default; `4096`
covers a page with margin.

### `prism tx` — why did this transaction fail?
Fetches a transaction and renders it through the lens explain path, pairing the
envelope with its result so the failure is attributed to the operation that
caused it:

```console
$ prism tx 2b2e276a8f99a2927580eca2c8d665c0adaef9526476f5549269798380a66648
Transaction failed at operation 2, payment (pay 20.0000000 EUR:GDUZL…3JGV to GD2J3…M4KA): The destination has no trustline for this asset.

  Source           GB5MEZYRAFCFANNJENG6UQOWVSGO2NDAWV7GSAVKASMYP2PHH6FPGDPJ
  Sequence         13427528056229711
  Fee bid          4.0000000 XLM (40000000 stroops)
  Fee charged      0.0000400 XLM (400 stroops)
  Memo             text: 8adeb170
  Signatures       4
  Precondition     valid until 2026-08-04T14:37:20Z

Operations (4)
  ✓ [0] payment — pay 77888.0000000 EUR:GDUZL…3JGV to GB47S…HX7B
  ✓ [1] payment — pay 77868.0000000 EUR:GDUZL…3JGV to GC3EK…3N7O
  ✗ [2] payment — pay 20.0000000 EUR:GDUZL…3JGV to GD2J3…M4KA
      payment_no_trust: The destination has no trustline for this asset.
      → The destination must establish a trustline before it can receive a non-native asset.
  ✗ [3] payment — pay 20.0000000 EUR:GDUZL…3JGV to GA7FP…GMAN
      payment_no_trust: The destination has no trustline for this asset.
```

### `prism ledgers` — watch the chain advance

```console
$ prism ledgers --follow --compact
2026-08-04T14:35:53Z  ledger 3966820  e68b8…0c1e
```

### Fee market, contract lifecycle and trades

```sh
# Ledger fee market and moving transaction fee averages
prism fee-stats --follow

# Follow contract storage, TTL expiry and deployment or upgrade activity
prism contracts --watch CDLZF...CYSC --follow

# Stream completed classic path-payment executions
prism trades --pair XLM/USDC --follow
```

`fee-stats` reports base fee and reserve, fee pool, circulating coins,
Soroban resource fees, and rolling 10 and 50 ledger transaction fee averages.
`contracts` reports ledger-key TTL and warns below 1,000 remaining ledgers;
RPC does not associate TTL key hashes with contract IDs, so TTL changes are
included even when `--watch` selects contracts. `trades` normalizes classic
strict-send and strict-receive path payments from transaction results. It also
recognizes Soroban swap events using `swap`, trader, input asset and output
asset topics with input and output amounts in the event value. Soroban amounts
remain raw token units because each contract defines its own decimal scale.

Failed transaction streams include the transaction or operation result code
inline, for example `failed at op 1 (payment): payment_underfunded`; NDJSON
records carry `error_code` and `error_detail` fields.

### `prism operations` - stream transaction operations

Deconstructs transaction envelopes into independently filterable records with
stable ledger/transaction/operation IDs:

```sh
prism operations --type payment --type invoke_host_function --follow --json
prism operations --source-account G... --from 3966000
```

### `prism transactions --detect-conflicts` - find footprint contention

Annotates Soroban transactions when read/write footprint keys overlap within a
ledger. Human output prints warnings; `--json` adds structured `conflicts`
metadata:

```sh
prism transactions --detect-conflicts --follow
prism transactions --detect-conflicts --follow --json
```

### `prism simulate` - follow a transaction simulation

Re-simulates a base64 transaction envelope as the chain head advances and
reports changes to return values, instructions, memory bytes, and minimum
resource fees:

```sh
prism simulate --tx-envelope AAA... --follow --json
```

## What prism guarantees

This is a streaming tool, so the guarantee matters more than the feature list:

> **No gaps, ever — detected rather than assumed. At-least-once delivery, with
> in-process de-duplication that makes it effectively exactly-once within a
> single run.**

- **No duplicates within a run.** Cursor paging is exact, and every record is
  checked against the highest ID already emitted, so a page replayed after a
  reconnect is suppressed. A `--rewind` is the one exception: it deliberately
  re-emits the records it steps over, which is what reprocessing means.
- **A dropped connection cannot skip records.** A failed fetch is retried with
  the cursor *unchanged*.
- **No silent gaps by default.** Records can only be genuinely lost by falling
  behind the RPC's ~24-hour retention window. prism detects that against the
  server's reported oldest ledger, prints the missed range, and **exits 2**
  rather than carrying on with a hole in the stream. If losing the pruned range
  is acceptable, `--gap-strategy resume` reports it and continues from the
  oldest ledger the server still retains; the skip is counted in the stats and
  printed on stderr.
- **Not exactly-once across restarts.** prism cannot atomically emit a record
  and commit its cursor, so a process killed between the two re-emits on
  restart. `--state-file` records the position after every delivered page and
  `--resume` starts the next run from it, which narrows that window to a
  single record — but it is still not exactly-once. Key off `id` if that
  matters to you.

These are enforced by tests, not just claimed — see
[docs/architecture.md](docs/architecture.md#testing-the-guarantee).

**Note:** Soroban RPC has no subscription or websocket API. "Streaming" here
means adaptive polling with exact cursor continuity — prism drains full pages
at network speed and only waits when it is caught up.

## Exit codes

Matching [stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens):

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Error |
| 2 | A specific negative outcome: the stream gapped, or `prism tx --fail-on-error` found a failed transaction |

## Library use

The streaming core is importable, and new sources or sinks plug into it
without touching the state machine:

```go
client := rpc.New(rpc.DefaultURL, 30*time.Second)

src, err := stream.NewEventSource(client, stream.EventQuery{
    ContractIDs: []string{"CDLZF..."},
    StartLedger: 3966000,
})
if err != nil {
    log.Fatal(err)
}

tailer := stream.NewTailer(src, sink.NewNDJSON(os.Stdout, true), stream.Options{
    Follow: true,
})
if err := tailer.Run(ctx); err != nil {
    log.Fatal(err)
}
```

See the [API reference](https://pkg.go.dev/github.com/stellar-optics/stellar-prism)
and [docs/architecture.md](docs/architecture.md).

## Project status

**Early but real.** v0.1 does what this README says, with the streaming state
machine covered by tests that run with no network. The library API may shift
before v1.0.

- Event, ledger and transaction streams; `prism tx` via the lens explain path.
- Count/time batching (`--batch`) and concurrent historical backfill
  (`--backfill-workers`).
- `--rewind` to step a stream back and reprocess recent ledgers, by count or
  by duration, while it runs.
- Explicit gap policy: stop by default, or `--gap-strategy resume` with the
  skipped range reported.
- Payloads render as the XDR wire shape — prism has no contract-spec decoding,
  and `stellar events` is better for that today.
- Optional cursor journal (`--state-file`) with background pruning, and
  `--resume` to continue from it. Still not exactly-once across restarts.
- Covered twice: by an offline suite with no network, and by an opt-in
  end-to-end run against a local Quickstart network (`make test-e2e`).

See [docs/roadmap.md](docs/roadmap.md).

## Contributing

Adding a stream source or an output sink is designed to be a one-file change.
See [CONTRIBUTING.md](CONTRIBUTING.md) and
[docs/architecture.md](docs/architecture.md).

## License

Apache-2.0. See [LICENSE](LICENSE).

### Safe transaction memos

`prism transactions --memo 'invoice:' --follow --json` selects transactions
whose **sanitized text memo starts with** `invoice:` (an exact memo also matches).
Filtering is local and preserves RPC pagination, including pages with no matches.
Memo IDs are decimal strings; hash and return memos are lowercase hex strings.
The `memo` object has `type` (`none`, `text`, `id`, `hash`, or `return`) and `value`.
Malformed envelopes retain their original XDR and report `memoError`.

Control characters, invalid UTF-8, and invisible Unicode formatting characters
are removed from decoded memo text. Terminal output escapes non-ASCII memo
characters. Both compact summaries and decoded payloads are protected. Original
base64 XDR is preserved for archival use.

### Ledger consensus diagnostics

```sh
prism ledgers --consensus --follow --compact
prism ledgers --consensus --expected-validator G... --follow --json
```

Consensus output includes the SCP value's close time, transaction-set hash,
value type, observed value signer, signature presence, and the close delta in
seconds between consecutive ledgers. Deltas over 10 seconds and absent expected
value signers produce warnings. The first ledger, gaps, and rewinds have no delta;
consensus diagnostics require sequential backfill (`--backfill-workers 1`).
Malformed headers produce a structured `consensus.error` without stopping the
stream. JSON sinks include these metrics under `consensus`, even with `--raw`.

**Header coverage:** `LedgerHeaderHistoryEntry` contains a signed Stellar value,
not the SCP envelopes, quorum-set hashes, or complete validator signatures.
`valueSigners` identifies value-signing evidence only; signatures are not
cryptographically verified. `quorumDataAvailable` is always false. An expected
validator warning means its value signature was not observed in this header;
it does not establish that the validator was offline. Full quorum health requires
an additional SCP history/live SCP data source.

### HTTP broadcasts

```sh
prism events --contract CDLZF... --follow --serve-sse 127.0.0.1:8080
prism serve --sse 127.0.0.1:8080 --ws 127.0.0.1:8081
curl -N http://127.0.0.1:8080/events
```

`serve` follows contract events by default and accepts the same contract/topic,
RPC, cursor, and output flags as `events`. `--serve-sse` (alias `--sse`) and `--ws`
are also available on other streaming commands. Each listener broadcasts records
in the existing raw NDJSON shape; local terminal/file output remains available.
Listeners stop when the stream ends, reaches `--limit`, or receives SIGINT/SIGTERM.
Use `--follow` for a persistent listener on commands other than `serve`.

SSE serves `/events`, with `id:` set to the record cursor and one JSON object in
`data:`. It sends comment pings every 15 seconds. A reconnect with
`Last-Event-ID` replays later records from the latest 1024 records held in memory,
then atomically joins live delivery. Unknown or expired IDs receive HTTP 409;
clients must explicitly restart from a suitable RPC ledger when replay expires.
Replay is shared by clients and does not survive process restarts. A fresh
connection without `Last-Event-ID` receives future records only.

WebSockets serve `/events` and `/ws`. Connections initially receive all records.
Send `{"subscribe":"contract","id":"CDLZF..."}` to restrict a connection to
that contract, or `{"subscribe":"topic","id":"AAAAAQ=="}` to match an exact
base64 XDR topic segment. Successful requests receive
`{"subscribed":"contract","id":"CDLZF..."}`; invalid requests receive an
`error` object. Subscriptions are additive (up to 128 per connection); contracts
are ORed, topics are ORed, and the two categories are ANDed. Reconnect to reset
subscriptions. WebSockets send periodic protocol pings and use the library's
same-origin browser policy.

Each client has a bounded queue. Slow clients disconnect rather than blocking
RPC polling or other clients. SSE clients can reconnect using retained history;
WebSocket delivery is live only. Write deadlines and shutdown cleanup release
stalled connections. These listeners provide no authentication or TLS; choose
the bind address explicitly and use a reverse proxy when needed.
