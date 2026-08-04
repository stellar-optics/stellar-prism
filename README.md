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
| **NDJSON for pipelines** | ❌ | ✅ |
| **Gap detection** | ❌ | ✅ |
| Ledger streaming | ❌ | ✅ |
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

# Filter by topic, using the server's wildcard syntax
prism events --topic 'AAAADwAAAANmZWUA,*' --limit 20
```

Topic filters are evaluated by the RPC server with the same semantics as
`stellar events --topic`: `*` matches exactly one segment, `**` matches zero or
more and must come last.

### NDJSON for pipelines

`--json` emits one object per line, flushed per record, so a live stream pipes
straight into `jq`:

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

## What prism guarantees

This is a streaming tool, so the guarantee matters more than the feature list:

> **No gaps, ever — detected rather than assumed. At-least-once delivery, with
> in-process de-duplication that makes it effectively exactly-once within a
> single run.**

- **No duplicates within a run.** Cursor paging is exact, and every record is
  checked against the highest ID already emitted, so a page replayed after a
  reconnect is suppressed.
- **A dropped connection cannot skip records.** A failed fetch is retried with
  the cursor *unchanged*.
- **No silent gaps.** Records can only be genuinely lost by falling behind the
  RPC's ~24-hour retention window. prism detects that against the server's
  reported oldest ledger, prints the missed range, and **exits 2** rather than
  carrying on with a hole in the stream.
- **Not exactly-once across restarts.** prism cannot atomically emit a record
  and commit its cursor, so a process killed between the two re-emits on
  restart. Key off `id` if that matters to you. Durable cursors are on the
  [roadmap](docs/roadmap.md).

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

- Event and ledger streams; `prism tx` via the lens explain path.
- Payloads render as the XDR wire shape — prism has no contract-spec decoding,
  and `stellar events` is better for that today.
- No durable cursor yet, so exactly-once across restarts is not offered.

See [docs/roadmap.md](docs/roadmap.md).

## Contributing

Adding a stream source or an output sink is designed to be a one-file change.
See [CONTRIBUTING.md](CONTRIBUTING.md) and
[docs/architecture.md](docs/architecture.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
