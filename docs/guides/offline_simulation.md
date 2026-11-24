# Transaction Simulation and Preflight Validation

Before signing and submitting a Soroban transaction to a live network, developers can evaluate an unsigned or template `TransactionEnvelope` against a Soroban RPC endpoint (such as a local Quickstart node, Futurenet, or Testnet) using `prism simulate`. Simulation executes the transaction in preflight mode without mutating ledger state and reports CPU instructions, disk read/write byte footprints, minimum resource fees, decoded return values, and host contract errors.

For CLI implementation details, see [`internal/cli/simulate.go`](../../internal/cli/simulate.go) and [`pkg/stream/simulate.go`](../../pkg/stream/simulate.go).

---

## The `prism simulate` Command

`prism simulate` takes a base64-encoded `TransactionEnvelope` via `--tx-envelope` (no positional arguments):

```sh
# Run a single preflight simulation against the configured RPC endpoint
prism simulate --tx-envelope "AAAAAgAAA..." --limit 1

# Output structured NDJSON for scripts and jq pipelines
prism simulate --tx-envelope "AAAAAgAAA..." --json --limit 1

# Continuously re-simulate at every newly closed ledger head
prism simulate --tx-envelope "AAAAAgAAA..." --follow --json

# Target a local standalone Quickstart node
prism simulate \
  --rpc-url http://localhost:8000/soroban/rpc \
  --network standalone \
  --tx-envelope "AAAAAgAAA..." \
  --json --limit 1
```

### Key Flags

| Flag | Purpose |
|---|---|
| `--tx-envelope <base64>` | **Required.** Base64-encoded `TransactionEnvelope` XDR to simulate. |
| `--limit <N>` | Stop after emitting `N` simulation records (use `--limit 1` for one-shot CI checks). |
| `--follow` | Keep polling `getLatestLedger` and re-simulate once per newly closed ledger. |
| `--from <ledger>` | Wait until the RPC head reaches `<ledger>` before running the first simulation. |
| `--json` | Emit one NDJSON object per simulation record, decoding `returnValue` via `stellar-xdr-lens`. |
| `--raw` | Keep `returnValueXdr` as base64 XDR in `--json` output without decoding. |
| `--compact` | Omit the decoded `return` `ScVal` tree in human-readable terminal output. |
| `--rpc-url <url>` | Override `SOROBAN_RPC_URL` (repeatable for failover pools). |
| `--network <name>` | Expected network passphrase check (`testnet`, `mainnet`, `futurenet`, `standalone`). |
| `--wire-log <path>` | Write redacted RPC request/response NDJSON logs (`-` for stderr) for debugging `simulateTransaction` payloads. |
| `--state-file <path>` / `--resume` | Persist and resume the last simulated ledger cursor across restarts (keyed by envelope SHA-256). |

### Resource Budget Metrics and Output Schema

On each newly observed ledger head, `SimulationSource` calls `simulateTransaction` and unmarshals `TransactionDataXDR` (`xdr.SorobanTransactionData`) into resource budget counters alongside `minResourceFee` and the first host function's `returnValue`.

#### Human-Readable Output

```text
ledger 3966820  simulation  instructions 142850  disk-read 1024  write 256  min-fee 18420  changed instructions,min_resource_fee
  return
    ScVal
    ├─ Type: ScValTypeScvU32
    └─ U32: 42
```

Use `--compact` to suppress the multi-line `return` tree and keep one line per ledger.

#### NDJSON Output (`--json`)

```json
{
  "kind": "simulation",
  "id": "0003966820",
  "ledger": 3966820,
  "instructions": 142850,
  "diskReadBytes": 1024,
  "writeBytes": 256,
  "minResourceFee": 18420,
  "returnValue": {
    "type": "ScVal",
    "value": {
      "Type": "ScValTypeScvU32",
      "U32": 42
    }
  },
  "changed": ["instructions", "min_resource_fee"],
  "previous": {
    "ReturnValueXDR": "AAAAAwAAACo=",
    "Instructions": 141200,
    "DiskReadBytes": 1024,
    "WriteBytes": 256,
    "MinResourceFee": 18100
  }
}
```

| Field | Type | Meaning |
|---|---|---|
| `instructions` | `uint32` | Simulated Soroban CPU instruction count from `SorobanResources.Instructions`. |
| `diskReadBytes` | `uint32` | Simulated ledger bytes read (`SorobanResources.DiskReadBytes`). |
| `writeBytes` | `uint32` | Simulated ledger bytes written (`SorobanResources.WriteBytes`). |
| `minResourceFee` | `int64` | Minimum Soroban resource fee (in stroops) estimated by the RPC node. |
| `returnValue` / `returnValueXDR` | JSON / `string` | Host function return `ScVal` (decoded by `stellar-xdr-lens`, or base64 with `--raw`). |
| `error` | `string` | Simulation error string returned by `simulateTransaction`, omitted when empty. |
| `changed` | `[]string` | Fields that changed since the previous ledger head (`return_value`, `instructions`, `memory`, `min_resource_fee`, `error`). |
| `previous` | `object` | Snapshot of the immediately preceding simulation for diffing drift across ledgers. |

### Surfacing Host Contract Panics, Diagnostic Events, and Auth Entries

#### Host Contract Panics in `prism simulate`

When a simulated invocation traps, panics, or fails footprint/auth validation, Soroban RPC populates the response `Error` field, which `prism simulate` surfaces directly:

- In terminal output, `sim.Error` is printed inline on the ledger summary line.
- In `--json` output, `.error` contains the full RPC host error (for example, `HostError: Error(Contract, #...)` or `Error(WasmVm, InvalidAction)`).
- Changes in failure state across ledgers appear in `.changed` as `"error"`.

#### Diagnostic Events and Authorization Trees

`prism simulate` focuses on resource budgets, return values, error strings, and cross-ledger drift (`SimulationSource` does not currently emit raw `DiagnosticEventsXDR` or `SorobanAuthorizationEntry` sub-trees inside `KindSimulation` records). Pair `prism simulate` with these companion tools when deeper inspection is needed:

- **Diagnostic events (against local Quickstart / testnet):** Stream `diagnostic` and `contract` events emitted during test executions:
  ```sh
  prism events --contract CDLZF... --type diagnostic --type contract --follow
  ```
- **Authorization and envelope XDR inspection:** Decode the `TransactionEnvelope` (including `SorobanAuthorizationEntry` credentials and footprint) directly with `stellar-xdr-lens`, or enable `--wire-log -` on `prism simulate` to inspect the raw `simulateTransaction` JSON-RPC response (`results[].auth`, `diagnosticEvents`).
- **Submitted transaction failure attribution:** If a simulated transaction is submitted and traps on-chain, explain the failure or stream trapped invocations with:
  ```sh
  prism tx <tx-hash> --fail-on-error
  prism transactions --trapped --json | jq '.failure'
  ```

---

## Preflight Simulation vs. On-Chain Execution

A passing preflight simulation does not guarantee identical execution when the transaction lands in a ledger. Account for these differences before submitting:

| Aspect | Preflight Simulation (`simulateTransaction`) | On-Chain Inclusion (`sendTransaction`) |
|---|---|---|
| **Ledger state** | Runs against a snapshot of the RPC node's current latest ledger. | Executes against the state at the future inclusion ledger after preceding transactions in that ledger apply. |
| **Account sequence** | Does not consume the source account's sequence number. | Consumes `seq + 1`; concurrent submissions from the same account invalidate stale envelopes. |
| **Footprint & contention** | Computes read/read-write ledger footprint and checks TTLs at simulation time. | Other transactions in the same ledger can mutate shared `ReadWrite` keys or cause contention (observable via `prism transactions --detect-conflicts`). |
| **Fee pricing** | Reports `minResourceFee` (resource floor only, excluding inclusion fee competition). | Inclusion competes in the ledger fee market; monitor recent surge fees with `prism fee-stats`. |
| **Authorization & signatures** | Can simulate an unsigned envelope to discover required auth entries and resource data. | Requires assembled `SorobanTransactionData`, non-expired auth `signatureExpirationLedger`, and valid signatures before broadcast. |
| **Time & ledger-dependent logic** | Uses the simulated ledger sequence and close timestamp. | Contract checks using `env.ledger().sequence()` or `env.ledger().timestamp()` evaluate against the inclusion ledger. |

Use `prism simulate --tx-envelope ... --follow --json` while testing against active contracts to observe whether `changed` reports `instructions`, `memory`, `min_resource_fee`, or `return_value` drift as ledger state advances.

---

## Automated CI Validation Recipes

### 1. Pre-Sign Resource Budget Gate (Live Testnet or Local Quickstart)

Run `prism simulate` with `--limit 1 --json` in CI before signing or broadcasting generated transaction envelopes. Fail the pipeline if `.error` is present or if simulated resources exceed your contract budget:

```sh
#!/usr/bin/env bash
set -euo pipefail

TX_ENVELOPE="${1:?usage: check_sim.sh <base64-tx-envelope>}"
RPC_URL="${SOROBAN_RPC_URL:-https://soroban-testnet.stellar.org}"
MAX_INSTRUCTIONS=50000000
MAX_MIN_FEE=500000

sim_json="$(prism simulate \
  --rpc-url "$RPC_URL" \
  --tx-envelope "$TX_ENVELOPE" \
  --json \
  --limit 1)"

echo "$sim_json" | jq -e '
  (.error // "") == "" and
  (.instructions <= '"$MAX_INSTRUCTIONS"') and
  (.minResourceFee <= '"$MAX_MIN_FEE"')
' >/dev/null || {
  echo "Preflight simulation failed or exceeded budget:" >&2
  echo "$sim_json" | jq . >&2
  exit 1
}

echo "Preflight OK: instructions=$(echo "$sim_json" | jq .instructions) minResourceFee=$(echo "$sim_json" | jq .minResourceFee)"
```

### 2. GitHub Actions Step Against Local Quickstart

```yaml
- name: Validate transaction envelope via prism simulate
  env:
    TX_ENVELOPE: ${{ steps.build_tx.outputs.envelope_xdr }}
    SOROBAN_RPC_URL: http://127.0.0.1:8000/soroban/rpc
  run: |
    prism simulate --network standalone --tx-envelope "$TX_ENVELOPE" --json --limit 1 \
      | jq -e '(.error // "") == "" and .instructions > 0'
```

### 3. Offline Deterministic Suite Without Network Calls

Inside Go tests for `stellar-prism` itself, `internal/fakerpc` and `pkg/prismtest` provide `SetSimulations(...)` to exercise `prism simulate` end-to-end with zero network access (see `TestSimulateCommandNDJSON` in `internal/cli/new_features_test.go` and `TestSimulationSourceEmitsResourceEstimate` in `pkg/stream/new_features_test.go`).
