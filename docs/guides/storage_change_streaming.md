# Contract Storage Change Streaming and State Diffing

Soroban contracts keep their state in ledger entries: `contract_data` entries
for instance, persistent and temporary storage, `contract_code` entries for
uploaded WASM, and `ttl` entries that decide when each of those expires. None
of these changes reach the event stream unless the contract chooses to emit an
event. They are visible in every transaction's `TransactionMeta`, though, and
`prism storage` decodes that metadata into a stream of storage transitions.

This guide covers the record schema, the three storage durabilities, filtering
by contract and storage key, rebuilding before/after diffs, and the TTL
projections prism attaches to `ttl` records.

Implementation: [`internal/cli/storage.go`](../../internal/cli/storage.go)
(flags) and [`pkg/stream/storage_changes.go`](../../pkg/stream/storage_changes.go)
(`StorageChangeSource`). Architecture notes are in
[docs/architecture.md](../architecture.md#contract-storage-changes-prism-storage).

---

## How it works

Soroban RPC has no storage-change endpoint. `StorageChangeSource` wraps the
transaction source, pages through `getTransactions` like `prism transactions`
does, and decodes each transaction's `ResultMetaXDR`. Every
`LedgerEntryChange` is mapped to a record, in the order the ledger applied it:
transaction-level changes before the operations, each operation's changes,
then transaction-level changes after them. Metadata versions 0 through 4 are
supported.

Because it reuses the transaction cursor, the stream has the same delivery
guarantee as every other prism stream: `--state-file` / `--resume`,
`--rewind`, gap detection and exit code 2 behave as they do for events.

Two kinds of change are not emitted:

- **`state` changes.** The ledger restates an entry immediately before it is
  updated or removed. That entry is what the transaction *read*, not what it
  changed, and emitting it would double the stream.
- **Non-contract entries.** Account, trustline, offer and other classic
  entries are skipped. Use `prism transactions` or `prism operations` for those.

---

## The record schema

Each record has `kind: "storage_change"`. With `--json` the NDJSON fields are:

| Field | Present on | Meaning |
|---|---|---|
| `id` | all | `LLLLLLLLLL-TTTTTT-NNNNNN`: ledger, transaction application order, and the change's position within that transaction |
| `ledger` | all | Ledger sequence the change was applied in |
| `time` | all | Ledger close time (RFC 3339, UTC) |
| `changeType` | all | `created`, `updated`, `removed` or `restored` |
| `entryType` | all | `contract_data`, `contract_code` or `ttl` |
| `contract` | `contract_data` | Strkey contract address (`C...`) that owns the entry |
| `durability` | `contract_data` | `persistent` or `temporary`; see below for instance storage |
| `wasmHash` | `contract_code` | Hex SHA-256 of the WASM |
| `keyHash` | `ttl` | Hex SHA-256 of the `LedgerKey` whose lifetime this entry sets |
| `key` / `keyXdr` | all | The `LedgerKey`: decoded JSON by default, base64 XDR with `--raw` |
| `entry` / `entryXdr` | all except `removed` | The full `LedgerEntry` after the change. For `contract_data` it holds the storage key (`Data.ContractData.Key`, an `xdr.ScVal`) and the **new value** (`Data.ContractData.Val`) |
| `change` / `changeXdr` | all | The `LedgerEntryChange` exactly as the ledger emitted it |
| `liveUntilLedger` | `ttl` | Last ledger the entry stays live |
| `remainingTtl` | `ttl` | `liveUntilLedger - ledger`, or 0 if already expired |
| `ttlWarning` | `ttl` | `true` when `remainingTtl` is below 1,000 ledgers |

An example persistent `contract_data` update, trimmed:

```json
{
  "kind": "storage_change",
  "id": "0000512340-000003-000001",
  "ledger": 512340,
  "time": "2026-11-20T09:14:05Z",
  "changeType": "updated",
  "entryType": "contract_data",
  "contract": "CBGSB...",
  "durability": "persistent",
  "entry": {"type": "LedgerEntry", "value": {"Data": {"Type": "LedgerEntryTypeContractData", "ContractData": {
    "Contract": {"Type": "ScAddressTypeScAddressTypeContract", "ContractId": "..."},
    "Durability": "ContractDataDurabilityPersistent",
    "Key": {"Type": "ScValTypeScvVec", "Vec": [
      {"Type": "ScValTypeScvSymbol", "Sym": "Balance"},
      {"Type": "ScValTypeScvAddress", "Address": {"Type": "ScAddressTypeScAddressTypeAccount", "AccountId": "GD5T..."}}
    ]},
    "Val": {"Type": "ScValTypeScvI128", "I128": {"Hi": 0, "Lo": 125000000}}
  }}}}
}
```

The decoded shapes come from
[stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens), so a
`jq` expression written against `lens decode` output works here too.

### Previous values

Records carry the value **after** the change. prism does not attach the
previous value: the `state` change that holds it is skipped for the reason
above. To get before/after diffs, keep the last value you saw for each key and
compare; see [State diffing](#state-diffing) below. For a `removed` record
there is no `entry`, so the last value you stored *is* the deleted value.

---

## Storage durability types

Soroban has three storage tiers. On the ledger they map to two `durability`
values:

| Soroban storage | `durability` | Storage key | Expiry behaviour |
|---|---|---|---|
| `env.storage().instance()` | `persistent` | always `LedgerKeyContractInstance` | One entry per contract. It holds the contract's executable reference and its whole instance map (admin, config, small shared state). Shares the contract's TTL |
| `env.storage().persistent()` | `persistent` | the contract's key, for example `Balance(addr)` | Archived when its TTL runs out and can be `restored` |
| `env.storage().temporary()` | `temporary` | the contract's key | Deleted for good when its TTL runs out. Never restored |

So "instance storage" is the `persistent` entry whose key is the
`LedgerKeyContractInstance` ScVal, base64 `AAAAFA==`. Every instance-map
write (an admin change, a config flag) shows up as an `updated` record on that
one entry.

What each change type means:

- **`created`**: the first write to a key, or a contract deployment creating
  its instance entry. Each `contract_data` creation comes with a `ttl`
  creation for the same key.
- **`updated`**: a new value under an existing key, or, for `ttl` entries, a
  TTL extension (`extend_ttl`, `ExtendFootprintTTLOp`).
- **`removed`**: an explicit `remove()`, or code removed from the ledger.
- **`restored`**: an archived persistent entry brought back with
  `RestoreFootprintOp` or automatic restoration. Temporary entries are never
  restored.

---

## Filtering

All filters are validated up front: a typo is reported straight away instead of
producing a stream that matches nothing.

| Flag | Values | Notes |
|---|---|---|
| `--contract C...` | strkey contract ID, repeatable | Keeps that contract's `contract_data`. `contract_code` and `ttl` entries are not tied to a contract address, so they are left out |
| `--change` | `created`, `updated`, `removed`, `restored` | Repeatable; default all |
| `--entry` | `contract_data`, `contract_code`, `ttl` | Repeatable; default all |
| `--key <ScVal>` | base64 XDR `xdr.ScVal`, repeatable | Keeps `contract_data` entries whose storage key equals one of these values. Like `--contract`, it leaves out `contract_code` and `ttl` |

The filters combine with AND: a record must pass every filter you set. Values
within one repeatable flag combine with OR.

### Filtering by storage key (`--key`)

`--key` takes the storage key as a base64-encoded XDR `ScVal`. It is decoded
and re-encoded before matching, so whitespace or padding differences don't
matter, and a value that isn't a valid ScVal is rejected on startup.

**Watch admin and config changes.** Instance storage is a single entry keyed by
`AAAAFA==`:

```sh
prism storage --contract CBGSB... --key AAAAFA== --follow --json
```

**Track one user's token balance.** Soroban token contracts (including the
Stellar Asset Contract) store balances under `Balance(address)`, a `Vec` of
the symbol `Balance` and the holder's address. Build the ScVal with the
`stellar` CLI or any XDR library, base64-encode it, and pass it to `--key`:

```sh
# Using the Go SDK, for example:
#   key := xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}   // [Symbol("Balance"), Address(G...)]
#   b64, _ := xdr.MarshalBase64(key)
BALANCE_KEY='AAAAEAAAAAEAAAACAAAADwAAAAdCYWxhbmNlAAAAABIAAAAA...'

prism storage --contract CDLZF... --key "$BALANCE_KEY" --follow --json \
  | jq -c '{ledger, changeType, balance: .entry.value.Data.ContractData.Val.I128}'
```

If you would rather not build XDR, filter the decoded key client-side
instead. It costs some extra processing, but the stream is the same:

```sh
prism storage --contract CDLZF... --entry contract_data --follow --json \
  | jq -c 'select(.entry.value.Data.ContractData.Key.Vec[0].Sym == "Balance")'
```

`--key` is part of the stream's identity in `--state-file`, so two runs with
different keys keep separate cursors in the same file, just as different
`--contract` values do.

---

## State diffing

A storage stream is a changelog. To turn it into before/after diffs, keep a map
from storage key to last-seen value and compare each record against it. The
`keyXdr` field (or `key` when decoded) is a stable map key for every entry type.

```sh
prism storage --contract CBGSB... --entry contract_data --follow --json --raw \
  | jq -c --unbuffered '{k: .keyXdr, t: .changeType, v: .entryXdr, ledger}' \
  | python3 diff_state.py
```

```python
# diff_state.py: print a delta for every storage transition
import json, sys

state = {}  # keyXdr -> entryXdr
for line in sys.stdin:
    rec = json.loads(line)
    key, change = rec["k"], rec["t"]
    before = state.get(key)
    if change == "removed":
        state.pop(key, None)
        after = None
    else:
        after = rec["v"]
        state[key] = after
    print(json.dumps({"ledger": rec["ledger"], "change": change,
                      "key": key, "before": before, "after": after}))
```

Some things to know:

- **Seed the map before you go live.** A map started from "now" knows nothing
  about entries written earlier, so the first `updated` record for each key
  will have no `before`. Either backfill with `--from <ledger>` (bounded by the
  RPC's retention window) or load the current values with `getLedgerEntries`
  first.
- **Persist the map along with the cursor.** Run with `--state-file` and
  `--resume`, and save your map whenever prism checkpoints, so that a restart
  replays from a position your map agrees with. prism delivers records
  at least once across restarts. Make your map updates idempotent, for example
  by keying on `id` or ignoring records at or below the last ledger you applied.
- **Removal versus expiry.** A `removed` record is an explicit delete.
  When a TTL runs out, nothing is written to the ledger, so no record appears.
  Use the TTL projections below to track expiry yourself.

---

## TTL remaining projections

Every `ttl` record that has an entry (`created`, `updated`, `restored`) gets
three extra fields, computed in `StorageChangeSource.extract`:

```
liveUntilLedger = TtlEntry.LiveUntilLedgerSeq
remainingTtl    = liveUntilLedger - ledger      (0 if liveUntilLedger <= ledger)
ttlWarning      = remainingTtl < 1000
```

`ledger` is the ledger the change was applied in, so `remainingTtl` is the
number of ledgers left *at the time of the change*, not at the time you read
it. Human output shows the same data inline:

```
2026-11-20T09:14:05Z  ledger 512340  updated  ttl  key 9f3a…c21e  live until 3622340 (3110000 ledgers)
2026-11-20T09:15:00Z  ledger 512351  updated  ttl  key 77be…04d1  live until 513100 (749 ledgers)  TTL WARNING
```

### Estimating an expiration date

prism reports ledgers, not wall-clock time: ledger close times vary, and turning
ledgers into a date means assuming an average close time. Mainnet targets about
5 seconds per ledger, so a rough estimate is:

```
expires_at ≈ time + remainingTtl × 5s
```

```sh
prism storage --entry ttl --follow --json \
  | jq -c 'select(.remainingTtl != null) | {
      keyHash, liveUntilLedger, remainingTtl, ttlWarning,
      expiresAt: ((.time | fromdateiso8601) + .remainingTtl * 5 | todateiso8601)
    }'
```

For a better estimate, measure the recent average close time with
`prism ledgers --follow --json` and use that in place of 5.

### Linking a TTL back to its entry

A `ttl` record names its target only by `keyHash`, the SHA-256 of the
target's `LedgerKey` XDR. To join TTLs to `contract_data` records, hash each
data record's `keyXdr` the same way:

```sh
# keyHash of a contract_data record's LedgerKey
echo -n "$KEY_XDR" | base64 -d | sha256sum | cut -d' ' -f1
```

The RPC does not link TTL key hashes to contract IDs. That is why `--contract`
and `--key` drop `ttl` records: prism cannot tell which contract owns them
without the original key. If you want TTLs and data together, run without those
two filters (or `--entry contract_data --entry ttl`) and join on the hash
yourself.

### Alerting before archival

To catch persistent entries before they are archived:

```sh
prism storage --entry ttl --follow --json \
  | jq -c --unbuffered 'select(.ttlWarning == true)' \
  | while read -r rec; do notify "TTL low: $rec"; done
```

`prism contracts --watch C...` applies the same 1,000-ledger warning in a
per-contract lifecycle view. See the README.

---

## Recipes

```sh
# Every storage mutation, human-readable
prism storage --follow

# One contract's persistent data, created or updated only
prism storage --contract CBGSB... --change created --change updated --follow

# Admin/config changes of one contract (instance storage)
prism storage --contract CBGSB... --key AAAAFA== --follow --json

# New WASM uploads and code removals
prism storage --entry contract_code --json | jq -r '.wasmHash'

# Restorations of archived state
prism storage --change restored --follow --json

# Resumable indexer feed
prism storage --contract CBGSB... --follow --json \
  --state-file /var/lib/prism/storage.state --resume
```
