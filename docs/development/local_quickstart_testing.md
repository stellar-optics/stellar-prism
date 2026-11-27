# Local Development and Testing with Soroban Quickstart

This guide explains how to run a local standalone Stellar network with `stellar/quickstart:testing`, deploy and invoke a sample Soroban contract using `stellar-cli`, and stream live contract events with `stellar-prism`.

---

## Prerequisites

- **Docker** (running daemon)
- **Go 1.25+** (or a built `prism` binary via `go build -o prism ./cmd/prism`)
- **`stellar` CLI (`stellar-cli`)** for creating standalone keys, compiling, deploying, and invoking contracts
- **Rust + `wasm32v1-none` target** (only required if compiling a custom Rust Soroban contract)

---

## 1. Start Soroban Quickstart in Standalone Mode

Launch the official `stellar/quickstart:testing` image in `--standalone` mode with Soroban RPC (and Horizon + Friendbot) exposed on port `8000`:

```sh
docker run --rm -it \
  --name stellar-quickstart \
  -p 8000:8000 \
  stellar/quickstart:testing \
  --standalone \
  --enable-soroban-rpc \
  --enable-soroban-diagnostic-events
```

> [!TIP]
> Add `-d` instead of `-it` to run the container in the background (`docker logs -f stellar-quickstart` to inspect output, `docker stop stellar-quickstart` to stop).

Wait until the local RPC responds with a closed ledger (typically 15–30 seconds):

```sh
curl -sS -X POST http://localhost:8000/soroban/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}'
```

Once `result.sequence` is greater than `0`, both Soroban RPC (`http://localhost:8000/soroban/rpc`) and Horizon/Friendbot (`http://localhost:8000/friendbot`) are ready.

---

## 2. Configure `stellar-cli` and Deploy a Test Contract

In a new terminal, register the standalone network profile in `stellar-cli` and generate funded test accounts:

```sh
stellar network add standalone \
  --rpc-url http://localhost:8000/soroban/rpc \
  --network-passphrase "Standalone Network ; February 2017"

stellar keys generate alice --network standalone --fund
stellar keys generate bob --network standalone --fund
```

### Option A: Compile and Deploy a Sample Soroban Contract

1. Scaffold a sample Soroban workspace (or use an existing token contract project):

   ```sh
   stellar contract init ./quickstart-token-demo
   cd quickstart-token-demo
   ```

2. Compile the contract to WebAssembly:

   ```sh
   stellar contract build
   ```

3. Deploy the compiled `.wasm` to your standalone network and save its contract ID under an alias:

   ```sh
   stellar contract deploy \
     --wasm target/wasm32v1-none/release/*.wasm \
     --source-account alice \
     --network standalone \
     --alias demo_contract
   ```

   Print the deployed contract ID (`C...`) for `prism`:

   ```sh
   CONTRACT_ID="$(stellar contract id --id demo_contract --network standalone)"
   echo "$CONTRACT_ID"
   ```

### Option B: Deploy the Built-in Stellar Asset Contract (No Rust Build Needed)

If you want to emit Soroban token events immediately without compiling Rust, deploy the Stellar Asset Contract (SAC) wrapper for native XLM:

```sh
CONTRACT_ID="$(
  stellar contract asset deploy \
    --asset native \
    --source-account alice \
    --network standalone \
    --alias xlm_sac
)"
echo "$CONTRACT_ID"
```

---

## 3. Tail Local Events with `stellar-prism`

Start `prism events` pointing at the local Quickstart RPC endpoint. Pass `--network standalone` so `prism` validates the RPC's network passphrase (`"Standalone Network ; February 2017"`) via `getNetwork`:

```sh
prism events \
  --rpc-url http://localhost:8000/soroban/rpc \
  --network standalone \
  --contract "$CONTRACT_ID" \
  --poll 1s \
  --follow
```

> [!NOTE]
> `prism` uses `--network standalone` (matching the `"Standalone Network ; February 2017"` preset in `pkg/rpc/network.go`). Use `--insecure-skip-network-check` only when pointing at a custom private network with a non-standard passphrase.

For structured NDJSON output suitable for `jq`:

```sh
prism events \
  --rpc-url http://localhost:8000/soroban/rpc \
  --network standalone \
  --contract "$CONTRACT_ID" \
  --poll 1s \
  --follow \
  --json
```

### Trigger Contract Events and Verify Immediate Output

In another terminal, invoke a method on the deployed contract. For the XLM Stellar Asset Contract (`Option B`), execute a `transfer` from `alice` to `bob`:

```sh
stellar contract invoke \
  --id xlm_sac \
  --source-account alice \
  --network standalone \
  -- \
  transfer \
  --from "$(stellar keys address alice)" \
  --to "$(stellar keys address bob)" \
  --amount 10000000
```

Within one poll interval (`--poll 1s`), `prism events` prints the decoded event in the terminal:

```text
2026-11-24T12:05:10Z  ledger 42  contract  CDLZF…CYSC  tx 9a1f…3c82
  topic[0]
    ScVal
    ├─ Type: ScValTypeScvSymbol
    └─ Sym: transfer
  topic[1]
    ScVal
    ├─ Type: ScValTypeScvAddress
    └─ Address
       ├─ Type: ScAddressTypeScAddressTypeAccount
       └─ AccountId: G...
  ...
  value
    ScVal
    ├─ Type: ScValTypeScvI128
    └─ I128
       ├─ Hi: 0
       └─ Lo: 10000000
```

For a custom contract (`Option A`), invoke the exported contract function with `stellar contract invoke --id demo_contract --source-account alice --network standalone -- <function> [args...]` and confirm the emitted event appears in the `prism` terminal.

---

## 4. Running `prism`'s End-to-End Test Suite

`internal/cli/e2e_test.go` includes live integration tests behind the `e2e` build tag. With your Quickstart container already running on port `8000`, run the suite against it via `PRISM_E2E_RPC_URL`:

```sh
PRISM_E2E=1 \
PRISM_E2E_RPC_URL=http://localhost:8000/soroban/rpc \
go test -tags e2e -race -count=1 ./internal/cli -run TestE2E -v
```

Or let `make test-e2e` manage a container lifecycle automatically when a compose file is configured (`PRISM_E2E_COMPOSE_FILE`):

```sh
make test-e2e
```

---

## Troubleshooting

- **Network passphrase mismatch error (`network mismatch: --network testnet expects ...`)**: `prism` defaults to `--network testnet`. Always pass `--network standalone` when targeting `stellar/quickstart --standalone`.
- **Container resets ledger history on restart**: `--rm` starts a fresh chain from ledger 1 each run. Pass `--from 1` (or omit `--from` to start from the current head) and avoid reusing `--state-file` from a prior ephemeral container.
- **No events shown immediately after `invoke`**: Ensure `--contract` matches the exact `C...` ID returned at deploy time, and include `--poll 1s` so `prism` polls every second while following.
