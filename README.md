# DEX Trades and AMM Swaps

`prism trades` streams decentralized-exchange activity from the Stellar network and normalizes each fill into a single **trade record**. It covers three sources:

| Source | Where it comes from | Amount units |
|---|---|---|
| Classic order book | Claim atoms in `ManageSellOffer`, `ManageBuyOffer`, `CreatePassiveSellOffer`, and path payment results | Decimal (7 dp) |
| Classic AMM (liquidity pools) | `ClaimAtomTypeLiquidityPool` claim atoms + pool reserve changes in ledger entry changes | Decimal (7 dp) |
| Soroban swaps | Contract events whose topics are `swap`, trader, input asset, output asset | **Raw token units** |

```bash
prism trades --pair XLM/USDC --follow
```

> ⚠ Verify: the README documents `--pair`; the issue asks for `--asset-pair`. Document whichever the code accepts, and add an alias if both should work.

---

## 1. Trade record structure

Each fill produces one record. Human output prints one line per trade; `--json` emits newline-delimited JSON (NDJSON), one object per line.

### Fields

| JSON key | Type | Description |
|---|---|---|
| `ledger` | number | Ledger sequence the trade closed in |
| `tx_hash` | string | Hash of the transaction that produced the trade |
| `source` | string | `orderbook`, `liquidity_pool`, or `soroban` |
| `seller` | string | Account (`G...`) whose offer was filled; for pool trades, the pool |
| `buyer` | string | Account (`G...`) or contract (`C...`) that took liquidity |
| `sold_asset` | string | Asset the seller gave up, as `CODE:ISSUER` or `native` |
| `sold_amount` | string | Amount of `sold_asset` |
| `bought_asset` | string | Asset the seller received |
| `bought_amount` | string | Amount of `bought_asset` |
| `price` | object | `{ "n": int, "d": int }`, the price as a fraction (bought ÷ sold) |
| `liquidity_pool_id` | string \| omitted | Hex pool ID; only present for AMM trades |

> ⚠ Verify every key name against the trade struct's JSON tags. Amounts are strings so 7-decimal precision and large raw `i128` values survive `jq`.

### Human output

```text
LEDGER    SOURCE          SOLD                 BOUGHT               PRICE
51234567  orderbook       120.0000000 XLM      13.2000000 USDC      0.1100000
51234567  liquidity_pool  500.0000000 XLM      54.9500000 USDC      0.1099000  pool=4f3c…a91e
```

### JSON (NDJSON)

```bash
prism trades --pair XLM/USDC --json
```

```json
{"ledger":51234567,"tx_hash":"a1b2…","source":"orderbook","seller":"GCSELLER…","buyer":"GDBUYER…","sold_asset":"native","sold_amount":"120.0000000","bought_asset":"USDC:GA5Z…","bought_amount":"13.2000000","price":{"n":11,"d":100}}
{"ledger":51234567,"tx_hash":"c3d4…","source":"liquidity_pool","seller":"4f3c…a91e","buyer":"GDTRADER…","sold_asset":"native","sold_amount":"500.0000000","bought_asset":"USDC:GA5Z…","bought_amount":"54.9500000","price":{"n":1099,"d":10000},"liquidity_pool_id":"4f3c…a91e"}
```

---

## 2. Filtering by asset pair

Restrict the stream to a single market:

```bash
prism trades --pair XLM/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN --follow
```

Pair syntax:

- `XLM` or `native` is the native lumen.
- `CODE:ISSUER` is a credit asset. Include the issuer: many accounts issue tokens called `USDC`.
- A bare code (`XLM/USDC`) is shorthand. ⚠ Verify how the code resolves a bare credit code; if it matches any issuer, warn readers that results can include look-alike assets.
- The match ignores direction: `XLM/USDC` matches trades in both directions.

> ⚠ Verify: whether filtering by pool ID (`--pool <id>`) exists. If it doesn't, list it as roadmap.

---

## 3. How trades are detected

### 3.1 Classic order-book trades

Every offer-crossing operation returns a list of **claim atoms** in its result:

- `ManageSellOfferResult.Success.OffersClaimed`
- `ManageBuyOfferResult.Success.OffersClaimed`
- `PathPaymentStrictSendResult.Success.Offers`
- `PathPaymentStrictReceiveResult.Success.Offers`

Each `ClaimAtomTypeOrderBook` (or the legacy `ClaimAtomTypeV0`) carries the seller, offer ID, asset/amount sold, and asset/amount bought. `prism` emits one trade record per atom: the seller comes from the atom, the buyer is the operation's source account (or the transaction's source when the operation has none), and `price` is `amount_bought / amount_sold` reduced to a fraction.

Path payments can cross several markets in one operation. Each hop produces its own atoms, so a single path payment can produce several trade records with the same `tx_hash`.

### 3.2 AMM liquidity-pool trades

When a trade crosses a constant-product pool (CAP-38), its claim atom is `ClaimAtomTypeLiquidityPool`. It has no seller account, only a `LiquidityPoolId`. `prism`:

1. Reads the atom's `LiquidityPoolId`, `AssetSold`/`AmountSold`, and `AssetBought`/`AmountBought`.
2. Sets `source` to `liquidity_pool`, sets `liquidity_pool_id`, and uses the pool ID as `seller`.
3. Cross-checks the operation's **ledger entry changes**: the `LedgerEntryTypeLiquidityPool` entry shows up as `STATE` (before) then `UPDATED` (after). The difference in `ReserveA` and `ReserveB` should match the atom's amounts, less the pool fee (30 bps).

> ⚠ Verify: whether step 3 is implemented (reserve-delta validation, or reserve fields in the output). If not, mark it as roadmap and don't document a reserve field.

### 3.3 Soroban swap events

Soroban AMMs don't produce claim atoms. `prism` recognizes contract events whose topics are:

```
["swap", <trader address>, <input asset>, <output asset>]
```

and whose data holds the input and output amounts. They become records with `source: "soroban"`, `buyer` = trader, `sold_asset` = input, `bought_asset` = output.

**Amounts stay in raw token units.** `prism` doesn't look up each contract's `decimals()`, so a 7-decimal SAC token shows `1000000000` for 100 units. Scale them yourself (see the recipes below).

> ⚠ Verify: whether `liquidity_pool_id` or the emitting contract ID is set for Soroban records, and under which key.

---

## 4. jq recipes

Only AMM trades:

```bash
prism trades --pair XLM/USDC --json | jq -c 'select(.liquidity_pool_id != null)'
```

Volume per pool:

```bash
prism trades --json | jq -s 'map(select(.liquidity_pool_id)) | group_by(.liquidity_pool_id)
  | map({pool: .[0].liquidity_pool_id, trades: length, sold: (map(.sold_amount|tonumber)|add)})'
```

Decimal price from the fraction:

```bash
prism trades --pair XLM/USDC --json | jq -r '"\(.ledger) \(.price.n / .price.d)"'
```

Scale a 7-decimal Soroban token's raw amounts:

```bash
prism trades --json | jq -c 'select(.source=="soroban")
  | .sold_amount = ((.sold_amount|tonumber) / 1e7) | .bought_amount = ((.bought_amount|tonumber) / 1e7)'
```

Large trades only (more than 10,000 XLM sold):

```bash
prism trades --pair XLM/USDC --json --follow | jq -c 'select(.sold_asset=="native" and (.sold_amount|tonumber) > 10000)'
```

---

## 5. Roadmap

- Decimal scaling for Soroban amounts via each token contract's `decimals()`
- Pool-ID filter (`--pool`)
- OHLC / candle aggregation

> ⚠ Before merging, remove any roadmap item the code already implements.

---

## 6. Documentation

- [CLI Command Reference](docs/cli/command_reference.md)
- [Configuration Options](docs/cli/config_options.md)
- [Architecture Overview](docs/architecture.md)
- [Event Deduplication Internals](docs/architecture/deduplication_internals.md)
- [Fee Stats Streaming Guide](docs/guides/fee_stats_streaming.md)
- [Troubleshooting Streaming Gaps and RPC Lag](docs/troubleshooting/streaming_gaps_and_lag.md)
- [Performance Tuning and Profiling](docs/performance/tuning_and_profiling.md)
- [KMS and State File Encryption](docs/security/kms_and_state_encryption.md)
- [Authoring Custom Sinks](docs/contributing/authoring_custom_sinks.md)
