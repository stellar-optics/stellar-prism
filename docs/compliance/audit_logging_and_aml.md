# Automated Compliance, Audit Logging, and AML Monitoring

Financial institutions, regulated Stellar anchors, and compliance teams use `stellar-prism` to stream ledger activity into Anti-Money Laundering (AML) monitoring pipelines and produce immutable, tamper-evident audit trails.

This guide covers:

- [Audit Log Sink Configuration](#audit-log-sink-configuration)
- [Verifying Audit Trail Integrity](#verifying-audit-trail-integrity)
- [Monitoring Regulated Operations for AML](#monitoring-regulated-operations-for-aml)
- [Data Retention, Encryption, and Cold Storage Compliance](#data-retention-encryption-and-cold-storage-compliance)

---

## Audit Log Sink Configuration

The audit sink ([`pkg/sink/audit_log.go`](../../pkg/sink/audit_log.go)) runs alongside your primary output or downstream sinks via `--sink-audit`. In `internal/cli/sinks.go`, the audit sink is attached before network-facing sinks (webhooks, object storage, message brokers) so every record delivered downstream is recorded locally first.

### How SHA-256 Hash Chaining Works

Each audit log is an append-only NDJSON file made up of three entry types:

1. **`audit_header`** (first line): Records the format version (`1`), hash algorithm (`sha256`), signature algorithm (`ed25519`), base64-encoded Ed25519 public key (`publicKey`), creation timestamp (`createdAt`), and a genesis SHA-256 `hash` computed over those header fields. Because the genesis hash covers the header, the header cannot be replaced without breaking every subsequent link.
2. **`audit_record`**: Wraps one streamed record (`record`, identical in schema to `--json` / `sink.MarshalRecord`) with its monotonic sequence number (`seq`), `ledger`, `time`, `recordId`, `recordKind`, `prevHash`, and `hash`. Each record hash links to the preceding digest:

   ```text
   Hash_n = SHA256(Hash_{n-1} || JSON(seq, ledger, time, recordId, recordKind, record))
   ```

   Modifying, deleting, or inserting any record invalidates `Hash_n` and every `prevHash` after it.
3. **`audit_checkpoint`**: Written every `--sink-audit-checkpoint` records (default `100`; `0` writes a checkpoint after every record) and on clean sink close. It records `seq`, `ledger`, `time`, `hash`, and an Ed25519 `signature` over the raw 32-byte chain hash. Signed checkpoints prevent an attacker from editing an early record and recomputing the entire SHA-256 chain from line 1.

### Signing Key Management and Resumption

| Flag | Default | Purpose |
|---|---|---|
| `--sink-audit <path>` | *(disabled)* | Path to the tamper-evident audit trail file |
| `--sink-audit-key <path>` | `<audit-file>.key` | PEM-encoded PKCS#8 Ed25519 private key used to sign checkpoints |
| `--sink-audit-checkpoint <n>` | `100` | Records between signed checkpoints (`0` signs after every record) |

If `--sink-audit-key` does not exist yet, `NewAuditSink` generates an Ed25519 key pair and creates the private key file exclusively (`O_CREATE|O_EXCL`) with permissions `0600`.

Across restarts, the audit sink is strictly append-only:

- It reads the existing header and verifies that `header.publicKey` matches the configured signing key; logs written with a different key are refused.
- If a prior run crashed mid-write and left an unterminated trailing line, `NewAuditSink` truncates back to the last complete newline before resuming `seq` and the chain hash.

### Example: Auditing a Regulated Stream

```sh
prism operations \
  --network mainnet \
  --type clawback --type set_trustline_flags --type payment \
  --follow \
  --state-file /var/lib/prism/aml-ops.state \
  --resume \
  --sink-audit /var/log/prism/aml-audit.log \
  --sink-audit-key /etc/prism/keys/audit-signing.key \
  --sink-audit-checkpoint 50 \
  --json
```

> [!IMPORTANT]
> Keep the Ed25519 private signing key in a restricted directory (or hardware-backed store) separate from the log output directory so a compromise of log storage does not expose the signing key.

---

## Verifying Audit Trail Integrity

Use `prism audit-verify` ([`pkg/sink/audit_verify.go`](../../pkg/sink/audit_verify.go), [`internal/cli/audit.go`](../../internal/cli/audit.go)) to validate a log end to end.

> [!NOTE]
> `prism audit-verify` verifies cryptographic hash chains and Ed25519 checkpoints produced by `--sink-audit`. The separate `prism audit` command checks plain NDJSON stream dumps for ledger ordering and duplicate IDs without cryptographic verification.

### Pinning the Public Key

Export the trail's Ed25519 public key and store it out-of-band (for example, in read-only configuration management or an auditor's workstation) so verification does not trust an attacker-modified header:

```sh
# Export the PEM public key to stdout (verification report goes to stderr)
prism audit-verify --print-public-key /var/log/prism/aml-audit.log > /etc/prism/audit.pub

# Verify against the pinned public key
prism audit-verify --public-key /etc/prism/audit.pub /var/log/prism/aml-audit.log
```

`prism audit-verify` also reads from `--file` or `stdin`:

```sh
cat /var/log/prism/aml-audit.log | prism audit-verify --public-key /etc/prism/audit.pub
```

### Exit Codes and Detected Anomalies

- Exit `0`: The chain, sequence numbers, and signed checkpoints are intact (`audit-verify: the trail is intact`).
- Exit `1`: One or more error-severity anomalies were detected (`audit-verify: the trail was tampered with`), with exact 1-indexed line numbers printed for each finding.

| Anomaly Type | Meaning |
|---|---|
| `modified` | A record's recomputed SHA-256 digest does not match its stored `hash` |
| `deleted` | Sequence numbers skip ahead (`record.seq > expected`) |
| `inserted` | Sequence numbers go backwards (`record.seq < expected`) or a second header exists |
| `chain_break` | `record.prevHash` does not match the preceding entry's hash |
| `checkpoint_mismatch` | Checkpoint `seq` or `hash` disagrees with the preceding chain |
| `bad_signature` | Checkpoint Ed25519 signature fails verification |
| `header_mismatch` | Unsupported version/algorithm or modified header hash |
| `key_mismatch` | Header `publicKey` differs from `--public-key` |
| `malformed` | Invalid JSON, missing header, or unreadable hash/entry |

Missing checkpoints or empty logs are reported as `warning` severity so unsigned partial chains remain inspectable, while every tampering anomaly above carries `error` severity and fails verification.

---

## Monitoring Regulated Operations for AML

`prism operations` decodes every transaction envelope into individual operation records (`kind: "operation"`), with operation type names normalized by `pkg/stream/operations.go` and XDR parameters decoded by `stellar-xdr-lens` when `--json` is used without `--raw`.

### Classic Operation Types for Asset Issuers and Anchors

| Stellar XDR Operation | `--type` Flag Value | Compliance / AML Use Case |
|---|---|---|
| `ClawbackOp` | `clawback` | Asset clawbacks ordered by compliance or legal recovery |
| `ClawbackClaimableBalanceOp` | `clawback_claimable_balance` | Clawbacks of unclaimed regulated asset balances |
| `SetTrustLineFlagsOp` | `set_trustline_flags` | Account authorization, freeze/revocation, or clawback flag changes |
| `PaymentOp` | `payment` | Direct value transfers subject to threshold/velocity monitoring |
| `PathPaymentStrictReceiveOp` / `PathPaymentStrictSendOp` | `path_payment_strict_receive`, `path_payment_strict_send` | Cross-asset / DEX-routed value transfers |

### Recipe 1: Tracking `ClawbackOp` and `SetTrustLineFlagsOp`

Stream all clawbacks and trustline flag transitions to an audit trail and a signed webhook alerting endpoint:

```sh
export PRISM_WEBHOOK_SECRET="$(cat /etc/prism/webhook.secret)"

prism operations \
  --network mainnet \
  --type clawback \
  --type clawback_claimable_balance \
  --type set_trustline_flags \
  --follow \
  --json \
  --state-file /var/lib/prism/regulated-ops.state \
  --resume \
  --sink-audit /var/log/prism/regulated-ops-audit.log \
  --webhook-url https://aml.internal.example/v1/stellar-alerts \
  --webhook-batch-size 25 \
  --webhook-timeout 2s \
  --dlq-file /var/log/prism/regulated-ops-dlq.ndjson \
  --max-record-retries 3
```

Downstream webhook receivers should verify the `X-Prism-Signature: sha256=<hex>` header (`HMAC-SHA256(secret, "<X-Prism-Timestamp>.<body>")`) and deduplicate using `X-Prism-Delivery`.

### Recipe 2: Threshold Alerting on Large-Volume `PaymentOp`

In `prism operations --json`, each record includes `type`, `sourceAccount`, `tx`, `resultCode`, and decoded `parameters` (wrapped in the `stellar-xdr-lens` `{type, value}` envelope). Pipe `prism operations --type payment --json` into `jq` to filter successful payments meeting or exceeding a stroop threshold (for example, `100000000000` stroops = `10,000` units at 7 decimals):

```sh
prism operations \
  --network mainnet \
  --type payment \
  --follow \
  --json \
  --sink-audit /var/log/prism/payments-audit.log \
| jq -c '
  select(
    .resultCode == "op_success" and
    ((.parameters.value.PaymentOp.Amount // 0) | tonumber) >= 100000000000
  )
  | {
      ledger,
      time,
      tx,
      source: .sourceAccount,
      destination: .parameters.value.PaymentOp.Destination,
      asset: .parameters.value.PaymentOp.Asset,
      amountStroops: .parameters.value.PaymentOp.Amount
    }'
```

For high-throughput AML screening, publish the full `prism operations --type payment --json` stream to Kafka (`--kafka-brokers`, `--kafka-topic`, `--kafka-partition-by tx`) or Elasticsearch/OpenSearch (`--sink-es-url`, `--sink-es-index aml-payments`) and evaluate velocity/structuring rules downstream while keeping `--sink-audit` active locally.

### Recipe 3: Monitoring Soroban Token Transfers and Clawbacks

For Soroban Stellar Asset Contracts (SAC) and SEP-41 tokens, `prism events` supports `--filter-expr` directly on decoded `ScVal` payloads:

```sh
prism events \
  --network mainnet \
  --contract C... \
  --topic-pattern 'transfer|clawback,*' \
  --filter-expr 'topics[0].symbol == "clawback" || (topics[0].symbol == "transfer" && value.amount >= 100000000000)' \
  --follow \
  --json \
  --sink-audit /var/log/prism/soroban-aml-audit.log
```

---

## Data Retention, Encryption, and Cold Storage Compliance

Regulated financial workloads typically require multi-year immutable archival (for example, 5–7 years under BSA/FinCEN or MiCA record-keeping rules) plus encryption at rest and in transit.

### Archiving Immutable Chunks to S3 or GCS

`prism` supports direct chunked archival via `pkg/sink/cloud_storage.go`, `pkg/sink/s3.go`, and `pkg/sink/gcs.go`. Objects are written deterministically under ledger-partitioned keys:

```text
<prefix>/<YYYY>/<MM>/<DD>/ledger-<start>-<end>/<firstID>-<lastID>.ndjson.gz
```

Because keys derive strictly from record timestamps and IDs, upload retries overwrite the same object rather than creating duplicates.

#### AWS S3 with Object Lock (WORM) and Glacier Lifecycle

1. Create an S3 bucket with **S3 Object Lock** enabled (Compliance mode for regulatory immutability, or Governance mode during validation) and default **SSE-KMS** encryption using a customer-managed AWS KMS key.
2. Add an S3 Lifecycle rule transitioning archived objects under your prefix (e.g., `prism/mainnet/`) to **S3 Glacier Flexible Retrieval** after 30 days or **S3 Glacier Deep Archive** after 90 days, aligned with your retention period.
3. Supply credentials via environment variables (never CLI flags) and stream:

```sh
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."

prism operations \
  --network mainnet \
  --type clawback --type set_trustline_flags --type payment \
  --follow \
  --cloud-provider s3 \
  --cloud-url https://s3.us-east-1.amazonaws.com \
  --cloud-region us-east-1 \
  --cloud-bucket stellar-compliance-archive \
  --cloud-prefix prism/mainnet/operations \
  --cloud-batch-records 5000 \
  --cloud-batch-timeout 5m \
  --cloud-compress \
  --sink-audit /var/log/prism/ops-audit.log
```

#### Google Cloud Storage (GCS) with Bucket Lock and Archive Class

1. Create a GCS bucket with a locked **Retention Policy (Bucket Lock)** matching your compliance horizon, **Autoclass** or lifecycle transition to the **Archive** storage class, and **Customer-Managed Encryption Keys (CMEK)** via Cloud KMS.
2. Provide a short-lived OAuth 2.0 bearer token via `GOOGLE_OAUTH_ACCESS_TOKEN`:

```sh
export GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)"

prism operations \
  --network mainnet \
  --type clawback --type set_trustline_flags --type payment \
  --follow \
  --cloud-provider gcs \
  --cloud-bucket stellar-compliance-archive \
  --cloud-prefix prism/mainnet/operations \
  --cloud-batch-records 5000 \
  --cloud-batch-timeout 5m \
  --cloud-compress
```

### Encrypting Cursor State Files

When `--state-file` persists stream checkpoints on shared filesystems or ephemeral hosts, enable authenticated **AES-256-GCM** encryption (`pkg/stream/crypto_state.go`, `pkg/stream/kms.go`) so cursor metadata is protected at rest:

```sh
# Using AWS KMS envelope encryption (plaintext data key never touches disk)
prism operations \
  --network mainnet \
  --follow \
  --state-file /var/lib/prism/ops.state.enc \
  --resume \
  --state-kms-provider aws \
  --state-kms-key-id arn:aws:kms:us-east-1:123456789012:key/your-key-id \
  --state-kms-region us-east-1
```

GCP Cloud KMS (`--state-kms-provider gcp` with `GOOGLE_OAUTH_ACCESS_TOKEN`) and passphrase-based encryption (`--state-passphrase-env` or `--state-passphrase-file`) are also supported; pass exactly one key source.

### Checklist for Production Compliance Deployments

- Run `prism audit-verify --public-key <pinned.pub> <audit.log>` on a scheduled timer and alert on non-zero exit codes.
- Ship local `--sink-audit` files (and rotated `.ndjson.gz` outputs) to WORM cold storage on a schedule or replicate via `--cloud-provider`.
- Combine `--ha-lease-file` with encrypted `--state-file` and `--health-port` when running active-passive redundant collectors so failover does not duplicate or drop audited records.
- Protect `--sink-audit-key` with `0600` permissions and store the exported public key separately from the audit log host.
