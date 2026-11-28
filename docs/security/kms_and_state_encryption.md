# State File Encryption and KMS Integration

`--state-file` records the stream cursor so a later run can `--resume` exactly
where the last one stopped. That journal names the contracts and filters you
watch and how far you have read, and it decides which records the next run
emits. On a shared volume, in a multi-tenant cluster or under a compliance
regime, you may need to keep it confidential and be able to show it hasn't been
tampered with.

prism can write the journal encrypted with **AES-256-GCM**. The key comes from
one of three places: a passphrase in an environment variable, a passphrase
file, or a cloud key management service (AWS KMS or Google Cloud KMS) using
envelope encryption.

Implementation:

- [`pkg/stream/crypto_state.go`](../../pkg/stream/crypto_state.go):
  `EncryptedStateStore`, `KeySource`, `PassphraseKeySource`, `KMSKeySource`
- [`pkg/stream/kms.go`](../../pkg/stream/kms.go): `KMSClient`,
  `AWSKMSClient`, `GCPKMSClient`
- [`internal/cli/runtime.go`](../../internal/cli/runtime.go): flag to key
  source wiring (`stateKeySource`, `kmsKeySource`)

See also [SECURITY.md](../../SECURITY.md) and
[docs/architecture.md](../architecture.md#encrypted-state-files-pkgstreamcrypto_statego).

---

## Quick reference

Encryption flags apply to any streaming command that accepts `--state-file`
(`events`, `serve`, `transactions`, `storage`, `operations`, `wasm`, ...). Pick **exactly
one** key source. Giving two is an error, not a silent override.

| Key source | Flags | Credentials read from |
|---|---|---|
| Passphrase (env) | `--state-passphrase-env NAME` | `$NAME` |
| Passphrase (file) | `--state-passphrase-file PATH` | the file (trailing newline trimmed) |
| AWS KMS | `--state-kms-provider aws --state-kms-key-id ARN [--state-kms-region R]` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` |
| GCP KMS | `--state-kms-provider gcp --state-kms-key-id projects/.../cryptoKeys/K [--state-kms-token-env NAME]` | `$GOOGLE_OAUTH_ACCESS_TOKEN` (or `$NAME`) |

The encryption flags require `--state-file`. Passing them without it is an
error.

---

## Encryption architecture

### File format

An encrypted state file is NDJSON: a plaintext header line followed by one
sealed envelope per checkpoint.

```
{"version":1,"alg":"aes-256-gcm","key":{"kdf":"aws-kms","wrapped":"AQIDAHh..."}}
{"version":1,"nonce":"r2Vd0J...","ciphertext":"8bq1mX..."}
{"version":1,"nonce":"Qk4Hn1...","ciphertext":"Zt9aPw..."}
```

The **header** says how to rebuild the key, never the key itself:

| `key.kdf` | Other fields | Meaning |
|---|---|---|
| `pbkdf2-hmac-sha256` | `salt`, `iterations` | Key derived from a passphrase |
| `aws-kms` | `wrapped` | Data key wrapped by AWS KMS |
| `gcp-kms` | `wrapped` | Data key wrapped by Cloud KMS |

Each **envelope** is one `CursorMetadata` checkpoint, JSON-encoded and then
sealed:

- **Cipher:** AES-256-GCM (`crypto/aes` + `crypto/cipher.NewGCM`), so each
  entry is both encrypted and authenticated.
- **Nonce:** 96 bits from `crypto/rand`, new for every entry. With a random
  96-bit nonce, a repeat under one key is vanishingly unlikely even after many
  writes.
- **Additional authenticated data:** the fixed label `prism.state.v1`. A
  ciphertext won't open in any context that uses a different label.
- **File mode:** created `0600`. Pruning rewrites to a temp file in the same
  directory and renames it over the original, keeping the existing mode, so
  a reader sees either the whole old file or the whole new one.

Pruning (`--prune-interval`, `--prune-keep`, `--prune-max-age`) carries sealed
lines into the new file without decrypting and re-encrypting them.

### Envelope encryption with KMS

With a KMS provider, prism uses standard envelope encryption:

```
            first run (file absent)                     later runs
 ┌────────┐  GenerateDataKey   ┌─────────┐      ┌────────┐  Decrypt(wrapped)  ┌─────────┐
 │ prism  │ ─────────────────▶ │   KMS   │      │ prism  │ ─────────────────▶ │   KMS   │
 │        │ ◀───────────────── │ (CMK)   │      │        │ ◀───────────────── │ (CMK)   │
 └────────┘ plaintext + wrapped└─────────┘      └────────┘     plaintext      └─────────┘
     │ writes header {kdf, wrapped}                 │ reads header
     │ seals entries with plaintext key (memory)    │ opens entries with plaintext key
     ▼                                              ▼
   state file  (never contains the plaintext key)
```

1. **First run.** `KMSKeySource.Resolve` asks the provider for a new 256-bit
   data key.
   - **AWS:** `TrentService.GenerateDataKey` with `KeySpec: AES_256` returns
     the plaintext and the `CiphertextBlob`.
   - **GCP:** Cloud KMS has no data-key API, so prism generates 32 bytes from
     `crypto/rand` and wraps them with `cryptoKeys.encrypt`.
2. The **wrapped** key goes into the header. The **plaintext** key stays in
   memory only and is used to seal entries.
3. **Later runs.** `KMSKeySource.Restore` reads the header, checks that `kdf`
   matches the configured provider, and calls `Decrypt` (AWS) or
   `cryptoKeys.decrypt` (GCP) to unwrap the key.
4. A key that isn't exactly 32 bytes is rejected.

KMS is called **once per process**, when the file is opened, not on each
checkpoint. Key-service latency and cost therefore don't grow with stream
throughput.

The header is created with `O_EXCL`. If two processes create the same new file
at once, only one writes a header and the other reads and uses it, so you can't
end up with entries sealed under two different keys in one file.

prism calls the providers' REST APIs directly (SigV4 for AWS, OAuth bearer
for GCP) instead of linking the cloud SDKs. Per-request timeout is 30 s.

### Passphrase keys

`PassphraseKeySource` derives the key with **PBKDF2-HMAC-SHA256**, 100,000
iterations (`DefaultPBKDF2Iterations`), over a random 16-byte salt. The salt and
iteration count are stored in the header. Because the header isn't trusted,
a file asking for more than 10,000,000 iterations is refused. That stops an
edited header from making prism spend minutes deriving a key.

---

## Cloud KMS setup

### AWS KMS

#### 1. Create a symmetric key

```sh
aws kms create-key \
  --description "stellar-prism state file encryption" \
  --key-spec SYMMETRIC_DEFAULT --key-usage ENCRYPT_DECRYPT
aws kms create-alias --alias-name alias/prism-state --target-key-id <KeyId>
```

#### 2. Grant the IAM permissions

prism makes exactly two KMS calls. Grant the principal that runs prism (an
IAM user, an instance profile, an IRSA role, or an ECS task role) only these:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PrismStateFileEnvelope",
      "Effect": "Allow",
      "Action": [
        "kms:GenerateDataKey",
        "kms:Decrypt"
      ],
      "Resource": "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
    }
  ]
}
```

| Action | Used when |
|---|---|
| `kms:GenerateDataKey` | the state file doesn't exist yet (first run, or after deletion) |
| `kms:Decrypt` | every run that opens an existing state file |

Notes:

- `Resource` must be the **key ARN**. IAM does not match policy resources on
  alias ARNs. `--state-kms-key-id` itself can be a key ID, key ARN, alias name
  or alias ARN. The KMS API resolves it.
- If the key policy doesn't delegate to IAM (the default key policy does,
  through the account root principal), add the same two actions to the
  **key policy** for the principal too.
- For a read-only replica that only resumes an existing file, `kms:Decrypt`
  alone is enough.
- Lock it down further with conditions such as `aws:SourceVpce` or
  `aws:PrincipalTag`. prism does not send an encryption context.

#### 3. Run prism

```sh
export AWS_ACCESS_KEY_ID=...        # or credentials injected by your platform
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...        # for temporary credentials (STS, IRSA, SSO)

prism events --contract CBGSB... --follow \
  --state-file /var/lib/prism/events.state --resume \
  --state-kms-provider aws \
  --state-kms-key-id arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab \
  --state-kms-region us-east-1
```

Region resolution: `--state-kms-region`, then `$AWS_REGION`, then `us-east-1`.
prism reads static credentials from the three environment variables above.
It does not query the instance metadata service or `~/.aws/credentials`
itself, so on EC2, EKS or ECS export the role's temporary credentials into the
environment (for example with `aws configure export-credentials --format env`)
before starting prism.

### Google Cloud KMS

#### 1. Create a key ring and key

```sh
gcloud kms keyrings create prism --location global
gcloud kms keys create state-file \
  --keyring prism --location global \
  --purpose encryption
```

The resource name you pass to prism is:

```
projects/PROJECT_ID/locations/global/keyRings/prism/cryptoKeys/state-file
```

#### 2. Grant the IAM role

prism calls `cryptoKeys.encrypt` (first run) and `cryptoKeys.decrypt` (every
later run). The predefined role **Cloud KMS CryptoKey Encrypter/Decrypter**
(`roles/cloudkms.cryptoKeyEncrypterDecrypter`) covers both. Grant it on the
**key**, not the project:

```sh
gcloud kms keys add-iam-policy-binding state-file \
  --keyring prism --location global \
  --member serviceAccount:prism@PROJECT_ID.iam.gserviceaccount.com \
  --role roles/cloudkms.cryptoKeyEncrypterDecrypter
```

| Permission | Used when |
|---|---|
| `cloudkms.cryptoKeyVersions.useToEncrypt` | the state file doesn't exist yet |
| `cloudkms.cryptoKeyVersions.useToDecrypt` | every run that opens an existing state file |

For least privilege, make a custom role with just those two permissions, or
give a resume-only replica `roles/cloudkms.cryptoKeyDecrypter`.

#### 3. Run prism

prism needs an OAuth 2.0 access token for the service account. It reads the
same variable the GCS sink uses, so one credential covers both:

```sh
export GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token \
  --impersonate-service-account prism@PROJECT_ID.iam.gserviceaccount.com)"

prism storage --contract CBGSB... --follow \
  --state-file /var/lib/prism/storage.state --resume \
  --state-kms-provider gcp \
  --state-kms-key-id projects/PROJECT_ID/locations/global/keyRings/prism/cryptoKeys/state-file
```

Use `--state-kms-token-env NAME` to read the token from another variable.
Access tokens expire (typically after one hour), but prism only uses the token
when it opens the file at startup. A long-running stream doesn't need a
refresh. A restart does need a fresh token.

### Key rotation

- **AWS:** enable automatic rotation (`aws kms enable-key-rotation`). KMS keeps
  older key material, so wrapped data keys in existing headers still decrypt.
- **GCP:** add a new primary version or set a rotation schedule. Cloud KMS
  decrypts with whichever version wrapped the key, as long as that version
  is still enabled. **Don't disable or destroy a version** that existing state
  files depend on.
- The **data key** inside a file doesn't change when the CMK rotates. To
  re-key a file, stop prism, note the last ledger (`prism state inspect`
  if available, or your own logs), delete the file, and restart with
  `--from <ledger>`.

---

## Tamper detection

Every checkpoint is a GCM-sealed envelope, and GCM includes a 128-bit
authentication tag computed over the ciphertext and the additional data. When
prism reads the journal, each line is opened with `gcm.Open`. If **any** byte
of the nonce, ciphertext or tag has changed, or the line was sealed under a
different key, the open fails and prism stops with
`stream.ErrStateAuthentication`:

```
state file: authentication failed: /var/lib/prism/events.state was tampered with,
truncated, or written with a different key: cipher: message authentication failed
```

This is what stops someone from hand-editing a cursor to make a stream skip
ledgers (hiding activity) or replay them (duplicating records downstream).
In a plaintext journal that takes a text editor. In an encrypted one it
can't be done without the key.

What the design does and doesn't protect against:

| Attack | Result |
|---|---|
| Edit a cursor, ledger or key inside an entry | **Detected.** The tag fails and the run stops |
| Flip bits in a nonce or ciphertext | **Detected** |
| Copy an entry from a file sealed with another key | **Detected.** The tag fails under this key |
| Edit the header (`kdf`, `salt`, `iterations`, `wrapped`) | **Detected** on the first entry, because a different key is derived. Iteration counts above 10M are refused before any work |
| Copy an entry from another prism state file sealed with the **same** key | Not detected. Every file uses the same AAD label, so use a separate key or passphrase per stream or tenant if this matters |
| Delete or reorder whole lines | Not detected per line. prism takes the last entry for each stream key, so deleting the newest lines **rolls the cursor back** (records re-delivered, never skipped). Downstream deduplication (`--dedup-window`, idempotent sinks) absorbs this |
| Delete the entire file | prism starts a fresh stream from `--from` or the chain head. Monitor for unexpected file recreation |
| Read the file without the key | Only the header is visible: algorithm, KDF name, salt or wrapped key. No cursors or filters |

Recovery is deliberate: prism never falls back to reading a file that failed
authentication, and never overwrites it. Investigate, then either restore the
file from a backup or delete it and restart with an explicit `--from`.

---

## Operational checklist

- [ ] Use a separate KMS key, or at least a separate state file and passphrase,
      for each tenant or environment.
- [ ] Grant only `GenerateDataKey` + `Decrypt` (AWS) or `useToEncrypt` +
      `useToDecrypt` (GCP), scoped to the single key.
- [ ] Keep the state directory on a volume readable only by the prism user. The
      file is created `0600`.
- [ ] Back up the state file *and* make sure the key stays available.
      Losing the KMS key or passphrase makes the cursor unrecoverable (you can
      still restart with `--from`).
- [ ] Alert on `state file: authentication failed` in prism's stderr.
- [ ] Enable CloudTrail / Cloud Audit Logs for the key to see every unwrap.
