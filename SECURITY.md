# Security Policy

## Supported versions

This project is pre-1.0. Security fixes are applied to the latest release and
to `main`; older tags are not patched.

| Version | Supported |
| --- | --- |
| 0.1.x | ✅ |
| < 0.1 | ❌ |

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report it through
[GitHub's private vulnerability reporting](https://github.com/stellar-optics/stellar-prism/security/advisories/new),
which opens a private channel with the maintainers.

Please include:

- what the issue is and why it matters;
- how to reproduce it — for a streaming issue, the sequence of RPC responses
  that triggers it, ideally expressed as an `internal/fakerpc` scenario;
- the version or commit you tested;
- any suggested fix, if you have one.

### What to expect

- **Acknowledgement** within 5 working days.
- **An assessment** within 10 working days.
- **A fix and advisory** for confirmed issues, coordinated with you on timing.

You will be credited unless you prefer otherwise.

## Threat model

prism is a **network client that parses untrusted responses**. It:

- talks to a Soroban RPC endpoint the user chooses, over HTTPS;
- never handles secret keys, and has no signing or submission code;
- never executes anything it decodes;
- writes only to stdout and stderr, and reads no files except those the user
  names.

The realistic attack surface is therefore a **hostile or compromised RPC
endpoint**, and malformed XDR arriving in its responses.

### In scope

- A crash, panic or unrecovered runtime error triggered by a malformed RPC
  response or malformed XDR within it.
- Unbounded memory or CPU consumption caused by a hostile response — an
  enormous page, a deeply nested payload, or a response that induces an
  infinite loop in the streaming state machine.
- **A silent gap or a silently dropped record.** prism's entire value is the
  claim that it does not lose data without telling you. A path that skips
  records without reporting a `GapError` is a security issue on a tool people
  use to audit contract activity, not merely a bug.
- **Output that misrepresents on-chain data** — a decoded value that does not
  match the XDR, or a failed transaction reported as successful.
- Any file read or written that the user did not ask for.
- Any credential or endpoint leaked into stdout, where it could end up in a
  log pipeline. Note that `--rpc-url` may contain an API key, so it is never
  echoed.

### Out of scope

- Vulnerabilities in `github.com/stellar/go-stellar-sdk` or in
  `stellar-xdr-lens`. Report those to their own projects; we will pick up the
  fixed version.
- A malicious RPC endpoint returning *plausible but false* data. prism cannot
  verify history against consensus; it reports what the endpoint says. Point
  it at an endpoint you trust.
- Denial of service that requires an input larger than the memory you chose to
  give the process.
- Anything requiring an attacker to already control the machine running prism.

## For users

- **`--rpc-url` may contain a secret.** Many hosted RPC providers embed an API
  key in the URL. Prefer `SOROBAN_RPC_URL` in the environment over a shell
  command that lands in your history, and be careful pasting a command line
  into an issue.
- **prism only reads.** It never submits transactions and never touches keys.
- **Exit code 2 means the stream gapped.** If you script around prism, treat
  it as distinct from a generic failure: records were missed, and the range is
  printed on stderr.
