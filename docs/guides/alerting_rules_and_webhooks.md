# Alerting Rules, Telegram, and PagerDuty Integrations

`stellar-prism` provides real-time alerting on Stellar and Soroban contract events using rule-based expression evaluation and direct dispatcher integrations with **Telegram** and **PagerDuty**.

---

## 1. Overview

While generic webhooks are suitable for ingestion pipelines, operational monitoring and security alerting require direct notifications to on-call incident response systems and team chat channels.

The rule engine inspects each contract event or ledger close record against configured predicates. When an alert condition is met, events are immediately queued for asynchronous delivery with exponential backoff retries.

---

## 2. Telegram Bot Integration

### Setup
1. Create a bot via [@BotFather](https://t.me/BotFather) on Telegram and copy the API token (e.g., `123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ`).
2. Obtain your target Telegram chat or channel ID (e.g., `-1001234567890`).

### Configuration

#### CLI Flags
```bash
prism events \
  --contract CBGSB... \
  --alert-telegram-token "123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ" \
  --alert-telegram-chat-id "-1001234567890" \
  --follow
```

#### Configuration File (`.prism.yaml`)
```yaml
alert-telegram-token: "123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ"
alert-telegram-chat-id: "-1001234567890"
```

### Alert Notification Format
Alerts delivered to Telegram are formatted in Telegram Markdown:
```markdown
*🚨 [CRITICAL] LargeTransferDetected*
Transfer exceeded threshold of 100,000 tokens

• *Ledger:* `123456`
• *Tx Hash:* `3389e9f0...`
• *Contract:* `CBGSB...`
• *Time:* `2026-11-24T12:00:00Z`
```

---

## 3. PagerDuty Events API v2 Integration

### Setup
1. In PagerDuty, navigate to **Services** -> **Service Directory** -> select or create a Service.
2. Under **Integrations**, add a new integration using **Events API v2**.
3. Copy the 32-character **Integration Key / Routing Key**.

### Configuration

#### CLI Flags
```bash
prism events \
  --contract CBGSB... \
  --alert-pagerduty-routing-key "0123456789abcdef0123456789abcdef" \
  --follow
```

#### Configuration File (`.prism.yaml`)
```yaml
alert-pagerduty-routing-key: "0123456789abcdef0123456789abcdef"
```

### Severity Mapping
Alert rule severities map directly to PagerDuty Events API v2 severity levels:

| Rule Severity | PagerDuty Severity |
| :--- | :--- |
| `critical`, `crit`, `fatal` | `critical` |
| `error`, `err` | `error` |
| `warn`, `warning` | `warning` |
| `info`, other | `info` |

### Payload & De-duplication
Incidents are dispatched to `https://events.pagerduty.com/v2/enqueue` with:
- **Dedup Key**: Formatted as `<RuleName>-<TxHash>-<EventIndex>` to prevent duplicate alerts for the same transaction event.
- **Custom Details**: Contains ledger sequence, transaction hash, contract ID, event index, and triggered rule details.

---

## 4. Reliability & Delivery Guarantees

- **Non-blocking Asynchronous Dispatch**: Alerts are pushed to an internal buffered worker queue so high-throughput ledger tailing is never throttled by third-party API latency.
- **Automatic Backoff & Retries**: Failed deliveries due to HTTP 429 (rate limits) or 5xx server errors retry with exponential backoff (100ms up to 2s with jitter).
- **Graceful Termination**: When `prism` shuts down, in-flight alert notifications are drained and workers joined cleanly.
