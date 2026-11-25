# Rotating File Sink, Syslog, and Log Management

Long-running `stellar-prism` streams produce continuous NDJSON, CSV/TSV, or human-readable output. Without active rotation or forwarding to a system log daemon, flat output files eventually exhaust disk space.

This guide covers:

- [Internal File Rotation (`--output-file` + `--rotate-*`)](#internal-file-rotation)
- [Linux Syslog and `journald` Integration](#linux-syslog-and-journald-integration)
- [`systemd` Service Supervision](#systemd-service-supervision)
- [External `logrotate` Configuration](#external-logrotate-configuration)

---

## Internal File Rotation

Every streaming command (`events`, `transactions`, `operations`, `ledgers`, `contracts`, `trades`, `fee-stats`, `storage`, `wasm`) supports `--output-file` with built-in rotation backed by [`pkg/sink/file_rotator.go`](../../pkg/sink/file_rotator.go).

### CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--output-file <path>` | *(stdout)* | Append records to `<path>` instead of stdout (required for `--rotate-*`) |
| `--rotate-size <bytes>` | `0` (off) | Rotate before a write would exceed this size in **bytes** (e.g. `104857600` for 100 MiB) |
| `--rotate-interval <duration>` | `0` (off) | Rotate once the active file has been open for this duration (e.g. `1h`, `24h`) |
| `--rotate-keep <count>` | `0` (keep all) | Keep at most this many rotated backup files, deleting the oldest |
| `--rotate-compress` | `false` | Gzip each rotated segment (`<path>.<UTC-timestamp>.gz`) |

> [!NOTE]
> `--rotate-size` expects an integer byte count (`int64`), not a unit suffix such as `100MB`. Passing any `--rotate-*` flag without `--output-file` is rejected at startup.

### Rotation Semantics and Crash Safety

- **Format-agnostic:** `NewFileRotator` wraps any output sink (`--json`, `--format csv`, `--format tsv`, or human-readable output) on top of `RotatingWriter`.
- **Single-record integrity on size limit:** Rotation is checked before each write; a single write larger than `--rotate-size` is written whole rather than split.
- **Restart continuity:** On startup, `NewRotatingWriter` reads the existing file's size and `ModTime()`. If an overdue interval or size threshold is already met, rotation happens on the first write.
- **Synchronous compression:** When `--rotate-compress` is enabled, `RotatingWriter` syncs and closes the old file, renames it to `<path>.<YYYYMMDD-HHMMSS.nnnnnnnnn>`, immediately reopens `<path>` so streaming can continue, and then **synchronously** gzips the rotated backup before removing the uncompressed backup. This guarantees a crash cannot leave a half-written `.gz` archive next to an unrotated segment.
- **Safe pruning:** Old backups beyond `--rotate-keep` are pruned after reopening the active file so a pruning failure never interrupts active writes.

### Examples

Rotate NDJSON output at 100 MiB (`104857600` bytes) or every 1 hour, compress rotated segments, and retain the last 10 archives:

```sh
prism events \
  --network mainnet \
  --contract CDLZF...CYSC \
  --follow \
  --json \
  --output-file /var/log/prism/events.ndjson \
  --rotate-size 104857600 \
  --rotate-interval 1h \
  --rotate-keep 10 \
  --rotate-compress \
  --state-file /var/lib/prism/events.state \
  --resume
```

Combine with `--batch` when writing high-volume streams to reduce per-record `Flush`/`Sync` overhead:

```sh
prism transactions \
  --follow \
  --json \
  --batch --batch-size 500 --batch-timeout 250ms \
  --output-file /var/log/prism/transactions.ndjson \
  --rotate-size 104857600 \
  --rotate-keep 24 \
  --rotate-compress
```

### Library Usage

In Go callers, wrap any `io.Writer`-backed sink with `sink.NewFileRotator`:

```go
rotator, err := sink.NewFileRotator("/var/log/prism/events.ndjson", func(w io.Writer) stream.Sink {
	return sink.NewNDJSON(w, true)
}, sink.RotatorOptions{
	MaxSize:    100 << 20, // 100 MiB
	MaxAge:     time.Hour,
	MaxBackups: 10,
	Compress:   true,
})
```

---

## Linux Syslog and `journald` Integration

To forward records directly to a local or remote syslog collector—or directly into `systemd-journald`—use the syslog sink ([`pkg/sink/syslog.go`](../../pkg/sink/syslog.go)).

### Syslog Flags

| Flag | Default | Description |
|---|---|---|
| `--syslog-address <addr>` | *(disabled)* | Socket or network endpoint (`unixgram:///dev/log`, `unix:///dev/log`, `udp://host:514`, `tcp://host:601`, or `/path/to/socket`) |
| `--syslog-format <fmt>` | `rfc5424` | Framing format: `rfc5424` or `journald` |
| `--syslog-facility <fac>` | `local0` | POSIX facility name (`local0`..`local7`, `user`, `daemon`, `auth`, `authpriv`, `cron`, etc.) or numeric `0`..`23` |
| `--syslog-app <name>` | `prism` | RFC 5424 `APP-NAME` / `SYSLOG_IDENTIFIER` (printable ASCII, no spaces, max 48 chars) |

Records are encoded with `sink.MarshalRecord` (the same JSON object as `--json`, or raw base64 XDR when `--raw` is passed). Severities are assigned automatically:

- **Warning (`4`)**: Failed transactions and failed WASM deployments
- **Debug (`7`)**: Diagnostic contract events (`type == "diagnostic"`)
- **Info (`6`)**: All other stream records

### Forwarding to `rsyslog` / POSIX Syslog (`rfc5424`)

```sh
prism events \
  --contract CDLZF...CYSC \
  --follow \
  --syslog-address unixgram:///dev/log \
  --syslog-format rfc5424 \
  --syslog-facility local0 \
  --syslog-app prism-events
```

Each message includes structured data under enterprise ID `prism@32473` (`kind`, `ledger`, `id`) followed by the single-line JSON payload:

```text
<134>1 2026-11-24T12:00:00.000Z host prism-events - event [prism@32473 kind="event" ledger="3966820" id="0017037362169118720-0000000000"] {"kind":"event",...}
```

Example `/etc/rsyslog.d/30-prism.conf` rule to route `local0` messages from `prism-events` into a dedicated log file:

```conf
if ($programname == 'prism-events') then {
    action(type="omfile" file="/var/log/prism/events-syslog.log")
    stop
}
```

For remote log collectors, use `tcp://collector.internal:601` (newline-framed) or `udp://collector.internal:514`.

### Streaming Directly to `systemd-journald` (`journald`)

When running under `systemd`, `journald` native protocol attaches structured fields (`PRIORITY`, `SYSLOG_IDENTIFIER`, `SYSLOG_FACILITY`, `PRISM_KIND`, `PRISM_LEDGER`, `PRISM_RECORD_ID`, `PRISM_LEDGER_TIME`, and `MESSAGE`) without extra parsing overhead:

```sh
prism events \
  --contract CDLZF...CYSC \
  --follow \
  --syslog-address unixgram:///run/systemd/journal/socket \
  --syslog-format journald \
  --syslog-facility local0 \
  --syslog-app prism-events
```

Query ingested records with `journalctl`:

```sh
# Follow structured logs for the prism-events identifier
journalctl -t prism-events -f -o json-pretty

# Filter by custom journald field attached by prism
journalctl SYSLOG_IDENTIFIER=prism-events PRISM_KIND=event -n 50
```

---

## `systemd` Service Supervision

When running `prism` as a daemon, pair `--state-file` + `--resume` with `--drain-timeout` so `systemd` restarts preserve cursor continuity without duplicating or truncating writes. Ensure `TimeoutStopSec` exceeds `--drain-timeout`:

```ini
# /etc/systemd/system/prism.service
[Unit]
Description=stellar-prism Soroban event streamer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=prism
Group=prism
StateDirectory=prism
LogsDirectory=prism
Environment="SOROBAN_RPC_URL=https://soroban-testnet.stellar.org"
ExecStart=/usr/local/bin/prism events \
  --network testnet \
  --contract CDLZF...CYSC \
  --follow \
  --json \
  --state-file /var/lib/prism/events.state \
  --resume \
  --drain-timeout 15s \
  --output-file /var/log/prism/events.ndjson \
  --rotate-size 104857600 \
  --rotate-interval 1h \
  --rotate-keep 10 \
  --rotate-compress
Restart=on-failure
RestartSec=5s
KillSignal=SIGTERM
TimeoutStopSec=25s

[Install]
WantedBy=multi-user.target
```

---

## External `logrotate` Configuration

If your infrastructure standardizes on `/etc/logrotate.d/` instead of `prism`'s internal `--rotate-*` flags, write to `--output-file` **without** any `--rotate-*` flags.

> [!WARNING]
> Never run external `logrotate` against a file managed by `--rotate-*`. Both tools will rename and prune files independently.

Because `prism` keeps `--output-file` open for appending while streaming, external `logrotate` must either use `copytruncate` or restart the service cleanly in `postrotate`:

### Option A: Clean `systemd` Restart (Recommended for External `logrotate`)

Combined with `--state-file` and `--resume`, restarting on rotation guarantees zero truncated lines and clean gzip archives:

```conf
# /etc/logrotate.d/prism
/var/log/prism/*.ndjson {
    hourly
    rotate 24
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    dateext
    dateformat -%Y%m%d-%H%M%S
    create 0640 prism prism
    sharedscripts
    postrotate
        systemctl restart prism.service >/dev/null 2>&1 || true
    endscript
}
```

### Option B: `copytruncate` (No Process Restart)

If restarting the process is undesirable and you cannot use `--rotate-*`, `copytruncate` copies the active file and truncates it in place:

```conf
# /etc/logrotate.d/prism
/var/log/prism/*.ndjson {
    daily
    rotate 14
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
}
```

> [!CAUTION]
> `copytruncate` carries a narrow race window between copying the file and truncating it where newly flushed records can be lost. Prefer `prism`'s built-in `--rotate-*` flags (or Option A with `--state-file` + `--resume`) for lossless archival.
