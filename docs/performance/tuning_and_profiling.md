# Performance Tuning and Profiling for High-Throughput Nodes

Tailing Stellar mainnet with a broad filter means thousands of contract events,
transactions and storage changes per ledger, every ~5 seconds, for weeks at a
time. At that rate the things that hurt are allocation rate (GC CPU and pause
time), memory growth in long-lived buffers, and RPC round-trips. This guide
covers how prism keeps allocation low, how to profile a live node, and how to
size and tune the Go runtime in containers.

Related:

- [docs/benchmarks.md](../benchmarks.md): baseline throughput and allocation
  numbers, and how to reproduce them
- [`cmd/prism/pprof.go`](../../cmd/prism/pprof.go): the profiling server
- [`pkg/stream/record.go`](../../pkg/stream/record.go): the `sync.Pool`s
- [docs/architecture.md](../architecture.md#runtime-profiling---pprof-port)

---

## 1. Memory allocations and `sync.Pool`

### Where the pools are

The steady-state hot path is `EventSource.Fetch` → `Tailer` → sink. Two
per-page structures would otherwise be allocated on every poll and dropped
right after. `pkg/stream/record.go` recycles both:

| Pool | Holds | Sized for | Used by |
|---|---|---|---|
| `recordBufferPool` | `*[]Record` slice backing arrays | `DefaultPageLimit` (200) records | `EventSource.Fetch` builds each merged page in a pooled slice |
| `eventIDSetPool` | `map[string]struct{}` | 200 entries | `EventSource.Fetch` de-duplicates event IDs across contract-ID groups (the RPC accepts ≤ 5 contracts per call, so larger watchlists are split and merged) |

The lifecycle of one page:

```
EventSource.Fetch
  ├─ getRecordBuffer(pageLimit)    ← reuse a []Record (cap 200) or make one
  ├─ getEventIDSet()               ← reuse a cleared map
  ├─ fetchGroup(...) × N groups    → append unique records
  ├─ putEventIDSet(seen)           → clear(map); Put   (deferred)
  └─ return Batch{Records: pooled}
Tailer.Run
  ├─ reorder / dedup / filter / sink.Emit(record) …
  └─ releaseBatchRecords(batch)    → putRecordBuffer: clear(slice[:cap]); Put
```

Rules that keep this safe:

- **Only the slice storage is recycled, not the payloads.** `putRecordBuffer`
  zeroes every slot (`clear(records[:cap(records)])`) before returning the
  slice, so the pool never keeps `*Event` pointers or strings alive. Sinks
  that hold on to a `Record` (batchers, the dedup ring, broadcast queues) keep
  the payload they were handed. Only the slice header storage gets reused.
- **Oversized buffers are not pooled.** A page larger than `DefaultPageLimit`
  (from a large `--page-size`) gets a fresh slice that is never put back, so
  one burst can't leave the pool holding very large arrays.
- **A custom `Process` hook turns recycling off.** If an embedding sets
  `stream.Options.Process`, it gets the backing slice and may retain it, so
  `releaseBatchRecords` skips the `Put`.
- **Maps are cleared, not reallocated.** `clear(map)` keeps the buckets, so a
  reused ID set doesn't grow again on every poll.

### What it buys

On an Apple M4 (Go 1.26), `BenchmarkEventPipelineAllocations` maps and
de-duplicates one 200-event RPC page:

| Variant | ns/page | B/page | allocs/page |
|---|---:|---:|---:|
| unpooled | 25,500 | 72,776 | 214 |
| pooled | 20,300 | 32,056 | 201 |

Pooling removes **56% of the bytes** allocated per page and makes it about 20%
faster. The remaining ~1 allocation per event is the `*Event` payload and its
strings, which the record's consumer owns and can't be pooled without changing
sink ownership rules.

Past the source, the tailer core (reorder buffer, cursor bookkeeping, sink
seam) is close to allocation-free: **0.077 allocations and ~370 bytes per
event** end to end (`BenchmarkTailerFiftyThousandEvents/raw`). At a sustained
5,000 events/s that's about 1.8 MB/s of garbage from the core. That's
small next to decoding and output encoding, which usually dominate a
real node's allocation profile.

### Features that allocate per record

Turn these on knowingly on a high-volume stream:

| Feature | Cost | Why |
|---|---|---|
| `--dedup-window N` | +1 alloc, ~54 B/event; ring of N fingerprints resident | fingerprint string per record. Size N to a page or two (4096 is plenty for 200-record pages) |
| default NDJSON decoding (no `--raw`) | several allocs per XDR field | lens decodes every topic/value into a tree before formatting |
| `--filter-expr`, `--topic-regex` | decode + evaluate per event | decoded values are needed to evaluate the expression |
| `--serve-grpc` / `--sse` / `--ws` | 1 JSON encode **per subscriber** per record | each broadcast endpoint encodes on its own |
| `--fanout-dir` | one open file per contract | file handles and buffers per watched contract |

For a firehose feeding another system, `--json --raw` is the cheapest output.
Decode further downstream, where it can be scaled out.

---

## 2. Live profiling with pprof

### Enabling the profiler

`--pprof-port` is a global flag, so it works on every command. It starts the
standard `net/http/pprof` handlers for the life of the process:

```sh
prism serve --pprof-port 6060 --sse 127.0.0.1:8080 --contract CBGSB...
# prism: serving pprof on http://127.0.0.1:6060/debug/pprof/

prism events --follow --json --raw --pprof-port 6060 > events.ndjson
```

- The server binds **`127.0.0.1` only**. Profiles expose memory contents,
  command lines and stack traces. The flag takes a port, not an address, so it
  can't be exposed by accident.
- `0` (the default) disables it and opens no listener.
- The server shuts down with the command (`http.Server.Shutdown`, then
  waiting for the serve goroutine), including on Ctrl-C and validation errors.

To profile a node in a container or on a remote host, tunnel to it instead of
rebinding:

```sh
kubectl port-forward pod/prism-0 6060:6060      # Kubernetes
ssh -N -L 6060:127.0.0.1:6060 indexer-host      # VM
docker exec -it prism wget -qO- 127.0.0.1:6060/debug/pprof/heap > heap.pprof
```

### Endpoints

| Path | Profile | Use it for |
|---|---|---|
| `/debug/pprof/profile?seconds=30` | CPU | where time goes: decoding, JSON, TLS, GC |
| `/debug/pprof/heap` | live heap (in-use) | what is resident: buffers, dedup rings, queues |
| `/debug/pprof/allocs` | allocations since start | what creates garbage, i.e. GC pressure |
| `/debug/pprof/goroutine` | goroutine stacks | leaks, stuck sinks, blocked subscribers |
| `/debug/pprof/trace?seconds=5` | execution trace | GC pauses, scheduler latency, poll timing |
| `/debug/pprof/block`, `/debug/pprof/mutex` | contention | **empty by default.** prism doesn't set `runtime.SetBlockProfileRate` / `SetMutexProfileFraction` |
| `/debug/pprof/threadcreate`, `cmdline`, `symbol` | misc | rarely needed |

### Capturing and reading profiles

**CPU** (30 s sample, then an interactive web UI):

```sh
go tool pprof -http=:8081 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```

**Heap**, what's resident right now:

```sh
curl -s -o heap.pprof http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -top -sample_index=inuse_space heap.pprof
```

**Allocation rate**, what creates garbage:

```sh
go tool pprof -top -sample_index=alloc_space http://127.0.0.1:6060/debug/pprof/allocs
```

**Leaks over time.** Take two heap snapshots a few minutes apart and diff
them:

```sh
curl -s -o heap1.pprof http://127.0.0.1:6060/debug/pprof/heap
sleep 300
curl -s -o heap2.pprof http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -top -diff_base=heap1.pprof heap2.pprof
```

Steady growth in `inuse_space` between snapshots suggests a leak. Stable
in-use with high `alloc_space` just means GC pressure.

**Goroutines**, for example a stuck sink or slow subscriber:

```sh
curl -s 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=2' | less
```

**GC pauses** via an execution trace:

```sh
curl -s -o trace.out 'http://127.0.0.1:6060/debug/pprof/trace?seconds=10'
go tool trace trace.out      # "GC" and "Minimum mutator utilization" views
```

Pass the binary for symbols if your build is stripped:
`go tool pprof ./prism heap.pprof`.

### What to look for

| Symptom in the profile | Likely cause | Fix |
|---|---|---|
| `lens.Decode` / `encoding/json` dominate CPU | decoding every payload | `--raw`, or a narrower `--contract`/`--topic` filter |
| `runtime.mallocgc` / `gcBgMarkWorker` > 15% CPU | high allocation rate | see section 3; drop per-record features you don't need |
| `crypto/tls` high | many short RPC connections | keep one long-lived process; use `--rpc-url` failover rather than restarts |
| heap grows with `DedupSink` frames | `--dedup-window` too large | size to 1–2 pages |
| goroutine count rising | client connections not closing | check `--sse`/`--ws` clients; `--health-port` `/metrics` |
| CPU idle, stream lagging | RPC latency-bound, not CPU-bound | `--page-size` up to the RPC limit; `--backfill-workers` for catch-up; `--adaptive` polling |

### Profiling the benchmarks offline

The same tools work on the benchmark suite, with no node needed:

```sh
go test -run '^$' -bench 'BenchmarkTailerFiftyThousandEvents/raw' \
  -cpuprofile cpu.out -memprofile mem.out -benchmem ./pkg/stream
go tool pprof -top cpu.out
go tool pprof -top -sample_index=alloc_space mem.out
```

---

## 3. GC tuning: `GOGC` and `GOMEMLIMIT`

prism doesn't set any runtime knobs itself. It uses whatever the environment
provides, so tuning is purely deployment configuration.

### How the two knobs interact

- **`GOGC`** (default `100`): start the next GC when the heap has grown by
  this percentage over the live heap after the previous GC. Higher means fewer
  GCs, more memory. `off` turns proportional GC off entirely.
- **`GOMEMLIMIT`**: a soft cap on total Go-managed memory. As the process nears
  it, the GC runs more often to stay under it, whatever `GOGC` says.
  Accepts `B`, `KiB`, `MiB`, `GiB` suffixes.

prism's live heap is small and stable (pooled page buffers, the reorder
buffer for the current ledger, an optional dedup ring, and sink buffers),
typically **tens of MB**. Most of what it allocates is short-lived garbage from
decoding. That profile suits a **memory-limit-driven GC**: let the heap grow
toward a known ceiling and collect only when needed.

### Recommendations

| Deployment | `GOMEMLIMIT` | `GOGC` | Notes |
|---|---|---|---|
| **Container with a memory limit (recommended)** | ~80–90% of the container limit | `100` (default) | Headroom covers non-heap memory (goroutine stacks, TLS buffers, cgo in DuckDB/Parquet sinks) |
| Container, maximise throughput | ~85% of limit | `off` | GC runs only near the limit. Lowest GC CPU, but the heap always sits near the cap. Use only with a hard container limit and monitoring |
| Bare VM / shared host | unset or a generous cap | `100`–`200` | Raising `GOGC` trades RAM for less GC CPU |
| Tiny sidecar (≤128 MiB) | ~100 MiB | `50`–`100` | Keep pressure on. Prefer `--raw` and no dedup |

Example for a 512 MiB container:

```yaml
# Kubernetes
resources:
  requests: { cpu: "500m", memory: "512Mi" }
  limits:   { memory: "512Mi" }
env:
  - name: GOMEMLIMIT
    value: "450MiB"
  - name: GOGC
    value: "100"
```

```sh
# Docker
docker run --memory=512m -e GOMEMLIMIT=450MiB -e GOGC=100 \
  your-registry/prism:tag events --follow --json --raw
```

To set the limit from the cgroup automatically:

```sh
# cgroup v2
export GOMEMLIMIT=$(( $(cat /sys/fs/cgroup/memory.max) * 85 / 100 ))B
exec prism "$@"
```

### CPU: `GOMAXPROCS`

prism is built with Go 1.26, which honours container CPU limits (cgroup quota)
when it sets `GOMAXPROCS` (Go 1.25+), so a pod limited to 2 CPUs doesn't
schedule across all host cores. Set `GOMAXPROCS` explicitly only to override
that. The streaming core is mostly sequential (one tailer per stream), so
more than 2–4 procs mainly helps with decoding, broadcast fan-out and
`--backfill-workers`.

### Verifying the effect

Turn on the runtime's GC trace for a short while:

```sh
GODEBUG=gctrace=1 prism events --follow --json --raw > /dev/null
# gc 42 @120.5s 0%: 0.02+1.1+0.03 ms clock, ... 18->19->9 MB, 19 MB goal, ...
```

The `0%` is cumulative GC CPU, `18->19->9 MB` is heap before → after mark →
live, and the `goal` follows `GOGC` / `GOMEMLIMIT`. Aim for low
single-digit GC CPU and sub-millisecond stop-the-world phases. Compare
`go tool trace` captures from before and after a change.

Watch the `/metrics` endpoint (`--health-port`) together with your platform's
container memory metrics. If RSS sits well below the limit and GC CPU is
high, raise `GOMEMLIMIT` or `GOGC`. If the container gets OOM-killed, lower
`GOMEMLIMIT` to leave more headroom for non-heap memory.

---

## 4. Throughput tuning checklist

- [ ] **Filter at the RPC.** `--contract`, `--topic`, `--type` cut volume
      before any bytes reach prism. Client-side filters (`--filter-expr`,
      `--topic-regex`, `--exclude-*`) still pay for fetch and decode.
- [ ] **`--json --raw`** for machine consumers. Decode downstream.
- [ ] **`--page-size`**: larger pages mean fewer round-trips. Pages above 200
      bypass the record-buffer pool, so measure before going much higher.
- [ ] **`--backfill-workers N`** with `--from` for historical catch-up.
- [ ] **`--adaptive`** with `--follow` so the poll interval follows the ledger
      close rate.
- [ ] **`--batch --batch-size --batch-timeout`** for sinks that benefit from
      fewer, larger writes.
- [ ] **`--dedup-window`** only if your RPC re-serves events across polls.
      Size it to 1–2 pages.
- [ ] **Multiple `--rpc-url`s** for failover, so a slow endpoint doesn't stall
      the stream.
- [ ] **`GOMEMLIMIT` ≈ 85% of the container limit**. Leave `GOGC` at default
      unless profiles say otherwise.
- [ ] **Profile before and after** each change with `--pprof-port`, and compare
      against [docs/benchmarks.md](../benchmarks.md).
