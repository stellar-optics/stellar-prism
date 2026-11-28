# Benchmarks

prism's streaming core has an offline benchmark suite sized around the
operational target of **50,000 events per second**. This page records baseline
throughput and memory numbers and explains how to reproduce and compare them.
For how to act on the numbers (pooling, pprof, `GOGC`/`GOMEMLIMIT`), see
[docs/performance/tuning_and_profiling.md](performance/tuning_and_profiling.md).

Sources: [`pkg/stream/bench_test.go`](../pkg/stream/bench_test.go),
[`pkg/stream/record_bench_test.go`](../pkg/stream/record_bench_test.go).

---

## Running

```sh
make bench
# equivalent to:
go test -run '^$' -bench 'FiftyThousandEvents' -benchmem ./pkg/stream

# include the pooled page-buffer comparison and repeat for stable numbers
go test -run '^$' -bench 'FiftyThousandEvents|EventPipelineAllocations' \
  -benchmem -count=6 ./pkg/stream | tee new.txt
```

The suite uses a fixed in-memory source and no wall-clock waits: no network,
no RPC, and the same record IDs every run. Each `FiftyThousandEvents`
iteration pushes a 50,000-event corpus (5,000 ledgers × 10 events, served in
500-record pages) through real code paths. Custom metrics save you doing the
division by hand:

| Metric | Meaning |
|---|---|
| `events/op` | events per iteration (always 50,000) |
| `events/s` | throughput |
| `allocs/event` | heap allocations per event |
| `bytes/event` | heap bytes allocated per event |

---

## Baseline results

**Environment:** Apple M4 (10 cores), macOS, `go1.26.0 darwin/arm64`,
commit `77a9287`, `-count=3`, median shown. Normal build (no `-race`).

### Throughput and allocation per event

| Benchmark | What it measures | events/s | ns/op (50k events) | allocs/event | bytes/event |
|---|---|---:|---:|---:|---:|
| `BenchmarkTailerFiftyThousandEvents/raw` | tailer, reorder buffer and sink seam end to end | **5.59 M** | 8.95 ms | 0.077 | 370 |
| `BenchmarkTailerFiftyThousandEvents/dedup` | same path with the `--dedup-window` sink | **3.92 M** | 12.77 ms | 1.08 | 423 |
| `BenchmarkDedupSinkFiftyThousandEvents` | fingerprinting plus the bounded de-dup ring | 12.6 M | 3.97 ms | 1.00 | 54 |
| `BenchmarkReorderBufferFiftyThousandEvents` | per-ledger sorting and release | 15.0 M | 3.33 ms | 0.0086 | 152 |
| `BenchmarkEventKeyFiftyThousandEvents` | the de-duplication key alone | 49.5 M | 1.01 ms | 1.00 | 48 |

Per iteration (50,000 events):

| Benchmark | B/op | allocs/op |
|---|---:|---:|
| `Tailer…/raw` | 18.5 MB | 3,840 |
| `Tailer…/dedup` | 21.2 MB | 53,861 |
| `DedupSink…` | 2.7 MB | 50,021 |
| `ReorderBuffer…` | 7.6 MB | 431 |
| `EventKey…` | 2.4 MB | 50,000 |

### Pooled vs unpooled page buffers

`BenchmarkEventPipelineAllocations` maps one 200-event RPC page
(`DefaultPageLimit`) into records and de-duplicates IDs, with and without the
`sync.Pool`-backed buffers in `pkg/stream/record.go`:

| Variant | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| unpooled | 25,500 | 72,776 | 214 |
| pooled | 20,300 | 32,056 | 201 |
| **change** | **−20%** | **−56%** | −6% |

The pooled slice and ID map account for the drop in bytes. The ~1 allocation
left per event is the record's `*Event` payload, which stays owned by sinks.

### Memory budget

`TestStreamingFiftyThousandEvents` runs the same corpus as an ordinary test
(so it runs in CI with `make test`) and enforces:

- every event delivered exactly once, in order;
- throughput ≥ 50,000 events/s (with a lower floor under `-race`);
- **≤ 8,192 bytes allocated per event**, measured over the whole run with
  `runtime.MemStats.TotalAlloc` (the measured ~423 B/event on the dedup path
  leaves wide headroom);
- no goroutines left behind.

### Reading the numbers

- The raw tailer path runs at **over 100× the 50k events/s target** on this
  machine. In production, prism is bound by RPC latency and output encoding,
  not by the core.
- `--dedup-window` costs about **30% throughput and 1 allocation per event**.
  Turn it on only if your RPC re-serves events.
- The reorder buffer is effectively allocation-free per event. Its ~430
  allocations per run are per page, not per event.
- These are diagnostics, not contracts. The contract is the test's throughput
  floor and allocation ceiling. A slower CI runner reports lower absolute
  numbers. Earlier reference runs in
  [docs/architecture.md](architecture.md#performance-benchmarks-pkgstreambench_testgo)
  saw ~1.4 M events/s raw on CI hardware.

---

## Comparing a change

Use [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) with
enough samples for a meaningful comparison:

```sh
go install golang.org/x/perf/cmd/benchstat@latest

git stash
go test -run '^$' -bench . -benchmem -count=10 ./pkg/stream > old.txt
git stash pop
go test -run '^$' -bench . -benchmem -count=10 ./pkg/stream > new.txt

benchstat old.txt new.txt
```

Include the `benchstat` output in any PR that touches `pkg/stream` hot paths
(`tail.go`, `events.go`, `record.go`, `reorder.go`, `dedup.go`).

To see *why* a number moved, profile the benchmark directly:

```sh
go test -run '^$' -bench 'TailerFiftyThousandEvents/raw' \
  -cpuprofile cpu.out -memprofile mem.out ./pkg/stream
go tool pprof -top -sample_index=alloc_space mem.out
```

For profiling a live node, see
[Live profiling with pprof](performance/tuning_and_profiling.md#2-live-profiling-with-pprof).
