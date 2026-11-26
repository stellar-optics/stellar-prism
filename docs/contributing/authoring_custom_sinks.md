# Authoring custom prism sinks in Go

prism ships sinks for stdout, files, Kafka, AMQP, NATS, ZeroMQ, webhooks,
Elasticsearch, S3/GCS, Parquet, syslog, SQS/SNS, Pub/Sub and more. When you
need to deliver records somewhere else, such as an in-house message bus, a
time-series database or a bespoke logging pipeline, you write a **sink**: a
small Go type that receives each record from prism's streaming engine.

This guide covers the sink contract, how to handle buffering, concurrency
and backpressure without breaking prism's delivery guarantee, and a complete,
tested `RedisStreamSink` that appends records to a Redis Stream with `XADD`.

- [Before you start: do you need a custom sink?](#before-you-start-do-you-need-a-custom-sink)
- [The Sink contract](#the-sink-contract)
- [Backpressure, buffering and concurrency](#backpressure-buffering-and-concurrency)
- [Tutorial: a Redis Streams sink](#tutorial-a-redis-streams-sink)
- [Contributing a sink to prism itself](#contributing-a-sink-to-prism-itself)
- [Checklist](#checklist)

## Before you start: do you need a custom sink?

You may not need one at all:

- **The webhook sink** (`--webhook-url`) POSTs signed JSON batches to any
  HTTP endpoint, with retries.
- **The broker sinks** (`--kafka-topic`, `--amqp-queue`, `--sink-nats`,
  `--sink-zmq`) cover most message buses. Your consumers can then do the
  integration work.
- **NDJSON on stdout** (`--json`) pipes into any program.

Write a custom sink when you need in-process delivery: a protocol none of
those speak, transactional writes alongside the cursor, or no extra hop.
There are two ways to use one:

1. **In your own program**, importing prism as a library. This needs no fork
   and is what the tutorial builds.
2. **As a built-in sink with CLI flags**, contributed upstream. See
   [Contributing a sink to prism itself](#contributing-a-sink-to-prism-itself).

## The Sink contract

The interface is defined in
[`pkg/stream/source.go`](../../pkg/stream/source.go):

```go
// Sink consumes records produced by a stream.
type Sink interface {
	// Emit writes a single record.
	Emit(r Record) error
	// Flush releases any buffered output. It is called when the stream ends,
	// including on cancellation.
	Flush() error
}
```

A sink that holds connections, files or goroutines should also implement
**`io.Closer`**.

### Lifecycle

prism splits a sink's lifecycle across the constructor, the interface and
`io.Closer`. If you are used to an `Open / Write / Flush / Close` shape, this
is how the phases map:

| Phase | In prism | Called by | Your job |
|---|---|---|---|
| Open | Your constructor, e.g. `New(ctx, opts) (*MySink, error)` | You, before the stream starts | Validate options, connect, check the destination is reachable, start any background goroutine. Fail here, before anything is fetched. |
| Write | `Emit(r stream.Record) error` | The tailer, once per record, in order | Accept responsibility for the record, or return an error |
| Flush | `Flush() error` | The tailer, when the stream ends (normally, on `--limit`, or on cancellation), bounded by the drain timeout | Deliver everything accepted so far |
| Close | `Close() error` (`io.Closer`) | Whoever built the sink, after the final `Flush`: the CLI for built-in sinks, your `defer` in library code | Stop goroutines and wait for them, release connections. Must be idempotent. |

There is no context parameter on `Emit` or `Flush`. Bound any blocking I/O
with your own timeouts. The tailer's drain timeout stops waiting for a
`Flush` that overruns, but it cannot interrupt your code.

### The guarantees you get

- **One goroutine.** The tailer calls `Emit` and `Flush` from a single
  goroutine, never concurrently. You only need locking for state shared with
  goroutines *you* start.
- **Ordered, de-duplicated records.** Records arrive in ascending `Record.ID`
  order, with server replays already removed. `ID` is fixed-width and
  zero-padded, so a plain string comparison orders it. It is the key to
  de-duplicate on downstream.
- **One payload per kind.** `Record.Kind` says which payload pointer is set
  (`Event`, `Transaction`, `FeeStats`, ...).

### The guarantees you must give

- **A nil return from `Emit` means the sink has taken responsibility for the
  record.** The tailer persists the stream position (`--state-file`,
  `--pg-dsn`) as soon as `Emit` returns nil. If the process then crashes, the
  record will **not** be fetched again. Anything you accepted but had not yet
  delivered is lost, so keep that window small and bounded.
- **A non-nil return stops the stream** (unless the CLI is wrapping your sink
  with `--dlq-file` / `--max-record-retries`), and the position does not move
  past the failed record. Returning an error is always safe; dropping a
  record silently never is.
- **`Flush` delivers everything accepted so far**, or returns an error.
- **Never mutate the `Record`.** With several sinks configured, the same
  record, including its payload pointers, is passed to each of them.
- **Do not fail on an undecodable payload.** Render it inline, or keep the raw
  XDR, so one malformed value cannot kill a long-running tail.

### Rendering records

Use `sink.MarshalRecord(r, decode)` to get the same JSON object `--json`
writes, byte for byte. Consumers of your sink then see the same schema as
every other prism output. `decode=false` keeps base64 XDR, like `--raw`. For
custom formats, decode XDR through
[stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens) rather
than the `xdr` package directly, so values render the same as everywhere else
in prism.

For a trivial sink with nothing to flush, `stream.SinkFunc` adapts a
function:

```go
out := stream.SinkFunc(func(r stream.Record) error {
	_, err := fmt.Println(r.ID, r.Kind, r.Sequence)
	return err
})
```

## Backpressure, buffering and concurrency

prism's streaming loop is deliberately single-threaded: fetch a page, emit
its records, advance the cursor, repeat. **Time spent in `Emit` is time not
spent polling.** That is the backpressure mechanism: a slow destination slows
fetching instead of filling an unbounded queue. Your design choice is how
much slack to put between the two.

| Design | `Emit` does | Blocks polling | Records at risk on a crash | Use when |
|---|---|---|---|---|
| **Synchronous** | Writes the record and waits | On every record | None | Destination is fast or local; low volume |
| **Batch in `Emit`** | Appends to a slice and writes when it reaches N | Once per N records | Up to N−1 | Destination prefers batches; you want no goroutines |
| **Bounded queue + writer goroutine** | Enqueues and returns | Only when the queue is full | Up to queue size + one batch | Bursty traffic; batched destinations with variable latency |

The tutorial uses the third design, because it is the one with the most ways
to go wrong. The rules that keep it correct:

1. **Bound the queue.** Use a buffered channel of fixed size, and have `Emit`
   block when it is full. An unbounded queue turns a slow destination into
   an out-of-memory crash, and widens the crash-loss window without limit.
2. **Own your goroutines.** Start them in the constructor. `Close` must
   signal them to stop *and wait* for them to exit. A finished stream leaves
   nothing running.
3. **Make errors sticky and visible.** When the background writer fails,
   record the error and make every later `Emit` and `Flush` return it, so the
   stream stops instead of acknowledging records that will never be written.
4. **Flush through the same queue.** Send the flush request down the channel
   the records use, so it is processed after every record queued before it.
   Ordering then needs no extra locking.
5. **Time-box every network call** (`context.WithTimeout`), because `Emit`
   and `Flush` get no context to cancel.
6. **Marshal on the caller's goroutine.** A malformed record then fails
   against the record that caused it, before it reaches the queue.
7. **Expect duplicates across restarts.** prism is at-least-once: after a
   crash, the last record may be delivered again. Include `Record.ID` in what
   you write so consumers can de-duplicate.

## Tutorial: a Redis Streams sink

We will build `RedisStreamSink`, which appends each record to a
[Redis Stream](https://redis.io/docs/latest/develop/data-types/streams/) as
an entry with four fields: `id`, `kind`, `ledger` and `record` (the JSON).
It batches entries into pipelined `XADD` calls, flushes partial batches on a
timer, optionally caps the stream length, and has a test suite that runs
without a Redis server.

The code below was compiled and tested against prism `main` with
`github.com/redis/go-redis/v9` v9.24.0 and
`github.com/alicebob/miniredis/v2` v2.40.0.

### Step 1: create a module

```console
mkdir prism-redis && cd prism-redis
go mod init example.com/prism-redis
go get github.com/stellar-optics/stellar-prism@main   # v0.1.0 predates sink.MarshalRecord and sink.MultiSink
go get github.com/redis/go-redis/v9
go get github.com/alicebob/miniredis/v2   # tests only
mkdir -p redissink cmd/prism-redis
```

Layout when done:

```text
prism-redis/
├── go.mod
├── redissink/
│   ├── redissink.go        # the sink
│   └── redissink_test.go   # tests against an in-process Redis
└── cmd/prism-redis/
    └── main.go             # follows a contract into Redis
```

### Step 2: the sink

Save as `redissink/redissink.go`:

```go
// Package redissink is a prism sink that appends every record to a Redis
// Stream with XADD.
package redissink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ErrClosed is returned by Emit and Flush after Close.
var ErrClosed = errors.New("redis sink: closed")

// Options configures a RedisStreamSink. Only Stream and one of Addr or
// Client are required.
type Options struct {
	// Addr is the Redis server, e.g. "localhost:6379". Ignored when Client
	// is set.
	Addr string
	// Client is an existing client to write through. The sink does not
	// close a client it was given.
	Client redis.UniversalClient
	// Stream is the Redis stream key, e.g. "prism:events".
	Stream string
	// MaxLen caps the stream with XADD MAXLEN ~ (approximate trimming,
	// which is far cheaper than exact). Zero leaves the stream unbounded.
	MaxLen int64
	// BatchSize is how many entries go into one pipelined round trip.
	// Default 100.
	BatchSize int
	// FlushInterval is the longest a partial batch waits. Default 250ms.
	FlushInterval time.Duration
	// BufferSize is how many records may wait for the writer before Emit
	// blocks. It bounds both memory and how far delivery can lag the
	// stream position. Default 4 * BatchSize.
	BufferSize int
	// WriteTimeout bounds one pipelined round trip. Default 5s.
	WriteTimeout time.Duration
	// Decode renders XDR the way --json does. False keeps the base64 XDR,
	// as --raw does.
	Decode bool
}

func (o Options) withDefaults() Options {
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 250 * time.Millisecond
	}
	if o.BufferSize <= 0 {
		o.BufferSize = 4 * o.BatchSize
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	return o
}

// item is one message to the writer goroutine: an entry to append, or a
// flush request when done is set.
type item struct {
	values map[string]any
	done   chan error
}

// RedisStreamSink implements stream.Sink and io.Closer.
//
// Emit, Flush and Close must be called from one goroutine, which is how the
// prism tailer calls them. The sink owns exactly one goroutine of its own,
// the writer, and Close waits for it to exit.
type RedisStreamSink struct {
	opts       Options
	client     redis.UniversalClient
	ownsClient bool

	queue  chan item     // bounded: a full queue blocks Emit (backpressure)
	quit   chan struct{} // closed by Close to stop the writer
	done   chan struct{} // closed by the writer when it exits
	failed chan struct{} // closed by the writer on its first error

	mu  sync.Mutex
	err error // the writer's first error, sticky

	closeOnce sync.Once
	closeErr  error
}

// New connects to Redis, checks it is reachable, and starts the writer.
// This is the "open" step: it fails fast, before any record is fetched.
func New(ctx context.Context, opts Options) (*RedisStreamSink, error) {
	opts = opts.withDefaults()
	if opts.Stream == "" {
		return nil, errors.New("redis sink: a stream key is required")
	}
	client, owns := opts.Client, false
	if client == nil {
		if opts.Addr == "" {
			return nil, errors.New("redis sink: Addr or Client is required")
		}
		client, owns = redis.NewClient(&redis.Options{Addr: opts.Addr}), true
	}
	if err := client.Ping(ctx).Err(); err != nil {
		if owns {
			_ = client.Close()
		}
		return nil, fmt.Errorf("redis sink: ping %s: %w", opts.Addr, err)
	}

	s := &RedisStreamSink{
		opts:       opts,
		client:     client,
		ownsClient: owns,
		queue:      make(chan item, opts.BufferSize),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		failed:     make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// Emit hands one record to the writer. It returns as soon as the record is
// queued, and blocks only while the queue is full, which slows the polling
// loop to the speed Redis can absorb instead of growing memory.
func (s *RedisStreamSink) Emit(r stream.Record) error {
	select {
	case <-s.done:
		return s.stoppedErr()
	default:
	}

	// Marshal on the caller's goroutine: a malformed record is reported
	// against the record that caused it, and it never reaches the queue.
	payload, err := sink.MarshalRecord(r, s.opts.Decode)
	if err != nil {
		return fmt.Errorf("redis sink: marshal record %s: %w", r.ID, err)
	}
	it := item{values: map[string]any{
		"id":     r.ID, // fixed-width, ordered: consumers de-duplicate on it
		"kind":   string(r.Kind),
		"ledger": r.Sequence,
		"record": string(payload),
	}}

	select {
	case s.queue <- it:
		return nil
	case <-s.failed:
		return s.stoppedErr()
	}
}

// Flush returns once every record emitted so far has been written to Redis,
// or with the error that stopped the writer.
func (s *RedisStreamSink) Flush() error {
	req := item{done: make(chan error, 1)}
	select {
	case s.queue <- req:
	case <-s.done:
		return s.stoppedErr()
	}
	select {
	case err := <-req.done:
		return err
	case <-s.done:
		return s.stoppedErr()
	}
}

// Close flushes, stops the writer, waits for it, and closes the client if
// the sink created it. It is safe to call more than once.
func (s *RedisStreamSink) Close() error {
	s.closeOnce.Do(func() {
		err := s.Flush()
		close(s.quit)
		<-s.done
		if s.ownsClient {
			err = errors.Join(err, s.client.Close())
		}
		s.closeErr = err
	})
	return s.closeErr
}

// run is the writer goroutine: it batches entries and pipelines them to
// Redis, by size or by time, whichever comes first.
func (s *RedisStreamSink) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.opts.FlushInterval)
	defer ticker.Stop()

	batch := make([]map[string]any, 0, s.opts.BatchSize)
	write := func() error {
		if len(batch) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.opts.WriteTimeout)
		defer cancel()
		_, err := s.client.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, values := range batch {
				p.XAdd(ctx, &redis.XAddArgs{
					Stream: s.opts.Stream,
					MaxLen: s.opts.MaxLen,
					Approx: s.opts.MaxLen > 0,
					Values: values,
				})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("redis sink: XADD %d entries to %s: %w", len(batch), s.opts.Stream, err)
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-s.quit:
			return
		case it := <-s.queue:
			if it.done != nil {
				err := write()
				it.done <- err
				if err != nil {
					s.fail(err)
					return
				}
				continue
			}
			batch = append(batch, it.values)
			if len(batch) >= s.opts.BatchSize {
				if err := write(); err != nil {
					s.fail(err)
					return
				}
			}
		case <-ticker.C:
			if err := write(); err != nil {
				s.fail(err)
				return
			}
		}
	}
}

func (s *RedisStreamSink) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		close(s.failed)
	}
}

func (s *RedisStreamSink) stoppedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return ErrClosed
}

// Compile-time checks that the sink satisfies the interfaces prism uses.
var _ stream.Sink = (*RedisStreamSink)(nil)
```

#### How it works

- **Open: `New`.** Applies defaults, validates the options, connects, and
  `PING`s so a wrong address fails before any RPC call. Only then does it
  start the writer goroutine. A client passed in through `Options.Client`
  belongs to the caller and is never closed by the sink.
- **Write: `Emit`.** Marshals the record with `sink.MarshalRecord`, the same
  JSON as `--json`, then sends it to the bounded `queue`. While the queue has
  room, `Emit` returns at once and polling continues. When it is full, `Emit`
  blocks until the writer catches up: that is the backpressure. If the writer
  has failed, `Emit` returns its error instead.
- **The writer goroutine: `run`.** The only goroutine the sink owns. It
  collects entries into a batch and sends the batch as one pipelined round
  trip of `XADD` commands, when the batch reaches `BatchSize` or when
  `FlushInterval` ticks, whichever comes first. Each round trip is bounded by
  `WriteTimeout`. `MaxLen` adds `MAXLEN ~`, Redis's cheap approximate
  trimming. go-redis already retries transient network errors, so the first
  error that reaches the writer is treated as fatal.
- **Sticky failure: `fail` / `stoppedErr`.** The first write error is
  recorded under a mutex and `failed` is closed. Every later call returns
  that error, the tailer stops, and the stream position stays at the last
  record the tailer saw accepted.
- **Flush: `Flush`.** Sends a request with a reply channel down the same
  queue. Because the queue is FIFO, the writer reaches the request only after
  every earlier record, writes the partial batch, and replies.
- **Close: `Close`.** Flushes, closes `quit`, waits on `done` for the writer
  to exit, then closes the client if the sink created it. `sync.Once` makes a
  second `Close` return the first result.

#### What is at risk on a crash

At most `BufferSize` queued records plus one batch in flight: records `Emit`
accepted but Redis had not yet stored. Lower `BufferSize` and `BatchSize` to
shrink that window, at the cost of more blocking. With `BufferSize: 1,
BatchSize: 1` the sink is effectively synchronous.

### Step 3: test it without a Redis server

[miniredis](https://github.com/alicebob/miniredis) runs a Redis-compatible
server inside the test process, which keeps the tests offline, fast and
deterministic. Save as `redissink/redissink_test.go`:

```go
package redissink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func record(i int) stream.Record {
	return stream.Record{
		ID:       fmt.Sprintf("%019d", i),
		Sequence: uint32(1000 + i),
		Time:     time.Unix(1_700_000_000, 0).UTC(),
		Kind:     stream.KindFeeStats,
		FeeStats: &stream.FeeStats{BaseFee: 100},
	}
}

func newSink(t *testing.T, opts Options) (*RedisStreamSink, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	opts.Client = client
	if opts.Stream == "" {
		opts.Stream = "prism:test"
	}
	s, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, mr, client
}

func TestWritesEveryRecordInOrder(t *testing.T) {
	s, _, client := newSink(t, Options{BatchSize: 100, Decode: true})
	for i := 1; i <= 250; i++ {
		if err := s.Emit(record(i)); err != nil {
			t.Fatalf("Emit %d: %v", i, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	entries, err := client.XRange(context.Background(), "prism:test", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 250 {
		t.Fatalf("stream has %d entries, want 250", len(entries))
	}
	for i, e := range entries {
		if want := fmt.Sprintf("%019d", i+1); e.Values["id"] != want {
			t.Fatalf("entry %d id = %v, want %s", i, e.Values["id"], want)
		}
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(entries[0].Values["record"].(string)), &body); err != nil {
		t.Fatalf("record field is not JSON: %v", err)
	}
	if body["kind"] != "fee_stats" {
		t.Fatalf("record kind = %v, want fee_stats", body["kind"])
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPartialBatchIsWrittenByTimer(t *testing.T) {
	s, _, client := newSink(t, Options{BatchSize: 100, FlushInterval: 20 * time.Millisecond})
	defer s.Close()
	for i := 1; i <= 3; i++ {
		if err := s.Emit(record(i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, err := client.XLen(context.Background(), "prism:test").Result()
		if err == nil && n == 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("XLEN = %d (err %v) after 2s, want 3 written by the timer", n, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWriteFailureStopsTheStream(t *testing.T) {
	s, mr, _ := newSink(t, Options{BatchSize: 1, WriteTimeout: time.Second})
	mr.Close()
	var err error
	for i := 1; i <= 50 && err == nil; i++ {
		err = s.Emit(record(i))
	}
	if err == nil {
		err = s.Flush()
	}
	if err == nil || errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want the Redis write failure surfaced", err)
	}
	if closeErr := s.Close(); closeErr == nil {
		t.Fatal("Close after a failure must report it")
	}
}

func TestEmitAfterClose(t *testing.T) {
	s, _, _ := newSink(t, Options{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := s.Emit(record(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Emit after Close = %v, want ErrClosed", err)
	}
}

func TestMaxLenTrimsTheStream(t *testing.T) {
	s, _, client := newSink(t, Options{MaxLen: 10, BatchSize: 5})
	for i := 1; i <= 100; i++ {
		if err := s.Emit(record(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	n, err := client.XLen(context.Background(), "prism:test").Result()
	if err != nil {
		t.Fatal(err)
	}
	if n >= 100 {
		t.Fatalf("XLEN = %d, want the stream trimmed toward MaxLen 10", n)
	}
}
```

```console
$ go test ./...
?   	example.com/prism-redis/cmd/prism-redis	[no test files]
ok  	example.com/prism-redis/redissink	0.928s
```

The tests cover order and completeness across several batches, the timer
flushing a partial batch, a failed Redis surfacing as an error (rather than
records being dropped), use after `Close`, and `MaxLen` trimming.

### Step 4: wire it into a stream

Save as `cmd/prism-redis/main.go`:

```go
// Command prism-redis follows a contract's events and appends each one to a
// Redis Stream, while still printing NDJSON to stdout.
//
//	go run ./cmd/prism-redis -contract CDLZF... -redis localhost:6379
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"

	"example.com/prism-redis/redissink"
)

func main() {
	contract := flag.String("contract", "", "contract ID to follow (required)")
	rpcURL := flag.String("rpc-url", rpc.DefaultURL, "Soroban RPC endpoint")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	key := flag.String("stream", "prism:events", "Redis stream key")
	flag.Parse()
	if *contract == "" {
		log.Fatal("-contract is required")
	}

	// Stop cleanly on Ctrl-C or SIGTERM: the tailer flushes the sink as it
	// returns, and the deferred Close drains and stops the writer.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := rpc.New(*rpcURL, 30*time.Second)
	src, err := stream.NewEventSource(client, stream.EventQuery{ContractIDs: []string{*contract}})
	if err != nil {
		log.Fatal(err)
	}

	redisOut, err := redissink.New(ctx, redissink.Options{
		Addr:   *redisAddr,
		Stream: *key,
		MaxLen: 1_000_000,
		Decode: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := redisOut.Close(); err != nil {
			log.Printf("closing redis sink: %v", err)
		}
	}()

	// MultiSink delivers each record to stdout and to Redis. With
	// MultiSinkAbortAll, a failure in either stops the stream rather than
	// letting the two outputs drift apart.
	out, err := sink.NewMultiSink(
		[]stream.Sink{sink.NewNDJSON(os.Stdout, true), redisOut},
		sink.MultiSinkOptions{Policy: sink.MultiSinkAbortAll},
	)
	if err != nil {
		log.Fatal(err)
	}

	tailer := stream.NewTailer(src, out, stream.Options{Follow: true})
	if err := tailer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		var gap *stream.GapError
		if errors.As(err, &gap) {
			log.Printf("stream gapped: %v", gap)
			os.Exit(2)
		}
		log.Fatal(err)
	}
}
```

Points to note:

- **Sink lifetime.** The tailer calls `Flush` when the stream ends. Closing
  is your job, hence the `defer redisOut.Close()`, which runs after
  `tailer.Run` returns.
- **Several outputs.** `sink.NewMultiSink` delivers each record to every
  sink in the list, as the CLI does when several sink flags are set. With
  `MultiSinkAbortAll`, a failure in any sink stops the stream, so the outputs
  never silently diverge.
- **Gaps.** A `*stream.GapError` means records were pruned before they could
  be read. Exiting 2 matches the CLI's convention. See
  [the troubleshooting guide](../troubleshooting/streaming_gaps_and_lag.md).
- **Resuming.** To resume across restarts, set `stream.Options.State` or
  `Options.Store` (see the [API reference](https://pkg.go.dev/github.com/stellar-optics/stellar-prism/pkg/stream)).
  Without them, a restart starts at the current head.

### Step 5: run it

```console
# A local Redis
docker run --rm -d -p 6379:6379 --name redis redis:7

# Follow a testnet contract into the stream "prism:events"
go run ./cmd/prism-redis -contract CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC

# In another terminal, read entries as they arrive
redis-cli XREAD BLOCK 0 STREAMS prism:events '$'
redis-cli XLEN prism:events
```

Consumers can use Redis consumer groups (`XGROUP CREATE`, `XREADGROUP`,
`XACK`) to share the work across processes, de-duplicating on the `id`
field after a prism restart.

## Contributing a sink to prism itself

To make a sink available as CLI flags on every streaming command:

1. **Put the sink in `pkg/sink/<name>.go`.** Define a small interface for the
   network client, as `NATSPublisher` does in
   [`pkg/sink/nats.go`](../../pkg/sink/nats.go), so tests can inject a fake.
   **Tests must not touch the network**; that is a project rule (see
   [CONTRIBUTING.md](../../CONTRIBUTING.md)).
2. **Add flags in [`internal/cli/sinks.go`](../../internal/cli/sinks.go):**
   - a field in `sinkFlags`;
   - registration in `sinkFlags.register`, with a `--sink-<name>-*` or
     `--<name>-*` prefix, off by default;
   - validation in `sinkFlags.validate`, so bad combinations fail before
     any RPC call;
   - construction in `sinkFlags.addTo`. Append the sink to `sinks`, and
     append it to `s.constructedSinks` if it implements `io.Closer`, so the
     CLI closes it.
3. **Read secrets from the environment** with an explicit fallback, as
   `--webhook-secret` falls back to `PRISM_WEBHOOK_SECRET`. Every flag is
   also configurable through `PRISM_<FLAG>` and config files automatically
   (see [config options](../cli/config_options.md)).
4. **Document it** in the "Built-in sinks" section of
   [`docs/architecture.md`](../architecture.md#built-in-sinks) and in
   [`docs/cli/command_reference.md`](../cli/command_reference.md#additional-sinks).
5. **Test it** table-driven, through the fake client: delivery, batching,
   error propagation, and `Close` stopping every goroutine.

## Checklist

- [ ] `Emit` returns nil only for records the sink has taken responsibility for
- [ ] Any internal buffer is bounded, and `Emit` blocks rather than drops when it is full
- [ ] `Flush` delivers everything accepted so far, or returns an error
- [ ] Background errors are sticky and surface from the next `Emit` or `Flush`
- [ ] Every network call has a timeout
- [ ] `Close` stops and waits for every goroutine, closes owned resources, and is idempotent
- [ ] Records are never mutated; output carries `Record.ID` for de-duplication
- [ ] JSON output uses `sink.MarshalRecord` so it matches `--json`
- [ ] Tests run offline (a fake client or an in-process server) and pass with `-race`
