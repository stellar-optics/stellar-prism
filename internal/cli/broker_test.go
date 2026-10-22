package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

type recordingKafkaProducer struct {
	mu     sync.Mutex
	calls  [][]sink.KafkaMessage
	closed int
}

func (p *recordingKafkaProducer) WriteMessages(_ context.Context, msgs ...sink.KafkaMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, append([]sink.KafkaMessage(nil), msgs...))
	return nil
}

func (p *recordingKafkaProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed++
	return nil
}

func (p *recordingKafkaProducer) all() []sink.KafkaMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []sink.KafkaMessage
	for _, c := range p.calls {
		out = append(out, c...)
	}
	return out
}

func withKafkaProducer(t *testing.T, producer sink.KafkaProducer) {
	t.Helper()
	original := newKafkaProducer
	newKafkaProducer = func([]string) sink.KafkaProducer { return producer }
	t.Cleanup(func() { newKafkaProducer = original })
}

type recordingAMQPPublisher struct {
	mu     sync.Mutex
	msgs   []sink.AMQPMessage
	closed int
}

func (p *recordingAMQPPublisher) Publish(_ context.Context, msg sink.AMQPMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	return nil
}

func (p *recordingAMQPPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed++
	return nil
}

func (p *recordingAMQPPublisher) all() []sink.AMQPMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sink.AMQPMessage(nil), p.msgs...)
}

func withAMQPPublisher(t *testing.T, publisher sink.AMQPPublisher) {
	t.Helper()
	original := newAMQPPublisher
	newAMQPPublisher = func(string, sink.AMQPOptions) (sink.AMQPPublisher, error) {
		return publisher, nil
	}
	t.Cleanup(func() { newAMQPPublisher = original })
}

// TestEventsCommandKafkaSink is the end-to-end wiring check: a stream of
// events reaches the producer instead of stdout, partitioned by contract, and
// the producer is closed when the stream finishes.
func TestEventsCommandKafkaSink(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	producer := &recordingKafkaProducer{}
	withKafkaProducer(t, producer)

	stdout, _, err := run(t, "events", "--from", "1000",
		"--kafka-brokers", "localhost:9092",
		"--kafka-topic", "soroban.events")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if stdout != "" {
		t.Errorf("broker sink also wrote to stdout: %q", stdout)
	}

	msgs := producer.all()
	if len(msgs) != 3 {
		t.Fatalf("published %d messages, want 3", len(msgs))
	}
	for i, msg := range msgs {
		if msg.Topic != "soroban.events" {
			t.Errorf("message %d topic = %q, want soroban.events", i, msg.Topic)
		}
		if got := string(msg.Key); got != "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3" {
			t.Errorf("message %d key = %q, want the contract ID", i, got)
		}

		var rec struct {
			Kind     string `json:"kind"`
			ID       string `json:"id"`
			Ledger   uint32 `json:"ledger"`
			Contract string `json:"contract"`
			Value    json.RawMessage
		}
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			t.Fatalf("message %d body is not JSON: %v\n%s", i, err, msg.Value)
		}
		if rec.Kind != "event" || rec.ID == "" || rec.Ledger != uint32(1000+i) {
			t.Errorf("message %d body = %s, want the event document", i, msg.Value)
		}
	}
	if producer.closed != 1 {
		t.Errorf("producer closed %d times, want 1", producer.closed)
	}
}

func TestEventsCommandKafkaPartitionByTx(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2000, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	producer := &recordingKafkaProducer{}
	withKafkaProducer(t, producer)

	if _, _, err := run(t, "events", "--from", "2000", "--raw",
		"--kafka-brokers", "localhost:9092",
		"--kafka-topic", "soroban.events",
		"--kafka-partition-by", "tx"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	msgs := producer.all()
	if len(msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(msgs))
	}
	// fakerpc formats the transaction hash as %064x of the ledger.
	want := "00000000000000000000000000000000000000000000000000000000000007d0"
	if got := string(msgs[0].Key); got != want {
		t.Errorf("key = %q, want the transaction hash %q", got, want)
	}
	if strings.Contains(string(msgs[0].Value), `"value":{`) {
		t.Error("--raw was ignored by the Kafka sink")
	}
}

// TestEventsCommandKafkaBatching checks both halves of the broker sink
// contract: batches reach the producer as one call, and the final partial
// batch is flushed before the producer closes.
func TestEventsCommandKafkaBatching(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(3000, 5), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	producer := &recordingKafkaProducer{}
	withKafkaProducer(t, producer)

	if _, _, err := run(t, "events", "--from", "3000", "--limit", "5",
		"--batch", "--batch-size", "2", "--batch-timeout", "1s",
		"--kafka-brokers", "localhost:9092",
		"--kafka-topic", "soroban.events"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if got := len(producer.all()); got != 5 {
		t.Errorf("published %d messages, want 5 (the final batch must be flushed)", got)
	}
	batchSeen := false
	for _, call := range producer.calls {
		if len(call) > 1 {
			batchSeen = true
		}
	}
	if !batchSeen {
		t.Errorf("producer never received a multi-message batch: %v", producer.calls)
	}
	if producer.closed != 1 {
		t.Errorf("producer closed %d times, want 1", producer.closed)
	}
}

func TestEventsCommandAMQPSink(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(4000, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	publisher := &recordingAMQPPublisher{}
	withAMQPPublisher(t, publisher)

	_, _, err := run(t, "events", "--from", "4000",
		"--amqp-url", "amqp://guest:guest@localhost:5672/",
		"--amqp-queue", "soroban.events",
		"--amqp-exchange", "stellar",
		"--amqp-routing-key", "events.all")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	msgs := publisher.all()
	if len(msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(msgs))
	}
	for i, msg := range msgs {
		if msg.Exchange != "stellar" || msg.RoutingKey != "events.all" {
			t.Errorf("message %d routed to %q/%q, want stellar/events.all", i, msg.Exchange, msg.RoutingKey)
		}
		if msg.Kind != "event" {
			t.Errorf("message %d kind = %q, want event", i, msg.Kind)
		}
		if !json.Valid(msg.Body) {
			t.Errorf("message %d body is not JSON: %s", i, msg.Body)
		}
	}
	if publisher.closed != 1 {
		t.Errorf("publisher closed %d times, want 1", publisher.closed)
	}
}

func TestBrokerFlagsRequireCompleteConfiguration(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "kafka topic without brokers",
			args: []string{"events", "--from", "100", "--kafka-topic", "events"},
		},
		{
			name: "kafka brokers without topic",
			args: []string{"events", "--from", "100", "--kafka-brokers", "localhost:9092"},
		},
		{
			name: "amqp queue without url",
			args: []string{"events", "--from", "100", "--amqp-queue", "events"},
		},
		{
			name: "amqp url without queue",
			args: []string{"events", "--from", "100", "--amqp-url", "amqp://localhost"},
		},
		{
			name: "two broker sinks at once",
			args: []string{"events", "--from", "100",
				"--kafka-brokers", "localhost:9092", "--kafka-topic", "events",
				"--amqp-url", "amqp://localhost", "--amqp-queue", "events"},
		},
		{
			name: "unknown kafka partition strategy",
			args: []string{"events", "--from", "100",
				"--kafka-brokers", "localhost:9092", "--kafka-topic", "events",
				"--kafka-partition-by", "ledger"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestEventsHelpAdvertisesBrokerFlags(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{
		"--kafka-brokers", "--kafka-topic", "--kafka-partition-by",
		"--amqp-url", "--amqp-queue", "--amqp-exchange", "--amqp-routing-key",
	} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s", flag)
		}
	}
}

// TestBrokerSinkClosesOnCancellation covers the goroutine/resource rule: a
// broker connection must not outlive a cancelled stream.
func TestBrokerSinkClosesOnCancellation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 5), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	producer := &recordingKafkaProducer{}
	withKafkaProducer(t, producer)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var out, errBuf bytes.Buffer
	root := newRootCmd(&out, &errBuf)
	root.SetArgs([]string{"events", "--from", "5000", "--follow",
		"--poll", "10ms",
		"--kafka-brokers", "localhost:9092",
		"--kafka-topic", "soroban.events"})
	root.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ExecuteWithContext() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled stream did not terminate")
	}

	producer.mu.Lock()
	defer producer.mu.Unlock()
	if producer.closed != 1 {
		t.Errorf("producer closed %d times after cancellation, want 1", producer.closed)
	}
}
