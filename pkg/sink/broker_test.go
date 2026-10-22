package sink_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// fakeKafkaProducer records every batch it is handed, so tests can assert on
// topics, keys and bodies without a broker.
type fakeKafkaProducer struct {
	mu       sync.Mutex
	calls    [][]sink.KafkaMessage
	err      error
	closeErr error
	closed   int
}

func (f *fakeKafkaProducer) WriteMessages(_ context.Context, msgs ...sink.KafkaMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, append([]sink.KafkaMessage(nil), msgs...))
	return nil
}

func (f *fakeKafkaProducer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func (f *fakeKafkaProducer) messages() []sink.KafkaMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []sink.KafkaMessage
	for _, call := range f.calls {
		out = append(out, call...)
	}
	return out
}

// fakeAMQPPublisher records every message it is handed.
type fakeAMQPPublisher struct {
	mu       sync.Mutex
	msgs     []sink.AMQPMessage
	err      error
	closeErr error
	closed   int
}

func (f *fakeAMQPPublisher) Publish(_ context.Context, msg sink.AMQPMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msg)
	return nil
}

func (f *fakeAMQPPublisher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func (f *fakeAMQPPublisher) published() []sink.AMQPMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sink.AMQPMessage(nil), f.msgs...)
}

func transactionRecord() stream.Record {
	return stream.Record{
		ID:       "0000000429496729600-0000000000",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindTransaction,
		Transaction: &stream.Transaction{
			Hash:          "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
			Index:         2,
			Successful:    true,
			EnvelopeXDR:   scValU32,
			ResultXDR:     scValU32,
			ResultMetaXDR: scValU32,
		},
	}
}

func TestKafkaPartitionsByContract(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{
		Topic:       "soroban.events",
		PartitionBy: sink.PartitionContract,
	}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	recs := []stream.Record{eventRecord(), ledgerRecord(), transactionRecord()}
	if err := k.EmitBatch(recs); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	msgs := producer.messages()
	if len(msgs) != len(recs) {
		t.Fatalf("published %d messages, want %d", len(msgs), len(recs))
	}

	wantKeys := []string{
		eventRecord().Event.ContractID,
		ledgerRecord().Close.Hash,
		transactionRecord().Transaction.Hash,
	}
	for i, want := range wantKeys {
		if got := string(msgs[i].Key); got != want {
			t.Errorf("message %d key = %q, want %q", i, got, want)
		}
		if msgs[i].Topic != "soroban.events" {
			t.Errorf("message %d topic = %q, want soroban.events", i, msgs[i].Topic)
		}
	}
}

func TestKafkaPartitionsByTx(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{
		Topic:       "soroban.events",
		PartitionBy: sink.PartitionTx,
	}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	ev := eventRecord()
	tx := transactionRecord()
	if err := k.EmitBatch([]stream.Record{ev, tx}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	msgs := producer.messages()
	if got, want := string(msgs[0].Key), ev.Event.TxHash; got != want {
		t.Errorf("event key = %q, want its transaction hash %q", got, want)
	}
	if got, want := string(msgs[1].Key), tx.Transaction.Hash; got != want {
		t.Errorf("transaction key = %q, want %q", got, want)
	}
}

func TestKafkaPartitionsByRecord(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{
		Topic:       "soroban.events",
		PartitionBy: sink.PartitionRecord,
	}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	ev := eventRecord()
	if err := k.Emit(ev); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got := string(producer.messages()[0].Key); got != ev.ID {
		t.Errorf("key = %q, want the record ID %q", got, ev.ID)
	}
}

// TestKafkaKeyIsNeverEmpty is the invariant that makes per-contract ordering
// meaningful: an empty key would round-robin instead.
func TestKafkaKeyIsNeverEmpty(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	bare := stream.Record{ID: "0000000001", Kind: stream.KindEvent, Event: &stream.Event{}}
	if err := k.Emit(bare); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got := string(producer.messages()[0].Key); got != bare.ID {
		t.Errorf("key = %q, want the record ID %q as the fallback", got, bare.ID)
	}
}

// TestKafkaBodyMatchesNDJSON is the compatibility contract: a broker consumer
// parses exactly what `prism --json` prints.
func TestKafkaBodyMatchesNDJSON(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t", Decode: true}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	if err := k.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	body := producer.messages()[0].Value
	want, err := sink.EncodeRecord(eventRecord(), true)
	if err != nil {
		t.Fatalf("EncodeRecord() error = %v", err)
	}
	if string(body) != string(want) {
		t.Errorf("body = %s\nwant %s", body, want)
	}

	// And EncodeRecord must be a single JSON object with no trailing newline.
	if strings.HasSuffix(string(body), "\n") {
		t.Error("broker body has NDJSON framing; messages must be one object")
	}
	var rec struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatalf("body is not valid JSON: %v\n%s", err, body)
	}
	if rec.Kind != "event" || rec.ID != eventRecord().ID {
		t.Errorf("body = %s, want the event document", body)
	}
}

func TestKafkaRawModePreservesXDR(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	if err := k.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	body := string(producer.messages()[0].Value)
	if !strings.Contains(body, `"valueXdr":"`+scValU32+`"`) {
		t.Errorf("raw mode did not preserve base64 XDR\n%s", body)
	}
	if strings.Contains(body, `"value":{`) {
		t.Errorf("raw mode decoded the payload anyway\n%s", body)
	}
}

func TestKafkaBatchesIntoOneProducerCall(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	recs := []stream.Record{eventRecord(), eventRecord(), eventRecord()}
	if err := k.EmitBatch(recs); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(producer.calls) != 1 {
		t.Fatalf("producer saw %d calls, want 1", len(producer.calls))
	}
	if got := len(producer.calls[0]); got != len(recs) {
		t.Errorf("batch carried %d messages, want %d", got, len(recs))
	}

	// An empty batch must not reach the broker at all.
	if err := k.EmitBatch(nil); err != nil {
		t.Fatalf("EmitBatch(nil) error = %v", err)
	}
	if len(producer.calls) != 1 {
		t.Errorf("empty batch reached the producer")
	}
}

func TestKafkaPropagatesFailures(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{err: errors.New("broker unavailable")}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}

	err = k.Emit(eventRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want the producer failure")
	}
	if !strings.Contains(err.Error(), "broker unavailable") {
		t.Errorf("error = %q, want it to wrap the producer failure", err)
	}
}

func TestKafkaRejectsBadConfig(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, nil); err == nil {
		t.Error("NewKafka(nil producer) error = nil, want an error")
	}
	if _, err := sink.NewKafka(sink.KafkaConfig{}, &fakeKafkaProducer{}); err == nil {
		t.Error("NewKafka(no topic) error = nil, want an error")
	}
	if _, err := sink.NewKafka(sink.KafkaConfig{Topic: "t", PartitionBy: "nonsense"}, &fakeKafkaProducer{}); err == nil {
		t.Error("NewKafka(bad partition) error = nil, want an error")
	}
}

func TestKafkaCloseReleasesProducer(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if producer.closed != 1 {
		t.Errorf("producer closed %d times, want 1", producer.closed)
	}

	producer.closeErr = errors.New("close failed")
	if err := k.Close(); err == nil {
		t.Error("Close() error = nil, want the producer's close failure")
	}
}

func TestKafkaRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	k, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, producer)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	if err := k.Emit(stream.Record{ID: "x", Kind: "nonsense"}); err == nil {
		t.Error("Emit(unknown kind) error = nil, want an error")
	}
	if len(producer.messages()) != 0 {
		t.Error("an unencodable record was published")
	}
}

func TestParseKafkaPartition(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "contract", "CONTRACT", "tx", "record"} {
		if _, err := sink.ParseKafkaPartition(in); err != nil {
			t.Errorf("ParseKafkaPartition(%q) error = %v", in, err)
		}
	}
	if _, err := sink.ParseKafkaPartition("chain"); err == nil {
		t.Error("ParseKafkaPartition(bad) error = nil, want an error")
	}
}

func TestAMQPPublishesDurableMessages(t *testing.T) {
	t.Parallel()

	publisher := &fakeAMQPPublisher{}
	a, err := sink.NewAMQP(sink.AMQPConfig{Queue: "soroban.events", Decode: true}, publisher)
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}

	rec := eventRecord()
	if err := a.Emit(rec); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	msgs := publisher.published()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}
	msg := msgs[0]
	if msg.RoutingKey != "soroban.events" {
		t.Errorf("routing key = %q, want the queue name by default", msg.RoutingKey)
	}
	if msg.MessageID != rec.ID {
		t.Errorf("message ID = %q, want the record ID %q", msg.MessageID, rec.ID)
	}
	if msg.Kind != "event" {
		t.Errorf("message kind = %q, want event", msg.Kind)
	}
	if !msg.Timestamp.Equal(rec.Time) {
		t.Errorf("timestamp = %v, want %v", msg.Timestamp, rec.Time)
	}

	want, err := sink.EncodeRecord(rec, true)
	if err != nil {
		t.Fatalf("EncodeRecord() error = %v", err)
	}
	if string(msg.Body) != string(want) {
		t.Errorf("body = %s\nwant %s", msg.Body, want)
	}
}

func TestAMQPRoutingOptions(t *testing.T) {
	t.Parallel()

	publisher := &fakeAMQPPublisher{}
	a, err := sink.NewAMQP(sink.AMQPConfig{
		Queue:      "soroban.events",
		Exchange:   "stellar",
		RoutingKey: "events.all",
	}, publisher)
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}
	if err := a.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	msg := publisher.published()[0]
	if msg.Exchange != "stellar" || msg.RoutingKey != "events.all" {
		t.Errorf("exchange/routing key = %q/%q, want stellar/events.all", msg.Exchange, msg.RoutingKey)
	}
}

func TestAMQPBatchesAndPropagatesFailures(t *testing.T) {
	t.Parallel()

	publisher := &fakeAMQPPublisher{}
	a, err := sink.NewAMQP(sink.AMQPConfig{Queue: "q"}, publisher)
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}
	if err := a.EmitBatch([]stream.Record{eventRecord(), eventRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if got := len(publisher.published()); got != 2 {
		t.Errorf("published %d messages, want 2", got)
	}

	publisher.err = errors.New("nack")
	if err := a.Emit(eventRecord()); err == nil {
		t.Fatal("Emit() error = nil, want the publisher failure")
	} else if !strings.Contains(err.Error(), "nack") {
		t.Errorf("error = %q, want it to wrap the publisher failure", err)
	}
}

func TestAMQPRejectsBadConfig(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewAMQP(sink.AMQPConfig{Queue: "q"}, nil); err == nil {
		t.Error("NewAMQP(nil publisher) error = nil, want an error")
	}
	if _, err := sink.NewAMQP(sink.AMQPConfig{}, &fakeAMQPPublisher{}); err == nil {
		t.Error("NewAMQP(no queue) error = nil, want an error")
	}
	if _, err := sink.NewAMQPPublisher("amqp://localhost", sink.AMQPOptions{}); err == nil {
		t.Error("NewAMQPPublisher(no queue) error = nil, want an error")
	}
}

func TestAMQPCloseIsIdempotentAtSink(t *testing.T) {
	t.Parallel()

	publisher := &fakeAMQPPublisher{}
	a, err := sink.NewAMQP(sink.AMQPConfig{Queue: "q"}, publisher)
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if publisher.closed != 1 {
		t.Errorf("publisher closed %d times, want 1", publisher.closed)
	}

	publisher.closeErr = errors.New("connection reset")
	if err := a.Close(); err == nil {
		t.Error("Close() error = nil, want the publisher's close failure")
	}
}

func TestAMQPRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	publisher := &fakeAMQPPublisher{}
	a, err := sink.NewAMQP(sink.AMQPConfig{Queue: "q"}, publisher)
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}
	if err := a.Emit(stream.Record{ID: "x", Kind: "nonsense"}); err == nil {
		t.Error("Emit(unknown kind) error = nil, want an error")
	}
	if len(publisher.published()) != 0 {
		t.Error("an unencodable record was published")
	}
}

// TestBrokerSinksSatisfyBatchSink pins the optional interface, because the
// batcher only calls EmitBatch when it is implemented.
func TestBrokerSinksSatisfyBatchSink(t *testing.T) {
	t.Parallel()

	var k any = &sink.Kafka{}
	var a any = &sink.AMQP{}
	if _, ok := k.(stream.BatchSink); !ok {
		t.Error("*sink.Kafka does not implement stream.BatchSink")
	}
	if _, ok := a.(stream.BatchSink); !ok {
		t.Error("*sink.AMQP does not implement stream.BatchSink")
	}
}

func TestNewKafkaProducerFailsWithoutBrokers(t *testing.T) {
	t.Parallel()

	producer := sink.NewKafkaProducer(nil)
	t.Cleanup(func() { _ = producer.Close() })

	err := producer.WriteMessages(context.Background(), sink.KafkaMessage{
		Topic: "t", Key: []byte("k"), Value: []byte("{}"),
	})
	if err == nil {
		t.Error("WriteMessages() with no brokers error = nil, want an error")
	}
}

func TestBrokerFlushIsANoOp(t *testing.T) {
	t.Parallel()

	kafka, err := sink.NewKafka(sink.KafkaConfig{Topic: "t"}, &fakeKafkaProducer{})
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	if err := kafka.Flush(); err != nil {
		t.Errorf("Kafka.Flush() error = %v, want nil because writes are synchronous", err)
	}

	amqpSink, err := sink.NewAMQP(sink.AMQPConfig{Queue: "q"}, &fakeAMQPPublisher{})
	if err != nil {
		t.Fatalf("NewAMQP() error = %v", err)
	}
	if err := amqpSink.Flush(); err != nil {
		t.Errorf("AMQP.Flush() error = %v, want nil because writes are synchronous", err)
	}
}

func TestEncodeRecordRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	if _, err := sink.EncodeRecord(stream.Record{ID: "x", Kind: "nonsense"}, true); err == nil {
		t.Error("EncodeRecord(unknown kind) error = nil, want an error")
	}
}
