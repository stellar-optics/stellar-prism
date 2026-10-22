package sink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// KafkaPartition selects which field of a record becomes the Kafka message
// key. The key decides the partition, so every record sharing a key lands on
// the same partition and a consumer sees that contract's or transaction's
// history in order.
type KafkaPartition string

const (
	// PartitionContract keys events by contract ID, so all activity for one
	// contract is ordered on one partition. Records without a contract fall
	// back to their transaction hash, then to their record ID.
	PartitionContract KafkaPartition = "contract"
	// PartitionTx keys every record by transaction hash, keeping all events
	// and the transaction that emitted them together.
	PartitionTx KafkaPartition = "tx"
	// PartitionRecord keys by the record's own ID, spreading records as
	// evenly as the hash balancer allows.
	PartitionRecord KafkaPartition = "record"
)

// ParseKafkaPartition validates a --kafka-partition-by value.
func ParseKafkaPartition(s string) (KafkaPartition, error) {
	switch p := KafkaPartition(strings.ToLower(strings.TrimSpace(s))); p {
	case PartitionContract, PartitionTx, PartitionRecord:
		return p, nil
	case "":
		return PartitionContract, nil
	default:
		return "", fmt.Errorf(
			"kafka: unknown partition strategy %q; use %q, %q or %q",
			s, PartitionContract, PartitionTx, PartitionRecord)
	}
}

// KafkaMessage is one message handed to a KafkaProducer.
type KafkaMessage struct {
	Topic string
	// Key selects the partition. A nil key lets the balancer choose.
	Key   []byte
	Value []byte
}

// KafkaProducer is the broker seam: Kafka writes messages and reports
// failures that must stop the stream.
//
// It is an interface so the sink and every test can run without a broker, the
// same way stream.Source keeps the tailer offline. NewKafkaProducer is the
// production implementation.
type KafkaProducer interface {
	// WriteMessages publishes every message or returns an error. A nil error
	// means the broker acknowledged the whole batch (RequiredAcks=all).
	WriteMessages(ctx context.Context, msgs ...KafkaMessage) error
	Close() error
}

// KafkaConfig configures a Kafka sink.
type KafkaConfig struct {
	// Topic is the destination topic. Required.
	Topic string
	// PartitionBy selects the message key. Empty means PartitionContract.
	PartitionBy KafkaPartition
	// Decode enables lens decoding of XDR payloads. When false the base64
	// XDR is published as-is, matching --raw on stdout.
	Decode bool
	// Timeout bounds a single publish. Zero means 10 seconds, so a wedged
	// broker fails the stream instead of hanging it.
	Timeout time.Duration
}

// Kafka publishes stream records to a Kafka topic as JSON messages.
//
// The message body is exactly the JSON document `prism --json` would print,
// so a consumer can reuse expressions written against the stdout format. The
// body is the same for every partition strategy; only the key changes.
//
// Kafka is a stream.Sink and a stream.BatchSink. Emit publishes one message
// synchronously, which gives the tailer natural backpressure and means a
// broker failure is never silent.
type Kafka struct {
	producer    KafkaProducer
	topic       string
	partitionBy KafkaPartition
	decode      bool
	timeout     time.Duration
}

// Kafka never buffers, so it satisfies both sink interfaces.
var (
	_ stream.Sink      = (*Kafka)(nil)
	_ stream.BatchSink = (*Kafka)(nil)
)

// NewKafka returns a sink publishing to cfg.Topic through producer.
func NewKafka(cfg KafkaConfig, producer KafkaProducer) (*Kafka, error) {
	if producer == nil {
		return nil, fmt.Errorf("kafka: a producer is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("kafka: a topic is required")
	}
	partitionBy, err := ParseKafkaPartition(string(cfg.PartitionBy))
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Kafka{
		producer:    producer,
		topic:       cfg.Topic,
		partitionBy: partitionBy,
		decode:      cfg.Decode,
		timeout:     timeout,
	}, nil
}

// Emit implements stream.Sink.
func (k *Kafka) Emit(r stream.Record) error {
	return k.EmitBatch([]stream.Record{r})
}

// EmitBatch implements stream.BatchSink, publishing the whole batch in one
// producer call so a batched tail costs one round trip per flush.
func (k *Kafka) EmitBatch(records []stream.Record) error {
	if len(records) == 0 {
		return nil
	}

	msgs := make([]KafkaMessage, 0, len(records))
	for _, r := range records {
		body, err := EncodeRecord(r, k.decode)
		if err != nil {
			return fmt.Errorf("kafka: encoding record %s: %w", r.ID, err)
		}
		msgs = append(msgs, KafkaMessage{
			Topic: k.topic,
			Key:   kafkaKey(r, k.partitionBy),
			Value: body,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), k.timeout)
	defer cancel()
	if err := k.producer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka: publishing %d records to %q: %w", len(msgs), k.topic, err)
	}
	return nil
}

// Flush implements stream.Sink. Emit writes synchronously, so there is
// nothing buffered to release.
func (k *Kafka) Flush() error { return nil }

// Close releases the producer. It is called once when the stream ends,
// including on cancellation, so no connection outlives the process.
func (k *Kafka) Close() error {
	if err := k.producer.Close(); err != nil {
		return fmt.Errorf("kafka: closing producer: %w", err)
	}
	return nil
}

// kafkaKey returns the message key for a record under the chosen strategy.
//
// The key is never empty: an empty key would round-robin and break the
// per-contract ordering the strategy exists to provide, so every branch falls
// back to the record ID, which is unique and stable.
func kafkaKey(r stream.Record, by KafkaPartition) []byte {
	switch by {
	case PartitionRecord:
		return []byte(r.ID)
	case PartitionTx:
		if key := txHash(r); key != "" {
			return []byte(key)
		}
	default: // PartitionContract
		if r.Event != nil && r.Event.ContractID != "" {
			return []byte(r.Event.ContractID)
		}
		if key := txHash(r); key != "" {
			return []byte(key)
		}
	}
	return []byte(r.ID)
}

// txHash returns the transaction hash a record belongs to, whichever kind it
// is, or "" when the source did not report one.
func txHash(r stream.Record) string {
	switch {
	case r.Event != nil:
		return r.Event.TxHash
	case r.Transaction != nil:
		return r.Transaction.Hash
	case r.Close != nil:
		return r.Close.Hash
	default:
		return ""
	}
}

// NewKafkaProducer returns the production KafkaProducer: a synchronous
// kafka-go Writer using the hash balancer, so equal keys map to the same
// partition, and RequiredAcks=all, so a publish is only successful once every
// in-sync replica has it.
//
// The writer connects lazily, so constructing a sink makes no network call.
// Records are written only when the stream produces them.
func NewKafkaProducer(brokers []string) KafkaProducer {
	return &kafkaProducer{w: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		// A tail publishes one record at a time, so keep the batching window
		// short: latency matters more than squeezing messages together.
		BatchTimeout: 10 * time.Millisecond,
	}}
}

type kafkaProducer struct {
	w *kafka.Writer
}

// WriteMessages implements KafkaProducer.
func (p *kafkaProducer) WriteMessages(ctx context.Context, msgs ...KafkaMessage) error {
	batch := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		batch[i] = kafka.Message{Topic: m.Topic, Key: m.Key, Value: m.Value}
	}
	return p.w.WriteMessages(ctx, batch...)
}

// Close implements KafkaProducer.
func (p *kafkaProducer) Close() error { return p.w.Close() }
