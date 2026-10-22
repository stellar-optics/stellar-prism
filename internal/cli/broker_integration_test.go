package cli

// Integration tests against real brokers. They are opt-in so the suite stays
// offline and deterministic:
//
//	PRISM_KAFKA_BROKERS=localhost:9092 PRISM_KAFKA_TOPIC=prism-test \
//	  go test ./internal/cli -run TestKafkaIntegration -v
//
//	PRISM_AMQP_URL=amqp://guest:guest@localhost:5672/ PRISM_AMQP_QUEUE=prism-test \
//	  go test ./internal/cli -run TestAMQPIntegration -v
//
// The Kafka test needs the topic to exist (the sink never auto-creates one).
// The AMQP test declares the queue itself, because that is what the sink does.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func TestKafkaIntegrationPublishesOneEvent(t *testing.T) {
	brokers := os.Getenv("PRISM_KAFKA_BROKERS")
	topic := os.Getenv("PRISM_KAFKA_TOPIC")
	if brokers == "" || topic == "" {
		t.Skip("set PRISM_KAFKA_BROKERS and PRISM_KAFKA_TOPIC to run against a real Kafka broker")
	}

	srv := fakerpc.NewServer(fakerpc.MakeEvents(9000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	if _, _, err := run(t, "events", "--from", "9000", "--limit", "1",
		"--kafka-brokers", brokers, "--kafka-topic", topic); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   strings.Split(brokers, ","),
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	t.Cleanup(func() { _ = reader.Close() })

	for ctx.Err() == nil {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading the published message: %v", err)
		}
		var rec struct {
			Kind     string `json:"kind"`
			Contract string `json:"contract"`
		}
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			t.Fatalf("published body is not JSON: %v\n%s", err, msg.Value)
		}
		if rec.Kind != "event" || rec.Contract == "" {
			continue
		}
		// The contract ID is the default partition key.
		if string(msg.Key) != rec.Contract {
			t.Errorf("message key = %q, want the contract ID %q", msg.Key, rec.Contract)
		}
		return
	}
	t.Fatal("the published event never appeared on the topic")
}

func TestAMQPIntegrationPublishesDurableMessage(t *testing.T) {
	url := os.Getenv("PRISM_AMQP_URL")
	queue := os.Getenv("PRISM_AMQP_QUEUE")
	if url == "" || queue == "" {
		t.Skip("set PRISM_AMQP_URL and PRISM_AMQP_QUEUE to run against a real AMQP broker")
	}

	srv := fakerpc.NewServer(fakerpc.MakeEvents(9100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	if _, _, err := run(t, "events", "--from", "9100", "--limit", "1",
		"--amqp-url", url, "--amqp-queue", queue); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dialling the broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	// A passive declaration only succeeds when the existing queue matches
	// the durability flags, so this proves the sink declared it durable.
	if _, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("queue %q was not declared durable: %v", queue, err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		delivery, ok, err := ch.Get(queue, false)
		if err != nil {
			t.Fatalf("reading from the queue: %v", err)
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("acknowledging the message: %v", err)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Errorf("delivery mode = %d, want persistent (%d)", delivery.DeliveryMode, amqp.Persistent)
		}
		var rec struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
		}
		if err := json.Unmarshal(delivery.Body, &rec); err != nil {
			t.Fatalf("published body is not JSON: %v\n%s", err, delivery.Body)
		}
		if rec.Kind != "event" || rec.ID == "" {
			continue
		}
		if delivery.MessageId != rec.ID {
			t.Errorf("message id = %q, want the record ID %q", delivery.MessageId, rec.ID)
		}
		return
	}
	t.Fatal("the published event never appeared in the queue")
}
