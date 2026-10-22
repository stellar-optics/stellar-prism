package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// AMQPMessage is one message handed to an AMQPPublisher.
type AMQPMessage struct {
	Exchange   string
	RoutingKey string
	// MessageID is the record ID, so a consumer can de-duplicate a record
	// redelivered after a restart.
	MessageID string
	// Kind is the record kind, carried as the AMQP type property.
	Kind      string
	Timestamp time.Time
	Body      []byte
}

// AMQPPublisher is the broker seam: it publishes messages and reports
// failures that must stop the stream.
//
// It is an interface so the sink and every test can run without a broker.
// NewAMQPPublisher is the production implementation, which also declares the
// durable queue and waits for publisher confirms.
type AMQPPublisher interface {
	// Publish delivers one message and returns only once the broker has
	// acknowledged it.
	Publish(ctx context.Context, msg AMQPMessage) error
	Close() error
}

// AMQPConfig configures an AMQP sink.
type AMQPConfig struct {
	// Queue is the durable queue records are published to. Required.
	Queue string
	// Exchange is the destination exchange; empty means the default exchange,
	// which routes by queue name.
	Exchange string
	// RoutingKey overrides the key used to route to the queue. Empty means
	// Queue.
	RoutingKey string
	// Decode enables lens decoding of XDR payloads. When false the base64
	// XDR is published as-is, matching --raw on stdout.
	Decode bool
	// Timeout bounds a single publish. Zero means 10 seconds, so a wedged
	// broker fails the stream instead of hanging it.
	Timeout time.Duration
}

// AMQP publishes stream records to a RabbitMQ queue as persistent JSON
// messages.
//
// The body is exactly the JSON document `prism --json` would print, so a
// consumer can reuse expressions written against the stdout format. Messages
// are published directly to a queue through the default exchange unless an
// exchange is configured.
//
// AMQP is a stream.Sink and a stream.BatchSink. Emit publishes one message
// synchronously and waits for the broker's acknowledgement, so a refused
// message is a stream error rather than silent loss.
type AMQP struct {
	publisher  AMQPPublisher
	exchange   string
	routingKey string
	decode     bool
	timeout    time.Duration
}

// AMQP never buffers, so it satisfies both sink interfaces.
var (
	_ stream.Sink      = (*AMQP)(nil)
	_ stream.BatchSink = (*AMQP)(nil)
)

// NewAMQP returns a sink publishing to cfg.Queue through publisher.
func NewAMQP(cfg AMQPConfig, publisher AMQPPublisher) (*AMQP, error) {
	if publisher == nil {
		return nil, fmt.Errorf("amqp: a publisher is required")
	}
	if strings.TrimSpace(cfg.Queue) == "" {
		return nil, fmt.Errorf("amqp: a queue is required")
	}
	routingKey := cfg.RoutingKey
	if routingKey == "" {
		routingKey = cfg.Queue
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &AMQP{
		publisher:  publisher,
		exchange:   cfg.Exchange,
		routingKey: routingKey,
		decode:     cfg.Decode,
		timeout:    timeout,
	}, nil
}

// Emit implements stream.Sink.
func (a *AMQP) Emit(r stream.Record) error {
	return a.EmitBatch([]stream.Record{r})
}

// EmitBatch implements stream.BatchSink, publishing each record with its own
// acknowledgement. AMQP has no atomic multi-message publish, so a batch is
// only as durable as the confirmations it has already received.
func (a *AMQP) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := a.publish(r); err != nil {
			return err
		}
	}
	return nil
}

func (a *AMQP) publish(r stream.Record) error {
	body, err := EncodeRecord(r, a.decode)
	if err != nil {
		return fmt.Errorf("amqp: encoding record %s: %w", r.ID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	msg := AMQPMessage{
		Exchange:   a.exchange,
		RoutingKey: a.routingKey,
		MessageID:  r.ID,
		Kind:       string(r.Kind),
		Timestamp:  r.Time,
		Body:       body,
	}
	if err := a.publisher.Publish(ctx, msg); err != nil {
		return fmt.Errorf("amqp: publishing record %s to %q: %w", r.ID, a.routingKey, err)
	}
	return nil
}

// Flush implements stream.Sink. Emit publishes synchronously, so there is
// nothing buffered to release.
func (a *AMQP) Flush() error { return nil }

// Close releases the publisher and its connection. It is called once when the
// stream ends, including on cancellation, so no connection or channel
// outlives the process.
func (a *AMQP) Close() error {
	if err := a.publisher.Close(); err != nil {
		return fmt.Errorf("amqp: closing publisher: %w", err)
	}
	return nil
}

// AMQPOptions configures the broker connection.
type AMQPOptions struct {
	// Queue is declared durable, so it survives a broker restart. Required.
	Queue string
}

// amqpChannel is the subset of *amqp.Channel the publisher uses. Keeping it
// an interface lets the publisher's acknowledgement logic be tested without a
// broker, which is where the delivery guarantee actually lives.
type amqpChannel interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// amqpConnection is the subset of *amqp.Connection the publisher uses.
type amqpConnection interface {
	Channel() (amqpChannel, error)
	Close() error
}

// dialAMQP is the production dialer. Tests pass a fake to newAMQPPublisher
// instead of substituting this, so nothing global is mutated.
func dialAMQP(url string) (amqpConnection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpConn{conn}, nil
}

// amqpConn adapts *amqp.Connection to amqpConnection, narrowing Channel's
// return type to the interface the publisher needs.
type amqpConn struct {
	*amqp.Connection
}

func (c amqpConn) Channel() (amqpChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// NewAMQPPublisher dials the broker, declares a durable queue and enables
// publisher confirms.
//
// Declaration is idempotent as long as the existing queue has the same
// durable, non-exclusive, non-auto-delete arguments; a mismatch is reported
// rather than silently adapted, because changing those flags would drop the
// messages already queued.
func NewAMQPPublisher(url string, opts AMQPOptions) (AMQPPublisher, error) {
	return newAMQPPublisher(url, opts, dialAMQP)
}

func newAMQPPublisher(url string, opts AMQPOptions, dial func(string) (amqpConnection, error)) (AMQPPublisher, error) {
	if strings.TrimSpace(opts.Queue) == "" {
		return nil, fmt.Errorf("amqp: a queue is required")
	}

	conn, err := dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp: dialling broker: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			return nil, errors.Join(fmt.Errorf("amqp: opening channel: %w", err), closeErr)
		}
		return nil, fmt.Errorf("amqp: opening channel: %w", err)
	}

	if _, err := ch.QueueDeclare(
		opts.Queue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	); err != nil {
		return nil, errors.Join(
			fmt.Errorf("amqp: declaring durable queue %q: %w", opts.Queue, err),
			ch.Close(),
			conn.Close(),
		)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, errors.Join(
			fmt.Errorf("amqp: enabling publisher confirms: %w", err),
			ch.Close(),
			conn.Close(),
		)
	}

	return &amqpPublisher{
		conn:     conn,
		ch:       ch,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

type amqpPublisher struct {
	mu       sync.Mutex
	conn     amqpConnection
	ch       amqpChannel
	confirms <-chan amqp.Confirmation
	closed   bool
}

// Publish implements AMQPPublisher.
//
// The mutex serialises publishes so each confirmation unambiguously belongs
// to the message being waited on. The tailer emits from one goroutine, so
// this costs nothing and keeps the acknowledgement check exact.
func (p *amqpPublisher) Publish(ctx context.Context, msg AMQPMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("amqp: publish after close")
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.MessageID,
		Type:         msg.Kind,
		Body:         msg.Body,
	}
	if !msg.Timestamp.IsZero() {
		publishing.Timestamp = msg.Timestamp
	}

	if err := p.ch.PublishWithContext(ctx, msg.Exchange, msg.RoutingKey,
		false, // mandatory
		false, // immediate
		publishing,
	); err != nil {
		return fmt.Errorf("amqp: writing message %s: %w", msg.MessageID, err)
	}

	select {
	case confirmation, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("amqp: confirmation channel closed before message %s was acknowledged", msg.MessageID)
		}
		if !confirmation.Ack {
			return fmt.Errorf("amqp: broker rejected message %s", msg.MessageID)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("amqp: waiting for acknowledgement of message %s: %w", msg.MessageID, ctx.Err())
	}
}

// Close implements AMQPPublisher. It is safe to call more than once.
func (p *amqpPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	return errors.Join(p.ch.Close(), p.conn.Close())
}
