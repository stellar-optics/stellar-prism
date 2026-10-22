package sink

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeAMQPChannel records the declaration and publish calls the publisher
// makes, so the broker contract can be asserted without a broker.
type fakeAMQPChannel struct {
	declareErr  error
	confirmErr  error
	publishErr  error
	closeErr    error
	ack         bool
	noConfirm   bool
	declared    bool
	durable     bool
	autoDelete  bool
	exclusive   bool
	confirmed   bool
	confirms    chan amqp.Confirmation
	published   []amqp.Publishing
	exchange    string
	routingKey  string
	mandatory   bool
	immediate   bool
	closeCalled bool
}

func (c *fakeAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, _ bool, _ amqp.Table) (amqp.Queue, error) {
	c.declared = true
	c.durable = durable
	c.autoDelete = autoDelete
	c.exclusive = exclusive
	return amqp.Queue{Name: name}, c.declareErr
}

func (c *fakeAMQPChannel) Confirm(_ bool) error {
	c.confirmed = true
	return c.confirmErr
}

func (c *fakeAMQPChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	c.confirms = confirm
	return confirm
}

func (c *fakeAMQPChannel) PublishWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if c.publishErr != nil {
		return c.publishErr
	}
	c.published = append(c.published, msg)
	c.exchange = exchange
	c.routingKey = key
	c.mandatory = mandatory
	c.immediate = immediate
	if !c.noConfirm {
		c.confirms <- amqp.Confirmation{Ack: c.ack}
	}
	return nil
}

func (c *fakeAMQPChannel) Close() error {
	c.closeCalled = true
	return c.closeErr
}

type fakeAMQPConnection struct {
	ch       *fakeAMQPChannel
	chErr    error
	closeErr error
	closed   bool
}

func (c *fakeAMQPConnection) Channel() (amqpChannel, error) {
	if c.chErr != nil {
		return nil, c.chErr
	}
	return c.ch, nil
}

func (c *fakeAMQPConnection) Close() error {
	c.closed = true
	return c.closeErr
}

// fakeDialer returns a dialer that hands back conn or err, so the publisher
// can be tested without touching a network.
func fakeDialer(conn amqpConnection, err error) func(string) (amqpConnection, error) {
	return func(string) (amqpConnection, error) { return conn, err }
}

func newTestPublisher(t *testing.T, ch *fakeAMQPChannel) *amqpPublisher {
	t.Helper()
	conn := &fakeAMQPConnection{ch: ch}

	pub, err := newAMQPPublisher("amqp://broker", AMQPOptions{Queue: "soroban.events"}, fakeDialer(conn, nil))
	if err != nil {
		t.Fatalf("NewAMQPPublisher() error = %v", err)
	}
	p, ok := pub.(*amqpPublisher)
	if !ok {
		t.Fatalf("NewAMQPPublisher() returned %T, want *amqpPublisher", pub)
	}
	return p
}

func TestNewAMQPPublisherDeclaresDurableQueueAndConfirms(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{ack: true}
	p := newTestPublisher(t, ch)

	if !ch.declared || !ch.durable {
		t.Error("queue was not declared durable")
	}
	if ch.autoDelete || ch.exclusive {
		t.Errorf("queue declared autoDelete=%v exclusive=%v, want both false", ch.autoDelete, ch.exclusive)
	}
	if !ch.confirmed {
		t.Error("publisher confirms were not enabled")
	}

	if err := p.Publish(context.Background(), AMQPMessage{
		Exchange:   "stellar",
		RoutingKey: "events.all",
		MessageID:  "rec-1",
		Kind:       "event",
		Timestamp:  time.Unix(1700000100, 0).UTC(),
		Body:       []byte(`{"kind":"event"}`),
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if len(ch.published) != 1 {
		t.Fatalf("channel saw %d publishes, want 1", len(ch.published))
	}
	msg := ch.published[0]
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent (%d)", msg.DeliveryMode, amqp.Persistent)
	}
	if msg.ContentType != "application/json" {
		t.Errorf("content type = %q, want application/json", msg.ContentType)
	}
	if msg.MessageId != "rec-1" || msg.Type != "event" {
		t.Errorf("message id/type = %q/%q, want rec-1/event", msg.MessageId, msg.Type)
	}
	if !msg.Timestamp.Equal(time.Unix(1700000100, 0).UTC()) {
		t.Errorf("timestamp = %v, want the record time", msg.Timestamp)
	}
	if ch.exchange != "stellar" || ch.routingKey != "events.all" {
		t.Errorf("routed to %q/%q, want stellar/events.all", ch.exchange, ch.routingKey)
	}
	if ch.mandatory || ch.immediate {
		t.Error("publish flags changed; mandatory/immediate must both be false")
	}
}

func TestPublishRejectedByBroker(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{ack: false}
	p := newTestPublisher(t, ch)

	err := p.Publish(context.Background(), AMQPMessage{MessageID: "rec-1"})
	if err == nil {
		t.Fatal("Publish(nack) error = nil, want an error")
	}
	if !strings.Contains(err.Error(), "rejected") {
		t.Errorf("error = %q, want it to report the broker rejection", err)
	}
}

func TestPublishWithoutConfirmation(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{noConfirm: true}
	p := newTestPublisher(t, ch)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := p.Publish(ctx, AMQPMessage{MessageID: "rec-1"})
	if err == nil {
		t.Fatal("Publish(no confirmation) error = nil, want a timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

func TestPublishReportsChannelFailures(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{publishErr: errors.New("channel closed")}
	p := newTestPublisher(t, ch)

	err := p.Publish(context.Background(), AMQPMessage{MessageID: "rec-1"})
	if err == nil {
		t.Fatal("Publish(channel error) error = nil, want an error")
	}
	if !strings.Contains(err.Error(), "channel closed") {
		t.Errorf("error = %q, want it to wrap the channel failure", err)
	}
}

func TestPublishAfterClose(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{ack: true}
	p := newTestPublisher(t, ch)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Publish(context.Background(), AMQPMessage{MessageID: "rec-1"}); err == nil {
		t.Error("Publish(after close) error = nil, want an error")
	}
}

func TestPublisherCloseIsIdempotentAndReportsFailures(t *testing.T) {
	t.Parallel()

	ch := &fakeAMQPChannel{closeErr: errors.New("channel close failed")}
	p := newTestPublisher(t, ch)

	if err := p.Close(); err == nil {
		t.Fatal("Close() error = nil, want the channel's close failure")
	}
	if !ch.closeCalled {
		t.Error("channel was not closed")
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

func TestNewAMQPPublisherFailurePaths(t *testing.T) {
	t.Parallel()

	t.Run("dial failure", func(t *testing.T) {
		t.Parallel()
		dialErr := errors.New("connection refused")
		if _, err := newAMQPPublisher("amqp://broker", AMQPOptions{Queue: "q"}, fakeDialer(nil, dialErr)); err == nil {
			t.Error("NewAMQPPublisher(dial failure) error = nil, want an error")
		}
	})

	t.Run("channel failure closes the connection", func(t *testing.T) {
		t.Parallel()
		conn := &fakeAMQPConnection{chErr: errors.New("channel error")}
		if _, err := newAMQPPublisher("amqp://broker", AMQPOptions{Queue: "q"}, fakeDialer(conn, nil)); err == nil {
			t.Error("NewAMQPPublisher(channel failure) error = nil, want an error")
		}
		if !conn.closed {
			t.Error("connection was not closed after a channel failure")
		}
	})

	t.Run("queue declaration failure closes everything", func(t *testing.T) {
		t.Parallel()
		ch := &fakeAMQPChannel{declareErr: errors.New("queue mismatch")}
		conn := &fakeAMQPConnection{ch: ch}
		if _, err := newAMQPPublisher("amqp://broker", AMQPOptions{Queue: "q"}, fakeDialer(conn, nil)); err == nil {
			t.Error("NewAMQPPublisher(declare failure) error = nil, want an error")
		}
		if !ch.closeCalled || !conn.closed {
			t.Error("channel and connection must both be closed after a declaration failure")
		}
	})

	t.Run("confirm failure closes everything", func(t *testing.T) {
		t.Parallel()
		ch := &fakeAMQPChannel{confirmErr: errors.New("confirm unsupported")}
		conn := &fakeAMQPConnection{ch: ch}
		if _, err := newAMQPPublisher("amqp://broker", AMQPOptions{Queue: "q"}, fakeDialer(conn, nil)); err == nil {
			t.Error("NewAMQPPublisher(confirm failure) error = nil, want an error")
		}
		if !ch.closeCalled || !conn.closed {
			t.Error("channel and connection must both be closed after a confirm failure")
		}
	})

	t.Run("empty queue", func(t *testing.T) {
		t.Parallel()
		if _, err := newAMQPPublisher("amqp://broker", AMQPOptions{}, fakeDialer(nil, nil)); err == nil {
			t.Error("NewAMQPPublisher(no queue) error = nil, want an error")
		}
	})
}
