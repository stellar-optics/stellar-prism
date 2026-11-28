package sink

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/go-zeromq/zmq4"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ZeroMQPublisher is the narrow seam used by ZeroMQSink. Publish sends a
// multipart PUB message whose first frame is the topic and whose second frame
// is the JSON payload.
type ZeroMQPublisher interface {
	Publish(topic string, data []byte) error
	Close() error
}

// ZeroMQOptions configures a ZeroMQ PUB sink. Topic is a template rendered
// from .ID, .Ledger, .ContractID and .Topic0; it defaults to the contract ID
// in the CLI so subscribers can filter on the first multipart frame.
type ZeroMQOptions struct {
	Endpoint  string
	Topic     string
	Decode    bool
	Publisher ZeroMQPublisher
}

// ZeroMQSink publishes records through a ZeroMQ PUB socket. ZeroMQ PUB is a
// lossy fan-out transport by design; callers needing acknowledged delivery
// should use the NATS JetStream sink instead.
type ZeroMQSink struct {
	publisher ZeroMQPublisher
	topic     *template.Template
	decode    bool
}

func NewZeroMQSink(opts ZeroMQOptions) (*ZeroMQSink, error) {
	if strings.TrimSpace(opts.Topic) == "" {
		return nil, fmt.Errorf("zeromq: topic is required")
	}
	t, err := template.New("topic").Option("missingkey=error").Parse(opts.Topic)
	if err != nil {
		return nil, fmt.Errorf("zeromq: invalid topic template: %w", err)
	}
	p := opts.Publisher
	if p == nil {
		if strings.TrimSpace(opts.Endpoint) == "" {
			return nil, fmt.Errorf("zeromq: endpoint is required")
		}
		pub, err := newZeroMQPublisher(opts.Endpoint)
		if err != nil {
			return nil, err
		}
		p = pub
	}
	return &ZeroMQSink{publisher: p, topic: t, decode: opts.Decode}, nil
}

func (s *ZeroMQSink) Emit(r stream.Record) error {
	var topic bytes.Buffer
	data := subjectTemplateData(r)
	if err := s.topic.Execute(&topic, data); err != nil {
		return fmt.Errorf("zeromq: render topic: %w", err)
	}
	subject := strings.TrimSpace(topic.String())
	if subject == "" {
		return fmt.Errorf("zeromq: rendered topic is empty")
	}
	payload, err := MarshalRecord(r, s.decode)
	if err != nil {
		return fmt.Errorf("zeromq: marshal record: %w", err)
	}
	if err := s.publisher.Publish(subject, payload); err != nil {
		return fmt.Errorf("zeromq: publish: %w", err)
	}
	return nil
}

func (s *ZeroMQSink) Flush() error { return nil }
func (s *ZeroMQSink) Close() error { return s.publisher.Close() }

var _ stream.Sink = (*ZeroMQSink)(nil)

type zeroMQPublisher struct {
	cancel context.CancelFunc
	socket zmq4.Socket
}

func newZeroMQPublisher(endpoint string) (*zeroMQPublisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	socket := zmq4.NewPub(ctx)
	if err := socket.Listen(endpoint); err != nil {
		cancel()
		_ = socket.Close()
		return nil, fmt.Errorf("zeromq: bind %s: %w", endpoint, err)
	}
	return &zeroMQPublisher{cancel: cancel, socket: socket}, nil
}

func (p *zeroMQPublisher) Publish(topic string, data []byte) error {
	return p.socket.SendMulti(zmq4.NewMsgFrom([]byte(topic), data))
}

func (p *zeroMQPublisher) Close() error {
	p.cancel()
	return p.socket.Close()
}
