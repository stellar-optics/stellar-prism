package sink

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/nats-io/nats.go"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// NATSPublisher is kept small so the delivery and subject rules can be tested
// without a broker. Implementations return only after a broker acknowledgement
// when JetStream is enabled.
type NATSPublisher interface {
	Publish(subject string, data []byte) error
	Close() error
}

// NATSSink supports publishers that can attach the JetStream Nats-Msg-Id
// header for deduplication; this comment documents the capability used by
// publishNATS, which checks each publisher type at runtime.

type NATSOptions struct {
	URL       string
	Subject   string
	JetStream bool
	// Decode controls whether XDR fields use the regular --json representation.
	Decode bool
	// Publisher is an optional test/broker abstraction. It may implement
	// Publish(subject, data), PublishWithMsgID(...), PublishMsg(*nats.Msg)
	// or Publish with a header argument.
	Publisher any
}

type NATSSink struct {
	publisher any
	subject   *template.Template
	decode    bool
}

func NewNATSSink(opts NATSOptions) (*NATSSink, error) {
	if strings.TrimSpace(opts.Subject) == "" {
		opts.Subject = "stellar.events.{{.ContractID}}.{{.Topic0}}"
	}
	t, err := template.New("subject").Option("missingkey=error").Parse(opts.Subject)
	if err != nil {
		return nil, fmt.Errorf("nats: invalid subject template: %w", err)
	}
	p := opts.Publisher
	if p == nil {
		if strings.TrimSpace(opts.URL) == "" {
			return nil, fmt.Errorf("nats: URL is required")
		}
		nc, err := nats.Connect(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("nats: connect: %w", err)
		}
		if opts.JetStream {
			js, err := nc.JetStream()
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("nats: JetStream: %w", err)
			}
			p = jetStreamPublisher{nc: nc, js: js}
		} else {
			p = coreNATSPublisher{nc: nc}
		}
	}
	return &NATSSink{publisher: p, subject: t, decode: opts.Decode}, nil
}

func (s *NATSSink) Emit(r stream.Record) error {
	var subject bytes.Buffer
	data := subjectTemplateData(r)
	if err := s.subject.Execute(&subject, data); err != nil {
		return fmt.Errorf("nats: render subject: %w", err)
	}
	if err := validateNATSSubject(subject.String()); err != nil {
		return err
	}
	payload, err := MarshalRecord(r, s.decode)
	if err != nil {
		return fmt.Errorf("nats: marshal record: %w", err)
	}
	msgID := r.ID
	if msgID == "" {
		msgID = fmt.Sprintf("%d", r.Sequence)
	}
	if err := publishNATS(s.publisher, subject.String(), payload, msgID); err != nil {
		return fmt.Errorf("nats: publish: %w", err)
	}
	return nil
}

func publishNATS(p any, subject string, data []byte, msgID string) error {
	switch v := p.(type) {
	case interface {
		PublishWithMsgID(string, []byte, string) error
	}:
		return v.PublishWithMsgID(subject, data, msgID)
	case interface {
		Publish(string, []byte, nats.Header) error
	}:
		header := nats.Header{}
		header.Set("Nats-Msg-Id", msgID)
		return v.Publish(subject, data, header)
	case interface {
		Publish(string, []byte, map[string]string) error
	}:
		return v.Publish(subject, data, map[string]string{"Nats-Msg-Id": msgID})
	case interface{ PublishMsg(*nats.Msg) error }:
		msg := &nats.Msg{Subject: subject, Data: data, Header: nats.Header{}}
		msg.Header.Set("Nats-Msg-Id", msgID)
		return v.PublishMsg(msg)
	case interface{ Publish(string, []byte) error }:
		return v.Publish(subject, data)
	default:
		return fmt.Errorf("nats: publisher %T does not implement Publish", p)
	}
}

func subjectTemplateData(r stream.Record) struct {
	ID         string
	Ledger     uint32
	ContractID string
	Topic0     string
} {
	data := struct {
		ID         string
		Ledger     uint32
		ContractID string
		Topic0     string
	}{ID: r.ID, Ledger: r.Sequence}
	if r.Event != nil {
		data.ContractID = r.Event.ContractID
		if len(r.Event.TopicXDR) > 0 {
			data.Topic0 = r.Event.TopicXDR[0]
		}
	}
	return data
}
func (s *NATSSink) Flush() error { return nil }
func (s *NATSSink) Close() error {
	if c, ok := s.publisher.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func validateNATSSubject(subject string) error {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n") {
		return fmt.Errorf("nats: rendered subject is empty or contains whitespace")
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" || token == "*" || token == ">" {
			return fmt.Errorf("nats: rendered subject %q contains an invalid publish token", subject)
		}
	}
	return nil
}

type coreNATSPublisher struct{ nc *nats.Conn }

func (p coreNATSPublisher) Publish(subject string, data []byte) error {
	if err := p.nc.Publish(subject, data); err != nil {
		return err
	}
	return p.nc.Flush()
}
func (p coreNATSPublisher) PublishWithMsgID(subject string, data []byte, _ string) error {
	return p.Publish(subject, data)
}
func (p coreNATSPublisher) Close() error { p.nc.Close(); return nil }

type jetStreamPublisher struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func (p jetStreamPublisher) Publish(subject string, data []byte) error {
	_, err := p.js.Publish(subject, data)
	return err
}

// PublishWithMsgID lets JetStream deduplicate redelivered records by their
// event ID. Nats-Msg-Id is the JetStream header for this.
func (p jetStreamPublisher) PublishWithMsgID(subject string, data []byte, msgID string) error {
	_, err := p.js.Publish(subject, data, nats.MsgId(msgID))
	return err
}
func (p jetStreamPublisher) Close() error { p.nc.Close(); return nil }

// NatsSink and NatsOptions are compatibility aliases for callers that spell
// the acronym in title case.
type NatsSink = NATSSink
type NatsOptions = NATSOptions

func NewNatsSink(opts NATSOptions) (*NATSSink, error) { return NewNATSSink(opts) }
