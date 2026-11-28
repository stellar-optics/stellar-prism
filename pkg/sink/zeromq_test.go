package sink_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestZeroMQSinkRendersTopicAndPropagatesPublishFailure(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewZeroMQSink(sink.ZeroMQOptions{
		Topic:     "stellar.events.{{.ContractID}}.{{.Topic0}}",
		Publisher: p,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := stream.Record{ID: "r", Kind: stream.KindEvent, Event: &stream.Event{ContractID: "CDL", TopicXDR: []string{"trade"}}}
	if err := s.Emit(r); err != nil {
		t.Fatal(err)
	}
	if p.subject != "stellar.events.CDL.trade" || len(p.data) == 0 {
		t.Errorf("publish = %q %q", p.subject, p.data)
	}
	p.err = errors.New("offline")
	if err := s.Emit(r); err == nil {
		t.Fatal("Emit error = nil")
	}
}

func TestZeroMQSinkRejectsEmptyRenderedTopic(t *testing.T) {
	p := &testPublisher{}
	s, err := sink.NewZeroMQSink(sink.ZeroMQOptions{Topic: "{{.ContractID}}", Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(stream.Record{ID: "r", Kind: stream.KindEvent}); err == nil {
		t.Fatal("Emit error = nil")
	}
}

func TestZeroMQSinkBindsPUBAndDeliversOverIPC(t *testing.T) {
	const contractID = "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3"
	endpoint := "ipc://" + filepath.Join(t.TempDir(), "prism.ipc")

	pub, err := sink.NewZeroMQSink(sink.ZeroMQOptions{Endpoint: endpoint, Topic: "{{.ContractID}}"})
	if err != nil {
		t.Fatalf("NewZeroMQSink() error = %v", err)
	}
	defer func() { _ = pub.Close() }()

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := zmq4.NewSub(subCtx)
	defer func() { _ = sub.Close() }()
	if err := sub.SetOption(zmq4.OptionSubscribe, contractID); err != nil {
		t.Fatalf("SetOption(SUBSCRIBE) error = %v", err)
	}
	if err := sub.Dial(endpoint); err != nil {
		t.Fatalf("Dial(%q) error = %v", endpoint, err)
	}

	record := stream.Record{
		ID:   "0000000001",
		Kind: stream.KindEvent,
		Time: time.Unix(1700000000, 0).UTC(),
		Event: &stream.Event{
			Type:       "contract",
			ContractID: contractID,
		},
	}
	received := make(chan zmq4.Msg, 1)
	failed := make(chan error, 1)
	go func() {
		msg, err := sub.Recv()
		if err != nil {
			failed <- err
			return
		}
		received <- msg
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := pub.Emit(record); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
		select {
		case msg := <-received:
			if len(msg.Frames) != 2 {
				t.Fatalf("frames = %d, want 2", len(msg.Frames))
			}
			if got := string(msg.Frames[0]); got != contractID {
				t.Errorf("topic frame = %q, want %q", got, contractID)
			}
			if body := string(msg.Frames[1]); !strings.Contains(body, `"contract":"`+contractID+`"`) {
				t.Errorf("payload frame = %q", body)
			}
			return
		case err := <-failed:
			t.Fatalf("Recv() error = %v", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for PUB/SUB delivery over IPC")
}
