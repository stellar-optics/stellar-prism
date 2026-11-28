package sink

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type fakeSQS struct {
	calls  []*sqs.SendMessageBatchInput
	err    error
	failed []sqstypes.BatchResultErrorEntry
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.SendMessageBatchOutput{Failed: f.failed}, nil
}

type fakeSNS struct {
	publishes []*sns.PublishInput
	batches   []*sns.PublishBatchInput
	err       error
	failed    []snstypes.BatchResultErrorEntry
}

func (f *fakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.publishes = append(f.publishes, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sns.PublishOutput{}, nil
}

func (f *fakeSNS) PublishBatch(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	f.batches = append(f.batches, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sns.PublishBatchOutput{Failed: f.failed}, nil
}

func awsTestRecords(n int) []stream.Record {
	rows := make([]stream.Record, n)
	for i := range rows {
		rows[i] = stream.Record{
			ID:   fmt.Sprintf("000000000%d", i),
			Kind: stream.KindEvent,
			Event: &stream.Event{
				ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			},
		}
	}
	return rows
}

func TestAwsSqsSinkBatchesAndDeduplicatesFIFO(t *testing.T) {
	f := &fakeSQS{}
	s, err := NewAwsSqsSink(f, "https://x/q.fifo", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EmitBatch(awsTestRecords(11)); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || len(f.calls[0].Entries) != AWSBatchLimit || len(f.calls[1].Entries) != 1 {
		t.Fatalf("batch sizes = %d calls, want %d + 1 entries", len(f.calls), AWSBatchLimit)
	}
	for i, entry := range f.calls[0].Entries {
		if got := aws.ToString(entry.Id); got != strconv.Itoa(i) {
			t.Errorf("entry %d Id = %q, want %q", i, got, strconv.Itoa(i))
		}
		if got := aws.ToString(entry.MessageDeduplicationId); got != awsTestRecords(11)[i].ID {
			t.Errorf("entry %d dedup = %q", i, got)
		}
		if got := aws.ToString(entry.MessageGroupId); got != "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3" {
			t.Errorf("entry %d group = %q", i, got)
		}
		if !strings.Contains(aws.ToString(entry.MessageBody), `"kind":"event"`) {
			t.Errorf("entry %d body = %q", i, aws.ToString(entry.MessageBody))
		}
	}
}

func TestAwsSqsSinkNonFIFOAndErrors(t *testing.T) {
	f := &fakeSQS{}
	s, err := NewSQSSink(f, "https://x/q", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(awsTestRecords(1)[0]); err != nil {
		t.Fatal(err)
	}
	entry := f.calls[0].Entries[0]
	if entry.MessageGroupId != nil || entry.MessageDeduplicationId != nil {
		t.Fatal("non-FIFO entry carried FIFO fields")
	}

	f.err = errors.New("offline")
	if err := s.Emit(awsTestRecords(1)[0]); err == nil || !strings.Contains(err.Error(), "offline") {
		t.Fatalf("Emit error = %v", err)
	}
	f.err = nil
	f.failed = []sqstypes.BatchResultErrorEntry{{
		Id:      aws.String("0"),
		Code:    aws.String("InternalError"),
		Message: aws.String("try again"),
	}}
	if err := s.Emit(awsTestRecords(1)[0]); err == nil || !strings.Contains(err.Error(), "InternalError") {
		t.Fatalf("Emit error = %v", err)
	}
}

func TestAwsSnsSinkPublishesAndBatchesFIFO(t *testing.T) {
	f := &fakeSNS{}
	s, err := NewAwsSnsSink(f, "arn:aws:sns:us-east-1:1:events.fifo", true, false)
	if err != nil {
		t.Fatal(err)
	}
	first := awsTestRecords(1)[0]
	if err := s.Emit(first); err != nil {
		t.Fatal(err)
	}
	if len(f.publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(f.publishes))
	}
	if got := aws.ToString(f.publishes[0].MessageDeduplicationId); got != first.ID {
		t.Errorf("Publish dedup = %q", got)
	}
	if got := aws.ToString(f.publishes[0].MessageGroupId); got != first.Event.ContractID {
		t.Errorf("Publish group = %q", got)
	}

	if err := s.EmitBatch(awsTestRecords(11)); err != nil {
		t.Fatal(err)
	}
	if len(f.batches) != 2 || len(f.batches[0].PublishBatchRequestEntries) != AWSBatchLimit || len(f.batches[1].PublishBatchRequestEntries) != 1 {
		t.Fatalf("batch sizes = %d calls, want %d + 1 entries", len(f.batches), AWSBatchLimit)
	}
	entry := f.batches[0].PublishBatchRequestEntries[0]
	if entry.MessageGroupId == nil || entry.MessageDeduplicationId == nil {
		t.Fatal("FIFO batch entry is missing FIFO fields")
	}
	if !strings.Contains(aws.ToString(entry.Message), `"kind":"event"`) {
		t.Errorf("batch message = %q", aws.ToString(entry.Message))
	}
}

func TestAwsSnsSinkNonFIFOAndErrors(t *testing.T) {
	f := &fakeSNS{}
	s, err := NewSNSSink(f, "arn:aws:sns:us-east-1:1:events", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EmitBatch(awsTestRecords(1)); err != nil {
		t.Fatal(err)
	}
	entry := f.batches[0].PublishBatchRequestEntries[0]
	if entry.MessageGroupId != nil || entry.MessageDeduplicationId != nil {
		t.Fatal("non-FIFO entry carried FIFO fields")
	}

	f.err = errors.New("offline")
	if err := s.EmitBatch(awsTestRecords(1)); err == nil || !strings.Contains(err.Error(), "offline") {
		t.Fatalf("EmitBatch error = %v", err)
	}
	f.err = nil
	f.failed = []snstypes.BatchResultErrorEntry{{
		Id:      aws.String("0"),
		Code:    aws.String("InvalidParameter"),
		Message: aws.String("bad message"),
	}}
	if err := s.EmitBatch(awsTestRecords(1)); err == nil || !strings.Contains(err.Error(), "InvalidParameter") {
		t.Fatalf("EmitBatch error = %v", err)
	}
}

func TestAwsSinkValidation(t *testing.T) {
	if _, err := NewAwsSqsSink(nil, "q", false, false); err == nil {
		t.Fatal("NewAwsSqsSink(nil) error = nil")
	}
	if _, err := NewAwsSnsSink(&fakeSNS{}, "", false, false); err == nil {
		t.Fatal("NewAwsSnsSink(empty topic) error = nil")
	}
}
