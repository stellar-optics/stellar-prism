package sink

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// AWSBatchLimit is the maximum number of messages SNS and SQS accept in one
// API call. Both sinks split larger batches at this boundary.
const AWSBatchLimit = 10

// SQSAPI and SNSAPI keep AWS delivery testable without credentials or a network.
type SQSAPI interface {
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}
type SNSAPI interface {
	Publish(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)
	PublishBatch(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

// AwsSqsSink publishes records to an SQS queue, at most AWSBatchLimit per call.
type AwsSqsSink struct {
	client       SQSAPI
	queue        string
	fifo, decode bool
}

// SQSSink is the original name of AwsSqsSink, kept for compatibility.
type SQSSink = AwsSqsSink

// NewAwsSqsSink creates a batching SQS sink. fifo enables SQS FIFO message
// group and deduplication fields.
func NewAwsSqsSink(client SQSAPI, queue string, fifo, decode bool) (*AwsSqsSink, error) {
	if client == nil || strings.TrimSpace(queue) == "" {
		return nil, fmt.Errorf("sqs: client and queue URL are required")
	}
	return &AwsSqsSink{client: client, queue: queue, fifo: fifo, decode: decode}, nil
}

// NewSQSSink creates an SQS sink with FIFO mode inferred from the queue name.
func NewSQSSink(client SQSAPI, queue string, fifo, decode bool) (*SQSSink, error) {
	return NewAwsSqsSink(client, queue, fifo, decode)
}

// Emit implements stream.Sink by sending one message.
func (s *AwsSqsSink) Emit(r stream.Record) error { return s.EmitBatch([]stream.Record{r}) }

// EmitBatch implements stream.BatchSink, splitting records into API-sized batches.
func (s *AwsSqsSink) EmitBatch(rs []stream.Record) error {
	for len(rs) > 0 {
		n := len(rs)
		if n > AWSBatchLimit {
			n = AWSBatchLimit
		}
		entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, n)
		for i, r := range rs[:n] {
			body, err := MarshalRecord(r, s.decode)
			if err != nil {
				return fmt.Errorf("sqs: marshal record: %w", err)
			}
			entry := sqstypes.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(i)),
				MessageBody: aws.String(string(body)),
			}
			if s.fifo {
				entry.MessageDeduplicationId = aws.String(r.ID)
				entry.MessageGroupId = aws.String(awsFIFOMessageGroup(r))
			}
			entries = append(entries, entry)
		}
		out, err := s.client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(s.queue),
			Entries:  entries,
		})
		if err != nil {
			return fmt.Errorf("sqs: send batch: %w", err)
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("sqs: %d messages failed: %s", len(out.Failed), awsFailureDetail(out.Failed[0].Id, out.Failed[0].Code, out.Failed[0].Message))
		}
		rs = rs[n:]
	}
	return nil
}

// Flush implements stream.Sink. Messages are sent synchronously.
func (*AwsSqsSink) Flush() error { return nil }

// Close implements io.Closer. The SDK client is owned by the caller.
func (*AwsSqsSink) Close() error { return nil }

// AwsSnsSink publishes records to an SNS topic, at most AWSBatchLimit per call.
type AwsSnsSink struct {
	client       SNSAPI
	topic        string
	fifo, decode bool
}

// SNSSink is the original name of AwsSnsSink, kept for compatibility.
type SNSSink = AwsSnsSink

// NewAwsSnsSink creates a batching SNS sink. fifo enables SNS FIFO message
// group and deduplication fields.
func NewAwsSnsSink(client SNSAPI, topic string, fifo, decode bool) (*AwsSnsSink, error) {
	if client == nil || strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("sns: client and topic ARN are required")
	}
	return &AwsSnsSink{client: client, topic: topic, fifo: fifo, decode: decode}, nil
}

// NewSNSSink creates an SNS sink with FIFO mode inferred from the topic ARN.
func NewSNSSink(client SNSAPI, topic string, decode bool) (*SNSSink, error) {
	return NewAwsSnsSink(client, topic, strings.HasSuffix(topic, ".fifo"), decode)
}

// Emit implements stream.Sink by publishing one message immediately.
func (s *AwsSnsSink) Emit(r stream.Record) error {
	body, err := MarshalRecord(r, s.decode)
	if err != nil {
		return fmt.Errorf("sns: marshal record: %w", err)
	}
	input := &sns.PublishInput{TopicArn: aws.String(s.topic), Message: aws.String(string(body))}
	if s.fifo {
		input.MessageDeduplicationId = aws.String(r.ID)
		input.MessageGroupId = aws.String(awsFIFOMessageGroup(r))
	}
	if _, err := s.client.Publish(context.Background(), input); err != nil {
		return fmt.Errorf("sns: publish: %w", err)
	}
	return nil
}

// EmitBatch implements stream.BatchSink, splitting records into API-sized batches.
func (s *AwsSnsSink) EmitBatch(rs []stream.Record) error {
	for len(rs) > 0 {
		n := len(rs)
		if n > AWSBatchLimit {
			n = AWSBatchLimit
		}
		entries := make([]snstypes.PublishBatchRequestEntry, 0, n)
		for i, r := range rs[:n] {
			body, err := MarshalRecord(r, s.decode)
			if err != nil {
				return fmt.Errorf("sns: marshal record: %w", err)
			}
			entry := snstypes.PublishBatchRequestEntry{
				Id:      aws.String(strconv.Itoa(i)),
				Message: aws.String(string(body)),
			}
			if s.fifo {
				entry.MessageDeduplicationId = aws.String(r.ID)
				entry.MessageGroupId = aws.String(awsFIFOMessageGroup(r))
			}
			entries = append(entries, entry)
		}
		out, err := s.client.PublishBatch(context.Background(), &sns.PublishBatchInput{
			TopicArn:                   aws.String(s.topic),
			PublishBatchRequestEntries: entries,
		})
		if err != nil {
			return fmt.Errorf("sns: publish batch: %w", err)
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("sns: %d messages failed: %s", len(out.Failed), awsFailureDetail(out.Failed[0].Id, out.Failed[0].Code, out.Failed[0].Message))
		}
		rs = rs[n:]
	}
	return nil
}

// Flush implements stream.Sink. Messages are sent synchronously.
func (*AwsSnsSink) Flush() error { return nil }

// Close implements io.Closer. The SDK client is owned by the caller.
func (*AwsSnsSink) Close() error { return nil }

// Each contract keeps its own FIFO order; other record kinds use their kind.
func awsFIFOMessageGroup(r stream.Record) string {
	if r.Event != nil && r.Event.ContractID != "" {
		return r.Event.ContractID
	}
	return string(r.Kind)
}

func awsFailureDetail(id, code, message *string) string {
	return fmt.Sprintf("entry %s: %s: %s", aws.ToString(id), aws.ToString(code), aws.ToString(message))
}

var (
	_ stream.Sink      = (*AwsSqsSink)(nil)
	_ stream.BatchSink = (*AwsSqsSink)(nil)
	_ stream.Sink      = (*AwsSnsSink)(nil)
	_ stream.BatchSink = (*AwsSnsSink)(nil)
)
