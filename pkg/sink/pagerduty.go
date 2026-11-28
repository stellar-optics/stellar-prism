package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

const (
	defaultPagerDutyEndpoint   = "https://events.pagerduty.com/v2/enqueue"
	defaultPagerDutyTimeout    = 10 * time.Second
	defaultPagerDutyMaxRetries = 3
	defaultPagerDutyQueueSize  = 256
)

// MapPagerDutySeverity normalizes an alert severity string into a valid PagerDuty Events API v2 severity.
func MapPagerDutySeverity(severity string) string {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "critical", "crit", "fatal":
		return "critical"
	case "error", "err":
		return "error"
	case "warn", "warning":
		return "warning"
	default:
		return "info"
	}
}

// PagerDutyOptions configures the PagerDuty alert dispatcher.
type PagerDutyOptions struct {
	RoutingKey string
	Endpoint   string
	Client     *http.Client
	Timeout    time.Duration
	MaxRetries int
	QueueSize  int
}

// PagerDutySink dispatches alert notifications to PagerDuty Events API v2.
type PagerDutySink struct {
	opts    PagerDutyOptions
	client  *http.Client
	url     string
	queue   chan AlertNotification
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closeMu sync.Mutex
	closed  bool
}

// NewPagerDutySink constructs a new PagerDuty alert dispatcher.
func NewPagerDutySink(opts PagerDutyOptions) (*PagerDutySink, error) {
	if strings.TrimSpace(opts.RoutingKey) == "" {
		return nil, errors.New("pagerduty: routing key must not be empty")
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultPagerDutyTimeout
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}

	retries := opts.MaxRetries
	if retries <= 0 {
		retries = defaultPagerDutyMaxRetries
	}

	qSize := opts.QueueSize
	if qSize <= 0 {
		qSize = defaultPagerDutyQueueSize
	}

	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = defaultPagerDutyEndpoint
	}

	opts.Timeout = timeout
	opts.MaxRetries = retries
	opts.QueueSize = qSize

	ctx, cancel := context.WithCancel(context.Background())

	s := &PagerDutySink{
		opts:   opts,
		client: client,
		url:    endpoint,
		queue:  make(chan AlertNotification, qSize),
		ctx:    ctx,
		cancel: cancel,
	}

	s.wg.Add(1)
	go s.worker()

	return s, nil
}

func (s *PagerDutySink) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case alert, ok := <-s.queue:
			if !ok {
				return
			}
			if err := s.Dispatch(context.Background(), alert); err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
			}
		}
	}
}

// PagerDutyPayload represents the Events API v2 payload.
type PagerDutyPayload struct {
	RoutingKey  string                `json:"routing_key"`
	EventAction string                `json:"event_action"`
	DedupKey    string                `json:"dedup_key,omitempty"`
	Payload     PagerDutyEventDetails `json:"payload"`
}

// PagerDutyEventDetails holds the incident details for PagerDuty.
type PagerDutyEventDetails struct {
	Summary       string                 `json:"summary"`
	Source        string                 `json:"source"`
	Severity      string                 `json:"severity"`
	Timestamp     string                 `json:"timestamp,omitempty"`
	CustomDetails map[string]interface{} `json:"custom_details,omitempty"`
}

// BuildPagerDutyPayload creates an Events API v2 payload from an AlertNotification.
func BuildPagerDutyPayload(routingKey string, alert AlertNotification) PagerDutyPayload {
	summary := alert.Message
	if summary == "" {
		summary = fmt.Sprintf("Alert [%s]: %s", strings.ToUpper(alert.Severity), alert.RuleName)
	}

	customDetails := make(map[string]interface{})
	if alert.Details != nil {
		for k, v := range alert.Details {
			customDetails[k] = v
		}
	}
	customDetails["rule_name"] = alert.RuleName
	if alert.LedgerSeq > 0 {
		customDetails["ledger_seq"] = alert.LedgerSeq
	}
	if alert.TxHash != "" {
		customDetails["tx_hash"] = alert.TxHash
	}
	if alert.ContractID != "" {
		customDetails["contract_id"] = alert.ContractID
	}

	dedupKey := ""
	if alert.TxHash != "" {
		dedupKey = fmt.Sprintf("%s-%s-%d", alert.RuleName, alert.TxHash, alert.EventIndex)
	}

	timestamp := ""
	if !alert.Timestamp.IsZero() {
		timestamp = alert.Timestamp.UTC().Format(time.RFC3339)
	}

	return PagerDutyPayload{
		RoutingKey:  routingKey,
		EventAction: "trigger",
		DedupKey:    dedupKey,
		Payload: PagerDutyEventDetails{
			Summary:       summary,
			Source:        "stellar-prism",
			Severity:      MapPagerDutySeverity(alert.Severity),
			Timestamp:     timestamp,
			CustomDetails: customDetails,
		},
	}
}

// Dispatch sends an alert synchronously to PagerDuty Events API v2 with retries.
func (s *PagerDutySink) Dispatch(ctx context.Context, alert AlertNotification) error {
	payload := BuildPagerDutyPayload(s.opts.RoutingKey, alert)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pagerduty: marshaling payload: %w", err)
	}

	maxAttempts := s.opts.MaxRetries + 1
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("pagerduty: building request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "stellar-prism")

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			respBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if readErr != nil {
				respBytes = nil
			}
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusAccepted || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
				return nil
			}

			lastErr = fmt.Errorf("pagerduty api error HTTP %d: %s", resp.StatusCode, string(respBytes))
			// Retry only on 429 or 5xx
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return lastErr
			}
		}

		if attempt < maxAttempts {
			delay := backoffDelay(100*time.Millisecond, 2*time.Second, attempt, 0.1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return fmt.Errorf("pagerduty: delivery failed after %d attempts: %w", maxAttempts, lastErr)
}

// DispatchAsync queues an alert notification for non-blocking asynchronous delivery.
func (s *PagerDutySink) DispatchAsync(alert AlertNotification) bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return false
	}

	select {
	case s.queue <- alert:
		return true
	default:
		return false
	}
}

// Emit implements the stream sink interface by extracting event alerts.
func (s *PagerDutySink) Emit(record stream.Record) error {
	alert := AlertNotification{
		RuleName:  "StreamEvent",
		Severity:  "info",
		LedgerSeq: record.Sequence,
		Timestamp: record.Time,
	}
	if record.Event != nil {
		alert.TxHash = record.Event.TxHash
		alert.ContractID = record.Event.ContractID
		alert.EventIndex = record.Event.EventIndex
		alert.Message = fmt.Sprintf("Event emitted by contract %s", record.Event.ContractID)
	} else {
		alert.Message = fmt.Sprintf("Ledger %d closed", record.Sequence)
	}

	s.DispatchAsync(alert)
	return nil
}

// Close gracefully closes the dispatcher queue and joins the worker.
func (s *PagerDutySink) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.queue)
	s.closeMu.Unlock()

	s.cancel()
	s.wg.Wait()
	return nil
}
