package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Environment variables for credentials, so a secret never appears in a
// process listing or shell history.
const (
	webhookSecretEnvVar = "PRISM_WEBHOOK_SECRET"
	esAPIKeyEnvVar      = "PRISM_ES_API_KEY"
	gcsTokenEnvVar      = "GOOGLE_OAUTH_ACCESS_TOKEN"
	awsAccessKeyEnvVar  = "AWS_ACCESS_KEY_ID"
	awsSecretKeyEnvVar  = "AWS_SECRET_ACCESS_KEY"
	awsSessionEnvVar    = "AWS_SESSION_TOKEN"
)

// sinkFlags are the optional non-stdout destinations shared by streaming
// commands. Every one is off unless its flag is set, so default behaviour is
// unchanged and no network is touched.
type sinkFlags struct {
	webhookURL       string
	webhookSecret    string
	webhookBatch     int
	webhookTimeout   time.Duration
	webhookMaxRetry  int
	cloudURL         string
	cloudProvider    string
	cloudBucket      string
	cloudPrefix      string
	cloudRegion      string
	cloudRecords     int
	cloudBytes       int
	cloudTimeout     time.Duration
	cloudCompress    bool
	metricsAddr      string
	syslogAddress    string
	syslogFormat     string
	syslogFacility   string
	syslogAppName    string
	esURL            string
	esIndex          string
	esUsername       string
	esPassword       string
	esAPIKey         string
	esBatchSize      int
	esFlushInterval  time.Duration
	natsURL          string
	natsSubject      string
	natsJetStream    bool
	zmqEndpoint      string
	zmqTopic         string
	auditFile        string
	auditKeyFile     string
	auditCheckpoint  int
	metrics          *sink.Prometheus
	constructedSinks []io.Closer
}

func (s *sinkFlags) register(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.StringVar(&s.webhookURL, "webhook-url", "",
		"POST batches of records as JSON to this HTTP(S) endpoint")
	flags.StringVar(&s.webhookSecret, "webhook-secret", "",
		"HMAC-SHA256 secret for X-Prism-Signature (default $"+webhookSecretEnvVar+")")
	flags.IntVar(&s.webhookBatch, "webhook-batch-size", 100,
		"records per webhook POST")
	flags.DurationVar(&s.webhookTimeout, "webhook-timeout", time.Second,
		"POST a partial webhook batch after this long (0 means only the batch size triggers)")
	flags.IntVar(&s.webhookMaxRetry, "webhook-max-retries", 3,
		"retry a failed webhook delivery this many times (-1 disables retries)")

	flags.StringVar(&s.cloudURL, "cloud-url", "",
		"object store endpoint for archival uploads (default GCS: "+defaultGCSEndpoint+")")
	flags.StringVar(&s.cloudProvider, "cloud-provider", "s3",
		`object store provider: "s3" or "gcs"`)
	flags.StringVar(&s.cloudBucket, "cloud-bucket", "",
		"object store bucket for archival uploads")
	flags.StringVar(&s.cloudPrefix, "cloud-prefix", "prism",
		"key prefix for archived objects")
	flags.StringVar(&s.cloudRegion, "cloud-region", "",
		"S3 signing region (default us-east-1)")
	flags.IntVar(&s.cloudRecords, "cloud-batch-records", 1000,
		"archive a chunk after this many records")
	flags.IntVar(&s.cloudBytes, "cloud-batch-bytes", 8<<20,
		"archive a chunk before it exceeds this many bytes")
	flags.DurationVar(&s.cloudTimeout, "cloud-batch-timeout", 30*time.Second,
		"archive a partial chunk after this long (0 disables the timer)")
	flags.BoolVar(&s.cloudCompress, "cloud-compress", true,
		"gzip archived chunks and name them .ndjson.gz")

	flags.StringVar(&s.metricsAddr, "metrics-addr", "",
		"serve Prometheus metrics at /metrics on this address, e.g. 127.0.0.1:9090")

	flags.StringVar(&s.syslogAddress, "syslog-address", "",
		"send records to a syslog socket, e.g. unixgram:///dev/log or udp://host:514")
	flags.StringVar(&s.syslogFormat, "syslog-format", string(sink.SyslogRFC5424),
		`syslog message format: "rfc5424" or "journald"`)
	flags.StringVar(&s.syslogFacility, "syslog-facility", "local0",
		"syslog facility (local0..local7, user, daemon, ...)")
	flags.StringVar(&s.syslogAppName, "syslog-app", "prism",
		"syslog APP-NAME / SYSLOG_IDENTIFIER")
	flags.StringVar(&s.esURL, "sink-es-url", "", "Elasticsearch or OpenSearch endpoint for bulk ingestion")
	flags.StringVar(&s.esIndex, "sink-es-index", "stellar-events", "base Elasticsearch index; records roll over daily")
	flags.StringVar(&s.esUsername, "sink-es-username", "", "Elasticsearch basic-auth username")
	flags.StringVar(&s.esPassword, "sink-es-password", "", "Elasticsearch basic-auth password (prefer $"+esAPIKeyEnvVar+" for API keys)")
	flags.StringVar(&s.esAPIKey, "sink-es-api-key", "", "Elasticsearch API key (default $"+esAPIKeyEnvVar+")")
	flags.IntVar(&s.esBatchSize, "sink-es-batch-size", 100, "records per Elasticsearch bulk request")
	flags.DurationVar(&s.esFlushInterval, "sink-es-flush-interval", time.Second, "flush a partial Elasticsearch bulk request after this interval")
	flags.StringVar(&s.natsURL, "sink-nats", "", "NATS server URL")
	flags.StringVar(&s.natsSubject, "nats-subject", "stellar.events.{{.ContractID}}", "NATS subject template using .ContractID, .Topic0, .ID or .Ledger")
	flags.BoolVar(&s.natsJetStream, "nats-jetstream", false, "wait for JetStream publish acknowledgements")
	flags.StringVar(&s.zmqEndpoint, "sink-zmq", "", "ZeroMQ PUB endpoint")
	flags.StringVar(&s.zmqTopic, "zmq-topic", "stellar.events.{{.ContractID}}", "ZeroMQ topic template using .ContractID, .Topic0, .ID or .Ledger")
	flags.StringVar(&s.auditFile, "sink-audit", "",
		"write a tamper-evident audit trail, chained with SHA-256 and signed at checkpoints, to this file")
	flags.StringVar(&s.auditKeyFile, "sink-audit-key", "",
		"Ed25519 private key signing the audit checkpoints (default <audit file>.key, created if absent)")
	flags.IntVar(&s.auditCheckpoint, "sink-audit-checkpoint", sink.DefaultAuditCheckpointRecords,
		"write a signed audit checkpoint every this many records (0 writes one after every record)")

	registerFlagCompletion(cmd, "cloud-provider", "s3", "gcs")
	registerFlagCompletion(cmd, "syslog-format", string(sink.SyslogRFC5424), string(sink.SyslogJournald))
	registerFlagCompletion(cmd, "syslog-facility",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
		"user", "daemon", "auth", "authpriv", "cron")
}

// defaultGCSEndpoint mirrors the sink package's default without exporting it
// only for help text.
const defaultGCSEndpoint = "https://storage.googleapis.com"

func (s *sinkFlags) validate(cmd *cobra.Command) error {
	if s.webhookSecret != "" && s.webhookURL == "" {
		return fmt.Errorf("--webhook-secret requires --webhook-url")
	}
	if s.webhookBatch < 0 {
		return fmt.Errorf("--webhook-batch-size must not be negative")
	}
	if s.webhookTimeout < 0 {
		return fmt.Errorf("--webhook-timeout must not be negative")
	}
	if s.webhookMaxRetry < -1 {
		return fmt.Errorf("--webhook-max-retries must be -1 or greater")
	}

	cloudEnabled := s.cloudURL != "" || s.cloudBucket != ""
	if cloudEnabled {
		if s.cloudBucket == "" {
			return fmt.Errorf("--cloud-url requires --cloud-bucket")
		}
		provider := strings.ToLower(s.cloudProvider)
		if provider != "s3" && provider != "gcs" {
			return fmt.Errorf(`invalid --cloud-provider %q: want "s3" or "gcs"`, s.cloudProvider)
		}
		if provider == "s3" && s.cloudURL == "" {
			return fmt.Errorf("--cloud-provider s3 requires --cloud-url")
		}
		if s.cloudRecords <= 0 {
			return fmt.Errorf("--cloud-batch-records must be greater than zero")
		}
		if s.cloudBytes <= 0 {
			return fmt.Errorf("--cloud-batch-bytes must be greater than zero")
		}
		if s.cloudTimeout < 0 {
			return fmt.Errorf("--cloud-batch-timeout must not be negative")
		}
	} else if s.cloudProvider != "s3" && s.cloudProvider != "" {
		return fmt.Errorf("--cloud-provider requires --cloud-bucket")
	}

	if s.syslogAddress != "" {
		format := sink.SyslogFormat(strings.ToLower(s.syslogFormat))
		if format != sink.SyslogRFC5424 && format != sink.SyslogJournald {
			return fmt.Errorf(`invalid --syslog-format %q: want "rfc5424" or "journald"`, s.syslogFormat)
		}
		if _, err := sink.ParseSyslogFacility(s.syslogFacility); err != nil {
			return err
		}
	}
	if s.esURL == "" && (s.esUsername != "" || s.esPassword != "" || s.esAPIKey != "") {
		return fmt.Errorf("elasticsearch credentials require --sink-es-url")
	}
	if s.esURL != "" && s.esBatchSize <= 0 {
		return fmt.Errorf("--sink-es-batch-size must be greater than zero")
	}
	if s.esFlushInterval < 0 {
		return fmt.Errorf("--sink-es-flush-interval must not be negative")
	}
	if s.natsURL == "" && s.natsJetStream {
		return fmt.Errorf("--nats-jetstream requires --sink-nats")
	}
	return s.validateAudit(cmd)
}

// validateAudit checks the audit trail options.
//
// The flags only mean something when there is a trail to write, so they are
// rejected on their own rather than being silently ignored.
func (s *sinkFlags) validateAudit(cmd *cobra.Command) error {
	if s.auditFile == "" {
		if s.auditKeyFile != "" {
			return fmt.Errorf("--sink-audit-key requires --sink-audit")
		}
		if cmd != nil && cmd.Flags().Changed("sink-audit-checkpoint") {
			return fmt.Errorf("--sink-audit-checkpoint requires --sink-audit")
		}
		return nil
	}
	if s.auditCheckpoint < 0 {
		return fmt.Errorf("--sink-audit-checkpoint must not be negative")
	}
	return nil
}

// addTo wraps base with the extra sinks the flags enable.
//
// A fanout is used only when there is somewhere else to write: the common path
// stays exactly the sink it was before these flags existed. On a construction
// failure every sink already opened is closed, so a bad flag cannot leak a
// metrics listener or a socket.
func (s *sinkFlags) addTo(base stream.Sink, decode bool, onAsyncError func(error)) (out stream.Sink, err error) {
	sinks := []stream.Sink{base}
	s.constructedSinks = nil

	closeConstructed := func() {
		for _, closer := range s.constructedSinks {
			_ = closer.Close()
		}
		s.constructedSinks = nil
	}
	defer func() {
		if err != nil {
			closeConstructed()
			s.metrics = nil
		}
	}()

	observeError := func(err error) {
		if s.metrics != nil {
			s.metrics.RecordError("sink")
		}
		if onAsyncError != nil {
			onAsyncError(err)
		}
	}

	if s.metricsAddr != "" {
		metrics, metricsErr := sink.NewPrometheus(sink.PrometheusOptions{ListenAddr: s.metricsAddr})
		if metricsErr != nil {
			return nil, metricsErr
		}
		s.metrics = metrics
		s.constructedSinks = append(s.constructedSinks, metrics)
		// Metrics first so they observe every record even when a later sink
		// fails the fanout.
		sinks = append(sinks, metrics)
	}

	if s.auditFile != "" {
		audit, auditErr := sink.NewAuditSink(sink.AuditOptions{
			Path:              s.auditFile,
			KeyPath:           s.auditKeyFile,
			CheckpointRecords: s.auditCheckpoint,
			Decode:            decode,
		})
		if auditErr != nil {
			return nil, auditErr
		}
		s.constructedSinks = append(s.constructedSinks, audit)
		// The trail is written before anything that can fail on the
		// network, so a record that was delivered is always a record the
		// audit trail saw.
		sinks = append(sinks, audit)
	}

	if s.webhookURL != "" {
		secret := s.webhookSecret
		if secret == "" {
			secret = os.Getenv(webhookSecretEnvVar)
		}
		hook, hookErr := sink.NewWebhook(sink.WebhookOptions{
			URL:        s.webhookURL,
			Secret:     []byte(secret),
			BatchSize:  s.webhookBatch,
			MaxDelay:   s.webhookTimeout,
			MaxRetries: s.webhookMaxRetry,
			Jitter:     0.2,
			Decode:     decode,
			OnError:    observeError,
		})
		if hookErr != nil {
			return nil, hookErr
		}
		s.constructedSinks = append(s.constructedSinks, hook)
		sinks = append(sinks, hook)
	}

	if s.cloudURL != "" || s.cloudBucket != "" {
		store, storeErr := s.objectStore()
		if storeErr != nil {
			return nil, storeErr
		}
		archive, archiveErr := sink.NewCloudStorage(sink.CloudStorageOptions{
			Store:        store,
			Prefix:       s.cloudPrefix,
			BatchRecords: s.cloudRecords,
			BatchBytes:   s.cloudBytes,
			MaxDelay:     s.cloudTimeout,
			Compress:     s.cloudCompress,
			Decode:       decode,
			Jitter:       0.2,
			OnError:      observeError,
		})
		if archiveErr != nil {
			return nil, archiveErr
		}
		s.constructedSinks = append(s.constructedSinks, archive)
		sinks = append(sinks, archive)
	}

	if s.syslogAddress != "" {
		conn, dialErr := sink.DialSyslog(s.syslogAddress)
		if dialErr != nil {
			return nil, dialErr
		}
		logSink, logErr := sink.NewSyslog(sink.SyslogOptions{
			Writer:   conn,
			Format:   sink.SyslogFormat(strings.ToLower(s.syslogFormat)),
			Facility: s.syslogFacility,
			AppName:  s.syslogAppName,
			Decode:   decode,
		})
		if logErr != nil {
			_ = conn.Close()
			return nil, logErr
		}
		s.constructedSinks = append(s.constructedSinks, logSink)
		sinks = append(sinks, logSink)
	}

	if s.esURL != "" {
		apiKey := s.esAPIKey
		if apiKey == "" {
			apiKey = os.Getenv(esAPIKeyEnvVar)
		}
		es, esErr := sink.NewElasticsearchSink(sink.ElasticsearchOptions{URL: s.esURL, Index: s.esIndex, Username: s.esUsername, Password: s.esPassword, APIKey: apiKey, BatchSize: s.esBatchSize, MaxDelay: s.esFlushInterval, Decode: decode})
		if esErr != nil {
			return nil, esErr
		}
		s.constructedSinks = append(s.constructedSinks, es)
		sinks = append(sinks, es)
	}
	if s.natsURL != "" {
		natsSink, natsErr := sink.NewNATSSink(sink.NATSOptions{URL: s.natsURL, Subject: s.natsSubject, JetStream: s.natsJetStream, Decode: decode})
		if natsErr != nil {
			return nil, natsErr
		}
		s.constructedSinks = append(s.constructedSinks, natsSink)
		sinks = append(sinks, natsSink)
	}
	if s.zmqEndpoint != "" {
		zmqSink, zmqErr := sink.NewZeroMQSink(sink.ZeroMQOptions{Endpoint: s.zmqEndpoint, Topic: s.zmqTopic, Decode: decode})
		if zmqErr != nil {
			return nil, zmqErr
		}
		s.constructedSinks = append(s.constructedSinks, zmqSink)
		sinks = append(sinks, zmqSink)
	}

	if len(sinks) == 1 {
		return base, nil
	}
	return sink.NewMultiSink(sinks, sink.MultiSinkOptions{Policy: sink.MultiSinkAbortAll})
}

func (s *sinkFlags) objectStore() (sink.ObjectStore, error) {
	switch strings.ToLower(s.cloudProvider) {
	case "gcs":
		token := os.Getenv(gcsTokenEnvVar)
		if token == "" {
			return nil, fmt.Errorf("--cloud-provider gcs needs %s", gcsTokenEnvVar)
		}
		return sink.NewGCSStore(sink.GCSOptions{
			Endpoint:    s.cloudURL,
			Bucket:      s.cloudBucket,
			AccessToken: token,
		})
	case "s3":
		accessKey := os.Getenv(awsAccessKeyEnvVar)
		secretKey := os.Getenv(awsSecretKeyEnvVar)
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("--cloud-provider s3 needs %s and %s",
				awsAccessKeyEnvVar, awsSecretKeyEnvVar)
		}
		return sink.NewS3Store(sink.S3Options{
			Endpoint:     s.cloudURL,
			Bucket:       s.cloudBucket,
			Region:       s.cloudRegion,
			AccessKey:    accessKey,
			SecretKey:    secretKey,
			SessionToken: os.Getenv(awsSessionEnvVar),
		})
	default:
		return nil, fmt.Errorf(`invalid --cloud-provider %q: want "s3" or "gcs"`, s.cloudProvider)
	}
}
