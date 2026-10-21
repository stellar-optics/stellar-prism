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
	esPasswordEnvVar    = "PRISM_ES_PASSWORD"
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
	esURL            string
	esIndex          string
	esUsername       string
	esBatchSize      int
	esFlushInterval  time.Duration
	esTimeout        time.Duration
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

	flags.StringVar(&s.esURL, "sink-es-url", "",
		"Elasticsearch/OpenSearch endpoint for bulk indexing")
	flags.StringVar(&s.esIndex, "sink-es-index", "",
		"base Elasticsearch/OpenSearch index name; records are routed to a daily index")
	flags.StringVar(&s.esUsername, "sink-es-username", "",
		"Elasticsearch/OpenSearch basic-auth username (password from $"+esPasswordEnvVar+")")
	flags.IntVar(&s.esBatchSize, "sink-es-batch-size", 100,
		"records per Elasticsearch/OpenSearch bulk request")
	flags.DurationVar(&s.esFlushInterval, "sink-es-flush-interval", time.Second,
		"flush a partial Elasticsearch/OpenSearch batch after this long (0 disables the timer)")
	flags.DurationVar(&s.esTimeout, "sink-es-timeout", 10*time.Second,
		"timeout for an Elasticsearch/OpenSearch bulk request")

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

	registerFlagCompletion(cmd, "cloud-provider", "s3", "gcs")
	registerFlagCompletion(cmd, "syslog-format", string(sink.SyslogRFC5424), string(sink.SyslogJournald))
	registerFlagCompletion(cmd, "syslog-facility",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
		"user", "daemon", "auth", "authpriv", "cron")
}

// defaultGCSEndpoint mirrors the sink package's default without exporting it
// only for help text.
const defaultGCSEndpoint = "https://storage.googleapis.com"

func (s *sinkFlags) validate() error {
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

	esEnabled := s.esURL != "" || s.esIndex != ""
	if esEnabled {
		if s.esURL == "" || s.esIndex == "" {
			return fmt.Errorf("--sink-es-url and --sink-es-index must be provided together")
		}
		if s.esBatchSize <= 0 {
			return fmt.Errorf("--sink-es-batch-size must be greater than zero")
		}
		if s.esFlushInterval < 0 {
			return fmt.Errorf("--sink-es-flush-interval must not be negative")
		}
		if s.esTimeout <= 0 {
			return fmt.Errorf("--sink-es-timeout must be greater than zero")
		}
		password := os.Getenv(esPasswordEnvVar)
		apiKey := os.Getenv(esAPIKeyEnvVar)
		if (s.esUsername == "") != (password == "") {
			return fmt.Errorf("--sink-es-username and %s must be provided together", esPasswordEnvVar)
		}
		if apiKey != "" && (s.esUsername != "" || password != "") {
			return fmt.Errorf("%s cannot be combined with --sink-es-username or %s", esAPIKeyEnvVar, esPasswordEnvVar)
		}
	} else if s.esUsername != "" {
		return fmt.Errorf("--sink-es-username requires --sink-es-url and --sink-es-index")
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
	return nil
}

// addTo wraps base with the extra sinks the flags enable.
//
// A fanout is used only when there is somewhere else to write: the common path
// stays exactly the sink it was before these flags existed. On a construction
// failure every sink already opened is closed, so a bad flag cannot leak a
// metrics listener or a socket.
func (s *sinkFlags) addTo(base stream.Sink, raw bool, onAsyncError func(error)) (out stream.Sink, err error) {
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
			Decode:     !raw,
			OnError:    observeError,
		})
		if hookErr != nil {
			return nil, hookErr
		}
		s.constructedSinks = append(s.constructedSinks, hook)
		sinks = append(sinks, hook)
	}

	if s.esURL != "" {
		es, esErr := sink.NewElasticsearch(sink.ElasticsearchOptions{
			URL:           s.esURL,
			Index:         s.esIndex,
			Username:      s.esUsername,
			Password:      os.Getenv(esPasswordEnvVar),
			APIKey:        os.Getenv(esAPIKeyEnvVar),
			BatchSize:     s.esBatchSize,
			FlushInterval: s.esFlushInterval,
			Timeout:       s.esTimeout,
			Decode:        !raw,
			OnError:       observeError,
		})
		if esErr != nil {
			return nil, esErr
		}
		s.constructedSinks = append(s.constructedSinks, es)
		sinks = append(sinks, es)
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
			Decode:       !raw,
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
			Decode:   !raw,
		})
		if logErr != nil {
			_ = conn.Close()
			return nil, logErr
		}
		s.constructedSinks = append(s.constructedSinks, logSink)
		sinks = append(sinks, logSink)
	}

	if len(sinks) == 1 {
		return base, nil
	}
	return sink.NewFanout(sinks...), nil
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
