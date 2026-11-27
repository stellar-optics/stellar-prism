package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultGCSEndpoint = "https://storage.googleapis.com"

// GCSOptions configures a GCSStore. See
// docs/sinks/parquet_and_cloud_storage.md for CLI flags, credentials, and
// object key layout.
type GCSOptions struct {
	// Endpoint is the storage host, for example
	// https://storage.googleapis.com. A fake server may be used in tests.
	// Empty uses the public endpoint.
	Endpoint string
	// Bucket is the destination bucket. Required.
	Bucket string
	// AccessToken is an OAuth 2.0 bearer token with write access. Required.
	// The CLI reads it from the environment rather than a flag so it does not
	// appear in process listings.
	AccessToken string
	// Client performs the upload. Nil builds one with Timeout.
	Client *http.Client
	// Timeout bounds a single request when Client is nil. Zero uses 30s.
	Timeout time.Duration
	// MaxResponseBytes bounds the error body kept in a returned error. Zero
	// uses 4KiB.
	MaxResponseBytes int64
}

// GCSStore uploads objects to Google Cloud Storage through the JSON API's
// simple media upload. No SDK is needed for one immutable PUT per chunk.
type GCSStore struct {
	endpoint string
	opts     GCSOptions
	client   *http.Client
}

// NewGCSStore validates opts and builds the store.
func NewGCSStore(opts GCSOptions) (*GCSStore, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(opts.Endpoint), "/")
	if endpoint == "" {
		endpoint = defaultGCSEndpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("gcs: invalid endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("gcs: endpoint %q must use http or https", endpoint)
	}
	if strings.TrimSpace(opts.Bucket) == "" {
		return nil, fmt.Errorf("gcs: Bucket is required")
	}
	if strings.TrimSpace(opts.AccessToken) == "" {
		return nil, fmt.Errorf("gcs: AccessToken is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultUploadTimeout
	}
	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = defaultWebhookResponseBytes
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}
	return &GCSStore{endpoint: endpoint, opts: opts, client: client}, nil
}

// PutObject implements ObjectStore.
func (g *GCSStore) PutObject(ctx context.Context, key string, body []byte, contentType string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	key = strings.TrimLeft(key, "/")
	if key == "" {
		return fmt.Errorf("gcs: object key is required")
	}

	query := url.Values{}
	query.Set("uploadType", "media")
	query.Set("name", key)
	uploadURL := g.endpoint + "/upload/storage/v1/b/" + url.PathEscape(g.opts.Bucket) + "/o?" + query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("gcs: building request for %s: %w", key, err)
	}
	req.Header.Set("Authorization", "Bearer "+g.opts.AccessToken)
	req.Header.Set("Content-Type", contentType)

	resp, err := g.client.Do(req)
	if err != nil {
		return fmt.Errorf("gcs: uploading %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, g.opts.MaxResponseBytes))
	if readErr != nil {
		return fmt.Errorf("gcs: reading response for %s: %w", key, readErr)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("gcs: uploading %s: server responded %s: %s",
		key, http.StatusText(resp.StatusCode), strings.TrimSpace(string(responseBody)))
}
