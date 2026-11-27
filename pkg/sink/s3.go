package sink

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	defaultS3Region      = "us-east-1"
	defaultUploadTimeout = 30 * time.Second
)

// S3Options configures an S3Store. See
// docs/sinks/parquet_and_cloud_storage.md for CLI flags, credentials, and
// object key layout.
type S3Options struct {
	// Endpoint is the service URL, for example
	// https://s3.us-east-1.amazonaws.com or a MinIO gateway.
	Endpoint string
	// Bucket is the destination bucket. Required.
	Bucket string
	// Region is the signing region. Zero uses us-east-1.
	Region string
	// AccessKey and SecretKey are static credentials. They are read from the
	// environment by the CLI; they are never written to an object key.
	AccessKey string
	SecretKey string
	// SessionToken is set for temporary credentials. It is signed as
	// x-amz-security-token.
	SessionToken string
	// Client performs the upload. Nil builds one with Timeout.
	Client *http.Client
	// Timeout bounds a single request when Client is nil. Zero uses 30s.
	Timeout time.Duration
	// Now is the signing clock. Tests set it; nil means time.Now.
	Now func() time.Time
	// MaxResponseBytes bounds the error body kept in a returned error. Zero
	// uses 4KiB.
	MaxResponseBytes int64
	// VirtualHosted addresses the bucket as <bucket>.<endpoint> instead of
	// putting it in the path. Some gateways only support path style, which is
	// the default.
	VirtualHosted bool
}

// S3Store uploads objects to an S3-compatible bucket with a SigV4-signed PUT.
//
// It deliberately uses the HTTP API rather than an SDK: the sink only ever
// writes immutable objects, and keeping the module free of the AWS SDK keeps
// prism's binary and dependency surface small.
type S3Store struct {
	endpoint *url.URL
	opts     S3Options
	client   *http.Client
	now      func() time.Time
}

// NewS3Store validates opts and builds the store.
func NewS3Store(opts S3Options) (*S3Store, error) {
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("s3: Endpoint is required")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: invalid endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("s3: endpoint %q must use http or https", opts.Endpoint)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("s3: endpoint %q has no host", opts.Endpoint)
	}
	if strings.TrimSpace(opts.Bucket) == "" {
		return nil, fmt.Errorf("s3: Bucket is required")
	}
	if strings.TrimSpace(opts.AccessKey) == "" || strings.TrimSpace(opts.SecretKey) == "" {
		return nil, fmt.Errorf("s3: AccessKey and SecretKey are required")
	}
	if opts.Region == "" {
		opts.Region = defaultS3Region
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
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	return &S3Store{endpoint: parsed, opts: opts, client: client, now: now}, nil
}

// PutObject implements ObjectStore.
func (s *S3Store) PutObject(ctx context.Context, key string, body []byte, contentType string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	key = strings.TrimLeft(key, "/")
	if key == "" {
		return fmt.Errorf("s3: object key is required")
	}

	host := s.endpoint.Host
	basePath := strings.TrimRight(s.endpoint.EscapedPath(), "/")
	var canonicalURI string
	if s.opts.VirtualHosted {
		host = s.opts.Bucket + "." + s.endpoint.Host
		canonicalURI = basePath + "/" + awsURIEncode(key, false)
	} else {
		canonicalURI = basePath + "/" + awsURIEncode(s.opts.Bucket, true) + "/" + awsURIEncode(key, false)
	}

	headers := map[string]string{
		"host":         host,
		"content-type": contentType,
	}
	if s.opts.SessionToken != "" {
		headers["x-amz-security-token"] = s.opts.SessionToken
	}

	authorization := awsAuthorization(awsSigningInput{
		method:       http.MethodPut,
		canonicalURI: canonicalURI,
		headers:      headers,
		payload:      body,
		accessKey:    s.opts.AccessKey,
		secretKey:    s.opts.SecretKey,
		region:       s.opts.Region,
		service:      "s3",
		now:          s.now(),
	})

	rawURL := s.endpoint.Scheme + "://" + host + canonicalURI
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("s3: building request for %s: %w", key, err)
	}
	for name, value := range headers {
		if name == "host" {
			continue
		}
		req.Header.Set(name, value)
	}
	req.Header.Set("Authorization", authorization)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3: uploading %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, s.opts.MaxResponseBytes))
	if readErr != nil {
		return fmt.Errorf("s3: reading response for %s: %w", key, readErr)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("s3: uploading %s: server responded %s: %s",
		key, http.StatusText(resp.StatusCode), strings.TrimSpace(string(responseBody)))
}

// awsSigningInput is everything SigV4 needs to sign one request.
type awsSigningInput struct {
	method       string
	canonicalURI string
	// canonicalQuery is empty for the PUTs this package makes; SigV4 requires
	// an empty line rather than omitting it.
	canonicalQuery string
	// headers are lowercase header names, including host. The signer adds
	// x-amz-content-sha256 and x-amz-date.
	headers   map[string]string
	payload   []byte
	accessKey string
	secretKey string
	region    string
	service   string
	now       time.Time
}

// awsAuthorization returns the Authorization header value for in.
func awsAuthorization(in awsSigningInput) string {
	const algorithm = "AWS4-HMAC-SHA256"

	amzDate := in.now.UTC().Format("20060102T150405Z")
	date := in.now.UTC().Format("20060102")
	payloadHash := sha256Hex(in.payload)

	in.headers["x-amz-content-sha256"] = payloadHash
	in.headers["x-amz-date"] = amzDate

	names := make([]string, 0, len(in.headers))
	for name := range in.headers {
		names = append(names, strings.ToLower(name))
	}
	sort.Strings(names)

	headerLines := make([]string, 0, len(names))
	for _, name := range names {
		headerLines = append(headerLines, name+":"+strings.TrimSpace(in.headers[name]))
	}
	signedHeaders := strings.Join(names, ";")

	// CanonicalHeaders itself ends with a newline and the format adds another
	// before SignedHeaders, which is why there is a blank line between them.
	canonicalRequest := in.method + "\n" +
		in.canonicalURI + "\n" +
		in.canonicalQuery + "\n" +
		strings.Join(headerLines, "\n") + "\n\n" +
		signedHeaders + "\n" +
		payloadHash

	scope := date + "/" + in.region + "/" + in.service + "/aws4_request"
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))

	signingKey := hmacSHA256([]byte("AWS4"+in.secretKey), date)
	signingKey = hmacSHA256(signingKey, in.region)
	signingKey = hmacSHA256(signingKey, in.service)
	signingKey = hmacSHA256(signingKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	return algorithm + " Credential=" + in.accessKey + "/" + scope +
		",SignedHeaders=" + signedHeaders +
		",Signature=" + signature
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// awsURIEncode implements the strict encoding SigV4 canonicalisation requires:
// only unreserved characters survive, every other byte becomes %XX with
// uppercase hex, and a slash survives only when encodeSlash is false.
//
// net/url.PathEscape is deliberately not used: it leaves characters such as
// '$' alone, which changes the canonical request and breaks the signature.
func awsURIEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"

	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0f])
		}
	}
	return b.String()
}
