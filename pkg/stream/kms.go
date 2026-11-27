package stream

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// KMSClient is the part of a key management service that an encrypted state
// file needs.
//
// The service generates a data key once, when the file is created, and the
// wrapped form of that key is stored in the file. Reading the file again
// unwraps it, so the plaintext key is never written to disk.
//
// The interface is deliberately tiny. The implementations speak the
// provider's HTTP API directly, which keeps prism free of a cloud SDK for
// the sake of one key.
//
// Operator guide: docs/security/kms_and_state_encryption.md describes the
// envelope scheme, the exact AWS and GCP IAM permissions these calls need,
// and how tampering with an encrypted state file is detected.
type KMSClient interface {
	// Name identifies the provider in the state file header.
	Name() string
	// GenerateDataKey returns a new data key and its wrapped form.
	GenerateDataKey(ctx context.Context) (plaintext, wrapped []byte, err error)
	// DecryptDataKey unwraps a data key.
	DecryptDataKey(ctx context.Context, wrapped []byte) ([]byte, error)
}

// defaultKMSResponseBytes bounds the error body kept in a returned error.
const defaultKMSResponseBytes = 4 << 10

// AWSKMSOptions configures an AWSKMSClient.
type AWSKMSOptions struct {
	// KeyID is the KMS key id or ARN. Required.
	KeyID string
	// Region is the signing region. Empty uses us-east-1.
	Region string
	// AccessKey and SecretKey are the credentials the request is signed
	// with. They are read from the environment by the CLI.
	AccessKey string
	// SecretKey is the secret half of the credentials.
	SecretKey string
	// SessionToken is set for temporary credentials. It is signed as
	// x-amz-security-token.
	SessionToken string
	// Endpoint overrides the service URL. Empty uses the regional KMS
	// endpoint. Tests point it at a local server.
	Endpoint string
	// Client performs the request. Nil builds one with Timeout.
	Client *http.Client
	// Timeout bounds a single request when Client is nil. Zero uses 30s.
	Timeout time.Duration
	// Now is the signing clock. Tests set it; nil means time.Now.
	Now func() time.Time
}

// AWSKMSClient talks to AWS Key Management Service.
//
// It signs its requests with Signature Version 4 rather than pulling in the
// AWS SDK: prism needs one key generated and one key unwrapped, and a
// dependency that large is not worth carrying for that.
type AWSKMSClient struct {
	opts AWSKMSOptions
}

// NewAWSKMSClient returns a client configured by opts.
func NewAWSKMSClient(opts AWSKMSOptions) (*AWSKMSClient, error) {
	if opts.KeyID == "" {
		return nil, errors.New("aws kms: a key id is required")
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.AccessKey == "" || opts.SecretKey == "" {
		return nil, errors.New("aws kms: credentials are required")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = fmt.Sprintf("https://kms.%s.amazonaws.com", opts.Region)
	}
	if opts.Client == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		opts.Client = &http.Client{Timeout: timeout}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &AWSKMSClient{opts: opts}, nil
}

// Name implements KMSClient.
func (c *AWSKMSClient) Name() string { return "aws-kms" }

// GenerateDataKey implements KMSClient.
func (c *AWSKMSClient) GenerateDataKey(ctx context.Context) ([]byte, []byte, error) {
	body, err := json.Marshal(map[string]string{
		"KeyId":   c.opts.KeyID,
		"KeySpec": "AES_256",
	})
	if err != nil {
		return nil, nil, fmt.Errorf("aws kms: encoding the request: %w", err)
	}

	var response struct {
		Plaintext      string `json:"Plaintext"`
		CiphertextBlob string `json:"CiphertextBlob"`
	}
	if err := c.call(ctx, "TrentService.GenerateDataKey", body, &response); err != nil {
		return nil, nil, err
	}
	plaintext, err := base64.StdEncoding.DecodeString(response.Plaintext)
	if err != nil {
		return nil, nil, fmt.Errorf("aws kms: decoding the data key: %w", err)
	}
	wrapped, err := base64.StdEncoding.DecodeString(response.CiphertextBlob)
	if err != nil {
		return nil, nil, fmt.Errorf("aws kms: decoding the wrapped key: %w", err)
	}
	return plaintext, wrapped, nil
}

// DecryptDataKey implements KMSClient.
func (c *AWSKMSClient) DecryptDataKey(ctx context.Context, wrapped []byte) ([]byte, error) {
	body, err := json.Marshal(map[string]string{
		"CiphertextBlob":      base64.StdEncoding.EncodeToString(wrapped),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	})
	if err != nil {
		return nil, fmt.Errorf("aws kms: encoding the request: %w", err)
	}

	var response struct {
		Plaintext string `json:"Plaintext"`
	}
	if err := c.call(ctx, "TrentService.Decrypt", body, &response); err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(response.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("aws kms: decoding the data key: %w", err)
	}
	return key, nil
}

// call performs one signed KMS request.
func (c *AWSKMSClient) call(ctx context.Context, target string, body []byte, out any) error {
	endpoint, err := url.Parse(c.opts.Endpoint)
	if err != nil {
		return fmt.Errorf("aws kms: invalid endpoint: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("aws kms: building the request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	if c.opts.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.opts.SessionToken)
	}
	c.sign(req, body)

	resp, err := c.opts.Client.Do(req)
	if err != nil {
		return fmt.Errorf("aws kms: calling %s: %w", target, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, defaultKMSResponseBytes))
	if err != nil {
		return fmt.Errorf("aws kms: reading the response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aws kms: %s responded %s: %s",
			target, http.StatusText(resp.StatusCode), strings.TrimSpace(string(responseBody)))
	}
	if err := json.Unmarshal(responseBody, out); err != nil {
		return fmt.Errorf("aws kms: decoding the response: %w", err)
	}
	return nil
}

// sign adds the SigV4 headers to a request.
func (c *AWSKMSClient) sign(req *http.Request, payload []byte) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	headers := map[string]string{
		"host":         host,
		"content-type": req.Header.Get("Content-Type"),
		"x-amz-target": req.Header.Get("X-Amz-Target"),
	}
	if token := req.Header.Get("X-Amz-Security-Token"); token != "" {
		headers["x-amz-security-token"] = token
	}
	authorization := awsAuthorizationV4(awsSigningInput{
		method:       req.Method,
		canonicalURI: req.URL.EscapedPath(),
		headers:      headers,
		payload:      payload,
		accessKey:    c.opts.AccessKey,
		secretKey:    c.opts.SecretKey,
		region:       c.opts.Region,
		service:      "kms",
		now:          c.opts.Now(),
	})
	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-Amz-Date", c.opts.Now().UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", sha256Hex(payload))
}

// awsSigningInput is everything SigV4 needs to sign one request.
type awsSigningInput struct {
	method       string
	canonicalURI string
	// canonicalQuery is empty for the requests this package makes; SigV4
	// requires an empty line rather than omitting it.
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

// awsAuthorizationV4 returns the Authorization header value for in.
func awsAuthorizationV4(in awsSigningInput) string {
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

// sha256Hex returns the hex-encoded SHA-256 of data.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hmacSHA256 signs data with key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// GCPKMSOptions configures a GCPKMSClient.
type GCPKMSOptions struct {
	// KeyName is the full crypto key resource name, for example
	// projects/p/locations/global/keyRings/r/cryptoKeys/k. Required.
	KeyName string
	// AccessToken is an OAuth 2.0 bearer token with permission to use the
	// key. The CLI reads it from the environment.
	AccessToken string
	// Endpoint overrides the service URL. Empty uses the public Google
	// Cloud endpoint. Tests point it at a local server.
	Endpoint string
	// Client performs the request. Nil builds one with Timeout.
	Client *http.Client
	// Timeout bounds a single request when Client is nil. Zero uses 30s.
	Timeout time.Duration
}

// GCPKMSClient talks to Google Cloud Key Management Service.
//
// Like the AWS client it speaks the REST API directly. The bearer token is
// the one the GCS sink already uses, so an operator needs only one
// credential for both.
type GCPKMSClient struct {
	opts GCPKMSOptions
}

// NewGCPKMSClient returns a client configured by opts.
func NewGCPKMSClient(opts GCPKMSOptions) (*GCPKMSClient, error) {
	if opts.KeyName == "" {
		return nil, errors.New("gcp kms: a key name is required")
	}
	if opts.AccessToken == "" {
		return nil, errors.New("gcp kms: an access token is required")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = "https://cloudkms.googleapis.com"
	}
	if opts.Client == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		opts.Client = &http.Client{Timeout: timeout}
	}
	return &GCPKMSClient{opts: opts}, nil
}

// Name implements KMSClient.
func (c *GCPKMSClient) Name() string { return "gcp-kms" }

// GenerateDataKey implements KMSClient.
//
// Cloud KMS has no data key API, so the key is generated here and handed to
// the service to wrap.
func (c *GCPKMSClient) GenerateDataKey(ctx context.Context) ([]byte, []byte, error) {
	plaintext := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, plaintext); err != nil {
		return nil, nil, fmt.Errorf("gcp kms: generating the data key: %w", err)
	}
	wrapped, err := c.encrypt(ctx, plaintext)
	if err != nil {
		return nil, nil, err
	}
	return plaintext, wrapped, nil
}

// DecryptDataKey implements KMSClient.
func (c *GCPKMSClient) DecryptDataKey(ctx context.Context, wrapped []byte) ([]byte, error) {
	body, err := json.Marshal(map[string]string{
		"ciphertext": base64.StdEncoding.EncodeToString(wrapped),
	})
	if err != nil {
		return nil, fmt.Errorf("gcp kms: encoding the request: %w", err)
	}

	var response struct {
		Plaintext string `json:"plaintext"`
	}
	if err := c.call(ctx, "decrypt", body, &response); err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(response.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("gcp kms: decoding the data key: %w", err)
	}
	return key, nil
}

// encrypt wraps a key with the service.
func (c *GCPKMSClient) encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	body, err := json.Marshal(map[string]string{
		"plaintext": base64.StdEncoding.EncodeToString(plaintext),
	})
	if err != nil {
		return nil, fmt.Errorf("gcp kms: encoding the request: %w", err)
	}

	var response struct {
		Ciphertext string `json:"ciphertext"`
	}
	if err := c.call(ctx, "encrypt", body, &response); err != nil {
		return nil, err
	}
	wrapped, err := base64.StdEncoding.DecodeString(response.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("gcp kms: decoding the wrapped key: %w", err)
	}
	return wrapped, nil
}

// call performs one request against the key service.
func (c *GCPKMSClient) call(ctx context.Context, action string, body []byte, out any) error {
	endpoint := strings.TrimSuffix(c.opts.Endpoint, "/") + "/v1/" + c.opts.KeyName + ":" + action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("gcp kms: building the request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.opts.AccessToken)

	resp, err := c.opts.Client.Do(req)
	if err != nil {
		return fmt.Errorf("gcp kms: calling %s: %w", action, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, defaultKMSResponseBytes))
	if err != nil {
		return fmt.Errorf("gcp kms: reading the response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gcp kms: %s responded %s: %s",
			action, http.StatusText(resp.StatusCode), strings.TrimSpace(string(responseBody)))
	}
	if err := json.Unmarshal(responseBody, out); err != nil {
		return fmt.Errorf("gcp kms: decoding the response: %w", err)
	}
	return nil
}
