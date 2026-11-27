package stream_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestNewAWSKMSClientValidation(t *testing.T) {
	if _, err := stream.NewAWSKMSClient(stream.AWSKMSOptions{}); err == nil {
		t.Fatal("expected error when KeyID is empty")
	}
	if _, err := stream.NewAWSKMSClient(stream.AWSKMSOptions{KeyID: "arn:aws:kms:us-east-1:123:key/abc"}); err == nil {
		t.Fatal("expected error when credentials are empty")
	}
}

func TestAWSKMSClientGenerateAndDecryptDataKey(t *testing.T) {
	wantPlaintext := []byte("0123456789abcdef0123456789abcdef")
	wantWrapped := []byte("wrapped-aws-dek")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch target {
		case "TrentService.GenerateDataKey":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"Plaintext":      base64.StdEncoding.EncodeToString(wantPlaintext),
				"CiphertextBlob": base64.StdEncoding.EncodeToString(wantWrapped),
			})
		case "TrentService.Decrypt":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"Plaintext": base64.StdEncoding.EncodeToString(wantPlaintext),
			})
		default:
			http.Error(w, "unexpected target: "+target, http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	client, err := stream.NewAWSKMSClient(stream.AWSKMSOptions{
		KeyID:           "arn:aws:kms:us-east-1:123456789012:key/test",
		Region:          "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:  srv.URL,
		Client:    srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewAWSKMSClient() error = %v", err)
	}
	if client.Name() != "aws-kms" {
		t.Fatalf("Name() = %q, want aws-kms", client.Name())
	}

	ctx := context.Background()
	plaintext, wrapped, err := client.GenerateDataKey(ctx)
	if err != nil {
		t.Fatalf("GenerateDataKey() error = %v", err)
	}
	if !bytes.Equal(plaintext, wantPlaintext) {
		t.Errorf("GenerateDataKey() plaintext = %q, want %q", plaintext, wantPlaintext)
	}
	if !bytes.Equal(wrapped, wantWrapped) {
		t.Errorf("GenerateDataKey() wrapped = %q, want %q", wrapped, wantWrapped)
	}

	decrypted, err := client.DecryptDataKey(ctx, wrapped)
	if err != nil {
		t.Fatalf("DecryptDataKey() error = %v", err)
	}
	if !bytes.Equal(decrypted, wantPlaintext) {
		t.Errorf("DecryptDataKey() = %q, want %q", decrypted, wantPlaintext)
	}
}

func TestNewGCPKMSClientValidation(t *testing.T) {
	if _, err := stream.NewGCPKMSClient(stream.GCPKMSOptions{}); err == nil {
		t.Fatal("expected error when KeyName is empty")
	}
	if _, err := stream.NewGCPKMSClient(stream.GCPKMSOptions{KeyName: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}); err == nil {
		t.Fatal("expected error when AccessToken is empty")
	}
}

func TestGCPKMSClientGenerateAndDecryptDataKey(t *testing.T) {
	var capturedPlaintext []byte
	wantWrapped := []byte("wrapped-gcp-dek")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, ":encrypt"):
			var req struct {
				Plaintext string `json:"plaintext"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			decoded, err := base64.StdEncoding.DecodeString(req.Plaintext)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			capturedPlaintext = decoded
			_ = json.NewEncoder(w).Encode(map[string]string{
				"ciphertext": base64.StdEncoding.EncodeToString(wantWrapped),
			})
		case strings.HasSuffix(r.URL.Path, ":decrypt"):
			_ = json.NewEncoder(w).Encode(map[string]string{
				"plaintext": base64.StdEncoding.EncodeToString(capturedPlaintext),
			})
		default:
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client, err := stream.NewGCPKMSClient(stream.GCPKMSOptions{
		KeyName:     "projects/p/locations/global/keyRings/r/cryptoKeys/k",
		AccessToken: "test-oauth-token",
		Endpoint:    srv.URL,
		Client:      srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewGCPKMSClient() error = %v", err)
	}
	if client.Name() != "gcp-kms" {
		t.Fatalf("Name() = %q, want gcp-kms", client.Name())
	}

	ctx := context.Background()
	plaintext, wrapped, err := client.GenerateDataKey(ctx)
	if err != nil {
		t.Fatalf("GenerateDataKey() error = %v", err)
	}
	if len(plaintext) != 32 {
		t.Errorf("GenerateDataKey() len(plaintext) = %d, want 32", len(plaintext))
	}
	if !bytes.Equal(wrapped, wantWrapped) {
		t.Errorf("GenerateDataKey() wrapped = %q, want %q", wrapped, wantWrapped)
	}

	decrypted, err := client.DecryptDataKey(ctx, wrapped)
	if err != nil {
		t.Fatalf("DecryptDataKey() error = %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("DecryptDataKey() = %x, want %x", decrypted, plaintext)
	}
}
