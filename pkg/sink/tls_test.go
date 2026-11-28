package sink

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateTestCert(t *testing.T, commonName string) ([]byte, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey error: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate error: %v", err)
	}

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey error: %v", err)
	}
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})

	return certPem, keyPem
}

func TestDynamicTLSReloader(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")

	// 1. Generate and write Cert 1
	certPem1, keyPem1 := generateTestCert(t, "prism-v1.local")
	if err := os.WriteFile(certPath, certPem1, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPem1, 0o600); err != nil {
		t.Fatal(err)
	}

	reloader, err := NewDynamicTLSReloader(certPath, keyPath, true, nil)
	if err != nil {
		t.Fatalf("NewDynamicTLSReloader() error = %v", err)
	}
	defer reloader.Close()

	// Verify initial certificate
	activeCert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate() error = %v", err)
	}
	leaf, err := x509.ParseCertificate(activeCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Subject.CommonName != "prism-v1.local" {
		t.Errorf("CommonName = %q, want prism-v1.local", leaf.Subject.CommonName)
	}

	// 2. Overwrite with Cert 2 (simulating ACME cert renewal)
	certPem2, keyPem2 := generateTestCert(t, "prism-v2.local")
	if err := os.WriteFile(certPath, certPem2, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPem2, 0o600); err != nil {
		t.Fatal(err)
	}

	// Trigger reload
	if err := reloader.Reload(); err != nil {
		t.Fatalf("reloader.Reload() error = %v", err)
	}

	activeCert2, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate() after reload error = %v", err)
	}
	leaf2, err := x509.ParseCertificate(activeCert2.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf2.Subject.CommonName != "prism-v2.local" {
		t.Errorf("CommonName = %q, want prism-v2.local", leaf2.Subject.CommonName)
	}

	// 3. Write corrupted cert file and verify resilience
	if err := os.WriteFile(certPath, []byte("NOT_A_VALID_CERTIFICATE"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reloader.Reload(); err == nil {
		t.Fatalf("expected Reload() error on corrupt cert, got nil")
	}

	// Verify that the active certificate is still valid and serving (prism-v2.local)
	activeCert3, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate() after failed reload error = %v", err)
	}
	leaf3, err := x509.ParseCertificate(activeCert3.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf3.Subject.CommonName != "prism-v2.local" {
		t.Errorf("CommonName = %q, want retain prism-v2.local", leaf3.Subject.CommonName)
	}
}

func TestDynamicTLSHandshake(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")

	certPem, keyPem := generateTestCert(t, "127.0.0.1")
	if err := os.WriteFile(certPath, certPem, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPem, 0o600); err != nil {
		t.Fatal(err)
	}

	reloader, err := NewDynamicTLSReloader(certPath, keyPath, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloader.Close()

	tlsCfg := reloader.TLSConfig()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	// Client connection verifying handshake
	clientCfg := &tls.Config{InsecureSkipVerify: true}
	clientConn, err := tls.Dial("tcp", listener.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("tls.Dial() handshake failed: %v", err)
	}
	_, _ = clientConn.Write([]byte("x"))
	_ = clientConn.Close()
	<-serverDone
}
