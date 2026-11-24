package sink

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

// DynamicTLSReloader provides zero-downtime dynamic TLS certificate reloading,
// designed for ACME / Let's Encrypt automated renewals without restarting servers.
type DynamicTLSReloader struct {
	certFile   string
	keyFile    string
	autoReload bool
	current    atomic.Pointer[tls.Certificate]
	watcher    *fsnotify.Watcher
	logger     func(string, ...any)
	done       chan struct{}
	closeOnce  sync.Once
	mu         sync.Mutex
}

// NewDynamicTLSReloader loads the initial certificate pair and optionally starts
// a file watcher to automatically reload certificates on change.
func NewDynamicTLSReloader(certFile, keyFile string, autoReload bool, logger func(string, ...any)) (*DynamicTLSReloader, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tls: cert and key paths must not be empty")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: loading initial keypair: %w", err)
	}

	if logger == nil {
		logger = func(string, ...any) {}
	}

	reloader := &DynamicTLSReloader{
		certFile:   certFile,
		keyFile:    keyFile,
		autoReload: autoReload,
		logger:     logger,
		done:       make(chan struct{}),
	}
	reloader.current.Store(&cert)

	if autoReload {
		if err := reloader.startWatcher(); err != nil {
			logger("prism: warning: failed to initialize TLS file watcher (%v); manual reload available", err)
		}
	}

	return reloader, nil
}

func (r *DynamicTLSReloader) startWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	r.watcher = watcher

	// Watch parent directories to handle atomic file swaps (symlink changes/renames from certbot)
	dirsToWatch := map[string]bool{
		filepath.Dir(r.certFile): true,
		filepath.Dir(r.keyFile):  true,
	}

	for dir := range dirsToWatch {
		if err := watcher.Add(dir); err != nil {
			// If directory watch fails, attempt watching the file directly
			_ = watcher.Add(r.certFile)
			_ = watcher.Add(r.keyFile)
		}
	}

	go r.watchLoop()
	return nil
}

func (r *DynamicTLSReloader) watchLoop() {
	var debounceTimer *time.Timer
	var debounceTimerCh <-chan time.Time

	for {
		select {
		case <-r.done:
			return
		case err, ok := <-r.watcher.Errors:
			if !ok {
				return
			}
			r.logger("prism: tls watcher error: %v", err)
		case event, ok := <-r.watcher.Events:
			if !ok {
				return
			}

			// Check if event targets our cert or key files
			baseName := filepath.Base(event.Name)
			if baseName == filepath.Base(r.certFile) || baseName == filepath.Base(r.keyFile) ||
				event.Name == r.certFile || event.Name == r.keyFile {
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Chmod) != 0 {
					if debounceTimer != nil {
						debounceTimer.Stop()
					}
					debounceTimer = time.NewTimer(100 * time.Millisecond)
					debounceTimerCh = debounceTimer.C
				}
			}
		case <-debounceTimerCh:
			if err := r.Reload(); err != nil {
				r.logger("prism: TLS auto-reload failed, keeping current valid certificate: %v", err)
			} else {
				r.logger("prism: TLS certificate successfully reloaded from %s", r.certFile)
			}
			debounceTimerCh = nil
		}
	}
}

// Reload reloads the certificate files from disk and atomically swaps them.
// If the new certificate files are invalid, corrupted, or unreadable, the
// reload fails and the current certificate remains active without disruption.
func (r *DynamicTLSReloader) Reload() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Verify both files exist before attempting load
	if _, err := os.Stat(r.certFile); err != nil {
		return fmt.Errorf("stat cert file: %w", err)
	}
	if _, err := os.Stat(r.keyFile); err != nil {
		return fmt.Errorf("stat key file: %w", err)
	}

	newCert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("parsing renewed keypair: %w", err)
	}

	r.current.Store(&newCert)
	return nil
}

// GetCertificate implements the tls.Config GetCertificate callback.
func (r *DynamicTLSReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := r.current.Load()
	if cert == nil {
		return nil, errors.New("tls: no active certificate loaded")
	}
	return cert, nil
}

// TLSConfig returns a tls.Config configured with the dynamic reloader callback.
func (r *DynamicTLSReloader) TLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: r.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}
}

// Close stops the watcher and frees resources.
func (r *DynamicTLSReloader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		close(r.done)
		if r.watcher != nil {
			err = r.watcher.Close()
		}
	})
	return err
}
