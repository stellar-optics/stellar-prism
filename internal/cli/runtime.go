package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// DefaultDrainTimeout is how long a shutdown may take to flush records and
// commit the final cursor when no other timeout is configured.
//
// It is a little under the thirty seconds Kubernetes gives a pod by default,
// so the drain finishes while the process is still alive and the final
// cursor is on disk rather than in a buffer that is about to be killed.
const DefaultDrainTimeout = 15 * time.Second

// once guards a shutdown step that must not run twice.
//
// The drain and the deferred teardown both close the output and the cursor
// store. Whichever gets there first does the work; the other one does
// nothing, so a file is never closed twice and an error is never reported
// twice.
type once struct {
	claimed atomic.Bool
}

// claim reports whether the caller is the one that should do the work.
//
// It never blocks: a step that is already running elsewhere is left to
// finish there, and the caller moves on rather than waiting on a flush that
// may never end.
func (o *once) claim() bool { return o.claimed.CompareAndSwap(false, true) }

// errTimedOut marks that runWithTimeout hit its deadline and walked away
// from the work goroutine instead of waiting for it.
//
// Callers use errors.Is to tell that abandonment apart from a finished run,
// so they never read state — stream counters, sink counters — that the
// abandoned goroutine may still be writing.
var errTimedOut = errors.New("timed out")

// runWithTimeout runs work and gives up on it after timeout.
//
// The work is run on its own goroutine, so a flush that hangs on a slow
// network cannot stop the process from exiting: the caller walks away after
// the deadline, and the goroutine is left to finish or die with the
// process. The returned error wraps errTimedOut when that happened.
func runWithTimeout(timeout time.Duration, work func() error) error {
	if timeout <= 0 {
		return work()
	}
	done := make(chan error, 1)
	go func() {
		done <- work()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("%w after %s", errTimedOut, timeout)
	}
}

// healthServer starts the probe endpoints, when they were asked for.
//
// It returns nil when no port was configured, which every caller reads as
// "there is no health server".
func startHealthServer(ctx context.Context, sf *streamFlags, stderr io.Writer) (*stream.HealthServer, *stream.HealthMonitor, error) {
	if sf.healthPort <= 0 {
		return nil, nil, nil
	}
	monitor := stream.NewHealthMonitor(stream.HealthMonitorOptions{MaxLag: sf.maxLag})
	server, err := stream.NewHealthServer(stream.HealthServerOptions{
		Addr:    fmt.Sprintf(":%d", sf.healthPort),
		Monitor: monitor,
	})
	if err != nil {
		return nil, nil, err
	}
	if err := server.Start(ctx); err != nil {
		return nil, nil, err
	}
	fmt.Fprintf(stderr, "prism: serving health probes on http://localhost:%d/healthz and /readyz\n",
		sf.healthPort)
	return server, monitor, nil
}

// leaderElection starts the lease that makes an instance the leader.
//
// It returns nil when no lease file was configured, which every caller
// reads as "there is no election and this instance always leads".
func leaderElection(ctx context.Context, sf *streamFlags, health *stream.HealthMonitor, stderr io.Writer) (*stream.LeaderElection, error) {
	if sf.haLeaseFile == "" {
		return nil, nil
	}
	election, err := stream.NewLeaderElection(stream.LeaderElectionOptions{
		Path:        sf.haLeaseFile,
		Heartbeat:   sf.haHeartbeat,
		TTL:         sf.haTTL,
		StandbyPoll: sf.haStandbyPoll,
		OnChange: func(leader bool) {
			if health != nil {
				health.SetActive(leader)
			}
			if leader {
				fmt.Fprintf(stderr, "prism: acquired the lease; this instance is the leader\n")
				return
			}
			fmt.Fprintf(stderr, "prism: lost the lease; standing by\n")
		},
		OnError: func(err error) {
			fmt.Fprintf(stderr, "prism: leader election: %v\n", err)
		},
	})
	if err != nil {
		return nil, err
	}
	if err := election.Start(ctx); err != nil {
		return nil, err
	}
	return election, nil
}

// stateKeySource builds the key source of an encrypted state file.
//
// The key comes from exactly one place: an environment variable, a file, or
// a key management service. Which one is decided by the flags, and the
// choice is validated before anything is fetched.
func stateKeySource(sf *streamFlags) (stream.KeySource, error) {
	switch {
	case sf.statePassphraseEnv != "":
		passphrase := os.Getenv(sf.statePassphraseEnv)
		if passphrase == "" {
			return nil, fmt.Errorf("--state-passphrase-env %s is empty or unset", sf.statePassphraseEnv)
		}
		return &stream.PassphraseKeySource{Passphrase: passphrase}, nil

	case sf.statePassphraseFile != "":
		raw, err := os.ReadFile(sf.statePassphraseFile)
		if err != nil {
			return nil, fmt.Errorf("reading the state passphrase file: %w", err)
		}
		passphrase := strings.TrimRight(string(raw), "\r\n")
		if passphrase == "" {
			return nil, fmt.Errorf("the state passphrase file %s is empty", sf.statePassphraseFile)
		}
		return &stream.PassphraseKeySource{Passphrase: passphrase}, nil

	case sf.stateKMSProvider != "":
		return kmsKeySource(sf)

	default:
		if key := os.Getenv("PRISM_STATE_KEY"); key != "" {
			return &stream.PassphraseKeySource{Passphrase: key}, nil
		}
	}
	return nil, nil
}

// kmsKeySource builds a key source backed by a key management service.
//
// The service generates the data key and keeps the wrapped form, so the
// plaintext key never reaches the disk.
func kmsKeySource(sf *streamFlags) (stream.KeySource, error) {
	switch strings.ToLower(sf.stateKMSProvider) {
	case "aws":
		accessKey := os.Getenv(awsAccessKeyEnvVar)
		secretKey := os.Getenv(awsSecretKeyEnvVar)
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("--state-kms-provider aws needs %s and %s",
				awsAccessKeyEnvVar, awsSecretKeyEnvVar)
		}
		region := sf.stateKMSRegion
		if region == "" {
			region = os.Getenv("AWS_REGION")
		}
		client, err := stream.NewAWSKMSClient(stream.AWSKMSOptions{
			KeyID:        sf.stateKMSKeyID,
			Region:       region,
			AccessKey:    accessKey,
			SecretKey:    secretKey,
			SessionToken: os.Getenv(awsSessionEnvVar),
		})
		if err != nil {
			return nil, err
		}
		return &stream.KMSKeySource{Client: client}, nil

	case "gcp":
		tokenEnv := sf.stateKMSTokenEnv
		if tokenEnv == "" {
			tokenEnv = gcsTokenEnvVar
		}
		token := os.Getenv(tokenEnv)
		if token == "" {
			return nil, fmt.Errorf("--state-kms-provider gcp needs %s", tokenEnv)
		}
		client, err := stream.NewGCPKMSClient(stream.GCPKMSOptions{
			KeyName:     sf.stateKMSKeyID,
			AccessToken: token,
		})
		if err != nil {
			return nil, err
		}
		return &stream.KMSKeySource{Client: client}, nil
	}
	return nil, fmt.Errorf(`invalid --state-kms-provider %q: want "aws" or "gcp"`, sf.stateKMSProvider)
}
