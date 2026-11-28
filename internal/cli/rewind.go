package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// rewindSignal asks a running stream to step back.
//
// SIGUSR2 is used because SIGUSR1 already belongs to the stack-dump
// handler, and a rewind is a request rather than an emergency.
const rewindSignal = syscall.SIGUSR2

// notifyRewindSignals starts delivering rewind signals.
//
// The caller owns the returned channel and must stop it with signal.Stop,
// which the watcher does when it is closed.
func notifyRewindSignals() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, rewindSignal)
	return signals
}

// rewindWatcher turns signals into rewind requests.
//
// It owns exactly one goroutine and nothing else: Close stops the signal
// delivery and waits for that goroutine to return, so a finished command
// never leaves a goroutine behind.
type rewindWatcher struct {
	signals chan os.Signal
	stop    chan struct{}
	done    chan struct{}
	handled chan struct{}
}

// newRewindWatcher starts a watcher that requests a rewind every time a
// signal arrives.
//
// The watcher stops when the context is cancelled or when it is closed.
// Applying the rewind is the tailer's job; the watcher only asks.
func newRewindWatcher(
	ctx context.Context,
	signals chan os.Signal,
	controller *stream.RewindController,
	distance stream.RewindDistance,
	stderr io.Writer,
) *rewindWatcher {
	w := &rewindWatcher{
		signals: signals,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		handled: make(chan struct{}, 1),
	}

	go func() {
		defer close(w.done)
		for {
			select {
			case <-w.stop:
				return
			case <-ctx.Done():
				return
			case <-signals:
				if err := controller.Request(stream.RewindRequest{Distance: distance}); err != nil {
					fmt.Fprintf(stderr, "prism: %v\n", err)
					continue
				}
				w.notify()
			}
		}
	}()

	return w
}

// notify reports that a signal has been turned into a rewind request.
func (w *rewindWatcher) notify() {
	select {
	case w.handled <- struct{}{}:
	default:
	}
}

// Handled returns a channel that receives a token after each signal has been
// turned into a rewind request, so a caller can observe that a request was
// made.
func (w *rewindWatcher) Handled() <-chan struct{} { return w.handled }

// Close stops the watcher and waits for its goroutine to finish.
func (w *rewindWatcher) Close() {
	signal.Stop(w.signals)
	close(w.stop)
	<-w.done
}

// newRewindCmd rewinds a state-file cursor so a stream can replay records.
func newRewindCmd(stdout io.Writer) *cobra.Command {
	var path string
	var by string
	cmd := &cobra.Command{
		Use:   "rewind",
		Short: "Rewind a state file so a stream can replay records",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if path == "" {
				return fmt.Errorf("--state-file is required")
			}
			distance, err := stream.ParseRewind(by)
			if err != nil {
				return err
			}
			if err := stream.RewindCursor(path, distance.LedgerCount, distance.Duration); err != nil {
				return err
			}
			streamID, err := rewindStreamID(path, streamID)
			if err != nil {
				return err
			}
			state, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{
				StreamID:  streamID,
				Retention: stream.DefaultStateRetention,
			})
			if err != nil {
				return err
			}
			current, err := state.Load()
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(stdout, "rewound state to ledger %d\n", current.Ledger)
			return err
		},
	}
	cmd.Flags().StringVar(&path, "state-file", "", "state file to rewind")
	cmd.Flags().StringVar(&by, "by", "", "rewind distance in ledgers or time (for example 1000 or 2h)")
	return cmd
}

// rewindStreamID picks the partition to rewind. An explicit --stream-id is
// used as given. Otherwise a single-stream file needs none, and a shared file
// holding exactly one stream rewinds that one; a shared file holding several
// is ambiguous, and the error lists them so the operator can choose.
func rewindStreamID(path, explicit string) (string, error) {
	ids, partitioned, err := stream.StateFileStreams(path)
	if err != nil {
		return "", err
	}
	if explicit != "" {
		if !partitioned && ids == nil {
			// A single-stream file: the stream ID selects the file as a
			// whole once it is upgraded, so accept it.
			return explicit, nil
		}
		for _, id := range ids {
			if id == explicit {
				return explicit, nil
			}
		}
		return "", fmt.Errorf("state file %s has no stream %q; it holds: %s", path, explicit, strings.Join(ids, ", "))
	}
	if !partitioned {
		return "", nil
	}
	switch len(ids) {
	case 0:
		return "", fmt.Errorf("cannot rewind an empty state file")
	case 1:
		return ids[0], nil
	default:
		return "", fmt.Errorf("state file %s is shared by %d streams; pass --stream-id with one of: %s",
			path, len(ids), strings.Join(ids, ", "))
	}
}
