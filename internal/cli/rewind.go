package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

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
