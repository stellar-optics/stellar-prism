package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// cursorState owns the optional cursor metadata file and the background
// pruner that keeps it bounded.
//
// The file is a journal of checkpoints: one line per emitted page. Left
// alone it grows for as long as the stream runs, which is why the pruner
// exists — the metadata has to be compacted while the stream is still going,
// not only when it stops.
type cursorState struct {
	store  stream.MetadataStore
	pruner *stream.Pruner
	key    string
	cursor string
}

// stateKey builds the key that identifies a stream inside the cursor
// metadata file.
//
// Two runs share a key only when they watch the same thing, so a recorded
// cursor is never applied to a different set of filters. Empty parts are
// dropped, which keeps the key stable whether or not a filter was given.
func stateKey(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "|")
}

// filterPart renders one filter of a state key. An unused filter contributes
// nothing, so a stream without filters is keyed by its name alone.
func filterPart(name string, values []string) string {
	if len(values) == 0 {
		return ""
	}
	return name + "=" + strings.Join(values, ",")
}

// eventsStateKey identifies an event stream by its filters.
func eventsStateKey(contractIDs, topics, eventTypes []string) string {
	return stateKey("events",
		filterPart("contract", contractIDs),
		filterPart("topic", topics),
		filterPart("type", eventTypes))
}

// openCursorState prepares the cursor metadata file, if one was requested,
// and resolves the position a resumed run starts from.
//
// It returns nil when no --state-file was given, which every caller reads as
// "no metadata and no pruning".
//
// The file is encrypted when a key was configured: the passphrase or the key
// management service decides how the checkpoints are sealed, and the key
// itself never reaches the disk.
func isJournalState(sf *streamFlags) bool {
	return sf.resume ||
		sf.pruneMaxAge > 0 ||
		sf.statePassphraseEnv != "" ||
		sf.statePassphraseFile != "" ||
		sf.stateKMSProvider != "" ||
		sf.stateKMSKeyID != "" ||
		sf.haLeaseFile != "" ||
		strings.HasSuffix(sf.stateFile, ".jsonl")
}

func openCursorState(ctx context.Context, sf *streamFlags, key string, stderr io.Writer) (*cursorState, error) {
	if sf.stateFile == "" || !isJournalState(sf) {
		return nil, nil
	}
	if sf.state.streamID != "" {
		// An explicit --stream-id names the stream in the journal too, so
		// processes sharing one journal keep their checkpoints apart.
		key = sf.state.streamID
	}

	source, err := stateKeySource(sf)
	if err != nil {
		return nil, err
	}

	var store stream.MetadataStore
	if source != nil {
		encrypted, err := stream.NewEncryptedStateStore(sf.stateFile, source)
		if err != nil {
			return nil, err
		}
		store = encrypted
	} else {
		plain, err := stream.NewSharedFileMetadataStore(sf.stateFile)
		if err != nil {
			return nil, err
		}
		store = plain
	}
	state := &cursorState{store: store, key: key}

	if sf.resume {
		meta, found, err := store.Latest(ctx, key)
		if err != nil {
			return nil, err
		}
		if found && meta.Cursor != "" {
			state.cursor = meta.Cursor
			fmt.Fprintf(stderr, "prism: resuming %s from the cursor recorded at ledger %d\n",
				key, meta.Ledger)
		}
	}

	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: sf.pruneInterval,
		Keep:     sf.pruneKeep,
		MaxAge:   sf.pruneMaxAge,
		OnPrune: func(stats stream.PruneStats) {
			if stats.Removed == 0 {
				return
			}
			fmt.Fprintf(stderr, "prism: pruned %d cursor metadata entries, %d kept\n",
				stats.Removed, stats.Kept)
		},
		OnError: func(err error) {
			fmt.Fprintf(stderr, "prism: pruning cursor metadata: %v\n", err)
		},
	})
	if err != nil {
		return nil, err
	}
	state.pruner = pruner
	if err := pruner.Start(ctx); err != nil {
		return nil, err
	}
	return state, nil
}

// record appends a checkpoint for the position the stream just reached.
//
// A failure here is reported rather than fatal: the stream is still
// delivering records, and a journal that lags behind only means a later run
// resumes a little further back than it could have.
//
// The context is stripped of its cancellation, because the most important
// checkpoint of all is the last one: it is written while the process is
// shutting down, and a checkpoint that is dropped there is exactly the one
// the next run needs.
func (s *cursorState) record(ctx context.Context, page stream.Page) error {
	if s == nil || s.store == nil {
		return nil
	}
	if page.Cursor == "" {
		// The source offered no position, so there is nothing to resume
		// from. Recording an empty cursor would only push a usable
		// checkpoint out of reach.
		return nil
	}
	return s.store.Append(context.WithoutCancel(ctx), stream.CursorMetadata{
		Key:        s.key,
		Cursor:     page.Cursor,
		Ledger:     page.Ledger,
		RecordedAt: time.Now().UTC(),
	})
}

// close stops the background pruner and compacts the file one last time, so
// a short run does not leave a journal behind for the next one to inherit.
func (s *cursorState) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.pruner != nil {
		if err := s.pruner.Close(); err != nil {
			errs = append(errs, err)
		}
		if _, err := s.pruner.PruneOnce(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
