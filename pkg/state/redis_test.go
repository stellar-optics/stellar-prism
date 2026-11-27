package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type mockRedisClient struct {
	mu   sync.Mutex
	data map[string]string
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{data: make(map[string]string)}
}

func (m *mockRedisClient) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return "", ErrRedisNil
	}
	return v, nil
}

func (m *mockRedisClient) Set(_ context.Context, key string, value any, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = fmt.Sprintf("%v", value)
	return nil
}

func (m *mockRedisClient) SetNX(_ context.Context, key string, value any, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[key]; exists {
		return false, nil
	}
	m.data[key] = fmt.Sprintf("%v", value)
	return true, nil
}

func (m *mockRedisClient) Eval(_ context.Context, _ string, _ []string, _ ...any) (any, error) {
	return nil, nil
}

func (m *mockRedisClient) Del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *mockRedisClient) Close() error { return nil }

func TestRedisMetadataStore(t *testing.T) {
	ctx := context.Background()
	client := newMockRedisClient()
	store, err := NewRedisMetadataStore(client, "prism:cursor:events")
	if err != nil {
		t.Fatalf("NewRedisMetadataStore: %v", err)
	}

	if _, found, err := store.Latest(ctx, "events"); err != nil || found {
		t.Fatalf("Latest on empty store = (%v, %v), want (false, nil)", found, err)
	}

	now := time.Now().UTC()
	for i := 1; i <= 3; i++ {
		err := store.Append(ctx, stream.CursorMetadata{
			Key:        "events",
			Cursor:     fmt.Sprintf("c-%d", i),
			Ledger:     uint32(i * 10),
			RecordedAt: now.Add(time.Duration(i) * time.Minute),
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	latest, found, err := store.Latest(ctx, "events")
	if err != nil || !found || latest.Cursor != "c-3" {
		t.Fatalf("Latest = (%+v, %v, %v), want c-3", latest, found, err)
	}

	stats, err := store.Prune(ctx, stream.PrunePolicy{Keep: 2})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if stats.Removed != 1 || stats.Kept != 2 {
		t.Fatalf("Prune stats = %+v, want Removed=1 Kept=2", stats)
	}

	entries, err := store.Entries(ctx, "events")
	if err != nil || len(entries) != 2 || entries[0].Cursor != "c-2" {
		t.Fatalf("Entries after prune = (%+v, %v), want [c-2, c-3]", entries, err)
	}
}

func TestRedisStateStore(t *testing.T) {
	client := newMockRedisClient()
	store, err := NewRedisStateStore(client, "prism:state:events")
	if err != nil {
		t.Fatalf("NewRedisStateStore: %v", err)
	}

	if err := store.Save(stream.State{}); err == nil {
		t.Fatal("Save without Source should fail")
	}

	want := stream.State{
		Source:      "events",
		Ledger:      42,
		PagingToken: "tok-42",
	}
	if err := store.SaveContext(context.Background(), want); err != nil {
		t.Fatalf("SaveContext: %v", err)
	}

	got, err := store.LoadContext(context.Background())
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	if got.Source != want.Source || got.Ledger != want.Ledger || got.PagingToken != want.PagingToken {
		t.Fatalf("LoadContext = %+v, want %+v", got, want)
	}
}

func TestRedisCursorStore(t *testing.T) {
	ctx := context.Background()
	client := newMockRedisClient()
	store, err := NewRedisCursorStore(client, "prism:cursor:default")
	if err != nil {
		t.Fatalf("NewRedisCursorStore: %v", err)
	}

	if _, err := store.Load(ctx, "missing"); !errors.Is(err, stream.ErrNoCursor) {
		t.Fatalf("Load missing = %v, want ErrNoCursor", err)
	}

	want := stream.CursorState{Cursor: "cur-9", LastID: "0009"}
	if err := store.Save(ctx, "events/a", want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := store.Load(ctx, "events/a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Cursor != want.Cursor || got.LastID != want.LastID {
		t.Fatalf("Load = %+v, want %+v", got, want)
	}
}

func TestRedisLeaseAndClientURL(t *testing.T) {
	ctx := context.Background()
	client := newMockRedisClient()
	lease, err := NewRedisLeaseWithTTL(client, "prism:lease:events", 10*time.Second, "node-1")
	if err != nil {
		t.Fatalf("NewRedisLeaseWithTTL: %v", err)
	}

	ok, err := lease.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("Acquire = (%v, %v), want (true, nil)", ok, err)
	}
	ok, err = lease.Acquire(ctx)
	if err != nil || ok {
		t.Fatalf("second Acquire = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := lease.Renew(ctx); err != nil || !ok {
		t.Fatalf("Renew = (%v, %v), want (true, nil)", ok, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if _, err := NewRedisClient("http://localhost:6379"); err == nil {
		t.Fatal("NewRedisClient with http:// should fail")
	}
	c, err := NewRedisClient("redis://user:pass@localhost:6379/2")
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
