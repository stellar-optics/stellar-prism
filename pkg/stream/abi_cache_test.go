package stream

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func makeTestSpecEntry(name string) xdr.ScSpecEntry {
	return xdr.ScSpecEntry{
		Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		FunctionV0: &xdr.ScSpecFunctionV0{
			Name: xdr.ScSymbol(name),
			Outputs: []xdr.ScSpecTypeDef{
				{Type: xdr.ScSpecTypeScSpecTypeU32},
			},
		},
	}
}

func TestABICacheBasicGetPut(t *testing.T) {
	t.Parallel()

	cache, err := NewABICache(ABICacheOptions{})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"
	wasmHash := xdr.Hash(sha256.Sum256([]byte("dummy wasm")))
	entries := []xdr.ScSpecEntry{makeTestSpecEntry("foo"), makeTestSpecEntry("bar")}

	// Initial get is a miss
	if _, ok := cache.Get(contractID); ok {
		t.Fatal("expected cache miss on empty cache")
	}

	cache.Put(contractID, wasmHash, entries)

	if cache.Len() != 1 {
		t.Fatalf("cache.Len() = %d, want 1", cache.Len())
	}

	cached, ok := cache.Get(contractID)
	if !ok {
		t.Fatal("expected cache hit after Put")
	}
	if cached.ContractID != contractID {
		t.Errorf("cached.ContractID = %s, want %s", cached.ContractID, contractID)
	}
	gotEntries := cached.Entries()
	if len(gotEntries) != 2 {
		t.Fatalf("len(Entries()) = %d, want 2", len(gotEntries))
	}
	if string(gotEntries[0].FunctionV0.Name) != "foo" || string(gotEntries[1].FunctionV0.Name) != "bar" {
		t.Errorf("unexpected entry names: %v, %v", gotEntries[0].FunctionV0.Name, gotEntries[1].FunctionV0.Name)
	}
}

func TestABICacheLRUEviction(t *testing.T) {
	t.Parallel()

	cache, err := NewABICache(ABICacheOptions{
		Capacity: 2,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	hash := xdr.Hash{1}
	cache.Put("c1", hash, []xdr.ScSpecEntry{makeTestSpecEntry("fn1")})
	cache.Put("c2", hash, []xdr.ScSpecEntry{makeTestSpecEntry("fn2")})
	if cache.Len() != 2 {
		t.Fatalf("cache.Len() = %d, want 2", cache.Len())
	}

	// Access c1 so c2 becomes the oldest
	if _, ok := cache.Get("c1"); !ok {
		t.Fatal("expected c1 hit")
	}

	// Put c3, which should evict c2
	cache.Put("c3", hash, []xdr.ScSpecEntry{makeTestSpecEntry("fn3")})
	if cache.Len() != 2 {
		t.Fatalf("cache.Len() = %d, want 2", cache.Len())
	}

	if _, ok := cache.Get("c2"); ok {
		t.Fatal("c2 should have been evicted by LRU")
	}
	if _, ok := cache.Get("c1"); !ok {
		t.Fatal("c1 should still be in cache")
	}
	if _, ok := cache.Get("c3"); !ok {
		t.Fatal("c3 should be in cache")
	}
}

func TestABICacheTTLExpiration(t *testing.T) {
	t.Parallel()

	currentTime := time.Now()
	nowFn := func() time.Time { return currentTime }

	cache, err := NewABICache(ABICacheOptions{
		TTL: 1 * time.Hour,
		Now: nowFn,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	contractID := "C_TTL_TEST"
	hash := xdr.Hash{2}
	cache.Put(contractID, hash, []xdr.ScSpecEntry{makeTestSpecEntry("test")})

	// Within TTL
	if _, ok := cache.Get(contractID); !ok {
		t.Fatal("expected cache hit within TTL")
	}

	// Advance time past TTL
	currentTime = currentTime.Add(2 * time.Hour)

	if _, ok := cache.Get(contractID); ok {
		t.Fatal("expected cache miss and eviction after TTL expiration")
	}
	if cache.Len() != 0 {
		t.Fatalf("cache.Len() = %d, want 0 after TTL eviction", cache.Len())
	}
}

func TestABICacheDiskPersistence(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	cache1, err := NewABICache(ABICacheOptions{
		Dir: tmpDir,
		TTL: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	contractID := "C_DISK_TEST"
	hash := xdr.Hash{3, 4, 5}
	entries := []xdr.ScSpecEntry{makeTestSpecEntry("disk_fn")}

	cache1.Put(contractID, hash, entries)

	// Verify file was written to disk
	expectedFile := filepath.Join(tmpDir, contractID+".json")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Fatalf("expected cache file on disk at %s", expectedFile)
	}

	// Create second cache pointing to the same directory
	cache2, err := NewABICache(ABICacheOptions{
		Dir: tmpDir,
		TTL: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	// Initially in-memory LRU of cache2 is empty
	if cache2.Len() != 0 {
		t.Fatalf("cache2.Len() = %d, want 0", cache2.Len())
	}

	// Get should hit disk and populate cache2's in-memory LRU
	cached, ok := cache2.Get(contractID)
	if !ok {
		t.Fatal("expected cache hit from disk")
	}
	if cache2.Len() != 1 {
		t.Fatalf("cache2.Len() = %d, want 1 after disk load", cache2.Len())
	}
	loadedEntries := cached.Entries()
	if len(loadedEntries) != 1 || string(loadedEntries[0].FunctionV0.Name) != "disk_fn" {
		t.Fatalf("unexpected loaded entries: %v", loadedEntries)
	}
}

func TestABICacheInvalidate(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cache, err := NewABICache(ABICacheOptions{
		Dir: tmpDir,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	contractID := "C_INVAL"
	cache.Put(contractID, xdr.Hash{9}, []xdr.ScSpecEntry{makeTestSpecEntry("inval")})

	cache.Invalidate(contractID)

	if _, ok := cache.Get(contractID); ok {
		t.Fatal("expected cache miss after Invalidate")
	}

	expectedFile := filepath.Join(tmpDir, contractID+".json")
	if _, err := os.Stat(expectedFile); !os.IsNotExist(err) {
		t.Fatalf("cache file %s should have been removed on Invalidate", expectedFile)
	}
}

func TestABICacheCheckAndInvalidate(t *testing.T) {
	t.Parallel()

	cache, err := NewABICache(ABICacheOptions{})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	contractID := "C_CHECK"
	origHash := xdr.Hash{10, 11}
	newHash := xdr.Hash{10, 12}

	cache.Put(contractID, origHash, []xdr.ScSpecEntry{makeTestSpecEntry("check")})

	// Matching hash: does not invalidate
	if invalidated := cache.CheckAndInvalidate(contractID, origHash); invalidated {
		t.Fatal("CheckAndInvalidate returned true for matching hash")
	}
	if _, ok := cache.Get(contractID); !ok {
		t.Fatal("entry should still be cached")
	}

	// Changed hash: invalidates
	if invalidated := cache.CheckAndInvalidate(contractID, newHash); !invalidated {
		t.Fatal("CheckAndInvalidate returned false for different hash")
	}
	if _, ok := cache.Get(contractID); ok {
		t.Fatal("entry should have been invalidated")
	}
}

func TestABICacheConcurrency(t *testing.T) {
	t.Parallel()

	cache, err := NewABICache(ABICacheOptions{
		Capacity: 50,
	})
	if err != nil {
		t.Fatalf("NewABICache() error = %v", err)
	}

	const goroutines = 10
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				contractID := fmt.Sprintf("C_%d_%d", gid, i%10)
				hash := xdr.Hash{byte(gid), byte(i)}
				cache.Put(contractID, hash, []xdr.ScSpecEntry{makeTestSpecEntry("concurrent")})
				_, _ = cache.Get(contractID)
				_ = cache.Len()
				if i%5 == 0 {
					cache.Invalidate(contractID)
				}
			}
		}(g)
	}

	wg.Wait()
}
