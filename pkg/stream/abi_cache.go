package stream

import (
	"container/list"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// DefaultABICacheTTL is the default time-to-live for cached contract specs (24 hours).
const DefaultABICacheTTL = 24 * time.Hour

// DefaultABICacheCapacity is the default in-memory LRU cache capacity.
const DefaultABICacheCapacity = 1000

// CachedSpec holds a contract's parsed spec entries and metadata.
type CachedSpec struct {
	ContractID string            `json:"contract_id"`
	WasmHash   string            `json:"wasm_hash"`
	EntriesXDR []string          `json:"entries_xdr"`
	UpdatedAt  time.Time         `json:"updated_at"`
	entries    []xdr.ScSpecEntry `json:"-"`
}

// Entries returns the decoded ScSpecEntry structures.
func (c *CachedSpec) Entries() []xdr.ScSpecEntry {
	if len(c.entries) > 0 || len(c.EntriesXDR) == 0 {
		return c.entries
	}
	var entries []xdr.ScSpecEntry
	for _, raw := range c.EntriesXDR {
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			continue
		}
		var entry xdr.ScSpecEntry
		if err := entry.UnmarshalBinary(b); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	c.entries = entries
	return c.entries
}

// ABICacheOptions configures an ABICache.
type ABICacheOptions struct {
	// Dir is the directory for on-disk persistence (e.g. ~/.cache/prism/specs).
	// If empty, the cache operates strictly in-memory.
	Dir string
	// TTL is how long cached entries remain valid (default: 24h).
	TTL time.Duration
	// Capacity is the max in-memory LRU items (default: 1000).
	Capacity int
	// Now is the time provider for testing (default: time.Now).
	Now func() time.Time
}

type lruItem struct {
	key   string
	value *CachedSpec
}

// ABICache is a thread-safe LRU in-memory and on-disk cache for contract specs.
type ABICache struct {
	mu        sync.RWMutex
	dir       string
	ttl       time.Duration
	capacity  int
	now       func() time.Time
	items     map[string]*list.Element
	evictList *list.List
}

// NewABICache creates a new contract spec cache with in-memory LRU and optional disk persistence.
func NewABICache(opts ABICacheOptions) (*ABICache, error) {
	if opts.TTL <= 0 {
		opts.TTL = DefaultABICacheTTL
	}
	if opts.Capacity <= 0 {
		opts.Capacity = DefaultABICacheCapacity
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	dir := opts.Dir
	if dir != "" {
		if strings.HasPrefix(dir, "~/") || dir == "~" {
			home, err := os.UserHomeDir()
			if err == nil {
				if dir == "~" {
					dir = home
				} else {
					dir = filepath.Join(home, dir[2:])
				}
			}
		}
		absDir, err := filepath.Abs(dir)
		if err != nil {
			return nil, fmt.Errorf("resolving cache dir %s: %w", dir, err)
		}
		if err := os.MkdirAll(absDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating cache dir %s: %w", absDir, err)
		}
		dir = absDir
	}

	return &ABICache{
		dir:       dir,
		ttl:       opts.TTL,
		capacity:  opts.Capacity,
		now:       opts.Now,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}, nil
}

// Get returns the cached spec for a contract ID if present and not expired.
func (c *ABICache) Get(contractID string) (*CachedSpec, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Check in-memory LRU
	if el, ok := c.items[contractID]; ok {
		item, _ := el.Value.(*lruItem) //nolint:errcheck // type is guaranteed by putMemory
		if c.now().Sub(item.value.UpdatedAt) > c.ttl {
			// Expired
			c.removeElement(el)
			if c.dir != "" {
				_ = os.Remove(c.filePath(contractID))
			}
			return nil, false
		}
		c.evictList.MoveToFront(el)
		return item.value, true
	}

	// 2. Check on-disk persistence
	if c.dir == "" {
		return nil, false
	}

	path := c.filePath(contractID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	var spec CachedSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		_ = os.Remove(path)
		return nil, false
	}

	if c.now().Sub(spec.UpdatedAt) > c.ttl {
		// Disk entry expired
		_ = os.Remove(path)
		return nil, false
	}

	// Inflate decoded entries
	_ = spec.Entries()

	// Add to in-memory LRU
	c.putMemory(contractID, &spec)
	return &spec, true
}

// Put stores a contract's parsed spec in the in-memory LRU and on disk.
func (c *ABICache) Put(contractID string, wasmHash xdr.Hash, entries []xdr.ScSpecEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entriesXDR := make([]string, len(entries))
	for i, e := range entries {
		b, err := e.MarshalBinary()
		if err == nil {
			entriesXDR[i] = base64.StdEncoding.EncodeToString(b)
		}
	}

	spec := &CachedSpec{
		ContractID: contractID,
		WasmHash:   hex.EncodeToString(wasmHash[:]),
		EntriesXDR: entriesXDR,
		UpdatedAt:  c.now(),
		entries:    entries,
	}

	c.putMemory(contractID, spec)

	if c.dir != "" {
		c.saveToDisk(contractID, spec)
	}
}

// Invalidate removes a contract spec from both memory and disk.
func (c *ABICache) Invalidate(contractID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[contractID]; ok {
		c.removeElement(el)
	}
	if c.dir != "" {
		_ = os.Remove(c.filePath(contractID))
	}
}

// CheckAndInvalidate invalidates the cache entry if the current on-chain WASM hash
// does not match the cached hash. Returns true if invalidated.
func (c *ABICache) CheckAndInvalidate(contractID string, currentWasmHash xdr.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	var cachedHash string
	if el, ok := c.items[contractID]; ok {
		item, _ := el.Value.(*lruItem) //nolint:errcheck // type is guaranteed by putMemory
		cachedHash = item.value.WasmHash
	} else if c.dir != "" {
		data, err := os.ReadFile(c.filePath(contractID))
		if err == nil {
			var spec CachedSpec
			if json.Unmarshal(data, &spec) == nil {
				cachedHash = spec.WasmHash
			}
		}
	}

	if cachedHash == "" {
		return false
	}

	expected := hex.EncodeToString(currentWasmHash[:])
	if cachedHash != expected {
		if el, ok := c.items[contractID]; ok {
			c.removeElement(el)
		}
		if c.dir != "" {
			_ = os.Remove(c.filePath(contractID))
		}
		return true
	}
	return false
}

// Len returns the current number of in-memory cached entries.
func (c *ABICache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all cached items from memory and disk.
func (c *ABICache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.items {
		delete(c.items, k)
		if c.dir != "" {
			_ = os.Remove(c.filePath(k))
		}
	}
	c.evictList.Init()
}

func (c *ABICache) putMemory(key string, val *CachedSpec) {
	if el, ok := c.items[key]; ok {
		c.evictList.MoveToFront(el)
		item, _ := el.Value.(*lruItem) //nolint:errcheck // type is guaranteed by putMemory
		item.value = val
		return
	}

	item := &lruItem{key: key, value: val}
	el := c.evictList.PushFront(item)
	c.items[key] = el

	if c.evictList.Len() > c.capacity {
		c.evictOldest()
	}
}

func (c *ABICache) evictOldest() {
	el := c.evictList.Back()
	if el != nil {
		c.removeElement(el)
	}
}

func (c *ABICache) removeElement(el *list.Element) {
	c.evictList.Remove(el)
	item, _ := el.Value.(*lruItem) //nolint:errcheck // type is guaranteed by putMemory
	delete(c.items, item.key)
}

func (c *ABICache) filePath(contractID string) string {
	return filepath.Join(c.dir, contractID+".json")
}

func (c *ABICache) saveToDisk(contractID string, spec *CachedSpec) {
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return
	}
	tmp := c.filePath(contractID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, c.filePath(contractID)); err != nil {
		_ = os.Remove(tmp)
	}
}
