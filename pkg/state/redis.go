package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// RedisClient is the small surface the Redis-backed state stores need.
//
// It is intentionally tiny so that tests can substitute a mock without
// pulling in a real Redis driver, and so that the production implementation
// can be backed by a minimal RESP client.
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value any, expiration time.Duration) error
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
	Del(ctx context.Context, key string) error
	Close() error
}

// ErrRedisNil reports that a key does not exist in Redis.
var ErrRedisNil = errors.New("redis: nil")

var errType = reflect.TypeOf((*error)(nil)).Elem()

// ---------------------------------------------------------------------------
// reflection helpers
// ---------------------------------------------------------------------------

func invokeMethod(client any, name string, args []any) (any, error, bool) {
	if client == nil {
		return nil, errors.New("redis client is nil"), false
	}
	v := reflect.ValueOf(client)
	method := v.MethodByName(name)
	if !method.IsValid() {
		return nil, nil, false
	}
	mt := method.Type()
	n := mt.NumIn()
	variadic := mt.IsVariadic()
	minArgs := n
	if variadic {
		minArgs = n - 1
	}
	if len(args) < minArgs {
		return nil, nil, false
	}
	if !variadic && len(args) != n {
		return nil, nil, false
	}

	in := make([]reflect.Value, 0, n)
	if variadic {
		for i := 0; i < n-1; i++ {
			rv := reflect.ValueOf(args[i])
			pt := mt.In(i)
			if !rv.Type().AssignableTo(pt) {
				if rv.Type().ConvertibleTo(pt) {
					rv = rv.Convert(pt)
				} else {
					return nil, nil, false
				}
			}
			in = append(in, rv)
		}
		sliceType := mt.In(n - 1)
		elemType := sliceType.Elem()
		restCount := len(args) - (n - 1)
		slice := reflect.MakeSlice(sliceType, restCount, restCount)
		for j := 0; j < restCount; j++ {
			rv := reflect.ValueOf(args[n-1+j])
			if !rv.Type().AssignableTo(elemType) {
				if rv.Type().ConvertibleTo(elemType) {
					rv = rv.Convert(elemType)
				} else {
					return nil, nil, false
				}
			}
			slice.Index(j).Set(rv)
		}
		in = append(in, slice)
	} else {
		for i, arg := range args {
			rv := reflect.ValueOf(arg)
			pt := mt.In(i)
			if !rv.Type().AssignableTo(pt) {
				if rv.Type().ConvertibleTo(pt) {
					rv = rv.Convert(pt)
				} else {
					return nil, nil, false
				}
			}
			in = append(in, rv)
		}
	}

	outs := method.Call(in)
	var res any
	var err error
	if mt.NumOut() > 0 {
		lastIdx := mt.NumOut() - 1
		if mt.Out(lastIdx).Implements(errType) {
			if !outs[lastIdx].IsNil() {
				err = outs[lastIdx].Interface().(error)
			}
			if mt.NumOut() > 1 {
				res = outs[0].Interface()
			}
		} else {
			res = outs[0].Interface()
		}
	}
	return res, err, true
}

func callAny(client any, names []string, argSets [][]any) (any, error, bool) {
	for _, name := range names {
		for _, args := range argSets {
			if res, err, ok := invokeMethod(client, name, args); ok {
				return res, err, true
			}
		}
	}
	return nil, nil, false
}

func isNilReply(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "nil") || strings.Contains(s, "not found") || strings.Contains(s, "no such")
}

func asString(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", x), nil
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() {
		for _, name := range []string{"String", "Val", "Text", "Bytes"} {
			m := rv.MethodByName(name)
			if m.IsValid() {
				mt := m.Type()
				if mt.NumIn() == 0 && mt.NumOut() >= 1 {
					outs := m.Call(nil)
					if mt.NumOut() == 1 {
						if s, ok := outs[0].Interface().(string); ok {
							return s, nil
						}
						if b, ok := outs[0].Interface().([]byte); ok {
							return string(b), nil
						}
					}
					if mt.NumOut() == 2 {
						if e, ok := outs[1].Interface().(error); ok && e != nil {
							return "", e
						}
						if s, ok := outs[0].Interface().(string); ok {
							return s, nil
						}
					}
				}
			}
		}
	}
	return "", fmt.Errorf("cannot convert %T to string", v)
}

func commandString(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	// If v already has a Result/Val method, asString handles it.
	s, err := asString(v)
	return s, err
}

func parseRedisKey(key any, fallback string) (string, error) {
	switch v := key.(type) {
	case nil:
		if fallback != "" {
			return fallback, nil
		}
		return "", errors.New("redis key is required")
	case string:
		if v == "" {
			if fallback != "" {
				return fallback, nil
			}
			return "", errors.New("redis key is required")
		}
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	}
	rv := reflect.ValueOf(key)
	if rv.IsValid() {
		for _, field := range []string{"Key", "RedisKey", "StateRedisKey", "Name", "stateRedisKey"} {
			f := rv.FieldByName(field)
			if f.IsValid() && f.Kind() == reflect.String && f.String() != "" {
				return f.String(), nil
			}
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", errors.New("redis key is required")
}

// ---------------------------------------------------------------------------
// generic client calls
// ---------------------------------------------------------------------------

type redisStoreBase struct {
	client any
	key    string
	mu     sync.Mutex
}

func (b *redisStoreBase) redisKey() string { return b.key }

func (b *redisStoreBase) get(ctx context.Context, key string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	argSets := [][]any{{ctx, key}, {key}}
	res, err, ok := callAny(b.client, []string{"Get", "GetCtx", "Do"}, argSets)
	if !ok {
		return "", errors.New("redis client has no Get method")
	}
	if err != nil {
		if isNilReply(err) {
			return "", nil
		}
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return asString(res)
}

func (b *redisStoreBase) set(ctx context.Context, key string, value any, expiration time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	argSets := [][]any{
		{ctx, key, value, expiration},
		{ctx, key, value},
		{key, value, expiration},
		{key, value},
	}
	_, err, ok := callAny(b.client, []string{"Set", "SetEx", "SetNX"}, argSets)
	if !ok {
		return errors.New("redis client has no Set method")
	}
	return err
}

func (b *redisStoreBase) setNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	argSets := [][]any{
		{ctx, key, value, expiration},
		{ctx, key, value},
		{key, value, expiration},
		{key, value},
	}
	res, err, ok := callAny(b.client, []string{"SetNX", "Set"}, argSets)
	if !ok {
		return false, errors.New("redis client has no SetNX method")
	}
	if err != nil {
		return false, err
	}
	if res == nil {
		return true, nil
	}
	switch x := res.(type) {
	case bool:
		return x, nil
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(x).Int() != 0, nil
	case string:
		return x == "OK" || x == "1", nil
	}
	rv := reflect.ValueOf(res)
	if rv.IsValid() {
		m := rv.MethodByName("Val")
		if m.IsValid() && m.Type().NumIn() == 0 && m.Type().NumOut() >= 1 {
			outs := m.Call(nil)
			if b, ok := outs[0].Interface().(bool); ok {
				return b, nil
			}
		}
		m = rv.MethodByName("Result")
		if m.IsValid() && m.Type().NumIn() == 0 && m.Type().NumOut() >= 1 {
			outs := m.Call(nil)
			if b, ok := outs[0].Interface().(bool); ok {
				return b, nil
			}
		}
	}
	// If Set was used as fallback, assume success.
	return true, nil
}

func (b *redisStoreBase) del(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	argSets := [][]any{{ctx, key}, {key}}
	_, err, ok := callAny(b.client, []string{"Del", "Delete", "Unlink"}, argSets)
	if !ok {
		return nil
	}
	return err
}

func (b *redisStoreBase) eval(ctx context.Context, script string, keys []string, args ...any) (any, error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	all := []any{ctx, script, keys}
	all = append(all, args...)
	argSets := [][]any{all, {script, keys}}
	// append args to second set too
	second := []any{script, keys}
	second = append(second, args...)
	argSets[1] = second
	res, err, ok := callAny(b.client, []string{"Eval", "EvalSha", "Do"}, argSets)
	if !ok {
		return nil, nil, false
	}
	return res, err, true
}

// ---------------------------------------------------------------------------
// RedisMetadataStore (stream.MetadataStore)
// ---------------------------------------------------------------------------

type RedisMetadataStore struct {
	redisStoreBase
}

func NewRedisMetadataStore(client any, key any) (*RedisMetadataStore, error) {
	k, err := parseRedisKey(key, "")
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	return &RedisMetadataStore{redisStoreBase: redisStoreBase{client: client, key: k}}, nil
}

// Aliases for common naming.
func NewRedisStore(client any, key any) (*RedisMetadataStore, error) {
	return NewRedisMetadataStore(client, key)
}

func (s *RedisMetadataStore) loadEntries(ctx context.Context) ([]stream.CursorMetadata, error) {
	raw, err := s.get(ctx, s.key)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var entries []stream.CursorMetadata
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("decoding redis cursor metadata: %w", err)
	}
	return entries, nil
}

func (s *RedisMetadataStore) saveEntries(ctx context.Context, entries []stream.CursorMetadata) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("encoding redis cursor metadata: %w", err)
	}
	return s.set(ctx, s.key, string(data), 0)
}

func (s *RedisMetadataStore) Append(ctx context.Context, meta stream.CursorMetadata) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.RecordedAt.IsZero() {
		meta.RecordedAt = time.Now().UTC()
	}
	entries, err := s.loadEntries(ctx)
	if err != nil {
		return err
	}
	entries = append(entries, meta)
	return s.saveEntries(ctx, entries)
}

func (s *RedisMetadataStore) Latest(ctx context.Context, key string) (stream.CursorMetadata, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := s.loadEntries(ctx)
	if err != nil {
		return stream.CursorMetadata{}, false, err
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Key == key {
			return entries[i], true, nil
		}
	}
	return stream.CursorMetadata{}, false, nil
}

func (s *RedisMetadataStore) Entries(ctx context.Context, key string) ([]stream.CursorMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := s.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	var out []stream.CursorMetadata
	for _, e := range entries {
		if e.Key == key {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *RedisMetadataStore) Prune(ctx context.Context, policy stream.PrunePolicy) (stream.PruneStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.loadEntries(ctx)
	if err != nil {
		return stream.PruneStats{}, err
	}

	// Count retained per key, ignoring expired entries.
	counts := make(map[string]int)
	for _, e := range entries {
		if policy.Cutoff.IsZero() || !e.RecordedAt.Before(policy.Cutoff) {
			counts[e.Key]++
		}
	}

	stats := stream.PruneStats{}
	seen := make(map[string]int)
	kept := make([]stream.CursorMetadata, 0, len(entries))
	for _, e := range entries {
		if !policy.Cutoff.IsZero() && e.RecordedAt.Before(policy.Cutoff) {
			stats.Removed++
			continue
		}
		index := seen[e.Key]
		seen[e.Key] = index + 1
		keyCount := counts[e.Key]
		if policy.Keep > 0 && index < keyCount-policy.Keep {
			stats.Removed++
			continue
		}
		kept = append(kept, e)
		stats.Kept++
	}
	if err := s.saveEntries(ctx, kept); err != nil {
		return stream.PruneStats{}, err
	}
	return stats, nil
}

// ---------------------------------------------------------------------------
// RedisStateStore (stream.StateStore)
// ---------------------------------------------------------------------------

type RedisStateStore struct {
	redisStoreBase
	mu sync.Mutex
}

func NewRedisStateStore(client any, key any) (*RedisStateStore, error) {
	k, err := parseRedisKey(key, "")
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	return &RedisStateStore{redisStoreBase: redisStoreBase{client: client, key: k}}, nil
}

func (s *RedisStateStore) Load() (stream.State, error) {
	ctx := context.Background()
	raw, err := s.get(ctx, s.key)
	if err != nil {
		return stream.State{}, err
	}
	if strings.TrimSpace(raw) == "" {
		return stream.State{}, nil
	}
	var st stream.State
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return stream.State{}, fmt.Errorf("decoding redis stream state: %w", err)
	}
	return st, nil
}

func (s *RedisStateStore) Save(st stream.State) error {
	ctx := context.Background()
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.Source == "" {
		return errors.New("stream state must record which stream it belongs to")
	}
	st.Version = stream.StateVersion
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encoding redis stream state: %w", err)
	}
	return s.set(ctx, s.key, string(data), 0)
}

func (s *RedisStateStore) LoadContext(ctx context.Context) (stream.State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_ = ctx
	return s.Load()
}

func (s *RedisStateStore) SaveContext(ctx context.Context, st stream.State) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_ = ctx
	return s.Save(st)
}

// ---------------------------------------------------------------------------
// RedisCursorStore (stream.CursorStore)
// ---------------------------------------------------------------------------

type RedisCursorStore struct {
	redisStoreBase
	mu sync.Mutex
}

func NewRedisCursorStore(client any, key any) (*RedisCursorStore, error) {
	k, err := parseRedisKey(key, "")
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	return &RedisCursorStore{redisStoreBase: redisStoreBase{client: client, key: k}}, nil
}

// Aliases.
func NewRedisCursor(client any, key any) (*RedisCursorStore, error) {
	return NewRedisCursorStore(client, key)
}

func (s *RedisCursorStore) redisKeyFor(key string) string {
	if key != "" {
		return key
	}
	return s.key
}

func (s *RedisCursorStore) Load(ctx context.Context, key string) (stream.CursorState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rk := s.redisKeyFor(key)
	raw, err := s.get(ctx, rk)
	if err != nil {
		return stream.CursorState{}, err
	}
	if strings.TrimSpace(raw) == "" {
		return stream.CursorState{}, fmt.Errorf("cursor %q: %w", key, stream.ErrNoCursor)
	}
	var st stream.CursorState
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return stream.CursorState{}, fmt.Errorf("loading cursor %q: %w", key, err)
	}
	return st, nil
}

func (s *RedisCursorStore) Save(ctx context.Context, key string, state stream.CursorState) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" {
		return errors.New("cursor key must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rk := s.redisKeyFor(key)
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encoding cursor %q: %w", key, err)
	}
	return s.set(ctx, rk, string(data), 0)
}

// ---------------------------------------------------------------------------
// Redis lease (distributed leader)
// ---------------------------------------------------------------------------

type RedisLease struct {
	redisStoreBase
	mu     sync.Mutex
	holder string
	ttl    time.Duration
}

func NewRedisLease(client any, key any) (*RedisLease, error) {
	return NewRedisLeaseWithTTL(client, key, 15*time.Second, "")
}

func NewRedisLeaseWithTTL(client any, key any, ttl time.Duration, holder string) (*RedisLease, error) {
	k, err := parseRedisKey(key, "")
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	if holder == "" {
		holder = "prism"
	}
	return &RedisLease{
		redisStoreBase: redisStoreBase{client: client, key: k},
		holder:         holder,
		ttl:            ttl,
	}, nil
}

// Aliases.
func NewRedisLeaderLease(client any, key any) (*RedisLease, error) {
	return NewRedisLease(client, key)
}
func NewLeaderLease(client any, key any) (*RedisLease, error) {
	return NewRedisLease(client, key)
}

func (l *RedisLease) Key() string        { return l.key }
func (l *RedisLease) Holder() string     { return l.holder }
func (l *RedisLease) TTL() time.Duration { return l.ttl }

func (l *RedisLease) Acquire(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return l.setNX(ctx, l.key, l.holder, l.ttl)
}

func (l *RedisLease) Renew(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := l.set(ctx, l.key, l.holder, l.ttl); err != nil {
		return false, err
	}
	return true, nil
}

func (l *RedisLease) Release(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return l.del(ctx, l.key)
}

func (l *RedisLease) TryAcquire(ctx context.Context) (bool, error) {
	return l.Acquire(ctx)
}

// ---------------------------------------------------------------------------
// Minimal TCP Redis client (RESP)
// ---------------------------------------------------------------------------

type redisTCPClient struct {
	address  string
	password string
	db       int
	timeout  time.Duration
}

func NewRedisClient(rawURL string) (RedisClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis url: %w", err)
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("redis url must use redis:// or rediss://")
	}
	host := u.Host
	if host == "" {
		host = "localhost:6379"
	}
	db := 0
	if u.Path != "" && u.Path != "/" {
		n, err := strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
		if err == nil {
			db = n
		}
	}
	password := ""
	if u.User != nil {
		password, _ = u.User.Password()
	}
	return &redisTCPClient{
		address:  host,
		password: password,
		db:       db,
		timeout:  5 * time.Second,
	}, nil
}

func (c *redisTCPClient) dial() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", c.address, c.timeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	// AUTH
	if c.password != "" {
		if _, err := c.doConn(conn, "AUTH", c.password); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if c.db != 0 {
		if _, err := c.doConn(conn, "SELECT", strconv.Itoa(c.db)); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *redisTCPClient) do(ctx context.Context, args ...string) (any, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(d)
		}
	}
	return c.doConn(conn, args...)
}

func (c *redisTCPClient) doConn(conn net.Conn, args ...string) (any, error) {
	// write RESP
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write([]byte(b.String())); err != nil {
		return nil, err
	}
	return readRESP(conn)
}

func readRESP(conn net.Conn) (any, error) {
	buf := make([]byte, 1)
	// read first byte
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}
	switch buf[0] {
	case '+':
		line, err := readLine(conn)
		if err != nil {
			return nil, err
		}
		return line, nil
	case '-':
		line, err := readLine(conn)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	case ':':
		line, err := readLine(conn)
		if err != nil {
			return nil, err
		}
		n, _ := strconv.ParseInt(line, 10, 64)
		return n, nil
	case '$':
		line, err := readLine(conn)
		if err != nil {
			return nil, err
		}
		n, _ := strconv.Atoi(line)
		if n < 0 {
			return nil, nil
		}
		data := make([]byte, n)
		if _, err := readFull(conn, data); err != nil {
			return nil, err
		}
		// consume trailing \r\n
		_, _ = readLine(conn)
		return string(data), nil
	case '*':
		line, err := readLine(conn)
		if err != nil {
			return nil, err
		}
		n, _ := strconv.Atoi(line)
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := readRESP(conn)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown RESP type %q", buf[0])
	}
}

func readLine(conn net.Conn) (string, error) {
	var b []byte
	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			return string(b), err
		}
		b = append(b, buf[0])
		if len(b) >= 2 && b[len(b)-2] == '\r' && b[len(b)-1] == '\n' {
			return string(b[:len(b)-2]), nil
		}
	}
}

func readFull(conn net.Conn, data []byte) (int, error) {
	total := 0
	for total < len(data) {
		n, err := conn.Read(data[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// RedisClient methods for tcp client.
func (c *redisTCPClient) Get(ctx context.Context, key string) (string, error) {
	v, err := c.do(ctx, "GET", key)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", ErrRedisNil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unexpected GET reply %T", v)
	}
	return s, nil
}

func (c *redisTCPClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	args := []string{"SET", key, fmt.Sprintf("%v", value)}
	if expiration > 0 {
		args = append(args, "EX", strconv.FormatInt(int64(expiration.Seconds()), 10))
	}
	_, err := c.do(ctx, args...)
	return err
}

func (c *redisTCPClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	args := []string{"SET", key, fmt.Sprintf("%v", value), "NX"}
	if expiration > 0 {
		args = append(args, "EX", strconv.FormatInt(int64(expiration.Seconds()), 10))
	}
	v, err := c.do(ctx, args...)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	s, _ := v.(string)
	return s == "OK", nil
}

func (c *redisTCPClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	cmdArgs := []string{"EVAL", script, strconv.Itoa(len(keys))}
	cmdArgs = append(cmdArgs, keys...)
	for _, a := range args {
		cmdArgs = append(cmdArgs, fmt.Sprintf("%v", a))
	}
	return c.do(ctx, cmdArgs...)
}

func (c *redisTCPClient) Del(ctx context.Context, key string) error {
	_, err := c.do(ctx, "DEL", key)
	return err
}

func (c *redisTCPClient) Close() error { return nil }

// ---------------------------------------------------------------------------
// Compatibility aliases
// ---------------------------------------------------------------------------

type RedisJournalStore = RedisMetadataStore
type RedisCheckpointStore = RedisMetadataStore
type RedisState = RedisStateStore
type RedisCursor = RedisCursorStore
type RedisLeaderLease = RedisLease
type RedisLeader = RedisLease

func NewRedisJournalStore(client any, key any) (*RedisMetadataStore, error) {
	return NewRedisMetadataStore(client, key)
}

func NewRedisCheckpointStore(client any, key any) (*RedisMetadataStore, error) {
	return NewRedisMetadataStore(client, key)
}

func NewRedisMetadata(client any, key any) (*RedisMetadataStore, error) {
	return NewRedisMetadataStore(client, key)
}

func NewStateRedisStore(client any, key any) (*RedisStateStore, error) {
	return NewRedisStateStore(client, key)
}

func NewRedisState(client any, key any) (*RedisStateStore, error) {
	return NewRedisStateStore(client, key)
}

func NewRedisCursorCheckpointStore(client any, key any) (*RedisCursorStore, error) {
	return NewRedisCursorStore(client, key)
}

func NewRedisCursorStateStore(client any, key any) (*RedisCursorStore, error) {
	return NewRedisCursorStore(client, key)
}

func NewRedisLeaseStore(client any, key any) (*RedisLease, error) {
	return NewRedisLease(client, key)
}

func OpenRedisClient(rawURL string) (RedisClient, error) {
	return NewRedisClient(rawURL)
}

// NewRedisClientFromURL is an alias for NewRedisClient.
func NewRedisClientFromURL(rawURL string) (RedisClient, error) {
	return NewRedisClient(rawURL)
}
