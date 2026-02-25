package impl

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/vincentAlen/mail"
	"github.com/vincentAlen/mail/cache"
	"github.com/vincentAlen/mail/repo"
)

// ---------------------------------------------------------------------------
// Package-level test infrastructure
// ---------------------------------------------------------------------------

var (
	testDB    *mongo.Database
	testRedis *redis.Client
	testRepo  *repo.Repository
	testCache *cache.Cache

	serverIDSeq int32
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// MongoDB
	mongoURI := envOr("MONGO_URI", "mongodb://localhost:27017")
	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		fmt.Printf("skip: mongo connect: %v\n", err)
		os.Exit(0)
	}
	if err := client.Ping(ctx, nil); err != nil {
		fmt.Printf("skip: mongo ping: %v\n", err)
		os.Exit(0)
	}

	// Redis
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("skip: redis ping: %v\n", err)
		os.Exit(0)
	}

	testDB = client.Database("mail_integration_test")
	_ = testDB.Drop(ctx) // clean start
	testRedis = rdb
	testRepo = repo.NewRepository(testDB)
	testCache = cache.NewCache(rdb)

	if err := testRepo.EnsureIndexes(ctx); err != nil {
		fmt.Printf("fail: ensure indexes: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = testDB.Drop(ctx)
	_ = client.Disconnect(ctx)
	_ = rdb.Close()
	os.Exit(code)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func nextServerID() int32 {
	return atomic.AddInt32(&serverIDSeq, 1)
}

// ---------------------------------------------------------------------------
// testEnv — per-test isolated environment (unique serverID)
// ---------------------------------------------------------------------------

type testEnv struct {
	serverID int32
	svc      mail.Service
	admin    mail.AdminService
	granter  *mockGranter
	push     *mockPush
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	sid := nextServerID()
	granter := &mockGranter{}
	push := &mockPush{}

	svc := NewService(
		mail.DefaultConfig(),
		testRepo,
		testCache,
		granter,
		&mockLocker{},
		WithPushNotifier(push),
	)
	admin := NewAdminService(svc)

	t.Cleanup(func() {
		ctx := context.Background()
		testRedis.Del(ctx,
			fmt.Sprintf("{mail:seq:%d}", sid),
			fmt.Sprintf("{broadcast:latest:%d}", sid),
			fmt.Sprintf("{mail:unread:%d}", sid),
		)
	})

	return &testEnv{
		serverID: sid,
		svc:      svc,
		admin:    admin,
		granter:  granter,
		push:     push,
	}
}

// ---------------------------------------------------------------------------
// Mock: RewardGranter
// ---------------------------------------------------------------------------

type grantCall struct {
	ServerID       int32
	UID            int64
	Items          []mail.RewardItem
	IdempotencyKey string
}

type mockGranter struct {
	mu       sync.Mutex
	calls    []grantCall
	failKeys map[string]error // idempotencyKey → error
}

func (m *mockGranter) Grant(ctx context.Context, serverID int32, uid int64, items []mail.RewardItem, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, grantCall{serverID, uid, items, key})
	if m.failKeys != nil {
		if err, ok := m.failKeys[key]; ok {
			return err
		}
	}
	return nil
}

func (m *mockGranter) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// ---------------------------------------------------------------------------
// Mock: Locker (executes fn directly, no real lock)
// ---------------------------------------------------------------------------

type mockLocker struct{}

func (m *mockLocker) WithUserLock(ctx context.Context, _ int32, _ int64, fn func(context.Context) error) error {
	return fn(ctx)
}

// ---------------------------------------------------------------------------
// Mock: PushNotifier
// ---------------------------------------------------------------------------

type pushEvent struct {
	ServerID int32
	UID      int64
	Event    string
}

type mockPush struct {
	mu     sync.Mutex
	events []pushEvent
}

func (m *mockPush) NotifyUser(ctx context.Context, serverID int32, uid int64, event string, _ any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, pushEvent{serverID, uid, event})
	return nil
}

func (m *mockPush) NotifyServer(ctx context.Context, serverID int32, event string, _ any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, pushEvent{ServerID: serverID, Event: event})
	return nil
}

func (m *mockPush) eventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// ---------------------------------------------------------------------------
// Mock: TargetResolver
// ---------------------------------------------------------------------------

type mockResolver struct {
	failScopes map[string]bool
}

func (m *mockResolver) Match(ctx context.Context, serverID int32, uid int64, target mail.Target) (bool, error) {
	if m.failScopes != nil && m.failScopes[target.Scope] {
		return false, fmt.Errorf("resolver error for scope %s", target.Scope)
	}
	return true, nil
}

func newTestEnvWithResolver(t *testing.T, resolver mail.TargetResolver) *testEnv {
	t.Helper()
	sid := nextServerID()
	granter := &mockGranter{}
	push := &mockPush{}

	svc := NewService(
		mail.DefaultConfig(),
		testRepo,
		testCache,
		granter,
		&mockLocker{},
		WithPushNotifier(push),
		WithTargetResolver(resolver),
	)
	admin := NewAdminService(svc)

	t.Cleanup(func() {
		ctx := context.Background()
		testRedis.Del(ctx,
			fmt.Sprintf("{mail:seq:%d}", sid),
			fmt.Sprintf("{broadcast:latest:%d}", sid),
			fmt.Sprintf("{mail:unread:%d}", sid),
		)
	})

	return &testEnv{
		serverID: sid,
		svc:      svc,
		admin:    admin,
		granter:  granter,
		push:     push,
	}
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertEq[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}

func assertTrue(t *testing.T, v bool, msg string) {
	t.Helper()
	if !v {
		t.Errorf("%s: expected true", msg)
	}
}

func assertFalse(t *testing.T, v bool, msg string) {
	t.Helper()
	if v {
		t.Errorf("%s: expected false", msg)
	}
}
