package upstream

import (
	"net/http"
	"testing"
	"time"
)

func testRegistryOrFail(t *testing.T) *Registry {
	t.Helper()
	r, err := NewRegistry(NewClaudeAdapter(), NewOpenAIAdapter(), NewAntigravityAdapter())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return r
}

// fakeClock is a manually advanced clock; every timing assertion in this file
// depends on it, so time is never flaky.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestPool(t *testing.T, clock *fakeClock, accounts []AccountConfig, opts ...Option) *Pool {
	t.Helper()
	base := []Option{WithClock(clock.now)}
	p, err := NewPool(testRegistryOrFail(t), accounts, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func retryAfterHeader(secs string) http.Header {
	h := make(http.Header, 1)
	h.Set("retry-after", secs)
	return h
}

// 429 + Retry-After: the account cools for exactly that window and comes back
// once it elapses. This is the one case the design note gets right.
func TestPoolRateLimitHonoursRetryAfter(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude, MaxInFlight: 1}})

	lease, err := p.Pick("s1", 1)
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	trace := p.Report(lease, 429, retryAfterHeader("30"), nil)
	if trace.State != StateCooling || trace.Failure != FailureRateLimit {
		t.Fatalf("state=%v failure=%v", trace.State, trace.Failure)
	}

	// Still cooling 29s later -> nothing to pick.
	clock.advance(29 * time.Second)
	if _, err := p.Pick("s2", 1); err != ErrNoAvailableAccount {
		t.Fatalf("cooling account must not be picked, got err=%v", err)
	}

	// Window elapsed -> schedulable again.
	clock.advance(2 * time.Second)
	lease2, err := p.Pick("s2", 1)
	if err != nil {
		t.Fatalf("expected account back in service: %v", err)
	}
	lease2.Release()
}

// The design's biggest hole: it treats every 429 as a cooldown. OpenAI reports
// exhausted quota as 429 + insufficient_quota, and cooling it forever means the
// pool keeps re-selecting a dead account instead of pulling it.
func TestPoolOpenAIQuotaExhaustedIsInvalidNotCooling(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{
		{ID: 1, Platform: PlatformOpenAI, Priority: 10},
		{ID: 2, Platform: PlatformOpenAI, Priority: 5},
	})

	lease, err := p.Pick("s1", 1)
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if lease.AccountID() != 1 {
		t.Fatalf("priority account should win, got %d", lease.AccountID())
	}

	body := []byte(`{"error":{"message":"You exceeded your current quota","code":"insufficient_quota"}}`)
	trace := p.Report(lease, 429, nil, body)
	if trace.State != StateInvalid {
		t.Fatalf("quota exhaustion must invalidate the account, got state=%v (failure=%v)", trace.State, trace.Failure)
	}

	// A plain rate limit on the same status code must NOT invalidate.
	p2 := newTestPool(t, clock, []AccountConfig{{ID: 3, Platform: PlatformOpenAI}})
	l2, _ := p2.Pick("", 1)
	tr2 := p2.Report(l2, 429, nil, []byte(`{"error":{"code":"rate_limit_exceeded"}}`))
	if tr2.State != StateCooling {
		t.Fatalf("plain 429 must cool, got %v", tr2.State)
	}
}

// Anthropic reports "credit balance is too low" as a 400.
func TestPoolAnthropicCreditExhaustedOn400(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude}})
	lease, _ := p.Pick("", 1)
	body := []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low"}}`)
	if trace := p.Report(lease, 400, nil, body); trace.State != StateInvalid {
		t.Fatalf("credit exhaustion must invalidate, got %v", trace.State)
	}
}

func TestPoolAuthFailureInvalidatesAndIsNotSchedulable(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude, MaxInFlight: 1}})

	lease, _ := p.Pick("", 1)
	if trace := p.Report(lease, 403, nil, nil); trace.State != StateInvalid {
		t.Fatalf("403 must invalidate, got %v", trace.State)
	}
	clock.advance(time.Hour)
	if _, err := p.Pick("", 1); err != ErrNoAvailableAccount {
		t.Fatalf("invalid account must stay out without Reinstate, err=%v", err)
	}

	// Reinstate models the token-refresh job finishing successfully.
	if err := p.Reinstate(1); err != nil {
		t.Fatalf("reinstate: %v", err)
	}
	l, err := p.Pick("", 1)
	if err != nil {
		t.Fatalf("reinstate must restore service: %v", err)
	}
	l.Release()
}

// 5xx degrades first, and only invalidates after consecutive failures. A success
// in between must reset the counter, otherwise a healthy account is killed by
// sporadic upstream blips.
func TestPoolDegradedThenInvalidWithReset(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{
		{ID: 1, Platform: PlatformClaude},
		{ID: 2, Platform: PlatformClaude, Priority: -1},
	}, WithFailThreshold(3))

	// Two failures -> degraded, still usable as a fallback.
	for i := 0; i < 2; i++ {
		l, err := p.Pick("", i+1)
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if trace := p.Report(l, 502, nil, nil); trace.State != StateDegraded {
			t.Fatalf("failure %d: want degraded, got %v", i+1, trace.State)
		}
	}

	// A success resets the streak.
	l, err := p.Pick("", 3)
	if err != nil {
		t.Fatalf("degraded account must still be reachable: %v", err)
	}
	if trace := p.Report(l, 200, nil, nil); trace.State != StateActive {
		t.Fatalf("success must reset to active, got %v", trace.State)
	}

	// Three consecutive failures now invalidate.
	for i := 0; i < 3; i++ {
		l, err := p.Pick("", i+10)
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		trace := p.Report(l, 500, nil, nil)
		if i == 2 && trace.State != StateInvalid {
			t.Fatalf("third consecutive 5xx must invalidate, got %v", trace.State)
		}
	}
	if snap, _ := p.Snapshot(1); snap.State != StateInvalid {
		t.Fatalf("account 1 state = %v", snap.State)
	}
}

// Degraded accounts are a last resort: an active account must always win.
func TestPoolDegradedIsLastResort(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{
		{ID: 1, Platform: PlatformClaude, Priority: 10},
		{ID: 2, Platform: PlatformClaude, Priority: 1},
	})
	l, _ := p.Pick("", 1)
	p.Report(l, 500, nil, nil) // account 1 -> degraded

	for i := 0; i < 3; i++ {
		lease, err := p.Pick("", i+2)
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if lease.AccountID() != 2 {
			t.Fatalf("degraded account 1 must not be preferred over active account 2")
		}
		lease.Release()
	}
}

// Sticky affinity: one session keeps one account, and the binding expires.
func TestPoolStickyAffinityAndTTL(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{
		{ID: 1, Platform: PlatformClaude},
		{ID: 2, Platform: PlatformClaude},
		{ID: 3, Platform: PlatformClaude},
	}, WithStickyTTL(time.Minute))

	first, err := p.Pick("session-a", 1)
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	first.Release()

	for i := 0; i < 5; i++ {
		lease, err := p.Pick("session-a", i+2)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if lease.AccountID() != first.AccountID() {
			t.Fatalf("session must stay pinned: got %d want %d", lease.AccountID(), first.AccountID())
		}
		lease.Release()
	}

	// Another session may land elsewhere.
	other, err := p.Pick("session-b", 1)
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	other.Release()

	// TTL expiry lets the binding go stale.
	clock.advance(2 * time.Minute)
	expired, err := p.Pick("session-a", 9)
	if err != nil {
		t.Fatalf("pick after ttl: %v", err)
	}
	if expired.AccountID() == 0 {
		t.Fatal("expected an account")
	}
	expired.Release()
}

// Picking must survive a panic-free but sloppy caller: releasing twice must not
// erode capacity (the classic inFlight leak in the design snippet).
func TestPoolDoubleReleaseDoesNotLeakCapacity(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude, MaxInFlight: 1}})

	lease, _ := p.Pick("", 1)
	lease.Release()
	lease.Release()
	lease.Release()

	snap, _ := p.Snapshot(1)
	if snap.InFlight != 0 {
		t.Fatalf("inFlight = %d, want 0", snap.InFlight)
	}
	next, err := p.Pick("", 1)
	if err != nil {
		t.Fatalf("capacity leaked, cannot pick: %v", err)
	}
	next.Release()
}

// Report releases the lease itself, and the capacity is honest afterwards.
func TestPoolReportReleasesLease(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude, MaxInFlight: 1}})

	lease, _ := p.Pick("", 1)
	if snap, _ := p.Snapshot(1); snap.InFlight != 1 {
		t.Fatalf("inFlight after pick = %d", snap.InFlight)
	}
	p.Report(lease, 200, nil, nil)
	if snap, _ := p.Snapshot(1); snap.InFlight != 0 {
		t.Fatalf("inFlight after report = %d", snap.InFlight)
	}
}

func TestPoolMaxInFlight(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude, MaxInFlight: 2}})

	l1, _ := p.Pick("", 1)
	l2, _ := p.Pick("", 1)
	if l1 == nil || l2 == nil {
		t.Fatal("expected two leases")
	}
	if _, err := p.Pick("", 1); err != ErrNoAvailableAccount {
		t.Fatalf("must block at MaxInFlight, got %v", err)
	}
	l1.Release()
	l3, err := p.Pick("", 1)
	if err != nil {
		t.Fatalf("slot should be free: %v", err)
	}
	l3.Release()
	l2.Release()
}

// Failover: trace carries the account the request came from.
func TestPoolFailoverRecordsRetryFrom(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{
		{ID: 1, Platform: PlatformClaude, Priority: 10},
		{ID: 2, Platform: PlatformClaude, Priority: 5},
	})

	f := p.NewFailover("req-1")
	first, err := f.Next()
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	trace1 := f.Report(first, 403, nil, nil)
	if trace1.RetryFrom != 0 {
		t.Fatalf("first attempt must not have RetryFrom, got %d", trace1.RetryFrom)
	}
	if trace1.AccountID != 1 {
		t.Fatalf("priority account should be first, got %d", trace1.AccountID)
	}
	if !f.ShouldRetry(trace1) {
		t.Fatal("403 must be retried on another account")
	}

	second, err := f.Next()
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.AccountID() == 1 {
		t.Fatal("must not retry the invalidated account")
	}
	trace2 := f.Report(second, 200, nil, nil)
	if trace2.RetryFrom != 1 {
		t.Fatalf("RetryFrom = %d, want 1", trace2.RetryFrom)
	}
	if trace2.Attempt != 2 {
		t.Fatalf("Attempt = %d, want 2", trace2.Attempt)
	}
	if f.ShouldRetry(trace2) {
		t.Fatal("success must not be retried")
	}
	if f.Attempts() != 2 {
		t.Fatalf("Attempts = %d", f.Attempts())
	}
}

// A caller error must not change account state at all.
func TestPoolClientErrorLeavesStateAlone(t *testing.T) {
	clock := newFakeClock()
	p := newTestPool(t, clock, []AccountConfig{{ID: 1, Platform: PlatformClaude}})

	for i := 0; i < 10; i++ {
		l, _ := p.Pick("", i+1)
		if trace := p.Report(l, 400, nil, []byte(`{"error":{"type":"invalid_request_error"}}`)); trace.State != StateActive {
			t.Fatalf("400 must keep the account active, got %v", trace.State)
		}
	}
}

func TestPoolTraceLogAttrsAreRedacted(t *testing.T) {
	trace := Trace{
		RequestID: "r1", AccountID: 7, UpstreamStatus: 429,
		Failure: FailureRateLimit, Reason: "rate limited",
	}
	attrs := trace.LogAttrs()
	for i := 0; i+1 < len(attrs); i += 2 {
		key, _ := attrs[i].(string)
		if key == "authorization" || key == "x-api-key" || key == "body" || key == "headers" {
			t.Fatalf("trace must not carry credentials: %s", key)
		}
	}
	if len(attrs)%2 != 0 {
		t.Fatalf("LogAttrs must be key/value pairs, got %d elements", len(attrs))
	}
}

func TestNewPoolRejectsBadWiring(t *testing.T) {
	if _, err := NewPool(nil, nil); err == nil {
		t.Fatal("nil registry must fail")
	}
	if _, err := NewPool(testRegistryOrFail(t), []AccountConfig{{ID: 1, Platform: "nope"}}); err == nil {
		t.Fatal("unknown platform must fail")
	}
	if _, err := NewPool(testRegistryOrFail(t), []AccountConfig{{ID: 0, Platform: PlatformClaude}}); err == nil {
		t.Fatal("non-positive id must fail")
	}
	if _, err := NewPool(testRegistryOrFail(t), []AccountConfig{
		{ID: 1, Platform: PlatformClaude}, {ID: 1, Platform: PlatformClaude},
	}); err == nil {
		t.Fatal("duplicate id must fail")
	}
}
