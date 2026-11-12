package upstream

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// This file implements the account state machine + request-level trace from the
// design note, with failure classification delegated to the platform adapters
// (so 429/401/403 semantics stay platform-correct instead of being guessed from
// the bare status code).
//
// It is the reference implementation for step 2 of the migration. The real
// scheduler (service/openai_account_scheduler.go and the layered filter in
// gateway_service.go) carries much more policy — groups, model availability,
// Redis-held concurrency leases, sticky-by-previous-response — but the state
// transitions and trace fields are the same, so they can be swapped one unit at
// a time.

// State is the schedulable state of an account.
type State string

const (
	// StateActive is the normal state.
	StateActive State = "active"
	// StateCooling means the upstream asked us to back off until CoolingAt
	// (429 with a reset hint). The credential itself is fine.
	StateCooling State = "cooling"
	// StateDegraded means repeated upstream-side failures (5xx/timeout): still
	// selectable, but only when no active account is free and one probe at a
	// time, so a sick account can recover without eating the request budget.
	StateDegraded State = "degraded"
	// StateInvalid means the credential is unusable (401/403/quota exhausted) and
	// needs a token refresh or a human.
	StateInvalid State = "invalid"
)

// Defaults implied by the design note but not stated in it.
const (
	defaultCoolDown      = 60 * time.Second
	defaultFailThreshold = 5
	defaultStickyTTL     = 30 * time.Minute
	defaultMaxSticky     = 10000
)

// AccountConfig is the immutable part of a pool member.
type AccountConfig struct {
	ID          int64
	Platform    Platform
	Priority    int // higher wins, matching service.Account.Priority
	MaxInFlight int // <=0 means unbounded
}

// Trace is the per-request record from the design note (item 1): the fields a
// support engineer needs to answer "which account failed and why" from a
// request id. It never carries headers or bodies, so it cannot leak a token.
type Trace struct {
	RequestID      string
	UserID         int64
	APIKeyID       int64
	AccountID      int64
	Model          string
	UpstreamStatus int
	Failure        FailureKind
	Reason         string
	// RetryFrom is the account used by the previous attempt of the same request,
	// i.e. the failover source. Zero on the first attempt.
	RetryFrom int64
	// Attempt is 1-based.
	Attempt int
	Latency time.Duration
	State   State
}

// LogAttrs renders the trace for structured logging (slog style).
func (t Trace) LogAttrs() []any {
	return []any{
		"request_id", t.RequestID,
		"user_id", t.UserID,
		"api_key_id", t.APIKeyID,
		"account_id", t.AccountID,
		"model", t.Model,
		"upstream_status", t.UpstreamStatus,
		"error_type", t.Failure.String(),
		"error_msg", t.Reason,
		"retry_from", t.RetryFrom,
		"attempt", t.Attempt,
		"latency_ms", t.Latency.Milliseconds(),
		"account_state", string(t.State),
	}
}

// ErrNoAvailableAccount is returned when every candidate is excluded or unusable.
var ErrNoAvailableAccount = errors.New("upstream: no available account")

// Pool is a reference scheduler + account state machine.
//
// Concurrency: all state sits behind one mutex. Fine for a few hundred accounts;
// the production scheduler instead hydrates an immutable snapshot
// (service/scheduler_snapshot_service.go) and reads it lock-free on the hot path.
type Pool struct {
	mu       sync.Mutex
	now      func() time.Time
	registry *Registry
	accounts map[int64]*poolAccount
	order    []int64 // stable iteration order keeps picks deterministic
	sticky   map[string]stickyBinding

	coolDown      time.Duration
	failThreshold int
	stickyTTL     time.Duration
	maxSticky     int
}

type poolAccount struct {
	cfg        AccountConfig
	state      State
	coolingAt  time.Time
	failCount  int
	inFlight   int
	lastUsedAt time.Time
}

type stickyBinding struct {
	accountID int64
	expiresAt time.Time
}

// Option customises a Pool.
type Option func(*Pool)

// WithClock overrides the clock, for tests.
func WithClock(now func() time.Time) Option { return func(p *Pool) { p.now = now } }

// WithStickyTTL bounds how long a session stays pinned to one account.
func WithStickyTTL(ttl time.Duration) Option {
	return func(p *Pool) {
		if ttl > 0 {
			p.stickyTTL = ttl
		}
	}
}

// WithFailThreshold sets how many consecutive upstream-side failures degrade an
// account before invalidating it.
func WithFailThreshold(n int) Option {
	return func(p *Pool) {
		if n > 0 {
			p.failThreshold = n
		}
	}
}

// WithCooldown sets the fallback cooling window used when a 429 carries no
// machine-readable reset hint.
func WithCooldown(d time.Duration) Option {
	return func(p *Pool) {
		if d > 0 {
			p.coolDown = d
		}
	}
}

// WithMaxSticky caps the affinity map so it cannot grow without bound. An
// unbounded session->account map is a slow memory leak in a design like this.
func WithMaxSticky(n int) Option {
	return func(p *Pool) {
		if n > 0 {
			p.maxSticky = n
		}
	}
}

// NewPool builds a pool. Every account platform must have a registered adapter:
// classification is delegated, so a missing adapter is a wiring bug rather than
// a silent runtime fallback.
func NewPool(registry *Registry, accounts []AccountConfig, opts ...Option) (*Pool, error) {
	if registry == nil {
		return nil, errors.New("upstream: nil registry")
	}
	p := &Pool{
		now:           time.Now,
		registry:      registry,
		accounts:      make(map[int64]*poolAccount, len(accounts)),
		sticky:        make(map[string]stickyBinding),
		coolDown:      defaultCoolDown,
		failThreshold: defaultFailThreshold,
		stickyTTL:     defaultStickyTTL,
		maxSticky:     defaultMaxSticky,
	}
	for _, o := range opts {
		o(p)
	}
	for _, cfg := range accounts {
		if cfg.ID <= 0 {
			return nil, fmt.Errorf("upstream: account id must be positive, got %d", cfg.ID)
		}
		if _, ok := registry.Get(cfg.Platform); !ok {
			return nil, fmt.Errorf("upstream: account %d: no adapter registered for platform %q", cfg.ID, cfg.Platform)
		}
		if _, dup := p.accounts[cfg.ID]; dup {
			return nil, fmt.Errorf("upstream: duplicate account id %d", cfg.ID)
		}
		p.accounts[cfg.ID] = &poolAccount{cfg: cfg, state: StateActive}
		p.order = append(p.order, cfg.ID)
	}
	return p, nil
}

// Lease is one in-flight slot on one account. Release must be called; it is safe
// to defer and safe to call twice.
type Lease struct {
	pool      *Pool
	accountID int64
	attempt   int
	startedAt time.Time
	released  bool
}

// AccountID returns the leased account.
func (l *Lease) AccountID() int64 {
	if l == nil {
		return 0
	}
	return l.accountID
}

// Attempt returns the 1-based attempt number this lease belongs to.
func (l *Lease) Attempt() int {
	if l == nil {
		return 0
	}
	return l.attempt
}

// Release frees the in-flight slot. Idempotent: a double release (defer plus an
// explicit call on an error path) must not corrupt the counter, otherwise the
// account silently loses capacity forever.
func (l *Lease) Release() {
	if l == nil || l.pool == nil {
		return
	}
	l.pool.mu.Lock()
	defer l.pool.mu.Unlock()
	l.releaseLocked()
}

// releaseLocked is the single place the counter is decremented. Callers must
// hold p.mu.
func (l *Lease) releaseLocked() {
	if l.released {
		return
	}
	l.released = true
	p := l.pool
	if p == nil {
		return
	}
	if a, ok := p.accounts[l.accountID]; ok && a.inFlight > 0 {
		a.inFlight--
	}
}

// Pick selects an account for session.
//
// Order: sticky binding (if still usable and not excluded) -> highest priority ->
// lowest load ratio -> degraded fallback. excluded holds the accounts already
// tried by this request, which is how failover avoids retrying a dead account.
func (p *Pool) Pick(session string, attempt int, excluded ...int64) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	skip := make(map[int64]struct{}, len(excluded))
	for _, id := range excluded {
		skip[id] = struct{}{}
	}
	// Promote cooldown-expired accounts before scanning, so a pool that only had
	// cooling members becomes schedulable again without needing a sticky hit to
	// trigger the transition.
	p.promoteDueLocked(now)

	// 1. Sticky affinity.
	if session != "" {
		if binding, ok := p.sticky[session]; ok {
			if now.After(binding.expiresAt) {
				delete(p.sticky, session)
			} else if _, banned := skip[binding.accountID]; !banned {
				if a, exists := p.accounts[binding.accountID]; exists && p.selectable(a, now) {
					return p.lease(a, attempt), nil
				}
			}
		}
	}

	// 2. Active candidates: highest priority, then lowest load ratio.
	var best *poolAccount
	var bestRatio float64
	for _, id := range p.order {
		a := p.accounts[id]
		if _, banned := skip[id]; banned {
			continue
		}
		if a.state != StateActive {
			continue
		}
		if a.cfg.MaxInFlight > 0 && a.inFlight >= a.cfg.MaxInFlight {
			continue
		}
		ratio := loadRatio(a)
		if best == nil || a.cfg.Priority > best.cfg.Priority ||
			(a.cfg.Priority == best.cfg.Priority && ratio < bestRatio) {
			best, bestRatio = a, ratio
		}
	}

	// 3. Degraded fallback: only when nothing active is free, one probe at a time.
	if best == nil {
		for _, id := range p.order {
			a := p.accounts[id]
			if _, banned := skip[id]; banned {
				continue
			}
			if a.state != StateDegraded || a.inFlight > 0 {
				continue
			}
			best = a
			break
		}
	}

	if best == nil {
		return nil, ErrNoAvailableAccount
	}
	if session != "" {
		p.bindStickyLocked(session, best.cfg.ID, now)
	}
	return p.lease(best, attempt), nil
}

// promoteDueLocked moves every cooling account whose window elapsed back to
// active. Callers must hold p.mu.
func (p *Pool) promoteDueLocked(now time.Time) {
	for _, id := range p.order {
		a := p.accounts[id]
		if a.state == StateCooling && !now.Before(a.coolingAt) {
			a.state = StateActive
			a.coolingAt = time.Time{}
		}
	}
}

// selectable reports whether a is usable for a sticky hit.
func (p *Pool) selectable(a *poolAccount, now time.Time) bool {
	if a.cfg.MaxInFlight > 0 && a.inFlight >= a.cfg.MaxInFlight {
		return false
	}
	switch a.state {
	case StateActive:
		return true
	case StateCooling:
		if !now.Before(a.coolingAt) {
			a.state = StateActive
			a.coolingAt = time.Time{}
			return true
		}
		return false
	default:
		return false
	}
}

func (p *Pool) lease(a *poolAccount, attempt int) *Lease {
	a.inFlight++
	now := p.now()
	a.lastUsedAt = now
	return &Lease{pool: p, accountID: a.cfg.ID, attempt: attempt, startedAt: now}
}

func (p *Pool) bindStickyLocked(session string, accountID int64, now time.Time) {
	if len(p.sticky) >= p.maxSticky {
		var oldestKey string
		var oldestExpiry time.Time
		for k, v := range p.sticky {
			if now.After(v.expiresAt) {
				delete(p.sticky, k)
				continue
			}
			if oldestKey == "" || v.expiresAt.Before(oldestExpiry) {
				oldestKey, oldestExpiry = k, v.expiresAt
			}
		}
		if oldestKey != "" {
			delete(p.sticky, oldestKey)
		}
	}
	p.sticky[session] = stickyBinding{accountID: accountID, expiresAt: now.Add(p.stickyTTL)}
}

// Report feeds an upstream response back into the state machine and releases the
// lease. Classification goes through the account's adapter, so platform-specific
// semantics (OpenAI insufficient_quota on 429, Anthropic credit-exhausted on 400,
// Google capacity-exhausted on 429) are honoured instead of being flattened into
// plain rate limiting.
func (p *Pool) Report(l *Lease, status int, header http.Header, body []byte) Trace {
	trace := Trace{UpstreamStatus: status}
	if l == nil {
		return trace
	}
	trace.AccountID = l.accountID
	trace.Attempt = l.attempt
	trace.Latency = p.now().Sub(l.startedAt)

	p.mu.Lock()
	defer p.mu.Unlock()

	a, ok := p.accounts[l.accountID]
	if !ok {
		// Account removed between Pick and Report. Release inline: calling
		// l.Release() would re-enter the mutex already held.
		l.releaseLocked()
		return trace
	}

	now := p.now()
	var failure Failure
	if adapter, hasAdapter := p.registry.Get(a.cfg.Platform); hasAdapter {
		failure = adapter.Classify(now, status, header, body)
	} else {
		failure = classifyStatus(now, status, header, body)
	}

	switch failure.Kind {
	case FailureNone:
		a.state = StateActive
		a.coolingAt = time.Time{}
		a.failCount = 0
	case FailureRateLimit:
		a.state = StateCooling
		a.coolingAt = failure.RetryAt
		if a.coolingAt.IsZero() {
			a.coolingAt = now.Add(p.coolDown)
		}
		a.failCount = 0
	case FailureAuth:
		a.state = StateInvalid
		a.coolingAt = time.Time{}
	case FailureOverload, FailureTransient:
		a.failCount++
		if a.failCount >= p.failThreshold {
			a.state = StateInvalid
		} else {
			a.state = StateDegraded
		}
	case FailureClient:
		// The caller's request was bad: the account is not at fault, and the
		// consecutive-failure counter must not be reset by it either.
	}

	trace.Failure = failure.Kind
	trace.Reason = failure.Reason
	trace.State = a.state

	// Release under the same lock as the transition, so a concurrent Pick cannot
	// observe inFlight+1 together with an already-cooling state.
	l.releaseLocked()
	return trace
}

// Reinstate puts an invalid account back in service. The design says "人工处理后
// 恢复", but most 401s are expired access tokens that the refresh job fixes
// automatically: recovery has to be a method, not an operator with a database
// console.
func (p *Pool) Reinstate(accountID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.accounts[accountID]
	if !ok {
		return fmt.Errorf("upstream: unknown account %d", accountID)
	}
	a.state = StateActive
	a.coolingAt = time.Time{}
	a.failCount = 0
	return nil
}

// AccountSnapshot is a read-only view for the aggregation/alerting layer.
type AccountSnapshot struct {
	ID         int64
	Platform   Platform
	State      State
	CoolingAt  time.Time
	FailCount  int
	InFlight   int
	LastUsedAt time.Time
}

// Snapshot returns the state of one account.
func (p *Pool) Snapshot(accountID int64) (AccountSnapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.accounts[accountID]
	if !ok {
		return AccountSnapshot{}, false
	}
	return a.snapshot(), true
}

func (a *poolAccount) snapshot() AccountSnapshot {
	return AccountSnapshot{
		ID: a.cfg.ID, Platform: a.cfg.Platform, State: a.state,
		CoolingAt: a.coolingAt, FailCount: a.failCount,
		InFlight: a.inFlight, LastUsedAt: a.lastUsedAt,
	}
}

// Snapshots returns every account state, for dashboards and alerting.
func (p *Pool) Snapshots() []AccountSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]AccountSnapshot, 0, len(p.accounts))
	for _, id := range p.order {
		out = append(out, p.accounts[id].snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// loadRatio is the scheduling weight: lower wins. Degraded accounts are pushed to
// the back by their zero concurrency allowance (they are only reachable through
// the explicit fallback branch), and unbounded accounts sort by raw in-flight.
func loadRatio(a *poolAccount) float64 {
	if a.state == StateDegraded {
		return 1 << 20
	}
	limit := a.cfg.MaxInFlight
	if limit <= 0 {
		return float64(a.inFlight)
	}
	return float64(a.inFlight) / float64(limit)
}

// Failover is a convenience wrapper for the forward flow in the design note:
// Pick -> forward -> Report -> (on 429/401/403) Pick again excluding what was
// tried, carrying the previous account into the trace's RetryFrom field.
//
// Deliberately explicit rather than a callback soup: the caller keeps full
// control over the actual HTTP forwarding.
type Failover struct {
	pool    *Pool
	session string
	attempt int
	tried   []int64
	last    Trace
}

// NewFailover starts a failover chain for one downstream request.
func (p *Pool) NewFailover(session string) *Failover {
	return &Failover{pool: p, session: session}
}

// Next picks the next account of this chain.
func (f *Failover) Next() (*Lease, error) {
	f.attempt++
	lease, err := f.pool.Pick(f.session, f.attempt, f.tried...)
	if err != nil {
		return nil, err
	}
	f.tried = append(f.tried, lease.AccountID())
	return lease, nil
}

// Report records the outcome; RetryFrom is filled from the previous attempt of
// this same chain, which is what makes failover traceable.
func (f *Failover) Report(l *Lease, status int, header http.Header, body []byte) Trace {
	trace := f.pool.Report(l, status, header, body)
	if len(f.tried) >= 2 {
		trace.RetryFrom = f.tried[len(f.tried)-2]
	}
	f.last = trace
	return trace
}

// ShouldRetry reports whether the outcome warrants trying another account.
func (f *Failover) ShouldRetry(trace Trace) bool {
	switch trace.Failure {
	case FailureRateLimit, FailureAuth, FailureOverload, FailureTransient:
		return true
	default:
		return false
	}
}

// Attempts returns how many accounts were tried so far.
func (f *Failover) Attempts() int { return f.attempt }

// Last returns the most recent trace.
func (f *Failover) Last() Trace { return f.last }
