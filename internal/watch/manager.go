/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/plan"
)

// Pacing of the reconcile loop.
const (
	// defaultResyncPeriod is the safety tick: how often the pool is diffed
	// against the registry even when no change notification arrived.
	//
	// The registry's notifications are reliable, so this is not the primary
	// trigger — it is the level-triggering backstop that makes the data plane
	// self-correcting. It is what re-attempts an informer whose start failed, what
	// picks up a kind whose CRD was installed after the rule naming it, and what
	// would repair the pool if a notification were ever missed. 30s is short
	// enough that an operator perceives such recoveries as automatic and long
	// enough that the pass (a snapshot plus a few map diffs) is free.
	defaultResyncPeriod = 30 * time.Second

	// defaultDebounceDelay is how long a change notification is allowed to
	// collect company before the pool is diffed.
	//
	// Rule churn is bursty by nature: a GitOps apply of twenty rules, or one
	// reconcile writing a rule's targets while another deletes them, all land
	// within milliseconds. Diffing once per burst instead of once per
	// notification is the difference between one List per new scope and several.
	// The window is fixed rather than sliding — a continuous stream of
	// notifications must not be able to starve the diff indefinitely — so the
	// worst-case latency from "rule applied" to "informer starting" is bounded by
	// this value.
	defaultDebounceDelay = 500 * time.Millisecond

	// defaultSettleQuietPeriod is how long the registry must go without a change
	// notification before the desired state is declared settled (see Settled).
	//
	// It exists for exactly one consumer: Task 1.6's boot reconciliation, which
	// closes scope epochs the sink's history shows as open but that no rule wants
	// any more. Running that judgement too early is actively harmful — at process
	// start the reconcilers have not necessarily written their rules to the
	// registry yet, so every still-valid scope would look orphaned, be closed, and
	// be reopened seconds later. A quiet window (rather than a fixed delay from
	// boot) is what makes the wait proportionate: an operator with three rules is
	// settled almost immediately, while a GitOps apply of hundreds keeps the gate
	// shut until the churn stops. Five seconds is comfortably longer than the
	// debounce window and short enough that a genuinely orphaned scope is closed
	// while an operator is still looking at the deletion they just made.
	defaultSettleQuietPeriod = 5 * time.Second
)

// Pipeline is the data plane as the WatchManager needs it: somewhere to hand work
// keys, and somewhere to tell that a scope's cached state is now meaningless.
//
// It is an interface owned by this package (rather than a *pipeline.Pipeline
// field) for the same reason the pipeline declares ListerRegistry rather than
// importing this one: the two halves of the data plane meet only at these two
// method sets, so each is testable without the other. pipeline.Pipeline is the
// production implementation, asserted below.
type Pipeline interface {
	Enqueuer

	// EvictScope drops a stopped scope's dedup baselines for one sink. It emits no
	// records of any kind — "we stopped watching" is recorded once, as a
	// watch_scopes Stopped row, never as Deleted rows for the objects that were in
	// scope.
	EvictScope(sinkName string, scope pipeline.ScopeKey)
}

// ScopeTransition is one watch-scope edge as the WatchManager observes it: a
// single (sink, informer-target) interest started being served, or stopped being
// wanted.
//
// It is an interest-level edge, not a scope-level one. Deriving "the scope started"
// from it is the recorder's job (see ScopeRecorder), because that is where the
// transition semantics — one row per scope, whatever the number of contributing
// rules or informers — belong.
type ScopeTransition struct {
	// Sink is the ClickHouseSink name this interest streams to.
	Sink string

	// Scope is the version-agnostic (group, kind, namespace) triple the pipeline
	// evicts and warms by.
	Scope pipeline.ScopeKey

	// Target distinguishes two interests that share a Scope. Two rules naming
	// apps/v1 and apps/v2 of one resource are two informers but one
	// version-agnostic scope (Invariant 7), so a recorder that refcounted by
	// (sink, scope) alone could not tell their edges apart — and an unpaired
	// Stopped would then close a scope another interest still holds.
	Target string

	// APIVersion is the version this interest's informer watches. It is provenance
	// for the recorded row only; scope identity never includes it.
	APIVersion string

	// RuleKeys are the rules contributing this interest, sorted (the registry
	// guarantees the ordering), so a transition is always attributable.
	RuleKeys []string

	// At is when the manager observed the edge. It is stamped once per reconcile
	// pass and handed to the recorder rather than re-derived downstream, because
	// two consumers need to agree on it exactly: the recorded row's timestamp and
	// the cutoff the warm coordinator's epoch check uses (see
	// pipeline.WarmTarget.EpochStart). A later re-stamp would let the epoch check
	// see the very row this transition is about to write.
	At time.Time
}

// ScopeRecorder is handed watch-scope transitions as they happen: an interest in a
// (sink, scope) pair appeared, or vanished.
//
// The WatchManager is the only component that can observe these edges — it is
// where the desired-state registry meets running informers — but it deliberately
// knows nothing about how they are recorded, nor about the transition semantics
// that turn interest edges into scope epochs. Task 1.6's ScopeEpochRecorder
// consumes this, applies those semantics, and writes the scope log.
//
// Implementations MUST NOT block: these calls happen inline on the reconcile
// loop, between deregistering a scope and evicting its cache, and a recorder that
// waited on a sink round-trip would stall every other rule's watch lifecycle
// behind an unreachable database (Invariant 1). A recorder that cannot write yet
// must queue and retry on its own.
type ScopeRecorder interface {
	// ScopeStarted reports that an interest is now being served. It fires once the
	// informer serving the scope is actually running, so a recorded Started is
	// never a promise the data plane failed to keep.
	ScopeStarted(transition ScopeTransition)

	// ScopeStopped reports that an interest is no longer wanted. It fires after the
	// interest is deregistered (so no further event can be attributed to it) and
	// before its cache is evicted. It may fire for an interest no ScopeStarted was
	// ever reported for — one whose informer never came up — which is exactly why
	// ScopeTransition carries Target.
	ScopeStopped(transition ScopeTransition)
}

// nopRecorder is the default when no ScopeRecorder is supplied: watches start and
// stop correctly, they are simply not narrated to the sink. A no-op implementation
// keeps the call sites free of nil checks, and it is what a deployment with a
// Writer-only sink effectively runs with.
type nopRecorder struct{}

func (nopRecorder) ScopeStarted(ScopeTransition) {}
func (nopRecorder) ScopeStopped(ScopeTransition) {}

// Options configures a WatchManager. Everything without a documented default is
// mandatory.
type Options struct {
	// Registry is the desired-state registry the pool is level-triggered towards.
	// Required.
	Registry *plan.Registry

	// Resolver maps the GVKs rules are authored in onto the GVRs informers need.
	// Required. It is shared with the control-plane reconcilers so a kind that
	// failed to resolve is retried on one gate rather than two.
	Resolver *Resolver

	// Dynamic is the client every informer lists and watches through. Required.
	Dynamic dynamic.Interface

	// Pipeline receives the work keys informer events produce and owns the dedup
	// state a stopped scope's eviction drops. Required.
	Pipeline Pipeline

	// Recorder receives scope transitions. Nil means they are not recorded; watch
	// lifecycle is unaffected either way.
	Recorder ScopeRecorder

	// ResyncPeriod overrides defaultResyncPeriod. Zero or negative means the
	// default.
	ResyncPeriod time.Duration

	// DebounceDelay overrides defaultDebounceDelay. Zero or negative means the
	// default; tests shorten it so a debounce assertion does not wait on real
	// pacing.
	DebounceDelay time.Duration

	// SettleQuietPeriod overrides defaultSettleQuietPeriod, the quiet window that
	// decides when the desired state is considered settled (see Settled). Zero or
	// negative means the default.
	SettleQuietPeriod time.Duration

	// StopTimeout overrides how long stopping an informer waits for its goroutine
	// (stopWaitTimeout). Zero or negative means the default.
	StopTimeout time.Duration
}

// WatchManager owns kuberecord's data-plane watches: a pool of self-managed
// dynamic informers, level-triggered towards the desired-state registry, plus the
// interest map that turns one informer's events into per-sink work keys.
//
// It exists in this shape because controller-runtime cannot provide it. A
// manager's cache is scoped once, at construction, and a controller cannot be
// removed from a running manager — but Phase 1's whole premise is that rules
// come and go at runtime without an operator restart. So the informers are
// hand-built, each with its own context and goroutine, and this type is the only
// thing that starts or stops them.
//
// It is also the production pipeline.ListerRegistry: the pool's indexers are the
// only view of cluster state the data plane has, and the interest map is what
// answers "is this scope still being watched?" — the question that keeps a
// stopped rule from being recorded as a mass deletion.
type WatchManager struct {
	registry *plan.Registry
	resolver *Resolver
	pipeline Pipeline
	recorder ScopeRecorder

	table *interestTable
	pool  *pool

	resyncPeriod      time.Duration
	debounceDelay     time.Duration
	settleQuietPeriod time.Duration

	// settled is closed once the desired state has been quiet for
	// settleQuietPeriod (see Settled). It is closed from the reconcile loop
	// goroutine only, guarded by settleDone so a second close can never panic.
	settled    chan struct{}
	settleDone bool

	// startedTargets are the (informer, sink) pairs a ScopeStarted has already
	// been reported for. It is read and written only from the reconcile loop
	// goroutine, so it needs no lock.
	//
	// It is tracked here rather than derived from the interest table's own diff
	// because an interest can be installed before the informer serving it manages
	// to start (a failed start is retried on the next pass): keying the
	// notification off "is it actually running now?" is what stops a Started from
	// being reported for a watch that never came up, and from being lost when the
	// retry succeeds.
	startedTargets map[interestID]struct{}

	// poolDiffs counts completed diff passes. It exists so the debounce test can
	// prove a burst of notifications collapses into exactly one pass; nothing
	// about behaviour depends on it.
	poolDiffs atomic.Int64
}

// New builds a WatchManager. The mandatory dependencies are validated eagerly
// because every one of them would otherwise surface as a nil-pointer panic inside
// an informer goroutine or a reconcile loop — long after the wiring mistake, and
// somewhere a stack trace is much harder to read than a startup error.
func New(opts Options) (*WatchManager, error) {
	if opts.Registry == nil {
		return nil, errors.New("watch: Options.Registry is required")
	}
	if opts.Resolver == nil {
		return nil, errors.New("watch: Options.Resolver is required")
	}
	if opts.Dynamic == nil {
		return nil, errors.New("watch: Options.Dynamic is required")
	}
	if opts.Pipeline == nil {
		return nil, errors.New("watch: Options.Pipeline is required")
	}

	recorder := opts.Recorder
	if recorder == nil {
		recorder = nopRecorder{}
	}
	resync := opts.ResyncPeriod
	if resync <= 0 {
		resync = defaultResyncPeriod
	}
	debounce := opts.DebounceDelay
	if debounce <= 0 {
		debounce = defaultDebounceDelay
	}
	settleQuiet := opts.SettleQuietPeriod
	if settleQuiet <= 0 {
		settleQuiet = defaultSettleQuietPeriod
	}

	table := newInterestTable()
	m := &WatchManager{
		registry:          opts.Registry,
		resolver:          opts.Resolver,
		pipeline:          opts.Pipeline,
		recorder:          recorder,
		table:             table,
		pool:              newPool(opts.Dynamic, table, opts.Pipeline, logf.Log.WithName("watch")),
		resyncPeriod:      resync,
		debounceDelay:     debounce,
		settleQuietPeriod: settleQuiet,
		settled:           make(chan struct{}),
		startedTargets:    make(map[interestID]struct{}),
	}
	if opts.StopTimeout > 0 {
		m.pool.stopTimeout = opts.StopTimeout
	}
	return m, nil
}

// Start runs the reconcile loop until ctx is cancelled, then stops every
// informer. It satisfies manager.Runnable.
//
// The loop is level-triggered, never edge-triggered: a wake-up (a registry change
// or the safety tick) only means "look again", and the response is always to take
// a fresh snapshot and move the pool towards it. That is the property that makes
// the data plane self-correcting — a missed notification, a failed informer start,
// or a kind that did not exist a minute ago all resolve on a later pass without
// anybody having to replay an event.
//
// Shutdown deliberately does *not* evict scopes or report them stopped. A process
// exiting is not a rule going away: the in-memory dedup state disappears with the
// process anyway (Invariant 6), and writing a Stopped row here would turn every
// restart into a spurious scope epoch — the exact audit lie scope epochs exist to
// prevent, just in the other direction. Reconciling scopes that really did go away
// while the operator was down is Task 1.6's boot pass.
func (m *WatchManager) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("watch")
	// The pool's event path has no context to carry a logger, so it holds one;
	// this is the single point where it picks up the manager's.
	m.pool.log = log
	// Every helper below reads its logger back out of the context rather than
	// taking it as a second argument, which keeps one decorated logger in play
	// without threading it through every signature.
	ctx = logf.IntoContext(ctx, log)
	log.Info("Starting watch manager",
		"resync_period", m.resyncPeriod.String(), "debounce_delay", m.debounceDelay.String())

	changes := m.registry.Changes()
	ticker := time.NewTicker(m.resyncPeriod)
	defer ticker.Stop()

	// The settle timer runs from the moment the loop starts and is restarted by
	// every change notification, so it fires only once the registry has been quiet
	// for settleQuietPeriod. Go 1.23 onwards guarantees Reset leaves no stale value
	// buffered in the channel, so a notification arriving just as the timer fires
	// cannot produce a premature settle.
	settle := time.NewTimer(m.settleQuietPeriod)
	defer settle.Stop()

	// One pass before waiting on anything: rules that already existed when this
	// runnable started (the common case after a restart, since reconcilers run
	// concurrently with it) have already posted their notification, and it may
	// well have been coalesced away.
	m.reconcilePool(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping watch manager", "informers", m.pool.size())
			m.pool.stopAll()
			log.Info("Watch manager stopped")
			return nil
		case <-changes:
			if !m.settleDone {
				settle.Reset(m.settleQuietPeriod)
			}
			if !m.debounce(ctx, changes) {
				// Cancelled mid-window; let the ctx.Done branch shut down.
				continue
			}
			m.reconcilePool(ctx)
		case <-settle.C:
			m.markSettled(log)
		case <-ticker.C:
			m.reconcilePool(ctx)
		}
	}
}

// Settled is closed once the desired state has been quiet for the settle quiet
// period — i.e. every rule that already existed when this process started has had
// its chance to reach the registry.
//
// It exists for Task 1.6's boot reconciliation, which is the one judgement in the
// operator that is only safe to make against a complete desired state: it closes
// scope epochs that history shows as open but that nothing wants any more, and an
// incomplete desired state would make it close scopes whose rules simply had not
// been reconciled yet. Every other part of the data plane is level-triggered and
// therefore needs no such signal.
//
// The channel is never closed if the manager never runs, which is correct: a
// non-leader has no desired state of its own to settle, and the boot pass is
// leader-gated for the same reason the watches are.
func (m *WatchManager) Settled() <-chan struct{} { return m.settled }

// markSettled closes the settle channel exactly once. It runs only on the
// reconcile loop goroutine, so the guard needs no lock.
func (m *WatchManager) markSettled(log logr.Logger) {
	if m.settleDone {
		return
	}
	m.settleDone = true
	close(m.settled)
	log.Info("Desired state settled", "quiet_period", m.settleQuietPeriod.String(),
		"interests", m.table.size(), "informers", m.pool.size())
}

// NeedLeaderElection makes the WatchManager a manager.LeaderElectionRunnable that
// runs only on the elected leader.
//
// Two operator pods both watching every rule's scopes would both feed their own
// pipelines and both write to the sink, doubling every row and racing each other's
// dedup state — the dedup caches are per-process (Invariant 6), so neither would
// see the other's writes. Verified against the pinned controller-runtime
// (v0.23.3, pkg/manager/runnable_group.go): a runnable implementing this
// interface with true is added to the leader-election group and started only once
// the lease is held. With --leader-elect=false there is no lease and the group
// starts immediately, which is correct for a single-replica deployment.
func (m *WatchManager) NeedLeaderElection() bool { return true }

// Get answers the pipeline's only question about cluster state: what does the
// watch cache hold for this identity, and is its scope still being watched?
//
// scopeActive comes from the interest map, not from the pool: a scope is active
// for a sink exactly as long as some rule wants it for that sink, which is a
// different question from "is an informer running" (one informer serves every sink
// interested in its resource). A false answer is what makes the pipeline drop a
// work item that outlived its rule instead of recording the object as deleted.
//
// found=false with scopeActive=true is the pipeline's delete trigger and is
// answered from the informer's indexer, which client-go updates strictly before
// it notifies handlers — so a not-found answer for a key that arrived through a
// handler genuinely means the object is gone.
//
// Selectors are deliberately not applied here. They are an enqueue-side filter
// (see scopeInterest.matchesEither); re-applying them on the read side would turn
// an object that just left a rule's selector into a phantom deletion.
//
// The returned object is the informer's own cached instance, shared with every
// other reader in the process. The interface documents that callers must not
// mutate it, and the pipeline deep-copies before normalizing, so no defensive copy
// is made on this path.
func (m *WatchManager) Get(ref pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	interests := m.table.lookupIdentity(ref)
	if len(interests) == 0 {
		return nil, false, false, nil
	}

	cacheKey := ref.Name
	if ref.Namespace != "" {
		cacheKey = ref.Namespace + "/" + ref.Name
	}

	// missingInformer records that a scope is registered but its informer is not
	// running yet — the brief window between a pool diff installing an interest
	// and starting the informer for it. Reporting "not found" then would be a lie
	// with teeth (the pipeline's delete path), so it is reported as a retryable
	// error instead, but only after every other candidate has been consulted.
	var missingInformer *scopeInterest

	for _, in := range interests {
		entry, running := m.pool.entryFor(in.informer)
		if !running {
			missingInformer = in
			continue
		}
		obj, exists, err := entry.informer.GetIndexer().GetByKey(cacheKey)
		if err != nil {
			return nil, false, true, fmt.Errorf("look up %q in the %s watch cache: %w", cacheKey, in.informer, err)
		}
		if !exists {
			continue
		}
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, false, true, fmt.Errorf("the %s watch cache holds a %T for %q, want *unstructured.Unstructured",
				in.informer, obj, cacheKey)
		}
		return u, true, true, nil
	}

	if missingInformer != nil {
		return nil, false, true, fmt.Errorf("no informer is running yet for %s: %w",
			missingInformer.informer, errInformerNotRunning)
	}
	return nil, false, true, nil
}

// errInformerNotRunning is the retryable condition behind a lookup against a
// registered scope whose informer has not started yet. It is a sentinel so a
// future caller can classify it; today every caller simply retries.
var errInformerNotRunning = errors.New("watch scope is registered but its informer is not running")

// RedactionFor returns the redaction policy ref's objects must be scrubbed with
// before they are hashed and written. It implements pipeline.RedactionRegistry.
//
// It answers from the very same interest table — and the same two-step
// exact-namespace-then-cluster-wide lookup — that Get uses to decide whether
// ref's scope is active at all. That is what makes the two answers consistent by
// construction: an object the pipeline is allowed to write is always one whose
// policy is installed, and ok=false means precisely "no interest covers this any
// more", which the pipeline never writes through.
//
// Interests are merged rather than picked between when more than one answers,
// because more than one is exactly the ambiguous case: a namespaced rule and a
// cluster-wide rule, or two API versions of one resource, all landing on one
// hashCache entry. Merging is a union (see pipeline.MergeRedaction), so no
// interest's presence can weaken another's policy.
func (m *WatchManager) RedactionFor(ref pipeline.Key) (*pipeline.RedactionPolicy, bool) {
	interests := m.table.lookupIdentity(ref)
	switch len(interests) {
	case 0:
		return nil, false
	case 1:
		// The overwhelmingly common case: hand back the policy compiled at
		// pool-diff time, allocating nothing on the hot path.
		return interests[0].redaction, true
	default:
		policies := make([]*pipeline.RedactionPolicy, 0, len(interests))
		for _, in := range interests {
			policies = append(policies, in.redaction)
		}
		return pipeline.MergeRedaction(policies...), true
	}
}

// ScopeDesired reports whether any rule currently wants this (sink, scope) pair.
// It implements half of pipeline.ScopeStates.
//
// The interest table — not the pool — is the authority, and deliberately so: the
// question a caller is really asking is "does the operator still intend to watch
// this?", which is true from the moment a rule names the scope, whether or not the
// informer serving it has managed to come up yet. Task 1.6's boot reconciliation
// depends on that reading: a false answer there closes a scope's epoch, so
// answering "no" for a scope whose informer is merely slow (or whose kind is not
// installed yet) would close an epoch the very next pass reopens.
func (m *WatchManager) ScopeDesired(sinkName string, scope pipeline.ScopeKey) bool {
	return len(m.table.interestsForScope(sinkName, scope)) > 0
}

// ScopeSynced reports whether every informer serving this (sink, scope) pair has
// completed its initial List. It implements the other half of
// pipeline.ScopeStates.
//
// It is the gate on the per-scope zombie GC pass, so it is deliberately
// conservative in both directions: a scope nothing is registered for is *not*
// synced (there is no cache to compare against), and a scope served by two
// informers — two versions of one resource, one version-agnostic scope — is synced
// only when both are, since a seeded object may live in either one's indexer.
// Reporting a premature "yes" would make every seeded object look absent and turn
// the GC pass into a mass-deletion event.
func (m *WatchManager) ScopeSynced(sinkName string, scope pipeline.ScopeKey) bool {
	interests := m.table.interestsForScope(sinkName, scope)
	if len(interests) == 0 {
		return false
	}
	for _, in := range interests {
		entry, running := m.pool.entryFor(in.informer)
		if !running || !entry.informer.HasSynced() {
			return false
		}
	}
	return true
}

// PoolSize reports how many informers are currently running. It is exported for
// the operator's own diagnostics (Task 1.10 logs it at startup); the pool's
// internal counter is what the sharing tests assert.
func (m *WatchManager) PoolSize() int { return m.pool.size() }

// reconcilePool is one level-triggering pass: snapshot the registry, translate it
// into informer targets and interests, install those, and move the pool.
//
// The order is load-bearing. Interests are installed *first*, so an informer that
// starts immediately afterwards cannot deliver an event nobody is registered for
// (a silently missing object). Scopes that vanished are deregistered by that same
// installation and only *then* evicted, so an in-flight work item for a stopped
// scope sees scopeActive=false and drops instead of re-populating a cache entry
// that is about to be dropped. Informers are stopped after their interests are
// gone, so the last event any of them can deliver is already un-attributable.
func (m *WatchManager) reconcilePool(ctx context.Context) {
	log := logf.FromContext(ctx)
	snapshot := m.registry.Snapshot()
	desired, wanted := m.translate(snapshot, log)

	// One instant for the whole pass. Every transition it reports carries it, so
	// the recorded rows and the warm coordinator's epoch cutoff cannot disagree
	// about when this pass's edges happened (see ScopeTransition.At).
	at := time.Now().UTC()

	removed := m.table.replace(desired)
	for _, in := range removed {
		// Deregistered above; settle the state it leaves behind. The recorder is
		// told before the cache is evicted so the Stopped row's timestamp cannot
		// fall after a subsequent epoch's rows for the same scope.
		delete(m.startedTargets, in.id())
		m.recorder.ScopeStopped(in.transition(at))
		m.pipeline.EvictScope(in.sink, in.scope)
		log.Info("Watch scope stopped",
			"sink", in.sink, "group", in.scope.Group, "kind", in.scope.Kind,
			"namespace", in.scope.Namespace, "rules", in.ruleKeys)
	}

	m.pool.retain(ctx, wanted)

	for _, in := range m.newlyServing(desired) {
		m.startedTargets[in.id()] = struct{}{}
		m.recorder.ScopeStarted(in.transition(at))
		log.Info("Watch scope started",
			"sink", in.sink, "group", in.scope.Group, "kind", in.scope.Kind,
			"namespace", in.scope.Namespace, "rules", in.ruleKeys)
	}

	m.poolDiffs.Add(1)
}

// newlyServing returns the desired interests whose informer is now running and
// whose start has not been reported yet, sorted for stable log and recorder
// ordering.
//
// Keying the report off "is it actually running?" rather than off the interest
// diff is what keeps a Started from being announced for a watch that failed to come
// up — and from being lost when the next pass's retry brings it up.
func (m *WatchManager) newlyServing(desired map[interestID]*scopeInterest) []*scopeInterest {
	var started []*scopeInterest
	for id, in := range desired {
		if _, reported := m.startedTargets[id]; reported {
			continue
		}
		if _, running := m.pool.entryFor(in.informer); !running {
			continue
		}
		started = append(started, in)
	}
	sortInterests(started)
	return started
}

// translate turns a registry snapshot into the interest set and the informer
// targets that serve it.
//
// A target that cannot be translated is skipped, never fatal: one rule naming a
// kind that is not installed (or that resolves to a scope it may not watch) must
// not stop every other rule from streaming (Invariant 5). The owning CR's
// Degraded condition is the operator-facing report — Task 1.7 raises it from the
// same resolver verdict — so this path logs rather than duplicating that
// judgement.
func (m *WatchManager) translate(snapshot map[plan.TargetKey]plan.TargetState,
	log logr.Logger) (map[interestID]*scopeInterest, map[informerKey]schema.GroupVersionKind) {
	desired := make(map[interestID]*scopeInterest, len(snapshot))
	wanted := make(map[informerKey]schema.GroupVersionKind, len(snapshot))

	for key, state := range snapshot {
		gvr, namespaced, err := m.resolver.Resolve(key.GVK)
		if err != nil {
			m.logResolveFailure(log, key, err)
			continue
		}
		if key.Namespace != "" && !namespaced {
			// The control plane rejects this before it ever reaches the registry
			// (a namespaced rule cannot watch a cluster-scoped kind), so reaching
			// it here means the two tiers disagree — worth an Error, not a silent
			// namespace-scoped List that would return nothing forever.
			log.Error(errClusterScopedTarget, "Refusing a namespaced watch target for a cluster-scoped resource",
				"sink", key.Sink, "gvk", key.GVK.String(), "namespace", key.Namespace, "rules", state.RuleKeys)
			continue
		}

		informer := informerKey{GVR: gvr, Namespace: key.Namespace}
		in, err := newScopeInterest(key, informer, state.Selectors, state.Redactions, state.RuleKeys)
		if err != nil {
			log.Error(err, "Skipping a watch target whose selectors or redaction policy could not be parsed",
				"sink", key.Sink, "gvk", key.GVK.String(), "namespace", key.Namespace, "rules", state.RuleKeys)
			continue
		}

		desired[in.id()] = in
		wanted[informer] = key.GVK
	}
	return desired, wanted
}

// errClusterScopedTarget backs the log line above; nothing branches on it.
var errClusterScopedTarget = errors.New("a namespaced watch target cannot be served by a cluster-scoped resource")

// logResolveFailure reports a target whose kind could not be resolved, at the
// severity the verdict deserves.
//
// A kind that is simply not installed yet is an ordinary, self-healing state — a
// rule may be applied before the CRD it names, and the resolver's own backoff gate
// keeps the retries cheap — so it is reported at Info; the rule's own Degraded
// condition is where an operator is meant to see it. Anything else means discovery
// itself is failing, which is an anomaly and logs at Error (Invariant 4).
func (m *WatchManager) logResolveFailure(log logr.Logger, key plan.TargetKey, err error) {
	values := []any{"sink", key.Sink, "gvk", key.GVK.String(), "namespace", key.Namespace}

	var notFound *ErrKindNotFound
	if errors.As(err, &notFound) {
		log.Info("Watch target's kind is not installed in this cluster, not watching it yet",
			append(values, "reason", ReasonKindNotFound)...)
		return
	}
	log.Error(err, "Failed to resolve a watch target's kind", values...)
}

// debounce holds off the next diff pass for debounceDelay, swallowing further
// change notifications, and reports whether the window completed (false means ctx
// was cancelled).
//
// The window is fixed, not sliding: notifications arriving inside it are drained
// but do not extend it, so a cluster with continuously churning rules still gets a
// pool diff every debounceDelay instead of starving until the churn stops. Because
// the registry's channel coalesces (capacity 1, non-blocking send), draining it
// once per iteration is enough to absorb a burst of any size.
func (m *WatchManager) debounce(ctx context.Context, changes <-chan struct{}) bool {
	timer := time.NewTimer(m.debounceDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-changes:
		case <-timer.C:
			return true
		}
	}
}

// Compile-time proof of the two contracts this type exists to satisfy: it is the
// production ListerRegistry the pipeline reads cluster state through (Task 1.5
// defines it and ships a fake), and it is a leader-election-gated
// manager.Runnable. Both are asserted here rather than discovered at wiring time
// in Task 1.10, where a signature drift would surface as a compile error in a file
// that has nothing to do with either contract.
var (
	_ pipeline.ListerRegistry        = (*WatchManager)(nil)
	_ pipeline.ScopeStates           = (*WatchManager)(nil)
	_ manager.Runnable               = (*WatchManager)(nil)
	_ manager.LeaderElectionRunnable = (*WatchManager)(nil)
	_ Pipeline                       = (*pipeline.Pipeline)(nil)
)
