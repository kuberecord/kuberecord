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

// Package pipeline is kubestream's data plane: a client-go workqueue drained by
// a pool of workers that run the normalize → hash → dedup → diff →
// version-gated-commit machinery for every observed object change.
//
// It exists as a package (rather than as controller-runtime reconcilers) for one
// reason: **Invariant 2, per-key serialization.** The whole hashCache design —
// Reserve, CommitIfCurrent, DeleteIfCurrent, UnclaimDelete — documents itself as
// depending on the guarantee that no two workers ever process the same object
// identity concurrently. controller-runtime provided that guarantee only at
// MaxConcurrentReconciles=1, which capped the operator's throughput at one
// object at a time per kind. client-go's workqueue provides it *contractually*
// at any worker count: an item handed to a worker is not eligible for another
// worker until Done is called for it, and re-adds during that window collapse
// into a single later delivery. That is why the queue may never be replaced by a
// naive channel fan-out, however tempting the simplification looks.
//
// The package depends on two interfaces it does not implement — ListerRegistry
// (the watch cache's view of reality, implemented by internal/watch's
// WatchManager in Task 1.4) and SinkRouter (name → live sink.Writer,
// implemented by the SinkManager in Task 1.8). That inversion keeps the hot path
// free of any informer or backend detail: a pipeline test needs neither an
// apiserver nor a database.
package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// DefaultWorkers is the shipped worker count: the number of goroutines draining
// the queue, shared across *every* watch target rather than sized per GVK. The
// old per-GVK MaxConcurrentReconciles knob is gone, and deliberately so — with
// one queue for the whole data plane, per-kind concurrency limits would just
// reserve idle capacity for quiet kinds while a busy kind backs up. 8 is a
// throughput/CPU compromise for a single operator pod; the operator-facing
// --pipeline-workers flag that overrides it is wired in Task 1.10 (nothing can
// construct a Pipeline until the WatchManager and SinkManager exist).
//
// Raising it is safe for correctness at any value: per-key serialization comes
// from the workqueue contract, not from the worker count.
const DefaultWorkers = 8

// queueName labels this pipeline's queue in client-go's workqueue metrics.
const queueName = "kuberecord_pipeline"

// unavailableSinkLogInterval bounds how often a missing sink is logged at Error
// level. A sink that is deleted or mid-recycle can affect every queued key at
// once, and each of those keys is retried on the rate limiter, so an unthrottled
// log line per attempt would bury every other signal in the operator's output.
// One line per sink per interval is enough to see the condition; the
// re-add-and-retry behaviour itself is not an anomaly worth repeating.
const unavailableSinkLogInterval = 30 * time.Second

// errSinkUnavailable is returned by Process when the key's sink cannot be
// resolved to a live Writer — it is missing, being recycled after a credential
// change, or not yet started. It is a retryable condition, never a drop: the
// object's change is real and must still be recorded once the sink is back, so
// the worker re-adds the key through the rate limiter.
var errSinkUnavailable = errors.New("sink is not currently available")

// errAsyncWriteFailed is logged when a sink write exhausts its retries.
// The actual driver error is already logged inside the sink implementation;
// this sentinel just gives Process's log.Error calls a non-nil error value.
var errAsyncWriteFailed = errors.New("sink write did not succeed after retries")

// errRedactionUnavailable is returned by processUpsert when no redaction policy
// can be resolved for a key — the scope stopped being watched between the
// lister's scopeActive check and the lookup. It is retryable, never a write:
// the value of a redaction policy is entirely in never writing content without
// one.
var errRedactionUnavailable = errors.New("no redaction policy is installed for this scope")

// errBaselineCompression is logged when compressBaseline could not compress a
// diff baseline and fell back to storing raw bytes. It gives that Error log a
// non-nil error value; the fallback itself is a safe degradation (Invariant
// 5) — a raw baseline still diffs correctly, it just costs more memory.
var errBaselineCompression = errors.New("failed to compress diff baseline, stored raw")

// ListerRegistry answers "what does the watch cache currently hold for this
// identity, and is its scope still actively watched?" — the pipeline's only
// window onto cluster state. Task 1.4's WatchManager is the production
// implementation, backed by its informer indexers plus the interest map; this
// package ships a map-backed fake so no pipeline test needs an apiserver.
//
// Making this an interface is what keeps Invariant 1 enforceable by
// construction: there is no client here to issue a synchronous API round-trip
// with, so a worker physically cannot block the hot path on the apiserver.
type ListerRegistry interface {
	// Get returns the object the watch cache holds for ref's identity.
	//
	// found=false means the cache has no such object: it was deleted (or never
	// existed), which is the pipeline's delete trigger. scopeActive=false means
	// ref's watch target is no longer active for ref.Sink — the sink travels
	// with the ref precisely because activity is per-(sink, scope): one shared
	// informer can serve a scope that one rule dropped while another rule, on a
	// different sink, still holds it. err is reserved for genuine lookup
	// failures (a malformed cache key, an indexer error) and makes the worker
	// retry; "not there" is not an error.
	//
	// The returned object may be the informer's own cached instance, shared with
	// every other reader. Callers must not mutate it — Process deep-copies
	// before normalizing for exactly this reason — so implementations are free
	// to skip a defensive copy on the hot path.
	Get(ref Key) (obj *unstructured.Unstructured, found bool, scopeActive bool, err error)
}

// SinkRouter resolves a key's sink name to the live Writer currently serving it.
// Task 1.8's SinkManager is the production implementation; resolution happens
// per work item (rather than being captured once at wiring time) so a sink that
// is recycled after a credential rotation swaps in without the pipeline holding
// a stale Writer.
type SinkRouter interface {
	// WriterFor returns the Writer for name, or ok=false when no live instance
	// exists — the sink's CR was deleted, or the instance is mid-recycle. False
	// is an ordinary, transient answer, never a reason to panic: the caller
	// re-queues the key on the rate limiter and retries.
	WriterFor(name string) (sink.Writer, bool)
}

// RedactionRegistry answers "what must be scrubbed out of this key's objects
// before they are hashed and written?" — the projection of every contributing
// rule's redaction policy, merged with its sink's (Task 3.3).
//
// It is a separate interface from ListerRegistry, rather than a fifth return
// value on Get, because the two answer questions of different lifetimes: the
// object changes on every event, while the policy changes only when a CR is
// edited. Task 1.4's WatchManager implements both, off the same interest table,
// so the policy a work item is redacted under is always the one the interest
// that made its scope active declares.
//
// ok=false means no interest currently covers the key — the scope stopped being
// watched — and is never an answer the pipeline writes through: see
// errRedactionUnavailable.
type RedactionRegistry interface {
	RedactionFor(ref Key) (policy *RedactionPolicy, ok bool)
}

// Options configures a Pipeline. Only Lister and Router are mandatory; the rest
// have documented defaults so cmd/main.go's wiring (Task 1.10) stays a short,
// readable struct literal.
type Options struct {
	// ClusterID identifies this operator's cluster in every row written
	// (Invariant 7: cluster_id is explicit in the schema, implicit in-process).
	ClusterID string
	// Workers is the number of goroutines draining the queue. Zero or negative
	// means DefaultWorkers.
	Workers int
	// Lister is the watch cache's view of reality. Required.
	Lister ListerRegistry
	// Router resolves a key's sink to a live Writer. Required.
	Router SinkRouter
	// Redactions resolves a key's redaction policy. Nil means no rule-configured
	// redaction exists in this process, and every object is scrubbed with the
	// built-in policy alone (see RedactionPolicy.Apply) — never with nothing.
	Redactions RedactionRegistry
	// Metrics collects the pipeline's Prometheus series. Nil means the
	// process-wide instance (PipelineMetricsInstance), which is what production
	// wants; tests pass an isolated instance built on their own registry.
	Metrics *PipelineMetrics
	// RateLimiter governs the delay before a failed key is retried. Nil means
	// client-go's default controller rate limiter (5ms → 1000s exponential per
	// item, plus a global 10 qps/100 burst bucket); see New for why that default
	// is the shipped choice and what it costs at scale. Tests substitute a faster
	// one so a retry assertion doesn't wait on real backoff.
	RateLimiter workqueue.TypedRateLimiter[Key]
}

// Pipeline is the data plane: a rate-limiting workqueue of identity Keys, a
// pool of workers draining it through Process, and the per-sink dedup state
// that Process settles its writes against.
//
// It is a manager.Runnable (see Start), so the controller-runtime manager owns
// its lifecycle and shutdown ordering — the queue drains before the process
// exits, and the sink's own drain (see sink.Writer.Start) then flushes whatever
// the workers handed off.
type Pipeline struct {
	clusterID  string
	workers    int
	lister     ListerRegistry
	router     SinkRouter
	redactions RedactionRegistry
	metrics    *PipelineMetrics
	queue      workqueue.TypedRateLimitingInterface[Key]

	// sinks holds the per-sink hashCache + warm-scope set. Nothing outside this
	// package touches the map; the lifecycle hooks (RemoveSink, EvictScope,
	// MarkScopeWarm) are the entire external surface.
	sinks *sinkStateRegistry

	// unavailableSinkLog throttles the "sink is not available" Error log so a
	// deleted sink cannot turn every retry into a log line.
	unavailableSinkLog *logThrottle

	// processFn is the function the workers call for each item. It is a field
	// only so tests can wrap Process (e.g. to make it artificially slow while
	// asserting the per-key serialization contract) without reaching into the
	// worker loop. Production always leaves it as Process.
	processFn func(ctx context.Context, key Key) error
}

// New builds a Pipeline. It validates the two mandatory dependencies eagerly
// rather than tolerating nils, because a nil Lister or Router would surface as a
// panic inside a worker goroutine — long after the wiring mistake was made, and
// on the hot path rather than at startup.
func New(opts Options) (*Pipeline, error) {
	if opts.Lister == nil {
		return nil, errors.New("pipeline: Options.Lister is required")
	}
	if opts.Router == nil {
		return nil, errors.New("pipeline: Options.Router is required")
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = PipelineMetricsInstance()
	}
	rateLimiter := opts.RateLimiter
	if rateLimiter == nil {
		// The shipped retry pacing is client-go's DefaultTypedControllerRateLimiter,
		// unchanged — and that is a recorded decision (Task 2.3), not an inherited
		// default nobody looked at.
		//
		// It is the max of two limiters, and only AddRateLimited passes through
		// them. Add — the path every informer event takes — is never delayed, so
		// nothing here paces normal streaming; this is exclusively about what
		// happens after a *failure*:
		//
		//   - per item, exponential 5 ms → 1000 s. A key whose write keeps failing
		//     backs off to a ~17-minute ceiling instead of spinning on a dead
		//     backend, and a settled item's Forget clears the accumulated penalty
		//     (see processNext), so a transient failure leaves no lasting tax.
		//   - overall, a 10 qps / 100 burst token bucket. This is what shapes a
		//     *mass* retry: a sink outage fails every in-flight write at once, and
		//     without it the entire working set would re-arrive together and
		//     hammer a backend that is already unhealthy.
		//
		// The consequence at the massive profile's scale (20,000 objects, see
		// docs/PERFORMANCE.md) is that re-delivery after a total sink outage is
		// paced at 10 keys/second, so the tail of a full recovery is on the order
		// of half an hour. That is accepted, because it is a latency window and
		// never a loss window: the pipeline is level-triggered, so whenever a key
		// does come back around it writes the object's *current* state, and any
		// object that changes again in the meantime is re-enqueued immediately by
		// its own informer event through the undelayed Add path. Raising the bucket
		// would buy a faster tail in exchange for a thundering herd against a
		// backend that has just recovered — the failure mode the chaos suite
		// (Task 2.1) exists to keep out.
		rateLimiter = workqueue.DefaultTypedControllerRateLimiter[Key]()
	}

	p := &Pipeline{
		clusterID:  opts.ClusterID,
		workers:    workers,
		lister:     opts.Lister,
		router:     opts.Router,
		redactions: opts.Redactions,
		metrics:    metrics,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(rateLimiter,
			workqueue.TypedRateLimitingQueueConfig[Key]{Name: queueName}),
		sinks:              newSinkStateRegistry(metrics),
		unavailableSinkLog: &logThrottle{interval: unavailableSinkLogInterval},
	}
	p.processFn = p.Process
	return p, nil
}

// Add enqueues a key for processing. It is the data plane's single entry point:
// Task 1.4's informer handlers call it (once per interested sink) for every
// Add/Update/Delete, and the failure paths inside Process re-add through the
// rate limiter. Adding a key that is already pending is free — the workqueue
// coalesces it, which is what turns an update storm on one object into one
// Process call per settled state rather than one per event.
//
// It never blocks on a worker and never fails: after shutdown the workqueue
// drops adds, which is correct — a process that is exiting has no business
// starting new work, and the pipeline's state is reconstructible from the
// Kubernetes API plus the sink (Invariant 6).
func (p *Pipeline) Add(key Key) {
	p.queue.Add(key)
}

// QueueLen reports how many work items are waiting to be picked up — the data
// plane's backlog, and the only figure that distinguishes "the pipeline kept up"
// from "the pipeline fell behind but the sink's hand-off queue never noticed."
//
// It exists for diagnostics: the load harness (Task 2.3) samples it to publish a
// peak-backlog figure per scale profile, since the sink-side write_queue_depth
// gauge only describes the last hop. It deliberately does not become a metric —
// client-go's own workqueue collectors already expose depth to Prometheus when a
// metrics provider is registered, and a second gauge for the same number would
// invite the two to disagree.
func (p *Pipeline) QueueLen() int {
	return p.queue.Len()
}

// Start runs the worker pool until ctx is cancelled, then shuts the queue down
// and waits for every worker to finish the item it holds. It satisfies
// manager.Runnable.
//
// Shutdown order matters: ShutDown makes Get return shutdown=true *after* the
// queue drains, so workers finish the already-queued items instead of abandoning
// them, and each worker's final Done keeps the "never two workers on one key"
// contract intact right through to exit. Only then does Start return, which is
// what lets the manager stop the sink's writer afterwards (Invariant 1's
// hand-off is asynchronous, so the sink's own drain is what persists the last
// records).
func (p *Pipeline) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("pipeline")
	log.Info("Starting pipeline workers", "workers", p.workers)

	// Cancellation is watched in its own goroutine because queue.Get blocks: a
	// worker parked on an empty queue has no other way to learn that ctx is
	// done. ShutDown is what unblocks every parked Get at once.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		p.queue.ShutDown()
	}()

	var wg sync.WaitGroup
	for range p.workers {
		wg.Go(func() {
			for p.processNext(ctx) {
			}
		})
	}
	wg.Wait()

	// The watcher goroutine may still be parked on ctx.Done() if the queue was
	// shut down by some other means; wait for it so Start never leaves a
	// goroutine behind (a goleak-verified property).
	<-shutdownDone

	log.Info("Pipeline workers stopped")
	return nil
}

// processNext handles exactly one queue item, returning false once the queue is
// shut down and drained (which ends the calling worker).
//
// The Get/Done pairing is the load-bearing part: between them, this key cannot
// be handed to any other worker, and any re-add for it is held until Done —
// which is precisely the serialization hashCache's version gating assumes. Done
// is deferred so it survives even an unexpected panic in Process, because a
// missing Done would strand that key forever (the queue would consider it
// permanently in flight).
func (p *Pipeline) processNext(ctx context.Context) bool {
	key, shutdown := p.queue.Get()
	if shutdown {
		return false
	}
	defer p.queue.Done(key)

	if err := p.processFn(ctx, key); err != nil {
		// A retryable failure: the object's change is still unrecorded, so the
		// key goes back on the rate limiter rather than being dropped. The
		// error itself was already logged with full context at the point it
		// occurred, so this stays silent to avoid double-logging every failure.
		p.queue.AddRateLimited(key)
		return true
	}
	// Settled (written, deduplicated, or deliberately dropped): clear this key's
	// accumulated backoff so a future, unrelated failure starts from the base
	// delay instead of inheriting an old exponential penalty.
	p.queue.Forget(key)
	return true
}

// redactionFor resolves the policy a key's object must be scrubbed with.
//
// A pipeline wired without a RedactionRegistry reports the built-in policy (nil)
// for every key, and always succeeds: with no registry there is no notion of a
// scope's policy to have lost, so failing closed would stall every write in a
// test or a load harness while protecting nothing.
func (p *Pipeline) redactionFor(key Key) (*RedactionPolicy, bool) {
	if p.redactions == nil {
		return nil, true
	}
	return p.redactions.RedactionFor(key)
}

// RemoveSink discards all pipeline state for a sink: its hashCache and its warm
// scopes. Task 1.8's SinkManager calls it after draining and closing a deleted
// sink's Writer, so the operator does not hold a growing set of dedup baselines
// for a backend it no longer writes to. Recreating the sink later starts from an
// empty cache and re-warms from that sink's own history (Invariant 6: in-memory
// state is always reconstructible from the API plus the sink).
//
// It is safe to call for a name the pipeline never saw.
func (p *Pipeline) RemoveSink(name string) {
	p.sinks.remove(name)
}

// EvictScope drops the cached baselines and warm marker for one watch scope on
// one sink. Task 1.4's WatchManager calls it when it stops a target, for each
// sink that was interested in it.
//
// Eviction emits no records of any kind: "we stopped watching" is recorded once,
// as a watch_scopes Stopped row by the scope recorder (Task 1.6), never as
// Deleted rows for the objects that were in scope. Keeping the entries instead
// would be worse than a leak — a stale hash could later suppress a genuine
// change if the same scope is watched again.
func (p *Pipeline) EvictScope(sinkName string, scope ScopeKey) {
	p.sinks.evictScope(sinkName, scope)
}

// MarkScopeWarm records that a (sink, scope) pair's dedup cache has been seeded
// from that sink's durable history, so a cache-miss for it can be trusted to
// mean "genuinely new" and is tagged Added rather than Snapshot. Task 1.6's
// per-scope warm-up coordinator is the caller.
//
// Until it is called, the scope is *not* warm — the readiness set starts empty
// on purpose. An un-warmed scope degrades to Snapshot-tagging (see
// process.go), which is the safe direction: a Snapshot row that should have been
// an Added is a cosmetic imprecision, whereas an Added row for an object the
// sink already knows about is a duplicate-write storm at cluster scale.
func (p *Pipeline) MarkScopeWarm(sinkName string, scope ScopeKey) {
	p.sinks.markScopeWarm(sinkName, scope)
}

// sinkState is everything the pipeline remembers about one sink: the dedup cache
// keyed by identity across all its GVKs, which of its scopes have been warmed,
// and the reincarnation close-outs still awaiting a successful write.
//
// It is per-sink because dedup and version state must be independent when the
// same object streams to two sinks: a write confirmed on sink A says nothing
// about whether sink B has that state, so a shared cache would let A's success
// silently suppress B's row.
type sinkState struct {
	cache     hashCache
	closeOuts closeOutRetryQueue

	// mu guards warm only. It is separate from hashCache's own mutex because the
	// two are read on different paths and at different rates; sharing one lock
	// would put every Snapshot-tagging check behind the cache's hot path.
	mu   sync.RWMutex
	warm map[ScopeKey]struct{}
}

// scopeWarm reports whether key's scope has been warmed for this sink.
//
// Both the key's exact namespace and the all-namespaces scope for its kind are
// consulted, because a scope's namespace is a property of the *rule*, not of the
// object: a ClusterStreamRule with no namespaceSelector warms one
// ScopeKey{group, kind, ""} covering every namespace, and objects arriving under
// it carry a concrete namespace that would otherwise never match.
func (s *sinkState) scopeWarm(key Key) bool {
	scope := key.Scope()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.warm[scope]; ok {
		return true
	}
	scope.Namespace = ""
	_, ok := s.warm[scope]
	return ok
}

// sinkStateRegistry maps sink name → sinkState, creating an entry lazily on a
// sink's first work item. Lazy creation (rather than a registration call from
// the SinkManager) keeps the two components decoupled: the pipeline needs no
// notification that a sink now exists, only that a key names it.
type sinkStateRegistry struct {
	mu      sync.Mutex
	states  map[string]*sinkState
	metrics *PipelineMetrics
}

func newSinkStateRegistry(metrics *PipelineMetrics) *sinkStateRegistry {
	return &sinkStateRegistry{states: make(map[string]*sinkState), metrics: metrics}
}

// get returns name's state, creating it on first use.
func (r *sinkStateRegistry) get(name string) *sinkState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.states[name]; ok {
		return st
	}
	st := &sinkState{warm: make(map[ScopeKey]struct{})}
	r.states[name] = st
	return st
}

// lookup returns name's state without creating it, so lifecycle hooks for an
// unknown sink are no-ops instead of allocating state for a sink that is on its
// way out.
func (r *sinkStateRegistry) lookup(name string) (*sinkState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.states[name]
	return st, ok
}

func (r *sinkStateRegistry) remove(name string) {
	r.mu.Lock()
	delete(r.states, name)
	r.mu.Unlock()
	// Delete the sink's series too: leaving them behind would report a stale entry
	// count, queue depth and capacity for a sink that no longer exists — which
	// reads as a live-but-idle backend rather than an absent one.
	r.metrics.hashcacheEntries.DeleteLabelValues(name)
	r.metrics.deleteSinkSeries(name)
}

func (r *sinkStateRegistry) evictScope(name string, scope ScopeKey) {
	st, ok := r.lookup(name)
	if !ok {
		return
	}

	st.mu.Lock()
	delete(st.warm, scope)
	st.mu.Unlock()
	r.metrics.safeMode.DeleteLabelValues(name, scope.Group, scope.Kind, scope.Namespace)

	// Entry removal happens outside every lock the metric touches: DeletePrefix
	// takes and releases the cache's own mutex, and the gauge Set below runs
	// strictly after it (no metric call ever runs while a hashCache lock is
	// held — a Task 0.1 acceptance criterion).
	removed := st.cache.DeletePrefix(scope.scopeKeyPrefix())
	r.metrics.hashcacheEntries.WithLabelValues(name).Set(float64(st.cache.Len()))

	logf.Log.WithName("pipeline").V(1).Info("Evicted watch scope from pipeline cache",
		"sink", name, "group", scope.Group, "kind", scope.Kind, "namespace", scope.Namespace,
		"entries_removed", removed)
}

func (r *sinkStateRegistry) markScopeWarm(name string, scope ScopeKey) {
	st := r.get(name)
	st.mu.Lock()
	st.warm[scope] = struct{}{}
	st.mu.Unlock()
	r.metrics.safeMode.WithLabelValues(name, scope.Group, scope.Kind, scope.Namespace).Set(0)
}

// recordScopeUnwarmed publishes safe_mode=1 for a (sink, scope) pair the pipeline
// has just observed to be un-warmed. It is called from the Snapshot-tagging
// branch rather than when a scope is first watched, because that branch is the
// only place the condition is actually established — and it runs on cache misses
// only (once per object), never on the steady-state hot path. Without it the
// gauge could only ever be observed at 0, which would make "is this scope still
// warming?" unanswerable from metrics alone.
func (p *Pipeline) recordScopeUnwarmed(key Key) {
	scope := key.Scope()
	p.metrics.safeMode.WithLabelValues(key.Sink, scope.Group, scope.Kind, scope.Namespace).Set(1)
}

// recordCacheEntries publishes a sink's current hashCache size to the
// hashcache_entries gauge. Len() takes and releases the cache's own lock, and
// the gauge Set happens here, strictly outside it — no metric call ever runs
// while a hashCache lock is held (a Task 0.1 acceptance criterion).
func (p *Pipeline) recordCacheEntries(sinkName string, st *sinkState) {
	p.metrics.hashcacheEntries.WithLabelValues(sinkName).Set(float64(st.cache.Len()))
}

// logThrottle allows one event per interval per key, so a condition affecting
// every queued item at once produces a readable log instead of a storm. It is
// deliberately a "first one wins, rest are silent" filter rather than a counter:
// the point is to prove the condition is happening, not to tally it (the
// metrics do that).
type logThrottle struct {
	interval time.Duration
	mu       sync.Mutex
	last     map[string]time.Time
}

// allow reports whether an event for key should be logged now.
func (t *logThrottle) allow(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if last, seen := t.last[key]; seen && now.Sub(last) < t.interval {
		return false
	}
	if t.last == nil {
		t.last = make(map[string]time.Time)
	}
	t.last[key] = now
	return true
}

// compile-time proof that a Pipeline is usable as a manager.Runnable without
// importing controller-runtime's manager package here.
var _ interface {
	Start(ctx context.Context) error
} = (*Pipeline)(nil)
