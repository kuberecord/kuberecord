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
	"sync"
	"time"

	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// Pacing and bounds of the recorder's retry queue. All three are overridable
// through ScopeRecorderOptions so tests never wait on production pacing.
const (
	// defaultScopeFlushInterval is how often the recorder retries a scope event it
	// could not hand to a sink. A transition is a rare, small, audit-critical
	// write, so the interval is short enough that an operator applying a rule sees
	// the row appear promptly once the sink is reachable, and long enough that a
	// sink that is down does not spin.
	defaultScopeFlushInterval = 2 * time.Second

	// defaultScopeQueueLimit bounds the pending queue. It is the one
	// unbounded-growth risk here: a sink that stays unreachable while rules churn
	// would otherwise accumulate events until the process died, trading a partial
	// audit gap for a total outage. On overflow the oldest events are dropped and
	// reported at Error level (Invariant 4) — the newest transitions describe each
	// scope's current epoch, which is the more useful half to keep.
	defaultScopeQueueLimit = 4096
)

// errScopeQueueOverflow gives the drop log line a non-nil error value. Losing an
// epoch row is an audit hole, so it is reported even though it is the deliberate
// response to a sink that has been unreachable for a very long time.
var errScopeQueueOverflow = errors.New("scope event queue overflowed, oldest transitions dropped")

// ScopeEventRouter resolves a sink identity to the live scope-log writer serving
// it.
//
// Task 1.8's SinkManager is the production implementation. Resolution happens per
// flush attempt rather than being captured at wiring time, so a sink recycled
// after a credential rotation is picked up without the recorder holding a stale
// instance — and a sink that is not live yet simply leaves its events queued.
type ScopeEventRouter interface {
	// ScopeEventWriterFor returns the scope-log writer for id, or ok=false when
	// no live instance exists (the sink's CR was deleted, it is mid-recycle, or it
	// does not record scope epochs at all).
	ScopeEventWriterFor(id sink.ID) (sink.ScopeEventWriter, bool)
}

// sinkIDFor lifts a bare sink name onto the typed identity the sink runtime
// routes on (Task 4.1).
//
// This package still holds sink *names* — the interest map, and the queued scope
// events derived from it, are keyed by one — so the lift happens at the routing
// boundary. Task 4.2 types those keys and the lift disappears. It is exact rather
// than a guess: ClickHouseSink is the only sink CRD, so every name held here
// belongs to one (see sink.DefaultSinkKind for why the manager still refuses to
// make the same substitution itself).
func sinkIDFor(name string) sink.ID {
	return sink.ID{Kind: sink.DefaultSinkKind, Name: name}
}

// ScopeWarmer is the per-scope warm/GC coordinator as the recorder needs it.
//
// The recorder drives it because warm-up is a per-*scope* concern and this is the
// one component that computes scope-level edges out of the interest-level ones the
// WatchManager reports. Task 1.6's pipeline.WarmCoordinator is the production
// implementation; both methods must be non-blocking, since they are called on the
// reconcile loop (Invariant 1).
type ScopeWarmer interface {
	// WarmScope asks for a scope's dedup cache to be seeded from the sink's
	// history and reconciled against live reality.
	WarmScope(target pipeline.WarmTarget)

	// StopScope abandons any warm for a scope that is no longer watched.
	StopScope(sinkName string, scope pipeline.ScopeKey)
}

// ScopeRecorderOptions configures a ScopeEpochRecorder. Only Events is mandatory.
type ScopeRecorderOptions struct {
	// ClusterID identifies this operator's cluster in every recorded scope event
	// (Invariant 7: cluster_id is explicit in the schema, implicit in-process).
	ClusterID string

	// Events resolves a sink to its scope-log writer. Required.
	Events ScopeEventRouter

	// Warmer receives the scope-level warm and stop edges the recorder derives.
	// Nil means warm-up and zombie GC are not driven from here, which is what a
	// recorder-only unit test wants.
	Warmer ScopeWarmer

	// FlushInterval overrides defaultScopeFlushInterval. Zero or negative means
	// the default.
	FlushInterval time.Duration

	// QueueLimit overrides defaultScopeQueueLimit. Zero or negative means the
	// default.
	QueueLimit int
}

// scopeKey identifies one (sink, scope) pair — the granularity a scope epoch is
// recorded at.
type scopeKey struct {
	sink  string
	scope pipeline.ScopeKey
}

// pendingScopeEvent is one queued event plus the sink it belongs to. The sink name
// travels alongside rather than inside sink.ScopeEvent because it is routing
// information, not part of the recorded row: the scope log lives inside the sink,
// so which sink it is has no column to occupy.
type pendingScopeEvent struct {
	sink  string
	event sink.ScopeEvent
}

// ScopeEpochRecorder narrates watch-scope transitions to the sink's scope log.
//
// It exists because "we stopped watching X" and "X was deleted" are different
// truths that a naive implementation records identically. The scope log is what
// keeps them apart, and this type is the only thing that writes it from live rule
// edges.
//
// Two properties define it:
//
// **Transition semantics.** The WatchManager reports interest-level edges — one
// per (sink, informer target) — but an epoch is a property of the (sink, scope)
// pair. So the recorder refcounts interests per scope and writes exactly one
// Started row when a scope gains its first and exactly one Stopped row when it
// loses its last. Two rules on one scope produce one row, not two; a scope served
// by two versions of a resource produces one row, not two; a rule edit that
// changes only a selector produces none. rule_ref names the rule that triggered
// the edge — multi-rule attribution belongs in the owning CR's status, not in an
// append-only log where it could never be corrected.
//
// **It never blocks the watch lifecycle.** ScopeStarted and ScopeStopped run
// inline on the reconcile loop, between deregistering a scope and evicting its
// cache. A sink round-trip there would stall every other rule's watch behind an
// unreachable database (Invariant 1), so those calls only stamp the event and put
// it on an internal queue; a goroutine owned by Start does the actual hand-off,
// retrying until it lands. The event's timestamp is taken at the transition, so a
// row delayed by an outage still records when the watch really started or stopped.
type ScopeEpochRecorder struct {
	clusterID string
	events    ScopeEventRouter
	warmer    ScopeWarmer

	flushInterval time.Duration
	queueLimit    int

	// mu guards active, pending and stopped. The write side runs on the reconcile
	// loop, the flush side on this recorder's own goroutine.
	mu sync.Mutex

	// active maps each scope to the interest targets currently holding it. A set
	// of targets rather than a counter, because the WatchManager may report a
	// Stopped for an interest whose Started it never reported (an informer that
	// failed to come up), and a counter would then close a scope another interest
	// still holds.
	active map[scopeKey]map[string]struct{}

	// pending is the FIFO of events awaiting hand-off. Order is preserved on flush
	// — events are popped only once accepted — because a scope's Started and
	// Stopped rows are meaningless if they can invert.
	pending []pendingScopeEvent

	// dropped counts events lost to queue overflow since the last flush. It is a
	// counter rather than a log at the drop site because dropping happens on the
	// reconcile loop, which must stay free of incidental work; the flush loop
	// reports it.
	dropped int

	// wake nudges the flush loop the moment an event is queued, so a transition on
	// a healthy sink is not delayed by up to a flush interval. Capacity 1 with a
	// non-blocking send, so it coalesces and can never block the reconcile loop.
	wake chan struct{}
}

// NewScopeEpochRecorder builds a ScopeEpochRecorder. Events is validated eagerly
// because a nil router would surface as a panic on the reconcile loop, taking the
// data plane's level-triggering with it.
func NewScopeEpochRecorder(opts ScopeRecorderOptions) (*ScopeEpochRecorder, error) {
	if opts.Events == nil {
		return nil, errors.New("watch: ScopeRecorderOptions.Events is required")
	}

	flush := opts.FlushInterval
	if flush <= 0 {
		flush = defaultScopeFlushInterval
	}
	limit := opts.QueueLimit
	if limit <= 0 {
		limit = defaultScopeQueueLimit
	}

	return &ScopeEpochRecorder{
		clusterID:     opts.ClusterID,
		events:        opts.Events,
		warmer:        opts.Warmer,
		flushInterval: flush,
		queueLimit:    limit,
		active:        make(map[scopeKey]map[string]struct{}),
		wake:          make(chan struct{}, 1),
	}, nil
}

// ScopeStarted records an interest-level start. It writes a Started row and kicks
// off the scope's warm-up only when this is the scope's *first* interest.
//
// Reporting happens after the informer is actually running (the WatchManager
// guarantees that), so a Started row is never a promise the data plane failed to
// keep — which matters because the row is what later tells a zombie GC pass that
// this scope was genuinely watched.
func (r *ScopeEpochRecorder) ScopeStarted(transition ScopeTransition) {
	key := scopeKey{sink: transition.Sink, scope: transition.Scope}

	r.mu.Lock()
	targets := r.active[key]
	if targets == nil {
		targets = make(map[string]struct{}, 1)
		r.active[key] = targets
	}
	_, held := targets[transition.Target]
	targets[transition.Target] = struct{}{}
	first := !held && len(targets) == 1
	if first {
		r.enqueueLocked(transition.Sink, r.eventFor(transition, sink.ScopeActionStarted))
	}
	r.mu.Unlock()

	if !first {
		return
	}
	// Outside the lock: the warmer must never be able to call back into this
	// recorder while it is held, and neither call blocks.
	r.signal()
	if r.warmer != nil {
		r.warmer.WarmScope(pipeline.WarmTarget{
			Sink:  transition.Sink,
			Scope: transition.Scope,
			// The same instant the Started row carries, so the warm's epoch check
			// cannot see the very row this transition writes (see
			// pipeline.WarmTarget.EpochStart).
			EpochStart: transition.At,
		})
	}
}

// ScopeStopped records an interest-level stop. It writes a Stopped row and
// abandons the scope's warm only when this was the scope's *last* interest.
//
// An interest the recorder never saw start (its informer never came up) leaves no
// trace here: there is no epoch of ours to close, and writing a Stopped row for a
// scope that was never recorded as Started would invent a watch that never
// happened.
func (r *ScopeEpochRecorder) ScopeStopped(transition ScopeTransition) {
	key := scopeKey{sink: transition.Sink, scope: transition.Scope}

	r.mu.Lock()
	targets := r.active[key]
	if _, held := targets[transition.Target]; !held {
		r.mu.Unlock()
		return
	}
	delete(targets, transition.Target)
	last := len(targets) == 0
	if last {
		delete(r.active, key)
		r.enqueueLocked(transition.Sink, r.eventFor(transition, sink.ScopeActionStopped))
	}
	r.mu.Unlock()

	if !last {
		return
	}
	r.signal()
	if r.warmer != nil {
		r.warmer.StopScope(transition.Sink, transition.Scope)
	}
}

// Start drains the pending queue until ctx is cancelled, then makes one final
// attempt at whatever is left. It satisfies manager.Runnable.
//
// The final attempt is the reason this is a runnable at all rather than a
// fire-and-forget goroutine: the manager stops runnables before the sink's writer
// drains, so an epoch queued moments before shutdown still gets handed over in
// time to be flushed. Events that cannot be handed over even then are lost, which
// is reported at Error level — the alternative, blocking shutdown on an
// unreachable database, is worse.
func (r *ScopeEpochRecorder) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("scope-recorder")
	log.Info("Starting watch-scope epoch recorder")

	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			remaining := len(r.pending)
			r.mu.Unlock()
			if remaining > 0 {
				// A short, bounded budget of its own: ctx is already cancelled, so
				// the hand-off would fail instantly if it were reused.
				drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.flushInterval)
				r.flush(drainCtx, log)
				cancel()
			}
			r.reportUnflushed(log)
			log.Info("Watch-scope epoch recorder stopped")
			return nil
		case <-r.wake:
			r.flush(ctx, log)
		case <-ticker.C:
			r.flush(ctx, log)
		}
	}
}

// NeedLeaderElection gates the recorder on leadership, like the WatchManager it
// listens to: only the leader observes scope transitions, and two pods narrating
// the same epochs would double every row in the scope log.
func (r *ScopeEpochRecorder) NeedLeaderElection() bool { return true }

// eventFor renders a transition as the sink-side event. The caller holds mu (or is
// about to enqueue under it); the method touches no recorder state beyond
// clusterID.
func (r *ScopeEpochRecorder) eventFor(transition ScopeTransition, action sink.ScopeAction) sink.ScopeEvent {
	return sink.ScopeEvent{
		Action: action,
		Scope: sink.ScopeFilter{
			ClusterID: r.clusterID,
			APIGroup:  transition.Scope.Group,
			Kind:      transition.Scope.Kind,
			Namespace: transition.Scope.Namespace,
		},
		APIVersion: transition.APIVersion,
		RuleRef:    triggeringRule(transition.RuleKeys),
		TS:         transition.At,
	}
}

// triggeringRule picks the rule a transition is attributed to. The manager
// supplies the contributing rules sorted, so the choice is deterministic across
// passes — an operator diffing two epochs of the same scope should not see the
// attribution flip because a map iterated differently.
func triggeringRule(ruleKeys []string) string {
	if len(ruleKeys) == 0 {
		return ""
	}
	return ruleKeys[0]
}

// enqueueLocked appends an event to the pending queue, dropping the oldest events
// if the queue is over its limit. The caller holds mu.
//
// Dropping is deliberately silent here and reported by the flush loop instead:
// this runs on the reconcile loop, and logging under the recorder's lock from that
// path is exactly the kind of incidental work the loop must stay free of.
func (r *ScopeEpochRecorder) enqueueLocked(sinkName string, event sink.ScopeEvent) {
	r.pending = append(r.pending, pendingScopeEvent{sink: sinkName, event: event})
	if overflow := len(r.pending) - r.queueLimit; overflow > 0 {
		r.pending = r.pending[overflow:]
		r.dropped += overflow
	}
}

// signal nudges the flush loop without ever blocking the caller.
func (r *ScopeEpochRecorder) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// flush hands as many queued events to their sinks as it can, in order.
//
// It stops at the first event that cannot be handed over and leaves it (and
// everything after it) queued. Stopping rather than skipping is what preserves
// per-scope ordering: a Started that overtook its own Stopped would invert the
// epoch, and there is no way to repair an append-only log afterwards.
//
//nolint:logcheck // Takes Start's already-named logger.
func (r *ScopeEpochRecorder) flush(ctx context.Context, log logr.Logger) {
	r.reportDrops(log)

	for {
		r.mu.Lock()
		if len(r.pending) == 0 {
			r.mu.Unlock()
			return
		}
		next := r.pending[0]
		queued := len(r.pending)
		r.mu.Unlock()

		values := []any{
			"sink", next.sink, "action", string(next.event.Action),
			"group", next.event.Scope.APIGroup, "kind", next.event.Scope.Kind,
			"namespace", next.event.Scope.Namespace, "rule_ref", next.event.RuleRef,
		}

		writer, ok := r.events.ScopeEventWriterFor(sinkIDFor(next.sink))
		if !ok {
			// Not an anomaly: the sink may simply not be live yet (a rule applied
			// before its ClickHouseSink became ready) or be mid-recycle. The events
			// stay queued and the next tick tries again.
			log.V(1).Info("Sink has no live scope-log writer, keeping its transitions queued",
				append(values, "queued", queued)...)
			return
		}

		if err := writer.EnqueueScopeEvent(ctx, next.event); err != nil {
			if ctx.Err() != nil {
				// Shutting down; whatever is left is reported once, by
				// reportUnflushed, rather than per event here.
				return
			}
			log.Error(err, "Failed to hand a watch-scope transition to its sink, will retry",
				append(values, "queued", queued)...)
			return
		}

		// Accepted — only now is it safe to pop. Popping before the hand-off would
		// lose the event outright on a failure, and popping out of order would let a
		// later transition for the same scope overtake this one.
		r.mu.Lock()
		if len(r.pending) > 0 && r.pending[0] == next {
			r.pending = r.pending[1:]
		}
		r.mu.Unlock()

		log.V(1).Info("Recorded watch-scope transition", values...)
	}
}

// reportDrops logs any queue overflow observed since the last flush.
//
//nolint:logcheck // Takes Start's already-named logger.
func (r *ScopeEpochRecorder) reportDrops(log logr.Logger) {
	r.mu.Lock()
	dropped := r.dropped
	r.dropped = 0
	r.mu.Unlock()
	if dropped > 0 {
		log.Error(errScopeQueueOverflow, "Dropped the oldest watch-scope transitions",
			"dropped", dropped, "queue_limit", r.queueLimit)
	}
}

// reportUnflushed logs the events that never reached a sink, so a shutdown with a
// dead sink leaves a record of exactly what the scope log is missing.
//
//nolint:logcheck // Takes Start's already-named logger.
func (r *ScopeEpochRecorder) reportUnflushed(log logr.Logger) {
	r.mu.Lock()
	remaining := len(r.pending)
	r.mu.Unlock()
	if remaining > 0 {
		log.Error(errScopeEventsUnflushed, "Watch-scope transitions could not be recorded before shutdown",
			"events", remaining)
	}
}

// errScopeEventsUnflushed gives the shutdown log line a non-nil error value.
var errScopeEventsUnflushed = errors.New("watch-scope transitions were not handed to a sink before shutdown")

// Compile-time proof of the contracts this type exists to satisfy: it is the
// production ScopeRecorder the WatchManager reports edges to, and a
// leader-election-gated manager.Runnable. Asserted here rather than at wiring
// time, where a signature drift would surface in a file that has nothing to do
// with either contract.
var (
	_ ScopeRecorder                  = (*ScopeEpochRecorder)(nil)
	_ manager.Runnable               = (*ScopeEpochRecorder)(nil)
	_ manager.LeaderElectionRunnable = (*ScopeEpochRecorder)(nil)
)
