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

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// Pacing of the warm/GC coordinator. Every value is overridable through
// WarmOptions so tests never wait on production pacing.
const (
	// defaultWarmRetryMaxInterval caps the backoff between warm attempts. The
	// retry itself is unbounded in time (only ctx cancellation gives up), because
	// the alternative to retrying is leaving a scope permanently in Snapshot mode
	// — correct, but blind to genuine "Added" transitions for as long as the
	// sink stays unreachable.
	defaultWarmRetryMaxInterval = 30 * time.Second

	// defaultSyncPollInterval is how often a warm re-checks whether the informer
	// serving its scope has finished its initial List. It is a poll rather than a
	// notification because client-go exposes HasSynced as a predicate, and a
	// hundred-millisecond granularity is invisible next to the List itself.
	defaultSyncPollInterval = 100 * time.Millisecond

	// closeOutEvidenceTimeout bounds the wait for the successor's own first row to
	// reach the sink, when the GC pass has already proved an incarnation dead but
	// history has not caught up yet (see recoverRefusedReincarnations). It is
	// generous — the evidence is one batch flush away — but finite, because the
	// alternative is a warm goroutine that never retires when the successor's
	// write is permanently failing.
	closeOutEvidenceTimeout = 2 * time.Minute

	// defaultBootInterval is how often the coordinator looks for a sink that
	// still needs its boot reconciliation pass. Sinks appear at runtime (a
	// ClickHouseSink CR created hours after boot), and a newly-live sink may
	// carry scopes some earlier process left open, so the pass is level-triggered
	// per sink rather than run once for the process.
	defaultBootInterval = 30 * time.Second
)

// errNoStateReader marks a sink that is live but cannot read its own history
// back. Warm-up, zombie GC and scope-epoch reconciliation are all disabled for
// it (see sink.StateReader's optionality note); its scopes stay in Snapshot mode
// permanently, which is the safe direction.
var errNoStateReader = errors.New("sink has no StateReader; warm-up and zombie GC are disabled for it")

// errSinkNotLive is the retryable condition behind a warm for a sink that has no
// live instance yet — the rule naming it was applied before its ClickHouseSink CR
// became ready, or the sink is mid-recycle after a credential change.
var errSinkNotLive = errors.New("sink has no live instance yet")

// errScopeStopped aborts a warm whose scope was deregistered while it ran. It is
// not a failure: the scope's cache is being evicted and its Stopped row written,
// so there is nothing left to warm or garbage-collect.
var errScopeStopped = errors.New("watch scope stopped while warming")

// errCloseOutEvidencePending is the retryable condition behind a reincarnation the
// GC pass proved dead but history cannot yet confirm: the successor's own first
// row has not reached the sink, so the identity still reads as a single
// incarnation. It is a wait, not a fault — see recoverRefusedReincarnations.
var errCloseOutEvidencePending = errors.New("the successor's first row has not reached the sink yet")

// WarmTarget is one per-scope warm/GC request: seed this (sink, scope) pair's
// dedup baselines from the sink's own history, then reconcile that history
// against live reality.
//
// EpochStart is what makes the request answerable rather than ambiguous, so it is
// part of the target rather than something the coordinator stamps for itself —
// see the field's doc comment.
type WarmTarget struct {
	// Sink is the typed identity of the sink whose history seeds this scope. The
	// kind travels with it because warm state, like dedup state, is per backend:
	// seeding a ClickHouseSink's scope from an S3Sink's history (or marking one
	// warm on the other's behalf) is exactly the confusion typed identity exists
	// to make unrepresentable.
	Sink sink.ID

	// Scope is the (group, kind, namespace) triple to warm, version-agnostic like
	// every other in-process scope key.
	Scope ScopeKey

	// EpochStart is the instant this scope became watched — the same instant the
	// scope's Started row carries.
	//
	// It is the cutoff for the epoch check (see sink.StateReader.ScopeWasActive):
	// "was this scope watched *before now*" can only be answered against a fixed
	// instant, because the current epoch's own Started row is written
	// asynchronously and may land in the middle of the warm. Taking the instant
	// from the caller (the scope recorder, which stamps it once for both the row
	// and this target) is what keeps the two from disagreeing.
	EpochStart time.Time
}

// scopeRef identifies one (sink, scope) pair, the granularity at which warm-up,
// GC and Snapshot-tagging readiness are all tracked.
type scopeRef struct {
	sink  sink.ID
	scope ScopeKey
}

// logValues returns this ref's fields as logr key/value pairs so every warm log
// line carries the same scope context (Invariant 4).
func (r scopeRef) logValues() []any {
	return []any{"sink", r.sink.String(), "group", r.scope.Group,
		"kind", r.scope.Kind, "namespace", r.scope.Namespace}
}

// key builds the pipeline work key for one object in this scope. The namespace
// comes from the object, not from the scope: an all-namespaces scope has an empty
// Namespace while every object under it carries a concrete one.
func (r scopeRef) key(namespace, name string) Key {
	return Key{
		Sink:      r.sink,
		Group:     r.scope.Group,
		Kind:      r.scope.Kind,
		Namespace: namespace,
		Name:      name,
	}
}

// filter renders this ref as the sink-side scope filter for clusterID.
func (r scopeRef) filter(clusterID string) sink.ScopeFilter {
	return sink.ScopeFilter{
		ClusterID: clusterID,
		APIGroup:  r.scope.Group,
		Kind:      r.scope.Kind,
		Namespace: r.scope.Namespace,
	}
}

// StateReaderRouter resolves a sink identity to the StateReader currently serving
// it, and enumerates the sinks that are live.
//
// Task 1.8's SinkManager is the production implementation; resolution happens per
// use (rather than being captured at wiring time) so a sink recycled after a
// credential rotation is picked up without holding a stale reader. The
// enumeration exists for boot reconciliation, which must consider a sink's whole
// scope history — including scopes nothing in the desired state mentions any
// more — and therefore cannot be driven by the scopes this process happens to
// warm.
type StateReaderRouter interface {
	// StateReaderFor returns the StateReader for id. ok=false means either that
	// no live instance exists (transient) or that this sink cannot read its own
	// history back at all (permanent); the caller distinguishes the two by asking
	// SinkRouter whether the sink is live.
	StateReaderFor(id sink.ID) (sink.StateReader, bool)

	// SinkIDs returns the identities of the sinks currently live, in any order.
	SinkIDs() []sink.ID
}

// ScopeEventRouter resolves a sink identity to the live ScopeEventWriter for it,
// so the coordinator can close a scope epoch some earlier process left open.
//
// It is separate from the recorder that writes ordinary transition rows
// (internal/watch): those follow live rule edges, this one follows history, and
// the two must not depend on each other — the recorder drives the coordinator, so
// a dependency back would be a cycle.
type ScopeEventRouter interface {
	// ScopeEventWriterFor returns the scope-log writer for id, or ok=false when
	// no live instance exists (deleted CR, mid-recycle, or a sink that does not
	// record scope epochs).
	ScopeEventWriterFor(id sink.ID) (sink.ScopeEventWriter, bool)
}

// ScopeStates is what the watch layer tells the warm/GC coordinator about scopes
// rather than about objects: whether a scope is wanted, whether its informers have
// caught up, and when the desired state as a whole can be trusted. Task 1.4's
// WatchManager is the production implementation — it is the only component that
// knows both what the rules want and what the informers are actually doing.
//
// It is deliberately separate from ListerRegistry, which is the *object* lookup
// the pipeline's hot path uses: these two are consulted once per warm, off the hot
// path entirely, and keeping them apart means a fake for one is not forced to
// implement the other.
type ScopeStates interface {
	// ScopeSynced reports whether every informer serving (id, scope) has
	// completed its initial List. It gates the GC pass: before the sync, the watch
	// cache legitimately holds nothing, so every seeded object would look like a
	// zombie and the pass would emit a Deleted row for the entire scope.
	ScopeSynced(id sink.ID, scope ScopeKey) bool

	// ScopeDesired reports whether any rule currently wants (id, scope).
	// Boot reconciliation uses it as the authority on "nothing in this process
	// wants this scope any more", which is what turns a scope left open in the
	// sink's history into an orphan to close.
	ScopeDesired(id sink.ID, scope ScopeKey) bool

	// Settled is closed once the desired state this process starts from is in
	// place. It gates the first boot-reconciliation pass, which must not run
	// before then: a rule that simply had not been reconciled yet would look like
	// a deleted one, and its still-live scope would be closed and immediately
	// reopened. Task 1.4's WatchManager.Settled is the production signal.
	//
	// A nil channel means no gating is needed and the pass may run immediately —
	// which is what a unit test wants, and why this is read at Start rather than
	// captured at construction: the watch layer that provides it is bound after
	// the coordinator exists (the two halves of the data plane point at each
	// other).
	Settled() <-chan struct{}
}

// WarmOptions configures a WarmCoordinator. Only the first four fields are
// mandatory; the rest have documented defaults.
type WarmOptions struct {
	// Pipeline is the data plane whose dedup caches this coordinator seeds and
	// whose delete path it drives. Required.
	Pipeline *Pipeline

	// Scopes answers the scope-level questions (sync, desire). Required.
	Scopes ScopeStates

	// Readers resolves a sink to its history reader and enumerates live sinks.
	// Required.
	Readers StateReaderRouter

	// ScopeEvents resolves a sink to its scope-log writer, for the Stopped rows
	// boot reconciliation writes. Required.
	ScopeEvents ScopeEventRouter

	// RetryMaxInterval caps the warm retry backoff. Zero or negative means
	// defaultWarmRetryMaxInterval.
	RetryMaxInterval time.Duration

	// SyncPollInterval is how often a warm re-checks its informer's sync state.
	// Zero or negative means defaultSyncPollInterval.
	SyncPollInterval time.Duration

	// BootInterval is how often the coordinator looks for sinks still needing a
	// boot pass. Zero or negative means defaultBootInterval.
	BootInterval time.Duration
}

// warmRun is one in-flight (or completed) warm for a scope: the epoch it was
// started for, and the handle that cancels it.
type warmRun struct {
	epoch  time.Time
	cancel context.CancelFunc
}

// WarmCoordinator owns per-scope cache warm-up, zombie garbage collection, and
// boot reconciliation of scope epochs.
//
// It is the port of the old per-GVK restoreAndWarm, and every correctness
// property is preserved: StoreIfAbsent seeding that never clobbers live state,
// unbounded ctx-cancellable retry while a sink is unreachable, Snapshot-tagging
// until the seed lands, and a UID-gated GC pass that claims through the same
// hashCache primitive as the live delete path. Three things changed, all forced
// by dynamic rules:
//
//   - Warm-up is *per (sink, scope)* and can start at any time, not once per GVK
//     at boot. A rule created hours into the process's life warms only its own
//     scope.
//   - The GC pass compares against the informer's indexer instead of issuing
//     client Gets, so it never touches the API server (Invariant 1), and it waits
//     for that informer to have synced — an unsynced cache would make every
//     seeded object look like a zombie.
//   - A zombie is only a zombie if the sink's own scope log says this scope was
//     watched in a previous epoch. Without that check, a brand-new rule over a
//     kind with older history would emit a Deleted row for every object recorded
//     by whatever scope wrote that history — the audit lie scope epochs exist to
//     prevent, in its most damaging form.
type WarmCoordinator struct {
	p       *Pipeline
	scopes  ScopeStates
	readers StateReaderRouter
	events  ScopeEventRouter

	retryMaxInterval time.Duration
	syncPollInterval time.Duration
	bootInterval     time.Duration

	// mu guards everything below. Warm requests arrive on the WatchManager's
	// reconcile loop, boot passes run on this coordinator's own goroutine, and
	// each warm runs on its own — so the bookkeeping is shared by construction.
	mu sync.Mutex
	// ctx is the coordinator's lifetime, installed by Start. It is nil until
	// then, which is not a wiring error: the WatchManager and this coordinator
	// are both manager.Runnables with no ordering guarantee between them, so a
	// warm request can legitimately arrive first and is held in pending.
	ctx     context.Context
	pending []WarmTarget
	// runs holds one entry per scope that has been warmed or is warming, keyed by
	// scope so a repeated request for the same epoch is a no-op. Entries are
	// removed by StopScope, so the map tracks live rules rather than growing with
	// every rule the process has ever seen.
	runs map[scopeRef]*warmRun
	// bootDone records the sinks whose boot pass has completed successfully, so a
	// pass runs once per sink rather than once per tick. A failed pass is left
	// unmarked and retried.
	bootDone map[sink.ID]struct{}
	// writerOnly records the sinks whose missing read half has already been
	// announced, so the announcement is one line per sink rather than one per
	// scope. See announceWriterOnly.
	writerOnly map[sink.ID]struct{}
	stopped    bool
	// wg tracks the warm goroutines, so Start returns only once every one of them
	// has exited (a goleak-verified property).
	wg sync.WaitGroup
}

// NewWarmCoordinator builds a WarmCoordinator. The mandatory dependencies are
// validated eagerly because each of them would otherwise surface as a nil-pointer
// panic inside a warm goroutine — long after the wiring mistake, and on a path
// whose whole job is to be trustworthy about deletions.
func NewWarmCoordinator(opts WarmOptions) (*WarmCoordinator, error) {
	if opts.Pipeline == nil {
		return nil, errors.New("pipeline: WarmOptions.Pipeline is required")
	}
	if opts.Scopes == nil {
		return nil, errors.New("pipeline: WarmOptions.Scopes is required")
	}
	if opts.Readers == nil {
		return nil, errors.New("pipeline: WarmOptions.Readers is required")
	}
	if opts.ScopeEvents == nil {
		return nil, errors.New("pipeline: WarmOptions.ScopeEvents is required")
	}

	retry := opts.RetryMaxInterval
	if retry <= 0 {
		retry = defaultWarmRetryMaxInterval
	}
	poll := opts.SyncPollInterval
	if poll <= 0 {
		poll = defaultSyncPollInterval
	}
	boot := opts.BootInterval
	if boot <= 0 {
		boot = defaultBootInterval
	}

	return &WarmCoordinator{
		p:                opts.Pipeline,
		scopes:           opts.Scopes,
		readers:          opts.Readers,
		events:           opts.ScopeEvents,
		retryMaxInterval: retry,
		syncPollInterval: poll,
		bootInterval:     boot,
		runs:             make(map[scopeRef]*warmRun),
		bootDone:         make(map[sink.ID]struct{}),
		writerOnly:       make(map[sink.ID]struct{}),
	}, nil
}

// Start runs the boot-reconciliation loop until ctx is cancelled, then cancels
// every in-flight warm and waits for it. It satisfies manager.Runnable.
//
// Warms themselves are goroutines rather than work items on this loop: a warm
// spends most of its life waiting (on a sink round-trip, on an informer's initial
// List), and serializing them would let one unreachable sink hold up every other
// scope's warm-up indefinitely.
func (c *WarmCoordinator) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("warm")
	ctx = logf.IntoContext(ctx, log)
	log.Info("Starting warm/GC coordinator")

	c.mu.Lock()
	c.ctx = ctx
	pending := c.pending
	c.pending = nil
	for _, target := range pending {
		c.startLocked(target)
	}
	c.mu.Unlock()

	c.reconcileEpochsUntilDone(ctx, log)

	// Shutdown. Cancelling explicitly (rather than relying on ctx propagation
	// alone) keeps the invariant local: after this block no warm can still be
	// running, whoever cancelled what.
	c.mu.Lock()
	c.stopped = true
	for ref, run := range c.runs {
		run.cancel()
		delete(c.runs, ref)
	}
	c.mu.Unlock()
	c.wg.Wait()

	log.Info("Warm/GC coordinator stopped")
	return nil
}

// NeedLeaderElection makes the coordinator a manager.LeaderElectionRunnable that
// runs only on the elected leader, for the same reason the WatchManager does: two
// pods warming and garbage-collecting the same scopes would each emit their own
// Deleted rows for the same disappearance, since the claim that makes a deletion
// exactly-once is per-process (Invariant 6).
func (c *WarmCoordinator) NeedLeaderElection() bool { return true }

// WarmScope requests a warm for target. It never blocks and never fails: it is
// called from the scope recorder on the WatchManager's reconcile loop, where a
// wait on a sink round-trip would stall every other rule's watch lifecycle
// (Invariant 1).
//
// It is idempotent per (sink, scope, epoch): a repeated request for an epoch
// already warming or warmed is dropped, so the recorder does not have to remember
// which scopes it has already handed over. A request carrying a *newer* epoch
// supersedes an older run — that is a scope that stopped and started again, whose
// previous warm is now answering the wrong question.
func (c *WarmCoordinator) WarmScope(target WarmTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	if c.ctx == nil {
		// Not started yet; hold the request rather than dropping it, so a rule
		// that existed before this runnable came up is still warmed.
		c.pending = append(c.pending, target)
		return
	}
	c.startLocked(target)
}

// StopScope abandons any warm for a scope that is no longer watched. Like
// WarmScope it never blocks: it runs between the scope's deregistration and its
// cache eviction.
//
// It emits nothing. A stopped scope's story is told by exactly one Stopped row in
// the scope log, never by Deleted rows for the objects it covered — and a warm
// that is still mid-GC when its scope stops must be cancelled precisely so it
// cannot write those rows.
func (c *WarmCoordinator) StopScope(id sink.ID, scope ScopeKey) {
	ref := scopeRef{sink: id, scope: scope}

	c.mu.Lock()
	defer c.mu.Unlock()
	if run, ok := c.runs[ref]; ok {
		run.cancel()
		delete(c.runs, ref)
	}
	c.pending = slices.DeleteFunc(c.pending, func(t WarmTarget) bool {
		return t.Sink == ref.sink && t.Scope == ref.scope
	})
}

// ForgetSink drops every trace of a sink from the coordinator: its
// boot-reconciliation mark, any warm in flight for it, and any warm still
// pending. It is the coordinator's half of the teardown Pipeline.RemoveSink
// performs on the dedup caches, and Task 1.8's SinkManager calls the two together
// (see sink.WarmHooks).
//
// Clearing the boot mark is the point. Without it a sink deleted and re-created
// under the same name would keep its "already boot-reconciled" flag forever, and
// the boot pass — the thing that writes Stopped rows for scopes an earlier
// process left open — would never run for it again. Scopes orphaned during the
// sink's absence would then stay open in watch_scopes indefinitely: not
// corruption (a spuriously-open epoch only makes the zombie GC more willing to
// collect, which is the safe direction), but a self-heal that had silently
// stopped working.
//
// It is safe to call for a sink the coordinator never saw, which is what lets the
// teardown path call it unconditionally. Matching is on the whole identity, so
// deleting one of two same-named sinks of different kinds leaves the other's warm
// state, boot mark and writer-only announcement exactly as they were.
func (c *WarmCoordinator) ForgetSink(id sink.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.bootDone, id)
	// Paired with bootDone, and for the same reason: a sink re-created under this
	// identity is a new detection, and its capability limit deserves the same one
	// unprompted line the first one got.
	delete(c.writerOnly, id)
	for ref, run := range c.runs {
		if ref.sink == id {
			run.cancel()
			delete(c.runs, ref)
		}
	}
	c.pending = slices.DeleteFunc(c.pending, func(t WarmTarget) bool {
		return t.Sink == id
	})
}

// startLocked launches (or supersedes) the warm for target. The caller holds mu.
func (c *WarmCoordinator) startLocked(target WarmTarget) {
	ref := scopeRef{sink: target.Sink, scope: target.Scope}
	if run, ok := c.runs[ref]; ok {
		if !target.EpochStart.After(run.epoch) {
			return
		}
		run.cancel()
	}

	ctx, cancel := context.WithCancel(c.ctx)
	c.runs[ref] = &warmRun{epoch: target.EpochStart, cancel: cancel}
	c.wg.Go(func() { c.warm(ctx, ref, target.EpochStart) })
}

// warm seeds one scope's dedup baselines from the sink's history and then
// reconciles that history against live reality.
//
// The order is load-bearing:
//
//  1. Seed first, with unbounded ctx-cancellable retry. Until the seed lands the
//     scope is *not* marked warm, so cache misses are tagged Snapshot rather than
//     Added — an unreachable sink degrades to imprecise event types instead of
//     re-emitting every live object in the scope as a duplicate.
//  2. Mark warm, which is also what flips this scope's safe_mode gauge to 0.
//  3. Only then garbage-collect, and only after the informer has synced and the
//     scope log confirms a previous epoch. Both gates exist to stop the pass from
//     manufacturing deletions.
//
//nolint:logcheck // Uses one logger decorated with this scope's identity throughout.
func (c *WarmCoordinator) warm(ctx context.Context, ref scopeRef, epochStart time.Time) {
	log := logf.FromContext(ctx).WithValues(ref.logValues()...)
	log.Info("🔄 Warming scope from sink history")

	seeded, priors, err := c.seedScope(ctx, log, ref)
	if err != nil {
		// Either ctx was cancelled (shutdown, or the scope stopped) or the sink
		// permanently cannot read its own history. Both leave the scope in
		// Snapshot mode, which is the safe direction; both are already logged
		// where they were decided.
		return
	}

	c.p.MarkScopeWarm(ref.sink, ref.scope)
	log.Info("🔓 Scope warm-up complete, leaving Snapshot mode", "objects_loaded", len(seeded))

	if ref.scope.ephemeral() {
		// A Kubernetes Events scope takes steps 1 and 2 above and none of step 3.
		// The seed *runs* — that is the whole point, since a restart must not
		// re-emit every live, unchanged Event as an Added — but nothing is ever
		// reconciled *away* from history.
		//
		// Everything below rests on one assumption: that the sink's history
		// describes objects which ought to still exist, so an object history knows
		// and reality does not is a deletion nobody recorded. For an Event the same
		// observation means the opposite — it expired, which is not a deletion and
		// is never recorded as one (see ephemeralKind). Running the pass anyway
		// would emit a Deleted row for every Event that aged out while this process
		// was down: the largest single source of false deletions the design could
		// have, at the volume of the Event stream itself.
		//
		// Close-out recovery goes with it for the same reason: a close-out is a
		// Deleted row, and an Event whose name was taken over by a newer Event is
		// not an unrecorded death. And with no claims to make, the informer-sync
		// wait and the epoch probe have nothing left to gate, so both are skipped
		// rather than paid for.
		log.V(1).Info("Events scope: seeded for dedup only, skipping zombie GC and close-out recovery",
			"seeded", len(seeded), "unclosed_priors", len(priors))
		return
	}

	if len(seeded) == 0 && len(priors) == 0 {
		// Nothing was seeded and history shows no unclosed incarnation, so there
		// is nothing a zombie could be hiding among and nothing to close out — no
		// reason to pay for the epoch check or the sync wait.
		return
	}

	// The GC pass must not run against a cache that has not finished its initial
	// List: every seeded object would be absent from it. Close-outs need no such
	// wait — their evidence is entirely historical, with no live lookup at all —
	// so a scope that has only priors to recover is not held behind the informer.
	if len(seeded) > 0 && !c.awaitScopeSync(ctx, log, ref) {
		return
	}

	wasActive, err := c.scopeWasActive(ctx, log, ref, epochStart)
	if err != nil {
		return
	}
	if !wasActive {
		// The seeded rows predate this scope's first epoch: they were written by
		// some other scope's watch, or by an epoch that was properly closed with a
		// Stopped row. Either way this process never observed those objects
		// disappear, so claiming they were deleted — and dating the deletion to
		// now — would be a fabrication. The baselines stay seeded, so a genuine
		// later change is still recorded as a Modified.
		//
		// The gate covers close-outs for exactly the same reason, and covering
		// them is not optional: a brand-new rule over a kind that carries old
		// history from some other scope would otherwise emit a Deleted row for
		// every unclosed incarnation in that pre-history.
		log.Info("🕰️ Scope has no previous open epoch, skipping zombie GC and close-out recovery (seeded history is pre-history)",
			"seeded", len(seeded), "unclosed_priors", len(priors))
		return
	}

	recovered := c.emitCloseOuts(ctx, log, ref, priors)

	var zombies int
	if len(seeded) > 0 {
		pass := c.collectZombies(ctx, log, ref, seeded)
		zombies = pass.zombies
		// The other way an unrecorded death surfaces. History held only the old
		// incarnation when the seed read ran — the successor's own first row had
		// not landed yet — so the old UID became an ordinary GC target and its
		// claim was refused by the live successor. That refusal is the proof;
		// history just has to catch up before the close-out can be dated from it.
		recovered += c.recoverRefusedReincarnations(ctx, log, ref, pass.reincarnated)
	}

	if zombies > 0 || recovered > 0 {
		log.Info("🧹 Scope history reconciled",
			"zombies_cleared", zombies, "close_outs_recovered", recovered, "checked", len(seeded))
	}
}

// gcTarget is one object the warm seeded, as the GC pass believes it to be: the
// identity plus the UID that belief is pinned to.
//
// The UID is what makes the belief falsifiable. It comes from a point-in-time
// read of the sink's history, so by the time the pass acts on it the object may
// have been recreated; carrying the UID lets the claim be refused instead of
// deleting a live object by name alone.
type gcTarget struct {
	namespace string
	name      string
	uid       string
}

// reincarnation is one prior incarnation of an identity whose death was never
// recorded: history holds it under an older UID, with no Deleted row of its own,
// while a newer incarnation of the same name has since been recorded.
//
// It exists because the pipeline's live reincarnation branch cannot see this
// case. That branch fires only when the dedup cache already holds the old UID,
// which after a restart it never does — warm has to dial the sink while the
// informer only has to reach the API server, so the successor is observed (and
// Reserved under its new UID) first. Both halves that then decline to act are
// individually correct: StoreIfAbsent must not clobber the live entry, and
// gcPass's UID-gated ReserveDelete must refuse a claim for a UID the key no
// longer holds — that refusal is what stops a live object being deleted by name
// alone. So the evidence is taken from where it actually survives, the sink's own
// history, which needs no cache claim and changes neither half.
type reincarnation struct {
	namespace  string
	name       string
	uid        string
	apiVersion string
	ts         time.Time
}

// closeOutRecord renders this prior incarnation as the Deleted row that closes
// its history. Every field comes from history; nothing is sampled from the
// current process.
//
// Timestamp is the incarnation's own last recorded ts, never time.Now(), and both
// reasons are load-bearing:
//
//   - Ordering. A now-stamp would sort *after* the successor's first row, so
//     argMax(event_type, ts) for the identity would return Deleted and the live
//     successor would be excluded from every future LastKnownStates — a later
//     warm would stop seeding an object that exists. Dating from history places
//     the close-out before the successor's first row, so a reconstruction reads
//     Added → Modified → Deleted (old UID) → Snapshot (new UID), in the order the
//     events actually happened.
//   - Idempotency. Because every field is derived from history, a re-emitted
//     close-out (the write failed, the process died mid-retry, the scope was
//     re-warmed before the row landed) is byte-identical to the first attempt and
//     is collapsed on merge by resource_states' ReplacingMergeTree (Task 0.9).
//     Anyone tempted to "fix" this to time.Now() would silently reintroduce
//     duplicate Deleted rows.
//
// Data, Diff and SHA256 are empty: in schema v1 event_type alone marks a deletion
// (see docs/SCHEMA.md), exactly as for the live delete path.
func (r reincarnation) closeOutRecord(clusterID string, key Key) sink.Record {
	return sink.Record{
		Timestamp:  r.ts,
		ClusterID:  clusterID,
		EventType:  "Deleted",
		APIGroup:   key.Group,
		APIVersion: r.apiVersion,
		Kind:       key.Kind,
		Namespace:  key.Namespace,
		Name:       key.Name,
		UID:        r.uid,
	}
}

// identity is the (namespace, name) an incarnation belongs to — the grouping key
// seedScope classifies history under. It is not the cache key: that one carries
// the group and kind too, which are constant within a scope.
type identity struct {
	namespace string
	name      string
}

// seedScope loads the scope's last-known states, stores the current incarnation
// of each identity as a dedup baseline, and collects the priors whose death
// history never recorded. It retries until it succeeds or the attempt becomes
// pointless.
//
// The retry is unbounded in time (only ctx cancellation or a permanently
// reader-less sink stops it) and each attempt starts from scratch — both returned
// slices are rebuilt from nil on every attempt: a partial read is reported as an
// error by StateReader precisely so the whole scan is retried rather than the
// scope being marked warm from an under-restored cache.
//
// LastKnownStates now answers per incarnation (see sink.KnownState), so one
// identity may come back as several rows. The one with the greatest ts is the
// current incarnation and is treated exactly as before; every other is a prior
// that is deliberately *not* seeded (its key belongs to the current incarnation)
// and deliberately *not* a GC target (the pass would only have its UID-gated
// claim refused, wasting an attempt and logging a zombie that is not one).
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) seedScope(ctx context.Context, log logr.Logger,
	ref scopeRef) ([]gcTarget, []reincarnation, error) {
	var seeded []gcTarget
	var priors []reincarnation

	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = c.retryMaxInterval
	eb.MaxElapsedTime = 0 // retry forever — only ctx cancellation gives up

	err := backoff.Retry(func() error {
		seeded = nil
		priors = nil

		reader, err := c.readerFor(ref.sink)
		if err != nil {
			if errors.Is(err, errNoStateReader) {
				c.announceWriterOnly(log, ref.sink)
				return backoff.Permanent(err)
			}
			log.V(1).Info("Sink is not live yet, retrying warm-up", "reason", err.Error())
			return err
		}

		states, err := reader.LastKnownStates(ctx, ref.filter(c.p.clusterID))
		if err != nil {
			log.Error(err, "⚠️ Failed to read scope history from the sink, staying in Snapshot mode and retrying")
			return err
		}

		st := c.p.sinks.get(ref.sink)
		for _, group := range groupByIdentity(states) {
			current, unclosed := splitIncarnations(group)

			key := ref.key(current.Namespace, current.Name)
			// StoreIfAbsent, not Store: a work item for this key may already have
			// reserved a newer entry while the read was in flight (tagged
			// Snapshot, since the scope was not warm yet). That live state is
			// authoritative and must not be clobbered by a historical baseline.
			st.cache.StoreIfAbsent(key.cacheKey(), CacheEntry{
				Hash: current.SHA256,
				// No JSON baseline: history carries the hash, not the object, so
				// the first genuine change diffs as a full state (Invariant 5).
				JSON: nil,
				UID:  current.UID,
			})
			seeded = append(seeded, gcTarget{namespace: current.Namespace, name: current.Name, uid: current.UID})

			for _, prior := range unclosed {
				log.Info("🧟 History holds an incarnation whose death was never recorded, queueing its close-out",
					"namespace", prior.Namespace, "name", prior.Name,
					"old_uid", prior.UID, "current_uid", current.UID)
				priors = append(priors, reincarnation{
					namespace:  prior.Namespace,
					name:       prior.Name,
					uid:        prior.UID,
					apiVersion: prior.APIVersion,
					ts:         prior.TS,
				})
			}
		}
		// Seeding may have added keys; refresh the size gauge outside any cache
		// lock (recordCacheEntries takes and releases it internally).
		c.p.recordCacheEntries(ref.sink, st)
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		return nil, nil, err
	}
	return seeded, priors, nil
}

// groupByIdentity buckets per-incarnation history rows by (namespace, name),
// preserving the order identities were first seen in.
//
// The order matters only for reproducibility — of the seeding sequence, of the GC
// pass, and of test expectations — but map iteration order would surrender it for
// nothing, so it is kept.
func groupByIdentity(states []sink.KnownState) [][]sink.KnownState {
	groups := make(map[identity]int, len(states))
	var out [][]sink.KnownState
	for _, state := range states {
		id := identity{namespace: state.Namespace, name: state.Name}
		if idx, seen := groups[id]; seen {
			out[idx] = append(out[idx], state)
			continue
		}
		groups[id] = len(out)
		out = append(out, []sink.KnownState{state})
	}
	return out
}

// splitIncarnations separates one identity's history rows into the current
// incarnation (the greatest ts) and the unclosed priors.
//
// Equal timestamps are broken by UID, lexicographically, so the classification is
// a pure function of the history rather than of the order ClickHouse happened to
// return them in. A tie is not expected — two incarnations of one name cannot
// have their last event at the same nanosecond in practice — but an arbitrary
// winner would make the emitted close-out non-deterministic, and determinism is
// exactly what lets a re-emitted close-out collapse on merge.
//
// group is never empty: groupByIdentity only creates a bucket when it has a row
// to put in it.
func splitIncarnations(group []sink.KnownState) (current sink.KnownState, unclosed []sink.KnownState) {
	current = group[0]
	for _, state := range group[1:] {
		if state.TS.After(current.TS) || (state.TS.Equal(current.TS) && state.UID > current.UID) {
			current = state
		}
	}
	for _, state := range group {
		if state.UID != current.UID {
			unclosed = append(unclosed, state)
		}
	}
	return current, unclosed
}

// readerFor resolves a sink's StateReader, distinguishing the two reasons it may
// be absent: the sink is not live yet (retryable) versus the sink is live but
// cannot read its history back at all (permanent).
//
// Conflating them would either spin a goroutine forever on a Writer-only sink or
// permanently disable warm-up for a sink that was merely a second late to become
// ready.
// announceWriterOnly states a sink's missing read half exactly once, however many
// scopes go on to discover the same thing.
//
// The volume is the whole reason it exists. A Writer-only sink is asked for its
// history once per scope, and a busy cluster has hundreds of scopes per sink — so
// the honest per-scope log is a storm, and a storm about an expected condition is
// how the *unexpected* ones stop being noticed. The condition is also not an
// anomaly, so it is never logged at Error: nothing has gone wrong, the backend is
// doing what it was built to do (D12).
//
// One line, at Info, naming all three behaviours it switches off, and then silence
// at V(1). The durable statements live elsewhere and are the ones an operator is
// meant to end up at: HistoryUnavailable=True on the sink and on every rule bound
// to it, and kuberecord_safe_mode pinned at 1 for each of the sink's scopes.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) announceWriterOnly(log logr.Logger, id sink.ID) {
	c.mu.Lock()
	_, announced := c.writerOnly[id]
	if !announced {
		c.writerOnly[id] = struct{}{}
	}
	c.mu.Unlock()

	if announced {
		log.V(1).Info("Sink cannot read its own history; nothing to warm, collect or reconcile",
			"sink", id.String())
		return
	}
	log.Info("Sink cannot read its own history: cache warm-up, zombie garbage collection and boot "+
		"reconciliation of scope epochs are disabled for every scope on it, and every record it receives "+
		"is a permanent Snapshot. Reported as HistoryUnavailable=True on the sink and on the rules bound "+
		"to it; observable as kuberecord_safe_mode staying at 1 for its scopes",
		"sink", id.String())
}

func (c *WarmCoordinator) readerFor(id sink.ID) (sink.StateReader, error) {
	if reader, ok := c.readers.StateReaderFor(id); ok {
		return reader, nil
	}
	if _, live := c.p.router.WriterFor(id); live {
		return nil, errNoStateReader
	}
	return nil, errSinkNotLive
}

// awaitScopeSync blocks until the informers serving this scope report synced,
// returning false if ctx ended first.
//
// This is the gate the GC pass may never skip. An informer that has not completed
// its initial List holds an empty (or partial) indexer, so every seeded object
// would read as absent and the pass would emit a Deleted row for the entire
// scope — the single most destructive way this component could fail.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) awaitScopeSync(ctx context.Context, log logr.Logger, ref scopeRef) bool {
	ticker := time.NewTicker(c.syncPollInterval)
	defer ticker.Stop()
	for {
		if c.scopes.ScopeSynced(ref.sink, ref.scope) {
			return true
		}
		if !c.scopes.ScopeDesired(ref.sink, ref.scope) {
			// The rule went away while we waited. StopScope normally cancels this
			// warm, but the desire check makes the abort independent of that
			// call's timing.
			log.V(1).Info("Scope is no longer desired, abandoning its zombie GC pass")
			return false
		}
		select {
		case <-ctx.Done():
			log.V(1).Info("Cancelled while waiting for the scope's informer to sync, skipping zombie GC")
			return false
		case <-ticker.C:
		}
	}
}

// scopeWasActive asks the sink's scope log whether this scope was already being
// watched before the current epoch began, retrying a transient failure.
//
// The answer decides whether the GC pass may run at all, so a read failure must
// never be resolved optimistically: it is retried, and if the retry is cancelled
// the pass is skipped. Skipping leaves a genuinely dead object recorded as alive
// until the scope is warmed again — recoverable. Guessing "yes" would fabricate a
// Deleted row — not.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) scopeWasActive(ctx context.Context, log logr.Logger,
	ref scopeRef, epochStart time.Time) (bool, error) {
	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = c.retryMaxInterval
	eb.MaxElapsedTime = 0

	var active bool
	err := backoff.Retry(func() error {
		reader, err := c.readerFor(ref.sink)
		if err != nil {
			if errors.Is(err, errNoStateReader) {
				return backoff.Permanent(err)
			}
			return err
		}
		active, err = reader.ScopeWasActive(ctx, ref.filter(c.p.clusterID), epochStart)
		if err != nil {
			log.Error(err, "⚠️ Failed to read the scope's epoch history, retrying before deciding on zombie GC")
			return err
		}
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		return false, err
	}
	return active, nil
}

// emitCloseOuts emits one Deleted row per prior incarnation history shows was
// never closed out, and returns how many it handed over.
//
// It is called only after the scopeWasActive gate — the same gate the zombie GC
// pass sits behind, and mandatory here for the same reason: without it a
// brand-new rule over a kind carrying older history from some other scope would
// fabricate deletions for pre-history.
//
// Unlike the GC pass this claims nothing in hashCache, and must not: the key
// belongs to the current incarnation, whose entry a claim would either be refused
// against (correctly) or corrupt. That is precisely why the retry for these
// writes lives in closeOutRetryQueue, outside hashCache — enqueueCloseOut records
// a failure there and re-queues the key, so the next work item for that name
// tries again (Invariant 3 is untouched: no version-gated commit is involved).
//
// Only resolving the writer can fail, so that is what the retry wraps. The pass
// is self-limiting: once a close-out lands, that UID's own latest event is
// Deleted and LastKnownStates' HAVING clause excludes it forever after, so no
// separate bookkeeping is needed to stop it being re-emitted. The one case that
// does not settle on the first read is a close-out sharing its ts with the event
// it closes (it is dated *from* that event), which leaves argMax(event_type, ts)
// free to answer with either — so a later warm may emit the row again. That is
// harmless by construction rather than by luck: the re-emission is byte-identical
// and resource_states collapses it, which is the whole reason the record is built
// from history.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) emitCloseOuts(ctx context.Context, log logr.Logger,
	ref scopeRef, priors []reincarnation) int {
	if len(priors) == 0 {
		return 0
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = c.retryMaxInterval
	eb.MaxElapsedTime = 0

	var recovered int
	err := backoff.Retry(func() error {
		recovered = 0

		writer, ok := c.p.router.WriterFor(ref.sink)
		if !ok {
			// Nothing to write through yet. Retried as a whole rather than per
			// record, exactly like the GC pass, so a sink that comes back
			// mid-recovery does not leave half the priors closed.
			return errSinkUnavailable
		}
		st := c.p.sinks.get(ref.sink)

		for _, prior := range priors {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			key := ref.key(prior.namespace, prior.name)
			c.p.enqueueCloseOut(ctx, log, key, st, writer, key.cacheKey(),
				prior.closeOutRecord(c.p.clusterID, key))
			recovered++
		}
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		log.V(1).Info("Close-out recovery did not complete", "reason", err.Error())
		return 0
	}
	return recovered
}

// recoverRefusedReincarnations closes out the incarnations the GC pass proved
// dead but could not claim, returning how many it handed over.
//
// It is the second face of the same bug emitCloseOuts covers, and which of the
// two fires is decided by a race the operator does not control: whether the
// successor's own first row reached the sink before the warm read history.
//
//   - It did. History returns two incarnations, seedScope classifies the older as
//     an unclosed prior, and it never becomes a GC target at all (emitCloseOuts).
//   - It did not. History returns only the old incarnation, so it is seeded and
//     swept like any other object — and the sweep finds a *different* UID live
//     under that name. Its UID-gated claim is then refused, correctly: the cache
//     entry belongs to the successor and deleting it would remove a live object
//     by name alone. Nobody is left to record that the old incarnation died.
//
// The refusal is what this pass acts on, and it is stronger evidence than the
// history-only inference: two independent sources — the sink's history and the
// live indexer — agree the seeded UID is gone and a successor holds its name. The
// close-out is still built from history rather than from that observation, for
// the same two reasons emitCloseOuts is (ordering and idempotency), which is why
// this waits for the successor's row to land before emitting. Until it does, the
// identity still reads as a single incarnation and there is no prior row to date
// a close-out from.
//
// The wait is bounded (closeOutEvidenceTimeout) and re-reads history rather than
// trusting a snapshot. Duplicate safety rests on two complementary guards, and
// neither covers the other's case:
//
//   - The refusal reason, checked at the source (gcPass). A target only reaches
//     this pass when the cache refused its claim with deleteClaimUIDMismatch, so a
//     close-out whose Deleted row is merely *in flight* — deleteClaimInFlight, i.e.
//     another caller already owns that exact deletion — is never handed over. That
//     decision is made inside hashCache's own lock, atomically with the refusal.
//   - The stillOpen re-read, below. If a close-out for that UID has already
//     *landed*, the UID's latest event is Deleted, the warm-up query excludes it,
//     and this pass finds nothing to do.
//
// The re-read cannot substitute for the first guard: an unwritten row is invisible
// to history, so an in-flight close-out still reads as an open incarnation and
// would be emitted a second time — with a different timestamp (one time.Now(), one
// historical), which is precisely the shape ReplacingMergeTree cannot collapse.
// Nor can the first substitute for the second: once a row lands, no cache entry
// remains to refuse anything. Do not remove either.
//
// It changes no GC decision: the refusal stands, the live entry is untouched, and
// no cache claim is taken (Invariant 3).
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) recoverRefusedReincarnations(ctx context.Context, log logr.Logger,
	ref scopeRef, refused []gcTarget) int {
	if len(refused) == 0 {
		return 0
	}

	pending := make(map[gcTarget]struct{}, len(refused))
	for _, target := range refused {
		pending[target] = struct{}{}
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = c.retryMaxInterval
	eb.MaxElapsedTime = closeOutEvidenceTimeout

	var recovered int
	err := backoff.Retry(func() error {
		reader, err := c.readerFor(ref.sink)
		if err != nil {
			if errors.Is(err, errNoStateReader) {
				return backoff.Permanent(err)
			}
			return err
		}
		states, err := reader.LastKnownStates(ctx, ref.filter(c.p.clusterID))
		if err != nil {
			log.Error(err, "⚠️ Failed to re-read scope history while closing out a refused reincarnation, retrying")
			return err
		}

		rows, current := indexIncarnations(states)

		var found []reincarnation
		for target := range pending {
			row, stillOpen := rows[target]
			if !stillOpen {
				// The UID is no longer an open incarnation: somebody — the worker
				// that saw the reincarnation live, or an earlier attempt of this
				// pass — already closed it out.
				delete(pending, target)
				continue
			}
			if current[identity{namespace: target.namespace, name: target.name}] == target.uid {
				// The successor's first row has not reached the sink yet, so this
				// UID still reads as the identity's newest incarnation and there is
				// no prior row to date a close-out from. Wait for it.
				continue
			}
			delete(pending, target)
			found = append(found, reincarnation{
				namespace:  row.Namespace,
				name:       row.Name,
				uid:        row.UID,
				apiVersion: row.APIVersion,
				ts:         row.TS,
			})
		}

		if len(found) > 0 {
			log.Info("🧟 Closing out an incarnation the GC pass proved dead but could not claim",
				"incarnations", len(found))
			recovered += c.emitCloseOuts(ctx, log, ref, found)
		}
		if len(pending) > 0 {
			return errCloseOutEvidencePending
		}
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		log.V(1).Info("Some refused reincarnations were left unclosed", "reason", err.Error(), "pending", len(pending))
	}
	return recovered
}

// indexIncarnations renders history as the two lookups recoverRefusedReincarnations
// needs: every open incarnation by its identity-and-UID, and which UID is the
// current incarnation of each identity.
func indexIncarnations(states []sink.KnownState) (map[gcTarget]sink.KnownState, map[identity]string) {
	rows := make(map[gcTarget]sink.KnownState, len(states))
	current := make(map[identity]string)
	for _, group := range groupByIdentity(states) {
		newest, _ := splitIncarnations(group)
		current[identity{namespace: newest.Namespace, name: newest.Name}] = newest.UID
		for _, state := range group {
			rows[gcTarget{namespace: state.Namespace, name: state.Name, uid: state.UID}] = state
		}
	}
	return rows, current
}

// gcResult is what one zombie sweep observed.
type gcResult struct {
	// zombies is how many deletions this pass claimed and enqueued.
	zombies int

	// reincarnated are the targets this pass proved dead — the live indexer holds
	// a different incarnation under the same name — but whose deletion it could
	// not claim *because the successor already owns the cache entry*
	// (deleteClaimUIDMismatch, and only that). The refusal is correct and stands;
	// these are handed to recoverRefusedReincarnations so the old UID's death still
	// reaches the audit trail. A refusal for any other reason is deliberately
	// absent from this list — see the classification in gcPass.
	reincarnated []gcTarget
}

// collectZombies reconciles the seeded history against the watch cache and emits
// one Deleted row per object that is genuinely gone, returning what it saw.
//
// The whole pass is retried on failure, which is safe precisely because every
// deletion is claimed through hashCache.ReserveDelete: a key whose Deleted row
// already landed no longer has an entry to claim, and a key whose claim is still
// in flight is refused, so a retried pass can never double-emit.
//
// It runs outside the workqueue, which does not violate per-key serialization
// (Invariant 2) — the claim primitive is exactly what makes the GC pass and a
// concurrent worker for the same key safe against each other, and it is the same
// primitive the live delete path uses (see emitDelete).
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) collectZombies(ctx context.Context, log logr.Logger,
	ref scopeRef, seeded []gcTarget) gcResult {
	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = c.retryMaxInterval
	eb.MaxElapsedTime = 0

	var result gcResult
	err := backoff.Retry(func() error {
		var passErr error
		result, passErr = c.gcPass(ctx, log, ref, seeded)
		return passErr
	}, backoff.WithContext(eb, ctx))
	if err != nil && !errors.Is(err, errScopeStopped) {
		log.V(1).Info("Zombie GC pass did not complete", "reason", err.Error())
	}
	return result
}

// gcPass is one attempt at the zombie sweep. It reports what *this* attempt saw:
// a retry re-walks the whole scope, and the keys an earlier attempt already
// claimed are refused this time round, so the count is a lower bound after a
// partial failure rather than a running total.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see warm.
func (c *WarmCoordinator) gcPass(ctx context.Context, log logr.Logger,
	ref scopeRef, seeded []gcTarget) (gcResult, error) {
	var result gcResult

	writer, ok := c.p.router.WriterFor(ref.sink)
	if !ok {
		// Nothing to write through yet; the whole pass is retried rather than
		// each object individually, so a sink that comes back mid-sweep does not
		// leave half the scope reconciled.
		return result, errSinkUnavailable
	}
	st := c.p.sinks.get(ref.sink)

	for _, target := range seeded {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		key := ref.key(target.namespace, target.name)
		obj, found, scopeActive, err := c.p.lister.Get(key)
		if err != nil {
			log.Error(err, "🧟 Failed to check whether an object still exists, retrying the GC pass",
				"namespace", target.namespace, "name", target.name)
			return result, err
		}
		if !scopeActive {
			// The scope stopped mid-sweep. Every remaining object is now
			// unobservable and its story is told by the Stopped row, so the pass
			// stops here instead of recording deletions for the rest.
			log.V(1).Info("Watch scope stopped during the GC pass, abandoning the rest of it")
			return result, backoff.Permanent(errScopeStopped)
		}
		// Still alive under the UID history recorded: not a zombie.
		if found && string(obj.GetUID()) == target.uid {
			continue
		}

		// Either the object is gone, or a different incarnation is live now — the
		// old UID's history has to be closed out either way. The claim is gated on
		// target.uid so a reincarnation that a worker already reserved is refused
		// rather than deleting a currently-existing object by name alone.
		outcome, enqueueErr := c.p.emitDelete(ctx, log, key, st, writer, target.uid)
		if enqueueErr != nil {
			log.Error(enqueueErr, "🧟 Failed to queue a zombie's deletion, retrying the GC pass",
				"namespace", target.namespace, "name", target.name)
			return result, enqueueErr
		}
		if outcome != deleteClaimed {
			// Only a UID mismatch leaves something unrecorded: the key belongs to the
			// successor, so the refusal protects a live object and nobody else will
			// record the old incarnation's death. deleteClaimInFlight is the opposite —
			// another caller already owns this exact deletion and its row is on its way,
			// so recovering it here would write a second, differently-timestamped
			// Deleted row that ReplacingMergeTree cannot collapse. deleteClaimAbsent
			// carries no evidence that a successor owns the key at all.
			//
			// deleteClaimInFlight is reachable from this pass's own retry: attempt 1
			// claims the old UID and enqueues a now-stamped Deleted row, a later target
			// errors the pass, and the retry re-walks this key while that row is still
			// unwritten. That interleaving is covered at the unit level here and by the
			// standing duplicate invariant in test/chaos, and deliberately *not* by a
			// dedicated chaos scenario: reproducing it from outside the process requires
			// erroring the sweep on a later target at a precise moment, which is not
			// externally controllable without a fault-injection hook in the recovery
			// path — test-only machinery in production code, for coverage that already
			// exists. See test/chaos's package comment for the full argument and the
			// condition under which to revisit it.
			//
			// Both conditions are required, and they are independent sources: the
			// outcome is the cache's statement that the key changed hands, decided
			// atomically under its lock, while found/liveUID is the live indexer's
			// statement that a successor exists — the two-source agreement
			// recoverRefusedReincarnations rests its evidence on.
			if outcome == deleteClaimUIDMismatch && found && string(obj.GetUID()) != target.uid {
				result.reincarnated = append(result.reincarnated, target)
			}
			continue
		}
		result.zombies++
	}

	return result, nil
}

// reconcileEpochsUntilDone runs boot reconciliation for every live sink, waiting
// for the settle gate first and then re-checking on a ticker until ctx ends.
//
// It keeps ticking rather than running once because sinks appear at runtime: a
// ClickHouseSink created long after boot may carry scopes an earlier process left
// open, and those are just as much of an audit lie as the ones present at
// startup.
//
//nolint:logcheck // Takes Start's already-named logger; see reconcileScopeEpochs.
func (c *WarmCoordinator) reconcileEpochsUntilDone(ctx context.Context, log logr.Logger) {
	if settled := c.scopes.Settled(); settled != nil {
		log.V(1).Info("Waiting for the desired state to settle before reconciling scope epochs")
		select {
		case <-settled:
		case <-ctx.Done():
			return
		}
	}

	ticker := time.NewTicker(c.bootInterval)
	defer ticker.Stop()
	for {
		c.reconcileScopeEpochs(ctx, log)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// reconcileScopeEpochs runs the boot pass for each live sink that has not had a
// successful one. A failure leaves the sink unmarked so the next tick retries it,
// and never stops the other sinks from being reconciled (Invariant 5).
//
//nolint:logcheck // Takes Start's already-named logger.
func (c *WarmCoordinator) reconcileScopeEpochs(ctx context.Context, log logr.Logger) {
	for _, id := range c.readers.SinkIDs() {
		if c.bootReconciled(id) {
			continue
		}
		if err := c.closeOrphanedScopes(ctx, log, id); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error(err, "Failed to reconcile watch-scope epochs for a sink, will retry",
				"sink", id.String())
			continue
		}
		c.markBootReconciled(id)
	}
}

// closeOrphanedScopes writes one Stopped row for every scope this sink's log
// still shows as open but that nothing in this process wants any more — the rule
// was deleted while the operator was down.
//
// It emits **no** Deleted rows, ever. The objects those scopes covered were not
// deleted; the operator simply stopped watching them, possibly a long time ago,
// and the Stopped row is the whole truth available. This is the audit-integrity
// keystone of Phase 1: get it wrong and a single rule deletion during a restart
// looks like a mass deletion event.
//
//nolint:logcheck // Takes Start's already-named logger.
func (c *WarmCoordinator) closeOrphanedScopes(ctx context.Context, log logr.Logger, id sink.ID) error {
	reader, err := c.readerFor(id)
	if err != nil {
		if errors.Is(err, errNoStateReader) {
			// No history to reconcile against. Marked done by the caller so this is
			// decided once per sink, not once per tick — and announced through the
			// same one-per-sink gate the warm path uses, so a sink whose scopes
			// already said this does not say it twice.
			c.announceWriterOnly(log, id)
			return nil
		}
		return err
	}

	events, ok := c.events.ScopeEventWriterFor(id)
	if !ok {
		return fmt.Errorf("sink %s has no live scope-log writer", id)
	}

	scopes, err := reader.ActiveScopes(ctx, c.p.clusterID)
	if err != nil {
		return err
	}

	closed := 0
	for _, filter := range scopes {
		scope := ScopeKey{Group: filter.APIGroup, Kind: filter.Kind, Namespace: filter.Namespace}
		if c.scopes.ScopeDesired(id, scope) {
			// Still wanted: this is an ordinary restart of a scope that was never
			// stopped, and its epoch legitimately stays open.
			continue
		}

		// RuleRef is empty on purpose: the rule that held this scope is gone, and
		// no rule now alive triggered this transition (see sink.ScopeEvent.RuleRef).
		if err := events.EnqueueScopeEvent(ctx, sink.ScopeEvent{
			Action: sink.ScopeActionStopped,
			Scope:  filter,
			TS:     time.Now().UTC(),
		}); err != nil {
			return err
		}
		closed++
		log.Info("Closed a watch scope left open by a previous process; no Deleted rows were written for it",
			"sink", id.String(), "group", filter.APIGroup, "kind", filter.Kind,
			"namespace", filter.Namespace)
	}

	log.Info("Reconciled watch-scope epochs for a sink",
		"sink", id.String(), "open_scopes", len(scopes), "closed", closed)
	return nil
}

func (c *WarmCoordinator) bootReconciled(id sink.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, done := c.bootDone[id]
	return done
}

func (c *WarmCoordinator) markBootReconciled(id sink.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bootDone[id] = struct{}{}
}

// compile-time proof that a WarmCoordinator is usable as a leader-election-gated
// manager.Runnable without importing controller-runtime's manager package here.
var _ interface {
	Start(ctx context.Context) error
	NeedLeaderElection() bool
} = (*WarmCoordinator)(nil)
