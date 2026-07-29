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

package sink

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Pacing of the sink runtime. Every value is overridable through ManagerOptions
// so tests never wait on production pacing.
const (
	// defaultProbeInterval is how often a healthy sink is re-probed.
	//
	// The probe is the only thing that can tell an operator "this sink went
	// unreachable" while nothing is being written to it, so it must keep running
	// after the first success rather than latching. A minute is short enough that
	// a broken sink is reported before anybody finishes reading the logs, and long
	// enough that fifty sinks cost nothing.
	defaultProbeInterval = 60 * time.Second

	// defaultProbeMinBackoff / defaultProbeMaxBackoff bound the retry schedule
	// after a failed probe. Starting at a second means a sink that is merely
	// slow to come up is reported ready almost immediately; capping at the steady
	// interval means a permanently unreachable sink settles into the same cadence
	// as a healthy one instead of quietly stopping.
	defaultProbeMinBackoff = 1 * time.Second
	defaultProbeMaxBackoff = 60 * time.Second

	// defaultProbeTimeout bounds a single probe attempt. It exists so a backend
	// that accepts a connection and then never answers is reported as a failure
	// rather than pinning its probe goroutine indefinitely.
	defaultProbeTimeout = 10 * time.Second

	// defaultDrainTimeout bounds how long the manager waits for one instance's
	// Start to return after cancelling it.
	//
	// It is deliberately larger than the ClickHouse writer's own shipped drain
	// budget (clickhouse.DefaultShutdownDrainTimeout, 15s): the instance is the
	// component that knows how to flush its queue, so this is the outer guard
	// against a backend that hangs, not a second, competing deadline. Reaching it
	// is an anomaly and logs at Error.
	defaultDrainTimeout = 30 * time.Second

	// defaultResultBuffer is the capacity of the probe-result channel. It absorbs
	// a burst of results from many sinks while the consuming reconciler is busy;
	// see ProbeResults for what happens when it fills.
	defaultResultBuffer = 64
)

// Probe outcome reasons, carried on ProbeResult.Reason so the consumer can turn
// one channel into distinct CR conditions without re-classifying the error
// itself. They are the strings Task 1.7 uses as condition reasons, which is why
// they are CamelCase rather than snake_case.
const (
	// ProbeReasonUnreachable marks a probe that could not complete: the backend
	// did not answer, refused the connection, or rejected the credentials.
	ProbeReasonUnreachable = "Unreachable"

	// ProbeReasonSchemaInvalid marks a backend that answered but whose schema is
	// not the one the operator writes against. It is a distinct reason because it
	// will not fix itself with time — it needs an operator to migrate the schema —
	// whereas an unreachable backend usually will.
	ProbeReasonSchemaInvalid = "SchemaInvalid"
)

// ErrSchemaInvalid is the classifier a backend wraps around a schema-mismatch
// probe failure, so the manager can label the result SchemaInvalid without
// knowing anything about that backend's schema or error types.
//
// It lives here, next to the Prober contract, rather than in the ClickHouse
// package, because the distinction it draws — "unreachable, keep waiting" versus
// "wrong shape, call a human" — is a property of the sink contract, not of any
// one backend.
var ErrSchemaInvalid = errors.New("sink schema does not match the schema the operator writes")

// errManagerStopped is returned by Ensure once the manager has begun shutting
// down. A reconciler seeing it should do nothing: the process is exiting, and the
// sink it wanted will be rebuilt from the CR on the next process's boot
// (Invariant 6).
var errManagerStopped = errors.New("sink manager is shutting down")

// errDrainTimeout gives the drain-timeout log line a non-nil error value. The
// timeout itself is a bounded degradation, not a crash: the manager stops waiting
// and continues, so one wedged backend cannot hold up every other sink's
// lifecycle (Invariant 5).
var errDrainTimeout = errors.New("sink instance did not finish draining within the drain timeout")

// InstanceConfig is one sink's fully-resolved backend configuration: everything
// needed to build a running instance, with the credential already fetched from
// its Secret.
//
// It is an interface with a single method rather than a concrete struct because
// the manager must not know what a backend needs to connect. D6's future sinks
// (PostgresSink, …) each carry their own settings, and a shared struct would
// either collect every backend's fields or force a lossy translation at the one
// point where losing a field means silently running the wrong configuration.
type InstanceConfig interface {
	// Fingerprint returns an opaque digest that changes if and only if a running
	// instance built from this configuration must be replaced. It is the whole
	// basis of Ensure's diff, so it must cover every field the instance is built
	// from — the credential included, since a rotated password is exactly the case
	// that has to force a recycle.
	//
	// Implementations must return a *digest*, never the settings in clear: the
	// value is logged and compared, so a fingerprint that embedded the password
	// would leak it into the operator's log.
	Fingerprint() string
}

// Factory builds — but does not start — the backend instance for one sink.
//
// The manager owns every lifecycle concern (starting, routing, draining,
// probing) and the factory owns every backend concern (which driver, which
// connection, which schema), which is the seam D6 anticipates: adding a
// PostgresSink means supplying a second factory branch at wiring time, not
// changing anything here.
//
// name is passed so the instance can label its own metrics with the sink it
// serves (see pipeline.PipelineMetrics.ForSink) — the manager itself never
// touches those collectors, because internal/sink cannot import
// internal/pipeline.
//
// The returned Writer is inspected for the optional halves of the sink contract
// (StateReader, ScopeEventWriter, Prober); a backend that implements none of
// them still routes writes correctly and simply has warm-up, scope epochs and
// health probing disabled for it.
type Factory func(name string, cfg InstanceConfig) (Writer, error)

// Prober is the optional health half of a sink: an active check that the backend
// is reachable and shaped the way the operator expects.
//
// It is separate from Writer because it answers a control-plane question with a
// control-plane cost — a round-trip — and Invariant 1 forbids a reconciler from
// paying it. The manager calls this from its own goroutine and reports the answer
// over ProbeResults, so a sink that is down slows nothing down; it just gets
// reported.
type Prober interface {
	// Probe returns nil when the backend is reachable and its schema matches. A
	// schema mismatch must be wrapped so it satisfies errors.Is(err,
	// ErrSchemaInvalid); every other failure is treated as unreachable.
	Probe(ctx context.Context) error
}

// ProbeResult is one probe attempt's outcome, destined for the SinkReconciler's
// event channel (Task 1.7) where it becomes the CredentialsResolved / SchemaValid
// / Ready conditions on the ClickHouseSink CR.
//
// This package deliberately stops at the channel: 1.8 lands before the
// reconciler exists, and a sink runtime that wrote CR status itself would put a
// Kubernetes client on the sink's own goroutines — exactly the coupling the
// two-tier split exists to prevent.
type ProbeResult struct {
	// Sink is the ClickHouseSink name this result describes.
	Sink string

	// At is when the attempt settled.
	At time.Time

	// Err is nil on success, and otherwise the probe's own error, unwrapped, so
	// the consumer can put the backend's own words in the CR's condition message.
	Err error

	// Reason classifies a failure (ProbeReasonUnreachable or
	// ProbeReasonSchemaInvalid). It is empty on success.
	Reason string
}

// Pipeline is the data-plane state a vanished sink's removal has to evict, as the
// sink runtime needs it.
//
// It is an interface owned by this package rather than a *pipeline.Pipeline field
// because internal/pipeline depends on internal/sink (it hands Records to a
// Writer), so the dependency can only run one way. pipeline.Pipeline is the
// production implementation; the assertion lives in this package's external test
// file, which can import both without forming a cycle.
type Pipeline interface {
	// RemoveSink discards every pipeline-side trace of a sink: its hashCache and
	// its warm-scope set. It is called only after the sink's instance has fully
	// drained, so no in-flight commit can still be looking for the state it drops
	// (Invariant 3).
	RemoveSink(name string)
}

// WarmHooks is the warm/GC coordinator's half of a vanished sink's teardown, as
// the sink runtime needs it.
//
// It is a second, separate interface rather than a method on Pipeline for the
// same dependency reason (internal/pipeline imports internal/sink, so the arrow
// can only point one way) and because the two are genuinely different components:
// the pipeline drops the sink's dedup caches, the coordinator drops its
// boot-reconciliation mark and any warm still running for it.
// pipeline.WarmCoordinator is the production implementation.
type WarmHooks interface {
	// ForgetSink discards the coordinator's per-sink bookkeeping. It must be safe
	// to call for a name the coordinator never saw, and is called immediately
	// after RemoveSink so a sink deleted and re-created under the same name is
	// boot-reconciled again instead of inheriting a stale "already done" mark.
	ForgetSink(name string)
}

// Dependents reports which rules currently stream to a sink, so the parking
// callback can name them.
//
// It is optional: without it the callback still fires with the sink's name and an
// empty rule list, which is enough for a consumer that can resolve dependents
// itself. It exists as a separate interface (rather than the manager reading the
// desired-state registry) to keep this package free of any notion of rules or
// watch targets.
type Dependents interface {
	// RulesForSink returns the keys of the rules streaming to sinkName, in any
	// order. An unknown sink yields an empty slice, never an error: a sink nothing
	// references is an ordinary state, not a fault.
	RulesForSink(sinkName string) []string
}

// ParkFunc is invoked when a sink is gone for good, with the rule keys that
// depended on it. Task 1.7's rule reconciler consumes it and parks those rules
// with Ready=False, reason=SinkMissing.
//
// Implementations must not block: it is called from the goroutine that just
// finished draining the sink, and a callback that waited on the API server would
// hold that goroutine (and, through the wait group, the manager's shutdown) for
// as long as the API server took to answer.
type ParkFunc func(sinkName string, ruleKeys []string)

// ManagerOptions configures a SinkManager. Factory and Pipeline are mandatory;
// everything else has a documented default or is genuinely optional.
type ManagerOptions struct {
	// Factory builds each sink's backend instance. Required.
	Factory Factory

	// Pipeline is the data plane whose per-sink state a deleted sink's removal
	// evicts. Required.
	Pipeline Pipeline

	// Warm is the warm/GC coordinator whose per-sink bookkeeping a deleted sink's
	// removal clears. Optional: nil means only the pipeline's state is evicted,
	// which is the correct behaviour for a deployment (or a test) that runs no
	// coordinator.
	Warm WarmHooks

	// Dependents resolves a sink's dependent rules for the parking callback. Nil
	// means the callback fires with an empty rule list.
	Dependents Dependents

	// OnSinkGone is called after a deleted sink has drained and its pipeline state
	// has been evicted. Nil means nothing is parked — correct for a deployment
	// with no rule reconciler yet (which is precisely the state 1.8 merges into).
	OnSinkGone ParkFunc

	// ProbeInterval is how often a healthy sink is re-probed. Zero or negative
	// means defaultProbeInterval.
	ProbeInterval time.Duration

	// ProbeMinBackoff / ProbeMaxBackoff bound the retry schedule after a failed
	// probe. Zero or negative means the package defaults.
	ProbeMinBackoff time.Duration
	ProbeMaxBackoff time.Duration

	// ProbeTimeout bounds one probe attempt. Zero or negative means
	// defaultProbeTimeout.
	ProbeTimeout time.Duration

	// DrainTimeout bounds the wait for one instance's Start to return after it is
	// cancelled. Zero or negative means defaultDrainTimeout.
	DrainTimeout time.Duration

	// ResultBuffer is the probe-result channel's capacity. Zero or negative means
	// defaultResultBuffer.
	ResultBuffer int
}

// liveSink is one running instance: the backend, the optional halves of the sink
// contract it turned out to implement, the fingerprint it was built from, and the
// handles that stop it.
//
// The optional halves are resolved once, at construction, rather than on every
// routing call: the type assertions are cheap but the hot path resolves a writer
// per work item, and a nil field is a clearer answer than a repeated assertion.
type liveSink struct {
	name        string
	fingerprint string

	writer Writer
	// reader, events and prober are nil when the backend does not implement that
	// half of the contract. See Factory.
	reader StateReader
	events ScopeEventWriter
	prober Prober

	// cancel stops this instance (its writer's own drain-then-close sequence and
	// its probe loop); done is closed once the writer's Start has returned, which
	// is the only reliable signal that its queue is flushed and its connection
	// closed.
	cancel context.CancelFunc
	done   chan struct{}
}

// newLiveSink wraps a freshly built writer, discovering which optional halves of
// the sink contract it implements.
func newLiveSink(name, fingerprint string, writer Writer) *liveSink {
	inst := &liveSink{name: name, fingerprint: fingerprint, writer: writer, done: make(chan struct{})}
	if reader, ok := writer.(StateReader); ok {
		inst.reader = reader
	}
	if events, ok := writer.(ScopeEventWriter); ok {
		inst.events = events
	}
	if prober, ok := writer.(Prober); ok {
		inst.prober = prober
	}
	return inst
}

// SinkManager runs one backend instance per sink CR and is the operator's single
// authority on which instance currently serves a given sink name.
//
// It exists because sinks, like rules, come and go at runtime (Phase 1's whole
// premise): a ClickHouseSink can be created hours after boot, have its password
// rotated, or be deleted, and none of those may cost a restart or a lost write.
// Three properties carry that:
//
//   - Routing is an immutable map swapped atomically, so a recycle is a single
//     store: no work item can ever observe a half-updated routing table, and the
//     hot path resolves a writer without taking a lock.
//   - A replaced instance is drained *after* the swap, never before. Jobs already
//     handed to it settle on it — their commit callbacks fire exactly once, on the
//     cache state they were reserved against (Invariant 3) — while new work goes
//     to the new instance.
//   - Draining happens on the manager's own goroutines. Ensure and Delete are
//     called from reconcilers, and Invariant 1 forbids a reconciler from waiting
//     on a sink round-trip.
//
// It is the production implementation of pipeline.SinkRouter,
// pipeline.StateReaderRouter and pipeline.ScopeEventRouter; those assertions live
// in this package's external test file (an in-package assertion would be an
// import cycle, since internal/pipeline imports internal/sink).
type SinkManager struct {
	factory    Factory
	pipeline   Pipeline
	warm       WarmHooks
	dependents Dependents
	park       ParkFunc

	probeInterval   time.Duration
	probeMinBackoff time.Duration
	probeMaxBackoff time.Duration
	probeTimeout    time.Duration
	drainTimeout    time.Duration

	// results carries probe outcomes to whoever consumes ProbeResults.
	results chan ProbeResult

	// live is the routing table: a pointer to an immutable map, replaced wholesale
	// on every change. Readers (WriterFor and friends, called once per work item)
	// load the pointer and never lock; writers hold mu while they build and store
	// the successor. Never nil — New stores an empty map.
	live atomic.Pointer[map[string]*liveSink]

	// mu serializes lifecycle operations (Ensure, Delete, shutdown) with each
	// other. It does not guard reads of live, which is the point of the atomic
	// pointer: a slow factory or a wedged drain must never stall the hot path.
	mu sync.Mutex
	// ctx is the manager's lifetime, installed by Start. It is nil until then,
	// which is not a wiring error: the manager and the reconcilers that drive it
	// are both manager.Runnables with no ordering guarantee between them, so an
	// Ensure can legitimately arrive first and is held in pending.
	ctx     context.Context
	pending map[string]InstanceConfig
	stopped bool

	// wg tracks every goroutine the manager owns — instance writers, probe loops,
	// and the drain/evict tails of Ensure and Delete — so Start returns only once
	// all of them have exited (a goleak-verified property).
	wg sync.WaitGroup

	// log is what the goroutines above log through: they outlive any single call
	// and have no context of their own to carry a logger.
	//
	// It is fixed at construction rather than replaced by Start with the logger
	// from its context, and deliberately so: a field written by Start while a
	// delete tail from before the shutdown still reads it is a data race for no
	// benefit — logf.Log is the same delegating sink a context logger ultimately
	// writes to, so the only difference would be the caller-injected values.
	log logr.Logger
}

// NewSinkManager builds a SinkManager. The two mandatory dependencies are
// validated eagerly because either of them would otherwise surface as a
// nil-pointer panic on a lifecycle goroutine — long after the wiring mistake, and
// in the middle of a drain whose whole job is to not lose writes.
func NewSinkManager(opts ManagerOptions) (*SinkManager, error) {
	if opts.Factory == nil {
		return nil, errors.New("sink: ManagerOptions.Factory is required")
	}
	if opts.Pipeline == nil {
		return nil, errors.New("sink: ManagerOptions.Pipeline is required")
	}

	probeInterval := opts.ProbeInterval
	if probeInterval <= 0 {
		probeInterval = defaultProbeInterval
	}
	minBackoff := opts.ProbeMinBackoff
	if minBackoff <= 0 {
		minBackoff = defaultProbeMinBackoff
	}
	maxBackoff := opts.ProbeMaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultProbeMaxBackoff
	}
	probeTimeout := opts.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	drainTimeout := opts.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}
	resultBuffer := opts.ResultBuffer
	if resultBuffer <= 0 {
		resultBuffer = defaultResultBuffer
	}

	m := &SinkManager{
		factory:         opts.Factory,
		pipeline:        opts.Pipeline,
		warm:            opts.Warm,
		dependents:      opts.Dependents,
		park:            opts.OnSinkGone,
		probeInterval:   probeInterval,
		probeMinBackoff: minBackoff,
		probeMaxBackoff: maxBackoff,
		probeTimeout:    probeTimeout,
		drainTimeout:    drainTimeout,
		results:         make(chan ProbeResult, resultBuffer),
		pending:         make(map[string]InstanceConfig),
		log:             logf.Log.WithName("sinks"),
	}
	empty := make(map[string]*liveSink)
	m.live.Store(&empty)
	return m, nil
}

// Start runs until ctx is cancelled, then drains and closes every instance. It
// satisfies manager.Runnable.
//
// There is no loop: the manager is edge-driven by Ensure and Delete, which the
// SinkReconciler calls level-triggered from its own reconcile passes, so a second
// level-triggering loop here would only duplicate that work. What Start owns is
// the *lifetime*: the context every instance derives from, the moment pending
// configurations become running instances, and the shutdown ordering.
//
// Shutdown deliberately does not call RemoveSink or park any rules. A process
// exiting is not a sink going away: the pipeline state disappears with the
// process anyway (Invariant 6), and parking rules on the way out would write
// Degraded conditions describing an operator that no longer exists.
func (m *SinkManager) Start(ctx context.Context) error {
	log := m.log
	// Every instance derives its context from this one, so a backend that reads its
	// logger from the context (as the pipeline and watch layers do) inherits the
	// manager's name rather than logging unnamed.
	ctx = logf.IntoContext(ctx, log)

	m.mu.Lock()
	m.ctx = ctx
	pending := m.pending
	m.pending = make(map[string]InstanceConfig)
	// Sorted so a boot with several pending sinks starts them in a stable order,
	// which keeps startup logs comparable between runs.
	for _, name := range slices.Sorted(maps.Keys(pending)) {
		if err := m.ensureLocked(name, pending[name]); err != nil {
			// One bad sink must not stop the others from starting (Invariant 5);
			// its reconciler retries, and its CR reports the failure (Task 1.7).
			log.Error(err, "Failed to start a sink declared before the manager ran", "sink", name)
		}
	}
	log.Info("Started sink manager", "sinks", len(*m.live.Load()))
	m.mu.Unlock()

	<-ctx.Done()

	// Take the instances out of the routing table before draining them, so a work
	// item that arrives during shutdown is told "no live writer" (and re-queued)
	// rather than handed a writer that is closing its connection.
	m.mu.Lock()
	m.stopped = true
	instances := slices.Collect(maps.Values(*m.live.Load()))
	empty := make(map[string]*liveSink)
	m.live.Store(&empty)
	m.mu.Unlock()

	log.Info("Stopping sink manager", "sinks", len(instances))
	// Concurrently: each instance's drain is dominated by flushing its own queue,
	// and serializing them would make shutdown as slow as the sum of every
	// backend's drain budget.
	var drains sync.WaitGroup
	for _, inst := range instances {
		drains.Go(func() { m.drain(inst) })
	}
	drains.Wait()

	// The instance writers, probe loops and delete tails are all in m.wg.
	m.wg.Wait()
	log.Info("Sink manager stopped")
	return nil
}

// NeedLeaderElection makes the SinkManager a manager.LeaderElectionRunnable that
// runs only on the elected leader.
//
// A non-leader has no data plane to serve — the WatchManager and the pipeline are
// leader-gated for the same reason — so holding open a ClickHouse connection per
// sink CR on every replica would buy nothing and cost a connection, a probe
// round-trip and a set of duplicate per-sink metric series each.
func (m *SinkManager) NeedLeaderElection() bool { return true }

// Ensure declares that the sink named name must be running with cfg.
//
// It is idempotent: a configuration whose fingerprint matches the running
// instance's is a no-op, which is what makes it safe to call from every reconcile
// pass (including the periodic resyncs that re-deliver an unchanged spec).
//
// When the fingerprint differs — a rotated password, a re-pointed address, a
// re-tuned queue — the new instance is built and started, routing is swapped to
// it atomically, and only then is the old one drained, on a background
// goroutine. That order is what makes a rotation lossless: every job already
// handed to the old instance settles there, with its commit callback firing
// exactly once against the cache version it reserved (Invariant 3), while every
// job issued after the swap goes to the new instance. The sink's pipeline state
// (hashCache, warm scopes) is deliberately *kept* across a recycle: it is the
// same sink, with the same durable history, so discarding its dedup baselines
// would re-emit every object in every scope it serves.
//
// A factory failure leaves the previous instance running and is returned to the
// caller, which is the graceful degradation Invariant 5 asks for: a
// mis-configured update must not take down a sink that was working.
func (m *SinkManager) Ensure(name string, cfg InstanceConfig) error {
	if name == "" {
		return errors.New("sink: Ensure requires a sink name")
	}
	if cfg == nil {
		return fmt.Errorf("sink: Ensure(%q) requires a configuration", name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return errManagerStopped
	}
	if m.ctx == nil {
		// Not started yet; hold the request rather than dropping it, so a sink CR
		// reconciled before this runnable came up is still brought up.
		m.pending[name] = cfg
		return nil
	}
	return m.ensureLocked(name, cfg)
}

// ensureLocked is Ensure's body, with mu held and the manager known to be
// running.
func (m *SinkManager) ensureLocked(name string, cfg InstanceConfig) error {
	fingerprint := cfg.Fingerprint()
	current := (*m.live.Load())[name]
	if current != nil && current.fingerprint == fingerprint {
		return nil
	}

	writer, err := m.factory(name, cfg)
	if err != nil {
		return fmt.Errorf("build sink %q: %w", name, err)
	}
	inst := newLiveSink(name, fingerprint, writer)
	m.startLocked(inst)
	m.swapLocked(name, inst)

	if current == nil {
		m.log.Info("Sink instance started", "sink", name, "fingerprint", fingerprint)
		return nil
	}

	m.log.Info("Sink instance recycled; draining the previous one",
		"sink", name, "fingerprint", fingerprint, "previous_fingerprint", current.fingerprint)
	m.wg.Go(func() { m.drain(current) })
	return nil
}

// Delete stops the sink named name for good: routing is withdrawn immediately,
// and then — on a background goroutine — the instance is drained and closed, the
// pipeline's per-sink state is evicted, and the rules that streamed to it are
// parked.
//
// The ordering is the load-bearing part. Routing goes first so no new job is
// handed to a sink that is going away (the pipeline re-queues those keys, which
// is correct: their changes are real and unrecorded). RemoveSink comes strictly
// after the drain, because the in-flight jobs settling during that drain still
// commit against the hashCache it discards — evicting first would let a confirmed
// write revert into a cache that no longer exists, and the next observation of
// those objects would re-emit them. Parking comes last, once there is genuinely
// nothing left running for the name.
//
// Deleting a name the manager never knew still evicts and parks: a rule can
// reference a sink whose CR never existed, and it needs the same Degraded
// condition as one whose sink was removed.
func (m *SinkManager) Delete(name string) {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	delete(m.pending, name)
	current := (*m.live.Load())[name]
	if current != nil {
		m.swapLocked(name, nil)
	}
	// was_live distinguishes the two shapes of a deletion: an instance that has to
	// be drained, and a name that was only ever referenced by rules.
	m.log.Info("Sink deleted; draining it and evicting its pipeline state",
		"sink", name, "was_live", current != nil)
	m.wg.Go(func() { m.finishDelete(name, current) })
	m.mu.Unlock()
}

// finishDelete is Delete's background tail: drain, evict, park.
func (m *SinkManager) finishDelete(name string, inst *liveSink) {
	if inst != nil {
		m.drain(inst)
	}

	// A sink recreated while its predecessor drained (delete-then-recreate, or a
	// CR re-applied within the drain window) already has a live instance and a
	// fresh hashCache under this name. Evicting now would drop *its* state, and
	// parking its rules would degrade rules that are working — so the tail stands
	// down instead.
	m.mu.Lock()
	_, recreated := (*m.live.Load())[name]
	m.mu.Unlock()
	if recreated {
		m.log.V(1).Info("Sink was recreated while its previous instance drained; keeping the new instance's state",
			"sink", name)
		return
	}

	m.pipeline.RemoveSink(name)
	if m.warm != nil {
		// Paired with RemoveSink, and for the same reason: the coordinator's
		// per-sink bookkeeping outlives the caches it describes otherwise, and a
		// sink re-created under this name would never be boot-reconciled again
		// (see WarmHooks).
		m.warm.ForgetSink(name)
	}
	m.parkDependents(name)
}

// parkDependents fires the parking callback for a sink that is gone.
func (m *SinkManager) parkDependents(name string) {
	if m.park == nil {
		return
	}
	var ruleKeys []string
	if m.dependents != nil {
		ruleKeys = m.dependents.RulesForSink(name)
	}
	m.log.Info("Parking the rules that streamed to a sink that is gone", "sink", name, "rules", ruleKeys)
	m.park(name, ruleKeys)
}

// startLocked launches inst's writer and (if the backend supports it) its probe
// loop, both under a context derived from the manager's own. The caller holds mu.
func (m *SinkManager) startLocked(inst *liveSink) {
	ctx, cancel := context.WithCancel(m.ctx)
	inst.cancel = cancel

	m.wg.Go(func() {
		// done is what drain waits on, so it must be closed however Start returns
		// — including on an error, which would otherwise hang every drain of this
		// instance until the timeout.
		defer close(inst.done)
		if err := inst.writer.Start(ctx); err != nil {
			m.log.Error(err, "Sink instance stopped with an error", "sink", inst.name)
		}
	})

	if inst.prober == nil {
		m.log.V(1).Info("Sink backend cannot be probed; no health results will be posted for it", "sink", inst.name)
		return
	}
	m.wg.Go(func() { m.probeLoop(ctx, inst) })
}

// swapLocked replaces the routing table with a copy carrying one changed entry (a
// nil instance removes the name). The caller holds mu.
//
// Copy-on-write, rather than mutating a shared map under a lock, is what lets
// WriterFor be lock-free on the hot path: a reader holds a snapshot that is never
// written to again, so there is no torn state to observe and no reader to block.
// Sinks change at human frequency, so the copy is free.
func (m *SinkManager) swapLocked(name string, inst *liveSink) {
	current := *m.live.Load()
	next := make(map[string]*liveSink, len(current)+1)
	maps.Copy(next, current)
	if inst == nil {
		delete(next, name)
	} else {
		next[name] = inst
	}
	m.live.Store(&next)
}

// drain stops one instance and waits for its writer's Start to return, which is
// the point at which its queue is flushed and its connection closed.
//
// It never settles a job itself: the instance's own shutdown sequence drains the
// queue and fires each job's commit callback exactly once, so the exactly-once
// contract survives a swap without this code knowing anything about it. Waiting
// is bounded, and exceeding the bound is an anomaly worth an Error log — but not
// worth blocking the manager, so it gives up and moves on (Invariant 5).
func (m *SinkManager) drain(inst *liveSink) {
	inst.cancel()

	timer := time.NewTimer(m.drainTimeout)
	defer timer.Stop()
	select {
	case <-inst.done:
		m.log.V(1).Info("Sink instance drained and closed", "sink", inst.name)
	case <-timer.C:
		m.log.Error(errDrainTimeout, "Sink instance is still draining; abandoning the wait",
			"sink", inst.name, "timeout", m.drainTimeout.String())
	}
}

// WriterFor implements pipeline.SinkRouter: the live Writer for name, or
// ok=false when there is none — the CR was deleted, its instance is mid-build, or
// the manager has not started yet. False is an ordinary, transient answer; the
// pipeline re-queues the key on its rate limiter.
//
// It is on the hot path (once per work item), which is why it only loads an
// atomic pointer and reads an immutable map.
func (m *SinkManager) WriterFor(name string) (Writer, bool) {
	inst, ok := (*m.live.Load())[name]
	if !ok {
		return nil, false
	}
	return inst.writer, true
}

// StateReaderFor implements pipeline.StateReaderRouter: the live StateReader for
// name, or ok=false when the sink has no live instance *or* when its backend
// cannot read its own history back.
//
// The two cases are deliberately not distinguished here — the caller separates
// them by asking WriterFor whether the sink is live at all, which is exactly what
// the warm coordinator's readerFor does. Collapsing them into one boolean keeps
// the routing surface uniform across the three routers.
func (m *SinkManager) StateReaderFor(name string) (StateReader, bool) {
	inst, ok := (*m.live.Load())[name]
	if !ok || inst.reader == nil {
		return nil, false
	}
	return inst.reader, true
}

// ScopeEventWriterFor implements pipeline.ScopeEventRouter: the live scope-log
// writer for name, or ok=false when the sink has no live instance or its backend
// does not record scope epochs.
func (m *SinkManager) ScopeEventWriterFor(name string) (ScopeEventWriter, bool) {
	inst, ok := (*m.live.Load())[name]
	if !ok || inst.events == nil {
		return nil, false
	}
	return inst.events, true
}

// SinkNames implements the enumeration half of pipeline.StateReaderRouter: the
// names of the sinks currently live, sorted.
//
// Boot reconciliation needs the enumeration rather than a per-sink probe, because
// a rule deleted while the operator was down leaves an open scope nothing in the
// desired state mentions any more — there is no candidate list to probe. Sorting
// is not required by the contract; it just makes the pass's logs stable.
func (m *SinkManager) SinkNames() []string {
	return slices.Sorted(maps.Keys(*m.live.Load()))
}

// ProbeResults returns the channel every probe attempt's outcome is published on.
// Task 1.7's SinkReconciler consumes it and turns each result into CR conditions;
// nothing in this package interprets them.
//
// The channel is buffered and never closed. A send blocks (against the manager's
// own context, so shutdown always wins) rather than dropping: a probe result is
// the only signal that a sink's health changed, and silently discarding one would
// leave a CR claiming Ready long after its backend stopped answering (Invariant
// 4). A consumer that stops reading therefore stalls its sinks' probing — and
// nothing else — until it resumes.
func (m *SinkManager) ProbeResults() <-chan ProbeResult { return m.results }

// probeLoop probes one instance until its context ends, posting every outcome.
//
// It keeps probing after a success (at probeInterval) because "this sink went
// unreachable an hour after it came up" is exactly the condition an operator
// needs reported, and a latching probe could never report it. Failures retry on
// an exponential schedule capped at probeMaxBackoff, so a permanently
// unreachable backend settles into a steady cadence instead of hammering it —
// and every attempt is reported, which is what lets a consumer distinguish "still
// failing" from "stopped checking".
//
// The backoff is deliberately un-jittered: probes are low-rate and per-sink, so
// jitter would buy no herd protection worth the untestable schedule it costs.
func (m *SinkManager) probeLoop(ctx context.Context, inst *liveSink) {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = m.probeMinBackoff
	eb.MaxInterval = m.probeMaxBackoff
	eb.RandomizationFactor = 0
	eb.MaxElapsedTime = 0 // never give up; only ctx cancellation stops the loop
	eb.Reset()

	for {
		result := m.probe(ctx, inst)
		if ctx.Err() != nil {
			// The failure is our own shutdown cancelling the attempt, not the
			// backend's. Reporting it would flip a healthy sink's condition to
			// Degraded on the way out.
			return
		}
		if !m.postResult(ctx, result) {
			return
		}

		timer := time.NewTimer(m.nextProbeDelay(eb, result.Err != nil))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// nextProbeDelay returns how long to wait before probing a sink again, and keeps
// eb's schedule in step with the outcomes seen so far.
//
// A success resets the schedule — a sink that recovers must not inherit the
// backoff its outage accumulated — and returns to the steady interval. A failure
// takes the next step of the exponential schedule, capped at probeMaxBackoff.
//
// It is a method (rather than two lines inside probeLoop) so the schedule itself
// is assertable without waiting for it: the loop's timing is only observable in
// real time, but the sequence of delays it chooses is pure.
func (m *SinkManager) nextProbeDelay(eb *backoff.ExponentialBackOff, failed bool) time.Duration {
	if !failed {
		eb.Reset()
		return m.probeInterval
	}
	delay := eb.NextBackOff()
	if delay <= 0 {
		// Unreachable while MaxElapsedTime is disabled (the schedule then never
		// returns backoff.Stop), but a non-positive delay would turn the probe loop
		// into a spin against an already-unreachable backend, so it degrades to the
		// cap rather than trusting that invariant to hold forever.
		return m.probeMaxBackoff
	}
	return delay
}

// probe runs one attempt under probeTimeout and classifies its outcome.
func (m *SinkManager) probe(ctx context.Context, inst *liveSink) ProbeResult {
	attemptCtx, cancel := context.WithTimeout(ctx, m.probeTimeout)
	defer cancel()

	err := inst.prober.Probe(attemptCtx)
	result := ProbeResult{Sink: inst.name, At: time.Now().UTC(), Err: err}
	if err == nil {
		return result
	}

	result.Reason = ProbeReasonUnreachable
	if errors.Is(err, ErrSchemaInvalid) {
		result.Reason = ProbeReasonSchemaInvalid
	}
	// Logged here, with the sink's identity, so the condition the reconciler
	// eventually writes is never the only record of the failure (Invariant 4).
	m.log.Error(err, "Sink probe failed", "sink", inst.name, "reason", result.Reason)
	return result
}

// postResult publishes one result, reporting false if the manager shut down
// before the consumer took it.
func (m *SinkManager) postResult(ctx context.Context, result ProbeResult) bool {
	select {
	case m.results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

// compile-time proof that a SinkManager is usable as a leader-election-gated
// manager.Runnable without importing controller-runtime's manager package here.
var _ interface {
	Start(ctx context.Context) error
	NeedLeaderElection() bool
} = (*SinkManager)(nil)
