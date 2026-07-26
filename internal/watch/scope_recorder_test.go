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
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/sink"
)

// The recorder's dependencies are both narrow interfaces, so these tests need
// neither an apiserver nor a ClickHouse: fakeScopeEventWriter stands in for the
// sink's scope log and fakeWarmer for Task 1.6's warm coordinator.

const (
	recorderSink    = "sink-a"
	recorderCluster = "test-cluster"
)

// recorderScope is the scope every test here narrates transitions for.
var recorderScope = pipeline.ScopeKey{Kind: "Pod", Namespace: "ns-a"}

// fakeScopeEventWriter records accepted scope events and can be scripted to
// refuse a given number of hand-offs first — the "the sink is down but the watch
// started anyway" scenario.
type fakeScopeEventWriter struct {
	mu       sync.Mutex
	events   []sink.ScopeEvent
	failures int
	attempts int
}

func (f *fakeScopeEventWriter) EnqueueScopeEvent(_ context.Context, event sink.ScopeEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failures > 0 {
		f.failures--
		return errors.New("scope log unavailable")
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakeScopeEventWriter) recorded() []sink.ScopeEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

func (f *fakeScopeEventWriter) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// failNext makes the next n hand-offs fail.
func (f *fakeScopeEventWriter) failNext(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = n
}

// fakeScopeEventRouter maps sink names to scope-log writers. A name that is absent
// reproduces a sink that is not live yet.
type fakeScopeEventRouter struct {
	mu      sync.Mutex
	writers map[string]sink.ScopeEventWriter
}

func newFakeScopeEventRouter() *fakeScopeEventRouter {
	return &fakeScopeEventRouter{writers: make(map[string]sink.ScopeEventWriter)}
}

func (f *fakeScopeEventRouter) ScopeEventWriterFor(name string) (sink.ScopeEventWriter, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.writers[name]
	return w, ok
}

// attach and detach operate on recorderSink, the one sink these tests use: a name
// that resolves to nothing is how they reproduce a sink that is not live yet.
func (f *fakeScopeEventRouter) attach(w sink.ScopeEventWriter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writers[recorderSink] = w
}

func (f *fakeScopeEventRouter) detach() {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.writers, recorderSink)
}

// warmCall is one WarmScope / StopScope invocation.
type warmCall struct {
	action string
	target pipeline.WarmTarget
}

// fakeWarmer records the scope-level warm edges the recorder derives.
type fakeWarmer struct {
	mu    sync.Mutex
	calls []warmCall
}

func (f *fakeWarmer) WarmScope(target pipeline.WarmTarget) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, warmCall{action: "warm", target: target})
}

func (f *fakeWarmer) StopScope(sinkName string, scope pipeline.ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, warmCall{action: "stop",
		target: pipeline.WarmTarget{Sink: sinkName, Scope: scope}})
}

func (f *fakeWarmer) recorded() []warmCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// recorderHarness is a running ScopeEpochRecorder plus its doubles.
type recorderHarness struct {
	recorder *ScopeEpochRecorder
	writer   *fakeScopeEventWriter
	router   *fakeScopeEventRouter
	warmer   *fakeWarmer
	logs     *logCapture
}

// newRecorderHarness builds a recorder with a fast flush interval and starts it.
func newRecorderHarness(t *testing.T, mutate ...func(*ScopeRecorderOptions)) *recorderHarness {
	t.Helper()

	h := &recorderHarness{
		writer: &fakeScopeEventWriter{},
		router: newFakeScopeEventRouter(),
		warmer: &fakeWarmer{},
		logs:   &logCapture{},
	}
	h.router.attach(h.writer)

	opts := ScopeRecorderOptions{
		ClusterID:     recorderCluster,
		Events:        h.router,
		Warmer:        h.warmer,
		FlushInterval: 10 * time.Millisecond,
	}
	for _, m := range mutate {
		m(&opts)
	}

	r, err := NewScopeEpochRecorder(opts)
	if err != nil {
		t.Fatalf("NewScopeEpochRecorder: %v", err)
	}
	h.recorder = r

	ctx, cancel := context.WithCancel(logf.IntoContext(t.Context(), h.logs.logger()))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.Start(ctx); err != nil {
			t.Errorf("Start returned an error: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the scope recorder did not stop")
		}
	})
	return h
}

// transition builds one interest-level edge for the shared test scope.
func transition(target string, ruleKeys ...string) ScopeTransition {
	return ScopeTransition{
		Sink:       recorderSink,
		Scope:      recorderScope,
		Target:     target,
		APIVersion: "v1",
		RuleKeys:   ruleKeys,
		At:         time.Now().UTC(),
	}
}

// awaitEvents waits for at least n recorded events and returns them.
func (h *recorderHarness) awaitEvents(t *testing.T, n int) []sink.ScopeEvent {
	t.Helper()
	var got []sink.ScopeEvent
	waitFor(t, func() bool {
		got = h.writer.recorded()
		return len(got) >= n
	}, func() string { return "recorded scope events" })
	return got
}

// eventActions returns the action of every recorded event, in order.
func (h *recorderHarness) eventActions() []string {
	recorded := h.writer.recorded()
	out := make([]string, 0, len(recorded))
	for _, e := range recorded {
		out = append(out, string(e.Action))
	}
	return out
}

// TestNewScopeEpochRecorderValidatesOptions covers the eager dependency check: a
// nil router would surface as a panic on the WatchManager's reconcile loop, taking
// the data plane's level-triggering down with it.
func TestNewScopeEpochRecorderValidatesOptions(t *testing.T) {
	if _, err := NewScopeEpochRecorder(ScopeRecorderOptions{}); err == nil {
		t.Error("expected an error when Events is nil, got nil")
	}

	r, err := NewScopeEpochRecorder(ScopeRecorderOptions{Events: newFakeScopeEventRouter()})
	if err != nil {
		t.Fatalf("NewScopeEpochRecorder: %v", err)
	}
	if r.flushInterval != defaultScopeFlushInterval || r.queueLimit != defaultScopeQueueLimit {
		t.Error("pacing and bounds were not defaulted to the documented values")
	}
	if !r.NeedLeaderElection() {
		t.Error("NeedLeaderElection = false, want true: two replicas would double every row in the scope log")
	}
	// A nil warmer is legal: the recorder still narrates epochs, it simply drives
	// no warm-up.
	r.ScopeStarted(transition("informer-1", "rule-1"))
}

// TestScopeEpochRecorderTransitionSemantics is the transition-semantics acceptance
// criterion: one Started row when a (sink, scope) pair gains its first interest, no
// row while it merely gains more, and one Stopped row when it loses its last —
// never one row per rule and never one per informer.
func TestScopeEpochRecorderTransitionSemantics(t *testing.T) {
	h := newRecorderHarness(t)

	// Two rules on one scope reach the recorder as one interest (they share an
	// informer), so this is the ordinary first-interest edge.
	h.recorder.ScopeStarted(transition("informer-v1", "rule-1", "rule-2"))
	events := h.awaitEvents(t, 1)
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1: %+v", len(events), events)
	}
	if events[0].Action != sink.ScopeActionStarted {
		t.Errorf("action = %q, want Started", events[0].Action)
	}
	if events[0].RuleRef != "rule-1" {
		t.Errorf("rule_ref = %q, want rule-1 (the first contributing rule, deterministically)", events[0].RuleRef)
	}

	// A second interest on the same scope — a rule naming a different version of the
	// same resource, which is a second informer but the same version-agnostic scope.
	h.recorder.ScopeStarted(transition("informer-v2", "rule-3"))
	// And a restatement of the first interest, as a selector edit produces.
	h.recorder.ScopeStarted(transition("informer-v1", "rule-1", "rule-2", "rule-4"))
	staysAt(t, func() int { return len(h.writer.recorded()) }, 1,
		"a scope that merely gained interests recorded more than one Started row")

	// Losing one of two interests is not the end of the epoch.
	h.recorder.ScopeStopped(transition("informer-v2", "rule-3"))
	staysAt(t, func() int { return len(h.writer.recorded()) }, 1,
		"losing one of two interests closed the scope's epoch")

	// Losing the last one is.
	h.recorder.ScopeStopped(transition("informer-v1", "rule-1"))
	events = h.awaitEvents(t, 2)
	if len(events) != 2 {
		t.Fatalf("recorded %d events, want 2: %+v", len(events), events)
	}
	if events[1].Action != sink.ScopeActionStopped {
		t.Errorf("action = %q, want Stopped", events[1].Action)
	}

	// A fresh interest afterwards opens a new epoch.
	h.recorder.ScopeStarted(transition("informer-v1", "rule-5"))
	events = h.awaitEvents(t, 3)
	if got := []string{string(events[0].Action), string(events[1].Action), string(events[2].Action)}; !slices.Equal(
		got, []string{"Started", "Stopped", "Started"}) {
		t.Errorf("epoch sequence = %v, want Started, Stopped, Started", got)
	}
}

// TestScopeEpochRecorderIgnoresAnUnreportedStop covers the asymmetry the
// WatchManager deliberately allows: an interest whose informer never came up is
// still reported as stopped when its rule goes away. There is no epoch of ours to
// close, and a Stopped row here would describe a watch that never happened — worse,
// with a plain refcount it would close an epoch another interest still holds.
func TestScopeEpochRecorderIgnoresAnUnreportedStop(t *testing.T) {
	h := newRecorderHarness(t)

	// One interest is genuinely serving the scope.
	h.recorder.ScopeStarted(transition("informer-v1", "rule-1"))
	h.awaitEvents(t, 1)

	// A second interest for the same scope never started (its informer failed to
	// come up) and is now withdrawn.
	h.recorder.ScopeStopped(transition("informer-v2", "rule-2"))
	staysAt(t, func() int { return len(h.writer.recorded()) }, 1,
		"a stop for an interest that never started closed the scope's epoch")

	// The scope is still considered active, so its real interest's removal is what
	// closes it.
	h.recorder.ScopeStopped(transition("informer-v1", "rule-1"))
	if got := h.awaitEvents(t, 2); got[1].Action != sink.ScopeActionStopped {
		t.Errorf("action = %q, want Stopped", got[1].Action)
	}

	if calls := h.warmer.recorded(); len(calls) != 2 {
		t.Errorf("warmer saw %d edges, want 2 (one warm, one stop): %+v", len(calls), calls)
	}
}

// TestScopeEpochRecorderRetriesAFailedWriteWithoutDelayingTheWatch is the recorder
// failure criterion. The watch itself must start immediately whatever the sink is
// doing (Invariant 1), and the row that eventually lands must carry the moment the
// watch started — not the moment the sink came back — or the audit trail would date
// every epoch to the end of the outage.
func TestScopeEpochRecorderRetriesAFailedWriteWithoutDelayingTheWatch(t *testing.T) {
	h := newRecorderHarness(t)
	h.writer.failNext(3)

	started := transition("informer-v1", "rule-1")

	// The reporting call runs inline on the reconcile loop: it must return
	// essentially instantly even though the sink is refusing writes.
	begin := time.Now()
	h.recorder.ScopeStarted(started)
	if elapsed := time.Since(begin); elapsed > 100*time.Millisecond {
		t.Errorf("ScopeStarted blocked for %s; the watch lifecycle must never wait on the sink", elapsed)
	}

	// The warm edge is likewise immediate, so the data plane is already working
	// while the epoch row is still being retried.
	if calls := h.warmer.recorded(); len(calls) != 1 || calls[0].action != "warm" {
		t.Fatalf("warmer edges = %+v, want exactly one warm", calls)
	}

	events := h.awaitEvents(t, 1)
	if !events[0].TS.Equal(started.At) {
		t.Errorf("recorded ts = %s, want the transition instant %s: a retried row must not be re-stamped",
			events[0].TS, started.At)
	}
	if attempts := h.writer.attemptCount(); attempts < 4 {
		t.Errorf("hand-off attempts = %d, want at least 4 (three refusals plus the success)", attempts)
	}
	if !h.logs.contains("Failed to hand a watch-scope transition") {
		t.Error("a refused hand-off was not reported at Error level (Invariant 4)")
	}

	// Ordering survives the outage: the Stopped row cannot overtake the Started row
	// it follows, or the epoch would read inverted.
	h.recorder.ScopeStopped(transition("informer-v1", "rule-1"))
	got := h.awaitEvents(t, 2)
	if got[0].Action != sink.ScopeActionStarted || got[1].Action != sink.ScopeActionStopped {
		t.Errorf("event order = %v, want Started then Stopped", h.eventActions())
	}
	if !got[0].TS.Before(got[1].TS) && !got[0].TS.Equal(got[1].TS) {
		t.Errorf("Stopped row (%s) predates the Started row (%s) it follows", got[1].TS, got[0].TS)
	}
}

// TestScopeEpochRecorderQueuesForASinkThatIsNotLiveYet covers the ordinary startup
// race: a rule (and therefore a watch) can exist before its ClickHouseSink does.
// The transition is held, not lost, and lands when the sink appears.
func TestScopeEpochRecorderQueuesForASinkThatIsNotLiveYet(t *testing.T) {
	h := newRecorderHarness(t)
	h.router.detach()

	h.recorder.ScopeStarted(transition("informer-v1", "rule-1"))
	staysAt(t, func() int { return h.writer.attemptCount() }, 0,
		"the recorder tried to hand an event to a sink that is not live")

	h.router.attach(h.writer)
	if got := h.awaitEvents(t, 1); got[0].Action != sink.ScopeActionStarted {
		t.Errorf("action = %q, want Started", got[0].Action)
	}
}

// TestScopeEpochRecorderDropsTheOldestOnOverflow covers the one bounded-memory
// decision on this path: a sink unreachable for a very long time must not grow the
// queue without limit, so the oldest transitions are dropped — loudly, because
// losing an epoch row is an audit hole rather than routine shedding.
func TestScopeEpochRecorderDropsTheOldestOnOverflow(t *testing.T) {
	h := newRecorderHarness(t, func(o *ScopeRecorderOptions) { o.QueueLimit = 2 })
	h.router.detach()

	// Four alternating edges for one scope, none of which can be handed over.
	for i, target := range []string{"informer-v1", "informer-v1", "informer-v1", "informer-v1"} {
		if i%2 == 0 {
			h.recorder.ScopeStarted(transition(target, "rule-1"))
		} else {
			h.recorder.ScopeStopped(transition(target, "rule-1"))
		}
	}

	h.router.attach(h.writer)
	events := h.awaitEvents(t, 2)
	if len(events) != 2 {
		t.Fatalf("recorded %d events, want the 2 the queue could hold: %+v", len(events), events)
	}
	// The newest transitions survive: they describe the scope's current epoch.
	if events[0].Action != sink.ScopeActionStarted || events[1].Action != sink.ScopeActionStopped {
		t.Errorf("surviving events = %v, want the newest Started then Stopped", h.eventActions())
	}
	waitFor(t, func() bool { return h.logs.contains("Dropped the oldest watch-scope transitions") },
		func() string { return "the queue-overflow report" })
}

// TestScopeEpochRecorderDrivesWarmOnScopeEdges pins the other half of the
// recorder's job: warm-up is per scope, so it is kicked off on exactly the same
// edges the epoch rows are written on, with the epoch instant the row carries.
// Sharing that instant is what keeps the warm's epoch check from seeing the very
// Started row this transition writes.
func TestScopeEpochRecorderDrivesWarmOnScopeEdges(t *testing.T) {
	h := newRecorderHarness(t)

	started := transition("informer-v1", "rule-1")
	h.recorder.ScopeStarted(started)
	h.recorder.ScopeStarted(transition("informer-v2", "rule-2"))
	h.recorder.ScopeStopped(transition("informer-v2", "rule-2"))

	calls := h.warmer.recorded()
	if len(calls) != 1 || calls[0].action != "warm" {
		t.Fatalf("warmer edges = %+v, want exactly one warm", calls)
	}
	want := pipeline.WarmTarget{Sink: recorderSink, Scope: recorderScope, EpochStart: started.At}
	if calls[0].target != want {
		t.Errorf("warm target = %+v, want %+v", calls[0].target, want)
	}

	h.recorder.ScopeStopped(transition("informer-v1", "rule-1"))
	calls = h.warmer.recorded()
	if len(calls) != 2 || calls[1].action != "stop" {
		t.Fatalf("warmer edges = %+v, want a warm then a stop", calls)
	}
	if calls[1].target.Sink != recorderSink || calls[1].target.Scope != recorderScope {
		t.Errorf("stop target = %+v, want the scope that just closed", calls[1].target)
	}
}

// TestScopeEpochRecorderEventCarriesScopeIdentity pins the recorded row's shape,
// including the two fields whose meaning is easy to get wrong: the namespace is the
// scope's own (empty meaning the all-namespaces scope, not a wildcard), and
// api_version is provenance that never becomes part of scope identity.
func TestScopeEpochRecorderEventCarriesScopeIdentity(t *testing.T) {
	h := newRecorderHarness(t)

	clusterWide := ScopeTransition{
		Sink:       recorderSink,
		Scope:      pipeline.ScopeKey{Group: "apps", Kind: "Deployment"},
		Target:     "apps/v1, Resource=deployments (cluster-wide)",
		APIVersion: "v1",
		RuleKeys:   []string{"cluster-rule"},
		At:         time.Now().UTC(),
	}
	h.recorder.ScopeStarted(clusterWide)

	got := h.awaitEvents(t, 1)[0]
	want := sink.ScopeEvent{
		Action:     sink.ScopeActionStarted,
		Scope:      sink.ScopeFilter{ClusterID: recorderCluster, APIGroup: "apps", Kind: "Deployment"},
		APIVersion: "v1",
		RuleRef:    "cluster-rule",
		TS:         clusterWide.At,
	}
	if got != want {
		t.Errorf("recorded event = %+v, want %+v", got, want)
	}
}

// TestScopeEpochRecorderConcurrentTransitions runs the write side against the flush
// side under -race, which is the only way to prove the bookkeeping and the queue are
// safe: transitions arrive on the WatchManager's loop while the flush loop drains.
func TestScopeEpochRecorderConcurrentTransitions(t *testing.T) {
	h := newRecorderHarness(t)

	const scopes, churn = 8, 20
	var wg sync.WaitGroup
	for s := range scopes {
		wg.Go(func() {
			scope := pipeline.ScopeKey{Kind: "Pod", Namespace: "ns-" + string(rune('a'+s))}
			for range churn {
				edge := ScopeTransition{
					Sink: recorderSink, Scope: scope, Target: "informer", APIVersion: "v1",
					RuleKeys: []string{"rule"}, At: time.Now().UTC(),
				}
				h.recorder.ScopeStarted(edge)
				h.recorder.ScopeStopped(edge)
			}
		})
	}
	wg.Wait()

	// Every scope's edges alternate, so each churn iteration produces exactly one
	// Started and one Stopped.
	waitFor(t, func() bool { return len(h.writer.recorded()) == scopes*churn*2 },
		func() string { return "every transition to be recorded" })
}

// TestScopeEpochRecorderStopsCleanly asserts the shutdown contract: Start makes one
// final hand-off attempt for whatever is queued (the manager stops runnables before
// the sink drains, so a last-moment epoch can still land), reports anything it could
// not place, and leaves no goroutine behind.
func TestScopeEpochRecorderStopsCleanly(t *testing.T) {
	snapshot := goleak.IgnoreCurrent()

	router := newFakeScopeEventRouter()
	writer := &fakeScopeEventWriter{}
	logs := &logCapture{}
	r, err := NewScopeEpochRecorder(ScopeRecorderOptions{
		ClusterID: recorderCluster,
		Events:    router,
		// Long enough that the ticker cannot flush before shutdown does.
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewScopeEpochRecorder: %v", err)
	}

	ctx, cancel := context.WithCancel(logf.IntoContext(context.Background(), logs.logger()))
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	// Queued with no live sink, so nothing can have been flushed yet.
	router.detach()
	r.ScopeStarted(transition("informer-v1", "rule-1"))

	// The sink becomes available just as the process shuts down: the drain attempt is
	// what gets this epoch recorded at all.
	router.attach(writer)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the recorder did not stop")
	}

	if got := writer.recorded(); len(got) != 1 || got[0].Action != sink.ScopeActionStarted {
		t.Errorf("recorded %+v, want the queued Started row flushed during the drain", got)
	}
	goleak.VerifyNone(t, snapshot)
}

// TestScopeEpochRecorderReportsUnflushedEventsOnShutdown covers the other shutdown
// outcome: a sink that never comes back means the scope log is genuinely missing
// rows, and that must be stated in the log rather than passed over in silence
// (Invariant 4).
func TestScopeEpochRecorderReportsUnflushedEventsOnShutdown(t *testing.T) {
	router := newFakeScopeEventRouter()
	logs := &logCapture{}
	r, err := NewScopeEpochRecorder(ScopeRecorderOptions{
		ClusterID: recorderCluster, Events: router, FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewScopeEpochRecorder: %v", err)
	}

	ctx, cancel := context.WithCancel(logf.IntoContext(context.Background(), logs.logger()))
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	r.ScopeStarted(transition("informer-v1", "rule-1"))
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}

	if !logs.contains("could not be recorded before shutdown") {
		t.Error("transitions that never reached a sink were not reported at shutdown")
	}
}

// staysAt asserts a counter does not move past want within a short window — the
// shape every "it must not record that" assertion in this file needs.
func staysAt(t *testing.T, count func() int, want int, describe string) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := count(); got > want {
			t.Fatalf("%s (got %d, want %d)", describe, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}
