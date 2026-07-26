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
	"fmt"
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/plan"
)

// bogusGVK is a kind no cluster in these tests serves, so a target naming it is
// skipped by the pool diff. It is how a test exercises the reconcile loop without
// starting any informer at all.
var bogusGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Nonexistent"}

// managerHarness is a running WatchManager plus the doubles it talks to.
type managerHarness struct {
	registry *plan.Registry
	manager  *WatchManager
	pipe     *fakePipeline
	recorder *fakeRecorder
	dyn      dynamic.Interface
	// logs captures everything the manager logs. It is supplied through the Start
	// context, which is also where the informer pool picks its logger up, so a test
	// can assert on reported anomalies without racing the loop over a log field.
	logs *logCapture
}

// newManagerHarness builds a WatchManager over the envtest API server and starts
// it, returning once its first pool-diff pass has completed.
//
// The debounce delay and resync period are shortened so that a test asserting
// either does not wait on production pacing, but they stay far enough apart that
// the safety tick cannot be mistaken for a debounced wake-up.
func newManagerHarness(t *testing.T, mutate ...func(*Options)) *managerHarness {
	t.Helper()

	h := &managerHarness{
		registry: plan.New(),
		pipe:     &fakePipeline{},
		recorder: &fakeRecorder{},
		dyn:      newDynamicClient(t),
		logs:     &logCapture{},
	}

	opts := Options{
		Registry:      h.registry,
		Resolver:      newStaticResolver([]schema.GroupVersionKind{podGVK, configMapGVK}, namespaceGVK),
		Dynamic:       h.dyn,
		Pipeline:      h.pipe,
		Recorder:      h.recorder,
		ResyncPeriod:  10 * time.Second,
		DebounceDelay: 100 * time.Millisecond,
		StopTimeout:   10 * time.Second,
	}
	for _, m := range mutate {
		m(&opts)
	}

	m, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.manager = m

	ctx := logf.IntoContext(t.Context(), h.logs.logger())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := m.Start(ctx); err != nil {
			t.Errorf("Start returned an error: %v", err)
		}
	}()
	t.Cleanup(func() {
		// t.Context is cancelled before cleanups run, so this only has to wait for
		// the loop to finish stopping its informers.
		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Error("the watch manager did not stop")
		}
	})

	h.waitForPasses(t, 1)
	return h
}

// waitForPasses blocks until at least n pool-diff passes have completed.
func (h *managerHarness) waitForPasses(t *testing.T, n int64) {
	t.Helper()
	waitFor(t, func() bool { return h.manager.poolDiffs.Load() >= n },
		func() string { return fmt.Sprintf("%d pool diffs, have %d", n, h.manager.poolDiffs.Load()) })
}

// upsert writes a rule's targets and waits for the resulting pool diff to land.
func (h *managerHarness) upsert(t *testing.T, ruleKey string, targets ...plan.WatchTarget) {
	t.Helper()
	before := h.manager.poolDiffs.Load()
	if err := h.registry.Upsert(ruleKey, targets); err != nil {
		t.Fatalf("Upsert(%q): %v", ruleKey, err)
	}
	h.waitForPasses(t, before+1)
}

// remove withdraws a rule and waits for the resulting pool diff to land.
func (h *managerHarness) remove(t *testing.T, ruleKey string) {
	t.Helper()
	before := h.manager.poolDiffs.Load()
	h.registry.Remove(ruleKey)
	h.waitForPasses(t, before+1)
}

// podTarget builds a watch target for Pods.
func podTarget(sink, namespace, selector string) plan.WatchTarget {
	return plan.WatchTarget{Sink: sink, GVK: podGVK, Namespace: namespace, Selector: selector}
}

// TestNewValidatesRequiredOptions covers the eager dependency checks: each missing
// dependency has to fail at construction, where the wiring mistake is, rather than
// as a nil dereference inside a goroutine later.
func TestNewValidatesRequiredOptions(t *testing.T) {
	registry := plan.New()
	resolver := newStaticResolver([]schema.GroupVersionKind{podGVK})
	dyn := newDynamicClient(t)
	pipe := &fakePipeline{}

	cases := []struct {
		name string
		opts Options
	}{
		{name: "no registry", opts: Options{Resolver: resolver, Dynamic: dyn, Pipeline: pipe}},
		{name: "no resolver", opts: Options{Registry: registry, Dynamic: dyn, Pipeline: pipe}},
		{name: "no dynamic client", opts: Options{Registry: registry, Resolver: resolver, Pipeline: pipe}},
		{name: "no pipeline", opts: Options{Registry: registry, Resolver: resolver, Dynamic: dyn}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.opts); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}

	m, err := New(Options{Registry: registry, Resolver: resolver, Dynamic: dyn, Pipeline: pipe})
	if err != nil {
		t.Fatalf("New with every dependency: %v", err)
	}
	// A nil recorder is legal (the pre-Task-1.6 state) and must not be left nil,
	// or every scope transition would panic on the reconcile loop.
	if m.recorder == nil {
		t.Error("a nil Recorder was not defaulted to a no-op")
	}
	if m.resyncPeriod != defaultResyncPeriod || m.debounceDelay != defaultDebounceDelay {
		t.Errorf("pacing = %s/%s, want the documented defaults", m.resyncPeriod, m.debounceDelay)
	}
}

// TestWatchManagerNeedsLeaderElection is the LeaderElectionRunnable acceptance
// criterion. The compile-time assertions in manager.go prove the interfaces are
// satisfied; this proves the answer is the one that keeps two operator replicas
// from double-writing every row.
func TestWatchManagerNeedsLeaderElection(t *testing.T) {
	m, err := New(Options{
		Registry: plan.New(),
		Resolver: newStaticResolver([]schema.GroupVersionKind{podGVK}),
		Dynamic:  newDynamicClient(t),
		Pipeline: &fakePipeline{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var runnable manager.LeaderElectionRunnable = m
	if !runnable.NeedLeaderElection() {
		t.Error("NeedLeaderElection = false, want true: the data plane must only run on the elected leader")
	}
}

// TestWatchManagerDebouncesRegistryChanges is the debounce acceptance criterion: 20
// registry notifications inside 100ms must collapse into exactly one pool-diff pass.
//
// The targets name an unresolvable kind on purpose, so the passes are pure
// bookkeeping and the assertion measures debouncing rather than informer startup.
func TestWatchManagerDebouncesRegistryChanges(t *testing.T) {
	h := newManagerHarness(t, func(o *Options) {
		// Long enough that 20 upserts land well inside one window, and far below
		// the resync period so a safety tick cannot be counted as a debounced pass.
		o.DebounceDelay = 500 * time.Millisecond
		o.ResyncPeriod = time.Hour
	})

	passesBefore := h.manager.poolDiffs.Load()
	for i := range 20 {
		target := plan.WatchTarget{Sink: "sink-a", GVK: bogusGVK, Namespace: fmt.Sprintf("ns-%d", i)}
		if err := h.registry.Upsert(fmt.Sprintf("rule-%d", i), []plan.WatchTarget{target}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}

	h.waitForPasses(t, passesBefore+1)
	// Give the loop several debounce windows to make a second pass if it were going
	// to, then assert it did not.
	time.Sleep(2 * time.Second)
	if got := h.manager.poolDiffs.Load() - passesBefore; got != 1 {
		t.Errorf("20 notifications caused %d pool diffs, want exactly 1", got)
	}
	if h.manager.PoolSize() != 0 {
		t.Errorf("pool size = %d, want 0: none of those kinds resolve", h.manager.PoolSize())
	}
}

// TestWatchManagerSharesOneInformerPerTarget is the informer-sharing acceptance
// criterion: rules that want the same (resource, namespace) share one informer
// whether they name the same sink or different ones, and each event fans out to one
// key per interested sink.
func TestWatchManagerSharesOneInformerPerTarget(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]

	// Two rules, same sink, same scope: one informer, one key per event.
	h.upsert(t, "rule-1", podTarget("sink-a", namespace, ""))
	h.upsert(t, "rule-2", podTarget("sink-a", namespace, ""))
	if got := h.manager.PoolSize(); got != 1 {
		t.Fatalf("pool size with two rules on one scope = %d, want 1", got)
	}

	createPod(t, h.dyn, newPod(namespace, "shared", nil))
	keys := h.pipe.waitForKeys(t, 1)
	if got := sinksOf(keys); !slices.Equal(got, []string{"sink-a"}) {
		t.Errorf("one event enqueued keys for %v, want one key for sink-a", got)
	}

	// A third rule on a *different* sink: still one informer, now two keys per
	// event, because the sink is part of the work key but not of informer identity.
	h.pipe.reset()
	h.upsert(t, "rule-3", podTarget("sink-b", namespace, ""))
	if got := h.manager.PoolSize(); got != 1 {
		t.Fatalf("pool size with two sinks on one scope = %d, want 1", got)
	}
	// Registering the new interest replays nothing by itself, so drive one event.
	createPod(t, h.dyn, newPod(namespace, "fanned-out", nil))
	waitFor(t, func() bool {
		return len(keysNamed(h.pipe.enqueued(), "fanned-out")) >= 2
	}, func() string {
		return fmt.Sprintf("two keys for the new pod, have %v", h.pipe.enqueued())
	})
	if got := sinksOf(keysNamed(h.pipe.enqueued(), "fanned-out")); !slices.Equal(got, []string{"sink-a", "sink-b"}) {
		t.Errorf("one event enqueued keys for %v, want one per sink", got)
	}

	// A different resource in the same namespace is a different informer.
	h.upsert(t, "rule-4", plan.WatchTarget{Sink: "sink-a", GVK: configMapGVK, Namespace: namespace})
	if got := h.manager.PoolSize(); got != 2 {
		t.Errorf("pool size with a second resource = %d, want 2", got)
	}
}

// TestWatchManagerSelectorChangeKeepsTheInformer is the selector-stability
// acceptance criterion: editing only a rule's selector must not disturb the
// informer serving it, yet must change what gets enqueued from the next event on.
func TestWatchManagerSelectorChangeKeepsTheInformer(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	informer := podsInNamespace(namespace)

	h.upsert(t, "rule-1", podTarget("sink-a", namespace, "app=web"))
	before, running := h.manager.pool.entryFor(informer)
	if !running {
		t.Fatal("the informer did not start")
	}

	createPod(t, h.dyn, newPod(namespace, "web", map[string]string{"app": "web"}))
	createPod(t, h.dyn, newPod(namespace, "db", map[string]string{"app": "db"}))
	h.pipe.waitForKeys(t, 1)
	if got := keysNamed(h.pipe.enqueued(), "db"); len(got) != 0 {
		t.Errorf("a non-matching object was enqueued: %+v", got)
	}

	// Same target, new selector.
	h.pipe.reset()
	h.upsert(t, "rule-1", podTarget("sink-a", namespace, "app=db"))

	after, running := h.manager.pool.entryFor(informer)
	if !running {
		t.Fatal("the informer stopped when only the selector changed")
	}
	if before != after {
		t.Error("the informer was replaced when only the selector changed")
	}
	if got := h.manager.PoolSize(); got != 1 {
		t.Errorf("pool size after a selector edit = %d, want 1", got)
	}
	if got := h.pipe.evictions(); len(got) != 0 {
		t.Errorf("a selector edit evicted scopes: %+v", got)
	}
	if stops := transitionsWithAction(h.recorder.recorded(), "Stopped"); len(stops) != 0 {
		t.Errorf("a selector edit reported scope stops: %+v", stops)
	}

	// The new selector takes effect on the next event.
	createPod(t, h.dyn, newPod(namespace, "db-two", map[string]string{"app": "db"}))
	waitFor(t, func() bool { return len(keysNamed(h.pipe.enqueued(), "db-two")) == 1 },
		func() string {
			return fmt.Sprintf("the newly matching object to be enqueued, have %v", h.pipe.enqueued())
		})
	if got := keysNamed(h.pipe.enqueued(), "web"); len(got) != 0 {
		t.Errorf("the no-longer-matching object was enqueued after the edit: %+v", got)
	}
}

// TestWatchManagerStopsStaleTargets covers the stop path: deleting a rule stops its
// informer, deregisters the scope so the pipeline drops in-flight work for it,
// evicts that scope's dedup state, and reports the transition — all without
// touching a sibling scope in another namespace.
func TestWatchManagerStopsStaleTargets(t *testing.T) {
	h := newManagerHarness(t)
	namespaces := newNamespaces(t, h.dyn, "ns-a", "ns-b")
	nsA, nsB := namespaces[0], namespaces[1]

	h.upsert(t, "rule-a", podTarget("sink-a", nsA, ""))
	h.upsert(t, "rule-b", podTarget("sink-a", nsB, ""))
	if got := h.manager.PoolSize(); got != 2 {
		t.Fatalf("pool size = %d, want 2", got)
	}

	createPod(t, h.dyn, newPod(nsA, "web", nil))
	createPod(t, h.dyn, newPod(nsB, "web", nil))
	h.pipe.waitForKeys(t, 2)

	refA := pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: nsA, Name: "web"}
	refB := pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: nsB, Name: "web"}
	waitFor(t, func() bool {
		_, found, _, err := h.manager.Get(refA)
		return found && err == nil
	}, func() string { return "the ns-a pod to appear in the watch cache" })

	h.remove(t, "rule-a")

	if got := h.manager.PoolSize(); got != 1 {
		t.Errorf("pool size after removing one rule = %d, want 1", got)
	}
	if _, running := h.manager.pool.entryFor(podsInNamespace(nsA)); running {
		t.Error("the stopped scope's informer is still running")
	}

	// The stopped scope reports inactive — which is what makes the pipeline drop a
	// queued item instead of recording the object as deleted.
	obj, found, active, err := h.manager.Get(refA)
	if err != nil {
		t.Fatalf("Get for a stopped scope returned an error: %v", err)
	}
	if active || found || obj != nil {
		t.Errorf("Get for a stopped scope = (found %t, active %t), want (false, false)", found, active)
	}

	// The surviving scope is untouched.
	if _, found, active, err := h.manager.Get(refB); err != nil || !found || !active {
		t.Errorf("Get for the surviving scope = (found %t, active %t, err %v), want (true, true, nil)", found, active, err)
	}

	wantScope := pipeline.ScopeKey{Kind: "Pod", Namespace: nsA}
	if got := h.pipe.evictions(); len(got) != 1 || got[0].sink != "sink-a" || got[0].scope != wantScope {
		t.Errorf("evictions = %+v, want exactly one for sink-a %+v", got, wantScope)
	}

	stops := transitionsWithAction(h.recorder.recorded(), "Stopped")
	if len(stops) != 1 {
		t.Fatalf("recorded %d scope stops, want 1: %+v", len(stops), stops)
	}
	if stops[0].scope != wantScope || !slices.Equal(stops[0].ruleKeys, []string{"rule-a"}) {
		t.Errorf("recorded stop = %+v, want scope %+v attributed to rule-a", stops[0], wantScope)
	}
}

// TestWatchManagerReportsScopeTransitions pins the transition edges the recorder
// sees: one Started when a scope appears, nothing further while it merely gains
// rules, and one Stopped when it goes away.
func TestWatchManagerReportsScopeTransitions(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	scope := pipeline.ScopeKey{Kind: "Pod", Namespace: namespace}

	h.upsert(t, "rule-1", podTarget("sink-a", namespace, ""))
	starts := transitionsWithAction(h.recorder.recorded(), "Started")
	if len(starts) != 1 || starts[0].scope != scope || starts[0].sink != "sink-a" {
		t.Fatalf("recorded starts = %+v, want exactly one for sink-a %+v", starts, scope)
	}

	// A second rule on the same scope is not a new transition: the informer, the
	// interest and the scope are all already there.
	h.upsert(t, "rule-2", podTarget("sink-a", namespace, "app=web"))
	if got := transitionsWithAction(h.recorder.recorded(), "Started"); len(got) != 1 {
		t.Errorf("a second rule on one scope recorded %d starts, want 1", len(got))
	}

	// Nor is a later pass that changes nothing about this scope: transitions are
	// edges, not a per-pass restatement of the current state. The pass is driven
	// through the loop (by a rule for an unresolvable kind) rather than by calling
	// reconcilePool directly, which would race the loop's own state.
	h.upsert(t, "rule-unrelated", plan.WatchTarget{Sink: "sink-a", GVK: bogusGVK, Namespace: namespace})
	if got := transitionsWithAction(h.recorder.recorded(), "Started"); len(got) != 1 {
		t.Errorf("a pass that changed nothing recorded %d starts, want 1", len(got))
	}

	// The scope survives losing one of its two rules.
	h.remove(t, "rule-1")
	if got := transitionsWithAction(h.recorder.recorded(), "Stopped"); len(got) != 0 {
		t.Errorf("losing one of two rules recorded %d stops, want 0", len(got))
	}
	h.remove(t, "rule-2")
	if got := transitionsWithAction(h.recorder.recorded(), "Stopped"); len(got) != 1 {
		t.Errorf("losing the last rule recorded %d stops, want 1", len(got))
	}
}

// TestWatchManagerGetReportsAnUnstartedInformer covers the one lookup that must not
// answer "not found": a scope that is registered but whose informer has not come up
// yet. Answering not-found there would hand the pipeline a phantom deletion, so it
// is a retryable error instead.
func TestWatchManagerGetReportsAnUnstartedInformer(t *testing.T) {
	m, err := New(Options{
		Registry: plan.New(),
		Resolver: newStaticResolver([]schema.GroupVersionKind{podGVK}),
		Dynamic:  newDynamicClient(t),
		Pipeline: &fakePipeline{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Register an interest without starting anything, which is exactly the window
	// between a pool diff installing interests and the pool starting informers.
	in := interestFor(t, "sink-a", "ns-a", nil, []string{"rule-1"})
	m.table.replace(map[interestID]*scopeInterest{in.id(): in})

	obj, found, active, err := m.Get(pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: "ns-a", Name: "web"})
	if err == nil {
		t.Fatal("Get against an unstarted informer returned no error")
	}
	if found || obj != nil {
		t.Error("Get against an unstarted informer reported the object as found")
	}
	if !active {
		t.Error("Get against an unstarted informer reported the scope as inactive")
	}
}

// TestWatchManagerSkipsUntranslatableTargets covers graceful degradation in the
// pool diff: a target naming a kind this cluster does not serve, and a namespaced
// target for a cluster-scoped resource, are each skipped with a log — while a
// perfectly good target in the same snapshot keeps streaming (Invariant 5).
func TestWatchManagerSkipsUntranslatableTargets(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]

	h.upsert(t, "rule-bogus", plan.WatchTarget{Sink: "sink-a", GVK: bogusGVK, Namespace: namespace})
	h.upsert(t, "rule-misscoped", plan.WatchTarget{Sink: "sink-a", GVK: namespaceGVK, Namespace: namespace})
	h.upsert(t, "rule-good", podTarget("sink-a", namespace, ""))

	if got := h.manager.PoolSize(); got != 1 {
		t.Fatalf("pool size = %d, want 1: only the Pod target is translatable", got)
	}

	// Both skips are reported (Invariant 4), at the severity each deserves: a kind
	// that is not installed is an ordinary, self-healing state, while a namespaced
	// target for a cluster-scoped resource means the two tiers disagree.
	if !h.logs.contains("is not installed in this cluster") {
		t.Errorf("the unresolvable kind was not reported; captured: %v", h.logs.captured())
	}
	if !h.logs.contains("Refusing a namespaced watch target for a cluster-scoped resource") {
		t.Errorf("the misscoped target was not reported; captured: %v", h.logs.captured())
	}

	// The good target still works, which is the whole point of skipping rather than
	// failing the pass.
	createPod(t, h.dyn, newPod(namespace, "web", nil))
	h.pipe.waitForKeys(t, 1)
}

// TestWatchManagerStopsEveryInformerOnShutdown is the goleak shutdown guard: once
// Start returns, no informer goroutine is left behind.
func TestWatchManagerStopsEveryInformerOnShutdown(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	registry := plan.New()
	pipe := &fakePipeline{}
	dyn := newDynamicClient(t)
	namespaces := newNamespaces(t, dyn, "ns-a", "ns-b")

	m, err := New(Options{
		Registry:      registry,
		Resolver:      newStaticResolver([]schema.GroupVersionKind{podGVK}),
		Dynamic:       dyn,
		Pipeline:      pipe,
		ResyncPeriod:  10 * time.Second,
		DebounceDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := registry.Upsert("rule-1", []plan.WatchTarget{
		podTarget("sink-a", namespaces[0], ""),
		podTarget("sink-a", namespaces[1], ""),
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := m.Start(ctx); err != nil {
			t.Errorf("Start returned an error: %v", err)
		}
	}()

	waitFor(t, func() bool { return m.PoolSize() == 2 },
		func() string { return fmt.Sprintf("two informers, have %d", m.PoolSize()) })

	cancel()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}

	if got := m.PoolSize(); got != 0 {
		t.Errorf("pool size after shutdown = %d, want 0", got)
	}
	// Shutdown is not a scope stop: a restart must not look like every rule was
	// deleted and recreated.
	if got := pipe.evictions(); len(got) != 0 {
		t.Errorf("shutdown evicted scopes: %+v", got)
	}
	goleak.VerifyNone(t, leaked)
}

// keysNamed filters work keys by object name, so a test can assert about one
// object's fan-out without depending on what else happened to be enqueued.
func keysNamed(keys []pipeline.Key, name string) []pipeline.Key {
	var out []pipeline.Key
	for _, key := range keys {
		if key.Name == name {
			out = append(out, key)
		}
	}
	return out
}

// transitionsWithAction filters recorded scope transitions by action.
func transitionsWithAction(transitions []scopeTransition, action string) []scopeTransition {
	var out []scopeTransition
	for _, transition := range transitions {
		if transition.action == action {
			out = append(out, transition)
		}
	}
	return out
}
