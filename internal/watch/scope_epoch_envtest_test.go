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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/plan"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// This file runs the whole scope-epoch stack against a real API server: registry →
// WatchManager → informers → recorder → warm/GC coordinator → pipeline → sink. The
// unit tests in internal/pipeline and scope_recorder_test.go each pin one seam with
// fakes; this proves the seams line up, and in particular that the GC pass reads a
// genuine informer indexer whose HasSynced the coordinator actually waited on.

// scopeHistory is a sink.StateReader over hand-written history, so a test can state
// exactly what a previous epoch of the operator had recorded.
type scopeHistory struct {
	mu sync.Mutex
	// states is read per *incarnation* (see sink.KnownState): several entries
	// sharing a Namespace/Name but differing in UID describe an identity whose
	// older incarnation died without a Deleted row ever being written for it.
	states    map[sink.ScopeFilter][]sink.KnownState
	wasActive map[sink.ScopeFilter]bool
	active    []sink.ScopeFilter
	reads     []sink.ScopeFilter
}

func newScopeHistory() *scopeHistory {
	return &scopeHistory{
		states:    make(map[sink.ScopeFilter][]sink.KnownState),
		wasActive: make(map[sink.ScopeFilter]bool),
	}
}

func (h *scopeHistory) LastKnownStates(_ context.Context, filter sink.ScopeFilter) ([]sink.KnownState, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reads = append(h.reads, filter)
	return slices.Clone(h.states[filter]), nil
}

func (h *scopeHistory) ScopeWasActive(_ context.Context, filter sink.ScopeFilter, _ time.Time) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wasActive[filter], nil
}

func (h *scopeHistory) ActiveScopes(_ context.Context, clusterID string) ([]sink.ScopeFilter, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []sink.ScopeFilter
	for _, scope := range h.active {
		if scope.ClusterID == clusterID {
			out = append(out, scope)
		}
	}
	return out, nil
}

func (h *scopeHistory) record(filter sink.ScopeFilter, wasActive bool, states ...sink.KnownState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.states[filter] = states
	h.wasActive[filter] = wasActive
}

func (h *scopeHistory) leaveOpen(scopes ...sink.ScopeFilter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = scopes
}

func (h *scopeHistory) historyReads() []sink.ScopeFilter {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.reads)
}

// singleSinkBackends resolves every sink name to one history reader and one
// scope-log writer — the shape Task 1.8's SinkManager will have for a
// single-ClickHouseSink deployment.
type singleSinkBackends struct {
	id     sink.ID
	reader sink.StateReader
	events sink.ScopeEventWriter
}

func (b singleSinkBackends) StateReaderFor(id sink.ID) (sink.StateReader, bool) {
	if id != b.id {
		return nil, false
	}
	return b.reader, true
}

func (b singleSinkBackends) SinkIDs() []sink.ID { return []sink.ID{b.id} }

func (b singleSinkBackends) ScopeEventWriterFor(id sink.ID) (sink.ScopeEventWriter, bool) {
	if id != b.id {
		return nil, false
	}
	return b.events, true
}

// scopeEventsFor returns the recorded events for one namespace's Pod scope, in
// order, so an assertion is not confused by another scope's epochs.
func scopeEventsFor(events []sink.ScopeEvent, namespace string) []sink.ScopeEvent {
	var out []sink.ScopeEvent
	for _, event := range events {
		if event.Scope.Kind == "Pod" && event.Scope.Namespace == namespace {
			out = append(out, event)
		}
	}
	return out
}

// TestScopeEpochsThroughTheRealDataPlane is the Task 1.6 integration criterion.
//
// One rule appears on a live cluster and one scope's epoch is opened for it: its
// history is warmed, its informer's initial List is awaited, and the one object
// history remembers but the cluster no longer has is closed out with exactly one
// Deleted row. A second scope, left open by an earlier process and wanted by nobody,
// is closed with exactly one Stopped row and no Deleted rows at all. Then the rule is
// deleted, and the scope that was streaming produces one Stopped row — and, for the
// live object it covered, not a single Deleted row.
//
// That last pair is the whole point of scope epochs: "we stopped watching" and "it
// was deleted" have to leave different traces.
func TestScopeEpochsThroughTheRealDataPlane(t *testing.T) {
	dyn := newDynamicClient(t)
	namespaces := newNamespaces(t, dyn, "ns-a", "ns-b")
	nsA, nsB := namespaces[0], namespaces[1]

	const clusterID = "test-cluster"
	scopeA := sink.ScopeFilter{ClusterID: clusterID, Kind: "Pod", Namespace: nsA}
	orphaned := sink.ScopeFilter{ClusterID: clusterID, Kind: "Pod", Namespace: nsB}

	// History: ns-a was watched before and remembers a Pod that no longer exists;
	// ns-b's epoch was left open by a process that never came back.
	history := newScopeHistory()
	history.record(scopeA, true, sink.KnownState{
		Namespace: nsA, Name: "ghost", UID: "uid-ghost", SHA256: "hash-ghost",
	})
	history.leaveOpen(orphaned)

	registry := plan.New()
	lister := &deferredLister{}
	writer := &recordingWriter{}
	events := &fakeScopeEventWriter{}
	backends := singleSinkBackends{id: sinkA, reader: history, events: events}

	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: clusterID,
		Workers:   2,
		Lister:    lister,
		Router:    singleSinkRouter{writer: writer},
		Metrics:   pipeline.NewPipelineMetrics(prometheus.NewRegistry()),
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[pipeline.Key](
			time.Millisecond, 50*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}

	coordinator, err := pipeline.NewWarmCoordinator(pipeline.WarmOptions{
		Pipeline:         pipe,
		Scopes:           lister,
		Readers:          backends,
		ScopeEvents:      backends,
		RetryMaxInterval: 20 * time.Millisecond,
		SyncPollInterval: 5 * time.Millisecond,
		BootInterval:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("pipeline.NewWarmCoordinator: %v", err)
	}

	recorder, err := NewScopeEpochRecorder(ScopeRecorderOptions{
		ClusterID: clusterID,
		Events: &fakeScopeEventRouter{
			writers: map[sink.ID]sink.ScopeEventWriter{sinkA: events},
		},
		Warmer:        coordinator,
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewScopeEpochRecorder: %v", err)
	}

	watchMgr, err := New(Options{
		Registry:      registry,
		Resolver:      newStaticResolver([]schema.GroupVersionKind{podGVK}),
		Dynamic:       dyn,
		Pipeline:      pipe,
		Recorder:      recorder,
		ResyncPeriod:  10 * time.Second,
		DebounceDelay: 50 * time.Millisecond,
		// Short enough that the boot pass runs inside the test, long enough that the
		// rule applied below is in the registry before the desired state settles.
		SettleQuietPeriod: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("watch.New: %v", err)
	}
	lister.manager = watchMgr

	ctx, cancel := context.WithCancel(t.Context())
	var running sync.WaitGroup
	running.Go(func() {
		if err := pipe.Start(ctx); err != nil {
			t.Errorf("pipeline.Start: %v", err)
		}
	})
	running.Go(func() {
		if err := recorder.Start(ctx); err != nil {
			t.Errorf("recorder.Start: %v", err)
		}
	})
	running.Go(func() {
		if err := watchMgr.Start(ctx); err != nil {
			t.Errorf("watch.Start: %v", err)
		}
	})
	running.Go(func() {
		if err := coordinator.Start(ctx); err != nil {
			t.Errorf("coordinator.Start: %v", err)
		}
	})

	// A live Pod in ns-a, created before the rule so the informer's initial List
	// already holds it: this is the object that must never be reported as deleted.
	createPod(t, dyn, newPod(nsA, "survivor", nil))

	// --- The rule appears ---
	if err := registry.Upsert("rule-a", []plan.WatchTarget{podTarget(sinkA, nsA, "")}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// One Started row for the scope, attributed to the rule that opened it.
	waitFor(t, func() bool { return len(scopeEventsFor(events.recorded(), nsA)) >= 1 },
		func() string { return "the Started row for the new scope" })
	started := scopeEventsFor(events.recorded(), nsA)[0]
	if started.Action != sink.ScopeActionStarted || started.RuleRef != "rule-a" {
		t.Errorf("first ns-a scope event = %+v, want Started attributed to rule-a", started)
	}
	if started.APIVersion != "v1" {
		t.Errorf("scope event api_version = %q, want v1 (provenance of the watched target)", started.APIVersion)
	}

	// The warm ran for this scope and reconciled its history against the real
	// informer: the remembered-but-absent object is closed out exactly once.
	waitFor(t, func() bool { return len(writer.recordsFor("ghost")) == 1 },
		func() string {
			return fmt.Sprintf("one Deleted row for the zombie, have %v", eventTypesOf(writer.recordsFor("ghost")))
		})
	if got := writer.recordsFor("ghost"); got[0].EventType != eventTypeDeleted || got[0].UID != "uid-ghost" {
		t.Errorf("zombie row = %+v, want a Deleted row for uid-ghost", got[0])
	}

	// The live object streamed normally and was never mistaken for a zombie.
	waitFor(t, func() bool { return len(writer.recordsFor("survivor")) >= 1 },
		func() string { return "the live object's own record" })
	for _, record := range writer.recordsFor("survivor") {
		if record.EventType == eventTypeDeleted {
			t.Fatalf("the live object was recorded as deleted: %+v", record)
		}
	}

	// Only this scope's history was read — a new rule warms its own scope and nothing
	// else, even though other scopes have history too.
	for _, filter := range history.historyReads() {
		if filter != scopeA {
			t.Errorf("warm-up read history for a scope no rule asked about: %+v", filter)
		}
	}

	// --- Boot reconciliation closes the scope nobody wants ---
	waitFor(t, func() bool { return len(scopeEventsFor(events.recorded(), nsB)) == 1 },
		func() string { return "the Stopped row for the orphaned scope" })
	orphanEvent := scopeEventsFor(events.recorded(), nsB)[0]
	if orphanEvent.Action != sink.ScopeActionStopped || orphanEvent.RuleRef != "" {
		t.Errorf("orphaned scope event = %+v, want an unattributed Stopped row", orphanEvent)
	}

	// --- The rule is deleted ---
	recordsBefore := len(writer.recordsFor("survivor"))
	registry.Remove("rule-a")

	waitFor(t, func() bool { return len(scopeEventsFor(events.recorded(), nsA)) == 2 },
		func() string {
			return fmt.Sprintf("the Stopped row for the deleted rule's scope, have %d ns-a events",
				len(scopeEventsFor(events.recorded(), nsA)))
		})
	stopped := scopeEventsFor(events.recorded(), nsA)[1]
	if stopped.Action != sink.ScopeActionStopped || stopped.RuleRef != "rule-a" {
		t.Errorf("second ns-a scope event = %+v, want Stopped attributed to rule-a", stopped)
	}
	if stopped.TS.Before(started.TS) {
		t.Errorf("the Stopped row (%s) predates the Started row (%s) it closes", stopped.TS, started.TS)
	}

	// The object that was in scope produced no further rows at all — least of all a
	// Deleted one. This is the acceptance criterion the whole task exists for.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		records := writer.recordsFor("survivor")
		if len(records) != recordsBefore {
			t.Fatalf("stopping a rule produced %d further rows for an object that still exists: %v",
				len(records)-recordsBefore, eventTypesOf(records))
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	running.Wait()
}

// TestWatchManagerScopeStateReflectsRealInformers pins the two scope-level answers
// the warm/GC coordinator depends on, against real informers: desire follows the
// interest table (true as soon as a rule names the scope), while sync follows the
// informer (false until its initial List completes, and false again once the scope is
// gone). Getting the second one wrong would let a GC pass run against an empty cache
// and delete an entire scope.
func TestWatchManagerScopeStateReflectsRealInformers(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	scope := pipeline.ScopeKey{Kind: "Pod", Namespace: namespace}

	if h.manager.ScopeDesired(sinkA, scope) {
		t.Error("a scope no rule names reports desired")
	}
	if h.manager.ScopeSynced(sinkA, scope) {
		t.Error("a scope with no informer reports synced")
	}

	h.upsert(t, "rule-a", podTarget(sinkA, namespace, ""))

	if !h.manager.ScopeDesired(sinkA, scope) {
		t.Error("a scope a rule names reports not desired")
	}
	waitFor(t, func() bool { return h.manager.ScopeSynced(sinkA, scope) },
		func() string { return "the scope's informer to report synced" })

	// A different sink's view of the same scope is independent: interests are
	// per-(sink, scope), so one sink's rule says nothing about another's.
	if h.manager.ScopeDesired(clickHouseSink("other-sink"), scope) || h.manager.ScopeSynced(clickHouseSink("other-sink"), scope) {
		t.Error("another sink inherited this scope's state")
	}
	// So is a different namespace's scope, including the cluster-wide one: an empty
	// namespace is its own scope with its own epoch, never a wildcard.
	clusterWide := pipeline.ScopeKey{Kind: "Pod"}
	if h.manager.ScopeDesired(sinkA, clusterWide) || h.manager.ScopeSynced(sinkA, clusterWide) {
		t.Error("a namespaced rule made the cluster-wide scope look desired or synced")
	}

	h.remove(t, "rule-a")
	if h.manager.ScopeDesired(sinkA, scope) {
		t.Error("a scope whose rule was removed still reports desired")
	}
	if h.manager.ScopeSynced(sinkA, scope) {
		t.Error("a scope whose interest is gone still reports synced")
	}
}

// TestWatchManagerSettledWaitsForAQuietRegistry covers the gate boot reconciliation
// hangs off. Closing it too early would make the boot pass judge orphans against a
// desired state the reconcilers had not finished writing — and close every scope in
// the cluster.
func TestWatchManagerSettledWaitsForAQuietRegistry(t *testing.T) {
	h := newManagerHarness(t, func(o *Options) {
		o.DebounceDelay = 10 * time.Millisecond
		o.SettleQuietPeriod = 400 * time.Millisecond
	})
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]

	// Churn the registry for longer than the quiet period: the gate must stay shut
	// throughout, because rules are still arriving.
	deadline := time.Now().Add(600 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		h.upsert(t, fmt.Sprintf("rule-%d", i), podTarget(sinkA, namespace, ""))
		select {
		case <-h.manager.Settled():
			t.Fatal("the desired state was declared settled while rules were still arriving")
		default:
		}
	}

	select {
	case <-h.manager.Settled():
	case <-time.After(10 * time.Second):
		t.Fatal("the desired state never settled after the registry went quiet")
	}
}
