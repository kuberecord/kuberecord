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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/plan"
	"github.com/yelzhy/kuberecord/internal/sink"
)

const (
	// testSinkName is the one sink every record in this file is destined for. The
	// per-sink dedup separation is internal/pipeline's own subject; here one sink
	// keeps the metric lookups unambiguous.
	testSinkName = "sink-a"

	// eventTypeDeleted is the row type these tests assert on most, in both
	// directions: it must appear for an object that really was deleted, and must
	// never appear for one whose scope merely stopped being watched.
	eventTypeDeleted = "Deleted"
)

// This file is the whole data plane running against a real API server: registry →
// WatchManager → informers → workqueue → pipeline → sink. Everything else in the
// package tests one seam at a time; this tests that the seams line up.

// deferredLister binds the pipeline to the WatchManager after both exist.
//
// The two halves of the data plane point at each other — the pipeline reads state
// through the WatchManager, the WatchManager hands it work — so one direction has
// to be resolved after construction. The pipeline's side is the one that is only
// ever called later (at Process time), which makes it the safe one to defer.
type deferredLister struct{ manager *WatchManager }

func (d *deferredLister) Get(ref pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	return d.manager.Get(ref)
}

// The scope-level half of the same binding, for the warm/GC coordinator. In
// production one WatchManager answers both interfaces, so one deferred wrapper
// stands in for both here too.
func (d *deferredLister) ScopeSynced(sinkName string, scope pipeline.ScopeKey) bool {
	return d.manager.ScopeSynced(sinkName, scope)
}

func (d *deferredLister) ScopeDesired(sinkName string, scope pipeline.ScopeKey) bool {
	return d.manager.ScopeDesired(sinkName, scope)
}

// Settled is read only when the coordinator's own Start runs, by which time the
// binding below has happened — which is exactly why the settle gate is reached
// through this interface rather than handed over at construction time.
func (d *deferredLister) Settled() <-chan struct{} { return d.manager.Settled() }

// recordingWriter is a sink.Writer that confirms every job immediately and keeps
// what it was handed, so a test can assert on the records the pipeline produced
// without a database anywhere near it.
type recordingWriter struct {
	mu      sync.Mutex
	records []sink.Record
}

// Start satisfies sink.Writer. There are no workers to run: Enqueue settles
// synchronously, which is the fastest possible sink and keeps this test's timing
// about the watch layer rather than about write latency.
func (w *recordingWriter) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (w *recordingWriter) Enqueue(_ context.Context, job sink.Job) error {
	w.mu.Lock()
	w.records = append(w.records, job.Record)
	w.mu.Unlock()
	job.Commit(true)
	return nil
}

// recordsFor returns the records written for one object name, in order.
func (w *recordingWriter) recordsFor(name string) []sink.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []sink.Record
	for _, record := range w.records {
		if record.Name == name {
			out = append(out, record)
		}
	}
	return out
}

// singleSinkRouter resolves every sink name to one writer.
type singleSinkRouter struct{ writer sink.Writer }

func (r singleSinkRouter) WriterFor(string) (sink.Writer, bool) { return r.writer, true }

// TestWatchManagerStreamsAndEvictsThroughTheRealPipeline is the envtest acceptance
// criterion, end to end.
//
// Activating a (Pods, ns-a) target must stream create/update/delete through to the
// sink; deactivating it must stop the informer goroutine (goleak-verified) and drop
// that scope's dedup entries while a sibling scope in ns-b keeps its own — the
// property that makes "we stopped watching ns-a" cost nothing to ns-b and produce no
// Deleted rows for ns-a's objects.
func TestWatchManagerStreamsAndEvictsThroughTheRealPipeline(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	dyn := newDynamicClient(t)
	namespaces := newNamespaces(t, dyn, "ns-a", "ns-b")
	nsA, nsB := namespaces[0], namespaces[1]

	registry := plan.New()
	lister := &deferredLister{}
	writer := &recordingWriter{}
	// An isolated registry: hashcache_entries is the only exported window onto the
	// pipeline's dedup state, and the process-wide instance would carry other
	// tests' values.
	metricsReg := prometheus.NewRegistry()
	metrics := pipeline.NewPipelineMetrics(metricsReg)

	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: "test-cluster",
		Workers:   2,
		Lister:    lister,
		Router:    singleSinkRouter{writer: writer},
		Metrics:   metrics,
		// A fast rate limiter so the retry that follows a lookup against a
		// not-yet-started informer does not dominate the test.
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[pipeline.Key](
			time.Millisecond, 50*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}

	watchMgr, err := New(Options{
		Registry:      registry,
		Resolver:      newStaticResolver([]schema.GroupVersionKind{podGVK}),
		Dynamic:       dyn,
		Pipeline:      pipe,
		ResyncPeriod:  10 * time.Second,
		DebounceDelay: 50 * time.Millisecond,
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
		if err := watchMgr.Start(ctx); err != nil {
			t.Errorf("watch.Start: %v", err)
		}
	})

	// Both scopes are marked warm so cache-misses are tagged Added rather than
	// Snapshot: warm-up itself is Task 1.6's, and this test is about what the watch
	// layer delivers.
	for _, namespace := range []string{nsA, nsB} {
		pipe.MarkScopeWarm(testSinkName, pipeline.ScopeKey{Kind: "Pod", Namespace: namespace})
	}

	// --- Activate both targets ---
	if err := registry.Upsert("rule-a", []plan.WatchTarget{podTarget(testSinkName, nsA, "")}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := registry.Upsert("rule-b", []plan.WatchTarget{podTarget(testSinkName, nsB, "")}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	waitFor(t, func() bool { return watchMgr.PoolSize() == 2 },
		func() string { return fmt.Sprintf("two informers, have %d", watchMgr.PoolSize()) })

	// --- Create, update, delete in ns-a; one object in ns-b for contrast ---
	createPod(t, dyn, newPod(nsA, "web", map[string]string{"app": "web"}))
	createPod(t, dyn, newPod(nsB, "survivor", nil))
	waitFor(t, func() bool { return len(writer.recordsFor("web")) >= 1 && len(writer.recordsFor("survivor")) >= 1 },
		func() string { return "the initial records for both namespaces" })

	relabelPod(t, dyn, nsA, "web", map[string]string{"app": "web", "tier": "frontend"})
	waitFor(t, func() bool { return len(writer.recordsFor("web")) >= 2 },
		func() string {
			return fmt.Sprintf("a Modified record, have %v", eventTypesOf(writer.recordsFor("web")))
		})

	// Both namespaces' baselines are cached now — that is what the eviction below
	// has to remove for one scope and leave alone for the other.
	waitFor(t, func() bool { return hashcacheEntries(t, metricsReg) == 2 },
		func() string {
			return fmt.Sprintf("two cache entries, have %v", hashcacheEntries(t, metricsReg))
		})

	deletePod(t, dyn, nsA, "web")
	// Wait for the Deleted record specifically rather than for a third record of any
	// kind: a real API server writes a deletionTimestamp before removing the object,
	// so the third record may well be that intermediate Modified.
	waitFor(t, func() bool {
		types := eventTypesOf(writer.recordsFor("web"))
		return len(types) > 0 && types[len(types)-1] == eventTypeDeleted
	}, func() string { return fmt.Sprintf("a Deleted record, have %v", eventTypesOf(writer.recordsFor("web"))) })

	// The sequence must open with Added and close with Deleted, with at least the
	// relabel's Modified in between. It is asserted as a shape rather than an exact
	// list because a real API server writes a deletionTimestamp before removing the
	// object, and that intermediate state is a genuine Modified — the object really
	// did change — not test noise to be filtered out.
	got := eventTypesOf(writer.recordsFor("web"))
	if len(got) < 3 || got[0] != "Added" || got[len(got)-1] != eventTypeDeleted {
		t.Errorf("records for the ns-a pod = %v, want Added … Deleted", got)
	}
	for _, eventType := range got[1 : len(got)-1] {
		if eventType != "Modified" {
			t.Errorf("records for the ns-a pod = %v, want only Modified between Added and Deleted", got)
			break
		}
	}

	// The delete settled ns-a's only entry out of the cache, so re-create an object
	// there: the eviction assertion needs a ns-a entry to actually remove.
	createPod(t, dyn, newPod(nsA, "second", nil))
	waitFor(t, func() bool { return hashcacheEntries(t, metricsReg) == 2 },
		func() string {
			return fmt.Sprintf("one entry per namespace, have %v", hashcacheEntries(t, metricsReg))
		})

	// --- Deactivate ns-a ---
	entryA, informerRunning := watchMgr.pool.entryFor(podsInNamespace(nsA))
	if !informerRunning {
		t.Fatal("the ns-a informer is not running")
	}
	registry.Remove("rule-a")

	waitFor(t, func() bool { return watchMgr.PoolSize() == 1 },
		func() string { return fmt.Sprintf("the ns-a informer to stop, pool size %d", watchMgr.PoolSize()) })
	select {
	case <-entryA.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the ns-a informer goroutine did not exit")
	}

	// ns-a's entry is gone; ns-b's survives.
	waitFor(t, func() bool { return hashcacheEntries(t, metricsReg) == 1 },
		func() string {
			return fmt.Sprintf("only the ns-b entry to remain, have %v", hashcacheEntries(t, metricsReg))
		})
	if got := len(writer.recordsFor("second")); got != 1 {
		t.Errorf("the evicted object produced %d records, want 1 (an Added and no Deleted)", got)
	}
	if _, _, active, err := watchMgr.Get(pipeline.Key{
		Sink: testSinkName, Kind: "Pod", Namespace: nsA, Name: "second",
	}); err != nil || active {
		t.Errorf("the stopped scope reports active=%t (err %v), want false", active, err)
	}
	// ns-b keeps streaming: stopping one rule may not disturb another.
	createPod(t, dyn, newPod(nsB, "still-here", nil))
	waitFor(t, func() bool { return len(writer.recordsFor("still-here")) == 1 },
		func() string { return "the surviving scope to keep streaming" })

	cancel()
	running.Wait()
	goleak.VerifyNone(t, leaked)
}

// TestInformerStoreHoldsTransformedObjects is the transform acceptance criterion
// against a real API server: an object read back out of the watch cache — through
// the very same Get the pipeline uses — must have lost managedFields and gained the
// actors annotation.
//
// It needs a real API server because managedFields is written by the API server's
// own apply machinery; a hand-built object could only prove the transform function
// works in isolation (TestTransformObject does that), not that it is actually wired
// into every informer the pool builds.
func TestInformerStoreHoldsTransformedObjects(t *testing.T) {
	h := newManagerHarness(t)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]

	h.upsert(t, "rule-1", podTarget("sink-a", namespace, ""))

	created := createPod(t, h.dyn, newPod(namespace, "web", nil))
	if _, found, _ := unstructured.NestedFieldNoCopy(created.Object, "metadata", "managedFields"); !found {
		t.Fatal("the API server did not set managedFields, so this test would prove nothing")
	}

	ref := pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: namespace, Name: "web"}
	var cached *unstructured.Unstructured
	waitFor(t, func() bool {
		obj, found, _, err := h.manager.Get(ref)
		if err != nil || !found {
			return false
		}
		cached = obj
		return true
	}, func() string { return "the pod to appear in the watch cache" })

	if _, found, _ := unstructured.NestedFieldNoCopy(cached.Object, "metadata", "managedFields"); found {
		t.Error("the cached object still carries managedFields")
	}

	actors := cached.GetAnnotations()[pipeline.ActorsAnnotation]
	if actors == "" {
		t.Fatal("the cached object carries no actors annotation")
	}
	// Deterministic ordering: the encoded value must be the sorted, de-duplicated
	// actor set, or an otherwise-unchanged object could hash differently between
	// events.
	names := strings.Split(actors, ",")
	if !slices.IsSorted(names) || len(slices.Compact(slices.Clone(names))) != len(names) {
		t.Errorf("actors annotation = %q, want sorted and de-duplicated", actors)
	}
}

// eventTypesOf projects records onto their event types, in order, which is how the
// create/update/delete assertion reads as a sequence rather than a set of counts.
func eventTypesOf(records []sink.Record) []string {
	types := make([]string, 0, len(records))
	for _, record := range records {
		types = append(types, record.EventType)
	}
	return types
}

// hashcacheEntries reads the pipeline's per-sink dedup entry count off the registry
// its metrics were built on.
//
// The gauge is the only exported view of hashCache's size (the cache itself is
// private to internal/pipeline), which makes it the right instrument here: this test
// asserts about eviction as an observable outcome, not about the cache's internals.
// A missing series reads as 0, which is the truthful answer before the first entry
// is ever created.
func hashcacheEntries(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering pipeline metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "kuberecord_hashcache_entries" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "sink" && label.GetValue() == testSinkName {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}
