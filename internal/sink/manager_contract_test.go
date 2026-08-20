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

// This file is an *external* test package on purpose. The assertions below are
// exactly the contracts internal/pipeline declares and the SinkManager
// implements, and an in-package assertion for them would be an import cycle:
// internal/pipeline imports internal/sink (it hands Records to a Writer), so
// internal/sink can never import internal/pipeline. A test binary can, because
// package sink_test is compiled as its own package.
//
// That is also what lets the two-sink test below drive a *real* Pipeline against
// a real SinkManager, which is the only way to assert the property Task 1.8
// promises: two sinks receiving the same object keep independent dedup state.
package sink_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// The routing contracts the data plane depends on, asserted at compile time so a
// signature drift is a build failure here rather than a wiring failure in Task
// 1.10's cmd/main.go — a file that has nothing to do with either side of the
// contract.
var (
	_ pipeline.SinkRouter        = (*sink.SinkManager)(nil)
	_ pipeline.StateReaderRouter = (*sink.SinkManager)(nil)
	_ pipeline.ScopeEventRouter  = (*sink.SinkManager)(nil)

	// And the two contracts that run the other way: the pipeline is what the
	// manager evicts per-sink state through when a sink is deleted, and the
	// warm/GC coordinator is what it clears the sink's boot-reconciliation mark
	// and in-flight warms through, immediately afterwards.
	_ sink.Pipeline  = (*pipeline.Pipeline)(nil)
	_ sink.WarmHooks = (*pipeline.WarmCoordinator)(nil)
)

// recordingWriter is a sink.Writer that settles every job immediately and keeps
// the records it was given, so a test can ask "what did *this* sink receive?".
//
// It settles inside Enqueue rather than on a drain because these tests are about
// what reaches each sink, not about shutdown ordering (which the in-package tests
// cover): committing at once keeps the pipeline's version-gated cache state
// settled between assertions.
type recordingWriter struct {
	mu      sync.Mutex
	records []sink.Record
}

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

// received returns the records this sink was handed, in order.
func (w *recordingWriter) received() []sink.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sink.Record(nil), w.records...)
}

// staticLister is a pipeline.ListerRegistry holding one object per identity, with
// every scope active — the "nothing has stopped, the cache is populated" baseline
// these tests need.
type staticLister struct {
	mu      sync.Mutex
	objects map[string]*unstructured.Unstructured
}

func newStaticLister() *staticLister {
	return &staticLister{objects: make(map[string]*unstructured.Unstructured)}
}

func (l *staticLister) set(key pipeline.Key, obj *unstructured.Unstructured) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.objects[key.Kind+"/"+key.Namespace+"/"+key.Name] = obj
}

func (l *staticLister) Get(ref pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	obj, found := l.objects[ref.Kind+"/"+ref.Namespace+"/"+ref.Name]
	return obj, found, true, nil
}

// nopPipeline satisfies sink.Pipeline for the manager built in a test that has no
// pipeline of its own to evict from.
type nopPipeline struct{}

func (nopPipeline) RemoveSink(sink.ID) {}

// newPod builds one Pod as the watch cache would hold it.
func newPod(name, uid, image string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             uid,
			"resourceVersion": "1",
		},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}},
	}}
}

// TestTwoSinksReceiveIndependentStreams covers AC (c): when two rules stream the
// same object to two different sinks, each sink receives its own record and each
// keeps its own dedup state.
//
// The pair of assertions is what makes it a real test of independence rather than
// of mere delivery: the second sink's first record must *not* be suppressed by the
// first sink's earlier write (a shared hashCache would suppress it — the object's
// content hash is identical), while a repeat for the first sink must still
// deduplicate (proving each sink genuinely has a cache, rather than none at all).
func TestTwoSinksReceiveIndependentStreams(t *testing.T) {
	// Keyed by identity, as the runtime is: a rule streams to a (kind, name) pair,
	// never to a bare name (Task 4.1).
	primaryID := sink.ID{Kind: sink.DefaultSinkKind, Name: "primary"}
	auditID := sink.ID{Kind: sink.DefaultSinkKind, Name: "audit"}
	writers := map[sink.ID]*recordingWriter{primaryID: {}, auditID: {}}

	mgr, err := sink.NewSinkManager(sink.ManagerOptions{
		Pipeline: nopPipeline{},
		// A long probe interval: these writers have no Prober, but a short interval
		// would be misleading to a later reader of this test.
		ProbeInterval: time.Hour,
		Factory: func(id sink.ID, _ sink.InstanceConfig) (sink.Writer, error) {
			w, ok := writers[id]
			if !ok {
				return nil, fmt.Errorf("unexpected sink %s", id)
			}
			return w, nil
		},
	})
	if err != nil {
		t.Fatalf("NewSinkManager: %v", err)
	}

	mgrCtx, stopMgr := context.WithCancel(context.Background())
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(mgrCtx) }()
	t.Cleanup(func() {
		stopMgr()
		if err := <-mgrDone; err != nil {
			t.Errorf("SinkManager.Start returned %v, want nil", err)
		}
	})

	// Both sinks live, routed by the manager the pipeline resolves through.
	for id := range writers {
		if err := mgr.Ensure(id, staticConfig(id.Name)); err != nil {
			t.Fatalf("Ensure(%s): %v", id, err)
		}
	}
	waitForCond(t, "both sinks to be routed", func() bool {
		_, primary := mgr.WriterFor(primaryID)
		_, audit := mgr.WriterFor(auditID)
		return primary && audit
	})

	lister := newStaticLister()
	// A work key carries the sink's whole identity, which is the same value the
	// manager's routing table is keyed by — so these resolve to exactly the two
	// instances declared above, with no lift or lookup by name in between.
	primaryKey := pipeline.Key{Sink: primaryID, Kind: "Pod", Namespace: "default", Name: "web"}
	auditKey := pipeline.Key{Sink: auditID, Kind: "Pod", Namespace: "default", Name: "web"}
	lister.set(primaryKey, newPod("web", "uid-1", "nginx:1"))

	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: "test-cluster",
		Workers:   2,
		Lister:    lister,
		Router:    mgr,
		Metrics:   pipeline.NewPipelineMetrics(prometheus.NewRegistry()),
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[pipeline.Key](
			time.Millisecond, 20*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}

	pipeCtx, stopPipe := context.WithCancel(context.Background())
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- pipe.Start(pipeCtx) }()
	t.Cleanup(func() {
		stopPipe()
		if err := <-pipeDone; err != nil {
			t.Errorf("Pipeline.Start returned %v, want nil", err)
		}
	})

	// One observation for the first sink.
	pipe.Add(primaryKey)
	waitForCond(t, "the primary sink to receive the object", func() bool {
		return len(writers[primaryID].received()) == 1
	})

	// A repeat for the same sink deduplicates: the content hash is unchanged.
	pipe.Add(primaryKey)
	time.Sleep(100 * time.Millisecond)
	if n := len(writers[primaryID].received()); n != 1 {
		t.Errorf("the primary sink received %d records for an unchanged object, want 1 (dedup)", n)
	}

	// The same object for the second sink is a *new* stream, and must not be
	// suppressed by the first sink's identical write.
	pipe.Add(auditKey)
	waitForCond(t, "the audit sink to receive the object independently", func() bool {
		return len(writers[auditID].received()) == 1
	})

	for id, w := range writers {
		records := w.received()
		if len(records) != 1 {
			t.Fatalf("sink %s received %d records, want 1", id, len(records))
		}
		if records[0].Name != "web" || records[0].UID != "uid-1" {
			t.Errorf("sink %s received %+v, want the web/uid-1 object", id, records[0])
		}
	}

	// And the second sink deduplicates on its own cache too — independence means
	// two caches, not zero.
	pipe.Add(auditKey)
	time.Sleep(100 * time.Millisecond)
	if n := len(writers[auditID].received()); n != 1 {
		t.Errorf("the audit sink received %d records for an unchanged object, want 1 (dedup)", n)
	}
}

// staticConfig is an InstanceConfig whose fingerprint never changes, so Ensure is
// a no-op on every repeat.
type staticConfig string

func (c staticConfig) Fingerprint() string { return string(c) }

// waitForCond polls cond until it holds or the deadline passes.
func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
