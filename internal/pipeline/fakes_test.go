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
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The doubles in this file are what let every test in this package run with no
// apiserver, no informer, and no ClickHouse (a Task 1.5 acceptance criterion):
// fakeLister stands in for Task 1.4's WatchManager, fakeRouter for Task 1.8's
// SinkManager, and fakeWriter for the ClickHouse sink. Because the pipeline
// depends only on the ListerRegistry / SinkRouter / sink.Writer interfaces, these
// three are a complete substitute for the production data plane.

// fakeLister is a map-backed ListerRegistry keyed by canonical identity, so it
// mirrors the real thing in the way that matters: one entry per object identity,
// shared across sinks, with scope activity tracked per (sink, scope).
type fakeLister struct {
	mu      sync.RWMutex
	objects map[string]*unstructured.Unstructured
	// stopped holds the (sink, scope) pairs whose target has been deactivated.
	// Absence means active, which keeps the common case in tests silent.
	stopped map[stoppedScope]struct{}
	err     error
	// gets counts lookups, so a test can prove the pipeline consulted the cache
	// rather than some other source.
	gets int
}

type stoppedScope struct {
	sink  string
	scope ScopeKey
}

func newFakeLister() *fakeLister {
	return &fakeLister{
		objects: make(map[string]*unstructured.Unstructured),
		stopped: make(map[stoppedScope]struct{}),
	}
}

func (f *fakeLister) Get(ref Key) (*unstructured.Unstructured, bool, bool, error) {
	f.mu.Lock()
	f.gets++
	err := f.err
	obj, found := f.objects[ref.cacheKey()]
	_, inactive := f.stopped[stoppedScope{sink: ref.Sink, scope: ref.Scope()}]
	f.mu.Unlock()

	if err != nil {
		return nil, false, true, err
	}
	return obj, found, !inactive, nil
}

// set makes obj the current state for key's identity.
func (f *fakeLister) set(key Key, obj *unstructured.Unstructured) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key.cacheKey()] = obj
}

// remove simulates a deletion: the identity is no longer in the watch cache.
func (f *fakeLister) remove(key Key) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key.cacheKey())
}

// stopScope simulates a watch target being deactivated for one sink.
func (f *fakeLister) stopScope(sinkName string, scope ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped[stoppedScope{sink: sinkName, scope: scope}] = struct{}{}
}

func (f *fakeLister) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeLister) getCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.gets
}

// fakeRedactions is a map-backed RedactionRegistry keyed by watch scope, which
// is the granularity a real policy is installed at (one compiled policy per
// interest — see WatchManager.RedactionFor).
//
// An unconfigured scope answers "built-in scrubs only, and yes this scope is
// live", so every test that says nothing about redaction behaves exactly as the
// pipeline did before Task 3.3. Only drop() produces the not-ok answer, which is
// the "the scope stopped between the lister read and the policy lookup" race.
type fakeRedactions struct {
	mu       sync.RWMutex
	policies map[ScopeKey]*RedactionPolicy
	dropped  map[ScopeKey]struct{}
}

func newFakeRedactions() *fakeRedactions {
	return &fakeRedactions{
		policies: make(map[ScopeKey]*RedactionPolicy),
		dropped:  make(map[ScopeKey]struct{}),
	}
}

func (f *fakeRedactions) RedactionFor(ref Key) (*RedactionPolicy, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if _, gone := f.dropped[ref.Scope()]; gone {
		return nil, false
	}
	return f.policies[ref.Scope()], true
}

// set installs a compiled policy for one scope.
func (f *fakeRedactions) set(scope ScopeKey, policy *RedactionPolicy) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policies[scope] = policy
}

// drop simulates a scope whose last interest disappeared, so no policy can be
// resolved for it at all.
func (f *fakeRedactions) drop(scope ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped[scope] = struct{}{}
}

// fakeRouter is a map-backed SinkRouter. Removing a sink reproduces the
// "sink deleted or mid-recycle" condition the pipeline must survive.
//
// It is keyed by sink.ID, exactly as the real SinkManager's routing table is, so
// a lookup carrying the wrong kind misses here too rather than being quietly
// answered by whatever shares its name. The setters take a name because the keys
// the pipeline itself still holds are names (see sinkIDFor).
type fakeRouter struct {
	mu      sync.RWMutex
	writers map[sink.ID]sink.Writer
}

func newFakeRouter() *fakeRouter {
	return &fakeRouter{writers: make(map[sink.ID]sink.Writer)}
}

func (f *fakeRouter) WriterFor(id sink.ID) (sink.Writer, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	w, ok := f.writers[id]
	return w, ok
}

func (f *fakeRouter) set(name string, w sink.Writer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writers[sinkIDFor(name)] = w
}

func (f *fakeRouter) remove(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.writers, sinkIDFor(name))
}

// fakeWriter is a sink.Writer that records every accepted job and settles it
// synchronously inside Enqueue. Settling synchronously (rather than on a
// goroutine) is deliberate: it makes each test's assertions deterministic while
// still exercising the real commit contract, since the production commit is
// version-gated precisely so it can run at *any* time relative to the caller.
//
// Failures are scripted rather than random: failEnqueue and failCommit queue up
// one-shot failures for the next job, so a test states exactly which attempt
// fails.
type fakeWriter struct {
	mu      sync.Mutex
	records []sink.Record
	// checkpointEvery makes this writer a CheckpointPolicy. It starts at 0 —
	// checkpointing off — so a test that says nothing about checkpoints gets a
	// pure diff stream, and one that does states its own cadence explicitly
	// rather than inheriting a number from the harness.
	checkpointEvery int
	// notify fans out accepted records so a test can wait for one instead of
	// polling. It is buffered generously and never blocks Enqueue.
	notify chan sink.Record
	// pendingEnqueueErrs / pendingCommitFails are consumed one per Enqueue call.
	pendingEnqueueErrs []error
	pendingCommitFails []bool
}

func newFakeWriter() *fakeWriter {
	return &fakeWriter{notify: make(chan sink.Record, 64)}
}

func (w *fakeWriter) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// CheckpointEvery implements CheckpointPolicy, so the fake sink expresses a
// Checkpoint cadence exactly the way the real one does (via the writer, resolved
// per work item) rather than through a back door into the pipeline.
func (w *fakeWriter) CheckpointEvery() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.checkpointEvery
}

// setCheckpointEvery declares this sink's Checkpoint cadence; 0 disables it.
func (w *fakeWriter) setCheckpointEvery(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkpointEvery = n
}

func (w *fakeWriter) Enqueue(_ context.Context, job sink.Job) error {
	w.mu.Lock()
	var enqueueErr error
	if len(w.pendingEnqueueErrs) > 0 {
		enqueueErr = w.pendingEnqueueErrs[0]
		w.pendingEnqueueErrs = w.pendingEnqueueErrs[1:]
	}
	if enqueueErr != nil {
		w.mu.Unlock()
		return enqueueErr
	}

	commitOK := true
	if len(w.pendingCommitFails) > 0 {
		commitOK = !w.pendingCommitFails[0]
		w.pendingCommitFails = w.pendingCommitFails[1:]
	}
	w.records = append(w.records, job.Record)
	w.mu.Unlock()

	select {
	case w.notify <- job.Record:
	default:
	}
	if job.Commit != nil {
		job.Commit(commitOK)
	}
	return nil
}

// failNextEnqueue makes the next Enqueue call return err without accepting the
// job — the "job never entered the write pipeline" path.
func (w *fakeWriter) failNextEnqueue(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pendingEnqueueErrs = append(w.pendingEnqueueErrs, err)
}

// failNextCommit makes the next accepted job settle as a failed write — the
// "abandoned after retries" path that must revert the cache.
func (w *fakeWriter) failNextCommit() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pendingCommitFails = append(w.pendingCommitFails, true)
}

func (w *fakeWriter) recorded() []sink.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.records)
}

// eventTypes returns the event_type of every accepted record, in order — the
// shape most ordering assertions want.
func (w *fakeWriter) eventTypes() []string {
	records := w.recorded()
	types := make([]string, 0, len(records))
	for _, r := range records {
		types = append(types, r.EventType)
	}
	return types
}

// awaitRecords waits until at least n records have been accepted, failing the
// test on timeout rather than deadlocking it.
func (w *fakeWriter) awaitRecords(t *testing.T, n int) []sink.Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if records := w.recorded(); len(records) >= n {
			return records
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records, got %v", n, w.eventTypes())
	return nil
}

// recordingLogSink is a logr.LogSink that captures the errors passed to
// log.Error, so a test can assert the pipeline logged a specific anomaly exactly
// as often as expected (Invariant 4: zero silent errors; plus the rate-limited
// logging requirement for an unavailable sink). It is concurrency-safe because
// commit callbacks and workers log from different goroutines.
type recordingLogSink struct {
	mu     sync.Mutex
	errors []error
}

func (s *recordingLogSink) Init(logr.RuntimeInfo)          {}
func (s *recordingLogSink) Enabled(int) bool               { return true }
func (s *recordingLogSink) Info(int, string, ...any)       {}
func (s *recordingLogSink) WithValues(...any) logr.LogSink { return s }
func (s *recordingLogSink) WithName(string) logr.LogSink   { return s }
func (s *recordingLogSink) Error(err error, _ string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, err)
}

func (s *recordingLogSink) loggedErrors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.errors)
}

// countOf returns how many captured errors are target.
func (s *recordingLogSink) countOf(target error) int {
	n := 0
	for _, err := range s.loggedErrors() {
		if err == target {
			n++
		}
	}
	return n
}

// testHarness bundles a Pipeline with the doubles behind it, so each test states
// only the behaviour it cares about.
type testHarness struct {
	pipeline   *Pipeline
	lister     *fakeLister
	router     *fakeRouter
	writer     *fakeWriter
	redactions *fakeRedactions
	logs       *recordingLogSink
	// ctx carries the recording logger, so log assertions work for code that
	// pulls its logger from the context (as Process does).
	ctx context.Context
}

// newHarness builds a Pipeline wired to fresh doubles, with an isolated metrics
// registry (Prometheus panics on duplicate registration, so a shared instance
// would make repeated test setups fatal) and a fast rate limiter so retry
// assertions don't wait on production backoff.
func newHarness(t *testing.T, sinkNames ...string) *testHarness {
	t.Helper()
	if len(sinkNames) == 0 {
		sinkNames = []string{testSink}
	}

	lister := newFakeLister()
	router := newFakeRouter()
	writer := newFakeWriter()
	redactions := newFakeRedactions()
	for _, name := range sinkNames {
		router.set(name, writer)
	}

	p := newTestPipeline(t, lister, router, redactions)

	logs := &recordingLogSink{}
	return &testHarness{
		pipeline:   p,
		lister:     lister,
		router:     router,
		writer:     writer,
		redactions: redactions,
		logs:       logs,
		ctx:        logr.NewContext(context.Background(), logr.New(logs)),
	}
}

// newTestPipeline builds a Pipeline over the given doubles, with an isolated
// metrics registry and a fast rate limiter. It is separate from newHarness so a
// test can build a *second* pipeline over the same doubles — which is what
// simulating an operator restart amounts to, since every pipeline-side cache is
// per-process (Invariant 6).
func newTestPipeline(t *testing.T, lister ListerRegistry, router SinkRouter,
	redactions RedactionRegistry) *Pipeline {
	t.Helper()
	p, err := New(Options{
		ClusterID:  "test-cluster",
		Workers:    4,
		Lister:     lister,
		Router:     router,
		Redactions: redactions,
		Metrics:    NewPipelineMetrics(prometheus.NewRegistry()),
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[Key](
			time.Millisecond, 20*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Every Pipeline owns a delaying queue with its own background goroutine;
	// shutting it down keeps tests that never call Start goroutine-clean.
	t.Cleanup(p.queue.ShutDown)
	return p
}

// restart replaces the harness's Pipeline with a fresh one over the same doubles,
// standing in for an operator restart: the watch cache and the sink's history
// survive (the lister and writer are the same objects), while every in-memory
// pipeline cache — hashCache entries, warm scopes, the per-key modified counter —
// starts empty, exactly as it does in a new process.
func (h *testHarness) restart(t *testing.T) {
	t.Helper()
	h.pipeline = newTestPipeline(t, h.lister, h.router, h.redactions)
}

// run starts the worker pool and returns a stop function that cancels it and
// waits for a clean exit, so a test can assert on shutdown as well as behaviour.
func (h *testHarness) run(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- h.pipeline.Start(ctx) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Start returned %v, want nil", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("pipeline workers did not stop within 10s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

const testSink = "default"

// testUID is the object UID the fixtures below stamp when a spec does not care
// which incarnation it is looking at (a spec that *does* care — the
// reincarnation ones — passes its own UID to newPod).
const testUID = "uid-1"

// podKey builds a v1/Pod key in the default namespace for the named object.
func podKey(name string) Key {
	return Key{Sink: testSink, Group: "", Kind: "Pod", Namespace: "default", Name: name}
}

// newPod builds a minimal Pod in the default namespace as the watch cache would
// hold it: a real UID and resourceVersion, plus an image the caller can vary to
// produce a genuine content change between events.
func newPod(name, uid, resourceVersion, image string) *unstructured.Unstructured {
	return newPodIn("default", name, uid, resourceVersion, image)
}

// selectorEntries is how many spec.nodeSelector entries newPodWithSelector adds.
// Forty is chosen so that renaming every key produces a patch comfortably larger
// than the whole object, with enough margin that the fixture does not sit on the
// trigger's boundary (the specs assert that premise explicitly).
const selectorEntries = 40

// newPodWithSelector builds a Pod carrying selectorEntries spec.nodeSelector
// entries named "<keyPrefix>0".."<keyPrefix>39".
//
// It exists to craft the one case the size-based checkpoint trigger is about: two
// consecutive states whose *diff* is larger than the newer state itself. Renaming
// every key (a different keyPrefix) makes jsondiff emit a remove **and** an add
// per entry, each carrying the full JSON pointer, so the patch outgrows a small
// object — which is precisely when storing the diff alone saves nothing and still
// costs the reader a replay step.
func newPodWithSelector(name, resourceVersion, keyPrefix string) *unstructured.Unstructured {
	selector := make(map[string]any, selectorEntries)
	for i := range selectorEntries {
		selector[fmt.Sprintf("%s%d", keyPrefix, i)] = "y"
	}
	pod := newPod(name, testUID, resourceVersion, "busybox:1")
	pod.Object["spec"].(map[string]any)["nodeSelector"] = selector
	return pod
}

// newPodIn is newPod for a specific namespace, so a spec can build two objects
// that belong to different watch scopes.
func newPodIn(namespace, name, uid, resourceVersion, image string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       namespace,
			"uid":             uid,
			"resourceVersion": resourceVersion,
		},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}},
	}}
}
