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
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// clickHouseSink builds the identity of a named ClickHouseSink — the only sink
// kind an authored rule can name today, so it is the right shape for a fixture
// that just needs *a* sink. A test about kind separation names its second kind
// explicitly (see TestInterestTableSeparatesSinksOfDifferentKinds).
func clickHouseSink(name string) sink.ID {
	return sink.ID{Kind: sink.DefaultSinkKind, Name: name}
}

// sinkA and sinkB are the two sinks these tests fan out to. Two sinks rather than
// one because the per-sink half of the interest map — one event becoming two work
// keys, one rule's scope stopping while another sink still holds it — is only
// observable with both.
var (
	sinkA = clickHouseSink("sink-a")
	sinkB = clickHouseSink("sink-b")
)

// The doubles in this file stand in for the two components the WatchManager hands
// work to — Task 1.5's pipeline and Task 1.6's scope recorder — and nothing else.
//
// The cluster side is deliberately *not* faked. client-go's fake dynamic client
// cannot serve the streaming watch-list the pinned client-go (v0.35.0) prefers by
// default (its WatchListClient feature gate defaults to on from 1.35), so an
// informer built over it never syncs. Anything event-driven therefore runs against
// the package's envtest API server, which is the more honest test anyway: the
// transform, the tombstone path and the store's semantics are client-go's
// behaviour rather than ours, and only a real API server reproduces them.

// configMapGVK / configMapGVR are the second kind these tests use, so a test can
// prove two *different* resources produce two informers while two rules on one
// resource produce one. ConfigMaps are namespaced, cheap, and label-able.
var (
	configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	configMapGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

// namespaceCounter makes every test's namespaces unique.
//
// The envtest API server is shared by the whole package and a deleted namespace
// lingers in Terminating, so tests watching a fixed "ns-a" would see each other's
// objects. A per-test suffix keeps the acceptance criteria's ns-a / ns-b framing
// while keeping the tests independent.
var namespaceCounter atomic.Int64

// fakePipeline records everything the WatchManager hands the data plane: the work
// keys informer events fan out to, and the scope evictions a stopped target
// triggers.
//
// Add is called from informer notification goroutines, so every field is
// mutex-guarded and the whole type is safe under -race.
type fakePipeline struct {
	mu      sync.Mutex
	keys    []pipeline.Key
	evicted []evictedScope
}

// evictedScope is one EvictScope call, recorded in order.
type evictedScope struct {
	sink  sink.ID
	scope pipeline.ScopeKey
}

func (f *fakePipeline) Add(key pipeline.Key) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
}

func (f *fakePipeline) EvictScope(id sink.ID, scope pipeline.ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evicted = append(f.evicted, evictedScope{sink: id, scope: scope})
}

// enqueued returns the keys seen so far, in order.
func (f *fakePipeline) enqueued() []pipeline.Key {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.keys)
}

// evictions returns the EvictScope calls seen so far, in order.
func (f *fakePipeline) evictions() []evictedScope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.evicted)
}

// reset drops everything recorded so far, so a test can assert on a single phase
// of a longer scenario without arithmetic on earlier phases.
func (f *fakePipeline) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = nil
	f.evicted = nil
}

// waitForKeys blocks until at least want keys have been enqueued, then returns
// them. Informer delivery is asynchronous by nature, so polling for the expected
// state (rather than sleeping a guessed interval) is what keeps these tests fast
// and non-flaky.
func (f *fakePipeline) waitForKeys(t *testing.T, want int) []pipeline.Key {
	t.Helper()
	var got []pipeline.Key
	waitFor(t, func() bool {
		got = f.enqueued()
		return len(got) >= want
	}, func() string {
		return fmt.Sprintf("%d enqueued keys, have %v", want, f.enqueued())
	})
	return got
}

// fakeRecorder records the scope transitions the WatchManager reports, standing in
// for the production ScopeEpochRecorder (whose own semantics are the subject of
// scope_recorder_test.go).
type fakeRecorder struct {
	mu          sync.Mutex
	transitions []recordedTransition
}

// recordedTransition is one ScopeStarted / ScopeStopped call, with the action
// spelled out so a test can filter on it.
type recordedTransition struct {
	action string
	ScopeTransition
}

func (r *fakeRecorder) ScopeStarted(transition ScopeTransition) {
	r.record("Started", transition)
}

func (r *fakeRecorder) ScopeStopped(transition ScopeTransition) {
	r.record("Stopped", transition)
}

func (r *fakeRecorder) record(action string, transition ScopeTransition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	transition.RuleKeys = slices.Clone(transition.RuleKeys)
	r.transitions = append(r.transitions, recordedTransition{action: action, ScopeTransition: transition})
}

// recorded returns the transitions seen so far, in order.
func (r *fakeRecorder) recorded() []recordedTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.transitions)
}

// newStaticResolver returns a Resolver over a hand-built REST mapper covering
// exactly the kinds a test names.
//
// It exists so a test can decide precisely which kinds are resolvable — including
// the case where one target's kind is not — without installing or uninstalling
// CRDs. The resolver's own behaviour against live discovery is resolver_test.go's
// subject; here it is a dependency, not the thing under test.
func newStaticResolver(namespaced []schema.GroupVersionKind, clusterScoped ...schema.GroupVersionKind) *Resolver {
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range namespaced {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	for _, gvk := range clusterScoped {
		mapper.Add(gvk, meta.RESTScopeRoot)
	}
	return NewResolver(mapper)
}

// newDynamicClient returns a dynamic client for the package's envtest API server —
// the same client production wires into the WatchManager.
func newDynamicClient(t *testing.T) dynamic.Interface {
	t.Helper()
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("building a dynamic client for envtest: %v", err)
	}
	return dyn
}

// newNamespaces creates one namespace per suffix and returns their real names, in
// order. Names are uniquified per test (see namespaceCounter).
func newNamespaces(t *testing.T, dyn dynamic.Interface, suffixes ...string) []string {
	t.Helper()
	id := namespaceCounter.Add(1)
	names := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		name := fmt.Sprintf("%s-%d", suffix, id)
		ns := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata":   map[string]any{"name": name},
		}}
		if _, err := dyn.Resource(namespaceGVR).Create(t.Context(), ns, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating namespace %q: %v", name, err)
		}
		names = append(names, name)
	}
	return names
}

// newPod builds a Pod the API server will accept, carrying the given labels.
func newPod(namespace, name string, objLabels map[string]string) *unstructured.Unstructured {
	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"containers": []any{
				map[string]any{"name": "app", "image": "registry.k8s.io/pause:3.10"},
			},
		},
	}}
	if len(objLabels) > 0 {
		pod.SetLabels(objLabels)
	}
	return pod
}

// podWithManagedFields builds a Pod carrying one managedFields entry per manager
// name. It is for the transform's unit tests: the API server writes managedFields
// itself, but a unit test needs to control exactly which managers appear —
// including duplicates and empty names.
func podWithManagedFields(namespace, name string, managers ...string) *unstructured.Unstructured {
	pod := newPod(namespace, name, nil)
	entries := make([]any, 0, len(managers))
	for _, manager := range managers {
		entries = append(entries, map[string]any{
			"manager":   manager,
			"operation": "Update",
			"fieldsV1":  map[string]any{"f:spec": map[string]any{}},
		})
	}
	if err := unstructured.SetNestedSlice(pod.Object, entries, "metadata", "managedFields"); err != nil {
		// Only reachable if the literal above stops being valid JSON-ish data.
		panic(err)
	}
	return pod
}

// createPod creates a Pod and returns what the API server stored.
func createPod(t *testing.T, dyn dynamic.Interface, pod *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	created, err := dyn.Resource(podGVR).Namespace(pod.GetNamespace()).
		Create(t.Context(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating pod %s/%s: %v", pod.GetNamespace(), pod.GetName(), err)
	}
	return created
}

// relabelPod replaces a Pod's labels, which is how the selector tests move an
// object in and out of a rule's scope.
func relabelPod(t *testing.T, dyn dynamic.Interface, namespace, name string, objLabels map[string]string) {
	t.Helper()
	pods := dyn.Resource(podGVR).Namespace(namespace)
	pod, err := pods.Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting pod %s/%s: %v", namespace, name, err)
	}
	pod.SetLabels(objLabels)
	if _, err := pods.Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating pod %s/%s: %v", namespace, name, err)
	}
}

// deletePod removes a Pod immediately.
//
// The zero grace period is not optional: envtest runs no kubelet, so a Pod deleted
// with the default 30-second grace period only ever gets a deletionTimestamp and
// stays Terminating forever — the informer would see an update and never a delete.
func deletePod(t *testing.T, dyn dynamic.Interface, namespace, name string) {
	t.Helper()
	err := dyn.Resource(podGVR).Namespace(namespace).Delete(t.Context(), name,
		metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting pod %s/%s: %v", namespace, name, err)
	}
}

// waitFor polls cond until it is true or the deadline passes, failing with the
// message describe returns. describe is a function so the failure message reflects
// the state at the moment of the timeout rather than at the start of the wait.
func waitFor(t *testing.T, cond func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", describe())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// logCapture is a logr.Logger that remembers the lines written to it, so a test can
// assert that an anomaly was actually reported (Invariant 4) rather than merely
// handled quietly.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

// logger returns a logr.Logger writing into this capture.
func (c *logCapture) logger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lines = append(c.lines, prefix+" "+args)
	}, funcr.Options{})
}

// captured returns the log lines seen so far.
func (c *logCapture) captured() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.lines)
}

// contains reports whether any captured line mentions substr.
func (c *logCapture) contains(substr string) bool {
	return slices.ContainsFunc(c.captured(), func(line string) bool {
		return strings.Contains(line, substr)
	})
}
