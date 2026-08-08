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
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/goleak"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/yelzhy/kuberecord/internal/pipeline"
)

// TestTransformObject is the transform half of D2: every informer's cached copy
// must lose managedFields and keep the one fact worth extracting from it.
func TestTransformObject(t *testing.T) {
	cases := []struct {
		name string
		obj  any
		// wantActors is the expected annotation value; "" means the annotation
		// must be absent.
		wantActors string
		// wantAnnotations is the expected full annotation map, checked only when
		// non-nil (to prove a pre-existing annotation survives).
		wantAnnotations map[string]string
	}{
		{
			name:       "actors are harvested and managedFields dropped",
			obj:        podWithManagedFields("ns-a", "web", "kubectl-client-side-apply"),
			wantActors: "kubectl-client-side-apply",
		},
		{
			name:       "actors are sorted and de-duplicated for determinism",
			obj:        podWithManagedFields("ns-a", "web", "zeta", "argocd-controller", "zeta", "kube-controller-manager"),
			wantActors: "argocd-controller,kube-controller-manager,zeta",
		},
		{
			name:       "an empty manager name is recorded as unknown",
			obj:        podWithManagedFields("ns-a", "web", ""),
			wantActors: "unknown",
		},
		{
			name:       "an object with no managedFields gains no annotation",
			obj:        newPod("ns-a", "web", nil),
			wantActors: "",
		},
		{
			name:            "a pre-existing annotation survives",
			obj:             podWithAnnotations("ns-a", "web", map[string]string{"team": "platform"}, "argocd-controller"),
			wantActors:      "argocd-controller",
			wantAnnotations: map[string]string{"team": "platform", pipeline.ActorsAnnotation: "argocd-controller"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := TransformObject(tc.obj)
			if err != nil {
				t.Fatalf("TransformObject returned an error: %v", err)
			}
			u, ok := out.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("TransformObject returned a %T, want *unstructured.Unstructured", out)
			}

			if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "metadata", "managedFields"); found {
				t.Error("managedFields survived the transform")
			}
			if got := u.GetAnnotations()[pipeline.ActorsAnnotation]; got != tc.wantActors {
				t.Errorf("actors annotation = %q, want %q", got, tc.wantActors)
			}
			if tc.wantAnnotations != nil {
				for key, want := range tc.wantAnnotations {
					if got := u.GetAnnotations()[key]; got != want {
						t.Errorf("annotation %q = %q, want %q", key, got, want)
					}
				}
			}
		})
	}
}

// TestTransformObjectIsIdempotent pins the property client-go asks of every
// transform: re-running it over an already-transformed object must not change it.
// Without the managedFields guard, a second pass would find no managers and wipe
// the actors annotation the first pass wrote.
func TestTransformObjectIsIdempotent(t *testing.T) {
	first, err := TransformObject(podWithManagedFields("ns-a", "web", "argocd-controller"))
	if err != nil {
		t.Fatalf("first transform: %v", err)
	}
	second, err := TransformObject(first)
	if err != nil {
		t.Fatalf("second transform: %v", err)
	}

	u := second.(*unstructured.Unstructured)
	if got := u.GetAnnotations()[pipeline.ActorsAnnotation]; got != "argocd-controller" {
		t.Errorf("actors annotation after a second transform = %q, want it preserved", got)
	}
}

// TestTransformObjectDegradesGracefully covers the two inputs that must not become
// errors: a transform error would drop the object from the informer entirely, so a
// malformed or unexpected object has to be cached as-is instead (Invariant 5).
func TestTransformObjectDegradesGracefully(t *testing.T) {
	cases := []struct {
		name string
		obj  any
	}{
		{name: "not an unstructured object", obj: "a string"},
		{
			name: "metadata is not a map",
			obj:  &unstructured.Unstructured{Object: map[string]any{"metadata": "not-a-map"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := TransformObject(tc.obj)
			if err != nil {
				t.Fatalf("TransformObject returned an error: %v", err)
			}
			if out == nil {
				t.Fatal("TransformObject dropped the object")
			}
		})
	}
}

// TestEventTargetOf covers identity extraction for every shape an informer event
// can take, including both tombstone variants.
func TestEventTargetOf(t *testing.T) {
	pod := newPod("ns-a", "web", map[string]string{"app": "web"})

	cases := []struct {
		name            string
		obj             any
		wantNamespace   string
		wantName        string
		wantLabelsKnown bool
		wantErr         bool
	}{
		{
			name:            "a live object",
			obj:             pod,
			wantNamespace:   "ns-a",
			wantName:        "web",
			wantLabelsKnown: true,
		},
		{
			name:            "a tombstone carrying its last known object",
			obj:             cache.DeletedFinalStateUnknown{Key: "ns-a/web", Obj: pod},
			wantNamespace:   "ns-a",
			wantName:        "web",
			wantLabelsKnown: true,
		},
		{
			name:          "a tombstone whose object was lost falls back to its key",
			obj:           cache.DeletedFinalStateUnknown{Key: "ns-a/web"},
			wantNamespace: "ns-a",
			wantName:      "web",
			// Labels are unknowable, which is what makes fan-out unconditional.
			wantLabelsKnown: false,
		},
		{
			name:          "a cluster-scoped tombstone key has no namespace",
			obj:           cache.DeletedFinalStateUnknown{Key: "kube-system"},
			wantNamespace: "",
			wantName:      "kube-system",
		},
		{name: "an unusable object is reported", obj: 42, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := eventTargetOf(tc.obj)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.namespace != tc.wantNamespace || got.name != tc.wantName {
				t.Errorf("identity = %q/%q, want %q/%q", got.namespace, got.name, tc.wantNamespace, tc.wantName)
			}
			if got.labelsKnown() != tc.wantLabelsKnown {
				t.Errorf("labelsKnown() = %t, want %t", got.labelsKnown(), tc.wantLabelsKnown)
			}
		})
	}
}

// TestPoolFanOutAppliesSelectorsPerSink covers the handler-side filter: one event
// reaches every interested sink whose selector matches, and only those.
func TestPoolFanOutAppliesSelectorsPerSink(t *testing.T) {
	table := newInterestTable()
	web := interestFor(t, "sink-web", "ns-a", []string{"app=web"}, []string{"rule-web"})
	all := interestFor(t, "sink-all", "ns-a", nil, []string{"rule-all"})
	table.replace(map[interestID]*scopeInterest{web.id(): web, all.id(): all})

	queue := &fakePipeline{}
	p := newPool(newDynamicClient(t), table, queue, logr.Discard())
	entry := &informerEntry{key: podsInNamespace("ns-a"), gvk: podGVK}

	// A matching object reaches both sinks.
	p.fanOut(entry, newPod("ns-a", "web", map[string]string{"app": "web"}), nil)
	if got := queue.enqueued(); len(got) != 2 {
		t.Fatalf("a matching object enqueued %d keys, want 2: %+v", len(got), got)
	}

	// A non-matching object reaches only the unfiltered sink.
	queue.reset()
	p.fanOut(entry, newPod("ns-a", "db", map[string]string{"app": "db"}), nil)
	got := queue.enqueued()
	if len(got) != 1 || got[0].Sink != "sink-all" {
		t.Fatalf("a non-matching object enqueued %+v, want one key for sink-all", got)
	}

	// An object leaving the selector's scope still produces one final key for the
	// filtered sink, so its last recorded state cannot silently freeze.
	queue.reset()
	p.fanOut(entry,
		newPod("ns-a", "web", map[string]string{"app": "db"}),
		newPod("ns-a", "web", map[string]string{"app": "web"}))
	sinks := sinksOf(queue.enqueued())
	if !slices.Equal(sinks, []string{"sink-all", "sink-web"}) {
		t.Errorf("a scope exit enqueued keys for %v, want both sinks", sinks)
	}
}

// TestPoolFanOutTombstone is the tombstone acceptance criterion: a
// DeletedFinalStateUnknown must produce the same delete key a plain Delete would,
// including when its inner object was lost and no selector can be evaluated.
func TestPoolFanOutTombstone(t *testing.T) {
	table := newInterestTable()
	// A narrow selector on purpose: a tombstone with no labels must still fan out.
	in := interestFor(t, "sink-a", "ns-a", []string{"app=web"}, []string{"rule-1"})
	table.replace(map[interestID]*scopeInterest{in.id(): in})

	queue := &fakePipeline{}
	p := newPool(newDynamicClient(t), table, queue, logr.Discard())
	entry := &informerEntry{key: podsInNamespace("ns-a"), gvk: podGVK}
	handler := p.handlerFor(entry)

	wantKey := pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: "ns-a", Name: "web"}

	cases := []struct {
		name      string
		tombstone cache.DeletedFinalStateUnknown
	}{
		{
			name:      "with its last known object",
			tombstone: cache.DeletedFinalStateUnknown{Key: "ns-a/web", Obj: newPod("ns-a", "web", map[string]string{"app": "web"})},
		},
		{
			name:      "with the object lost",
			tombstone: cache.DeletedFinalStateUnknown{Key: "ns-a/web"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queue.reset()
			handler.OnDelete(tc.tombstone)
			got := queue.enqueued()
			if len(got) != 1 || got[0] != wantKey {
				t.Fatalf("tombstone enqueued %+v, want exactly %+v", got, wantKey)
			}
		})
	}
}

// TestPoolStartStop is the pool's lifecycle contract: an informer serves events
// while it runs, its goroutine is gone once stop returns, and stopping the last
// informer leaves the pool empty.
func TestPoolStartStop(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	dyn := newDynamicClient(t)
	namespace := newNamespaces(t, dyn, "ns-a")[0]

	table := newInterestTable()
	in := interestFor(t, "sink-a", namespace, nil, []string{"rule-1"})
	table.replace(map[interestID]*scopeInterest{in.id(): in})

	queue := &fakePipeline{}
	p := newPool(dyn, table, queue, logr.Discard())

	key := podsInNamespace(namespace)
	if err := p.start(t.Context(), key, podGVK); err != nil {
		t.Fatalf("start: %v", err)
	}
	if p.size() != 1 {
		t.Fatalf("pool size after start = %d, want 1", p.size())
	}

	createPod(t, dyn, newPod(namespace, "web", nil))
	queue.waitForKeys(t, 1)

	// Starting the same key twice would silently leak the first informer.
	if err := p.start(t.Context(), key, podGVK); err == nil {
		t.Error("starting an already-running informer succeeded, want an error")
	}

	p.stop(key)
	if p.size() != 0 {
		t.Fatalf("pool size after stop = %d, want 0", p.size())
	}
	goleak.VerifyNone(t, leaked)
}

// TestPoolStopReportsALeakedGoroutine covers the degradation path: an informer
// goroutine that outlives the stop timeout must be reported at Error level and
// abandoned, never waited on forever — a wedged watch cannot be allowed to hold up
// a rule deletion.
func TestPoolStopReportsALeakedGoroutine(t *testing.T) {
	capture := &logCapture{}
	p := newPool(newDynamicClient(t), newInterestTable(), &fakePipeline{}, capture.logger())
	p.stopTimeout = 20 * time.Millisecond

	key := podsInNamespace("ns-a")
	// An entry whose goroutine never finishes: stopped is never closed.
	p.entries[key] = &informerEntry{key: key, gvk: podGVK, cancel: func() {}, stopped: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.stop(key)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop blocked past the stop timeout")
	}

	if !capture.contains("did not exit within the stop timeout") {
		t.Errorf("the leaked goroutine was not reported; captured: %v", capture.captured())
	}
	if p.size() != 0 {
		t.Errorf("pool size = %d, want the abandoned entry removed", p.size())
	}
}

// TestPoolRetainLevelTriggers covers retain in both directions at once: it starts
// what is missing and stops what is no longer wanted, which is the only way the
// pool ever changes shape.
func TestPoolRetainLevelTriggers(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	dyn := newDynamicClient(t)
	namespace := newNamespaces(t, dyn, "ns-a")[0]
	p := newPool(dyn, newInterestTable(), &fakePipeline{}, logr.Discard())
	pods := podsInNamespace(namespace)
	configMaps := informerKey{GVR: configMapGVR, Namespace: namespace}

	p.retain(t.Context(), map[informerKey]schema.GroupVersionKind{pods: podGVK})
	if p.size() != 1 {
		t.Fatalf("pool size = %d, want 1", p.size())
	}

	// Swap one target for another: one stop, one start, size unchanged.
	p.retain(t.Context(), map[informerKey]schema.GroupVersionKind{configMaps: configMapGVK})
	if p.size() != 1 {
		t.Fatalf("pool size after the swap = %d, want 1", p.size())
	}
	if _, running := p.entryFor(pods); running {
		t.Error("the pods informer is still running after it stopped being wanted")
	}
	if _, running := p.entryFor(configMaps); !running {
		t.Error("the configmaps informer did not start")
	}

	p.retain(t.Context(), nil)
	if p.size() != 0 {
		t.Fatalf("pool size after retaining nothing = %d, want 0", p.size())
	}
	goleak.VerifyNone(t, leaked)
}

// TestPoolStopAllStopsEveryInformer is the shutdown path: every goroutine is gone
// once stopAll returns, which is what lets the WatchManager's Start return cleanly.
func TestPoolStopAllStopsEveryInformer(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	dyn := newDynamicClient(t)
	namespaces := newNamespaces(t, dyn, "ns-a", "ns-b")
	p := newPool(dyn, newInterestTable(), &fakePipeline{}, logr.Discard())
	p.retain(t.Context(), map[informerKey]schema.GroupVersionKind{
		podsInNamespace(namespaces[0]):                podGVK,
		podsInNamespace(namespaces[1]):                podGVK,
		{GVR: configMapGVR, Namespace: namespaces[0]}: configMapGVK,
	})
	if p.size() != 3 {
		t.Fatalf("pool size = %d, want 3", p.size())
	}

	p.stopAll()
	if p.size() != 0 {
		t.Fatalf("pool size after stopAll = %d, want 0", p.size())
	}
	goleak.VerifyNone(t, leaked)
}

// podWithAnnotations builds a Pod that already carries annotations, so the
// transform can be shown to add to them rather than replace them.
func podWithAnnotations(namespace, name string, annotations map[string]string,
	managers ...string) *unstructured.Unstructured {
	pod := podWithManagedFields(namespace, name, managers...)
	pod.SetAnnotations(annotations)
	return pod
}

// sinksOf projects work keys onto their sink names, sorted, for order-independent
// assertions about fan-out.
func sinksOf(keys []pipeline.Key) []string {
	sinks := make([]string, 0, len(keys))
	for _, key := range keys {
		sinks = append(sinks, key.Sink)
	}
	slices.Sort(sinks)
	return sinks
}
