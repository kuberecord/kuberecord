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

// This file is Task 19.3's informer-level half: which watch a derived field
// selector actually produces, when several rules on one informer force a
// fallback, and — the criterion the task exists to protect — that the server-side
// and handler-side paths record byte-identical rows (D49).
//
// deriveEventFieldSelector's own cases live in eventfilter_test.go. What is
// asserted here is everything that only becomes true once a real API server and a
// shared informer are involved.

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/workqueue"

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/plan"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// modernEventGVK / modernEventGVR are `events.k8s.io/v1`, the second Event API.
// It is the same storage behind a renamed shape, which is exactly why push-down
// has to treat it separately: the rows are identical and the field labels are
// not.
var (
	modernEventGVK = schema.GroupVersionKind{Group: "events.k8s.io", Version: "v1", Kind: "Event"}
	modernEventGVR = schema.GroupVersionResource{Group: "events.k8s.io", Version: "v1", Resource: "events"}
)

// eventRuleTarget builds a watch target for Events carrying one rule's filter.
func eventRuleTarget(t *testing.T, sinkID sink.ID, gvk schema.GroupVersionKind,
	namespace string, spec EventFilterSpec) plan.WatchTarget {
	t.Helper()
	return plan.WatchTarget{
		Sink:        sinkID,
		GVK:         gvk,
		Namespace:   namespace,
		EventFilter: mustCanonical(t, spec),
	}
}

// withEventKinds teaches a manager harness's resolver about both Event APIs. The
// default harness resolver knows only the kinds the older tests name.
func withEventKinds(o *Options) {
	o.Resolver = newStaticResolver(
		[]schema.GroupVersionKind{podGVK, configMapGVK, eventGVK, modernEventGVK}, namespaceGVK)
}

// informerKeysOf returns the pool's running informer keys rendered as strings,
// for failure messages that name what is actually running.
func informerKeysOf(t *testing.T, h *managerHarness) []string {
	t.Helper()
	keys := h.manager.pool.keys()
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key.String())
	}
	return out
}

// waitForInformer blocks until exactly one informer is running for scope and
// returns its key, so a test can assert on the field selector it came up with.
func waitForInformer(t *testing.T, h *managerHarness, scope informerScope) informerKey {
	t.Helper()
	var found informerKey
	waitFor(t, func() bool {
		for _, key := range h.manager.pool.keys() {
			if key.informerScope == scope {
				found = key
				return true
			}
		}
		return false
	}, func() string { return fmt.Sprintf("an informer for %s, have %v", scope, informerKeysOf(t, h)) })
	return found
}

// TestEventFilterPushesDownWhenInterestsAgree is the sharing criterion's happy
// half: two rules for two sinks that ask for the same thing cost one informer,
// and that one informer is narrowed at the API server.
func TestEventFilterPushesDownWhenInterestsAgree(t *testing.T) {
	h := newManagerHarness(t, withEventKinds)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	scope := informerScope{GVR: eventGVR, Namespace: namespace}
	filter := EventFilterSpec{ExcludeReasons: []string{"Created", "Pulled", "Started"}}

	h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, eventGVK, namespace, filter))
	h.upsert(t, "rule-b", eventRuleTarget(t, sinkB, eventGVK, namespace, filter))

	key := waitForInformer(t, h, scope)
	want := "reason!=Created,reason!=Pulled,reason!=Started"
	if key.FieldSelector != want {
		t.Errorf("field selector = %q, want %q", key.FieldSelector, want)
	}
	if got := h.manager.PoolSize(); got != 1 {
		t.Errorf("pool size = %d, want 1: two rules on one scope must share one informer (have %v)",
			got, informerKeysOf(t, h))
	}
}

// TestEventFilterFallsBackWhenInterestsDisagree is the other half, and the one
// that matters: an informer is shared, so a selector may only be pushed when
// nobody on it would be under-served. Two rules wanting different subsets watch
// the whole stream and narrow handler-side — still one informer, and each sink
// still gets exactly what its own rule asked for.
func TestEventFilterFallsBackWhenInterestsDisagree(t *testing.T) {
	h := newManagerHarness(t, withEventKinds)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	scope := informerScope{GVR: eventGVR, Namespace: namespace}

	h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, eventGVK, namespace,
		EventFilterSpec{Types: []string{"Warning"}}))
	h.upsert(t, "rule-b", eventRuleTarget(t, sinkB, eventGVK, namespace,
		EventFilterSpec{Reasons: []string{"BackOff"}}))

	key := waitForInformer(t, h, scope)
	if key.FieldSelector != "" {
		t.Errorf("field selector = %q, want none: two interests deriving different selectors must watch unfiltered",
			key.FieldSelector)
	}
	if got := h.manager.PoolSize(); got != 1 {
		t.Errorf("pool size = %d, want 1: a disagreement must cost a selector, never an informer (have %v)",
			got, informerKeysOf(t, h))
	}

	// The fallback is a fallback, not a surrender: both rules still record
	// exactly their own subset, handler-side.
	interests := h.manager.table.interestsFor(key)
	if len(interests) != 2 {
		t.Fatalf("the shared informer serves %d interests, want 2", len(interests))
	}
	backoff := newEvent("backoff.1", "Warning", "BackOff", "kubelet", "Pod")
	pulled := newEvent("pulled.1", "Normal", "Pulled", "kubelet", "Pod")
	for _, in := range interests {
		if in.recordsEveryEvent() {
			t.Errorf("%s records every Event; the handler-side filter was lost with the push-down", in.sink)
		}
		if !in.matchesEvent(backoff.Object) {
			t.Errorf("%s rejected the Warning BackOff both rules select", in.sink)
		}
		if in.matchesEvent(pulled.Object) {
			t.Errorf("%s accepted a Normal Pulled neither rule selects", in.sink)
		}
	}
}

// TestEventFilterFallsBackForAnUnpushableFilter covers the two shapes a single
// rule can hold that no field selector can express: a multi-valued include (there
// is no OR) and `sourceComponents` (core's `source` is a fallback chain, while
// the matcher ORs four spellings — see eventSelectableFields).
func TestEventFilterFallsBackForAnUnpushableFilter(t *testing.T) {
	cases := []struct {
		name string
		spec EventFilterSpec
	}{
		{"a multi-valued include", EventFilterSpec{Reasons: []string{"BackOff", "Killing"}}},
		{"sourceComponents", EventFilterSpec{SourceComponents: []string{"default-scheduler"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newManagerHarness(t, withEventKinds)
			namespace := newNamespaces(t, h.dyn, "ns-a")[0]

			h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, eventGVK, namespace, tc.spec))

			key := waitForInformer(t, h, informerScope{GVR: eventGVR, Namespace: namespace})
			if key.FieldSelector != "" {
				t.Errorf("field selector = %q, want none", key.FieldSelector)
			}
			interests := h.manager.table.interestsFor(key)
			if len(interests) != 1 || interests[0].recordsEveryEvent() {
				t.Error("the filter must still be applied handler-side when it cannot be pushed")
			}
		})
	}
}

// TestEventFilterPushDownUsesTheModernLabels is the `events.k8s.io/v1` criterion,
// and the reason it is a criterion: the modern API is *not* selector-less, it is
// renamed. It registers `regarding.*` and rejects `involvedObject.*`, so sending
// the core spelling would fail with "field label not supported" and the reflector
// would retry it forever. Where it registers nothing — the component axis — the
// fallback is silent, because that is a performance difference and not a
// functional one.
func TestEventFilterPushDownUsesTheModernLabels(t *testing.T) {
	cases := []struct {
		name string
		spec EventFilterSpec
		want string
	}{
		{
			name: "the subject axis is renamed, not absent",
			spec: EventFilterSpec{SubjectKinds: []string{"Pod"}},
			want: "regarding.kind=Pod",
		},
		{
			name: "type and reason are shared unchanged",
			spec: EventFilterSpec{Types: []string{"Warning"}, ExcludeReasons: []string{"Pulled"}},
			want: "type=Warning,reason!=Pulled",
		},
		{
			name: "the component axis falls back silently",
			spec: EventFilterSpec{SourceComponents: []string{"kubelet"}},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newManagerHarness(t, withEventKinds)
			namespace := newNamespaces(t, h.dyn, "ns-a")[0]

			h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, modernEventGVK, namespace, tc.spec))

			key := waitForInformer(t, h, informerScope{GVR: modernEventGVR, Namespace: namespace})
			if key.FieldSelector != tc.want {
				t.Errorf("field selector = %q, want %q", key.FieldSelector, tc.want)
			}
			if strings.Contains(key.FieldSelector, "involvedObject") {
				t.Error("a core-API field label was sent to events.k8s.io/v1; the API server rejects those")
			}
		})
	}
}

// TestNonEventTargetsNeverAcquireAFieldSelector guards the blast radius. Only an
// Event can carry a filter, so every other informer in the pool must be exactly
// what it was before Task 19.3 — no selector, and therefore no re-List when one
// is derived somewhere else in the same pass.
func TestNonEventTargetsNeverAcquireAFieldSelector(t *testing.T) {
	h := newManagerHarness(t, withEventKinds)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]

	h.upsert(t, "rule-pods", podTarget(sinkA, namespace, ""))
	h.upsert(t, "rule-events", eventRuleTarget(t, sinkA, eventGVK, namespace,
		EventFilterSpec{Types: []string{"Warning"}}))

	pods := waitForInformer(t, h, informerScope{GVR: podGVR, Namespace: namespace})
	if pods.FieldSelector != "" {
		t.Errorf("the Pod informer acquired the field selector %q", pods.FieldSelector)
	}
	events := waitForInformer(t, h, informerScope{GVR: eventGVR, Namespace: namespace})
	if events.FieldSelector != "type=Warning" {
		t.Errorf("the Event informer's field selector = %q, want type=Warning", events.FieldSelector)
	}
}

// TestPushDownChangeIsNotAScopeTransition is the property interestID exists to
// protect, and the one whose loss would be silent.
//
// Adding a rule whose filter derives a different selector flips its *neighbour's*
// informer from filtered to unfiltered — a change to how the stream is fetched,
// not to what is watched. If that moved the interest's identity, the first rule's
// scope would be reported Stopped and immediately Started again, its dedup
// baselines evicted, and the recorder (which pairs the two edges by
// ScopeTransition.Target) would hold a Stopped that never matched its Started.
// An auditor would read a scope that closed and reopened for no reason anyone
// could name.
func TestPushDownChangeIsNotAScopeTransition(t *testing.T) {
	h := newManagerHarness(t, withEventKinds)
	namespace := newNamespaces(t, h.dyn, "ns-a")[0]
	scope := informerScope{GVR: eventGVR, Namespace: namespace}

	h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, eventGVK, namespace,
		EventFilterSpec{Types: []string{"Warning"}}))
	if key := waitForInformer(t, h, scope); key.FieldSelector != "type=Warning" {
		t.Fatalf("the first rule's informer is %q, want type=Warning", key.FieldSelector)
	}
	before := len(h.recorder.recorded())

	// A second sink, a filter that derives a different selector: the informer
	// must give up its push-down and serve both unfiltered.
	h.upsert(t, "rule-b", eventRuleTarget(t, sinkB, eventGVK, namespace,
		EventFilterSpec{Reasons: []string{"BackOff"}}))
	waitFor(t, func() bool {
		// An exact-key lookup: the unfiltered informer existing *is* the
		// fallback having happened, since the key carries the selector.
		_, running := h.manager.pool.entryFor(informerKey{informerScope: scope})
		return running
	}, func() string {
		return fmt.Sprintf("the informer to fall back to unfiltered, have %v", informerKeysOf(t, h))
	})

	// Exactly one new transition: sink B's scope starting. Nothing about sink A
	// changed, so nothing about sink A may be recorded.
	for _, got := range h.recorder.recorded()[before:] {
		if got.action != "Started" || got.Sink != sinkB {
			t.Errorf("recorded a %s for %s; push-down is invisible to the scope log", got.action, got.Sink)
		}
	}
	if evicted := h.pipe.evictions(); len(evicted) != 0 {
		t.Errorf("push-down evicted %v; a change to how a stream is fetched drops no dedup state", evicted)
	}
}

//
// The push-down table against the real API server
//

// TestEventSelectableFieldsMatchTheAPIServer is what makes eventSelectableFields
// a measurement rather than a memory.
//
// Every label the table claims is asserted to be accepted — on the Watch as well
// as the List, since both carry the selector — and every label it deliberately
// omits is asserted to be *rejected*, which is the half that would otherwise rot
// silently: a future Kubernetes that started registering `source` on
// events.k8s.io/v1, or that renamed a label again, would leave this code pushing
// nothing (a quiet cost) or pushing the wrong thing (a reflector that retries
// forever). It runs against the package's envtest API server, so "the pinned
// Kubernetes" is the thing actually answering.
func TestEventSelectableFieldsMatchTheAPIServer(t *testing.T) {
	dyn := newDynamicClient(t)

	accepts := func(t *testing.T, gvr schema.GroupVersionResource, selector string) bool {
		t.Helper()
		opts := metav1.ListOptions{FieldSelector: selector}
		_, listErr := dyn.Resource(gvr).Namespace("default").List(t.Context(), opts)
		// A watch is opened and immediately closed: the rejection this test
		// looks for arrives from the API server before any event does.
		watcher, watchErr := dyn.Resource(gvr).Namespace("default").Watch(t.Context(), opts)
		if watchErr == nil {
			watcher.Stop()
		}
		if (listErr == nil) != (watchErr == nil) {
			t.Errorf("%s %q: the List and the Watch disagree (list %v, watch %v)",
				gvr.GroupResource(), selector, listErr, watchErr)
		}
		return listErr == nil
	}

	gvrs := map[schema.GroupResource]schema.GroupVersionResource{
		eventGVR.GroupResource():       eventGVR,
		modernEventGVR.GroupResource(): modernEventGVR,
	}

	for gr, labels := range eventSelectableFields {
		gvr, known := gvrs[gr]
		if !known {
			t.Fatalf("eventSelectableFields names %s, which this test has no GVR for", gr)
		}
		for _, label := range []string{labels.eventType, labels.reason, labels.subjectKind, labels.subjectName} {
			t.Run(gr.String()+" accepts "+label, func(t *testing.T) {
				if !accepts(t, gvr, label+"=probe") {
					t.Errorf("%s rejects %q, which eventSelectableFields claims it registers", gr, label)
				}
			})
		}
	}

	// The omissions, each one a decision recorded in eventSelectableFields.
	rejected := []struct {
		gvr   schema.GroupVersionResource
		label string
		why   string
	}{
		{eventGVR, "regarding.kind", "the core API did not adopt the modern subject spelling"},
		{eventGVR, "reportingController", "the core API spells it reportingComponent"},
		{modernEventGVR, "involvedObject.kind", "the modern API renamed the subject to regarding"},
		{modernEventGVR, "source", "the modern API registers no component label at all"},
		{modernEventGVR, "reportingComponent", "the modern API spells it reportingController"},
	}
	for _, tc := range rejected {
		t.Run(tc.gvr.GroupResource().String()+" rejects "+tc.label, func(t *testing.T) {
			if accepts(t, tc.gvr, tc.label+"=probe") {
				t.Errorf("%s now accepts %q (%s); eventSelectableFields is out of date",
					tc.gvr.GroupResource(), tc.label, tc.why)
			}
		})
	}

	// The component axis is left handler-side because core's `source` is a
	// *fallback chain* while componentMatches is an OR over four spellings. That
	// is the one omission an optimiser would be tempted to undo, so it is
	// demonstrated rather than argued: an Event carrying both spellings is
	// returned by one and not the other, while the matcher accepts either.
	namespace := newNamespaces(t, dyn, "src")[0]
	both := newEventWithSource("both.1", "kubelet", "my-controller")
	if _, err := dyn.Resource(eventGVR).Namespace(namespace).
		Create(t.Context(), withNamespace(both, namespace), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the probe Event: %v", err)
	}
	names := listedNames(t, dyn, eventGVR, namespace, "source=my-controller")
	if slices.Contains(names, "both.1") {
		t.Error("`source` no longer falls back; re-check whether sourceComponents could push down")
	}
	matcher := mustCompile(t, EventFilterSpec{SourceComponents: []string{"my-controller"}})
	if !matcher.matches(both.Object) {
		t.Error("componentMatches no longer accepts a reportingComponent-only match; the two sides now agree")
	}
}

// listedNames returns the object names a field-selected List returns.
func listedNames(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource,
	namespace, selector string) []string {
	t.Helper()
	list, err := dyn.Resource(gvr).Namespace(namespace).
		List(t.Context(), metav1.ListOptions{FieldSelector: selector})
	if err != nil {
		t.Fatalf("List(%q): %v", selector, err)
	}
	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	return names
}

// newEventWithSource builds a core v1 Event carrying both spellings of "who
// emitted this", which is the shape that tells the fallback chain and the
// four-way OR apart.
func newEventWithSource(name, component, reportingComponent string) *unstructured.Unstructured {
	event := newEvent(name, "Warning", "ProbeReason", component, "Pod")
	event.Object["reportingComponent"] = reportingComponent
	event.Object["reportingInstance"] = "probe-instance"
	event.Object["action"] = "Probe"
	event.Object["eventTime"] = "2026-01-01T00:00:00.000000Z"
	return event
}

// withNamespace retargets one of newEvent's fixtures at a real namespace.
func withNamespace(event *unstructured.Unstructured, namespace string) *unstructured.Unstructured {
	event.SetNamespace(namespace)
	if involved, ok := event.Object["involvedObject"].(map[string]any); ok {
		involved["namespace"] = namespace
	}
	return event
}

//
// D49: the two paths record the same rows
//

// TestServerSideAndHandlerSideRecordIdenticalRows is the criterion Task 19.3
// exists to protect, asserted directly rather than argued: one Event corpus, both
// paths, byte-identical rows.
//
// Push-down is a performance decision. A divergence would silently make it a
// content decision — what an archive holds would depend on which *other* rules
// happened to exist when it was written, which is unauditable and irreproducible.
//
// One process, one namespace, one corpus, so that every field of every record
// except its instant must match exactly:
//
//   - Phase 1: rule-A alone on (events, ns). Its filter pushes down, so the
//     informer is genuinely narrowed at the API server.
//   - Phase 2: rule-C asks for the same subset for a *different* sink, and rule-D
//     asks for a different one — which forces the shared informer unfiltered and
//     every interest on it back to handler-side evaluation.
//
// Sink A's rows from phase 1 and sink C's from phase 2 are then the same corpus
// through the two paths. Dedup is per (sink, identity), so sink C sees the corpus
// fresh rather than deduplicated against sink A's.
func TestServerSideAndHandlerSideRecordIdenticalRows(t *testing.T) {
	dyn := newDynamicClient(t)
	namespace := newNamespaces(t, dyn, "d49")[0]

	// A corpus that straddles the filter: Warnings and Normals, several reasons,
	// two emitters, two subject kinds. The filter below keeps some of each, so a
	// path that silently kept everything (or nothing) fails rather than passing
	// by coincidence.
	corpus := []*unstructured.Unstructured{
		newEvent("backoff.1", "Warning", "BackOff", "kubelet", "Pod"),
		newEvent("failedsched.1", "Warning", "FailedScheduling", "default-scheduler", "Pod"),
		newEvent("unhealthy.1", "Warning", "Unhealthy", "kubelet", "Pod"),
		newEvent("pulled.1", "Normal", "Pulled", "kubelet", "Pod"),
		newEvent("created.1", "Normal", "Created", "kubelet", "Pod"),
		newEvent("started.1", "Normal", "Started", "kubelet", "Pod"),
		newEvent("scaled.1", "Normal", "SuccessfulCreate", "replicaset-controller", "ReplicaSet"),
		newEvent("scaledown.1", "Normal", "SuccessfulDelete", "replicaset-controller", "ReplicaSet"),
	}
	for _, event := range corpus {
		if _, err := dyn.Resource(eventGVR).Namespace(namespace).
			Create(t.Context(), withNamespace(event, namespace), metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating %s: %v", event.GetName(), err)
		}
	}

	// Pushes down completely, and keeps five of the eight above.
	kept := EventFilterSpec{ExcludeReasons: []string{"Created", "Pulled", "Started"}}
	// Derives a different selector, which is what forces the fallback.
	other := EventFilterSpec{Types: []string{"Warning"}}

	const wantKept = 5
	sinkC := clickHouseSink("sink-c")
	sinkD := clickHouseSink("sink-d")

	router := newPerSinkRouter(sinkA, sinkC, sinkD)
	h := newPushDownHarness(t, dyn, router)

	// --- Phase 1: the filter reaches the API server ---
	h.upsert(t, "rule-a", eventRuleTarget(t, sinkA, eventGVK, namespace, kept))
	key := waitForInformer(t, h.managerHarness, informerScope{GVR: eventGVR, Namespace: namespace})
	if key.FieldSelector == "" {
		t.Fatal("phase 1's informer was not narrowed; this test would compare one path with itself")
	}
	waitFor(t, func() bool { return len(router.recordsFor(sinkA)) == wantKept },
		func() string {
			return fmt.Sprintf("%d server-side rows, have %v", wantKept, namesOf(router.recordsFor(sinkA)))
		})
	serverSide := normalizedRecords(router.recordsFor(sinkA))

	// --- Phase 2: the same filter, forced handler-side ---
	h.upsert(t, "rule-c", eventRuleTarget(t, sinkC, eventGVK, namespace, kept))
	h.upsert(t, "rule-d", eventRuleTarget(t, sinkD, eventGVK, namespace, other))
	waitFor(t, func() bool {
		entry, running := h.manager.pool.entryFor(informerKey{
			informerScope: informerScope{GVR: eventGVR, Namespace: namespace},
		})
		return running && entry.key.FieldSelector == ""
	}, func() string { return "the informer to fall back to an unfiltered watch" })
	waitFor(t, func() bool { return len(router.recordsFor(sinkC)) == wantKept },
		func() string {
			return fmt.Sprintf("%d handler-side rows, have %v", wantKept, namesOf(router.recordsFor(sinkC)))
		})
	handlerSide := normalizedRecords(router.recordsFor(sinkC))

	// --- The criterion ---
	if len(serverSide) != len(handlerSide) {
		t.Fatalf("server-side recorded %v, handler-side %v", namesOf(serverSide), namesOf(handlerSide))
	}
	for i := range serverSide {
		if !reflect.DeepEqual(serverSide[i], handlerSide[i]) {
			t.Errorf("row %d diverges between the two paths:\n server-side: %+v\n handler-side: %+v",
				i, serverSide[i], handlerSide[i])
		}
	}

	// The corpus really did straddle the filter, so "identical" above is not two
	// empty sets agreeing, and the filter was applied rather than ignored.
	for _, dropped := range []string{"pulled.1", "created.1", "started.1"} {
		if slices.Contains(namesOf(serverSide), dropped) {
			t.Errorf("%q was recorded; the excluded reasons were not applied", dropped)
		}
	}

	// Sink A's rows are unchanged by the informer having been restarted
	// unfiltered underneath it: a re-List re-delivers, and dedup settles it.
	if got := len(router.recordsFor(sinkA)); got != wantKept {
		t.Errorf("sink A holds %d rows after the informer restarted, want %d", got, wantKept)
	}
}

// pushDownHarness is a managerHarness wired to the real pipeline and a recording
// sink, which the D49 comparison needs: the property is about the *rows*, and a
// fake enqueuer only ever proves something about keys.
type pushDownHarness struct {
	*managerHarness
}

// newPushDownHarness builds the full data plane — registry, WatchManager,
// pipeline, sink — over the package's envtest API server, and tears it down with
// the test.
func newPushDownHarness(t *testing.T, dyn dynamic.Interface, router *perSinkRouter) *pushDownHarness {
	t.Helper()

	lister := &deferredLister{}
	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: "test-cluster",
		Workers:   2,
		Lister:    lister,
		Router:    router,
		Metrics:   pipeline.NewPipelineMetrics(prometheus.NewRegistry()),
		// A fast limiter so the retry after a lookup against a not-yet-started
		// informer does not dominate the test.
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[pipeline.Key](
			time.Millisecond, 50*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}

	h := newManagerHarness(t, withEventKinds, func(o *Options) {
		o.Dynamic = dyn
		o.Pipeline = pipe
	})
	lister.manager = h.manager

	ctx := t.Context()
	var running sync.WaitGroup
	running.Go(func() {
		if err := pipe.Start(ctx); err != nil {
			t.Errorf("pipeline.Start: %v", err)
		}
	})
	t.Cleanup(running.Wait)

	return &pushDownHarness{managerHarness: h}
}

// perSinkRouter gives every sink its own recordingWriter, which is what lets the
// D49 comparison tell two sinks' rows apart: a sink.Job carries the row but not
// the identity it is bound for, and the router is the one place that knows.
//
// The map is built once and never written again, so the pipeline's workers read
// it concurrently without a lock.
type perSinkRouter struct{ writers map[sink.ID]*recordingWriter }

func newPerSinkRouter(ids ...sink.ID) *perSinkRouter {
	r := &perSinkRouter{writers: make(map[sink.ID]*recordingWriter, len(ids))}
	for _, id := range ids {
		r.writers[id] = &recordingWriter{}
	}
	return r
}

func (r *perSinkRouter) WriterFor(id sink.ID) (sink.Writer, bool) {
	writer, known := r.writers[id]
	if !known {
		return nil, false
	}
	return writer, true
}

// recordsFor returns everything written for one sink, in order.
func (r *perSinkRouter) recordsFor(id sink.ID) []sink.Record {
	writer := r.writers[id]
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return slices.Clone(writer.records)
}

// normalizedRecords sorts records by name and clears the one field that cannot
// be equal across two observations of one object: the instant it was recorded.
//
// Nothing else is normalised, deliberately. Every other field — uid,
// resourceVersion, labels, actors, event type, the full data payload and its
// hash — is derived from the stored object, so a difference in any of them is a
// difference in what the archive would hold, which is exactly what D49 forbids.
// The comparison is a deep one because a Record carries a map and a slice; a
// shallow == would not compile, and a field-by-field walk would silently stop
// covering a field added later.
func normalizedRecords(records []sink.Record) []sink.Record {
	out := slices.Clone(records)
	for i := range out {
		out[i].Timestamp = time.Time{}
	}
	slices.SortFunc(out, func(a, b sink.Record) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// namesOf renders a record set for a failure message.
func namesOf(records []sink.Record) []string {
	names := make([]string, 0, len(records))
	for _, record := range records {
		names = append(names, record.Name)
	}
	return names
}
