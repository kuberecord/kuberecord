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
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/plan"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// These benchmarks measure the two per-event costs on the watch side, which is
// the hottest path in the process: fanOut runs inside an informer's notification
// goroutine for every Add/Update/Delete (Invariant 1 — nothing there may block or
// waste), and lookupIdentity runs again for every work item the pipeline picks up.
//
// They exist for Task 2.3's allocation diet: the load harness reports what the
// whole process allocates, which cannot show whether the event path itself got
// cheaper. See internal/pipeline/allocation_bench_test.go for the same instrument
// on the pipeline side.

// countingEnqueuer is the cheapest possible Enqueuer: fanOut's subject is the
// work it does *before* handing a key over, so the hand-off must contribute
// nothing measurable of its own.
type countingEnqueuer struct{ n int }

func (c *countingEnqueuer) Add(pipeline.Key) { c.n++ }

// benchInformerScope is the (GVR, namespace) target every benchmark's events
// arrive from, and benchInformerKey the unfiltered informer that serves it.
var benchInformerScope = informerScope{
	GVR:       schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
	Namespace: "",
}

var benchInformerKey = informerKey{informerScope: benchInformerScope}

var benchGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

// benchWarehouse is the sink the benchmarks' interests are installed for, and
// benchUnknown one that never is — the miss path.
var (
	benchWarehouse = clickHouseSink("warehouse")
	benchUnknown   = clickHouseSink("unknown")
)

// benchTable installs one interest per sink, each with the given selectors and
// Event filters (nil meaning "match everything", which is what a rule with
// neither produces and the overwhelmingly common shape in practice).
func benchTable(b *testing.B, sinks []sink.ID, selectors, eventFilters []string) *interestTable {
	b.Helper()
	table := newInterestTable()
	desired := make(map[interestID]*scopeInterest, len(sinks))
	for _, sinkID := range sinks {
		in, err := newScopeInterest(
			plan.TargetState{
				Key:          plan.TargetKey{GVK: benchGVK, Namespace: "", Sink: sinkID},
				RuleKeys:     []string{"ClusterStreamRule/bench"},
				Selectors:    selectors,
				EventFilters: eventFilters,
			},
			benchInformerScope,
		)
		if err != nil {
			b.Fatalf("newScopeInterest: %v", err)
		}
		desired[in.id()] = in
	}
	table.replace(desired)
	return table
}

// benchObject is an object with the label set a real workload carries — enough
// labels that copying the map is not free, few enough that it is realistic.
func benchObject(name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(benchGVK)
	obj.SetNamespace("production")
	obj.SetName(name)
	obj.SetLabels(map[string]string{
		"app.kubernetes.io/name":      "checkout",
		"app.kubernetes.io/instance":  "checkout-prod",
		"app.kubernetes.io/version":   "1.42.0",
		"app.kubernetes.io/component": "api",
		"app.kubernetes.io/part-of":   "storefront",
		"team":                        "payments",
	})
	return obj
}

// BenchmarkFanOut measures the informer notification path: identify the object,
// ask the interest table who cares, enqueue one key each.
//
// The three shapes are the ones that differ in cost, not in behaviour:
// match-everything (what a rule with no selector produces), a real label
// selector, and an update — which is the only case with a *previous* object to
// consider as well as the current one.
func BenchmarkFanOut(b *testing.B) {
	current := benchObject("checkout")
	previous := benchObject("checkout")
	previous.SetResourceVersion("1")

	cases := []struct {
		name      string
		selectors []string
		previous  any
	}{
		{name: "match-all/add", selectors: nil, previous: nil},
		{name: "match-all/update", selectors: nil, previous: previous},
		{name: "selector/add", selectors: []string{"app.kubernetes.io/name=checkout"}, previous: nil},
		{name: "selector/update", selectors: []string{"app.kubernetes.io/name=checkout"}, previous: previous},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			table := benchTable(b, []sink.ID{benchWarehouse}, tc.selectors, nil)
			queue := &countingEnqueuer{}
			p := newPool(nil, table, queue, logr.Discard())
			entry := &informerEntry{key: benchInformerKey, gvk: benchGVK}

			b.ReportAllocs()
			for b.Loop() {
				p.fanOut(entry, current, tc.previous)
			}
			if queue.n == 0 {
				b.Fatal("fanOut enqueued nothing; the benchmark measured the wrong path")
			}
		})
	}
}

// BenchmarkLookupIdentity measures the pipeline's per-work-item scope lookup.
//
// The namespaced case is the one that matters: an object in a concrete namespace
// served by a cluster-wide interest has to consult two index keys, which is where
// a per-lookup allocation would hide.
func BenchmarkLookupIdentity(b *testing.B) {
	cases := []struct {
		name string
		ref  pipeline.Key
	}{
		{
			name: "namespaced-object-clusterwide-interest",
			ref: pipeline.Key{
				Sink: benchWarehouse, Group: "apps", Kind: "Deployment",
				Namespace: "production", Name: "checkout",
			},
		},
		{
			name: "cluster-scoped-object",
			ref: pipeline.Key{
				Sink: benchWarehouse, Group: "apps", Kind: "Deployment", Name: "checkout",
			},
		},
		{
			name: "no-interest",
			ref: pipeline.Key{
				Sink: benchUnknown, Group: "apps", Kind: "Deployment",
				Namespace: "production", Name: "checkout",
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			table := benchTable(b, []sink.ID{benchWarehouse}, nil, nil)
			b.ReportAllocs()
			for b.Loop() {
				table.lookupIdentity(tc.ref)
			}
		})
	}
}

// benchEvent is the informer target and object the Event-filter benchmark runs
// against: a core v1 Event of the shape client-go's legacy recorder writes.
var (
	benchEventInformerScope = informerScope{
		GVR:       schema.GroupVersionResource{Version: "v1", Resource: "events"},
		Namespace: "production",
	}
	benchEventInformerKey = informerKey{informerScope: benchEventInformerScope}
	benchEventGVK         = schema.GroupVersionKind{Version: "v1", Kind: "Event"}
)

func benchEventObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"type":   "Warning",
		"reason": "BackOff",
		"source": map[string]any{"component": "kubelet", "host": "node-7"},
		"involvedObject": map[string]any{
			"kind":      "Pod",
			"namespace": "production",
			"name":      "checkout-api-69dfc5f67d-ldw5j",
		},
		"count": int64(42),
	}}
	obj.SetGroupVersionKind(benchEventGVK)
	obj.SetNamespace("production")
	obj.SetName("checkout-api-69dfc5f67d-ldw5j.1830f2c9a1b2c3d4")
	return obj
}

// benchEventTable installs one interest in the Event informer, carrying the given
// merged filter set. It is separate from benchTable because the interest has to
// be keyed on the informer the benchmark's events arrive from — installed against
// the Deployment informer, every event would fan out to nobody and the benchmark
// would measure an empty loop.
func benchEventTable(b *testing.B, eventFilters []string) *interestTable {
	b.Helper()
	in, err := newScopeInterest(
		plan.TargetState{
			Key: plan.TargetKey{
				GVK:       benchEventGVK,
				Namespace: benchEventInformerKey.Namespace,
				Sink:      benchWarehouse,
			},
			RuleKeys:     []string{"StreamRule/bench"},
			EventFilters: eventFilters,
		},
		benchEventInformerScope,
	)
	if err != nil {
		b.Fatalf("newScopeInterest: %v", err)
	}
	table := newInterestTable()
	table.replace(map[interestID]*scopeInterest{in.id(): in})
	return table
}

// BenchmarkFanOutEventFilter holds the Event filter to **zero** allocations per
// event, which is the claim internal/watch/eventfilter.go makes and the reason
// its field reads are spelled out rather than delegated.
//
// It matters more here than anywhere else on this path. Events are the
// highest-volume kind in a cluster, a crash-looping namespace writes one per
// `count` bump, and a filter reads up to six fields per interest — so one heap
// allocation per field read would be six per Event per sink, on the one path
// Invariant 1 says must stay cheap.
//
// The matching and rejecting cases are both measured because they leave the
// matcher at different points, and match-all is the baseline every target for
// every kind but Event carries.
func BenchmarkFanOutEventFilter(b *testing.B) {
	warning, err := CanonicalEventFilter(EventFilterSpec{
		Types:            []string{"Warning"},
		Reasons:          []string{"BackOff", "FailedScheduling", "Unhealthy"},
		SourceComponents: []string{"kubelet"},
		SubjectKinds:     []string{"Pod"},
	})
	if err != nil {
		b.Fatalf("CanonicalEventFilter(warning): %v", err)
	}
	normal, err := CanonicalEventFilter(EventFilterSpec{Types: []string{"Normal"}})
	if err != nil {
		b.Fatalf("CanonicalEventFilter(normal): %v", err)
	}

	cases := []struct {
		name         string
		eventFilters []string
		wantEnqueue  bool
	}{
		{name: "match-all", eventFilters: nil, wantEnqueue: true},
		{name: "matches", eventFilters: []string{warning}, wantEnqueue: true},
		{name: "rejects", eventFilters: []string{normal}, wantEnqueue: false},
	}

	event := benchEventObject()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			table := benchEventTable(b, tc.eventFilters)
			queue := &countingEnqueuer{}
			p := newPool(nil, table, queue, logr.Discard())
			entry := &informerEntry{key: benchEventInformerKey, gvk: benchEventGVK}

			b.ReportAllocs()
			for b.Loop() {
				p.fanOut(entry, event, nil)
			}
			if (queue.n > 0) != tc.wantEnqueue {
				b.Fatalf("fanOut enqueued %d keys, wantEnqueue=%t; the benchmark measured the wrong path",
					queue.n, tc.wantEnqueue)
			}
		})
	}
}
