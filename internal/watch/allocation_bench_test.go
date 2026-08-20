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

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/plan"
	"github.com/yelzhy/kuberecord/internal/sink"
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

// benchInformerKey is the (GVR, namespace) target every benchmark's events arrive
// from.
var benchInformerKey = informerKey{
	GVR:       schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
	Namespace: "",
}

var benchGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

// benchWarehouse is the sink the benchmarks' interests are installed for, and
// benchUnknown one that never is — the miss path.
var (
	benchWarehouse = clickHouseSink("warehouse")
	benchUnknown   = clickHouseSink("unknown")
)

// benchTable installs one interest per sink, each with the given selectors (nil
// meaning "match everything", which is what a rule with no selector produces and
// the overwhelmingly common shape in practice).
func benchTable(b *testing.B, sinks []sink.ID, selectors []string) *interestTable {
	b.Helper()
	table := newInterestTable()
	desired := make(map[interestID]*scopeInterest, len(sinks))
	for _, sinkID := range sinks {
		in, err := newScopeInterest(
			plan.TargetKey{GVK: benchGVK, Namespace: "", Sink: sinkID},
			benchInformerKey,
			selectors,
			nil,
			[]string{"ClusterStreamRule/bench"},
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
			table := benchTable(b, []sink.ID{benchWarehouse}, tc.selectors)
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
			table := benchTable(b, []sink.ID{benchWarehouse}, nil)
			b.ReportAllocs()
			for b.Loop() {
				table.lookupIdentity(tc.ref)
			}
		})
	}
}
