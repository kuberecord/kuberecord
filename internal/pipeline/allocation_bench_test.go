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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// These benchmarks are the allocation-diet instrument for Task 2.3. The load
// harness (test/loadgen) reports what the whole process allocates under a scale
// profile, which is the right figure to publish but the wrong one to optimize
// against: an envtest apiserver, a churn generator and a zap logger allocate in
// the same process, so a 20% improvement in the pipeline is invisible inside it.
// These pin the per-event cost of the hot path itself — allocs/op and B/op for
// exactly the work one work item does — so a fix can be proven and, later,
// prevented from silently regressing.
//
// They are benchmarks rather than tests because there is no defensible absolute
// threshold to assert: the numbers are hardware- and payload-dependent. What
// matters is the before/after comparison, which docs/perf/ records.

// benchCorpus loads the realistic objects in testdata/ as live *unstructured
// objects, in the two shapes the pipeline actually sees.
//
// Both shapes matter. In production, every object reaching Process has already
// been through the informer transform: managedFields is gone and the actors
// annotation is present (the "transformed" shape). An object can still arrive
// untransformed — a lister that installs no transform — and then normalizeObject
// has to harvest the actors and strip managedFields itself (the "raw" shape),
// which is strictly more work. Measuring only one of the two would optimize for
// the wrong one.
type benchObject struct {
	name string
	obj  *unstructured.Unstructured
}

// loadBenchCorpus builds the benchmark corpus. transformed selects the shape:
// true for the production shape (actors annotation, no managedFields), false for
// an object that still carries managedFields.
func loadBenchCorpus(b testing.TB, transformed bool) []benchObject {
	b.Helper()

	entries, err := os.ReadDir("testdata")
	if err != nil {
		b.Fatalf("reading testdata: %v", err)
	}

	var corpus []benchObject
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("testdata", entry.Name()))
		if err != nil {
			b.Fatalf("reading %s: %v", entry.Name(), err)
		}
		var content map[string]any
		if err := json.Unmarshal(raw, &content); err != nil {
			b.Fatalf("unmarshalling %s: %v", entry.Name(), err)
		}
		obj := &unstructured.Unstructured{Object: content}

		if transformed {
			annotations := obj.GetAnnotations()
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[ActorsAnnotation] = EncodeActors([]string{"argocd-controller", "kubectl-client-side-apply"})
			obj.SetAnnotations(annotations)
		} else {
			// A managedFields section shaped like a real one: two managers, each
			// with the fieldsV1 tree that makes it the largest part of an object.
			if err := unstructured.SetNestedSlice(obj.Object, benchManagedFields(), "metadata", "managedFields"); err != nil {
				b.Fatalf("installing managedFields on %s: %v", entry.Name(), err)
			}
		}

		corpus = append(corpus, benchObject{
			name: strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name())),
			obj:  obj,
		})
	}
	if len(corpus) == 0 {
		b.Fatal("testdata corpus is empty")
	}
	return corpus
}

// benchManagedFields returns a managedFields value with the shape and rough size
// of a real one, so the untransformed benchmark pays a realistic strip cost.
func benchManagedFields() []any {
	entry := func(manager string) map[string]any {
		fields := map[string]any{}
		for i := range 12 {
			fields[fmt.Sprintf("f:field-%02d", i)] = map[string]any{".": map[string]any{}}
		}
		return map[string]any{
			"manager":    manager,
			"operation":  "Apply",
			"apiVersion": "v1",
			"time":       "2026-01-01T00:00:00Z",
			"fieldsType": "FieldsV1",
			"fieldsV1": map[string]any{
				"f:metadata": map[string]any{"f:labels": fields},
				"f:spec":     map[string]any{"f:containers": fields},
			},
		}
	}
	return []any{entry("argocd-controller"), entry("kubectl-client-side-apply")}
}

// BenchmarkNormalizeObject measures the deep-copy + strip + marshal + hash
// sequence every changed object pays exactly once per work item. It is the
// single hottest function in the data plane: it runs for every event that is not
// deduplicated away, before any diff or sink hand-off happens.
func BenchmarkNormalizeObject(b *testing.B) {
	for _, shape := range []struct {
		name        string
		transformed bool
	}{
		{name: "transformed", transformed: true},
		{name: "raw-managedfields", transformed: false},
	} {
		corpus := loadBenchCorpus(b, shape.transformed)
		for _, object := range corpus {
			b.Run(shape.name+"/"+object.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := normalizeObject(object.obj, nil); err != nil {
						b.Fatalf("normalizeObject: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkNormalizeObjectDoesNotMutate is the guard that makes the benchmark
// above trustworthy: if normalizing ever mutated its argument, every iteration
// after the first would be measuring an already-stripped object (and production
// would be corrupting the shared informer cache). It asserts, per iteration, that
// the object's own serialization is unchanged.
func BenchmarkNormalizeObjectDoesNotMutate(b *testing.B) {
	corpus := loadBenchCorpus(b, false)
	object := corpus[0].obj
	before, err := json.Marshal(object.Object)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := normalizeObject(object, nil); err != nil {
			b.Fatalf("normalizeObject: %v", err)
		}
	}
	after, err := json.Marshal(object.Object)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	if string(before) != string(after) {
		b.Fatal("normalizeObject mutated its argument")
	}
}

// benchWriter is a sink.Writer that settles every job successfully and keeps
// nothing. The test fake records every record it accepts, which is right for
// assertions and wrong for a benchmark: a million retained records would swamp
// the very allocation figure being measured.
type benchWriter struct{ accepted int }

func (w *benchWriter) Enqueue(_ context.Context, job sink.Job) error {
	w.accepted++
	if job.Commit != nil {
		job.Commit(true)
	}
	return nil
}

func (w *benchWriter) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// benchRouter routes everything to one writer.
type benchRouter struct{ writer sink.Writer }

func (r benchRouter) WriterFor(sink.ID) (sink.Writer, bool) { return r.writer, true }

// benchLister serves one object for one key, swappable between iterations so
// each Process call sees genuinely changed content.
type benchLister struct {
	mu  sync.RWMutex
	obj *unstructured.Unstructured
}

func (l *benchLister) Get(Key) (*unstructured.Unstructured, bool, bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.obj, l.obj != nil, true, nil
}

func (l *benchLister) set(obj *unstructured.Unstructured) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.obj = obj
}

// benchPipeline builds a Pipeline wired to in-memory fakes and a private metrics
// registry, so a benchmark measures the pipeline and nothing else.
func benchPipeline(b *testing.B, lister ListerRegistry) *Pipeline {
	b.Helper()
	pipe, err := New(Options{
		ClusterID: "bench",
		Lister:    lister,
		Router:    benchRouter{writer: &benchWriter{}},
		Metrics:   NewPipelineMetrics(prometheus.NewRegistry()),
	})
	if err != nil {
		b.Fatalf("build pipeline: %v", err)
	}
	pipe.MarkScopeWarm("bench", ScopeKey{Kind: "Pod", Namespace: "default"})
	return pipe
}

// benchKey is the identity every Process benchmark settles.
func benchKey() Key {
	return Key{Sink: "bench", Kind: "Pod", Namespace: "default", Name: "bench-object"}
}

// benchContext carries a discarded logger. Production's logger is a real cost on
// this path (one Info line per recorded change), but it is not *this* benchmark's
// subject: the load harness measures the process with the shipped logger
// installed, while these numbers have to isolate the pipeline's own allocations.
func benchContext() context.Context {
	return logr.NewContext(context.Background(), logr.Discard())
}

// BenchmarkProcessModified is the steady-state hot path end to end: an object
// whose content changed, so the work item pays normalize, hash, cache load, diff,
// diff marshal, baseline compression, version reserve and sink hand-off. This is
// what one update to one watched object costs the operator.
func BenchmarkProcessModified(b *testing.B) {
	corpus := loadBenchCorpus(b, true)
	for _, object := range corpus {
		b.Run(object.name, func(b *testing.B) {
			// Two alternating variants, so consecutive iterations always differ
			// and never short-circuit on the dedup path.
			even := object.obj.DeepCopy()
			even.SetName("bench-object")
			even.SetNamespace("default")
			even.SetUID("bench-uid")
			odd := even.DeepCopy()
			odd.SetLabels(map[string]string{"bench-round": "odd"})

			lister := &benchLister{}
			lister.set(even)
			pipe := benchPipeline(b, lister)
			ctx := benchContext()
			key := benchKey()

			// One settled call first, so the measured iterations all take the
			// cache-hit diff path rather than the first one taking the
			// cache-miss full-state path.
			if err := pipe.Process(ctx, key); err != nil {
				b.Fatalf("warm-up Process: %v", err)
			}

			b.ReportAllocs()
			round := 0
			for b.Loop() {
				if round%2 == 0 {
					lister.set(odd)
				} else {
					lister.set(even)
				}
				round++
				if err := pipe.Process(ctx, key); err != nil {
					b.Fatalf("Process: %v", err)
				}
			}
		})
	}
}

// BenchmarkProcessDedup is the same path for an *unchanged* object — the most
// frequent outcome in a real cluster, since a resourceVersion bump that changes
// nothing hashable (a status touch, a managedFields rewrite) still delivers an
// event. It is the fixed per-work-item cost: lister read, identity key,
// normalize, hash, cache load, and out.
func BenchmarkProcessDedup(b *testing.B) {
	corpus := loadBenchCorpus(b, true)
	for _, object := range corpus {
		b.Run(object.name, func(b *testing.B) {
			obj := object.obj.DeepCopy()
			obj.SetName("bench-object")
			obj.SetNamespace("default")
			obj.SetUID("bench-uid")

			lister := &benchLister{}
			lister.set(obj)
			pipe := benchPipeline(b, lister)
			ctx := benchContext()
			key := benchKey()

			if err := pipe.Process(ctx, key); err != nil {
				b.Fatalf("warm-up Process: %v", err)
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := pipe.Process(ctx, key); err != nil {
					b.Fatalf("Process: %v", err)
				}
			}
		})
	}
}
