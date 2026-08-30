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

package objectsource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// progressRecorder is a caller of the read plane's progress half, written the way
// a real one has to be: the callback runs on the scan's own fetch goroutines, so
// it is guarded, and the whole sequence is kept so a test can assert on the shape
// of the reporting rather than only on its total.
type progressRecorder struct {
	mu   sync.Mutex
	seen []query.ScanProgress
}

func (r *progressRecorder) report(progress query.ScanProgress) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, progress)
}

func (r *progressRecorder) all() []query.ScanProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]query.ScanProgress(nil), r.seen...)
}

func (r *progressRecorder) last() (query.ScanProgress, bool) {
	all := r.all()
	if len(all) == 0 {
		return query.ScanProgress{}, false
	}
	return all[len(all)-1], true
}

// TestScanProgressCountsWhatTheScanFetched pins the two figures a caller renders
// against an estimate.
//
// They have to be counted the way the estimate counts them — stored objects and
// stored bytes — or the progress line finishes at sixty per cent against its own
// denominator. So the assertion is not "some progress was reported": it is that
// the final report equals the estimate for a scan that read the whole window.
func TestScanProgressCountsWhatTheScanFetched(t *testing.T) {
	t.Parallel()

	const objects = 12
	engine, spy := engineOver(t, manyChanges(objects), Options{Prefix: "audit"})

	var recorder progressRecorder
	engine.SetScanProgress(recorder.report)

	from, to := testEpoch().Add(-time.Minute), testEpoch().Add(time.Hour)
	estimate, err := engine.EstimateScan(context.Background(), testRef().ClusterID, from, to)
	if err != nil {
		t.Fatalf("EstimateScan: %v", err)
	}
	// The estimate opens nothing, so it must report nothing: a progress line that
	// jumped before the scan began would be describing the warning rather than the
	// work.
	if reported := recorder.all(); len(reported) != 0 {
		t.Errorf("estimating reported %d progress updates, want none — it opens no object", len(reported))
	}

	q := wholeWindow(testRef())
	q.To = to
	if changes := drain(t, engine, q); len(changes) != objects {
		t.Fatalf("the scan returned %d changes, want %d", len(changes), objects)
	}

	reported := recorder.all()
	if got, want := len(reported), len(spy.opened()); got != want {
		t.Errorf("%d progress updates for %d fetched objects; the contract is one per object",
			got, want)
	}
	final, ok := recorder.last()
	if !ok {
		t.Fatal("the scan reported no progress at all, which is indistinguishable from a hang")
	}
	if final.Objects != estimate.Objects {
		t.Errorf("the scan reported %d objects but the estimate promised %d; a caller renders one "+
			"against the other, so they have to count the same things",
			final.Objects, estimate.Objects)
	}
	if final.Bytes != estimate.Bytes {
		t.Errorf("the scan reported %d bytes but the estimate promised %d; both are stored bytes, "+
			"which is the only figure a listing can supply", final.Bytes, estimate.Bytes)
	}
	assertMonotonic(t, reported)
}

// TestScanProgressRestartsWithEachScan is the reset the contract promises.
//
// One timeline costs this engine two passes over the same partitions — the
// incarnations, then the changes — and a caller rendering a running total against
// a one-window estimate would watch its progress run past a hundred per cent and
// stop believing the number.
func TestScanProgressRestartsWithEachScan(t *testing.T) {
	t.Parallel()

	const objects = 6
	engine, _ := engineOver(t, manyChanges(objects), Options{Prefix: "audit"})

	var first progressRecorder
	engine.SetScanProgress(first.report)

	from, to := testEpoch().Add(-time.Minute), testEpoch().Add(time.Hour)
	q := wholeWindow(testRef())
	q.To = to
	drain(t, engine, q)

	if final, ok := first.last(); !ok || final.Objects != objects {
		t.Fatalf("the first pass reported %+v, want %d objects", final, objects)
	}

	var second progressRecorder
	engine.SetScanProgress(second.report)
	if _, err := engine.Incarnations(context.Background(), testRef(), from, to); err != nil {
		t.Fatalf("Incarnations: %v", err)
	}

	reported := second.all()
	if len(reported) == 0 {
		t.Fatal("listing the incarnations is a scan of the same partitions and reported nothing")
	}
	if reported[0].Objects != 1 {
		t.Errorf("the second scan's first report counts %d objects, want 1: the figures are per scan, "+
			"not per engine", reported[0].Objects)
	}
	if final := reported[len(reported)-1]; final.Objects != objects {
		t.Errorf("the second scan reported %d objects, want the %d it fetched", final.Objects, objects)
	}
}

// TestScanProgressStopsWhenItIsRemoved covers the half of the contract a caller
// depends on for correctness rather than for looks: the closure paints a terminal
// line, and one that outlived its command would paint over the next one.
func TestScanProgressStopsWhenItIsRemoved(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, manyChanges(4), Options{Prefix: "audit"})

	var recorder progressRecorder
	engine.SetScanProgress(recorder.report)
	engine.SetScanProgress(nil)

	q := wholeWindow(testRef())
	q.To = testEpoch().Add(time.Hour)
	drain(t, engine, q)

	if reported := recorder.all(); len(reported) != 0 {
		t.Errorf("%d progress updates arrived after the callback was removed", len(reported))
	}
}

// TestScanProgressReportsFromEveryFetch is the race assertion.
//
// The callback is invoked from the goroutines performing the scan — that is the
// documented contract, and the reason it is documented is that a caller writing an
// unguarded closure would be fine at a concurrency of one and corrupt at eight.
// Under -race this fails if the counters are ever read or written unguarded.
func TestScanProgressReportsFromEveryFetch(t *testing.T) {
	t.Parallel()

	const objects = 32
	engine, _ := engineOver(t, manyChanges(objects), Options{Prefix: "audit", Concurrency: 8})

	var recorder progressRecorder
	engine.SetScanProgress(recorder.report)

	q := wholeWindow(testRef())
	q.To = testEpoch().Add(time.Hour)
	drain(t, engine, q)

	reported := recorder.all()
	if len(reported) != objects {
		t.Fatalf("%d progress updates for %d objects", len(reported), objects)
	}
	assertMonotonic(t, reported)
}

// assertMonotonic checks the one ordering guarantee the contract makes.
//
// The two figures are not a consistent snapshot of each other — eight parallel
// fetches make that impossible without a lock nobody should pay for — but each of
// them only ever grows, which is what stops a progress line from going backwards.
func assertMonotonic(t *testing.T, reported []query.ScanProgress) {
	t.Helper()

	var objects, bytes int64
	for i, progress := range reported {
		if progress.Objects < objects || progress.Bytes < bytes {
			t.Fatalf("report %d went backwards: %+v after objects=%d bytes=%d",
				i, progress, objects, bytes)
		}
		objects, bytes = progress.Objects, progress.Bytes
	}
}
