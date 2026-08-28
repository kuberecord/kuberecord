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

// The memory property, and the measurement that makes it a property rather than a
// hope: peak memory during a scan must not grow with the number of objects scanned.
//
// # What is being ruled out
//
// The obvious implementation of a cold scan decodes each object into a slice of its
// lines and filters afterwards. It returns exactly the right answer, passes every other
// test in this package, and holds the whole scanned window in memory — so a ninety-day
// query against a busy cluster is an out-of-memory kill rather than a slow answer, on a
// laptop, in the mode whose entire purpose is to be easy to try.
//
// The engine instead filters at decode time and keeps only what matched, which makes
// peak memory a function of the fetch concurrency and of the *answer* rather than of the
// archive. These fixtures are built so those two are far apart: one object out of many
// hundred names the object being asked about, and every other object carries a payload
// large enough that retaining them all would be unmistakable.
//
// # Why live heap is sampled mid-scan
//
// Total allocations grow with the number of objects under any implementation — each
// object is opened, decompressed and decoded — so B/op says nothing about this. What
// distinguishes them is how much is *live at once*, and the moment it differs most is
// the end of the scan, when a retaining implementation is holding everything it has
// read. The spy's probe is what reaches into that moment: on the last few fetches it
// forces a collection and reads the live heap, so the figure is live bytes rather than
// the sawtooth a sampler would catch mid-cycle.

package objectsource

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// wideFiller is the payload every object in a wide archive carries.
//
// Sixteen kilobytes of decoded JSON per object, which is an unremarkable Kubernetes
// object with a managed-fields history. The size is chosen so that the *linear* term
// dominates: retaining a thousand of these is sixteen megabytes, against the couple of
// megabytes a bounded number of concurrent decoders holds however wide the archive is.
// Without that separation the assertion below would be satisfied by the implementation
// it exists to reject.
//
// It compresses to almost nothing on disk, which is exactly the asymmetry a real archive
// has and the reason peak memory has to be reasoned about in decoded bytes.
var wideFiller = strings.Repeat("kuberecord-", (16<<10)/11)

// wideArchive is one object's short history buried in a partition full of other
// objects' changes.
//
// It is the shape a single-object question really has against an archive that
// partitions by time alone (there is no kind= segment by design), and the shape that
// makes "a non-matching line is discarded without being retained" measurable: all but
// matching of these lines are for other objects and must leave nothing behind.
func wideArchive(others, matching int) conformance.History {
	rows := make([]conformance.Row, 0, others+matching)
	data := func(i int) string {
		return fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d},"filler":%q}`, i, wideFiller)
	}

	for i := range others {
		ref := testRef()
		ref.Name = fmt.Sprintf("neighbour-%05d", i)
		rows = append(rows, conformance.Row{
			Ref: ref,
			Change: query.Change{
				TS:              testEpoch().Add(time.Duration(i) * time.Millisecond),
				EventType:       query.EventAdded,
				UID:             fmt.Sprintf("neighbour-uid-%05d", i),
				ResourceVersion: fmt.Sprintf("%d", i),
				APIVersion:      "apps/v1",
				Actors:          []string{actorKubectl},
				Data:            data(i),
				SHA256:          fmt.Sprintf("%064d", i),
			},
		})
	}
	for i := range matching {
		rows = append(rows, conformance.Row{
			Ref: testRef(),
			Change: query.Change{
				TS:              testEpoch().Add(time.Duration(others+i) * time.Millisecond),
				EventType:       query.EventAdded,
				UID:             uidOld,
				ResourceVersion: fmt.Sprintf("%d", others+i),
				APIVersion:      "apps/v1",
				Actors:          []string{actorKubectl},
				Data:            data(i),
				SHA256:          fmt.Sprintf("%064d", i),
			},
		})
	}
	return conformance.History{Rows: rows}
}

// matchingChanges is how many of a wide archive's objects belong to the object under
// test. Small and fixed, so the answer's own size cannot be what grows.
const matchingChanges = 4

// scanPeakHeap runs one timeline over a wide archive and returns how much live heap the
// scan added, in bytes, along with the number of changes it returned.
//
// The figure is a *delta* over a baseline taken immediately before the scan, and that is
// what makes the assertion mean anything: a test binary carries a dozen megabytes of its
// own, and comparing absolute readings would drown the very term being measured — the
// growth would be a few percent whether the engine retained everything or nothing.
//
// The archive is built and written inside this function so that nothing in the caller's
// frame keeps the fixture alive: a history of sixteen megabytes held by the test would
// be counted as the engine's own footprint.
func scanPeakHeap(t *testing.T, others int) (uint64, int) {
	t.Helper()

	engine, spy := engineOver(t, wideArchive(others, matchingChanges),
		Options{Prefix: "audit", Concurrency: DefaultConcurrency})

	total := others + matchingChanges
	var mu sync.Mutex
	var peak uint64
	spy.onOpen(func(opened int) {
		// Only the last few fetches: a forced collection per object would dominate the
		// runtime, and the end of the scan is where a retaining implementation is
		// holding the most.
		if opened < total-2 {
			return
		}
		live := liveHeap()
		mu.Lock()
		defer mu.Unlock()
		if live > peak {
			peak = live
		}
	})

	// Taken after the engine is built and the fixture has been written and dropped, so
	// what is left above it is the scan's own footprint.
	baseline := liveHeap()
	changes := drain(t, engine, wholeWindow(testRef()))
	if peak < baseline {
		return 0, len(changes)
	}
	return peak - baseline, len(changes)
}

// liveHeap forces a collection and reports the bytes still reachable.
//
// Twice, because the first collection can leave finalizable objects alive that the
// second reclaims — and the figure this is compared against has to be live bytes rather
// than a point on the allocator's sawtooth.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// TestTimelinePeakMemoryDoesNotScaleWithObjectsScanned: scanning sixteen times as many
// objects must not cost anything like sixteen times the memory.
//
// The assertion is on the *slope* — how many bytes of live heap each additional object
// scanned adds — rather than on a ratio, and that is what makes it non-vacuous. Two
// terms grow with the object count and they differ by two orders of magnitude:
//
//   - What this engine keeps: a key and an accumulator slot per object in the partition
//     group it is reading, a few hundred bytes each. It is not zero, and pretending
//     otherwise would make this test a slogan; it is released with each group, so a
//     wide window does not accumulate it.
//   - What a retaining implementation keeps: the object's decoded lines, sixteen
//     kilobytes each in this fixture. That is the implementation this test exists to
//     reject, and its slope is the payload size.
//
// The threshold sits between them, an eighth of one object's payload: comfortably above
// the bookkeeping, and unreachable by anything holding the content it has read.
func TestTimelinePeakMemoryDoesNotScaleWithObjectsScanned(t *testing.T) {
	if testing.Short() {
		t.Skip("the memory assertion writes and scans a few thousand objects")
	}

	const (
		few  = 64
		many = 1024
	)
	// An eighth of one object's decoded payload, per additional object scanned.
	perObjectBudget := uint64(len(wideFiller)) / 8

	fewPeak, fewChanges := scanPeakHeap(t, few)
	manyPeak, manyChanges := scanPeakHeap(t, many)

	if fewChanges != matchingChanges || manyChanges != matchingChanges {
		t.Fatalf("the scans returned %d and %d changes, want %d each; the answer's size is meant to be "+
			"the constant here", fewChanges, manyChanges, matchingChanges)
	}
	if fewPeak == 0 || manyPeak == 0 {
		t.Fatalf("the scan added no measurable live heap (%d and %d bytes over the baseline); the probe "+
			"fires on the last fetches of a scan, so either it no longer fires or the measurement has "+
			"stopped measuring anything", fewPeak, manyPeak)
	}
	if manyPeak < fewPeak {
		t.Fatalf("scanning %d objects added less live heap (%s) than scanning %d (%s); the measurement "+
			"is too noisy to conclude anything from", many, humanBytes(manyPeak), few,
			humanBytes(fewPeak))
	}

	perObject := (manyPeak - fewPeak) / uint64(many-few)
	t.Logf("live heap added by a scan: %s over %d objects, %s over %d — %d bytes per additional "+
		"object, against a %d-byte payload each",
		humanBytes(fewPeak), few, humanBytes(manyPeak), many, perObject, len(wideFiller))

	if perObject > perObjectBudget {
		t.Errorf("each additional object scanned added %d bytes of live heap, over a budget of %d. "+
			"An object's decoded payload is %d bytes, so a figure in that region means the scan is "+
			"keeping what it reads: a wide window is then an out-of-memory kill rather than a slow "+
			"answer, in the mode whose whole purpose is to be easy to try. A non-matching line has to "+
			"be discarded as it is decoded, not collected and filtered afterwards",
			perObject, perObjectBudget, len(wideFiller))
	}
}

// BenchmarkTimelineOverAWideArchive reports what a cold single-object scan costs against
// archives of growing width, in time and in peak live heap.
//
// The peak-heap metric is the one to read across the sub-benchmarks: it is expected to
// stay flat while the object count grows by an order of magnitude, and the time is
// expected to grow with it — which is the trade this backend declares (PointQuery false)
// rather than hides.
func BenchmarkTimelineOverAWideArchive(b *testing.B) {
	for _, objects := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("objects=%d", objects), func(b *testing.B) {
			dir := b.TempDir()
			if _, err := archiveFor(dir, objects); err != nil {
				b.Fatalf("seeding the fixture archive: %v", err)
			}
			local, err := NewLocal(dir)
			if err != nil {
				b.Fatalf("opening the fixture archive: %v", err)
			}
			b.Cleanup(func() { _ = local.Close() })
			engine, err := NewEngine(local, Options{Prefix: "audit"})
			if err != nil {
				b.Fatalf("building an engine: %v", err)
			}

			q := wholeWindow(testRef())
			var peak uint64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				it, err := engine.Timeline(context.Background(), q)
				if err != nil {
					b.Fatalf("Timeline: %v", err)
				}
				count := 0
				for it.Next() {
					count++
				}
				if err := it.Err(); err != nil {
					b.Fatalf("the scan failed: %v", err)
				}
				if err := it.Close(); err != nil {
					b.Fatalf("closing the iterator: %v", err)
				}
				if count != matchingChanges {
					b.Fatalf("the scan returned %d changes, want %d", count, matchingChanges)
				}
			}
			b.StopTimer()

			if live := liveHeap(); live > peak {
				peak = live
			}
			b.ReportMetric(float64(peak)/(1<<20), "peak-heap-MiB")
		})
	}
}

// archiveFor writes a wide archive into dir, out of line so that neither the benchmark's
// frame nor the test's keeps the fixture alive while the heap is being measured.
func archiveFor(dir string, others int) (int, error) {
	history := wideArchive(others, matchingChanges)
	if _, err := archivetest.WriteDir(dir, "audit", history); err != nil {
		return 0, err
	}
	return len(history.Rows), nil
}

// humanBytes renders a byte count for a failure message.
func humanBytes(n uint64) string {
	const mib = 1 << 20
	if n >= mib {
		return fmt.Sprintf("%.1f MiB", float64(n)/mib)
	}
	return fmt.Sprintf("%.0f KiB", float64(n)/1024)
}
