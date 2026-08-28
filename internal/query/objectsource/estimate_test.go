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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// TestEstimateScanOpensNothing is the requirement that makes the estimate worth
// rendering.
//
// It exists to be shown *before* a scan, so one that fetched even a sample would charge
// a fraction of the scan for the warning about the scan — and a warning that costs
// something is a warning people learn to skip. Every figure therefore comes from the
// listing, which already reports sizes.
func TestEstimateScanOpensNothing(t *testing.T) {
	t.Parallel()

	const objects = 12
	engine, spy := engineOver(t, manyChanges(objects), Options{Prefix: "audit"})

	from, to := testEpoch().Add(-time.Minute), testEpoch().Add(time.Hour)
	got, err := engine.EstimateScan(context.Background(), testRef().ClusterID, from, to)
	if err != nil {
		t.Fatalf("EstimateScan: %v", err)
	}
	if opened := spy.opened(); len(opened) != 0 {
		t.Errorf("estimating fetched %d objects: %v", len(opened), opened)
	}
	if got.Objects != objects {
		t.Errorf("the estimate counts %d objects, want %d", got.Objects, objects)
	}
	if want := storedBytes(t, spy); got.Bytes != want {
		t.Errorf("the estimate is %d stored bytes, want %d — the figure has to be the bytes off the "+
			"wire, which is what predicts the wait", got.Bytes, want)
	}
	// The partition count is reported so a caller can say *why* an estimate is large — a
	// wide window rather than a busy cluster — and so a pruning regression shows up in
	// output rather than only in a timing.
	if want := len(spy.listed()); got.Partitions != want {
		t.Errorf("the estimate reports %d partitions, want the %d it listed", got.Partitions, want)
	}
}

// TestEstimateScanDescribesTheScanThatWillRun: the widening is applied to the estimate
// too.
//
// An estimate that quietly described a smaller scan than the one about to happen would
// be worse than none, because it would be believed — and the widening is exactly the
// part a reader would not think to add.
func TestEstimateScanDescribesTheScanThatWillRun(t *testing.T) {
	t.Parallel()

	from, to := testEpoch(), testEpoch().Add(time.Hour)

	narrow, narrowSpy := engineOverDir(t, t.TempDir(), Options{Prefix: "audit", ObjectSpan: NoObjectSpan})
	wide, wideSpy := engineOverDir(t, t.TempDir(), Options{Prefix: "audit", ObjectSpan: DefaultObjectSpan})

	narrowEstimate, err := narrow.EstimateScan(context.Background(), testRef().ClusterID, from, to)
	if err != nil {
		t.Fatalf("EstimateScan without widening: %v", err)
	}
	wideEstimate, err := wide.EstimateScan(context.Background(), testRef().ClusterID, from, to)
	if err != nil {
		t.Fatalf("EstimateScan with widening: %v", err)
	}

	if wideEstimate.Partitions != narrowEstimate.Partitions+1 {
		t.Errorf("widening by one hour changed the partition count from %d to %d, want one more",
			narrowEstimate.Partitions, wideEstimate.Partitions)
	}
	if got, want := len(wideSpy.listed()), wideEstimate.Partitions; got != want {
		t.Errorf("the estimate reports %d partitions but listed %d", want, got)
	}
	if got, want := len(narrowSpy.listed()), narrowEstimate.Partitions; got != want {
		t.Errorf("the estimate reports %d partitions but listed %d", want, got)
	}
}

// TestEstimateScanRefusesAnUnboundedWindow: there is nothing to estimate without a
// window, and saying so beats reporting the size of the whole archive as though it were
// the cost of the query.
func TestEstimateScanRefusesAnUnboundedWindow(t *testing.T) {
	t.Parallel()

	engine, _ := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})
	if _, err := engine.EstimateScan(context.Background(), testRef().ClusterID,
		time.Time{}, time.Time{}); !errors.Is(err, query.ErrTimeBoundRequired) {
		t.Errorf("EstimateScan = %v, want query.ErrTimeBoundRequired", err)
	}
}

// TestEstimateScanOverAnEmptyArchiveIsZero: an empty archive is a zero estimate and not
// a failure.
//
// "Nothing to read" and "could not be read" are different answers, and this is the one
// place a caller sees them before it has read anything at all — so the distinction has
// to survive here too (Invariant 4).
func TestEstimateScanOverAnEmptyArchiveIsZero(t *testing.T) {
	t.Parallel()

	engine, _ := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})
	got, err := engine.EstimateScan(context.Background(), testRef().ClusterID,
		testEpoch(), testEpoch().Add(time.Hour))
	if err != nil {
		t.Fatalf("EstimateScan over an empty archive: %v", err)
	}
	if got.Objects != 0 || got.Bytes != 0 {
		t.Errorf("an empty archive estimated %+v", got)
	}
	if got.Partitions == 0 {
		t.Error("an empty archive reported no partitions; the window still resolves to a range, and " +
			"a caller showing \"0 objects across 2 partitions\" is telling the truth about both")
	}
}

// TestEstimateScanReportsAnUnreadableListing: a listing that failed is reported rather
// than estimated around.
//
// A zero estimate would be read as "this is cheap", and the scan behind it would then
// fail anyway — having promised otherwise.
func TestEstimateScanReportsAnUnreadableListing(t *testing.T) {
	t.Parallel()

	engine, spy := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})
	spy.mu.Lock()
	spy.listErr = errors.New("the archive refused the listing")
	spy.mu.Unlock()

	if _, err := engine.EstimateScan(context.Background(), testRef().ClusterID,
		testEpoch(), testEpoch().Add(time.Hour)); err == nil {
		t.Error("EstimateScan reported a figure over a partition it could not list")
	}
}

// storedBytes adds up the archive on disk, so the estimate is compared against the
// bytes that really exist rather than against a second reading of the listing.
func storedBytes(t *testing.T, spy *spySource) int64 {
	t.Helper()

	local, ok := spy.inner.(*Local)
	if !ok {
		t.Fatalf("the fixture source is %T, not a local archive", spy.inner)
	}
	var total int64
	err := filepath.WalkDir(local.root.Name(), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("measuring the fixture archive: %v", err)
	}
	return total
}
