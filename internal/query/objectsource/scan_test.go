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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// manyChanges builds a history of n changes to one object, one second apart from
// testEpoch — so they all land in one hour partition and, because the fixture writes
// one object per change, in as many objects.
//
// One partition holding many objects is the shape every property in this file is
// about: the key order inside it is content-hash order, which has nothing to do with
// time, so an engine that emitted objects as it listed or fetched them would produce
// a shuffled timeline.
func manyChanges(n int) conformance.History {
	rows := make([]conformance.Row, 0, n)
	for i := range n {
		event := query.EventModified
		diff := fmt.Sprintf(`[{"op":"replace","path":"/spec/replicas","value":%d}]`, i)
		if i == 0 {
			event = query.EventAdded
			diff = ""
		}
		rows = append(rows, conformance.Row{
			Ref: testRef(),
			Change: query.Change{
				TS:              testEpoch().Add(time.Duration(i) * time.Second),
				EventType:       event,
				UID:             "11111111-1111-1111-1111-111111111111",
				ResourceVersion: fmt.Sprintf("%d", 1000+i),
				APIVersion:      "apps/v1",
				Actors:          []string{"kubectl"},
				Data:            fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, i),
				Diff:            diff,
				SHA256:          fmt.Sprintf("%064d", i),
			},
		})
	}
	return conformance.History{Rows: rows}
}

// wholeWindow brackets manyChanges with room to spare.
func wholeWindow(ref query.ObjectRef) query.TimelineQuery {
	return query.TimelineQuery{
		Ref:  ref,
		From: testEpoch().Add(-time.Minute),
		To:   testEpoch().Add(time.Hour),
	}
}

// TestTimelineFetchesInParallelUnderTheCap: objects are fetched concurrently, and
// never more than the cap at once.
//
// Both halves are the point. Without parallelism a cold scan against a remote store
// is one round trip per object and evaluation mode is unusably slow; without the cap
// a wide window opens as many objects as the window holds, and peak memory stops
// being a number anybody can reason about. The gate makes the first half
// deterministic rather than hopeful — every opened object waits for its peers, so a
// serial engine fails with a peak of one rather than by flaking.
//
// It runs under -race in CI because this is the concurrency the engine's determinism
// rests on: the accumulators are per object precisely so that no two goroutines touch
// the same one.
func TestTimelineFetchesInParallelUnderTheCap(t *testing.T) {
	t.Parallel()

	const objects = 24
	const limit = 4

	engine, spy := engineOver(t, manyChanges(objects), Options{Prefix: "audit", Concurrency: limit})
	spy.gateUntil(limit)

	changes := drain(t, engine, wholeWindow(testRef()))
	if len(changes) != objects {
		t.Fatalf("the timeline returned %d changes, want %d", len(changes), objects)
	}
	if peak := spy.peakOpen(); peak != limit {
		t.Errorf("at most %d objects were open at once, want exactly the cap of %d. Below it the scan "+
			"is serial and a cold query costs one round trip per object; above it the cap is not a "+
			"ceiling and peak memory is a function of the window rather than of the configuration",
			peak, limit)
	}
	if opened := len(spy.opened()); opened != objects {
		t.Errorf("%d objects were fetched, want %d", opened, objects)
	}
}

// TestTimelineIsOrderedDespiteConcurrentFetches: what comes back is in ts order,
// whatever order the fetches finished in.
//
// This is the "merge across concurrent readers, not a race to the iterator" property.
// The fixture's keys are content hashes, so the listing order is effectively random
// with respect to time, and an engine that appended to a shared result as each fetch
// completed would pass every count assertion and return an audit timeline with the
// effect before the cause.
func TestTimelineIsOrderedDespiteConcurrentFetches(t *testing.T) {
	t.Parallel()

	const objects = 32
	engine, _ := engineOver(t, manyChanges(objects), Options{Prefix: "audit", Concurrency: 8})

	changes := drain(t, engine, wholeWindow(testRef()))
	if len(changes) != objects {
		t.Fatalf("the timeline returned %d changes, want %d", len(changes), objects)
	}
	if !slices.IsSortedFunc(changes, byChangeTS) {
		t.Errorf("the timeline is not in ts order: %v", instantsOf(changes))
	}
	for i := range changes {
		if want := testEpoch().Add(time.Duration(i) * time.Second); !changes[i].TS.Equal(want) {
			t.Fatalf("change %d is at %s, want %s", i, formatInstant(changes[i].TS), formatInstant(want))
			return
		}
	}
}

// TestTimelineDeliversWhatItReadWhenAnObjectFails: a scan that could not read one
// object delivers the changes it did read, and reports the failure through Err.
//
// This is the shape the contract's loop is written for, and the reason the check
// after the loop is not optional. Discarding what was read would answer a question
// about an outage with nothing at all, having held the rows that explained it;
// delivering it *without* the error would present a history short by an unknown
// amount as a complete one, which is the worse of the two (Invariant 4).
func TestTimelineDeliversWhatItReadWhenAnObjectFails(t *testing.T) {
	t.Parallel()

	const objects = 6
	history := manyChanges(objects)
	dir := t.TempDir()
	layout := seedDir(t, dir, "audit", history)
	engine, spy := engineOverDir(t, dir, Options{Prefix: "audit"})

	// The objects holding the last two changes. Refusing by key rather than by
	// position is what makes the delivered count deterministic: a scan does not
	// cancel the siblings of a failed fetch, so the four before them are read whole.
	refused := errors.New("the archive refused this object")
	for _, key := range layout.RecordKeys[objects-2:] {
		spy.refuseKey(key, refused)
	}

	changes, err := drainWithErr(t, engine, wholeWindow(testRef()))
	if len(changes) != objects-2 {
		t.Errorf("the iterator delivered %d changes, want the %d it could read", len(changes), objects-2)
	}
	if !errors.Is(err, refused) {
		t.Errorf("Err = %v, want the archive's own failure. A backend that replaced it would still be "+
			"reporting a failure, but a caller matching on a sentinel to choose an exit code could no "+
			"longer tell which", err)
	}
	if !slices.IsSortedFunc(changes, byChangeTS) {
		t.Errorf("a partial result is still in ts order: %v", instantsOf(changes))
	}
}

// TestTimelineReportsAnObjectThatVanished: an object named by a listing and gone by
// the time it was fetched is reported, not swallowed.
//
// It is the most ordinary failure an archive under a lifecycle rule produces, and the
// temptation is to treat it as nothing at all. The seam's own rule is to carry on with
// a *recorded* gap, and this is the recording: every change that was readable is
// delivered, and Err says the result is short by whatever that object held. Silence
// here would make an expired partition indistinguishable from a quiet one.
func TestTimelineReportsAnObjectThatVanished(t *testing.T) {
	t.Parallel()

	const objects = 5
	history := manyChanges(objects)
	dir := t.TempDir()
	layout := seedDir(t, dir, "audit", history)
	engine, spy := engineOverDir(t, dir, Options{Prefix: "audit"})

	gone := layout.RecordKeys[objects-1]
	spy.refuseKey(gone, fmt.Errorf("%w: %q", ErrKeyNotFound, gone))

	changes, err := drainWithErr(t, engine, wholeWindow(testRef()))
	if len(changes) != objects-1 {
		t.Errorf("the iterator delivered %d changes, want %d", len(changes), objects-1)
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Err = %v, want it to wrap ErrKeyNotFound so a caller can say the archive aged out "+
			"from under the scan rather than that the archive is broken", err)
	}
	if !containsAll(err.Error(), "listed", "gone") {
		t.Errorf("Err reads %q; it has to say the object was listed and then gone, or a reader will "+
			"take it for a mistyped key", err)
	}
}

// TestTimelineAbortsWhenAPartitionCannotBeListed: a listing failure abandons the
// query instead of answering from the partitions that did list.
//
// The asymmetry with a failed *object* is deliberate. A failed object is a known hole
// of known size — the scan can say "this is short by that object". A failed listing is
// a hole of unknown size: nothing knows what was in the partition, so an answer built
// from the others would report "nothing changed" for a window that was never read.
func TestTimelineAbortsWhenAPartitionCannotBeListed(t *testing.T) {
	t.Parallel()

	engine, spy := engineOver(t, manyChanges(3), Options{Prefix: "audit"})
	spy.mu.Lock()
	spy.listErr = errors.New("the archive refused the listing")
	spy.mu.Unlock()

	changes, err := drainWithErr(t, engine, wholeWindow(testRef()))
	if len(changes) != 0 {
		t.Errorf("the iterator delivered %d changes over a partition it could not list", len(changes))
	}
	if err == nil {
		t.Fatal("a listing that failed was reported as an empty timeline; an unread partition and a " +
			"quiet one must not look the same (Invariant 4)")
	}
	if !containsAll(err.Error(), "listing") {
		t.Errorf("Err reads %q, and does not say a listing failed", err)
	}
}

// TestTheReportedFailureIsTheFirstInKeyOrder: with several unreadable objects, the
// one reported is the same one every time.
//
// The obvious implementation reports whichever fetch failed first, which is a
// function of goroutine scheduling: the message then changes between runs of the same
// query over the same archive, and nobody can search for it or paste it into an issue.
func TestTheReportedFailureIsTheFirstInKeyOrder(t *testing.T) {
	t.Parallel()

	const objects = 8
	history := manyChanges(objects)
	dir := t.TempDir()
	layout := seedDir(t, dir, "audit", history)
	engine, spy := engineOverDir(t, dir, Options{Prefix: "audit", Concurrency: objects})

	// Two failures, distinguishable, on the two objects whose keys sort first and
	// last among the refused set.
	refused := slices.Clone(layout.RecordKeys)
	slices.Sort(refused)
	first := errors.New("the object whose key sorts first")
	last := errors.New("the object whose key sorts last")
	spy.refuseKey(refused[0], first)
	spy.refuseKey(refused[len(refused)-1], last)

	for range 5 {
		_, err := drainWithErr(t, engine, wholeWindow(testRef()))
		if !errors.Is(err, first) {
			t.Fatalf("Err = %v, want the failure of the object whose key sorts first (%v). A scan that "+
				"reported whichever fetch lost the race would describe the same archive differently "+
				"from one run to the next", err, first)
		}
	}
}

// TestTimelineOverAnEmptyArchiveIsAnEmptyResult: an archive with no objects in the
// window is an empty timeline and not a failure.
//
// "Nothing is recorded here" and "this could not be read" are different answers and a
// caller has to be able to tell them apart — which, for the first of the two, means
// Coverage rather than an error (Invariant 9).
func TestTimelineOverAnEmptyArchiveIsAnEmptyResult(t *testing.T) {
	t.Parallel()

	engine, _ := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})

	changes, err := drainWithErr(t, engine, wholeWindow(testRef()))
	if err != nil {
		t.Errorf("an empty archive reported %v; emptiness is a result, not a failure", err)
	}
	if len(changes) != 0 {
		t.Errorf("an empty archive returned %d changes", len(changes))
	}
}

// instantsOf renders a result's timestamps for a failure message about ordering.
func instantsOf(changes []query.Change) []string {
	out := make([]string, 0, len(changes))
	for _, c := range changes {
		out = append(out, formatInstant(c.TS))
	}
	return out
}
