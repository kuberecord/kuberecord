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

// The newest-first walk's *schedule*, as distinct from its effect.
//
// shortcircuit_test.go proves the walk is correct and proves it is shorter than a
// full scan. Neither of those pins how it gets there, and the how is two decisions
// serving two different cases (see scanNewestFirst, "Why the steps widen"). The
// first step reads one partition, so the common question — a busy object, the last
// hundred changes — costs one partition rather than a cap's worth. Each step that
// fails to settle doubles up to the concurrency cap, so a sparse ninety-day archive
// does not pay one round trip per partition to discover it has nothing to say.
//
// A change that flattened the first step to the cap would still pass the ratio
// assertion in shortcircuit_test.go while turning the best case from one partition
// into eight; a change that removed the doubling would still pass it while turning
// the sparse case from five round trips into twenty-three. Both are invisible to a
// test that only asks whether the walk was shorter than the whole window, which is
// why the widths themselves are asserted here.
//
// The schedule is a trade and not a law. A maintainer who deliberately retunes it is
// meant to update this file as a decision — the failure messages say what each half
// is buying — rather than to delete it as an obstacle.

package objectsource

import (
	"slices"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// roundWidths reads the walk's step schedule back out of what the source was asked.
//
// It is an observation and not an inference from a total. A step is one scanOneGroup:
// it lists every partition of the step, in parallel, and only then fetches the objects
// under them, and the next step does not begin until it has returned. So the number of
// prefixes listed when an object is opened identifies the step that object belongs to,
// every object of a step reports the same number, and those numbers are the *running
// totals* of the widths — [1, 3, 7] for steps of [1, 2, 4].
//
// The one thing it cannot see is a step that opened nothing, because a step is only
// witnessed by its own fetches. Every fixture in this file therefore puts at least one
// object in every partition, which is checked here rather than assumed: a silently
// dropped step would make a schedule assertion pass for the wrong reason.
func roundWidths(t *testing.T, spy *spySource, wantPartitions int) []int {
	t.Helper()

	var cumulative []int
	for _, count := range spy.listCounts() {
		if len(cumulative) == 0 || count > cumulative[len(cumulative)-1] {
			cumulative = append(cumulative, count)
			continue
		}
		if count < cumulative[len(cumulative)-1] {
			t.Fatalf("an object was opened after %d listings, behind the %d of the step before it. "+
				"Steps are sequential and each one lists before it fetches, so this reading of the "+
				"schedule no longer holds: %v", count, cumulative[len(cumulative)-1], spy.listCounts())
		}
	}
	if len(cumulative) == 0 {
		t.Fatalf("the walk opened nothing, so it witnessed no step. A schedule is only observable "+
			"through the fetches each step performs, and this fixture was meant to put an object in "+
			"every one of its %d partitions", wantPartitions)
	}
	if total := cumulative[len(cumulative)-1]; total != wantPartitions {
		t.Fatalf("the walk listed %d partitions by its last fetch, want %d. Either a step opened "+
			"nothing and is invisible to this reading, or the fixture does not hold what the test "+
			"thinks it does", total, wantPartitions)
	}

	widths := make([]int, 0, len(cumulative))
	previous := 0
	for _, total := range cumulative {
		widths = append(widths, total-previous)
		previous = total
	}
	return widths
}

// TestTheNewestFirstWalkFollowsItsStepSchedule pins the two halves of the schedule
// against the two cases each of them exists for.
//
// **Best case — the first step is one partition.** A busy object whose newest
// partition already holds the answer must cost one partition, not the cap's worth
// that a walk starting wide would list and fetch. This half fails if the first step
// is ever widened; what such a change would be trading is the flagship command's own
// default invocation, which is exactly this shape.
//
// **Sparse case — the steps double.** A window of many partitions holding nothing the
// query wants must reach its end in a number of steps logarithmic in the partition
// count until the cap, and linear in cap-sized strides after it. This half fails if
// the doubling is ever removed; what such a change would be trading is the ninety-day
// question over a quiet archive, which would go back to one round trip per partition.
//
// Both cases declare NoObjectSpan, and that is a property of the *ceiling* rather
// than of the schedule. The walk may stop once it holds its limit at or above the
// newest unread instant, which is the read partition's start plus one object span —
// so with the default span, as wide as a partition, the ceiling sits at that
// partition's own end and only a straddling record can settle a first step. Switching
// the widening off is what makes "the newest partition answered it" expressible at
// all, and it is what the archive of a writer that rotates on time has earned. It
// also proves the other half of the option's contract: a deliberately-zero span
// reaches answerIsSettled as zero rather than being replaced by the default.
//
// TestAReverseLimitedScanReadsFarFewerObjects is the ratio property and stays as it
// is. It and this are different assertions and both are wanted: one says the walk is
// short, this says why it is short in the two ways it needs to be.
func TestTheNewestFirstWalkFollowsItsStepSchedule(t *testing.T) {
	t.Parallel()

	t.Run("the first step is one partition", func(t *testing.T) {
		t.Parallel()

		// Twelve hour partitions, eight changes in each, and a limit the newest
		// partition fills on its own.
		const (
			hours   = 12
			perHour = 8
		)
		history := spreadChanges(hours, perHour, 0)
		opts := Options{Prefix: "audit", Concurrency: 8, ObjectSpan: NoObjectSpan}

		q := spreadWindow(hours, perHour)
		q.Reverse = true
		q.Limit = 3

		// The reference runs against its own engine over its own copy of the archive.
		// Sharing one would leave the schedule assertions below reading a spy that had
		// also watched a full scan of the same window.
		limited, spy := engineOver(t, history, opts)
		got := drain(t, limited, q)

		full, _ := engineOver(t, history, opts)
		want, err := fullScanAnswer(t, full, q)
		if err != nil {
			t.Fatalf("the full scan failed: %v", err)
		}
		assertSameChanges(t, got, want, "the reverse-limited timeline")

		newest := spreadEpoch().Add((hours - 1) * time.Hour)
		wantPrefix := recordsRoot("audit", testRef().ClusterID) +
			dateSegment + newest.UTC().Format(dateLayout) + "/" +
			hourSegment + newest.UTC().Format(hourLayout) + "/"

		if listed := spy.listed(); !slices.Equal(listed, []string{wantPrefix}) {
			t.Fatalf("a query the newest partition answers on its own listed %v, want only %q. The "+
				"first step reads one partition because that is the whole of the common case; a "+
				"first step widened to the concurrency cap would list and fetch eight partitions "+
				"to answer a question one of them held", listed, wantPrefix)
		}
		if widths := roundWidths(t, spy, 1); !slices.Equal(widths, []int{1}) {
			t.Errorf("the walk's step widths were %v, want [1]", widths)
		}
		if opened := len(spy.opened()); opened != perHour {
			t.Errorf("the walk opened %d objects, want the newest partition's %d", opened, perHour)
		}
	})

	t.Run("the steps double up to the concurrency cap", func(t *testing.T) {
		t.Parallel()

		// Twenty-three consecutive hour partitions, one object in each, and a query
		// nothing in the window matches: the walk cannot settle and must reach the
		// oldest partition.
		//
		// Twenty-three is 1+2+4+8+8, so the schedule lands exactly on the end of the
		// window and the last step is a full one rather than a remainder — which keeps
		// the expected sequence a statement about the schedule instead of about where
		// the fixture happened to stop.
		const (
			hours       = 23
			concurrency = 8
		)
		engine, spy := engineOver(t, spreadChanges(hours, 1, 0),
			Options{Prefix: "audit", Concurrency: concurrency, ObjectSpan: NoObjectSpan})

		// From the first partition's own start, so the window resolves to exactly the
		// hours the fixture wrote: with the widening off there is no extra partition
		// below the lower bound.
		last := spreadEpoch().Add((hours-1)*time.Hour + 5*time.Minute)
		q := query.TimelineQuery{
			Ref:     testRef(),
			From:    spreadEpoch(),
			To:      last.Add(time.Minute),
			Reverse: true,
			Limit:   3,
			// Nothing in the fixture is helm's: every partition is read, none of them
			// contributes an answer row, and the walk runs to the end of the window.
			Actors: []string{actorHelm},
		}

		if got := drain(t, engine, q); len(got) != 0 {
			t.Fatalf("the filtered query returned %d changes; the fixture holds none of that "+
				"actor's, so the walk under test is no longer the one that reaches the end of its "+
				"window: %v", len(got), instantsOf(got))
		}
		if opened := len(spy.opened()); opened != hours {
			t.Fatalf("the walk opened %d objects, want one per partition (%d). Every partition holds "+
				"exactly one object so that no step is invisible to roundWidths", opened, hours)
		}

		widths := roundWidths(t, spy, hours)
		if want := []int{1, 2, 4, 8, 8}; !slices.Equal(widths, want) {
			t.Errorf("the walk's step widths over %d partitions were %v, want %v. The doubling is "+
				"what keeps a sparse window logarithmic until the cap and cap-sized after it; "+
				"without it this query is %d round trips instead of %d, which is the ninety-day "+
				"question over a quiet archive going back to one listing per partition",
				hours, widths, want, hours, len(want))
		}
	})
}

// TestADeliberatelyZeroObjectSpanReachesTheStoppingRule: the option's zero survives
// all the way to the inequality it is part of.
//
// This is the half of the ObjectSpan contract a constructor test cannot state.
// NewEngine resolving NoObjectSpan to zero is necessary and not sufficient: what the
// declaration buys is a *tighter ceiling* in answerIsSettled — lo exactly, rather than
// lo plus an hour — and an engine that resolved the option correctly and then read the
// default anywhere downstream would pass every constructor assertion and settle a
// partition later than the archive permits. So the rule is asked directly, with one
// change sitting exactly on the boundary the two spans disagree about.
func TestADeliberatelyZeroObjectSpanReachesTheStoppingRule(t *testing.T) {
	t.Parallel()

	local, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("opening a source: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := local.Close(); closeErr != nil {
			t.Errorf("closing the fixture source: %v", closeErr)
		}
	})

	lo := spreadEpoch()
	// One change and its mark, stamped at the read partition's own start: at the
	// ceiling when the span is zero, an hour below it when the span is the default.
	steps := []timelineWalkStep{{
		acc: timelineAccumulator{
			changes: []query.Change{{TS: lo, UID: uidNew, EventType: query.EventAdded}},
			marks:   []incarnationMark{{ts: lo, uid: uidNew}},
		},
	}}
	q := query.TimelineQuery{Ref: testRef(), Limit: 1, Reverse: true}

	tests := []struct {
		name       string
		span       time.Duration
		wantSettle bool
	}{
		{
			name:       "a declared no-spill archive settles on a row at the partition's start",
			span:       NoObjectSpan,
			wantSettle: true,
		},
		{
			name:       "the default span holds the same walk open for another step",
			span:       0,
			wantSettle: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine, err := NewEngine(local, Options{ObjectSpan: tc.span})
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			t.Cleanup(func() {
				if closeErr := engine.Close(); closeErr != nil {
					t.Errorf("closing the engine: %v", closeErr)
				}
			})

			if got := engine.answerIsSettled(q, steps, lo); got != tc.wantSettle {
				t.Errorf("answerIsSettled = %t, want %t. The ceiling is the read partition's start "+
					"plus the engine's object span, so a declared zero has to arrive here as zero: "+
					"replacing it with the default would keep the walk going for a partition the "+
					"archive has said it does not need", got, tc.wantSettle)
			}
		})
	}
}
