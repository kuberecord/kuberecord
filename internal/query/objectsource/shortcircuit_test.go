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

// The newest-first walk: the one query shape whose *reading* a limit can bound, and
// the four ways getting it wrong is invisible.
//
// A reverse-limited timeline — "the last hundred things that happened", which is the
// command-line client's own default — names a suffix of the window, and partitions are
// ordered, so the scan can stop once it can prove no unread partition holds anything
// newer. The optimisation is worth having: ninety days read as three partitions instead
// of two thousand is the difference between evaluation mode being usable and being a
// demo.
//
// It is also the one place in this phase where a fast wrong answer is available, and a
// wrong answer here does not look wrong. It looks like a timeline. So the properties
// below are, in order: that the short circuit changes no result, that it actually
// shortens the scan, that it survives the partition boundary not being a change
// boundary, and that a filter cannot trick it into stopping early with too few rows.

package objectsource

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// spreadEpoch is the hour the spread fixtures start in — testEpoch's own hour, floored,
// so that a change lands at a stated offset into a stated partition.
func spreadEpoch() time.Time { return hourStart(testEpoch()) }

// spreadChanges builds one object's history across hours consecutive hour partitions,
// perHour changes in each.
//
// Many partitions is the shape the walk is about, and the fixture writes one object per
// change, so "how many partitions did it read" and "how many objects did it open" are
// both observable. The actors alternate on a stride so that a filtered query has
// matches spread through the window rather than clustered at one end — see
// TestAFilteredReverseLimitedScanKeepsWalkingToFillItsLimit.
func spreadChanges(hours, perHour, helmStride int) conformance.History {
	rows := make([]conformance.Row, 0, hours*perHour)
	n := 0
	for h := range hours {
		for j := range perHour {
			actor := actorKubectl
			if helmStride > 0 && n%helmStride == 0 {
				actor = actorHelm
			}
			rows = append(rows, conformance.Row{
				Ref: testRef(),
				Change: query.Change{
					TS: spreadEpoch().Add(time.Duration(h)*time.Hour +
						time.Duration(j+1)*5*time.Minute),
					EventType:       query.EventAdded,
					UID:             uidNew,
					ResourceVersion: fmt.Sprintf("%d", 1000+n),
					APIVersion:      "apps/v1",
					Actors:          []string{actor},
					Data:            fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, n),
					SHA256:          fmt.Sprintf("%064d", n),
				},
			})
			n++
		}
	}
	return conformance.History{Rows: rows}
}

// spreadWindow brackets a spread history, tightly enough that the partition range is
// the hours the fixture actually used.
func spreadWindow(hours, perHour int) query.TimelineQuery {
	last := spreadEpoch().Add(time.Duration(hours-1)*time.Hour + time.Duration(perHour)*5*time.Minute)
	return query.TimelineQuery{
		Ref:  testRef(),
		From: spreadEpoch().Add(-time.Minute),
		To:   last.Add(time.Minute),
	}
}

// fullScanAnswer is what the same question costs without the short circuit: the whole
// window read forwards, reversed, and cut to the limit by the caller.
//
// It is derived from an *unlimited* reverse query rather than from a flag on the engine,
// because a test-only switch would be a second implementation of the thing under test —
// and the property is precisely that the two paths agree, so one of them has to be the
// path a real caller takes.
func fullScanAnswer(t *testing.T, engine *Engine, q query.TimelineQuery) ([]query.Change, error) {
	t.Helper()

	unlimited := q
	unlimited.Limit = 0
	all, err := drainWithErr(t, engine, unlimited)
	if q.Limit > 0 && len(all) > q.Limit {
		all = all[:q.Limit]
	}
	return all, err
}

// assertSameChanges compares two answers row by row, on content and not only on
// instants.
//
// The distinction earns its keep here: the short circuit reads a *different set of
// objects* from the full scan, so a row that came back attributed to the wrong
// incarnation, or carrying another observation's state, would still be at the right
// instant and in the right position. Comparing timestamps alone would let all of that
// through.
func assertSameChanges(t *testing.T, got, want []query.Change, what string) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("%s returned %d changes, want the %d the full scan returned:\n got: %v\nwant: %v",
			what, len(got), len(want), instantsOf(got), instantsOf(want))
		return
	}
	for i := range want {
		if !sameChange(got[i], want[i]) {
			t.Errorf("%s differs from the full scan at row %d:\n got: %s\nwant: %s",
				what, i, describeChange(got[i]), describeChange(want[i]))
			return
		}
	}
}

// sameChange reports whether two rows are the same recorded observation.
func sameChange(a, b query.Change) bool {
	return a.TS.Equal(b.TS) && a.UID == b.UID && a.ResourceVersion == b.ResourceVersion &&
		a.EventType == b.EventType && a.SHA256 == b.SHA256 && a.Data == b.Data &&
		a.Diff == b.Diff && slices.Equal(a.Actors, b.Actors)
}

// describeChange renders a row for a failure message: the fields that identify it,
// without the state, which for some fixtures is kilobytes.
func describeChange(c query.Change) string {
	return fmt.Sprintf("%s %s uid=%s rv=%s actors=%v sha=%s…",
		formatInstant(c.TS), c.EventType, c.UID, c.ResourceVersion, c.Actors, c.SHA256[:min(8, len(c.SHA256))])
}

// TestAReverseLimitedScanAnswersExactlyAsTheFullScanDoes: the short circuit is
// invisible in the answer.
//
// This is the acceptance criterion that matters. A partial walk resolves the
// incarnation, applies the predicates, merges the commentary and orders the result from
// a *subset* of the window, and every one of those steps has a reading that is right
// over the whole window and wrong over a suffix of it. None of them fails loudly: what
// comes back is a timeline, correctly ordered, plausible, and about the wrong rows.
//
// So the table is deliberately not a list of easy shapes. It carries the two
// incarnations that make resolution order matter, the commentary that is merged after
// resolution, the filters that change how many rows a limit finds, and the pinned UID
// that bypasses resolution entirely.
func TestAReverseLimitedScanAnswersExactlyAsTheFullScanDoes(t *testing.T) {
	t.Parallel()

	const (
		hours     = 10
		perHour   = 3
		helmEvery = 4
	)
	history := spreadChanges(hours, perHour, helmEvery)
	// A second incarnation, older, under the same name: resolving from a partial view
	// would answer with its history under the living object's name (Invariant 7).
	for i, row := range history.Rows[:perHour] {
		row.Change.UID = uidOld
		row.Change.ResourceVersion = fmt.Sprintf("old-%d", i)
		row.Change.Actors = []string{actorHelm}
		history.Rows[i] = row
	}
	history.Rows = append(history.Rows,
		eventRow(20*time.Minute, "", "checkout.core", "ScalingReplicaSet", uidNew),
		eventRow(6*time.Hour, "events.k8s.io", "checkout.next", "FailedCreate", uidNew),
	)

	engine, _ := engineOver(t, history, Options{Prefix: "audit", Concurrency: 4})
	base := spreadWindow(hours, perHour)

	tests := []struct {
		name string
		with func(q *query.TimelineQuery)
	}{
		{name: "the newest incarnation", with: func(*query.TimelineQuery) {}},
		{name: "every incarnation", with: func(q *query.TimelineQuery) { q.AllIncarnations = true }},
		{name: "pinned to the older incarnation", with: func(q *query.TimelineQuery) { q.UID = uidOld }},
		{name: "pinned to the newer incarnation", with: func(q *query.TimelineQuery) { q.UID = uidNew }},
		{name: "with commentary", with: func(q *query.TimelineQuery) { q.IncludeEvents = true }},
		{
			name: "with commentary over every incarnation",
			with: func(q *query.TimelineQuery) { q.IncludeEvents, q.AllIncarnations = true, true },
		},
		{name: "filtered to one actor", with: func(q *query.TimelineQuery) { q.Actors = []string{actorHelm} }},
		{
			name: "filtered to one actor over every incarnation",
			with: func(q *query.TimelineQuery) {
				q.Actors, q.AllIncarnations = []string{actorHelm}, true
			},
		},
		{
			name: "excluding an actor",
			with: func(q *query.TimelineQuery) { q.ExcludeActors = []string{actorKubectl} },
		},
		{
			name: "filtered to a field path",
			with: func(q *query.TimelineQuery) { q.FieldPaths = []string{"/spec/replicas"} },
		},
	}

	// Every limit from "one row" up to "more rows than the window holds", because the
	// stopping rule is a comparison against the limit and the interesting values are at
	// its edges: a limit of one settles on the first partition that holds anything, and
	// a limit larger than the answer must never settle at all.
	for _, limit := range []int{1, 2, 7, hours * perHour * 2} {
		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s/limit=%d", tc.name, limit), func(t *testing.T) {
				t.Parallel()

				q := base
				tc.with(&q)
				q.Reverse = true

				q.Limit = limit
				short, shortErr := drainWithErr(t, engine, q)
				want, wantErr := fullScanAnswer(t, engine, q)

				assertSameChanges(t, short, want, "the reverse-limited timeline")
				if (shortErr == nil) != (wantErr == nil) {
					t.Errorf("the reverse-limited timeline reported %v and the full scan reported %v; "+
						"over a readable archive the two paths read different objects but must reach "+
						"the same conclusion about whether the answer is complete", shortErr, wantErr)
				}
			})
		}
	}
}

// TestAReverseLimitedScanReadsFarFewerObjects: the short circuit is not merely correct,
// it is the reason the phase has a story.
//
// Without this the optimisation is a comment. With it, the gap between "read the window
// and throw most of it away" and "read the newest partitions and stop" is measured on a
// counting source, and it is the gap Task 11.3's default invocation lives in.
func TestAReverseLimitedScanReadsFarFewerObjects(t *testing.T) {
	t.Parallel()

	const (
		hours   = 12
		perHour = 8
		objects = hours * perHour
	)
	history := spreadChanges(hours, perHour, 0)
	base := spreadWindow(hours, perHour)

	limited, limitedSpy := engineOver(t, history, Options{Prefix: "audit", Concurrency: 4})
	q := base
	q.Reverse = true
	q.Limit = 3
	short := drain(t, limited, q)

	full, fullSpy := engineOver(t, history, Options{Prefix: "audit", Concurrency: 4})
	want, err := fullScanAnswer(t, full, q)
	if err != nil {
		t.Fatalf("the full scan failed: %v", err)
	}
	assertSameChanges(t, short, want, "the reverse-limited timeline")

	opened, wholeScan := len(limitedSpy.opened()), len(fullSpy.opened())
	if wholeScan != objects {
		t.Fatalf("the full scan opened %d objects, want the whole fixture's %d; the comparison below "+
			"means nothing if the baseline is not a whole scan", wholeScan, objects)
	}
	t.Logf("the reverse-limited scan opened %d objects and listed %d partitions, against %d and %d "+
		"for the full scan", opened, len(limitedSpy.listed()), wholeScan, len(fullSpy.listed()))

	if opened*2 > wholeScan {
		t.Errorf("a reverse-limited scan opened %d of the window's %d objects. Partitions are ordered, "+
			"so a walk from the newest end can stop once nothing unread could be newer — and the "+
			"flagship command's default is exactly this query. Reading the window and discarding it "+
			"is the difference between evaluation mode feeling usable and feeling broken",
			opened, wholeScan)
	}
}

// TestAStraddlingRecordIsNotLostToTheShortCircuit: a partition boundary is not a change
// boundary, and the stopping rule knows it.
//
// An object's partition comes from its *first* record and it keeps accepting records
// until it rotates, so a record stamped 18:30 can live in the hour=17 object. That is
// docs/SCHEMA.md's own warning and it is the reason partitionSpans widens a window
// downward by an object span; the newest-first walk needs the same widening in the same
// direction, applied to its ceiling.
//
// The naive rule — stop once the limit is filled from partitions at or above the last
// one read — passes every other test in this file and returns the wrong two rows here.
// The fixture is built so that it does: the newest changes by instant sit in a
// partition the naive rule would never reach.
func TestAStraddlingRecordIsNotLostToTheShortCircuit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	hourSeventeen := spreadEpoch().Add(10 * time.Hour) // 17:00
	hourEighteen := hourSeventeen.Add(time.Hour)

	// Three ordinary changes in hour=18, each in its own object, and one object in
	// hour=17 that opened at 17:45 and was still accepting records at 18:30 — inside
	// the writer's one-hour rotation ceiling, so this is an archive an operator really
	// produces rather than a shape invented for the test.
	//
	// Every object here is placed by hand rather than through archivetest, which derives
	// a key from the record's own timestamp. That derivation is right for every other
	// fixture and cannot express the one thing this test is about, so the placement is
	// spelled out for the ordinary objects too — otherwise half the fixture's layout
	// would be implicit and half explicit, and a reader could not tell which half the
	// property rests on.
	for i, at := range []time.Duration{5 * time.Minute, 10 * time.Minute, 15 * time.Minute} {
		writeFrameAt(t, dir, keyForHour("audit", hourEighteen, fmt.Sprintf("ordinary-%d", i)),
			[]query.Change{spreadRow(hourEighteen.Add(at), i+1).Change})
	}
	writeFrameAt(t, dir, keyForHour("audit", hourSeventeen, "straddler"),
		[]query.Change{
			spreadRow(hourSeventeen.Add(45*time.Minute), 4).Change,
			spreadRow(hourEighteen.Add(30*time.Minute), 5).Change,
		})

	engine, _ := engineOverDir(t, dir, Options{Prefix: "audit"})
	q := query.TimelineQuery{
		Ref:     testRef(),
		From:    hourSeventeen.Add(-time.Hour),
		To:      hourEighteen.Add(59 * time.Minute),
		Reverse: true,
		Limit:   2,
	}

	got := drain(t, engine, q)
	want, err := fullScanAnswer(t, engine, q)
	if err != nil {
		t.Fatalf("the full scan failed: %v", err)
	}
	assertSameChanges(t, got, want, "the reverse-limited timeline over a straddling object")

	newest := hourEighteen.Add(30 * time.Minute)
	if len(got) == 0 || !got[0].TS.Equal(newest) {
		t.Fatalf("the newest change came back as %v, want %s — the record in the hour=%s object that "+
			"belongs to the hour after it. A walk that stopped as soon as its limit was filled from "+
			"the partitions it had read would return the two rows before it, in order, plausibly, and "+
			"with the actual newest change missing",
			instantsOf(got), formatInstant(newest), hourSeventeen.Format(hourLayout))
	}
}

// TestAFilteredReverseLimitedScanKeepsWalkingToFillItsLimit: a predicate that excludes
// most rows makes the walk longer, not the answer shorter.
//
// The stopping rule counts what the *answer* will hold, and the near miss is specific.
// Every line naming the object leaves an incarnationMark, recorded before any predicate
// runs — it has to be, because the incarnation is resolved before filtering (Invariant
// 7). The marks are therefore the obvious thing to count, they are already in hand for
// the settledness check above them, and counting them is wrong by exactly the
// predicate's selectivity.
//
// The query that exposes it is the one a person actually types at 02:47: "what did helm
// do to this", where nine rows in ten are dropped. Counting marks settles inside the
// newest partition and returns one of the three matching changes, in order, with
// nothing in the output to say the other two exist.
func TestAFilteredReverseLimitedScanKeepsWalkingToFillItsLimit(t *testing.T) {
	t.Parallel()

	const (
		hours   = 10
		perHour = 4
		// Helm touched the object once every ten changes, so filling a limit of three
		// takes most of the window however the walk is paced.
		helmEvery = 10
	)
	history := spreadChanges(hours, perHour, helmEvery)
	engine, spy := engineOver(t, history, Options{Prefix: "audit", Concurrency: 4})

	q := spreadWindow(hours, perHour)
	q.Reverse = true
	q.Limit = 3
	q.Actors = []string{actorHelm}

	got := drain(t, engine, q)
	want, err := fullScanAnswer(t, engine, q)
	if err != nil {
		t.Fatalf("the full scan failed: %v", err)
	}
	assertSameChanges(t, got, want, "the filtered reverse-limited timeline")

	if len(got) != q.Limit {
		t.Fatalf("a filtered reverse-limited timeline returned %d changes, want its limit of %d. The "+
			"window holds enough matches to fill it, so a short answer means the walk settled on rows "+
			"the predicate had not been applied to — the marks, most likely, which every line naming "+
			"the object leaves whether or not it survives a filter: %v", len(got), q.Limit,
			instantsOf(got))
	}
	// And it really did have to keep walking: a rule that counted marks would have
	// settled inside the newest partition.
	if opened := len(spy.opened()); opened <= perHour {
		t.Errorf("the filtered scan opened %d objects, which is at most the newest partition's %d — so "+
			"it stopped before it could have known whether older partitions held matching changes",
			opened, perHour)
	}
}

// spreadRow is one change of the fixture object at an instant, for the fixtures that
// place their rows by hand rather than on a stride.
func spreadRow(at time.Time, n int) conformance.Row {
	return conformance.Row{
		Ref: testRef(),
		Change: query.Change{
			TS:              at,
			EventType:       query.EventAdded,
			UID:             uidNew,
			ResourceVersion: fmt.Sprintf("%d", 2000+n),
			APIVersion:      "apps/v1",
			Actors:          []string{actorKubectl},
			Data:            fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, n),
			SHA256:          fmt.Sprintf("%064d", n),
		},
	}
}

// keyForHour names an object in a chosen hour partition, whatever the records inside it
// are stamped.
//
// Choosing the partition is the whole point: archivetest derives a key from the record's
// own timestamp, which is right for every other fixture and cannot express the one
// property this file needs — an object holding records from the hour after its own.
func keyForHour(prefix string, hour time.Time, name string) string {
	return joinSegments(prefix, formatPartition, clusterSegment+testRef().ClusterID,
		dateSegment+hour.UTC().Format(dateLayout),
		hourSegment+hour.UTC().Format(hourLayout), name+objectSuffix)
}

// writeFrameAt writes one archive object holding changes, at a key the caller chose.
//
// The line's field names are spelled out here rather than taken from recordLine, which
// is the discipline archivetest follows and for the same reason: a fixture that asked
// the reader what the reader expects would agree with it by construction, and a fixture
// that cannot disagree proves nothing about the format.
func writeFrameAt(t *testing.T, dir, key string, changes []query.Change) {
	t.Helper()

	var body bytes.Buffer
	encoder, err := zstd.NewWriter(&body)
	if err != nil {
		t.Fatalf("opening a zstd writer for %q: %v", key, err)
	}
	for _, change := range changes {
		line, err := json.Marshal(map[string]any{
			"timestamp":        change.TS.UTC(),
			"cluster_id":       testRef().ClusterID,
			"event_type":       change.EventType,
			"group":            testRef().APIGroup,
			"version":          change.APIVersion,
			"kind":             testRef().Kind,
			"namespace":        testRef().Namespace,
			"name":             testRef().Name,
			"uid":              change.UID,
			"resource_version": change.ResourceVersion,
			"actors":           change.Actors,
			"data":             change.Data,
			"diff":             change.Diff,
			"sha256":           change.SHA256,
		})
		if err != nil {
			t.Fatalf("encoding a line of %q: %v", key, err)
		}
		if _, err := encoder.Write(append(line, '\n')); err != nil {
			t.Fatalf("writing a line of %q: %v", key, err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("closing the zstd frame of %q: %v", key, err)
	}

	path := filepath.Join(dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating the parents of %q: %v", key, err)
	}
	if err := os.WriteFile(path, body.Bytes(), 0o600); err != nil {
		t.Fatalf("writing %q: %v", key, err)
	}
}
