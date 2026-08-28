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

package conformance

import (
	"context"
	"errors"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The subtest names, as constants because the non-vacuity tests address individual
// properties by name and a typo there would silently prove nothing.
const (
	propOrderAscending        = "Ordering/Ascending"
	propOrderReverse          = "Ordering/Reverse"
	propOrderNanoseconds      = "Ordering/NanosecondPrecision"
	propOrderLimit            = "Ordering/LimitTakesFromEmissionEnd"
	propIncarnationNewest     = "Incarnations/NewestByDefault"
	propIncarnationAll        = "Incarnations/AllIncarnations"
	propIncarnationPinned     = "Incarnations/PinnedUID"
	propIncarnationEnumerated = "Incarnations/Enumerated"
	propDeletionVisible       = "Deletions/Visible"
	propTimeBounds            = "TimeBounds/UnboundedQuery"
	propStreamEarlyClose      = "Streaming/EarlyCloseDoesNotLeak"
	propStreamMidError        = "Streaming/MidStreamErrorSurfaces"
)

// errStreamFault is the failure the suite injects to break a stream part-way
// through. It is a sentinel so the property can insist the engine surfaced *this*
// error rather than merely some error: a backend that replaced it with one of its
// own would still be reporting a failure, but a caller matching on a sentinel to
// decide an exit code would no longer be able to.
var errStreamFault = errors.New("conformance: injected mid-stream backend failure")

// leakSettle is how long a released goroutine is given to actually go away. Close
// schedules an exit rather than performing one, so the honest question is whether
// the count comes back down, not whether it already has.
const leakSettle = 5 * time.Second

// windowFrom and windowTo bracket every fixture with room to spare.
//
// Properties bound their queries even against a backend that does not demand it, so
// that one backend's TimeBoundRequired is not a special case running through the
// whole suite. The one property that issues a genuinely unbounded query is the one
// whose subject that is.
func windowFrom() time.Time { return suiteEpoch.Add(-time.Hour) }
func windowTo() time.Time   { return suiteEpoch.Add(24 * time.Hour) }

// fixtureQuery is the bounded, unfiltered query for the object every fixture
// records.
func fixtureQuery() query.TimelineQuery {
	return query.TimelineQuery{Ref: fixtureRef(), From: windowFrom(), To: windowTo()}
}

// orderingAscending: the recorded changes come back oldest first.
func orderingAscending(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	got := timelineChanges(t, h, fixtureQuery())
	want := expected(h.Capabilities.declaredCapabilities(), history.Rows)

	assertOrdered(t, got, false, "an unreversed timeline")
	assertChanges(t, got, want, "an unreversed timeline")
}

// orderingReverse: Reverse emits newest first, and nothing else changes.
func orderingReverse(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	q := fixtureQuery()
	q.Reverse = true
	got := timelineChanges(t, h, q)
	want := reversed(expected(h.Capabilities.declaredCapabilities(), history.Rows))

	assertOrdered(t, got, true, "a reversed timeline")
	assertChanges(t, got, want, "a reversed timeline")
}

// orderingNanosecondPrecision: two changes a nanosecond apart are two changes.
//
// The schema records at nanosecond precision and the contract says so, but the
// consequence of losing it is easy to under-rate: a backend storing at microseconds
// does not fail, it *reorders*. Three changes made inside one microsecond come back
// in whatever order the storage felt like, and an audit timeline that puts the
// effect before the cause is worse than one that admits it cannot tell.
func orderingNanosecondPrecision(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	got := timelineChanges(t, h, fixtureQuery())
	want := expected(h.Capabilities.declaredCapabilities(), history.Rows)
	if len(got) != len(want) {
		t.Fatalf("conformance: the nanosecond fixture came back with %d changes, want %d; the precision "+
			"assertions below need the whole history.\ngot:%s", len(got), len(want), describeChanges(got))
	}

	for i := range want {
		if !got[i].TS.Equal(want[i].TS) {
			t.Errorf("conformance: row %d came back at %s, want %s; the schema records event time to the "+
				"nanosecond and a caller renders exactly what it is given",
				i, got[i].TS.UTC().Format(time.RFC3339Nano), want[i].TS.UTC().Format(time.RFC3339Nano))
		}
	}

	// The first rows of the fixture are deliberately one nanosecond apart. A
	// backend that rounded them would collapse the gaps to zero while leaving every
	// other assertion in this suite satisfied.
	for i := 1; i < len(orderingOffsets); i++ {
		wantGap := orderingOffsets[i] - orderingOffsets[i-1]
		if gotGap := got[i].TS.Sub(got[i-1].TS); gotGap != wantGap {
			t.Errorf("conformance: rows %d and %d came back %s apart, want %s; a backend storing at a "+
				"coarser precision than the schema's does not lose the gap, it loses the *order* of "+
				"everything inside it", i-1, i, gotGap, wantGap)
		}
	}
}

// orderingLimitTakesFromEmissionEnd: a limit takes the first Limit changes in the
// emission order, which is the order Reverse selects.
//
// This is asserted bluntly because the other reading is tempting and wrong. Taking
// the newest hundred from an unreversed query would mean reading the whole window
// and sorting it in memory — the exact cost a limit exists to avoid — so a caller
// that wants the newest sets Reverse, and a backend must not quietly do it for them.
func orderingLimitTakesFromEmissionEnd(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	const limit = 2
	all := expected(h.Capabilities.declaredCapabilities(), history.Rows)

	q := fixtureQuery()
	q.Limit = limit
	assertChanges(t, timelineChanges(t, h, q), firstN(all, limit),
		"a limited, unreversed timeline (the oldest changes in the window)")

	q.Reverse = true
	assertChanges(t, timelineChanges(t, h, q), firstN(reversed(all), limit),
		"a limited, reversed timeline (the newest changes in the window)")
}

// incarnationNewestByDefault: a (namespace, name) that has worn two UIDs yields the
// newest incarnation's history, not both spliced together.
//
// This is the property most likely to be got wrong, and the reason is that getting
// it wrong still produces a plausible answer. A merged timeline shows a Deployment
// scaled to 2, deleted, created at 9 and scaled to 8 — a coherent-looking account of
// something that never happened, with nothing in the output to say two different
// objects are being described (Invariant 7).
func incarnationNewestByDefault(t conformanceT, h Harness) {
	t.Helper()
	history := incarnationHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	got := timelineChanges(t, h, fixtureQuery())
	want := expected(caps, incarnationRows(history, uidB))

	assertChanges(t, got, want, "a default timeline over a name that has worn two UIDs")
	for i, c := range got {
		if c.UID != uidB {
			t.Errorf("conformance: row %d belongs to incarnation %s, but the default timeline is the "+
				"newest incarnation (%s) alone: %s", i, c.UID, uidB, describeChange(c))
			return
		}
	}
}

// incarnationAllIncarnations: AllIncarnations returns every incarnation in the
// window, in ts order, each row still carrying its own UID.
//
// The UID is asserted separately from the row content because it is what makes the
// flag safe. Interleaving is a rendering decision, and a reader keys on Change.UID
// to tell one object from the next; a backend that returned the right rows with the
// field blanked would have produced exactly the merged timeline this flag exists to
// make explicit.
func incarnationAllIncarnations(t conformanceT, h Harness) {
	t.Helper()
	history := incarnationHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	q := fixtureQuery()
	q.AllIncarnations = true
	got := timelineChanges(t, h, q)

	assertOrdered(t, got, false, "an all-incarnations timeline")
	assertChanges(t, got, expected(caps, history.Rows), "an all-incarnations timeline")

	seen := map[string]bool{}
	for i, c := range got {
		if c.UID == "" {
			t.Errorf("conformance: row %d came back with no UID: %s. Under AllIncarnations the UID is the "+
				"only thing separating two objects that shared a name, and a reader keying on it gets one "+
				"timeline instead of two", i, describeChange(c))
			return
		}
		seen[c.UID] = true
	}
	if !seen[uidA] || !seen[uidB] {
		t.Errorf("conformance: an all-incarnations timeline returned only %v; the window holds both %s "+
			"and %s", seen, uidA, uidB)
	}
}

// incarnationPinnedUID: a pinned UID restricts the result to that incarnation, and
// AllIncarnations is ignored when it is set.
func incarnationPinnedUID(t conformanceT, h Harness) {
	t.Helper()
	history := incarnationHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	q := fixtureQuery()
	q.UID = uidA
	// Set deliberately: the contract says AllIncarnations is ignored when UID is
	// set, and a backend honouring both would answer a question nobody asked.
	q.AllIncarnations = true

	got := timelineChanges(t, h, q)
	assertChanges(t, got, expected(caps, incarnationRows(history, uidA)),
		"a timeline pinned to one incarnation (with AllIncarnations also set, which must be ignored)")
	for i, c := range got {
		if c.UID != uidA {
			t.Errorf("conformance: row %d belongs to incarnation %s, but the query pinned %s: %s",
				i, c.UID, uidA, describeChange(c))
			return
		}
	}
}

// incarnationEnumerated: Incarnations lists every UID recorded under the name,
// oldest first, with the span each one occupied.
//
// It exists so a caller can say "there are two other incarnations of this name"
// before rendering one of them. A timeline that shows one incarnation without
// admitting the others is the splice told quietly.
func incarnationEnumerated(t conformanceT, h Harness) {
	t.Helper()
	history := incarnationHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	got, err := h.Engine.Incarnations(context.Background(), fixtureRef(), windowFrom(), windowTo())
	if err != nil {
		t.Fatalf("conformance: Incarnations returned %v; the window holds two incarnations of %s/%s",
			err, fixtureNS, fixtureName)
	}

	aRows := retainedRows(incarnationRows(history, uidA), caps)
	bRows := retainedRows(incarnationRows(history, uidB), caps)
	want := []query.Incarnation{
		{
			UID:       uidA,
			FirstSeen: aRows[0].Change.TS,
			LastSeen:  aRows[len(aRows)-1].Change.TS,
			// False on a backend that records no deletions is the truthful answer
			// about its *history*, not a claim that the object still exists — which
			// is why a renderer has to qualify it by the capability.
			Deleted: caps.Deletions,
		},
		{
			UID:       uidB,
			FirstSeen: bRows[0].Change.TS,
			LastSeen:  bRows[len(bRows)-1].Change.TS,
			Deleted:   false,
		},
	}

	if len(got) != len(want) {
		t.Fatalf("conformance: Incarnations returned %d entries, want %d (%s then %s).\ngot: %v",
			len(got), len(want), uidA, uidB, got)
	}
	for i := range want {
		switch {
		case got[i].UID != want[i].UID:
			t.Errorf("conformance: incarnation %d is %s, want %s; the list is oldest first by FirstSeen",
				i, got[i].UID, want[i].UID)
		case !got[i].FirstSeen.Equal(want[i].FirstSeen) || !got[i].LastSeen.Equal(want[i].LastSeen):
			t.Errorf("conformance: incarnation %s spans [%s, %s], want [%s, %s]", got[i].UID,
				got[i].FirstSeen.UTC().Format(time.RFC3339Nano), got[i].LastSeen.UTC().Format(time.RFC3339Nano),
				want[i].FirstSeen.UTC().Format(time.RFC3339Nano), want[i].LastSeen.UTC().Format(time.RFC3339Nano))
		case got[i].Deleted != want[i].Deleted:
			t.Errorf("conformance: incarnation %s reports Deleted=%t, want %t (this backend declares "+
				"Deletions=%t)", got[i].UID, got[i].Deleted, want[i].Deleted, caps.Deletions)
		}
	}
}

// deletionVisibility: a backend that can record deletions returns the Deleted row;
// one that cannot must not invent one, and must say so in its capabilities.
//
// The two halves are one property because the failure they guard against is one
// failure seen from two sides. History with no deletions in it is indistinguishable
// from history of a cluster where nothing was deleted, and the only thing standing
// between those two readings is a truthful Capabilities().Deletions. An engine that
// declared it falsely would let a renderer print a timeline that simply stops and
// let an engineer read it as "still there"; an engine that fabricated a row would
// tell them the object was deleted at the moment the archive happened to end.
func deletionVisibility(t conformanceT, h Harness) {
	t.Helper()
	history := deletionHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	got := timelineChanges(t, h, fixtureQuery())

	var deletions []query.Change
	for _, c := range got {
		if c.EventType == query.EventDeleted {
			deletions = append(deletions, c)
		}
	}

	switch {
	case caps.Deletions && len(deletions) == 0:
		t.Fatalf("conformance: this backend declares Deletions, and the seeded history ends in a "+
			"deletion, but no Deleted row came back. Declared and observed must agree: either the engine "+
			"is dropping the row, or the declaration is wrong and every timeline this backend renders "+
			"will simply stop where an object was deleted, with nothing saying so.\ngot:%s",
			describeChanges(got))
	case !caps.Deletions && len(deletions) > 0:
		t.Fatalf("conformance: this backend does not declare Deletions, yet %d Deleted row(s) came back — "+
			"the first at %s. A backend whose storage never receives a deletion must not synthesize one "+
			"to close a timeline that merely ended (Invariant 4): the row would tell an engineer the "+
			"object was deleted at the instant the history happened to stop",
			len(deletions), deletions[0].TS.UTC().Format(time.RFC3339Nano))
	}

	assertOrdered(t, got, false, "a timeline over a deleted object")
	assertChanges(t, got, expected(caps, history.Rows), "a timeline over a deleted object")

	if !caps.Deletions {
		return
	}
	// A deletion carries no data, no patch, no hash and no actors. Those are not
	// missing values a renderer should fill in — there is no live object left to
	// attribute one to, and the emptiness is the answer.
	d := deletions[len(deletions)-1]
	switch {
	case d.Data != "" || d.Diff != "" || d.SHA256 != "":
		t.Errorf("conformance: the Deleted row carries data=%q diff=%q sha256=%q; a deletion records "+
			"none of the three", d.Data, d.Diff, d.SHA256)
	case len(d.Actors) != 0:
		t.Errorf("conformance: the Deleted row is attributed to %v; a deletion records no actors, and an "+
			"empty list is the honest answer rather than a missing one", d.Actors)
	case d.UID != uidA:
		t.Errorf("conformance: the Deleted row belongs to incarnation %s, want %s; a deletion is terminal "+
			"for one incarnation and not for the name", d.UID, uidA)
	}
}

// timeBoundsUnboundedQuery: a backend that requires a time bound refuses an
// unbounded query rather than starting a scan it cannot bound.
//
// Refusing is the kinder outcome. An unbounded scan over a large archive is
// indistinguishable from a hang, and the sentinel is what lets a caller turn the
// refusal into a message naming the flag that fixes it.
func timeBoundsUnboundedQuery(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	required := h.Capabilities.declaredCapabilities().TimeBoundRequired
	it, err := h.Engine.Timeline(context.Background(), query.TimelineQuery{Ref: fixtureRef()})
	if it != nil {
		defer closeIterator(t, it)
	}

	if required {
		switch {
		case err == nil:
			t.Fatalf("conformance: this backend declares TimeBoundRequired but answered a query with no " +
				"From and no To. Declared and observed must agree: a caller consults the flag while " +
				"composing the query and supplies its default window, so a backend that quietly accepts " +
				"an unbounded one will be handed one the day a caller means it")
		case !errors.Is(err, query.ErrTimeBoundRequired):
			t.Errorf("conformance: an unbounded query was refused with %v, which is not "+
				"query.ErrTimeBoundRequired. A caller matches the sentinel to name the flag that fixes "+
				"the problem; matching on message text would break the first time this backend reworded "+
				"its own error", err)
		}
	} else {
		switch {
		case errors.Is(err, query.ErrTimeBoundRequired):
			t.Fatalf("conformance: an unbounded query was refused with ErrTimeBoundRequired, but this " +
				"backend never declared TimeBoundRequired. A caller reading the declaration would issue " +
				"exactly this query and be told the backend cannot answer a question it never said it " +
				"could not answer")
		case err != nil:
			t.Fatalf("conformance: an unbounded query returned %v; a backend that does not require a time "+
				"bound must answer one", err)
		default:
			assertChanges(t, collect(t, it), expected(h.Capabilities.declaredCapabilities(), history.Rows),
				"an unbounded timeline")
		}
	}

	// Incarnations carries the same rule, and carries it for the same reason: it is
	// a scan over the same rows.
	_, incErr := h.Engine.Incarnations(context.Background(), fixtureRef(), time.Time{}, time.Time{})
	switch {
	case required && !errors.Is(incErr, query.ErrTimeBoundRequired):
		t.Errorf("conformance: unbounded Incarnations returned %v; a backend declaring TimeBoundRequired "+
			"refuses it with query.ErrTimeBoundRequired, exactly as it refuses an unbounded Timeline",
			incErr)
	case !required && errors.Is(incErr, query.ErrTimeBoundRequired):
		t.Errorf("conformance: unbounded Incarnations was refused with ErrTimeBoundRequired, but this " +
			"backend never declared TimeBoundRequired")
	}
}

// streamingEarlyCloseDoesNotLeak: an iterator abandoned part-way through releases
// what it holds.
//
// Breaking out early is the normal path, not the exceptional one: it is what every
// limited query and every "show me the last twenty" does. A backend whose merge
// goroutines or driver rows survive it turns the flagship command into a leak that
// only shows up under load, long after the change that caused it.
func streamingEarlyCloseDoesNotLeak(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, orderingHistory())

	// Warm up first, so that whatever the engine starts lazily is already running
	// when the baseline is taken. Without this the property would count the
	// backend's first-use goroutines as a leak.
	if len(timelineChanges(t, h, fixtureQuery())) == 0 {
		t.Fatalf("conformance: the warm-up query returned nothing; the leak check needs a result set to " +
			"abandon part-way through")
	}
	baseline := goroutines()

	it := timeline(t, h, fixtureQuery())
	for range 2 {
		if !it.Next() {
			t.Fatalf("conformance: the iterator ended after fewer than 2 changes (%v); the property needs "+
				"a stream that is genuinely in progress when it is closed", it.Err())
		}
	}
	closeIterator(t, it)
	// Closing again must be safe: a caller that breaks out early and also defers a
	// Close is doing the documented thing.
	closeIterator(t, it)

	if !waitFor(func() bool { return goroutines() <= baseline }, leakSettle) {
		t.Errorf("conformance: %d goroutines are still running %s after the iterator was closed, against "+
			"a baseline of %d taken after a warm-up query. An iterator closed before its result set is "+
			"exhausted must release what it holds — that is the normal path for every limited query, not "+
			"an exceptional one", goroutines(), leakSettle, baseline)
	}
}

// streamingMidStreamErrorSurfaces: a backend that dies half-way through reports it
// through Err rather than ending the loop quietly.
//
// This is the property that stops a partial audit history from reading as a whole
// one. The loop shape the contract documents ends with a check of Err precisely
// because Next returning false is ambiguous, and a backend that let a failure look
// like the end of the result set would produce a timeline that is short by exactly
// the rows that would have explained the incident, with nothing anywhere saying so.
func streamingMidStreamErrorSurfaces(t conformanceT, h Harness) {
	t.Helper()
	history := orderingHistory()
	seed(t, h, history)

	const deliver = 3
	h.SetStreamFault(&StreamFault{AfterChanges: deliver, Err: errStreamFault})

	it := timeline(t, h, fixtureQuery())
	delivered := 0
	for it.Next() {
		delivered++
	}
	err := it.Err()
	closeIterator(t, it)

	switch {
	case err == nil:
		t.Fatalf("conformance: the backend failed after %d changes and the iterator ended with a nil Err, "+
			"so a caller following the documented loop would render %d rows as a complete history. A "+
			"truncated audit timeline that looks whole is the worst available outcome (Invariant 4)",
			deliver, delivered)
	case !errors.Is(err, errStreamFault):
		t.Errorf("conformance: the iterator reported %v, which does not wrap the injected fault. Backends "+
			"are expected to wrap the underlying failure with context rather than replace it, so that a "+
			"caller can still tell what went wrong", err)
	case delivered != deliver:
		t.Errorf("conformance: the iterator delivered %d changes before failing, want %d; "+
			"Harness.SetStreamFault is asked to break the stream after exactly that many",
			delivered, deliver)
	}

	// Clearing the fault must restore the engine. Without this the property could
	// be satisfied by an engine that is simply broken, which would prove nothing
	// about how it reports a failure.
	h.SetStreamFault(nil)
	assertChanges(t, timelineChanges(t, h, fixtureQuery()),
		expected(h.Capabilities.declaredCapabilities(), history.Rows),
		"a timeline issued after the injected fault was cleared")
}
