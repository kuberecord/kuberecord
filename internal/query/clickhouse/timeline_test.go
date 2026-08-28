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

package clickhouse

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// eventFixture is one object's changes with the cluster's commentary around them,
// recorded through **both** Event APIs.
//
// v1/Event and events.k8s.io/v1/Event are one storage behind two APIs, and a
// cluster's rules may name either. A backend that correlated one spelling would
// drop whichever half happened to be captured the other way — and the reader would
// see a shorter list with nothing saying it was short.
func eventFixture() conformance.History {
	return conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(0, query.EventAdded, uidB, []string{actorKubectl}), `{"spec":{"replicas":1}}`),
		eventRow(30*time.Second, "", "checkout.core", "ScalingReplicaSet"),
		withDiff(fixtureRow(time.Minute, query.EventModified, uidB, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":2}]`),
		eventRow(90*time.Second, "events.k8s.io", "checkout.new", "FailedCreate"),
	}}
}

// TestIncludeEventsMergesBothGroupSpellingsInOrder is the post-mortem shape: what
// changed, and what the cluster said about it, on one timeline.
func TestIncludeEventsMergesBothGroupSpellingsInOrder(t *testing.T) {
	engine, _ := seededEngine(t, eventFixture())
	from, to := fixtureWindow()

	got := drainTimeline(t, engine, query.TimelineQuery{
		Ref: testRef(), From: from, To: to, IncludeEvents: true,
	})

	wantTypes := []string{
		query.EventAdded, query.EventKubernetes, query.EventModified, query.EventKubernetes,
	}
	gotTypes := make([]string, 0, len(got))
	for _, c := range got {
		gotTypes = append(gotTypes, c.EventType)
	}
	if !slices.Equal(gotTypes, wantTypes) {
		t.Fatalf("the merged timeline is %v, want %v — the two changes with an Event from each API "+
			"spelling interleaved in ts order", gotTypes, wantTypes)
	}
	for i := 1; i < len(got); i++ {
		if got[i].TS.Before(got[i-1].TS) {
			t.Errorf("rows %d and %d are out of ts order (%s then %s)", i-1, i,
				got[i-1].TS.Format(time.RFC3339Nano), got[i].TS.Format(time.RFC3339Nano))
		}
	}

	// Every field of a merged row but its event type describes the Event, not the
	// target: the stamp is the only thing a reader has to tell a row *about* the
	// object from a row about something that happened to it.
	if data := got[1].Data; !strings.Contains(data, "ScalingReplicaSet") {
		t.Errorf("the first merged row carries %q, want the core-group Event's own state", data)
	}
	if data := got[3].Data; !strings.Contains(data, "FailedCreate") {
		t.Errorf("the second merged row carries %q, want the events.k8s.io Event's own state", data)
	}
}

// TestIncludeEventsSurvivesAnActorFilter: the commentary is not attributed to
// whoever changed the object, so an actor predicate must not silence it.
//
// An Event's actors column holds the field managers of the Event object — the
// controller that wrote it. Filtering on them would empty the Event half of almost
// every filtered timeline, and the reader would be shown "Kubernetes said nothing"
// about an incident Kubernetes had plenty to say about (Invariant 4).
func TestIncludeEventsSurvivesAnActorFilter(t *testing.T) {
	engine, _ := seededEngine(t, eventFixture())
	from, to := fixtureWindow()

	got := drainTimeline(t, engine, query.TimelineQuery{
		Ref: testRef(), From: from, To: to, IncludeEvents: true,
		Actors: []string{actorKubectl},
	})

	events := 0
	for _, c := range got {
		if c.EventType == query.EventKubernetes {
			events++
		}
		if c.EventType != query.EventKubernetes && !slices.Contains(c.Actors, actorKubectl) {
			t.Errorf("a change by %v survived an actor filter for %q", c.Actors, actorKubectl)
		}
	}
	if events != 2 {
		t.Errorf("%d Events survived an actor filter, want both. The filter names who changed the "+
			"object; an Event has no such author, and dropping it manufactures a silence", events)
	}
}

// TestLimitIsAppliedAfterAClientSideFilter: a limit beside a field-path predicate
// is not pushed into SQL.
//
// Pushed down, it would take the first n rows and *then* narrow them — returning
// fewer changes than were asked for, and the wrong ones. This asserts both halves:
// the statement carries no LIMIT, and the answer is still n rows.
func TestLimitIsAppliedAfterAClientSideFilter(t *testing.T) {
	history := conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(0, query.EventAdded, uidA, []string{actorKubectl}), `{"spec":{"replicas":1}}`),
		withDiff(fixtureRow(time.Minute, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/status/readyReplicas","value":1}]`),
		withDiff(fixtureRow(2*time.Minute, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":2}]`),
		withDiff(fixtureRow(3*time.Minute, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/status/readyReplicas","value":2}]`),
		withDiff(fixtureRow(4*time.Minute, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":3}]`),
	}}
	engine, conn := seededEngine(t, history)
	from, to := fixtureWindow()

	got := drainTimeline(t, engine, query.TimelineQuery{
		Ref: testRef(), From: from, To: to,
		FieldPaths: []string{"spec.replicas"},
		Limit:      3,
	})

	// The first sighting carries no patch and is kept regardless — it is a boundary
	// of the object's existence — so the three are the Added and the two spec
	// changes, with the status-only rows filtered out.
	if len(got) != 3 {
		t.Fatalf("a limit of 3 beside a field-path filter returned %d changes, want 3", len(got))
	}
	if got[2].Diff == "" || !strings.Contains(got[2].Diff, "/spec/replicas") {
		t.Errorf("the third change is %q; a limit pushed down over an unfiltered stream would have cut "+
			"the result before the second spec change was reached", got[2].Diff)
	}

	for _, sqlText := range conn.statements() {
		if strings.Contains(sqlText, "LIMIT ") && !strings.Contains(sqlText, "LIMIT 1") {
			t.Errorf("a limit was pushed into a statement that still had a client-side filter to "+
				"apply:\n%s", sqlText)
		}
	}
}

// TestLimitIsPushedDownWhenNothingRemains: the cheap path is still taken when it
// is safe.
//
// The bound on cost is the whole reason a limit exists, so a backend that always
// filtered client-side would be correct and useless.
func TestLimitIsPushedDownWhenNothingRemains(t *testing.T) {
	engine, conn := seededEngine(t, incarnationFixture())
	from, to := fixtureWindow()

	got := drainTimeline(t, engine, query.TimelineQuery{
		Ref: testRef(), From: from, To: to, AllIncarnations: true, Limit: 2,
	})
	if len(got) != 2 {
		t.Fatalf("a limit of 2 returned %d changes", len(got))
	}
	if !slices.ContainsFunc(conn.statements(), func(s string) bool { return strings.Contains(s, "LIMIT 2") }) {
		t.Errorf("no statement carried LIMIT 2, so the limit was applied to rows already read when it "+
			"could have bounded the read: %v", conn.statements())
	}
}

// TestTimelineOverAnUnrecordedObjectIsAnEmptyResult: no rows is a result, not a
// failure and not a statement that nothing happened.
//
// Which of those it is remains a question for Coverage. A timeline that returned
// an error here would make "nothing was watching" unreportable by the one call
// that exists to report it (Invariant 9).
func TestTimelineOverAnUnrecordedObjectIsAnEmptyResult(t *testing.T) {
	engine, _ := seededEngine(t, conformance.History{})
	from, to := fixtureWindow()

	it, err := engine.Timeline(context.Background(), query.TimelineQuery{Ref: testRef(), From: from, To: to})
	if err != nil {
		t.Fatalf("Timeline over an object with no history returned %v, want an empty iterator", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("closing the iterator: %v", err)
		}
	}()
	if it.Next() {
		t.Errorf("the iterator yielded %+v over an empty history", it.Change())
	}
	if err := it.Err(); err != nil {
		t.Errorf("the empty iterator reports %v", err)
	}
}

// TestEarlyCloseReleasesDriverRows: abandoning a stream releases the cursor.
//
// Breaking out early is the normal path, not the exceptional one — every limited
// query and every "show me the last twenty" does it — so a backend whose driver
// rows survived it would leak on the flagship command, under load, long after the
// change that caused it.
func TestEarlyCloseReleasesDriverRows(t *testing.T) {
	engine, _ := seededEngine(t, incarnationFixture())
	from, to := fixtureWindow()

	it, err := engine.Timeline(context.Background(), query.TimelineQuery{
		Ref: testRef(), From: from, To: to, AllIncarnations: true,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if !it.Next() {
		t.Fatalf("the iterator ended immediately: %v", it.Err())
	}

	if err := it.Close(); err != nil {
		t.Fatalf("closing the iterator part-way through: %v", err)
	}
	// Closing again must be safe: a caller that breaks out early and also defers a
	// Close is doing the documented thing, and the stand-in's rows report a second
	// release as an error — so a missing guard fails here rather than in whatever
	// a real driver makes of it.
	if err := it.Close(); err != nil {
		t.Errorf("closing an already-closed iterator returned %v; Close is safe to repeat", err)
	}
	if it.Next() {
		t.Errorf("a closed iterator yielded %+v", it.Change())
	}
}

// TestClosedEngineRefusesReads: a use-after-close says so.
//
// The alternative is not that it works anyway — it is a failure with no name and
// no obvious author, arriving from whatever state the connection happens to be in.
func TestClosedEngineRefusesReads(t *testing.T) {
	store := newFakeStore()
	engine, err := New(&fakeConn{store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent, which the contract promises and a caller relies on.
	if err := engine.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}

	ctx := context.Background()
	from, to := fixtureWindow()
	if _, err := engine.Timeline(ctx, query.TimelineQuery{Ref: testRef()}); err == nil {
		t.Errorf("Timeline on a closed engine succeeded")
	}
	if _, err := engine.StateAt(ctx, testRef(), to, ""); err == nil {
		t.Errorf("StateAt on a closed engine succeeded")
	}
	if _, err := engine.Incarnations(ctx, testRef(), from, to); err == nil {
		t.Errorf("Incarnations on a closed engine succeeded")
	}
	if _, err := engine.Coverage(ctx, query.ScopeQuery{ClusterID: testRef().ClusterID}); err == nil {
		t.Errorf("Coverage on a closed engine succeeded")
	}
}

// TestNewRefusesANilConnection: this package dials nothing, so there is nothing
// for it to fall back on.
//
// Construction and credential handling belong to the caller, which is what keeps
// "where does this cluster's history live?" a question the command-line client
// answers rather than one the query semantics have an opinion about.
func TestNewRefusesANilConnection(t *testing.T) {
	engine, err := New(nil)
	if err == nil {
		t.Fatalf("New(nil) returned an engine: %+v", engine)
	}
	if engine != nil {
		t.Errorf("New returned both an error and an engine")
	}
}

// TestMidStreamFailureSurfacesThroughErr: a backend that dies half-way reports it.
//
// This is what stops a partial audit history from reading as a whole one. The loop
// shape the contract documents ends with a check of Err precisely because Next
// returning false is ambiguous, and a backend that let a failure look like the end
// of the result set would produce a timeline short by exactly the rows that would
// have explained the incident.
func TestMidStreamFailureSurfacesThroughErr(t *testing.T) {
	store := newFakeStore()
	if err := store.seed(incarnationFixture()); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	engine, err := New(&fakeConn{store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})

	injected := errors.New("connection reset by peer")
	store.setFault(&conformance.StreamFault{AfterChanges: 2, Err: injected})

	from, to := fixtureWindow()
	it, err := engine.Timeline(context.Background(), query.TimelineQuery{
		Ref: testRef(), From: from, To: to, AllIncarnations: true,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	delivered := 0
	for it.Next() {
		delivered++
	}
	streamErr := it.Err()
	if err := it.Close(); err != nil {
		t.Errorf("closing the iterator: %v", err)
	}

	switch {
	case streamErr == nil:
		t.Fatalf("the stream broke after %d changes and Err reported nothing, so a caller following the "+
			"documented loop would render a truncated history as a complete one", delivered)
	case !errors.Is(streamErr, injected):
		t.Errorf("Err reported %v, which does not wrap the injected failure; a backend wraps the "+
			"underlying error with context rather than replacing it", streamErr)
	case delivered != 2:
		t.Errorf("the iterator delivered %d changes before failing, want 2", delivered)
	}
}
