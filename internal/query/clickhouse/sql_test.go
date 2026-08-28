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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// dedupForms are the three ways a read of a ReplacingMergeTree can be made to
// return one row per key.
//
// FINAL is what this backend uses everywhere; the other two are listed because
// they are equally correct and a later change might reach for one, and a test
// that only knew about FINAL would then reject a correct statement — which is how
// a correctness check gets weakened into a formatting check by whoever hits it.
var dedupForms = []string{" FINAL", "argMax(", "LIMIT 1 BY "}

// carriesDedupForm reports whether a statement reduces duplicates.
func carriesDedupForm(sqlText string) bool {
	return slices.ContainsFunc(dedupForms, func(form string) bool { return strings.Contains(sqlText, form) })
}

// readsResourceStates reports whether a statement reads the ReplacingMergeTree.
func readsResourceStates(sqlText string) bool {
	return strings.Contains(sqlText, "FROM "+tableResourceStates)
}

// testRef is the identity the SQL tests build statements for.
func testRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: "prod", APIGroup: "apps", Kind: "Deployment",
		Namespace: "payments", Name: "checkout",
	}
}

// testWindow is a bounded window with a nanosecond component on each side, so a
// bound that lost precision is visible in the rendered argument.
func testWindow() (time.Time, time.Time) {
	from := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	return from, from.Add(2 * time.Hour)
}

// TestEveryResourceStatesReadCarriesADedupForm is the correctness assertion this
// package exists to keep.
//
// resource_states is a ReplacingMergeTree over an at-least-once write path, so a
// read without a dedup form can return one recorded change as two rows — the same
// scale-down, at the same nanosecond, twice, with nothing saying the cluster did
// not do it twice. The assertion is over the *builders* rather than over a
// handful of statements written by hand, because a builder is what a later change
// edits.
func TestEveryResourceStatesReadCarriesADedupForm(t *testing.T) {
	ref := testRef()
	from, to := testWindow()
	at := from.Add(time.Hour)

	base := query.TimelineQuery{Ref: ref, From: from, To: to}
	filtered := base
	filtered.Actors = []string{"kubectl"}
	filtered.ExcludeActors = []string{"kube-controller-manager"}

	unbounded := query.TimelineQuery{Ref: ref}

	tests := []struct {
		name string
		stmt statement
	}{
		{"timeline", timelineStatement(base, "", 0)},
		{"timeline pinned to an incarnation", timelineStatement(base, "uid-a", 0)},
		{"timeline with a pushed-down limit", timelineStatement(base, "uid-a", 20)},
		{"timeline with actor predicates", timelineStatement(filtered, "uid-a", 0)},
		{"timeline reversed", timelineStatement(query.TimelineQuery{
			Ref: ref, From: from, To: to, Reverse: true,
		}, "", 0)},
		{"timeline unbounded", timelineStatement(unbounded, "", 0)},
		{"newest incarnation", newestIncarnationStatement(ref, from, to)},
		{"newest incarnation unbounded", newestIncarnationStatement(ref, time.Time{}, time.Time{})},
		{"incarnations", incarnationsStatement(ref, from, to)},
		{"replay", replayStatement(ref, at, "uid-a")},
		{"newest incarnation at an instant", newestIncarnationAtStatement(ref, at)},
		{"events", eventsStatement(ref, from, to, "uid-a", false)},
		{"events without a pinned incarnation", eventsStatement(ref, from, to, "", true)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !readsResourceStates(tc.stmt.SQL) {
				t.Fatalf("this statement does not read %s, so the test table has drifted from what it "+
					"means to cover:\n%s", tableResourceStates, tc.stmt.SQL)
			}
			if !carriesDedupForm(tc.stmt.SQL) {
				t.Errorf("this read of %s carries none of %v, so an unmerged duplicate would be rendered "+
					"as a second change in an audit timeline:\n%s", tableResourceStates, dedupForms, tc.stmt.SQL)
			}
		})
	}
}

// TestCoverageReadCarriesNoFinal: watch_scopes is a plain MergeTree.
//
// The mirror image of the assertion above, and worth its own test: FINAL on a
// table with nothing to collapse is a cost paid on every "was anything watching
// this?" for no return, and the reason the two tables differ is exactly the kind
// of thing a reader generalises away.
func TestCoverageReadCarriesNoFinal(t *testing.T) {
	stmt := coverageStatement(query.ScopeQuery{ClusterID: "prod", Kind: "Deployment", Namespace: "payments"})
	if !strings.Contains(stmt.SQL, "FROM "+tableWatchScopes) {
		t.Fatalf("the coverage statement does not read %s:\n%s", tableWatchScopes, stmt.SQL)
	}
	if strings.Contains(stmt.SQL, "FINAL") {
		t.Errorf("%s is a plain MergeTree whose rows are written once each, so FINAL on it collapses "+
			"nothing and costs on every read:\n%s", tableWatchScopes, stmt.SQL)
	}
}

// TestEmittedStatementsCarryADedupForm asserts the same property over what the
// engine really sends, rather than over what its builders can produce.
//
// The two are not the same claim. The builder table above would keep passing if a
// method stopped calling a builder and assembled a statement inline; this one
// drives every entry point through a recording connection and inspects everything
// that reached it, which is the form the acceptance criterion is written in.
func TestEmittedStatementsCarryADedupForm(t *testing.T) {
	engine, conn := seededEngine(t, incarnationFixture())
	ctx := context.Background()
	ref := testRef()
	from, to := fixtureWindow()

	drainTimeline(t, engine, query.TimelineQuery{
		Ref: ref, From: from, To: to,
		Actors:        []string{actorKubectl},
		ExcludeActors: []string{actorHelm},
		FieldPaths:    []string{"spec.replicas"},
		IncludeEvents: true,
		Limit:         5,
	})
	drainTimeline(t, engine, query.TimelineQuery{Ref: ref, From: from, To: to, AllIncarnations: true})

	if _, err := engine.Incarnations(ctx, ref, from, to); err != nil {
		t.Fatalf("Incarnations: %v", err)
	}
	if _, err := engine.StateAt(ctx, ref, to, ""); err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if _, err := engine.Coverage(ctx, query.ScopeQuery{ClusterID: ref.ClusterID}); err != nil {
		t.Fatalf("Coverage: %v", err)
	}

	statements := conn.statements()
	if len(statements) < 6 {
		t.Fatalf("only %d statements reached the connection; the entry points above emit more than that, "+
			"so this test is no longer covering what it claims to", len(statements))
	}
	reads := 0
	for _, sqlText := range statements {
		if !readsResourceStates(sqlText) {
			continue
		}
		reads++
		if !carriesDedupForm(sqlText) {
			t.Errorf("this emitted read of %s carries none of %v:\n%s", tableResourceStates, dedupForms, sqlText)
		}
	}
	if reads == 0 {
		t.Errorf("no statement reaching the connection read %s, so this test asserted nothing about the "+
			"property it exists for", tableResourceStates)
	}
}

// TestIdentityAndWindowArePushedDown: the predicates that decide how much of the
// table is scanned reach the WHERE clause, in the sort key's own order.
//
// The order is not cosmetic. (cluster_id, api_group, kind, namespace, name, ts) is
// the ORDER BY of the table, and a query pinning its first five columns and
// bounding the sixth reads one contiguous run rather than a partition scan — which
// is the whole of what Capabilities().PointQuery claims.
func TestIdentityAndWindowArePushedDown(t *testing.T) {
	ref := testRef()
	from, to := testWindow()
	stmt := timelineStatement(query.TimelineQuery{Ref: ref, From: from, To: to}, "uid-a", 0)

	wantOrder := []string{
		"cluster_id = ?", "api_group = ?", "kind = ?", "namespace = ?", "name = ?",
		"ts >= ?", "ts <= ?", "uid = ?",
	}
	if got := whereClauses(t, stmt.SQL); !slices.Equal(got, wantOrder) {
		t.Errorf("the timeline's WHERE clause is %v, want %v (the sort key's own order)", got, wantOrder)
	}

	wantArgs := []any{
		ref.ClusterID, ref.APIGroup, ref.Kind, ref.Namespace, ref.Name,
		chTime(from), chTime(to), "uid-a",
	}
	if !slices.Equal(stmt.Args, wantArgs) {
		t.Errorf("the timeline bound %v, want %v", stmt.Args, wantArgs)
	}
}

// TestTimeBoundsBindNanosecondStrings: a bound keeps every digit the schema
// records.
//
// The pinned driver renders a positional time.Time into a statement at second
// precision. Binding one here would not fail — it would quietly move a window
// edge by up to a second, which for a fixture spacing changes a nanosecond apart
// is the difference between two changes and one.
func TestTimeBoundsBindNanosecondStrings(t *testing.T) {
	from, to := testWindow()
	stmt := timelineStatement(query.TimelineQuery{Ref: testRef(), From: from, To: to}, "", 0)

	for i, arg := range stmt.Args {
		if _, isInstant := arg.(time.Time); isInstant {
			t.Fatalf("argument %d is a time.Time; the driver renders one at second precision, so a "+
				"nanosecond bound would be silently blunted", i)
		}
	}
	if want := "2026-03-01 12:00:00.123456789"; !slices.Contains(stmt.Args, any(want)) {
		t.Errorf("the lower bound was bound as %v, want the literal %q", stmt.Args, want)
	}
}

// TestUnboundedQueryPushesNoTimePredicate: this backend answers an unbounded
// query, and says so by emitting no ts predicate at all.
func TestUnboundedQueryPushesNoTimePredicate(t *testing.T) {
	stmt := timelineStatement(query.TimelineQuery{Ref: testRef()}, "", 0)
	for _, clause := range whereClauses(t, stmt.SQL) {
		if strings.HasPrefix(clause, "ts ") {
			t.Errorf("an unbounded timeline emitted %q; a sentinel instant in place of an absent bound "+
				"would hide from a log that the caller asked an unbounded question", clause)
		}
	}
}

// TestActorPredicatesArePushedDownAndExcludeWins: both directions reach the
// backend, and the exclusion is its own predicate.
//
// Emitting them as two predicates rather than one combined expression is what
// gives ExcludeActors the documented last word: a change made by an actor named
// in both lists is dropped, which is the narrower reading when a caller has
// contradicted itself.
func TestActorPredicatesArePushedDownAndExcludeWins(t *testing.T) {
	q := query.TimelineQuery{
		Ref:           testRef(),
		Actors:        []string{"kubectl"},
		ExcludeActors: []string{"kubectl", "kube-controller-manager"},
	}
	stmt := timelineStatement(q, "", 0)

	clauses := whereClauses(t, stmt.SQL)
	include := slices.Index(clauses, "hasAny(actors, ?)")
	exclude := slices.Index(clauses, "NOT hasAny(actors, ?)")
	switch {
	case include < 0:
		t.Fatalf("the actor include predicate was not pushed down: %v", clauses)
	case exclude < 0:
		t.Fatalf("the actor exclude predicate was not pushed down: %v", clauses)
	case exclude < include:
		t.Errorf("the exclusion is emitted before the inclusion, which reads as though it could be "+
			"narrowed by it; it is applied after Actors and wins on conflict: %v", clauses)
	}
}

// TestFieldPathsAreNotPushedDown: the one predicate applied to rows already read
// leaves no trace in the statement.
//
// See matchesFieldPaths for why. The assertion is here so that an attempt to push
// it down has to come with a deliberate edit to this test, rather than arriving as
// a second implementation of RFC 6901 that only the conformance agreement property
// would catch disagreeing with the first.
func TestFieldPathsAreNotPushedDown(t *testing.T) {
	q := query.TimelineQuery{Ref: testRef(), FieldPaths: []string{"spec.replicas"}}
	stmt := timelineStatement(q, "", 0)
	if strings.Contains(stmt.SQL, "JSONExtract") || slices.Contains(stmt.Args, any("spec.replicas")) {
		t.Errorf("a field-path predicate reached the statement:\n%s\nargs: %v", stmt.SQL, stmt.Args)
	}
}

// TestNewestIncarnationProbeCarriesNoFilters: incarnation selection happens before
// filtering, and the statement is where that is decided.
//
// This is the property whose failure still produces a plausible answer. A name
// whose newest incarnation was only ever touched by an excluded actor would, if
// the filter ran first, resolve to the *previous* object — and the caller would be
// shown a deleted Deployment's history under the living Deployment's name, with
// nothing in the output admitting the substitution (Invariant 7).
func TestNewestIncarnationProbeCarriesNoFilters(t *testing.T) {
	ref := testRef()
	from, to := testWindow()
	stmt := newestIncarnationStatement(ref, from, to)

	for _, clause := range whereClauses(t, stmt.SQL) {
		if strings.Contains(clause, "actors") {
			t.Errorf("the newest-incarnation probe carries %q; a filter applied before the incarnation "+
				"is chosen can change which incarnation is chosen", clause)
		}
	}
	if got := len(stmt.Args); got != 7 {
		t.Errorf("the probe bound %d arguments, want 7 (five identity columns and two bounds): %v",
			got, stmt.Args)
	}
}

// TestEventsStatementHandlesBothGroupSpellings: v1/Event and
// events.k8s.io/v1/Event are one storage behind two APIs.
//
// Handling one of them would drop whichever half of the cluster's events happens
// to be reported the other way — a silent hole rather than a visible gap, and one
// nothing in the output would mark.
func TestEventsStatementHandlesBothGroupSpellings(t *testing.T) {
	ref := testRef()
	from, to := testWindow()
	stmt := eventsStatement(ref, from, to, "uid-a", false)

	for _, want := range []string{"involvedObject", "regarding", eventGroups} {
		if !strings.Contains(stmt.SQL, want) {
			t.Errorf("the events statement does not mention %q:\n%s", want, stmt.SQL)
		}
	}

	// The Event row's own namespace column must not be constrained: an Event lives
	// in a namespace of its own, and for a cluster-scoped object it is not the
	// object's — which has none. Pinning the column would correlate nothing for
	// exactly the objects whose events are hardest to find another way.
	for _, clause := range whereClauses(t, stmt.SQL) {
		if clause == "namespace = ?" {
			t.Errorf("the events statement pins the Event row's own namespace column, which is not the "+
				"subject's: %v", whereClauses(t, stmt.SQL))
		}
	}
	if !slices.Contains(stmt.Args, any(ref.Namespace)) {
		t.Errorf("the subject's namespace was never bound: %v", stmt.Args)
	}
}

// TestCoveragePushesTheCoveringNamespaceReading: a query for one namespace matches
// the all-namespaces scope too.
//
// A cluster-wide rule genuinely was watching the object in that namespace, and
// answering "never observed" about something that was observed the whole time is
// the failure this reading exists to prevent.
func TestCoveragePushesTheCoveringNamespaceReading(t *testing.T) {
	stmt := coverageStatement(query.ScopeQuery{ClusterID: "prod", Kind: "ConfigMap", Namespace: "kube-system"})
	clauses := whereClauses(t, stmt.SQL)
	if !slices.Contains(clauses, "(namespace = ? OR namespace = '')") {
		t.Errorf("the coverage statement narrows to the queried namespace alone: %v", clauses)
	}

	// An empty group or kind means "every one of them", so no predicate at all —
	// not an equality against the empty string, which for api_group would mean the
	// core group and would silently exclude every other.
	wide := coverageStatement(query.ScopeQuery{ClusterID: "prod"})
	if got := whereClauses(t, wide.SQL); !slices.Equal(got, []string{"cluster_id = ?"}) {
		t.Errorf("an unrestricted coverage query emitted %v, want the cluster alone", got)
	}
}

// whereClauses reads a statement's predicates back out, in order.
func whereClauses(t *testing.T, sqlText string) []string {
	t.Helper()
	parsed, err := parseStatement(sqlText, nil)
	if err != nil {
		t.Fatalf("taking the statement apart: %v\n%s", err, sqlText)
	}
	return parsed.predicates
}
