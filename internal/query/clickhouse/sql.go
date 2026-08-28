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
	"fmt"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Every statement this backend emits is built here, by a pure function, and
// nowhere else.
//
// That is a testability decision before it is a tidiness one. The load-bearing
// correctness property of this package — that no read of resource_states can
// return an unmerged duplicate — is a property of the *text* of every statement,
// and a property of text can only be asserted over statements a test can
// enumerate. SQL assembled inline at each call site would leave the assertion
// with nothing to iterate.

// The tables of the frozen v1 schema. They are unqualified, so the connection's
// own default database decides where they live — resolving that is the caller's
// business, exactly as dialling is (see New).
const (
	tableResourceStates = "resource_states"
	tableWatchScopes    = "watch_scopes"
)

// chTimeFormat renders an instant as a datetime literal for a *query argument*.
//
// Time bounds are bound as strings rather than as time.Time, and the asymmetry
// with the write path is deliberate and load-bearing. The pinned driver
// (clickhouse-go v2.46.0) renders a positional time.Time into a statement at
// second precision, which would blunt every bound this package binds: the schema
// records ts at DateTime64(9), the contract promises that two changes a
// nanosecond apart are two changes, and a truncated bound would silently include
// or exclude the rows either side of a whole second. A quoted string is parsed
// server-side against the DateTime64(9, 'UTC') column instead — full precision,
// no zone inference anywhere.
//
// The write path binds an instant for the mirror-image reason: the driver parses
// such a string client-side and reinterprets its digits in time.Local. So the two
// paths must disagree, and the reader that copies one convention into the other's
// place gets a wrong answer rather than an error.
const chTimeFormat = "2006-01-02 15:04:05.999999999"

// The projections each read asks for, named because they are also how a
// statement is identified — by a test asserting what this package emits, and by
// the stand-in connection that has to answer it. Changing one is changing the
// shape of a read, not merely its formatting.
const (
	// changeColumns is the whole of what a query.Change carries, in the order
	// scanChange reads it. cluster_id, api_group, kind, namespace and name are
	// deliberately absent: every row an iterator yields describes the object the
	// caller asked for, so repeating its identity on each one would be noise in
	// the contract people script against.
	changeColumns = "ts, event_type, api_version, uid, resource_version, labels, actors, data, diff, sha256"

	// replayColumns is the narrower projection a reconstruction needs. It omits
	// the columns a replay never reads, because StateAt materializes an
	// incarnation's history and data is the expensive column in this schema.
	replayColumns = "ts, event_type, data, diff, sha256"

	// incarnationColumn is the single column the newest-incarnation probe selects.
	incarnationColumn = "uid"

	// incarnationColumns is the per-UID span Incarnations reports. countIf yields
	// UInt64, so "was this incarnation deleted" arrives as a count rather than as
	// a UInt8 a scan target would have to guess the Go type of.
	incarnationColumns = "uid, min(ts) AS first_seen, max(ts) AS last_seen, " +
		"countIf(event_type = 'Deleted') AS deletions"

	// scopeColumns is one watch-scope transition, as Coverage pairs them.
	scopeColumns = "api_group, kind, namespace, action, rule_ref, ts"
)

// eventGroups is the predicate that catches both API spellings of a Kubernetes
// Event.
//
// It is a literal rather than a bound argument because it is not user input: the
// two groups are the two the Kubernetes API defines, and a rule naming either one
// records into the same kind. Handling only one of them would drop whichever half
// of the cluster's events happens to be reported the other way, which is a silent
// hole rather than a visible gap (Task 3.1).
const eventGroups = "api_group IN ('', 'events.k8s.io')"

// eventKind is the kind an Event is recorded under, in both groups.
const eventKind = "Event"

// statement is one rendered read: the text, and the arguments bound to it in the
// order their placeholders appear.
type statement struct {
	SQL  string
	Args []any
}

// conditions accumulates WHERE predicates and the arguments they bind, keeping
// the two in step.
//
// Keeping them in step is the entire job. A predicate list and an argument list
// maintained separately drift the first time an optional clause is added, and the
// symptom is not a failure — it is a namespace filter silently applied to the
// name column.
type conditions struct {
	clauses []string
	args    []any
}

// add appends one predicate and the arguments its placeholders bind.
func (c *conditions) add(clause string, args ...any) {
	c.clauses = append(c.clauses, clause)
	c.args = append(c.args, args...)
}

// render joins the predicates into the canonical layout every statement here
// uses: the first on the WHERE line, each subsequent one indented under its own
// AND.
//
// The layout is fixed rather than incidental. A test asserting what this package
// emits, and the stand-in connection that answers it, both read the statement
// back predicate by predicate; a free-form rendering would leave them parsing
// SQL instead of reading a list.
func (c conditions) render() string {
	return strings.Join(c.clauses, "\n  AND ")
}

// renderSelect assembles a statement in that canonical layout.
//
// final is passed rather than inferred because it is the correctness requirement
// of this package: resource_states is a ReplacingMergeTree over an at-least-once
// write path, so a read of it without FINAL can return a row twice, and a
// duplicated row in an audit timeline is a lie about the cluster rather than a
// cosmetic defect. watch_scopes is a plain MergeTree with nothing to collapse.
func renderSelect(projection, table string, final bool, conds conditions, tail ...string) statement {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(projection)
	b.WriteString("\nFROM ")
	b.WriteString(table)
	if final {
		b.WriteString(" FINAL")
	}
	b.WriteString("\nWHERE ")
	b.WriteString(conds.render())
	for _, line := range tail {
		b.WriteString("\n")
		b.WriteString(line)
	}
	return statement{SQL: b.String(), Args: conds.args}
}

// chTime renders an instant as the datetime literal a bound argument carries.
func chTime(t time.Time) string { return t.UTC().Format(chTimeFormat) }

// identityConditions pushes the canonical identity into the WHERE clause, in the
// order the table's ORDER BY declares it.
//
// The order is what makes a single-object timeline a sorted-range read rather
// than a scan: (cluster_id, api_group, kind, namespace, name, ts) is the sort
// key, so a query pinning the first five columns and bounding the sixth reads one
// contiguous run of one part per partition, whatever the table has grown to.
//
// api_group is bound even when it is the empty core group, and that is a value
// and not a wildcard: a query for the core group must not match every group, and
// omitting the predicate would also drop the prefix that makes the read cheap.
func identityConditions(c *conditions, ref query.ObjectRef) {
	c.add("cluster_id = ?", ref.ClusterID)
	c.add("api_group = ?", ref.APIGroup)
	c.add("kind = ?", ref.Kind)
	c.add("namespace = ?", ref.Namespace)
	c.add("name = ?", ref.Name)
}

// windowConditions pushes an inclusive time bound, omitting either side that was
// left unbounded.
//
// Omitting rather than substituting a sentinel instant matters to the reader as
// much as to the plan: a statement that carries no ts predicate says the caller
// asked an unbounded question, which this backend answers (it declares no
// TimeBoundRequired) but which is worth being able to see in a log.
func windowConditions(c *conditions, from, to time.Time) {
	if !from.IsZero() {
		c.add("ts >= ?", chTime(from))
	}
	if !to.IsZero() {
		c.add("ts <= ?", chTime(to))
	}
}

// actorConditions pushes the actor predicates into the backend.
//
// These are the filters that *are* expressible in SQL: actors is an Array column
// and hasAny is an index-free but row-local test, so both directions push down
// whole. ExcludeActors is emitted second and as its own predicate, which is what
// gives it the documented last word — a change made by an actor named in both
// lists is dropped, the narrower reading when a caller has contradicted itself.
//
// Field-path predicates are deliberately absent. Pushing them down would buy
// nothing and cost correctness: the diff column is returned on every row of a
// timeline regardless, so the same bytes are read off disk either way, while the
// SQL form would be a reimplementation of RFC 6901 in a query language — each
// operation's path unescaped (~1 before ~0, in that order, or a path holding a
// literal tilde is silently mangled), converted to the dotted grammar and
// prefix-matched, with a row carrying no patch surviving anyway. Every one of
// those steps is a place to disagree with the client-side reading, and a
// disagreement between two backends about which rows a filter keeps is exactly
// what the conformance suite's agreement property exists to catch. So the
// contract owns the one reading: query.MatchesFieldPaths.
func actorConditions(c *conditions, include, exclude []string) {
	if len(include) > 0 {
		c.add("hasAny(actors, ?)", include)
	}
	if len(exclude) > 0 {
		c.add("NOT hasAny(actors, ?)", exclude)
	}
}

// orderByTS renders the ordering clause for a direction.
func orderByTS(reverse bool) string {
	if reverse {
		return "ORDER BY ts DESC"
	}
	return "ORDER BY ts"
}

// timelineStatement renders the object's own change stream.
//
// uid is passed rather than read off q because the incarnation a default query
// means has already been resolved by then, and passing the resolved value keeps
// this function a rendering of a decision rather than a second place that makes
// it. An empty uid means every incarnation in the window, which is what
// AllIncarnations asks for.
//
// limit is likewise passed rather than read off q: it is only pushed into SQL
// when nothing remains to be applied to the rows afterwards. A limit pushed down
// over a stream still awaiting a client-side predicate would take the first n
// rows and then filter them, returning fewer rows than were asked for and, worse,
// the wrong ones.
func timelineStatement(q query.TimelineQuery, uid string, limit int) statement {
	var c conditions
	identityConditions(&c, q.Ref)
	windowConditions(&c, q.From, q.To)
	if uid != "" {
		c.add("uid = ?", uid)
	}
	actorConditions(&c, q.Actors, q.ExcludeActors)

	tail := []string{orderByTS(q.Reverse)}
	if limit > 0 {
		tail = append(tail, fmt.Sprintf("LIMIT %d", limit))
	}
	return renderSelect(changeColumns, tableResourceStates, true, c, tail...)
}

// newestIncarnationStatement finds the UID owning the most recent row in the
// window: the incarnation a query that named none of them means.
//
// It carries the identity and the window and deliberately *not* the actor
// predicates. Incarnation selection happens before filtering, because a filter
// applied first could change which incarnation looks newest — a name whose newest
// incarnation was only ever touched by an excluded actor would answer with the
// previous object's history, under the current object's name, with nothing in the
// output saying so (Invariant 7).
func newestIncarnationStatement(ref query.ObjectRef, from, to time.Time) statement {
	var c conditions
	identityConditions(&c, ref)
	windowConditions(&c, from, to)
	return renderSelect(incarnationColumn, tableResourceStates, true, c, "ORDER BY ts DESC", "LIMIT 1")
}

// incarnationsStatement renders the per-UID spans recorded under one name.
//
// The aggregate is grouped by uid alone: the identity columns are already pinned
// by the WHERE clause, and api_version is provenance rather than identity, so
// grouping on it would split one incarnation into two rows the day a resource was
// observed at two versions.
func incarnationsStatement(ref query.ObjectRef, from, to time.Time) statement {
	var c conditions
	identityConditions(&c, ref)
	windowConditions(&c, from, to)
	return renderSelect(incarnationColumns, tableResourceStates, true, c,
		"GROUP BY uid", "ORDER BY first_seen")
}

// replayStatement renders step 1 of the reconstruction recipe: one incarnation's
// history up to the target instant, oldest first.
//
// It reads the incarnation's whole recorded history rather than seeking to the
// base row first. The recipe in docs/SCHEMA.md is written that way, and following
// it literally is what keeps this implementation checkable against the document
// that specifies it; what checkpointing bounds is the *replay*, which is the step
// that is not idempotent, and that bound holds however many rows were read.
func replayStatement(ref query.ObjectRef, at time.Time, uid string) statement {
	var c conditions
	identityConditions(&c, ref)
	c.add("uid = ?", uid)
	c.add("ts <= ?", chTime(at))
	return renderSelect(replayColumns, tableResourceStates, true, c, "ORDER BY ts")
}

// newestIncarnationAtStatement finds the incarnation alive at or before an
// instant — the one an empty uid means in StateAt.
//
// It is the same question newestIncarnationStatement asks with a one-sided
// window, and it is a separate builder rather than a call with a zero From
// because the two reads mean different things: this one is anchored to an instant
// a caller named, and rendering it as a window would invite a later change to
// give it a lower bound it must not have.
func newestIncarnationAtStatement(ref query.ObjectRef, at time.Time) statement {
	var c conditions
	identityConditions(&c, ref)
	c.add("ts <= ?", chTime(at))
	return renderSelect(incarnationColumn, tableResourceStates, true, c, "ORDER BY ts DESC", "LIMIT 1")
}

// subjectMatch renders the predicate that matches one field of an Event's
// subject, whichever of the two API spellings recorded it.
//
// core v1 names the subject in involvedObject; events.k8s.io/v1 names it in
// regarding. coalesce over nullIf is the form docs/QUERIES.md publishes, and
// using the published form rather than a private one means a recipe an engineer
// pastes into clickhouse-client and the answer this backend gives are the same
// query.
func subjectMatch(field string) string {
	return fmt.Sprintf(
		"coalesce(nullIf(JSONExtractString(data, 'involvedObject', '%[1]s'), ''), "+
			"nullIf(JSONExtractString(data, 'regarding', '%[1]s'), '')) = ?", field)
}

// eventsStatement renders the Kubernetes Events naming one object.
//
// Two choices in here are worth stating, because both are departures from the
// obvious.
//
// The Event row's own namespace column is *not* constrained, though the published
// recipe constrains it. An Event lives in a namespace of its own choosing, and
// for a cluster-scoped object it is not the object's — which has no namespace at
// all. Pinning the column would therefore correlate nothing for exactly the
// objects whose events are hardest to find another way. The subject's namespace
// is read out of data instead, which is the value that actually answers the
// question.
//
// The subject is matched by (kind, namespace, name), and additionally by uid when
// the timeline has been pinned to one incarnation. Name is the forgiving key that
// still finds events for an object since recreated; uid is the exact one, and it
// is right to add precisely when the caller has already said which incarnation
// they mean.
func eventsStatement(ref query.ObjectRef, from, to time.Time, uid string, reverse bool) statement {
	var c conditions
	c.add("cluster_id = ?", ref.ClusterID)
	c.add(eventGroups)
	c.add("kind = ?", eventKind)
	windowConditions(&c, from, to)
	c.add(subjectMatch("kind"), ref.Kind)
	c.add(subjectMatch("namespace"), ref.Namespace)
	c.add(subjectMatch("name"), ref.Name)
	if uid != "" {
		c.add(subjectMatch("uid"), uid)
	}
	return renderSelect(changeColumns, tableResourceStates, true, c, orderByTS(reverse))
}

// coverageStatement renders the watch-scope transitions a coverage query is
// about, grouped by scope and oldest first within each.
//
// Three things it does not do, each for a reason.
//
// It carries no FINAL: watch_scopes is a plain MergeTree whose rows are written
// once each, so there is nothing to collapse and FINAL would be a cost with no
// return.
//
// It pushes no time bound, though the query carries one. An interval that merely
// overlaps the window is returned whole, so the transitions that opened and
// closed it may both sit outside the window — and a scope opened last year and
// still open would otherwise be reported as opening when the window did, which is
// a false statement about when the recorder started watching.
//
// Its namespace predicate has ScopeQuery's covering reading rather than the scope
// log's: a query for one namespace matches that namespace's own scope *and* the
// all-namespaces scope, because a cluster-wide rule genuinely was watching the
// object and answering "never observed" about it would be false.
func coverageStatement(q query.ScopeQuery) statement {
	var c conditions
	c.add("cluster_id = ?", q.ClusterID)
	if q.APIGroup != "" {
		c.add("api_group = ?", q.APIGroup)
	}
	if q.Kind != "" {
		c.add("kind = ?", q.Kind)
	}
	if q.Namespace != "" {
		c.add("(namespace = ? OR namespace = '')", q.Namespace)
	}
	return renderSelect(scopeColumns, tableWatchScopes, false, c,
		"ORDER BY api_group, kind, namespace, ts")
}
