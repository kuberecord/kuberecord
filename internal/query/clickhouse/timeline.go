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
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Timeline streams one object's recorded changes.
//
// The order of what happens here is the contract's, and it is load-bearing at one
// point in particular: the incarnation is resolved *before* any predicate is
// applied. A name that has been reused belongs to whichever incarnation owns the
// most recent row in the window, and letting an actor filter run first would let
// it choose a different one — answering with a deleted object's history, under
// the living object's name, with nothing in the output admitting the substitution
// (Invariant 7).
//
// It never returns ErrNoCoverage. Proving that nothing ever watched this object
// would mean a second read of the scope log on the flagship command, on the
// chance that the answer is empty; the contract explicitly allows an engine that
// cannot prove it to yield nothing and leave the distinction to Coverage, which
// is the call a caller makes precisely when a timeline came back empty
// (Invariant 9).
//
// What the resolution decides is which incarnation the *state* half is about, and
// nothing more. An object with no state rows still has a timeline when Events were
// asked for, because the two halves are independent queries (D40) — see
// eventsWithoutState, which is the path this used to return an empty iterator on.
func (e *Engine) Timeline(ctx context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	uid, err := e.resolveIncarnation(ctx, q)
	if err != nil {
		return nil, err
	}
	if uid == noIncarnation {
		return e.eventsWithoutState(ctx, q)
	}

	// A limit may only be pushed into SQL when nothing is left to apply
	// afterwards. Pushed down over a stream still awaiting a field-path predicate
	// or an Event merge, it would take the first n rows and *then* narrow them,
	// returning fewer changes than were asked for and the wrong ones at that.
	clientSide := len(q.FieldPaths) > 0
	pushLimit := 0
	if q.Limit > 0 && !clientSide && !q.IncludeEvents {
		pushLimit = q.Limit
	}

	stmt := timelineStatement(q, uid, pushLimit)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the timeline of %s: %w", describeRef(q.Ref), err)
	}

	var it query.ChangeIterator = &rowIterator{rows: rows}
	if clientSide {
		it = &filterIterator{inner: it, keep: func(c query.Change) bool {
			return query.MatchesFieldPaths(c, q.FieldPaths)
		}}
	}
	if q.IncludeEvents {
		it, err = e.mergeEvents(ctx, q, uid, it)
		if err != nil {
			return nil, err
		}
	}
	if q.Limit > 0 && pushLimit == 0 {
		it = &limitIterator{inner: it, limit: q.Limit}
	}
	return it, nil
}

// eventsWithoutState answers a timeline for an object whose own changes were never
// recorded.
//
// # No state rows is not no timeline
//
// An Event names its subject in its own row: the correlation reads involvedObject
// out of the Event's data and never consults the subject's rows at all. So the two
// halves of a merged timeline are independent queries, and the object half coming
// back empty says nothing whatever about the other one (D40). Returning an empty
// iterator here reported an answer that had never been measured.
//
// The case is not exotic — it is the quickstart's. A rule capturing Events,
// Deployments and ConfigMaps leaves every Pod in the namespace with Events and no
// history of its own, and a Pod is the first thing an engineer types after the
// Deployment they came for.
//
// # Why no suite objected
//
// Both this backend's stand-in connection and the command-line client's fake engine
// answer an Event query without requiring an incarnation, because that is the
// contract they were written against — and an early return taken *before* the query
// is issued is precisely what a fake does not model (D42). The shared agreement
// corpus now holds an object with Events and no state, which is where the two live
// engines are made to agree about it.
//
// # The uid, and the limit
//
// The uid handed to mergeEvents is the empty string, which eventsStatement reads as
// "no uid predicate" and leaves matching on the forgiving (kind, namespace, name)
// key. That is the right key and the only available one: an object with no
// incarnation has no incarnation to pin. A caller who pinned one never arrives here
// — resolveIncarnation hands a pinned UID straight back — so the narrowing a pinned
// timeline gets is unchanged.
//
// The limit is applied over the stream rather than pushed down, for the same reason
// the merged path applies it there: eventsStatement renders no LIMIT, and it should
// not learn one to serve this. The statement was already right for this question;
// what was missing was the call.
func (e *Engine) eventsWithoutState(
	ctx context.Context, q query.TimelineQuery,
) (query.ChangeIterator, error) {
	if !q.IncludeEvents {
		// Nobody asked about Events, so there is genuinely nothing to read: no rows
		// for the object, and no second question to answer. That is an empty result
		// and not a statement that nothing happened (Invariant 9).
		return emptyIterator{}, nil
	}

	// An exhausted changes side rather than a bespoke events-only iterator: the merge
	// carries the EventKubernetes stamp, the emission order, the failure wrapping and
	// the Close discipline, and a second path holding copies of those four is a second
	// path for one of them to be got wrong in.
	it, err := e.mergeEvents(ctx, q, "", emptyIterator{})
	if err != nil {
		return nil, err
	}
	if q.Limit > 0 {
		it = &limitIterator{inner: it, limit: q.Limit}
	}
	return it, nil
}

// noIncarnation is what resolveIncarnation returns when the window holds no rows
// for the object at all. It is spelled as a constant because the empty string
// also means "every incarnation" one line away, and two opposite meanings for one
// value is how a default query quietly becomes an AllIncarnations one.
const noIncarnation = "\x00none"

// resolveIncarnation decides which incarnation a timeline is about.
//
// A pinned UID wins outright, and AllIncarnations is ignored when one is set —
// the contract says so, and a backend honouring both would answer a question
// nobody asked. AllIncarnations yields the empty string, which the statement
// builder reads as "no uid predicate".
//
// Its result no longer decides whether the timeline is empty. noIncarnation says
// the window holds no rows for the object *itself*, which settles which incarnation
// the state half is about — there is none — and settles nothing at all about the
// Events naming it, since those are found by a query this one is not an input to
// (D40). See eventsWithoutState.
func (e *Engine) resolveIncarnation(ctx context.Context, q query.TimelineQuery) (string, error) {
	if q.UID != "" {
		return q.UID, nil
	}
	if q.AllIncarnations {
		return "", nil
	}

	stmt := newestIncarnationStatement(q.Ref, q.From, q.To)
	uid, err := e.scanOneString(ctx, stmt)
	if err != nil {
		return "", fmt.Errorf("finding the newest incarnation of %s: %w", describeRef(q.Ref), err)
	}
	if uid == "" {
		return noIncarnation, nil
	}
	return uid, nil
}

// scanOneString runs a statement projecting a single string column and returns
// the first row's value, or the empty string when there is no row.
//
// An empty result is not an error here: "this object has no rows in the window"
// is an ordinary answer, and the callers turn it into the empty result or the
// ErrObjectNotFound their own contract calls for.
func (e *Engine) scanOneString(ctx context.Context, stmt statement) (value string, err error) {
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return "", err
	}
	defer closeAfter(rows, &err)

	if rows.Next() {
		if scanErr := rows.Scan(&value); scanErr != nil {
			return "", scanErr
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return "", rowsErr
	}
	return value, nil
}

// closeAfter releases driver rows once a materializing read has finished with
// them, promoting a close failure into the read's own error when the read
// otherwise succeeded.
//
// Promoting rather than discarding it matters for a reader whose result is a
// list: a connection that failed on close may well have failed mid-stream too,
// and a short list returned with a nil error is a partial answer presented as a
// whole one. When the read has already failed the close failure is dropped, since
// it is usually a consequence of the first failure rather than news.
func closeAfter(rows driver.Rows, err *error) {
	closeErr := rows.Close()
	if closeErr != nil && *err == nil {
		*err = fmt.Errorf("releasing rows: %w", closeErr)
	}
}

// describeRef renders an identity for an error message: enough to find the object
// without pasting a struct into the output.
func describeRef(ref query.ObjectRef) string {
	group := ref.APIGroup
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s %s/%s", group, ref.Kind, ref.Namespace, ref.Name)
}
