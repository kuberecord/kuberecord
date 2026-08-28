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
func (e *Engine) Timeline(ctx context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	uid, err := e.resolveIncarnation(ctx, q)
	if err != nil {
		return nil, err
	}
	if uid == noIncarnation {
		return emptyIterator{}, nil
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
			return matchesFieldPaths(c, q.FieldPaths)
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
