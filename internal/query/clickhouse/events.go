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

	"github.com/kuberecord/kuberecord/internal/query"
)

// mergeEvents interleaves the Kubernetes Events naming this object into its
// change stream, in ts order.
//
// # Both spellings, always
//
// v1/Event and events.k8s.io/v1/Event are one storage behind two APIs, and a
// cluster's rules may name either. The subject predicate reads involvedObject and
// regarding through one coalesce, so a caller never has to know which spelling
// captured a given Event — and, more to the point, never silently loses the half
// that happened to be captured the other way (Task 3.1).
//
// # Why the actor predicates do not reach here
//
// An Event's actors column holds the field managers of the *Event object*: the
// controller that wrote the Event, not whoever changed the object it is about.
// Applying an actor filter to it would empty the Event half of almost every
// filtered timeline, and the reader would be shown "Kubernetes said nothing"
// about an incident Kubernetes had plenty to say about — a silence manufactured
// by a predicate that was never about Events (Invariant 4).
//
// So the actor predicates narrow the object's own changes and leave the
// commentary alone. The window, the ordering and the limit apply to the merged
// stream exactly as they do to an unmerged one, because those are properties of
// the question rather than of who authored a row.
//
// Field-path predicates need no exception: an Event row carries no diff, and a
// row with no patch survives a field-path filter by the same rule that keeps a
// first sighting in one.
func (e *Engine) mergeEvents(
	ctx context.Context, q query.TimelineQuery, uid string, changes query.ChangeIterator,
) (query.ChangeIterator, error) {
	// uid is either one the caller pinned, one the newest-incarnation probe
	// resolved, or empty for an all-incarnations timeline — Timeline has already
	// short-circuited the case where nothing was recorded at all. Empty falls back
	// to the forgiving key, (kind, namespace, name), which is the right one for a
	// question that spans a delete-and-recreate.
	stmt := eventsStatement(q.Ref, q.From, q.To, uid, q.Reverse)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		if closeErr := changes.Close(); closeErr != nil {
			return nil, fmt.Errorf("reading the events naming %s: %w (and releasing the change stream: %v)",
				describeRef(q.Ref), err, closeErr)
		}
		return nil, fmt.Errorf("reading the events naming %s: %w", describeRef(q.Ref), err)
	}

	// EventKubernetes is stamped on every merged row because Change carries no
	// other way to say it. An ingested Event is an ordinary object with its own
	// history, so its rows arrive as Added or Modified; every field of a merged row
	// but this one describes the Event rather than the target, and without the
	// stamp a reader could not tell a row *about* the object from a row about
	// something that happened to it.
	events := &rowIterator{rows: rows, stamp: query.EventKubernetes}
	return &mergeIterator{changes: changes, events: events, reverse: q.Reverse}, nil
}
