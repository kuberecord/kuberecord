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
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// incarnationMark is what a scan remembers about every line naming the object,
// including the lines it did not keep.
//
// It exists because the incarnation must be resolved *before* any predicate is
// applied. A name that has been reused belongs to whichever incarnation owns the
// most recent line in the window, and resolving that from the filtered lines would
// let an actor filter choose a different one — answering with a deleted object's
// history under the living object's name, with nothing in the output admitting the
// substitution (Invariant 7).
//
// It is a mark rather than the line itself so that a filtered-out line is still
// discarded: two strings and an instant per line naming the object, against the
// object's whole recorded state.
type incarnationMark struct {
	uid     string
	ts      time.Time
	deleted bool
}

// correlatedEvent is a Kubernetes Event that named the object, and the incarnation
// it named.
//
// The subject's uid travels beside the change because the change describes the
// *Event* — every field of a merged row does — so there would otherwise be nowhere
// to record which incarnation the commentary was about.
type correlatedEvent struct {
	change     query.Change
	subjectUID string
}

// timelineAccumulator is what one object contributed to a scan. One per object,
// touched only by the goroutine decoding it (see scanObjects).
type timelineAccumulator struct {
	changes []query.Change
	marks   []incarnationMark
	events  []correlatedEvent
}

// recordScan is the per-line decision a timeline or incarnation scan makes.
type recordScan struct {
	ref      query.ObjectRef
	from, to time.Time

	// keep is the predicate applied to a line that names the object, after the
	// window and before retention. It is applied here, at decode time, rather than
	// to a collected result, because that is what makes "a non-matching line is
	// discarded without being retained" true of the memory and not only of the
	// answer. nil keeps everything.
	keep func(query.Change) bool

	// retain is false for a scan that only needs to know which incarnations exist,
	// which then holds marks and no state at all.
	retain bool

	// events asks for Kubernetes Events naming the object to be collected too.
	events bool
}

// decode reads one object and accumulates what it holds for this scan.
func (s recordScan) decode(acc *timelineAccumulator, body io.Reader) error {
	return decodeFrame(body, func(line *recordLine) error {
		if !s.inWindow(line.Timestamp) {
			return nil
		}
		switch {
		case line.namesObject(s.ref):
			acc.marks = append(acc.marks, incarnationMark{
				uid: line.UID, ts: line.Timestamp, deleted: line.EventType == query.EventDeleted,
			})
			if !s.retain {
				return nil
			}
			change := line.change()
			if s.keep != nil && !s.keep(change) {
				return nil
			}
			acc.changes = append(acc.changes, change)
		case s.events && line.ClusterID == s.ref.ClusterID && line.isEvent():
			subject := line.subject()
			if !subject.namesTarget(s.ref) {
				return nil
			}
			change := line.change()
			// EventKubernetes is stamped because Change carries no other way to say
			// it: an ingested Event is an ordinary object with its own history, so its
			// lines record Added or Modified, and without the stamp a reader could not
			// tell a row *about* the object from a row about something that happened
			// to it.
			change.EventType = query.EventKubernetes
			acc.events = append(acc.events, correlatedEvent{change: change, subjectUID: subject.UID})
		}
		return nil
	})
}

// inWindow reports whether an instant falls in the scan's window, inclusive. A zero
// bound is unbounded on that side, which only the backward walk of a state
// reconstruction uses.
func (s recordScan) inWindow(ts time.Time) bool {
	if !s.from.IsZero() && ts.Before(s.from) {
		return false
	}
	return s.to.IsZero() || !ts.After(s.to)
}

// Timeline streams one object's recorded changes.
//
// It returns immediately: the scan runs on the first Next, which is what lets a
// failure part-way through reach the caller through Err — beside the changes that
// were read before it — rather than replacing the whole answer with an error
// (Invariant 4). The refusals that are *not* about the data, on the other hand,
// happen here, because a caller that has asked an unanswerable question should
// learn so before it starts rendering.
//
// It never returns ErrNoCoverage. Proving nothing ever watched this object would
// mean reading the scope log on every timeline, on the chance the answer is empty;
// the contract allows an engine that cannot prove it to yield nothing and leave the
// distinction to Coverage, which is the call a caller makes precisely when a
// timeline came back empty (Invariant 9).
func (e *Engine) Timeline(ctx context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if err := requireWindow(q.From, q.To); err != nil {
		return nil, fmt.Errorf("reading the timeline of %s: %w", describeRef(q.Ref), err)
	}
	return &changeIterator{scan: func() ([]query.Change, error) { return e.scanTimeline(ctx, q) }}, nil
}

// scanTimeline performs the one pass a timeline costs, and assembles its answer.
//
// The order of the steps is the contract's: read, order, resolve the incarnation,
// then filter by it, then merge commentary, then reverse, then limit. Two of them
// are worth stating because the obvious alternative is wrong.
//
// The changes are ordered by a stable sort over the whole window rather than by
// emitting each partition as it is read. Partitions overlap: an object's partition
// comes from its first record, so a record in the hour=09 object can be newer than
// one in hour=10, and emitting per partition would produce a timeline that is
// almost in order. "Almost" puts an effect before its cause.
//
// The limit is applied last, and it bounds only the *answer*. Nothing about it
// bounds the work: there is no index to stop reading early against, which is
// exactly what Capabilities.PointQuery being false declares. A caller that wants
// the cost bounded bounds the window.
func (e *Engine) scanTimeline(ctx context.Context, q query.TimelineQuery) ([]query.Change, error) {
	scan := recordScan{
		ref: q.Ref, from: q.From, to: q.To, retain: true, events: q.IncludeEvents,
		keep: func(c query.Change) bool {
			return query.MatchesActors(c, q.Actors, q.ExcludeActors) &&
				query.MatchesFieldPaths(c, q.FieldPaths)
		},
	}

	var (
		changes []query.Change
		marks   []incarnationMark
		events  []correlatedEvent
	)
	failure := scanPartitions(ctx, e, e.recordPrefixes(q.Ref.ClusterID, q.From, q.To), scan.decode,
		func(acc *timelineAccumulator) {
			changes = append(changes, acc.changes...)
			marks = append(marks, acc.marks...)
			events = append(events, acc.events...)
		})
	if failure != nil {
		// Wrapped, not returned instead of the answer: whatever was read is delivered
		// and Err then says the result is short. A partial audit timeline that looks
		// complete is the worst available outcome.
		failure = fmt.Errorf("reading the timeline of %s: %w", describeRef(q.Ref), failure)
	}

	slices.SortStableFunc(changes, byChangeTS)
	slices.SortStableFunc(marks, func(a, b incarnationMark) int { return a.ts.Compare(b.ts) })

	uid, recorded := resolveIncarnation(q, marks)
	if !recorded {
		// Nothing named this object in the window. That is an empty result and not a
		// statement that nothing happened — see Coverage, and Invariant 9.
		return nil, failure
	}
	if uid != "" {
		changes = slices.DeleteFunc(changes, func(c query.Change) bool { return c.UID != uid })
	}
	if q.IncludeEvents {
		changes = mergeCommentary(changes, events, uid)
	}
	if q.Reverse {
		// Reversing the ascending order rather than sorting descending, so that two
		// changes recorded at the same nanosecond appear in mirrored order rather
		// than in whichever order a second sort happened to leave them.
		slices.Reverse(changes)
	}
	if q.Limit > 0 && len(changes) > q.Limit {
		changes = changes[:q.Limit]
	}
	return changes, failure
}

// resolveIncarnation decides which incarnation a timeline is about, and reports
// whether the window recorded the object at all.
//
// A pinned UID wins outright, and AllIncarnations is ignored when one is set — the
// contract says so, and a backend honouring both would answer a question nobody
// asked. Otherwise it is the incarnation owning the newest mark, which is what "the
// newest incarnation in the window" means and is why the marks are sorted first.
func resolveIncarnation(q query.TimelineQuery, marks []incarnationMark) (uid string, recorded bool) {
	switch {
	case q.UID != "":
		return q.UID, true
	case q.AllIncarnations:
		return "", true
	case len(marks) == 0:
		return "", false
	default:
		return marks[len(marks)-1].uid, true
	}
}

// mergeCommentary interleaves the Kubernetes Events naming the object into its own
// changes, in ts order.
//
// The events are narrowed to the resolved incarnation when there is one, and left
// alone when the timeline spans every incarnation: name is the forgiving key that
// still finds the events of an object since recreated, and uid is the exact one,
// which is right to add precisely when the caller has already said which
// incarnation they mean.
//
// The actor predicates deliberately did not reach here. An Event's actors are the
// field managers of the Event object — the controller that wrote it, never whoever
// changed the object it is about — so filtering commentary by them would empty the
// Event half of almost every filtered timeline, and show the reader "Kubernetes said
// nothing" about an incident Kubernetes had plenty to say about (Invariant 4).
// Field-path predicates need no exception: an Event line carries no diff, and a row
// with no patch survives such a filter by the same rule that keeps a first sighting.
//
// Ties go to the object's own change. When an Event lands at the same nanosecond as
// the change it describes, the change is the row the caller asked for and the Event
// is commentary on it, so the commentary reads better underneath.
func mergeCommentary(changes []query.Change, events []correlatedEvent, uid string) []query.Change {
	commentary := make([]query.Change, 0, len(events))
	for _, event := range events {
		if uid != "" && event.subjectUID != uid {
			continue
		}
		commentary = append(commentary, event.change)
	}
	if len(commentary) == 0 {
		return changes
	}
	slices.SortStableFunc(commentary, byChangeTS)

	merged := make([]query.Change, 0, len(changes)+len(commentary))
	next := 0
	for _, change := range changes {
		for next < len(commentary) && commentary[next].TS.Before(change.TS) {
			merged = append(merged, commentary[next])
			next++
		}
		merged = append(merged, change)
	}
	return append(merged, commentary[next:]...)
}

// Incarnations lists the distinct UIDs recorded for an object in a window.
//
// It exists so that a caller can say "there are two other incarnations of this
// name" before rendering one of them: a timeline that shows one incarnation without
// admitting the others is the splice Invariant 7 forbids, told quietly.
//
// Unlike Timeline it cannot deliver a partial answer, so a scan that failed part-way
// is returned as a failure rather than as a shorter list. A list of incarnations is
// read as complete — it is the answer to "how many were there" — and a short one
// would say there was one object where there were two.
func (e *Engine) Incarnations(
	ctx context.Context, ref query.ObjectRef, from, to time.Time,
) ([]query.Incarnation, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if err := requireWindow(from, to); err != nil {
		return nil, fmt.Errorf("listing the incarnations of %s: %w", describeRef(ref), err)
	}

	// retain is false: this question is answered entirely from the marks, so the
	// scan decodes every object and keeps no object state at all.
	scan := recordScan{ref: ref, from: from, to: to}
	var marks []incarnationMark
	err := scanPartitions(ctx, e, e.recordPrefixes(ref.ClusterID, from, to), scan.decode,
		func(acc *timelineAccumulator) { marks = append(marks, acc.marks...) })
	if err != nil {
		return nil, fmt.Errorf("listing the incarnations of %s: %w", describeRef(ref), err)
	}

	slices.SortStableFunc(marks, func(a, b incarnationMark) int { return a.ts.Compare(b.ts) })
	return spansOf(marks), nil
}

// spansOf reduces ts-ordered marks to one entry per incarnation, oldest first by
// FirstSeen.
//
// Deleted is reported from the marks rather than assumed, even though this format
// never receives a deletion. The engine declares Deletions false and the archive
// bears that out; hard-coding false here as well would make the declaration and the
// data two independent claims, and the day one of them changed the other would keep
// asserting the old answer.
func spansOf(marks []incarnationMark) []query.Incarnation {
	spans := make([]query.Incarnation, 0, len(marks))
	at := make(map[string]int, len(marks))
	for _, mark := range marks {
		if i, seen := at[mark.uid]; seen {
			spans[i].LastSeen = mark.ts
			spans[i].Deleted = spans[i].Deleted || mark.deleted
			continue
		}
		at[mark.uid] = len(spans)
		spans = append(spans, query.Incarnation{
			UID: mark.uid, FirstSeen: mark.ts, LastSeen: mark.ts, Deleted: mark.deleted,
		})
	}
	return spans
}

// recordPrefixes is the pruned partition range a question about one cluster's
// records resolves to.
func (e *Engine) recordPrefixes(clusterID string, from, to time.Time) []string {
	return partitionPrefixes(recordsRoot(e.prefix, clusterID), from, to, e.objectSpan)
}

// byChangeTS orders changes by the instant they were recorded, to the nanosecond.
// Used with a stable sort throughout, so that two changes recorded at the same
// nanosecond keep the order the archive listed them in rather than an arbitrary one.
func byChangeTS(a, b query.Change) int { return a.TS.Compare(b.TS) }

// changeIterator serves an assembled result, and the failure that cut it short.
//
// # Why the result is assembled rather than streamed
//
// The contract's cursor exists because a result set is unbounded in principle, and
// this engine honours the shape while filling it in one pass. It cannot do
// otherwise: "the newest incarnation in the window" is not knowable until the window
// has been read, so an engine that emitted as it went would have emitted the older
// incarnation's changes before learning they were not the answer. What the buffer
// holds is the object's own filtered history — never the archive, and never a
// function of how many objects were scanned, which is the property the memory
// benchmark pins.
//
// # Why a failure does not replace the answer
//
// A scan that failed on its fortieth object still read thirty-nine. Those changes
// are delivered and Err then reports the failure, which is exactly the shape the
// contract's loop is written for: Next returning false is ambiguous, and the check
// after the loop is what distinguishes an exhausted result from a truncated one. The
// alternative — discarding what was read — would answer a question about an outage
// with nothing at all, having held the rows that explained it.
type changeIterator struct {
	// scan is the one-shot pass, run on the first Next and dropped afterwards.
	scan func() ([]query.Change, error)

	changes []query.Change
	next    int
	cur     query.Change
	err     error
	closed  bool
}

// Next advances to the next change. See query.ChangeIterator.
func (it *changeIterator) Next() bool {
	if it.closed {
		return false
	}
	if it.scan != nil {
		it.changes, it.err = it.scan()
		it.scan = nil
	}
	if it.next >= len(it.changes) {
		return false
	}
	it.cur = it.changes[it.next]
	it.next++
	return true
}

// Change returns the change Next advanced to. Its maps and slices belong to the
// caller: each was decoded for one line and is never written to again.
func (it *changeIterator) Change() query.Change { return it.cur }

// Err returns what cut the result short, or nil if it was complete.
func (it *changeIterator) Err() error { return it.err }

// Close releases the assembled result. It holds no goroutines and no open readers —
// a scan joins every fetch before it returns — so breaking out of a loop early costs
// nothing, which is the normal path for every limited query. Calling it more than
// once is safe.
func (it *changeIterator) Close() error {
	it.closed = true
	it.scan = nil
	it.changes = nil
	return nil
}

// Compile-time proof that the iterator satisfies the contract, asserted where it
// lives rather than at the call site that would otherwise discover a drift.
var _ query.ChangeIterator = (*changeIterator)(nil)
