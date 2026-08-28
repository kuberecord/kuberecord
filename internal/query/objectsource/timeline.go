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
// touched only by the goroutine decoding it (see fetchObjects).
type timelineAccumulator struct {
	changes []query.Change
	marks   []incarnationMark
	events  []correlatedEvent
}

// merge folds one accumulator into another, which is what a scan's fold does with
// each object as its group is released.
//
// It is a method rather than three appends at each call site because there are now
// two scan orders — forward, and the newest-first walk — and a fold that forgot one
// of the three fields in one of them would produce a timeline missing its commentary
// or, worse, resolving the wrong incarnation, in exactly one query shape.
func (a *timelineAccumulator) merge(other *timelineAccumulator) {
	a.changes = append(a.changes, other.changes...)
	a.marks = append(a.marks, other.marks...)
	a.events = append(a.events, other.events...)
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
// The limit is applied last, and it bounds only the *answer*. It does not bound the
// work in general: there is no index to seek against, which is exactly what
// Capabilities.PointQuery being false declares. There is one shape where it can bound
// the reading too — a reverse-limited query, which is the flagship command's own —
// and scanNewestFirst is that path. Everything from the sort down is deliberately the
// same code either way, because the two must produce the same answer and the cheapest
// guarantee of that is that they share the assembly rather than agree about it.
func (e *Engine) scanTimeline(ctx context.Context, q query.TimelineQuery) ([]query.Change, error) {
	scan := recordScan{
		ref: q.Ref, from: q.From, to: q.To, retain: true, events: q.IncludeEvents,
		keep: func(c query.Change) bool {
			return query.MatchesActors(c, q.Actors, q.ExcludeActors) &&
				query.MatchesFieldPaths(c, q.FieldPaths)
		},
	}

	var collected timelineAccumulator
	var failure error
	if reverseLimited(q) {
		failure = e.scanNewestFirst(ctx, q, scan, &collected)
	} else {
		failure = scanPartitions(ctx, e, e.recordPrefixes(q.Ref.ClusterID, q.From, q.To),
			scan.decode, collected.merge)
	}
	if failure != nil {
		// Wrapped, not returned instead of the answer: whatever was read is delivered
		// and Err then says the result is short. A partial audit timeline that looks
		// complete is the worst available outcome.
		failure = fmt.Errorf("reading the timeline of %s: %w", describeRef(q.Ref), failure)
	}

	changes, marks, events := collected.changes, collected.marks, collected.events
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

// reverseLimited reports whether a query asks for the newest N changes, which is the
// one shape whose reading a limit can bound.
//
// Reverse without a limit still wants the whole window, and a limit without Reverse
// wants the *oldest* N — which the forward scan already reaches first. Only the pair
// names a suffix of the window, and only a suffix is provable without an index.
func reverseLimited(q query.TimelineQuery) bool { return q.Reverse && q.Limit > 0 }

// scanNewestFirst reads a reverse-limited query's window newest partition first and
// stops as soon as the answer it holds is settled.
//
// # Why this exists
//
// Task 11.3's flagship default is a hundred changes, newest first. Read forwards, that
// costs every partition in the window and discards almost all of it — a ninety-day
// question reads ninety days to render an afternoon. There is no index to seek with,
// but there is an *order*: partitions are contiguous and ascending (partitionSpans),
// so a walk from the newest end can reach a point where nothing left could matter.
//
// # The stopping rule, which is the same one the backward state walk uses
//
// An object's partition comes from its first record and it accepts records for at most
// one object span afterwards, so every record in a partition is newer than that
// partition's own start and older than its end plus a span. Partitions are contiguous,
// so once the walk has read down to the partition beginning at lo, every partition it
// has *not* read ends at or before lo — and therefore holds nothing at or after
// lo+span. That instant is the ceiling, and it is the identical inequality
// baseIsSettled applies to a reconstruction; it is written the other way round here
// only because this walk is bounded by a count rather than by a base row.
//
// The walk may stop when it holds at least Limit answer rows at or above the ceiling.
// Every unread row is *strictly* below it, so every unread row sorts strictly before
// all of them: the newest Limit of the full window and the newest Limit of what has
// been read are the same rows, in the same order. See answerIsSettled, which also
// settles the incarnation before the count is trusted.
//
// # Why the steps widen
//
// The first step reads one partition, because the common case — a busy object, a
// hundred changes wanted — is answered by the newest one. Each step that fails to
// settle doubles, up to the concurrency cap. A narrow first step is what makes the
// best case a single partition instead of a cap's worth; the doubling is what stops a
// sparse ninety-day archive from paying one round trip per partition to discover it
// has nothing to say.
//
// # What a failure does
//
// A per-object failure does not stop the walk and does not disqualify the short
// circuit, because an unreadable object in a partition the ceiling has excluded could
// not have held an answer row in the first place — the answer is complete whether or
// not that object was readable. The consequence, stated plainly: a failure living in a
// partition this walk proved irrelevant is not reported, where a full scan would have
// reported it. Nothing is missing from the answer, which is what Invariant 4 is about.
//
// A *listing* failure or a cancellation stops the walk where the forward scan would
// also have stopped, and what was read is still delivered.
func (e *Engine) scanNewestFirst(
	ctx context.Context, q query.TimelineQuery, scan recordScan, into *timelineAccumulator,
) error {
	spans := e.recordPartitions(q.Ref.ClusterID, q.From, q.To)

	// One bucket per step, newest step first. They are kept apart rather than folded
	// as they arrive because the assembly downstream is order-sensitive: a stable sort
	// keeps same-nanosecond rows in the order they were accumulated, and that order has
	// to be the forward scan's — ascending by partition, then by key — or two rows
	// recorded at the same instant would come back in a different order from the same
	// query asked without a limit.
	var steps []timelineWalkStep

	end := len(spans)
	for width := 1; end > 0; width = min(width*2, e.concurrency) {
		start := max(0, end-width)
		var step timelineWalkStep
		failure, abort := scanOneGroup(ctx, e, prefixesOf(spans[start:end]), scan.decode, step.acc.merge)
		step.failure = failure
		steps = append(steps, step)
		end = start
		if abort || e.answerIsSettled(q, steps, spans[start].start) {
			break
		}
	}

	// Back into ascending partition order. Each step already holds its own partitions
	// ascending, and the steps cover contiguous ranges, so reversing the steps restores
	// exactly the order a forward scan would have folded in — whatever widths the walk
	// happened to use.
	slices.Reverse(steps)

	var failure error
	for i := range steps {
		into.merge(&steps[i].acc)
		if steps[i].failure != nil && failure == nil {
			// First in ascending partition order, which is the forward scan's own rule.
			// An archive must be described the same way every time it is read, and the
			// order the walk visited its partitions in is not a property of the archive.
			failure = steps[i].failure
		}
	}
	return failure
}

// timelineWalkStep is one step of the newest-first walk: what its partitions held, and
// what went wrong reading them.
type timelineWalkStep struct {
	acc     timelineAccumulator
	failure error
}

// answerIsSettled reports whether a newest-first walk that has read down to the
// partition beginning at lo may stop.
//
// Two things have to be settled, and the incarnation is the one that is easy to miss.
// resolveIncarnation picks the UID owning the newest *mark* in the window, before any
// predicate is applied, and a walk that stopped while an unread partition could still
// hold a newer mark would resolve it from a partial view — answering with one
// incarnation's history under a name that had since been reused, which is the splice
// Invariant 7 forbids. So: the short circuit does not merely avoid the question, it
// answers it. A mark at or above the ceiling cannot be outranked by anything unread,
// because everything unread is strictly below the ceiling, so the UID chosen here is
// provably the UID a full scan would have chosen. When no mark reaches the ceiling the
// walk simply keeps going.
//
// The count that follows is of *answer* rows, not of collected ones: the incarnation
// filter and the commentary merge both change how many rows a limit will find, and a
// walk that counted raw changes would stop early on a query whose filter excludes most
// of them. Predicates need no attention here — recordScan.keep applied them at decode
// time, so a collected change has already survived them.
func (e *Engine) answerIsSettled(q query.TimelineQuery, steps []timelineWalkStep, lo time.Time) bool {
	ceiling := lo.Add(e.objectSpan)

	var uid string
	switch {
	case q.UID != "":
		uid = q.UID
	case q.AllIncarnations:
		uid = ""
	default:
		newest, found := newestMark(steps)
		if !found || newest.ts.Before(ceiling) {
			return false
		}
		uid = newest.uid
	}

	rows := 0
	for i := range steps {
		for _, change := range steps[i].acc.changes {
			if (uid == "" || change.UID == uid) && !change.TS.Before(ceiling) {
				rows++
			}
		}
		if !q.IncludeEvents {
			continue
		}
		for _, event := range steps[i].acc.events {
			// The same narrowing mergeCommentary performs: commentary is pinned to the
			// resolved incarnation, and left alone when the timeline spans every one.
			if (uid == "" || event.subjectUID == uid) && !event.change.TS.Before(ceiling) {
				rows++
			}
		}
	}
	return rows >= q.Limit
}

// newestMark returns the mark a full scan's resolveIncarnation would land on, given
// only the steps read so far.
//
// The tie-break is the reason this is not a plain maximum. resolveIncarnation sorts
// marks by ts with a *stable* sort and takes the last, so among marks recorded at the
// same nanosecond it takes the one accumulated last — which is the one from the latest
// partition, then the latest key. Walking the steps in ascending partition order and
// preferring the later of two equals reproduces that exactly. Taking a strict maximum
// instead would pick the earliest of the tied marks and could resolve a different
// incarnation from the one the same query answers without a limit.
func newestMark(steps []timelineWalkStep) (incarnationMark, bool) {
	var newest incarnationMark
	found := false
	for i := len(steps) - 1; i >= 0; i-- {
		for _, mark := range steps[i].acc.marks {
			if !found || !mark.ts.Before(newest.ts) {
				newest, found = mark, true
			}
		}
	}
	return newest, found
}

// prefixesOf is the prefix-only view of a run of partitions, which is what a scan
// takes.
func prefixesOf(spans []partition) []string {
	prefixes := make([]string, len(spans))
	for i := range spans {
		prefixes[i] = spans[i].prefix
	}
	return prefixes
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

// recordPartitions is recordPrefixes with each partition's own window start beside it,
// which is what a walk that reasons about the partitions it has *not* read needs.
func (e *Engine) recordPartitions(clusterID string, from, to time.Time) []partition {
	return partitionSpans(recordsRoot(e.prefix, clusterID), from, to, e.objectSpan)
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
