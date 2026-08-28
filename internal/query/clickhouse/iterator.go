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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The iterators a timeline is assembled from.
//
// They are small, single-purpose and composed rather than merged into one type
// that knows every case, because the cases are independent: a stream may need
// client-side filtering, or an Event merge, or a limit applied after both, in any
// combination. A single iterator carrying flags for all three would make the
// interesting question — which of them is in play, and in what order — a matter
// of reading its Next method rather than of reading the composition.
//
// None of them starts a goroutine. A merge is done with one row of lookahead per
// side instead, which costs nothing and means an iterator abandoned part-way
// through has nothing to leak: Close releases driver rows and returns. Breaking
// out early is the normal path — every limited query and every "show me the last
// twenty" does it — so it must not be the path that leaks.

// rowIterator streams driver rows as changes.
//
// stamp, when set, overwrites the event type of every change it yields. It exists
// for the Event half of a merged timeline: an ingested Event is recorded as an
// ordinary object in its own right, so its rows carry Added or Modified, and only
// the merge knows that in *this* stream they are something a caller has to be
// able to tell apart from a change to the object it asked about.
type rowIterator struct {
	rows  driver.Rows
	stamp string

	cur    query.Change
	err    error
	closed bool
}

// Next advances to the next row, decoding it into a change.
//
// A failure mid-stream is recorded and reported through Err rather than through
// Next, which is the whole reason the contract's loop ends with an error check:
// Next returning false is ambiguous between "the result set ended" and "the
// backend died", and a caller that could not tell them apart would render a
// truncated audit history as a whole one.
func (it *rowIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if !it.rows.Next() {
		if err := it.rows.Err(); err != nil {
			it.err = fmt.Errorf("streaming %s: %w", tableResourceStates, err)
		}
		return false
	}
	change, err := scanChange(it.rows)
	if err != nil {
		it.err = err
		return false
	}
	if it.stamp != "" {
		change.EventType = it.stamp
	}
	it.cur = change
	return true
}

func (it *rowIterator) Change() query.Change { return it.cur }

func (it *rowIterator) Err() error { return it.err }

// Close releases the driver rows. It is guarded rather than delegated so that a
// caller which breaks out early *and* defers a Close — the documented shape — is
// not handed whatever a driver makes of a second close.
func (it *rowIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if err := it.rows.Close(); err != nil {
		return fmt.Errorf("releasing %s rows: %w", tableResourceStates, err)
	}
	return nil
}

// scanChange decodes one row into a change.
//
// Every scan target is freshly declared per call, so nothing an iterator hands
// out is a buffer it intends to overwrite. The contract insists on that, and it
// insists because the failure is quiet: a caller appending rows to a slice is
// doing an ordinary thing, and a recycled labels map would give every row the
// last row's labels with nothing anywhere reporting an error.
func scanChange(rows driver.Rows) (query.Change, error) {
	var (
		change query.Change
		labels map[string]string
		actors []string
	)
	err := rows.Scan(
		&change.TS, &change.EventType, &change.APIVersion, &change.UID, &change.ResourceVersion,
		&labels, &actors, &change.Data, &change.Diff, &change.SHA256,
	)
	if err != nil {
		return query.Change{}, fmt.Errorf("decoding a %s row: %w", tableResourceStates, err)
	}
	change.Labels = labels
	change.Actors = actors
	return change, nil
}

// filterIterator applies a client-side predicate to a stream.
type filterIterator struct {
	inner query.ChangeIterator
	keep  func(query.Change) bool
}

func (it *filterIterator) Next() bool {
	for it.inner.Next() {
		if it.keep(it.inner.Change()) {
			return true
		}
	}
	return false
}

func (it *filterIterator) Change() query.Change { return it.inner.Change() }
func (it *filterIterator) Err() error           { return it.inner.Err() }
func (it *filterIterator) Close() error         { return it.inner.Close() }

// limitIterator caps a stream at n changes.
//
// It takes the first n in the *emission* order, which is the order Reverse
// selects, because that is what the contract specifies and the other reading is
// both tempting and expensive: taking from the far end would mean reading the
// whole window and sorting it in memory, the exact cost a limit exists to avoid.
//
// It is only reached when the limit could not be pushed into SQL. When it is
// reached, satisfying it stops the read — the underlying iterator is left open
// for the caller's Close, which is the normal early break.
type limitIterator struct {
	inner query.ChangeIterator
	limit int
	seen  int
}

func (it *limitIterator) Next() bool {
	if it.seen >= it.limit {
		return false
	}
	if !it.inner.Next() {
		return false
	}
	it.seen++
	return true
}

func (it *limitIterator) Change() query.Change { return it.inner.Change() }
func (it *limitIterator) Err() error           { return it.inner.Err() }
func (it *limitIterator) Close() error         { return it.inner.Close() }

// emptyIterator is an exhausted stream.
//
// It is returned when the newest-incarnation probe finds no rows at all, and the
// distinction it preserves is the one Invariant 9 is about: no rows is an empty
// result, not a failure and not a statement that nothing happened. Which of those
// it is remains a question for Coverage, and this iterator is how the timeline
// declines to answer it.
type emptyIterator struct{}

func (emptyIterator) Next() bool           { return false }
func (emptyIterator) Change() query.Change { return query.Change{} }
func (emptyIterator) Err() error           { return nil }
func (emptyIterator) Close() error         { return nil }

// mergeIterator interleaves two streams by ts, in the direction both were read.
//
// One row of lookahead per side is the whole mechanism. It is synchronous by
// design: a goroutine-fed merge would need a cancellation path for the early
// break that limited queries take constantly, and would turn an abandoned
// iterator into a leak that only shows up under load.
//
// Ties go to the object's own change. When an Event lands at the same nanosecond
// as the change it describes, the change is the row the caller asked for and the
// Event is commentary on it, so the commentary reads better underneath.
type mergeIterator struct {
	changes query.ChangeIterator
	events  query.ChangeIterator
	reverse bool

	primed     bool
	changesOK  bool
	eventsOK   bool
	changesCur query.Change
	eventsCur  query.Change

	cur    query.Change
	closed bool
}

// prime fills both lookaheads before the first comparison.
func (it *mergeIterator) prime() {
	it.primed = true
	it.changesOK = it.changes.Next()
	if it.changesOK {
		it.changesCur = it.changes.Change()
	}
	it.eventsOK = it.events.Next()
	if it.eventsOK {
		it.eventsCur = it.events.Change()
	}
}

func (it *mergeIterator) Next() bool {
	if it.closed {
		return false
	}
	if !it.primed {
		it.prime()
	}
	// A failure on either side ends the merge, so that a half-failed stream is
	// never presented as a complete one. Err reports which side, unchanged.
	if it.changes.Err() != nil || it.events.Err() != nil {
		return false
	}
	switch {
	case it.changesOK && it.eventsOK:
		if it.eventsFirst() {
			it.cur = it.eventsCur
			it.eventsOK = it.events.Next()
			if it.eventsOK {
				it.eventsCur = it.events.Change()
			}
			return true
		}
		it.cur = it.changesCur
		it.changesOK = it.changes.Next()
		if it.changesOK {
			it.changesCur = it.changes.Change()
		}
		return true
	case it.changesOK:
		it.cur = it.changesCur
		it.changesOK = it.changes.Next()
		if it.changesOK {
			it.changesCur = it.changes.Change()
		}
		return true
	case it.eventsOK:
		it.cur = it.eventsCur
		it.eventsOK = it.events.Next()
		if it.eventsOK {
			it.eventsCur = it.events.Change()
		}
		return true
	default:
		return false
	}
}

// eventsFirst reports whether the pending Event precedes the pending change in
// the emission order. A tie is not "first": see the type's comment.
func (it *mergeIterator) eventsFirst() bool {
	if it.reverse {
		return it.changesCur.TS.Before(it.eventsCur.TS)
	}
	return it.eventsCur.TS.Before(it.changesCur.TS)
}

func (it *mergeIterator) Change() query.Change { return it.cur }

// Err reports the first side that failed. Either one failing is a failure of the
// merged stream: an Event half that died silently would leave the object's
// changes looking like the whole of what the cluster had to say.
func (it *mergeIterator) Err() error {
	if err := it.changes.Err(); err != nil {
		return err
	}
	return it.events.Err()
}

// Close releases both sides, reporting the first failure but always attempting
// the second — half a merge released is a leak with a plausible excuse.
func (it *mergeIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	changesErr := it.changes.Close()
	eventsErr := it.events.Close()
	if changesErr != nil {
		return changesErr
	}
	return eventsErr
}
