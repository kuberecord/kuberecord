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

package cli

import (
	"context"
	"errors"
	"slices"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/coldscan"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `timeline` in its structured renderings, where the answer is not gathered
// first.
//
// # Why this is a second path and not a rendering of the first
//
// The tabular path collects every row before it renders one, and it has to: the
// value each patch destroyed is recovered by replaying the object's state over
// the whole consecutive run, and a table cannot be laid out until its widest cell
// is known. Neither of those applies here. A structured item is the schema's own
// columns, exactly as the backend returned them, so nothing downstream needs a
// row that has already gone past — which is what makes `jsonl` able to keep its
// promise that memory does not scale with the result.
//
// The risk in having two paths is Invariant 9: a second sequence is a second
// place for the coverage consultation to be dropped. So the pieces that carry the
// invariant are the *same* functions the gathered path calls — timelineBounds,
// selectIncarnation, askCoverage, deletionsNotice, explainNoChanges, eventsNotice —
// and only the middle, where rows are turned into output, differs. What is
// duplicated here is the order they are called in, and that order is asserted by
// tests over both paths rather than by a comment.
//
// # Where the memory actually goes
//
// One case cannot stream, and it is bounded rather than hidden. The default
// rendering is oldest first, while --limit selects the *newest* N — so with a
// limit in force, which is the default, the newest N have to be read before the
// oldest of them can be written. The buffer is therefore at most --limit items:
// bounded by a number the user typed, never by the size of the result. --limit 0
// makes the two orderings select the same set, so the query is simply asked
// oldest-first and nothing is held at all, and --reverse asks for the newest
// first, which is the order the backend already emits.

// runTimelineStructured answers a timeline request into the versioned envelope.
//
// It writes the document to stdout as it goes and every qualification of it to
// stderr at the end, which is the same split the tabular rendering keeps. The
// notices arrive after the document rather than before it because some of them —
// whether the result was empty, whether a deletion was seen, whether --with-events
// interleaved anything — are facts about rows that had not been read yet when the
// first byte was written. That is the cost of streaming, and it is paid on the
// stream nothing is piping.
func runTimelineStructured(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	capabilities := backend.Engine.Capabilities()

	from, to, windowNotice := timelineBounds(request, capabilities, opts.Zone)
	notices := appendNotice(nil, windowNotice)

	// Same position as the gathered path's, and for the same reason: it qualifies
	// the question rather than the answer, so it leads what is written to stderr.
	notices = appendNotice(notices, inertPredicateNotice(request))

	// Same position as the gathered path's, and for the same reason: this is before
	// the first query of the invocation, incarnation listing included. See
	// coldscan.Begin.
	scan, err := coldscan.Begin(ctx, backend, request.Scan, request.Ref.ClusterID, from, to, streams)
	if err != nil {
		return err
	}
	defer scan.Stop()
	ctx = scan.Ctx

	selection, selectionNotices := chooseIncarnation(ctx, backend.Engine, request, from, to)
	notices = append(notices, selectionNotices...)

	// Same position as the gathered path's, and for the same reason: the walk starts
	// from the incarnation just resolved, and the subjects it finds are a predicate
	// of the timeline query below. See resolveOwnedTree.
	owned, err := resolveOwnedTree(ctx, backend, request, selection, from, to, opts.Zone)
	if err != nil {
		return err
	}
	request.Subjects = owned.subjects
	notices = append(notices, owned.notices...)

	// The scope the rows will come from, which under --events-only is the Events'
	// rather than the object's — and which reaches metadata.coverage in the envelope
	// as well as the table's header. A script comparing two runs is owed the same
	// substitution a reader is (relevantCoverage): the intervals it carries name the
	// kind they are about, so the narrowing is legible there rather than only in the
	// prose summary beside them.
	coverage, err := relevantCoverage(ctx, backend, request, from, to)
	if err != nil {
		return err
	}

	stream, err := render.NewStream(
		streams.Out, request.Structured, envelopeHead(backend, render.KindTimeline, coverage))
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}

	emitted, emitErr := emitChanges(
		ctx, backend.Engine, request, request.timelineQuery(selection, from, to), stream)

	// Asked before the scan is stopped, because Stop cancels the context every
	// query of a cold read must be issued with — and these may walk partitions
	// like any other. They are skipped when the emission failed, since the notices
	// would be explaining the shape of an answer that was never produced.
	//
	// explainNoChanges is called here rather than beside the notices it belongs to,
	// and that is Task 18.5's gate arriving on this path. Its answer decides whether
	// the Event question is asked at all — the no-coverage finding absorbs the
	// --with-events explanation, and two findings for one cause is noise — so it has
	// to run before eventsNotice, which in turn has to run before the context is
	// cancelled. It may ask that question itself, which is the other reason it
	// belongs above the line rather than below it: since Task 18.7 the absorbed
	// clause measures the Event scope instead of assuming it, and its read is a
	// query like any other and must be issued while the cold-scan context is alive.
	// The two are still exclusive, so the invocation pays for one of them at most.
	// The gathered path's gate is the same condition in the same place relative to
	// the same two calls (gatherChanges).
	var (
		predicate    render.Notice
		attributed   bool
		emptyNotices []render.Notice
		emptyErr     error
		eventNotice  render.Notice
	)
	switch {
	case emitErr != nil:
		// Nothing to explain. Every notice below is about the shape of an answer,
		// and this invocation did not produce one.
	case request.EventsOnly:
		// The trap, closed on this path too, and in the same words: everything the
		// other branch does is about the object's own changes, which this question
		// excluded — and its last step is the no-coverage finding, which would exit 3
		// over the absence of state the reader had just declined. What is left is the
		// question that was asked, answered from the Event coverage already in hand.
		// See gatherChanges, whose branch this mirrors.
		eventNotice = noEventsNotice(
			request, from, to, coverage, nil, emitted.sawEvent, opts.Zone)
	default:
		predicate, attributed = predicateNotice(
			ctx, backend.Engine, request, selection, from, to, emitted.sawChange, opts.Zone)
		if !attributed {
			emptyNotices, emptyErr = explainNoChanges(request, from, to, emitted.shape(), coverage,
				func() (coverageAnswer, error) {
					return eventCoverage(ctx, backend, request, from, to)
				}, opts.Zone)
		}
		if emptyErr == nil {
			eventNotice = eventsNotice(ctx, backend, request, from, to, emitted.sawEvent, opts.Zone)
		}
	}

	// Stopped here rather than left to the defer: the reading is over, and the
	// progress line has to be off the terminal before the notices below are written
	// or a short notice lands on top of a longer line and leaves its tail behind.
	// stop is idempotent, so the defer above remains the early-return path's.
	scan.Stop()
	// Closed on every path. For the whole-document formats nothing has reached
	// stdout until it runs, so returning early on a failed emission would turn a
	// backend that died halfway into a command that produced no document at all
	// rather than the head and the rows it had.
	if closeErr := stream.Close(); closeErr != nil && emitErr == nil {
		emitErr = exit.RuntimeErrorf("%w", closeErr)
	}
	if emitErr != nil {
		return errors.Join(emitErr, render.WriteNotices(streams.ErrOut, notices, opts))
	}

	notices = appendNotice(notices, deletionsNotice(capabilities, emitted.sawDeleted))

	// Same order and same gates as the gathered path's, which is the half of this
	// file that is duplicated on purpose: an emptiness a query predicate produced
	// is explained by the predicate rather than by coverage, and consulting
	// coverage about it could report "nothing was watching" over a window that
	// demonstrably held changes. The Event notice is empty when the finding above
	// absorbed it, which is decided where it is asked.
	notices = appendNotice(notices, predicate)
	notices = append(notices, emptyNotices...)
	notices = appendNotice(notices, eventNotice)

	if writeErr := render.WriteNotices(streams.ErrOut, notices, opts); writeErr != nil {
		return exit.RuntimeErrorf("%w", writeErr)
	}
	return emptyErr
}

// emission is what a streamed answer turns out to have been, once it is gone.
//
// Every field is a fact about rows that have already been written to stdout and
// are no longer held anywhere, and each one is read by a notice the gathered path
// works out from the rows themselves — see sawDeletion and sawEvent, which is why
// both of those are functions over an enum value rather than inlined comparisons.
//
// It is a struct rather than three return values because it is one answer to one
// question, and because a third positional bool beside a second is the shape a
// caller eventually passes in the wrong order.
type emission struct {
	// items is how many envelope items were written.
	items int
	// sawDeleted reports whether a deletion was among them.
	sawDeleted bool
	// sawEvent reports whether a merged Kubernetes Event was.
	sawEvent bool
	// sawChange reports whether a change to the object itself was — which is not
	// the complement of items, since a --with-events document can be made
	// entirely of Event rows. See sawChange, whose question this answers.
	sawChange bool
}

// emitChanges runs the query and writes each change into the envelope.
//
// It reports what the answer turned out to be — how many items, and which kinds
// of row were among them — because those are the facts the notices need and the
// ones a streaming path cannot recover afterwards from rows it no longer holds.
//
// Err is checked after the loop and Close called on every path, for the reason
// collectChanges does both: skipping either turns a backend that failed halfway
// into a result that looks complete and merely short, which for an audit timeline
// is the worst available outcome.
func emitChanges(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest,
	q query.TimelineQuery, stream *render.Stream,
) (emitted emission, err error) {
	hold := holdForDisplayOrder(request)
	if !hold {
		// The query is asked in the order the output is written in, so nothing has
		// to be held back. The two spellings of Reverse mean the same thing —
		// newest first — so this is an assignment rather than a negation. See the
		// file comment for why --limit 0 selects the same changes either way.
		q.Reverse = request.Reverse
	}

	iterator, err := engine.Timeline(ctx, q)
	if err != nil {
		return emission{}, timelineQueryError(ctx, request, err)
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil && err == nil {
			err = exit.RuntimeErrorf("releasing the change stream: %w", closeErr)
		}
	}()

	var held []query.Change
	for iterator.Next() {
		change := iterator.Change()
		emitted.observe(change)
		if hold {
			held = append(held, change)
			continue
		}
		if writeErr := writeChange(stream, change); writeErr != nil {
			return emitted, writeErr
		}
		emitted.items++
	}
	if iterErr := iterator.Err(); iterErr != nil {
		return emitted, timelineQueryError(ctx, request, iterErr)
	}

	// At most --limit items, and only when a limit is in force and the display
	// order is the default, oldest first.
	slices.Reverse(held)
	for _, change := range held {
		if writeErr := writeChange(stream, change); writeErr != nil {
			return emitted, writeErr
		}
		emitted.items++
	}
	return emitted, nil
}

// shape is what the emission turned out to hold, in the form explainNoChanges
// reads.
//
// The gathered path measures the same thing with shapeOf over rows it still has.
// Both spellings exist so that neither path answers the question with a
// convenience of its own — items > 0 was one, and it could not tell a document
// made entirely of Kubernetes Events from one holding the object's own history.
func (e emission) shape() timelineShape {
	return timelineShape{changes: e.sawChange, events: e.sawEvent}
}

// observe records what one change was, before it is written and forgotten.
//
// It is called on every change the iterator yields, including the ones held back
// for the display flip, because what the answer contained is a fact about the
// query rather than about the order it was written in.
func (e *emission) observe(change query.Change) {
	switch change.EventType {
	case query.EventDeleted:
		e.sawDeleted, e.sawChange = true, true
	case query.EventKubernetes:
		e.sawEvent = true
	default:
		// Every other event type is a change to the object: an addition, a
		// modification, a checkpoint. Spelled as the default rather than
		// enumerated so that a type added to the schema is counted as a change
		// until somebody decides otherwise, which is the conservative direction —
		// the alternative is a new row type silently reading as "the filter
		// matched nothing".
		e.sawChange = true
	}
}

// writeChange prepares one change and writes it, so that the two emission orders
// above cannot come to prepare a row differently.
//
// Both failures it can report end the invocation. A row whose recorded columns
// will not parse is corrupt evidence and is refused rather than flattened into a
// string (see render.NewChangeItem); a write that fails is a broken output
// stream, and continuing to feed it would produce a document nothing can parse.
func writeChange(stream *render.Stream, change query.Change) error {
	item, err := changeItem(change)
	if err != nil {
		return err
	}
	if err := stream.Write(item); err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	return nil
}

// holdForDisplayOrder reports whether the emission order and the display order
// disagree, so that items must be held back and reversed.
//
// They disagree in exactly one case — the default one — and the reasoning is
// worth stating because the obvious simplification is wrong. --limit takes the
// first N changes in the *query's* order (see query.TimelineQuery.Limit), so a
// limited query asked oldest-first would return the oldest N — a different set of
// changes from the one the table shows, not merely the same set in another order.
// So a limited query keeps its newest-first shape, the answer is bounded by the
// limit, and the reversal into the display's oldest-first order happens here.
func holdForDisplayOrder(request TimelineRequest) bool {
	return !request.Reverse && request.Limit > 0
}
