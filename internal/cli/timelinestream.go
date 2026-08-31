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
// selectIncarnation, askCoverage, deletionsNotice, explainEmpty — and only the
// middle, where rows are turned into output, differs. What is duplicated here is
// the order they are called in, and that order is asserted by tests over both
// paths rather than by a comment.
//
// # Where the memory actually goes
//
// One case cannot stream, and it is bounded rather than hidden. --reverse asks
// for the oldest change first, while --limit selects the *newest* N — so with
// both set, the newest N have to be read before the oldest of them can be
// written. The buffer is therefore at most --limit items: bounded by a number the
// user typed, never by the size of the result. With no limit the two orderings
// select the same set, so the query is simply asked oldest-first and nothing is
// held at all.

// runTimelineStructured answers a timeline request into the versioned envelope.
//
// It writes the document to stdout as it goes and every qualification of it to
// stderr at the end, which is the same split the tabular rendering keeps. The
// notices arrive after the document rather than before it because some of them —
// whether the result was empty, whether a deletion was seen — are facts about
// rows that had not been read yet when the first byte was written. That is the
// cost of streaming, and it is paid on the stream nothing is piping.
func runTimelineStructured(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	capabilities := backend.Engine.Capabilities()

	from, to, windowNotice := timelineBounds(request, capabilities)
	notices := appendNotice(nil, windowNotice)

	// Same position as the gathered path's, and for the same reason: this is before
	// the first query of the invocation, incarnation listing included. See
	// coldscan.Begin.
	scan, err := coldscan.Begin(ctx, backend, request.Scan, request.Ref.ClusterID, from, to, streams)
	if err != nil {
		return err
	}
	defer scan.Stop()
	ctx = scan.Ctx

	selection, selectionNotices := selectIncarnation(ctx, backend.Engine, request, from, to)
	notices = append(notices, selectionNotices...)

	coverage, err := askCoverage(
		ctx, backend, request.scopeQuery(from, to), describeObject(request.Ref))
	if err != nil {
		return err
	}

	stream, err := render.NewStream(
		streams.Out, request.Structured, envelopeHead(backend, render.KindTimeline, coverage))
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}

	emitted, sawDeleted, emitErr := emitChanges(
		ctx, backend.Engine, request, request.timelineQuery(selection, from, to), stream)

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

	notices = appendNotice(notices, deletionsNotice(capabilities, sawDeleted))
	emptyNotices, emptyErr := explainEmpty(request, from, to, emitted > 0, coverage)
	notices = append(notices, emptyNotices...)

	if writeErr := render.WriteNotices(streams.ErrOut, notices, opts); writeErr != nil {
		return exit.RuntimeErrorf("%w", writeErr)
	}
	return emptyErr
}

// emitChanges runs the query and writes each change into the envelope.
//
// It reports how many items were written and whether a deletion was among them,
// which are the two facts the notices need and the two a streaming path cannot
// recover afterwards from rows it no longer holds.
//
// Err is checked after the loop and Close called on every path, for the reason
// collectChanges does both: skipping either turns a backend that failed halfway
// into a result that looks complete and merely short, which for an audit timeline
// is the worst available outcome.
func emitChanges(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest,
	q query.TimelineQuery, stream *render.Stream,
) (emitted int, sawDeleted bool, err error) {
	hold := holdForDisplayOrder(request)
	if !hold {
		// The query is asked in the order the output is written in, so nothing has
		// to be held back. See the file comment for why --reverse without a limit
		// selects the same changes either way.
		q.Reverse = !request.Reverse
	}

	iterator, err := engine.Timeline(ctx, q)
	if err != nil {
		return 0, false, timelineQueryError(ctx, request, err)
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil && err == nil {
			err = exit.RuntimeErrorf("releasing the change stream: %w", closeErr)
		}
	}()

	var held []query.Change
	for iterator.Next() {
		change := iterator.Change()
		sawDeleted = sawDeleted || change.EventType == query.EventDeleted
		if hold {
			held = append(held, change)
			continue
		}
		if writeErr := stream.Write(changeItem(change)); writeErr != nil {
			return emitted, sawDeleted, exit.RuntimeErrorf("%w", writeErr)
		}
		emitted++
	}
	if iterErr := iterator.Err(); iterErr != nil {
		return emitted, sawDeleted, timelineQueryError(ctx, request, iterErr)
	}

	// At most --limit items, and only when --reverse and --limit are both set.
	slices.Reverse(held)
	for _, change := range held {
		if writeErr := stream.Write(changeItem(change)); writeErr != nil {
			return emitted, sawDeleted, exit.RuntimeErrorf("%w", writeErr)
		}
		emitted++
	}
	return emitted, sawDeleted, nil
}

// holdForDisplayOrder reports whether the emission order and the display order
// disagree, so that items must be held back and reversed.
//
// They disagree in exactly one case, and the reasoning is worth stating because
// the obvious simplification is wrong. --limit takes the first N changes in the
// *query's* order (see query.TimelineQuery.Limit), so a limited query asked
// oldest-first would return the oldest N — a different set of changes from the
// one the table shows, not merely the same set in another order. So when both
// flags are set the query keeps its newest-first shape, the answer is bounded by
// the limit, and the reversal happens here.
func holdForDisplayOrder(request TimelineRequest) bool {
	return request.Reverse && request.Limit > 0
}
