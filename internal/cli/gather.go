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
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/coldscan"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/replay"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// One question, asked once, rendered two ways.
//
// `timeline` and `diff` ask the backend the identical question and differ only in
// how the answer is laid out. Everything between the request and the layout —
// which incarnation was chosen, whether a window had to be completed, what the
// watch scopes say about a silence, whether the backend can record deletions at
// all, and the state replay that recovers the value each operation destroyed — is
// gathered here so that there is exactly one implementation of it.
//
// The alternative was for `diff` to repeat the sequence, and the failure mode of
// that is specific rather than theoretical: Invariant 9 says no command may
// present emptiness without consulting coverage, and a second copy of this
// sequence is a second place for that consultation to be dropped, reordered, or
// quietly conditioned on something. Extracting it makes `diff` obey the invariant
// by construction rather than by review.

// gatherResult is one question's whole answer, before anything decides how it
// will look.
//
// Rows are in display order — the order --reverse asked for — because the replay
// that fills Op.Old has already run over them in the order history happened in,
// and handing a renderer the historical order plus a note to reverse it would be
// making the same decision twice.
type gatherResult struct {
	// UID is the incarnation being shown, for the header.
	UID string

	// From and To are the window every call below was bounded by, after a
	// backend that insists on one has had a half or absent window completed.
	//
	// They are returned rather than recomputed by a caller that needs to name the
	// window, because the completed bounds are what the answer was actually read
	// over: a header stating the window the user typed, beside rows fetched over
	// a window the backend forced, would be a document disagreeing with itself.
	From time.Time
	To   time.Time

	// Incarnations is every UID in the window, set only when all of them are
	// being shown. Its presence is what gives a table its UID column.
	Incarnations []string

	// Coverage is what the watch scopes said, carried whole rather than
	// pre-rendered: the header wants a sentence and the structured envelope wants
	// the intervals themselves, and building one from the other afterwards would
	// be a second reading of the same answer.
	Coverage coverageAnswer

	// Rows are the changes, in display order.
	Rows []render.TimelineRow

	// Notices are every qualification of the answer, in the order they are to be
	// written to standard error.
	Notices []render.Notice

	// Empty is the no-coverage finding, or nil. It is returned rather than acted
	// on here because a command has to write its document before it fails: the
	// header carries the coverage summary that explains the finding.
	Empty error
}

// gatherChanges asks the backend everything a rendered answer needs.
//
// The order below is load-bearing and is the order RunTimeline established: the
// window is completed first because every later call is bounded by it, the
// incarnation is chosen before the query so that the header and the rows cannot
// disagree about which object they describe (Invariant 7), the replay runs over
// the rows in the query's own order before they are turned into the display's,
// and coverage is consulted on every invocation rather than only on an empty one
// — because a timeline whose rows stop at a scope's edge is as misleading as an
// empty one.
//
// The rows come back in display order, which by default is oldest first. Only the
// *query* is newest-first, and it stays that way whatever is displayed: see
// timelineQuery.
func gatherChanges(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	streams genericiooptions.IOStreams,
) (gatherResult, error) {
	var result gatherResult
	capabilities := backend.Engine.Capabilities()

	from, to, windowNotice := timelineBounds(request, capabilities)
	result.From, result.To = from, to
	result.Notices = appendNotice(result.Notices, windowNotice)

	// Before the first query rather than before the timeline query: listing the
	// incarnations costs the same partitions, so a guard placed after it would
	// narrate the second scan of a question that had already silently run one.
	scan, err := coldscan.Begin(ctx, backend, request.Scan, request.Ref.ClusterID, from, to, streams)
	if err != nil {
		return gatherResult{}, err
	}
	defer scan.Stop()
	ctx = scan.Ctx

	selection, selectionNotices := selectIncarnation(ctx, backend.Engine, request, from, to)
	result.Notices = append(result.Notices, selectionNotices...)
	result.UID = selection.uid
	result.Incarnations = selection.listed

	changes, err := collectChanges(ctx, backend.Engine, request.timelineQuery(selection, from, to))
	if err != nil {
		return gatherResult{}, timelineQueryError(ctx, request, err)
	}
	result.Rows = replay.DecodeRows(changes)
	if request.AllIncarnations && len(result.Incarnations) == 0 {
		// The listing failed, and a table that may span several incarnations must
		// still carry the column that tells them apart (Invariant 7). The rows
		// themselves are the fallback source of the identities.
		result.Incarnations = distinctUIDs(result.Rows)
	}
	if result.UID == "" {
		// The incarnation could not be listed, so the header takes the identity
		// from the rows themselves rather than leaving the field blank.
		result.UID = firstObjectUID(result.Rows)
	}

	result.Notices = append(result.Notices,
		priorValueNotices(ctx, backend.Engine, request, result.Rows)...)

	// The replay above needed every row of the consecutive run. Narrowing for
	// display happens only now, so that a path filter costs the reader the rows
	// they did not ask for and not the prior values they did.
	scanned := len(result.Rows)
	result.Rows = displayRows(result.Rows, request.DisplayFieldPaths)
	if !request.Reverse {
		// The query answered newest first, and the default display is oldest first,
		// so the default is the case that reverses. --reverse asks for the query's
		// own order and leaves the rows alone.
		slices.Reverse(result.Rows)
	}

	coverage, err := askCoverage(
		ctx, backend, request.scopeQuery(from, to), describeObject(request.Ref))
	if err != nil {
		// Not routed through timelineQueryError: this failure is about the scope log
		// rather than about the timeline, and askCoverage has already said so in the
		// words that name what could not be read.
		return gatherResult{}, err
	}
	result.Coverage = coverage
	result.Notices = appendNotice(result.Notices,
		deletionsNotice(capabilities, sawDeletion(result.Rows)))

	result.Notices = appendNotice(result.Notices,
		displayFilterNotice(request, from, to, scanned, len(result.Rows)))

	// Measured once, after the rows are final, and read by the three notices
	// below. They are three questions about the same document — did a predicate
	// empty it, is the object's own history missing, did --with-events interleave
	// anything — and answering each from its own walk of the rows is how two of
	// them come to disagree about what was on the page.
	shape := shapeOf(result.Rows)

	// The same question about the predicates the *query* carried, which the two
	// counts above cannot answer: those rows were removed before they arrived, so
	// scanned and len(Rows) are both zero and an emptiness the filter produced is
	// indistinguishable from an empty window. See predicateNotice.
	predicate, attributed := predicateNotice(
		ctx, backend.Engine, request, selection, from, to, shape.changes)
	result.Notices = appendNotice(result.Notices, predicate)

	if !attributed && (len(result.Rows) > 0 || scanned == 0) {
		// An emptiness a filter produced has already been explained by one of the
		// two notices above, and it is an explanation the watch scopes cannot
		// improve on: changes were recorded, and the filter removed them.
		// Consulting coverage about it would answer a question nobody asked and
		// could report "nothing was watching" about a window that demonstrably
		// held changes.
		// The Event scope is passed unasked. Only the no-coverage finding under
		// --with-events spends it, and that is the one branch on which eventsNotice
		// below is withheld — so the two never both run and the round trip is bought
		// exactly once, by whichever of them is reached (Task 18.7).
		emptyNotices, emptyErr := explainNoChanges(request, from, to, shape, coverage,
			func() (coverageAnswer, error) { return eventCoverage(ctx, backend, request, from, to) })
		result.Notices = append(result.Notices, emptyNotices...)
		result.Empty = emptyErr
	}

	// Last, and after the object's own emptiness has been explained: this is the
	// sub-question --with-events asked inside the main one, and a reader works
	// outwards. It is still inside the cold-scan guard, which is where any query
	// that may walk partitions belongs.
	//
	// Withheld when the explanation above turned out to be the no-coverage finding,
	// which is Task 18.5's fourth item. Nothing was ever watching this object, so a
	// second notice about the Events is a second paragraph for a reader who has not
	// finished acting on the first. uncoveredNoChanges absorbs the point instead, in
	// one sentence — and since Task 18.7 it absorbs the standalone notice's coverage
	// read along with it, so what the reader is left with is one finding that has
	// measured both halves of what it claims.
	//
	// The gate is result.Empty rather than the coverage answer itself, and the
	// difference is D31. An emptiness a predicate produced is explained by
	// predicateNotice and never reaches explainNoChanges, so gating on the raw scope
	// log would silence --with-events with nothing left to account for it — a flag
	// that produced no visible effect and did not say why. It also spares the extra
	// round trip on the one path that has nothing to learn from it.
	if result.Empty == nil {
		result.Notices = appendNotice(result.Notices,
			eventsNotice(ctx, backend, request, from, to, shape.events))
	}
	return result, nil
}

// eventsNotice explains a --with-events that interleaved nothing, and says
// nothing when the flag was not passed.
//
// The gate is the flag and not the archive's contents, which is the whole of
// Invariant 9's reading here: a bare `timeline` over a cluster that records no
// Events is not an unanswered question, because nothing asked it. Only a reader
// who typed --with-events is owed a sentence, and D31 says they are owed it
// whatever the answer turns out to be.
//
// The consultation costs one extra round trip and is paid only on the path that
// needs it — the flag was passed and no Event came back. An invocation that
// interleaved Events has its answer in front of it and is asked nothing further.
//
// A failed read degrades into the notice rather than ending the command. See
// explainNoEvents, which states why.
func eventsNotice(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	from, to time.Time, interleaved bool,
) render.Notice {
	if !request.WithEvents || interleaved {
		return render.Notice{}
	}
	coverage, err := eventCoverage(ctx, backend, request, from, to)
	return explainNoEvents(request, from, to, coverage, err)
}

// eventCoverage asks the scope log whether Events were being recorded where the
// object was.
//
// It is one function because two callers ask the identical question and must
// receive the identical answer: this notice, for a watched object whose Events
// are missing, and the no-coverage finding's absorbed clause, for an object that
// was never watched at all (Task 18.7). They are mutually exclusive — gatherChanges
// asks the second only when the first is withheld — so an invocation pays for at
// most one Event coverage read whichever of them it reaches.
//
// Two formulations of one question is how the two answers come to disagree, and
// the disagreement would be invisible: both produce a confident, well-formed
// sentence about Events, and only one of them would be about the scope that was
// actually consulted.
func eventCoverage(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest, from, to time.Time,
) (coverageAnswer, error) {
	coverage, err := askCoverage(
		ctx, backend, eventScopeQuery(request, from, to), describeEventScope(request))
	// Narrowed after the query rather than in it, because the query had to ask
	// about every group in order to reach the core one. See eventIntervals.
	coverage.Intervals = eventIntervals(coverage.Intervals)
	return coverage, err
}

// describeEventScope names the scope the Event coverage question was asked about,
// for the failure message.
//
// It spells the unrestricted half out — a cluster-scoped subject has no namespace
// of its own, so the question really is about every namespace — for the reason
// ScopesRequest.describeScope does: an answer about a scope is only actionable if
// the reader can see how wide the scope was.
func describeEventScope(request TimelineRequest) string {
	if namespace := request.Ref.Namespace; namespace != "" {
		return "Kubernetes Events in namespace " + namespace
	}
	return "Kubernetes Events in every namespace"
}

// displayRows narrows a gathered run to the paths a command was asked to show.
//
// It applies query.MatchesFieldPaths rather than a second reading of the same
// rule: the contract's own function unescapes RFC 6901 in the mandated order,
// converts to the dotted grammar, prefix-matches it, and keeps a row that carries
// no patch at all. Each of those is somewhere a private copy could disagree with a
// backend that pushed the same predicate down, and two answers to one question is
// worse than either answer alone.
func displayRows(rows []render.TimelineRow, paths []string) []render.TimelineRow {
	if len(paths) == 0 {
		return rows
	}
	kept := make([]render.TimelineRow, 0, len(rows))
	for _, row := range rows {
		if query.MatchesFieldPaths(row.Change, paths) {
			kept = append(kept, row)
		}
	}
	return kept
}

// displayFilterNotice reports what a display-time path filter removed.
//
// It is printed whenever the filter removed anything, not only when it removed
// everything, because the two numbers together are what tell a reader that
// --limit bounded the changes *examined* rather than the ones shown. Without it,
// `--limit 100 --field spec.replicas` returning three hunks reads as three
// changes in the window, and the reader has no way to see the ninety-seven that
// were fetched, replayed and then set aside.
func displayFilterNotice(request TimelineRequest, from, to time.Time, scanned, shown int) render.Notice {
	if len(request.DisplayFieldPaths) == 0 || scanned == shown {
		return render.Notice{}
	}
	paths := strings.Join(request.DisplayFieldPaths, ", ")
	if shown == 0 {
		return render.Notice{
			Text: fmt.Sprintf("%d changes are recorded for %s in %s, and none of them touched %s; "+
				"the window itself is not empty", scanned, describeObject(request.Ref),
				options.DescribeWindow(from, to), paths),
		}
	}
	return render.Notice{Text: fmt.Sprintf(
		"%d of the %d changes examined touched %s; the rest were read and replayed so that the values "+
			"shown are exact, then set aside. --limit bounds the changes examined, not the ones shown",
		shown, scanned, paths)}
}

// predicateNotice explains a timeline the query's own predicates emptied.
//
// The gate is two facts. A predicate was in force — otherwise there is nothing to
// attribute an emptiness to — and no *object change* was rendered, which is the
// honest reading of "the flag produced no visible effect" here: the predicates
// narrow the object's own changes and deliberately leave merged Kubernetes Events
// alone (an Event's actors are the field managers of the Event object, not of
// whoever changed the subject), so a document holding nothing but Event rows is
// one where --actor still removed everything it could have kept.
//
// The consultation costs one extra query and is paid only on that path, which is
// the bargain eventsNotice already strikes. It is the cheapest question either
// backend can be asked — one row, newest first, which is the shape the object
// archive short-circuits on — and it is asked inside the cold-scan guard, where
// every query that may walk partitions belongs.
//
// A failed probe degrades into the notice rather than ending the command. See
// explainNoMatches, which states why, and why its second return value suppresses
// the coverage explanation.
func predicateNotice(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest,
	selection incarnationChoice, from, to time.Time, renderedChange bool,
) (render.Notice, bool) {
	if !request.filtered() || renderedChange {
		return render.Notice{}, false
	}
	hadChanges, err := anyChangeInWindow(ctx, engine, request, selection, from, to)
	return explainNoMatches(request, from, to, hadChanges, err)
}

// anyChangeInWindow asks whether this window holds a single change at all.
//
// It is the same question the timeline just asked, minus the predicates: the same
// bounds, the same incarnation, the same ordering. Everything but the predicates
// is kept deliberately — a probe that widened the window or unpinned the
// incarnation would answer about a different question and could report that a
// filter emptied a window which was empty for the object being shown.
//
// Events are excluded because they are not what the predicates narrow, and the
// limit is one because existence is the whole of what the answer turns on. A count
// would be a nicer sentence and would cost the reader an unbounded read of a
// window that has already proved slow enough to be filtered.
func anyChangeInWindow(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest,
	selection incarnationChoice, from, to time.Time,
) (bool, error) {
	q := request.timelineQuery(selection, from, to)
	q.Actors, q.ExcludeActors, q.FieldPaths = nil, nil, nil
	q.IncludeEvents = false
	q.Limit = 1

	changes, err := collectChanges(ctx, engine, q)
	if err != nil {
		return false, err
	}
	return len(changes) > 0, nil
}
