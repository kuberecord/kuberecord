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
// the rows in historical order before they are reversed for display, and coverage
// is consulted on every invocation rather than only on an empty one — because a
// timeline whose rows stop at a scope's edge is as misleading as an empty one.
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
	if request.Reverse {
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
	if len(result.Rows) > 0 || scanned == 0 {
		// An emptiness the display filter produced has already been explained by
		// the notice above, and it is an explanation the watch scopes cannot
		// improve on: changes were recorded, and the filter removed them.
		// Consulting coverage about it would answer a question nobody asked and
		// could report "nothing was watching" about a window that demonstrably
		// held changes.
		emptyNotices, emptyErr := explainEmpty(request, from, to, len(result.Rows) > 0, coverage)
		result.Notices = append(result.Notices, emptyNotices...)
		result.Empty = emptyErr
	}
	return result, nil
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
			Warning: true,
		}
	}
	return render.Notice{Text: fmt.Sprintf(
		"%d of the %d changes examined touched %s; the rest were read and replayed so that the values "+
			"shown are exact, then set aside. --limit bounds the changes examined, not the ones shown",
		shown, scanned, paths)}
}
