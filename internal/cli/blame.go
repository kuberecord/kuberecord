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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/coldscan"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/replay"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `blame` turns the history inside out.
//
// `timeline` and `diff` are organized by change: here is an instant, here is what
// moved. This is organized by field, which is the shape of the question somebody
// actually arrives with — not "what happened at 14:05" but "who set this replica
// count, and when". The answer is computed by replaying the window's patches in ts
// order and recording the last write of each JSON Pointer (see attribute.go).
//
// It asks the backend the same question `timeline` asks, through the same shared
// gathering, so the two commands agree about which incarnation they are describing
// and about why an empty answer is empty (Invariant 9).
//
// # Three flags this command deliberately does not have
//
// --limit, because a limit takes the newest changes in the window and would move
// the anchor of the replay to the oldest change *fetched*. Fields written inside
// the window but before that anchor would then render as "(before window)", which
// is a false statement produced by a flag rather than by the data. The window is
// the bound here, and --max-objects is still the circuit breaker for a cold scan.
//
// --all-incarnations, because one field table spanning two UIDs is precisely the
// splice Invariant 7 forbids: the fields would be attributed to changes made to
// two different objects that happened to share a name. --uid pins one.
//
// --reverse, because the order is not a property of the question. The rows come
// out most recently written first, which is what makes the top of the page the
// answer to "what moved last".
//
// # --field means something slightly different here
//
// For `timeline` and `diff` a path predicate selects whole *changes*: a change
// that touched the path is shown entire, other fields included, because those are
// the context for the one asked about. Here it selects *fields*, because the rows
// are fields. Both use query.MatchesFieldPath, so the two commands agree about
// which paths a prefix covers even though they apply the answer to different
// things.

// blameFlags is one invocation's own flag surface.
type blameFlags struct {
	window windowFlags
	uid    string
	fields []string
	depth  int
}

// newBlameCommand builds `blame`.
func newBlameCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	local := &blameFlags{}

	command := &cobra.Command{
		Use:   "blame (KIND/NAME | KIND NAME)",
		Short: "Show which change last wrote each field of one object, and who made it",
		Long: `Show which change last wrote each field of one object, and who made it.

Each row is one field: its path, when it was last written, and the field managers
seen on the change that wrote it. It is computed by replaying the recorded
patches in order and keeping the last write of every JSON path, so a field
written by a change that replaced the whole block above it is attributed to that
change rather than left looking untouched.

The replay starts from the full state recorded at or before the start of the
window, which is what keeps a bounded window honest: a field whose last write is
older than the window is shown as ` + render.BeforeWindow + ` rather than
omitted or credited to the wrong change. A field the window deleted is kept and
marked ` + render.RemovedMarker + `, because who removed it is one of the two
questions this command answers.

An empty result is never presented on its own. It is explained against the watch
scopes that were open at the time: "nothing changed" and "nothing was watching"
are different findings, and the second exits ` + fmt.Sprint(exit.NoCoverage) + `.`,
		Example: `  # Who last touched each field of this Deployment.
  kuberecord blame deploy/checkout -n payments

  # Only the container spec, over the last week.
  kuberecord blame deploy/checkout -n payments --since 7d --field spec.template.spec.containers

  # A fat object, collapsed to the top two levels of every path.
  kuberecord blame crd/widgets.example.com --depth 2

  # Who last set the replica count, for a script.
  kuberecord blame deploy/checkout -n payments --field spec.replicas -o json`,

		// The kind completes from the static short-name table; the name is an
		// object in a cluster or an archive, and is not read from here. See
		// completeObjectAddress.
		ValidArgsFunction: completeObjectAddress,

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := local.window.resolve(cmd.Flags()); err != nil {
				return err
			}
			return runBlameCommand(cmd.Context(), flags, local, args, streams, invokedAs)
		},
	}

	local.window.addFlags(command.Flags(),
		"Attribute changes at or after this point: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z). A field last written before it reads "+
			render.BeforeWindow+".",
		"Attribute nothing after this point, in the same forms as --since. The fields listed are "+
			"the ones the object held then.")
	command.Flags().StringVar(&local.uid, "uid", local.uid,
		"Pin the attribution to one incarnation by UID.")
	command.Flags().StringSliceVar(&local.fields, "field", local.fields,
		"Only fields at or beneath one of these paths. Repeatable. Either spelling works: "+
			"spec.containers[0].image or spec.containers.0.image.")
	command.Flags().IntVar(&local.depth, "depth", local.depth,
		"Collapse every path to at most this many levels, so a fat object stays readable. "+
			"Levels are JSON Pointer tokens, so an array index is one of them: --depth 4 collapses "+
			"a container array into a row and --depth 5 gives a row per container. Zero shows every field.")

	return command
}

// runBlameCommand turns one invocation into a request, opens the backend, and
// runs it.
func runBlameCommand(
	ctx context.Context, flags *options.GlobalFlags, local *blameFlags,
	args []string, streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	structured, err := blameFormat(flags.Output)
	if err != nil {
		return err
	}
	arg, err := ParseResourceArg(args)
	if err != nil {
		return err
	}
	if local.depth < 0 {
		return exit.UsageErrorf("--depth %d is negative; zero shows every field", local.depth)
	}

	now := time.Now()
	from, to, err := parseWindow(local.window.since, local.window.until, now)
	if err != nil {
		return err
	}

	backend, ref, err := resolveObject(ctx, flags, streams, invokedAs, arg)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, backend.Close())
	}()

	request := BlameRequest{
		Timeline: TimelineRequest{
			Ref:  ref,
			From: from,
			To:   to,
			Now:  now,
			UID:  local.uid,
			// The whole window, deliberately uncapped: see the file's opening
			// comment on why a limit would manufacture false "(before window)" rows.
			Limit: 0,
			// The prior-value replay would cost a round trip to recover values this
			// command never prints. Its own replay, over the same rows, is below.
			NoPriorValues: true,
			Structured:    structured,
			Scan:          coldscan.OptionsFrom(flags, streams),
		},
		Fields: normalizeFieldPaths(local.fields),
		Depth:  local.depth,
	}
	return RunBlame(ctx, backend, request, streams, blameRenderOptions(flags, streams))
}

// blameFormat decides which of this command's two renderings an invocation asked
// for.
//
// `diff` is refused by name rather than quietly rendered as a table, for the
// reason `timeline` refuses it: a user who asked for one shape and received
// another has been answered in a form their eye or their script cannot read, and
// finding that out at the `jq` is worse than finding it out here.
func blameFormat(format options.OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case options.OutputTable, options.OutputWide:
		return "", nil
	case options.OutputDiff:
		return "", exit.UsageErrorf("blame does not render %s: its rows are fields rather than changes, "+
			"and there is no old value beside a new one to lay out. The `%s` command spends the "+
			"whole page on the changes themselves", options.OutputDiff, options.OutputDiff)
	}
	structured, ok := structuredFormat(format)
	if !ok {
		return "", exit.UsageErrorf("blame cannot render %s", format)
	}
	return structured, nil
}

// blameRenderOptions decides how the document will look.
//
// --full has no meaning here: nothing in this table is a summary of something
// longer, so there is nothing for it to expand.
func blameRenderOptions(flags *options.GlobalFlags, streams genericiooptions.IOStreams) render.Options {
	return render.Options{
		Width: options.TerminalWidth(streams.Out),
		Color: options.ShouldColorize(flags.Color, streams.Out),
		Wide:  flags.Output == options.OutputWide,
	}
}

// BlameRequest is one `blame` invocation, resolved.
//
// It is exported, and RunBlame takes an already-opened resolve.Backend, for the reason
// TimelineRequest is: the whole of the command's behaviour is then reachable from
// a test holding a fake QueryEngine rather than only from one that can reach a
// kubeconfig and a live sink.
type BlameRequest struct {
	// Timeline is the question, in the shape every command in this release asks
	// it. `blame` differs in what it does with the answer, not in what is asked.
	Timeline TimelineRequest

	// Fields narrows the rows to the paths at or beneath these prefixes, in the
	// read plane's own dotted grammar. It never narrows the query: the replay
	// needs the whole consecutive run, and a filtered slice of history would
	// attribute fields to changes that did not write them.
	Fields []string

	// Depth collapses every path to at most this many JSON Pointer tokens; zero
	// shows every field.
	Depth int
}

// RunBlame answers one blame request against an opened backend and renders the
// result.
func RunBlame(
	ctx context.Context, backend *resolve.Backend, request BlameRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	gathered, err := gatherChanges(ctx, backend, request.Timeline, streams)
	if err != nil {
		return err
	}

	// The rows arrive newest first, because that is the order both backends answer
	// cheaply. The replay must run in the order history happened in, so it walks a
	// reversed clone rather than reversing what the caller holds.
	ascending := slices.Clone(gathered.Rows)
	slices.Reverse(ascending)

	seed, base, seedNotice := seedBlameState(ctx, backend.Engine, request, gathered, ascending)
	attributed := replay.AttributeRun(seed, ascending)
	rows := attributed.BlameRows(request.Fields, request.Depth)

	notices := append(slices.Clone(gathered.Notices), attributed.Notices...)
	if gathered.Empty == nil {
		// A scope nobody ever watched explains the absent state as well as the
		// absent changes, and it is about to be returned as the finding it is.
		// Printing "the object's state could not be established" in front of it
		// would offer a second, weaker reason for the same silence.
		notices = appendNotice(notices, seedNotice)
	}
	notices = appendNotice(notices, blameFilterNotice(request, gathered, len(rows)))

	if writeErr := writeBlameAnswer(backend, request, gathered, base, rows, notices, streams, opts); writeErr != nil {
		return writeErr
	}
	return gathered.Empty
}

// seedBlameState establishes the document the attribution starts from, and the
// sentence the header describes it with.
//
// The anchor is one nanosecond before the oldest change read, not the window's own
// start: a change recorded exactly on the boundary is inside the window and must
// be attributed, and a reconstruction asked for the boundary instant would have
// already applied it. The schema records nanoseconds, so this is the smallest
// representable step rather than an approximation.
//
// A first sighting needs no anchor at all — nothing preceded it, so every field it
// carries is a field it created, and asking the backend to reconstruct the state
// before an object existed would spend a round trip to be told so.
func seedBlameState(
	ctx context.Context, engine query.QueryEngine, request BlameRequest,
	gathered gatherResult, ascending []render.TimelineRow,
) ([]byte, string, render.Notice) {
	if len(ascending) > 0 && ascending[0].Change.EventType == query.EventAdded {
		return nil, fmt.Sprintf("%s (%s), this incarnation's first sighting",
			render.FormatInstant(ascending[0].Change.TS), query.EventAdded), render.Notice{}
	}

	at := blameAnchor(request, gathered, ascending)
	reconstruction, err := engine.StateAt(ctx, request.Timeline.Ref, at, gathered.UID)
	if err != nil {
		return fallbackSeed(ascending, err)
	}

	encoded, encodeErr := json.Marshal(reconstruction.Object)
	if encodeErr != nil {
		return fallbackSeed(ascending, encodeErr)
	}
	return encoded, describeBase(reconstruction), render.Notice{}
}

// blameAnchor is the instant the state is reconstructed for.
//
// With no changes in the window there is nothing to anchor against, so the field
// list is the object as it stood at the end of the window — which is the honest
// answer to "what are this object's fields, and when was each last written" when
// the answer to the second half is "before you asked".
func blameAnchor(request BlameRequest, gathered gatherResult, ascending []render.TimelineRow) time.Time {
	if len(ascending) > 0 {
		return ascending[0].Change.TS.Add(-time.Nanosecond)
	}
	if !gathered.To.IsZero() {
		return gathered.To
	}
	if !request.Timeline.Now.IsZero() {
		return request.Timeline.Now
	}
	return time.Now()
}

// fallbackSeed degrades when no earlier state could be reconstructed.
//
// The oldest full-state row already read is used as the base rather than as a
// change, which is the distinction that keeps the degradation honest: its fields
// are shown as older than the window, because that is what is known about them —
// treating the row as though it had written them would credit a snapshot with
// every field of the object it merely observed.
//
// With no such row either, the command still answers: the paths the window's own
// patches name are attributable without any state at all, and the object's other
// fields are what is lost. Failing here instead would trade the whole answer for
// the part of it that could not be assembled (Invariant 5).
func fallbackSeed(ascending []render.TimelineRow, err error) ([]byte, string, render.Notice) {
	for _, row := range ascending {
		if row.Change.Data == "" {
			continue
		}
		return []byte(row.Change.Data),
			fmt.Sprintf("%s (%s), the oldest full state in this window",
				render.FormatInstant(row.Change.TS), row.Change.EventType),
			render.Notice{
				Text: fmt.Sprintf("no state survives from before this window (%s), so the fields the "+
					"changes below did not touch are shown as %s rather than attributed",
					replay.DescribeStateFailure(err), render.BeforeWindow),
			}
	}
	return nil, "not established", render.Notice{
		Text: fmt.Sprintf("the object's state could not be established (%s), so only the fields the "+
			"changes in this window wrote are listed; the rest of the object is missing from the "+
			"table rather than shown as %s", replay.DescribeStateFailure(err), render.BeforeWindow),
		Warning: true,
	}
}

// describeBase names the row a reconstruction started from, for the header.
//
// The patch count travels with it for the reason `get --at` prints one: a state
// assembled from a base an hour old and two patches invites more confidence than
// one assembled from a base three months old and four hundred.
func describeBase(reconstruction *query.Reconstruction) string {
	base := fmt.Sprintf("%s (%s)",
		render.FormatInstant(reconstruction.BaseTS), reconstruction.BaseEvent)
	if reconstruction.PatchesApplied == 0 {
		return base
	}
	return fmt.Sprintf("%s plus %d %s", base,
		reconstruction.PatchesApplied, patchWord(reconstruction.PatchesApplied))
}

// patchWord spells the count's noun, which "patch"+"s" does not.
func patchWord(count int) string {
	if count == 1 {
		return "patch"
	}
	return "patches"
}

// blameFilterNotice reports a table emptied by --field or --depth rather than by
// the data.
//
// It matters because those two emptinesses lead a reader in opposite directions,
// and because the object demonstrably has fields: without this line, a mistyped
// path produces a page that reads as an object with nothing in it.
func blameFilterNotice(request BlameRequest, gathered gatherResult, shown int) render.Notice {
	if shown > 0 || len(request.Fields) == 0 || gathered.Empty != nil {
		return render.Notice{}
	}
	return render.Notice{
		Text: fmt.Sprintf("%s has no field at or beneath %s in %s; the object itself is not empty",
			describeObject(request.Timeline.Ref), strings.Join(request.Fields, ", "),
			options.DescribeWindow(gathered.From, gathered.To)),
		Warning: true,
	}
}

// writeBlameAnswer renders the gathered answer in whichever shape was asked for.
func writeBlameAnswer(
	backend *resolve.Backend, request BlameRequest, gathered gatherResult, base string,
	rows []render.BlameRow, notices []render.Notice,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	if request.Timeline.Structured == "" {
		document := render.BlameDocument{
			Kind:     describeKind(request.Timeline.Ref),
			Object:   describeObject(request.Timeline.Ref),
			Cluster:  request.Timeline.Ref.ClusterID,
			UID:      gathered.UID,
			Window:   options.DescribeWindow(gathered.From, gathered.To),
			Base:     base,
			Coverage: gathered.Coverage.Summary(),
			Rows:     rows,
			Notices:  notices,
		}
		if err := render.WriteBlame(streams.Out, streams.ErrOut, document, opts); err != nil {
			return exit.RuntimeErrorf("%w", err)
		}
		return nil
	}

	stream, err := render.NewStream(
		streams.Out, request.Timeline.Structured,
		envelopeHead(backend, render.KindBlame, gathered.Coverage))
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	if err := writeItems(stream, blameItems(rows)); err != nil {
		return err
	}
	if err := render.WriteNotices(streams.ErrOut, notices, opts); err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	return nil
}

// blameItems turns rendered rows into envelope items.
//
// An unattributed row carries a null ts and an empty actor list rather than being
// dropped, and the `attributed` field is what a consumer reads to tell that null
// from a change that happened at the zero time. Dropping the row instead would
// make `-o json` and the table disagree about what the object's fields are.
func blameItems(rows []render.BlameRow) []any {
	items := make([]any, 0, len(rows))
	for _, row := range rows {
		item := render.BlameItem{
			Path:            row.Path,
			Pointer:         row.Pointer,
			Attributed:      row.Attributed,
			Actors:          row.Actors,
			UID:             row.UID,
			ResourceVersion: row.ResourceVersion,
			EventType:       row.EventType,
			Removed:         row.Removed,
			Fields:          row.Fields,
		}
		if row.Attributed {
			ts := row.TS
			item.TS = &ts
		}
		if item.Actors == nil {
			// Empty rather than null, for the reason changeItem gives: `.actors[]`
			// fails on a null and yields nothing on an empty list, and failing is
			// not what "this change recorded no field managers" should do to
			// somebody's pipeline.
			item.Actors = []string{}
		}
		items = append(items, item)
	}
	return items
}
