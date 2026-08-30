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
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// `diff` is the detail view, once `timeline` has named a suspect.
//
// It asks the backend the identical question `timeline` asks — same window, same
// incarnation, same coverage consultation, same state replay — and spends the
// whole page on the answer instead of one column of it. Everything before the
// layout is shared (see gather.go), which is what makes the two commands agree
// about which object they are describing and about why an empty answer is empty.
//
// # Why the path filter is not pushed down
//
// --field narrows what is displayed, not what is fetched. The value on the left
// of every hunk comes from replaying the object's state forward, and that replay
// is only valid over a consecutive run of history: hand it a filtered slice and
// it reports values the object never held. So the query stays unfiltered, the
// replay runs over everything it needs, and the narrowing happens afterwards —
// with a notice saying how many changes were examined, because a reader who is
// not told will read three hunks as three changes.
//
// The predicate itself is query.MatchesFieldPaths, unchanged, which selects whole
// *changes* rather than individual operations. That is deliberate: it is the same
// rule `timeline --field` applies, so the two commands agree about which changes a
// path selects, and a change that touched the path is shown entire — the other
// fields it moved at the same instant are the context for the one that was asked
// about.

// diffFlags is one invocation's own flag surface.
type diffFlags struct {
	since    string
	until    string
	limit    int
	reverse  bool
	uid      string
	fields   []string
	full     bool
	exitCode bool
}

// newDiffCommand builds `diff`.
func newDiffCommand(
	flags *GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	local := &diffFlags{limit: defaultLimit}

	command := &cobra.Command{
		Use:   "diff (KIND/NAME | KIND NAME)",
		Short: "Show every field one object's changes touched, old value beside new",
		Long: `Show every field one object's changes touched, old value beside new.

Each block is one recorded change: when it happened, what kind of change it was,
and who was seen on the object, then one hunk per field — the path, the value
that was destroyed, and the value that replaced it.

The old value is not in the recorded patch; it is recovered by replaying the
object's state up to each change. Where that replay could not run, the hunk says
so rather than leaving the field looking as though it had no value before.

Values longer than ` + fmt.Sprint(render.MaxValueRunes) + ` characters are cut, and a change touching more than ` +
			fmt.Sprint(render.MaxOpsPerChange) + `
fields shows the first of them and counts the rest; --full prints everything.

An empty result is never presented on its own. It is explained against the watch
scopes that were open at the time: "nothing changed" and "nothing was watching"
are different findings, and the second exits ` + fmt.Sprint(ExitNoCoverage) + `.`,
		Example: `  # What actually changed on this Deployment in the last two hours.
  kuberecord diff deploy/checkout -n payments --since 2h

  # A window with both ends named, and every operation of every patch.
  kuberecord diff deploy/checkout -n payments --since 2026-08-28T14:00:00Z --until 2026-08-28T15:00:00Z --full

  # Only the hunks under one path, with the prior values still exact.
  kuberecord diff deploy/checkout -n payments --field spec.template.spec.containers

  # git-diff semantics for a script: 0 if nothing changed, 1 if something did.
  kuberecord diff deploy/checkout -n payments --since 15m --exit-code`,

		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiffCommand(cmd.Context(), flags, local, args, streams, invokedAs)
		},
	}

	command.Flags().StringVar(&local.since, "since", local.since,
		"Only changes at or after this point: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z).")
	command.Flags().StringVar(&local.until, "until", local.until,
		"Only changes at or before this point, in the same forms as --since.")
	command.Flags().IntVar(&local.limit, "limit", local.limit,
		"Examine at most this many changes, newest first. Zero means no limit.")
	command.Flags().BoolVar(&local.reverse, "reverse", local.reverse,
		"Show the same changes oldest first. It reorders the blocks; it does not select different ones.")
	command.Flags().StringVar(&local.uid, "uid", local.uid,
		"Pin the diff to one incarnation by UID.")
	command.Flags().StringSliceVar(&local.fields, "field", local.fields,
		"Only changes touching one of these field paths, matched by prefix, with every hunk of "+
			"those changes. Either spelling works: spec.containers[0].image or "+
			"spec.containers.0.image. It narrows what is shown, not what is read, so the prior "+
			"values stay exact.")
	command.Flags().BoolVar(&local.full, "full", local.full,
		"Print every operation of every patch and every value in full, unshortened.")
	command.Flags().BoolVar(&local.exitCode, "exit-code", local.exitCode,
		fmt.Sprintf("Exit %d when there are no changes and %d when there are, as `git diff` does. "+
			"Exit %d still means nothing was ever watching.",
			ExitSuccess, ExitRuntimeError, ExitNoCoverage))

	return command
}

// runDiffCommand turns one invocation into a request, opens the backend, and runs
// it.
func runDiffCommand(
	ctx context.Context, flags *GlobalFlags, local *diffFlags,
	args []string, streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	structured, err := diffFormat(flags.Output)
	if err != nil {
		return err
	}
	arg, err := ParseResourceArg(args)
	if err != nil {
		return err
	}
	if local.limit < 0 {
		return UsageErrorf("--limit %d is negative; zero means no limit", local.limit)
	}

	now := time.Now()
	from, to, err := parseWindow(local.since, local.until, now)
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

	request := DiffRequest{
		Timeline: TimelineRequest{
			Ref:               ref,
			From:              from,
			To:                to,
			Now:               now,
			UID:               local.uid,
			DisplayFieldPaths: normalizeFieldPaths(local.fields),
			Limit:             local.limit,
			Reverse:           local.reverse,
			Structured:        structured,
			Scan:              scanOptions(flags, streams),
		},
		ExitCode: local.exitCode,
	}
	return RunDiff(ctx, backend, request, streams, diffRenderOptions(flags, local, streams))
}

// diffFormat decides which of this command's two renderings an invocation asked
// for.
//
// An empty StructuredFormat means the hunk view. Three -o values arrive at it:
// `diff` is the rendering this command exists for, `wide` widens the timestamps
// the way it does everywhere else, and `table` is in the list only because it is
// the global default a user who typed no -o at all arrives with — refusing that
// would make a bare `kuberecord diff` a usage error.
func diffFormat(format OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case OutputTable, OutputWide, OutputDiff:
		return "", nil
	}
	structured, ok := structuredFormat(format)
	if !ok {
		return "", UsageErrorf("diff cannot render %s", format)
	}
	return structured, nil
}

// diffRenderOptions decides how the document will look.
func diffRenderOptions(
	flags *GlobalFlags, local *diffFlags, streams genericiooptions.IOStreams,
) render.Options {
	return render.Options{
		Width: TerminalWidth(streams.Out),
		Color: ShouldColorize(flags.Color, streams.Out),
		Wide:  flags.Output == OutputWide,
		Full:  local.full,
	}
}

// DiffRequest is one `diff` invocation, resolved.
//
// It is exported, and RunDiff takes an already-opened Backend, for the reason
// TimelineRequest is: the whole of the command's behaviour is then reachable from
// a test holding a fake QueryEngine, rather than only from a test that can reach a
// kubeconfig and a live sink.
type DiffRequest struct {
	// Timeline is the question, in the shape every command in this release asks
	// it. `diff` differs from `timeline` in how the answer is laid out, not in
	// what is asked.
	Timeline TimelineRequest

	// ExitCode turns the result into git-diff's exit contract: 0 for no changes,
	// 1 for changes found.
	ExitCode bool
}

// RunDiff answers one diff request against an opened backend and renders the
// result.
func RunDiff(
	ctx context.Context, backend *Backend, request DiffRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	gathered, err := gatherChanges(ctx, backend, request.Timeline, streams)
	if err != nil {
		return err
	}

	// The finding, not the failure, decides the exit code: a scope nobody ever
	// watched is reported as such whatever --exit-code was asked for, because a
	// script told that "nothing changed" when nothing was watching has been given
	// the one answer Invariant 9 exists to prevent.
	notices := gathered.Notices
	changed := gathered.Empty == nil && len(gathered.Rows) > 0
	if request.ExitCode && changed {
		notices = append(notices, changesFoundNotice(len(gathered.Rows)))
	}

	if writeErr := writeDiffAnswer(backend, request, gathered, notices, streams, opts); writeErr != nil {
		return writeErr
	}
	if gathered.Empty != nil {
		return gathered.Empty
	}
	if request.ExitCode && changed {
		return &Error{Code: ExitRuntimeError, Quiet: true, Err: errChangesFound}
	}
	return nil
}

// writeDiffAnswer renders the gathered answer in whichever shape was asked for.
//
// Both shapes describe the identical answer, gathered identically: the structured
// one is not a reduced version of the page, and in one respect it is the fuller
// of the two, since the hunk view elides long values and caps the operations it
// prints while an envelope carries every one of them.
func writeDiffAnswer(
	backend *Backend, request DiffRequest, gathered gatherResult, notices []render.Notice,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	if request.Timeline.Structured == "" {
		document := render.DiffDocument{
			Kind:     describeKind(request.Timeline.Ref),
			Object:   describeObject(request.Timeline.Ref),
			Cluster:  request.Timeline.Ref.ClusterID,
			UID:      gathered.UID,
			Coverage: gathered.Coverage.Summary(),
			Changes:  gathered.Rows,
			Notices:  notices,
		}
		if err := render.WriteDiff(streams.Out, streams.ErrOut, document, opts); err != nil {
			return RuntimeErrorf("%w", err)
		}
		return nil
	}

	stream, err := render.NewStream(
		streams.Out, request.Timeline.Structured,
		envelopeHead(backend, render.KindDiff, gathered.Coverage))
	if err != nil {
		return RuntimeErrorf("%w", err)
	}
	if err := writeItems(stream, diffItems(gathered.Rows)); err != nil {
		return err
	}
	if err := render.WriteNotices(streams.ErrOut, notices, opts); err != nil {
		return RuntimeErrorf("%w", err)
	}
	return nil
}

// diffItems turns gathered rows into envelope items.
//
// Each item is the schema's own columns plus the two things this command computes
// that nothing else does: the decoded operations, and the prior value each one
// destroyed where the replay established it. An operation whose prior value could
// not be established carries old_known false rather than a null that would read as
// "the field was null" — the distinction render.Hunk exists to keep.
func diffItems(rows []render.TimelineRow) []any {
	items := make([]any, 0, len(rows))
	for _, row := range rows {
		items = append(items, render.DiffItem{
			Change:     changeItem(row.Change),
			PatchError: row.PatchErr,
			Hunks:      render.Hunks(row.Ops),
		})
	}
	return items
}

// errChangesFound is what --exit-code returns when the answer is "yes".
//
// It is an error only in the sense that it carries a non-zero exit code; it is
// marked Quiet so nothing prints "error:" over a successful query, and the notice
// beside the document is where the reader is told what the code means.
var errChangesFound = errors.New("changes were found and --exit-code was requested")

// changesFoundNotice explains the exit code a human is about to be given.
func changesFoundNotice(count int) render.Notice {
	return render.Notice{Text: fmt.Sprintf(
		"%d %s shown; --exit-code reports that as exit %d, the way `git diff` does. "+
			"It is not a failure", count, plural(count, "change"), ExitRuntimeError)}
}

// plural spells a count's noun so a notice reads as a sentence.
func plural(count int, noun string) string {
	if count == 1 {
		return noun
	}
	return noun + "s"
}
