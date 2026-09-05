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
	"slices"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `scopes` is the compliance command, and the mechanism behind Invariant 9.
//
// Every other command in this release can end in a silence, and a silence is only
// readable against what was being watched at the time. This is the command that
// answers that directly: which kinds, in which namespaces, over which periods,
// under which rule. Three notices elsewhere in this package point a reader here by
// name, which is why the constant they use is shared with the command rather than
// spelled twice.
//
// Two properties of the answer decide the whole of the rendering.
//
// An interval's end says the recorder stopped watching. It emphatically does not
// say the objects in the scope were deleted, and nothing here may blur the two —
// which is why a still-open interval says "(open)" in a word rather than leaving
// a cell blank for a reader to interpret.
//
// An empty answer is a finding rather than a listing of nothing. If no scope
// covered the question, then no silence anywhere in this cluster's history means
// what it appears to mean for that scope, and the command exits 3 to say so — the
// same code `timeline` exits when it reaches the same conclusion the long way
// round.

// scopesFlags is one invocation's own flag surface.
type scopesFlags struct {
	kind   string
	window windowFlags
}

// newScopesCommand builds `scopes`.
func newScopesCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	local := &scopesFlags{}

	command := &cobra.Command{
		Use:   "scopes",
		Short: "Show what was being recorded, and when",
		Long: `Show what was being recorded, and when.

Each row is one period during which the recorder was watching one scope: the
kind, the namespace, when it started, when it stopped, and the rule that opened
it. A period with no end is still open, which means the scope is being watched
now.

This is what makes every other command's empty result readable. "Nothing
changed" and "nothing was watching" are different facts, and the second one is
only visible here.

The end of a period says the recorder stopped watching. It does not say the
objects in that scope were deleted.

Without --namespace this lists every namespace, which is what a compliance
question means by default — unlike the object commands, the kubeconfig's current
namespace does not narrow it. With one, the listing matches both that namespace's
own scopes and the cluster-wide ones, because a cluster-wide rule genuinely was
watching objects in it. Such a row shows ` + render.AllNamespaces +
			` in the namespace column.

An answer with no periods in it is a finding rather than an empty list, and it
exits ` + fmt.Sprint(exit.NoCoverage) + `.`,
		Example: `  # Everything this cluster has ever recorded.
  kuberecord scopes

  # What was being watched in one namespace, cluster-wide rules included.
  kuberecord scopes -n payments

  # Whether Deployments were being recorded at all last week.
  kuberecord scopes --kind deploy --since 2w

  # The compliance answer, for a script.
  kuberecord scopes --since 90d -o json`,

		Args: rejectPositionalArgs,

		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := local.window.resolve(cmd.Flags()); err != nil {
				return err
			}
			return runScopesCommand(cmd.Context(), flags, local, streams, invokedAs)
		},
	}

	command.Flags().StringVar(&local.kind, "kind", local.kind,
		"Only scopes for this kind: a short name, a resource name or a kind, as the object commands "+
			"accept. Without a cluster to ask, give it as it is recorded — Deployment or "+
			"Deployment.apps.")
	mustCompleteFlag(command, "kind", completeResourceKind)
	local.window.addFlags(command.Flags(),
		"Only periods overlapping this point onwards: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z). A period that merely overlaps is shown whole.",
		"Only periods overlapping up to this point, in the same forms as --since.")

	return command
}

// rejectPositionalArgs is the Args validator for a command that addresses no
// object.
//
// Cobra's own NoArgs produces a plain error, and a plain error is a runtime
// failure by exit.CodeFor's reckoning — the wrong code for somebody typing an
// object name at a command that lists scopes. Exit 2 is the one that says "you
// typed something this program does not accept".
func rejectPositionalArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	return exit.UsageErrorf("%s takes no arguments, and %q is not one: it lists the scopes that were being "+
		"recorded rather than answering a question about one object. Narrow it with --kind and "+
		"--namespace", cmd.Name(), args[0])
}

// runScopesCommand turns one invocation into a request, opens the backend, and
// runs it.
func runScopesCommand(
	ctx context.Context, flags *options.GlobalFlags, local *scopesFlags,
	streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	structured, err := scopesFormat(flags.Output)
	if err != nil {
		return err
	}

	now := time.Now()
	from, to, err := parseWindow(local.window.since, local.window.until, now)
	if err != nil {
		return err
	}

	backend, err := resolveBackend(ctx, flags, streams, invokedAs)
	if err != nil {
		return err
	}
	defer func() {
		// Joined rather than replacing: the reason the command is ending is
		// whatever happened above, and the tidying up must not hide it.
		err = errors.Join(err, backend.Close())
	}()

	gvk := schema.GroupVersionKind{}
	if local.kind != "" {
		if gvk, err = resolveScopeKind(flags, streams, local.kind); err != nil {
			return err
		}
	}

	request := ScopesRequest{
		ClusterID: backend.ClusterID,
		APIGroup:  gvk.Group,
		Kind:      gvk.Kind,
		// The namespace the user typed, and not the kubeconfig's current one.
		// See scopesNamespace.
		Namespace:  scopesNamespace(flags),
		From:       from,
		To:         to,
		Structured: structured,
	}
	return RunScopes(ctx, backend, request, streams, scopesRenderOptions(flags, streams))
}

// scopesNamespace is the namespace this command narrows to: the one the user
// typed, and never the kubeconfig's current one.
//
// This is a deliberate departure from the other commands, and from kubectl. For
// an object command the kubeconfig's namespace is the right default, because an
// object lives in one and the address is incomplete without it. This command
// lists what was being *recorded*, and the honest default for a compliance
// question is everything: silently narrowing it to whatever namespace the last
// `kubens` selected would drop every namespaced scope but one, and the answer
// would look like a complete listing of a cluster. Widening is visible in the
// header, which names the question; narrowing would not have been.
func scopesNamespace(flags *options.GlobalFlags) string { return explicitNamespace(flags) }

// scopesFormat decides which of this command's two renderings an invocation asked
// for. An empty StructuredFormat means the table.
func scopesFormat(format options.OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case options.OutputTable, options.OutputWide:
		return "", nil
	case options.OutputDiff:
		return "", exit.UsageErrorf("scopes does not render %s: its rows are periods rather than changes, "+
			"and there is no patch to lay out", options.OutputDiff)
	}
	structured, ok := structuredFormat(format)
	if !ok {
		return "", exit.UsageErrorf("scopes cannot render %s", format)
	}
	return structured, nil
}

// scopesRenderOptions decides how the listing will look.
func scopesRenderOptions(flags *options.GlobalFlags, streams genericiooptions.IOStreams) render.Options {
	return render.Options{
		Width: options.TerminalWidth(streams.Out),
		Color: options.ShouldColorize(flags.Color, streams.Out),
		Wide:  flags.Output == options.OutputWide,
	}
}

// resolveScopeKind maps --kind onto the group and kind the scope log records,
// through the cluster when one can be reached and offline when one cannot.
//
// It is resolveObjectAddress's reasoning applied to a kind with no object behind
// it, and it makes the same two distinctions. A cluster that answered and said it
// does not serve this kind is the user's spelling rather than the tool's reach, so
// that is reported instead of being retried offline — otherwise a typo would
// resolve to a kind nobody has and be reported as a scope nobody watched, which is
// the one conclusion this command must never reach by accident. And an
// unreachable cluster falls back to the identity the schema itself stores, because
// reading an archive without the cluster it came from is a supported way to work
// (D18).
func resolveScopeKind(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, kind string,
) (schema.GroupVersionKind, error) {
	resolved, err := clusterResolution(flags, ResourceArg{Resource: kind})
	if err == nil {
		return resolved.GVK, nil
	}

	var unknown *UnknownResourceError
	if errors.As(err, &unknown) {
		return schema.GroupVersionKind{}, err
	}

	gvk, ok := parseRecordedKind(kind)
	if !ok {
		return schema.GroupVersionKind{}, exit.RuntimeErrorf(
			"the cluster could not be reached to resolve --kind %q (%v), and short names and plural "+
				"resource names come from its own discovery data. Give the kind as it is recorded — "+
				"Deployment or Deployment.apps — which needs no cluster at all", kind, err)
	}
	if writeErr := options.WriteLine(streams.ErrOut, fmt.Sprintf(
		"→ read --kind %s as %s as recorded, without the cluster: %s", kind,
		render.ScopeKind(query.ScopeInterval{APIGroup: gvk.Group, Kind: gvk.Kind}),
		reachFailure(err))); writeErr != nil {
		return schema.GroupVersionKind{}, writeErr
	}
	return gvk, nil
}

// ScopesRequest is one `scopes` invocation, resolved.
//
// It is exported, and RunScopes takes an already-opened resolve.Backend, for the reason
// TimelineRequest is: the whole of the command's behaviour — the empty finding,
// the covering-namespace notice, the rendering — is then reachable from a test
// holding a fake QueryEngine rather than only from one that can reach a kubeconfig
// and a live sink.
type ScopesRequest struct {
	// ClusterID is the kuberecord cluster identity whose scope log to read (D21).
	ClusterID string

	// APIGroup and Kind narrow the listing to one kind; empty means every kind.
	// The core group is the empty APIGroup with a non-empty Kind, which is a
	// value rather than a wildcard.
	APIGroup string
	Kind     string

	// Namespace narrows the listing, with ScopeQuery's covering reading: a
	// namespace matches both its own scopes and the cluster-wide ones.
	Namespace string

	// From and To bound the window. A period that merely overlaps it is returned
	// whole rather than clipped, because trimming it would make a scope opened
	// last year look as though it opened when the window did.
	From time.Time
	To   time.Time

	// Structured names the serialization of the versioned envelope to write.
	// Empty means the table.
	Structured render.StructuredFormat
}

// scopeQuery builds the read-plane query.
func (r ScopesRequest) scopeQuery() query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: r.ClusterID,
		APIGroup:  r.APIGroup,
		Kind:      r.Kind,
		Namespace: r.Namespace,
		From:      r.From,
		To:        r.To,
	}
}

// describeScope names what was asked about, for the header and for the finding.
//
// Both halves are spelled out even when they are unrestricted, because an empty
// answer has to name the question it is empty about: "nothing was watching" is
// only actionable if the reader can see how wide "nothing" was.
func (r ScopesRequest) describeScope() string {
	kind := "every kind"
	if r.Kind != "" {
		// The table's own spelling of a kind, not the address spelling: the header
		// and the rows under it must not name one kind two ways.
		kind = render.ScopeKind(query.ScopeInterval{APIGroup: r.APIGroup, Kind: r.Kind})
	}
	namespace := "every namespace"
	if r.Namespace != "" {
		namespace = "namespace " + r.Namespace
	}
	return kind + " in " + namespace
}

// RunScopes answers one scopes request against an opened backend and renders the
// result.
func RunScopes(
	ctx context.Context, backend *resolve.Backend, request ScopesRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	coverage, err := askCoverage(ctx, backend, request.scopeQuery(), request.describeScope())
	if err != nil {
		return err
	}
	if coverage.Gap != nil {
		// Every other command degrades around a missing scope log and answers the
		// rest of what it was asked (Invariant 5). This command *is* the scope log,
		// so there is no remaining half to answer with, and saying so plainly is
		// better than an empty table that would read as "nothing was watching".
		return exit.RuntimeErrorf("the %s backend has no scope log, so it cannot say what was being "+
			"recorded: %w", backend.Engine.Capabilities().Backend, coverage.Gap)
	}

	notices := appendNotice(nil, coveringNamespaceNotice(request, coverage.Intervals))
	if writeErr := writeScopesAnswer(backend, request, coverage, notices, streams, opts); writeErr != nil {
		return writeErr
	}
	return scopesFinding(request, coverage.Intervals)
}

// writeScopesAnswer renders the answer in whichever shape was asked for.
func writeScopesAnswer(
	backend *resolve.Backend, request ScopesRequest, coverage coverageAnswer,
	notices []render.Notice, streams genericiooptions.IOStreams, opts render.Options,
) error {
	if request.Structured == "" {
		document := render.ScopesDocument{
			Cluster:   request.ClusterID,
			Scope:     request.describeScope(),
			Window:    options.DescribeWindow(request.From, request.To),
			Intervals: coverage.Intervals,
			Notices:   notices,
		}
		if err := render.WriteScopes(streams.Out, streams.ErrOut, document, opts); err != nil {
			return exit.RuntimeErrorf("%w", err)
		}
		return nil
	}

	// metadata.coverage repeats what the items say, and deliberately so: a
	// consumer branches on the same field for every kind of envelope this CLI
	// produces, and a Coverage envelope whose metadata was the one exception would
	// be the shape every script had to special-case.
	stream, err := render.NewStream(
		streams.Out, request.Structured, envelopeHead(backend, render.KindCoverage, coverage))
	if err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	items := make([]any, 0, len(coverage.Intervals))
	for _, interval := range coverage.Intervals {
		items = append(items, interval)
	}
	if err := writeItems(stream, items); err != nil {
		return err
	}
	if err := render.WriteNotices(streams.ErrOut, notices, opts); err != nil {
		return exit.RuntimeErrorf("%w", err)
	}
	return nil
}

// scopesFinding turns an empty listing into the finding it is.
//
// It is returned after the document has been written rather than instead of it,
// because the header is what says which question came back empty and a finding
// with no question attached is not actionable.
//
// It wraps query.ErrNoCoverage so that exit.CodeFor gives it exit code 3 without
// this call site having to know the number, which is the same code `timeline`
// reaches when it works the same fact out from the other end. A script watching
// for "was anything recording this" therefore keys on one code whichever command
// it asked.
func scopesFinding(request ScopesRequest, intervals []query.ScopeInterval) error {
	if len(intervals) > 0 {
		return nil
	}
	return fmt.Errorf("%w: no watch scope covering %s was open in cluster %q during %s, so a "+
		"silence there is not evidence that nothing changed — nothing was being recorded to change",
		query.ErrNoCoverage, request.describeScope(), request.ClusterID,
		options.DescribeWindow(request.From, request.To))
}

// coveringNamespaceNotice explains a cluster-wide row in a namespaced question.
//
// ScopeQuery's namespace has the covering reading, so asking about one namespace
// returns the all-namespaces scopes too — they genuinely were watching objects in
// it. Without this line, a row saying (all) in reply to `-n payments` reads as the
// filter having been ignored, which is the one thing it must not read as.
func coveringNamespaceNotice(request ScopesRequest, intervals []query.ScopeInterval) render.Notice {
	if request.Namespace == "" {
		return render.Notice{}
	}
	if !slices.ContainsFunc(intervals, func(i query.ScopeInterval) bool { return i.Namespace == "" }) {
		return render.Notice{}
	}
	return render.Notice{Text: fmt.Sprintf(
		"rows marked %s are cluster-wide scopes: they were watching objects in %s as well, so they "+
			"are part of the answer rather than the --namespace filter being ignored",
		render.AllNamespaces, request.Namespace)}
}
