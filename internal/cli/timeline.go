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
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/coldscan"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/replay"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `timeline` is the command the release exists for.
//
// The target is one row that names the actor and the field that changed, side by
// side, at 02:47, from one command with no SQL in it. Everything else in this
// file is in service of that row being both present and honest:
//
//   - The incarnation is chosen here rather than implicitly by the engine, so
//     that the UID in the header, the banner about the others, and the rows in
//     the table cannot disagree about which object is being shown (Invariant 7).
//   - Coverage is consulted on every invocation, not only on an empty one,
//     because the header states it — and because an empty result presented
//     without it cannot tell "nothing changed" from "nothing was watching"
//     (Invariant 9).
//   - A backend that cannot record deletions gets a notice saying so, because a
//     timeline that simply stops is otherwise indistinguishable from an object
//     that is still there (Invariant 4).
//   - --with-events that interleaves nothing is explained against the coverage of
//     Events themselves, because a flag whose output is identical to its absence
//     cannot be told from a flag that was ignored (Invariant 9, D31).
//   - --events-only moves the *subject* of all of that from the object to the
//     Events about it: the coverage stated in the header, the explanation of an
//     empty page, and the scope a silence is measured against are all the Event
//     scope. A header describing a scope the rows did not come from would be a
//     document disagreeing with itself (D45), and the object's own no-coverage
//     finding would be an error about state raised against a reader who had just
//     excluded it.
//
// The command writes the document to stdout and every qualification of it to
// stderr. See internal/cli/render for the rule and why it is worth keeping.

// defaultLimit is how many changes a bare invocation renders.
//
// A hundred, and they are the *newest* hundred, because the question that brings
// somebody here is "what happened to this recently" and because both backends
// answer a reverse-limited query cheaply — the object archive has a short circuit
// that stops walking partitions once the limit is filled, which an unlimited
// query would forfeit.
//
// Which hundred are selected and which way up they are printed are separate
// decisions, and only the second is a matter of taste. They are printed oldest
// first, because this CLI does not page: the last line written sits immediately
// above the prompt, so the newest change belongs there rather than a hundred rows
// up. See holdForDisplayOrder for the trap in conflating the two.
const defaultLimit = 100

// timelineCommand is the command a notice points a reader at when the question
// it raises is answerable somewhere other than where it was asked. It is bare,
// as scopesCommand is, because a notice names the subcommand and the reader
// already knows how they invoked the binary.
const timelineCommand = "timeline"

// scopesCommand is the command a notice points a reader at when the answer
// depends on what was being watched.
const scopesCommand = "scopes"

// timelineFlags is one invocation's own flag surface, kept apart from the global
// one so that the command's dependencies are visible in its signature rather
// than reachable through a package-level variable two concurrently-built roots
// would share.
type timelineFlags struct {
	window          windowFlags
	limit           int
	reverse         bool
	actors          []string
	excludeActors   []string
	fields          []string
	uid             string
	allIncarnations bool
	full            bool
	withEvents      bool
	eventsOnly      bool
}

// newTimelineCommand builds `timeline`.
func newTimelineCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	local := &timelineFlags{limit: defaultLimit}

	command := &cobra.Command{
		Use:   "timeline (KIND/NAME | KIND NAME)",
		Short: "Show what changed on one object, and who changed it",
		Long: `Show what changed on one object, and who changed it.

Each row is one recorded change: when it happened, what kind of change it was,
the field managers seen on the object, and the field that moved. A single-field
edit renders as one line with the old and the new value; a larger patch renders
as a count, and --full expands it.

The history shown is one incarnation's. A name that has been reused belongs to
several objects with different UIDs, and splicing their histories together would
be a coherent-looking account of something that never happened, so the newest is
chosen, the others are named, and --all-incarnations shows them all.

--with-events adds the Kubernetes Events recorded about the object, and
--events-only shows those and none of its own changes — a Deployment's rows are
mostly status churn, and the Events are where the decisions are. Under it the
coverage reported is the coverage of Events, because that is the scope the rows
came from.

An empty result is never presented on its own. It is explained against the watch
scopes that were open at the time: "nothing changed" and "nothing was watching"
are different findings, and the second exits ` + fmt.Sprint(exit.NoCoverage) + `.`,
		Example: `  # What happened to this Deployment lately.
  kuberecord timeline deploy/checkout -n payments

  # Only the changes one controller made, over the last two days.
  kuberecord timeline deploy/checkout -n payments --since 2d --actor kube-controller-manager

  # Only the changes that touched the container images, with every operation.
  kuberecord timeline deploy/checkout -n payments --field spec.template.spec.containers --full

  # With the Kubernetes Events that were recorded about it, newest first.
  kuberecord timeline pod/checkout-7d4f -n payments --with-events --reverse

  # Only what Kubernetes said about it, without the status churn in between.
  kuberecord timeline deploy/checkout -n payments --events-only`,

		// The kind completes from the static short-name table; the name is an
		// object in a cluster or an archive, and is not read from here. See
		// completeObjectAddress.
		ValidArgsFunction: completeObjectAddress,

		RunE: func(cmd *cobra.Command, args []string) error {
			// Before anything reads the window: past this line the command sees one
			// spelling of each bound, which is what keeps the alias a fact about the
			// flag layer rather than a branch in every caller.
			if err := local.window.resolve(cmd.Flags()); err != nil {
				return err
			}
			return runTimelineCommand(cmd.Context(), flags, local, args, streams, invokedAs)
		},
	}

	local.window.addFlags(command.Flags(),
		"Only changes at or after this point: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z).",
		"Only changes at or before this point, in the same forms as --since.")
	command.Flags().IntVar(&local.limit, "limit", local.limit,
		"Show at most this many changes. It selects the newest ones, which are then "+
			"displayed oldest first. Zero means no limit.")
	command.Flags().BoolVar(&local.reverse, "reverse", local.reverse,
		"Show the same changes newest first. It reorders the rows; it does not select different ones.")
	command.Flags().StringSliceVar(&local.actors, "actor", local.actors,
		"Only changes with one of these field managers. Repeatable. "+
			"Note that a deletion records no actors, so any --actor excludes every deletion.")
	command.Flags().StringSliceVar(&local.excludeActors, "exclude-actor", local.excludeActors,
		"Drop changes with one of these field managers. Applied after --actor and wins on conflict.")
	command.Flags().StringSliceVar(&local.fields, "field", local.fields,
		"Only changes touching one of these field paths, matched by prefix. "+
			"Either spelling works: spec.containers[0].image or spec.containers.0.image.")
	command.Flags().StringVar(&local.uid, "uid", local.uid,
		"Pin the timeline to one incarnation by UID.")
	command.Flags().BoolVar(&local.allIncarnations, "all-incarnations", local.allIncarnations,
		"Show every incarnation of this name in the window, with a UID column so none of them blur together.")
	command.Flags().BoolVar(&local.full, "full", local.full,
		"Print every operation of every patch, unshortened.")
	command.Flags().BoolVar(&local.withEvents, "with-events", local.withEvents,
		"Interleave the Kubernetes Events recorded about this object. Both Event API groups are correlated.")
	command.Flags().BoolVar(&local.eventsOnly, "events-only", local.eventsOnly,
		"Show those Events and none of the object's own changes. Implies --with-events, and the "+
			"header then reports the coverage of Events rather than of the object, because that is "+
			"the scope the rows came from. An Event carries no patch, so --full and --field have "+
			"nothing to act on; --actor and --all-incarnations likewise, and any of them is reported "+
			"as ignored rather than dropped quietly. --uid still pins the Events to one incarnation.")

	return command
}

// runTimelineCommand turns one invocation into a request, opens the backend, and
// runs it.
//
// The backend is opened before the address is resolved so that the resolution
// notice on stderr — which sink, which cluster identity — precedes anything said
// about the object, and so that a reader watching a slow question already knows
// what is being asked.
func runTimelineCommand(
	ctx context.Context, flags *options.GlobalFlags, local *timelineFlags,
	args []string, streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	structured, err := timelineFormat(flags.Output)
	if err != nil {
		return err
	}
	arg, err := ParseResourceArg(args)
	if err != nil {
		return err
	}
	if local.uid != "" && local.allIncarnations {
		return exit.UsageErrorf("--uid and --all-incarnations contradict each other: " +
			"one pins the timeline to a single incarnation and the other shows every one of them")
	}
	if local.limit < 0 {
		return exit.UsageErrorf("--limit %d is negative; zero means no limit", local.limit)
	}

	now := time.Now()
	from, to, err := parseWindow(local.window.since, local.window.until, now, flags.Zone())
	if err != nil {
		return err
	}

	backend, ref, err := resolveObject(ctx, flags, streams, invokedAs, arg)
	if err != nil {
		return err
	}
	defer func() {
		// Joined rather than replacing: the reason the command is ending is
		// whatever happened above, and the tidying up must not hide it.
		err = errors.Join(err, backend.Close())
	}()

	request := TimelineRequest{
		Ref:             ref,
		From:            from,
		To:              to,
		Now:             now,
		UID:             local.uid,
		AllIncarnations: local.allIncarnations,
		Actors:          local.actors,
		ExcludeActors:   local.excludeActors,
		FieldPaths:      normalizeFieldPaths(local.fields),
		Limit:           local.limit,
		Reverse:         local.reverse,
		WithEvents:      local.withEvents,
		EventsOnly:      local.eventsOnly,
		Structured:      structured,
		Scan:            coldscan.OptionsFrom(flags, streams),
	}
	return RunTimeline(ctx, backend, request, streams, timelineRenderOptions(flags, local, streams))
}

// resolveObject opens the backend and turns an address into the canonical
// identity the read plane answers questions about.
//
// It is the half of every object command that is not about the question being
// asked: which backend, which cluster identity, which kind the address names,
// which namespace the question is in. Sharing it is what keeps `timeline`, `diff`
// and `get` reading the same object for the same address — three copies of this
// sequence would be three chances for one of them to resolve a short name
// differently, and a command that quietly reads a different object's history is
// the worst defect this CLI could ship.
//
// The backend is opened before the address is resolved so that the resolution
// notice on stderr — which sink, which cluster identity — precedes anything said
// about the object. The caller owns the returned backend and must Close it; on
// failure this closes it, so a caller only has one path to think about.
func resolveObject(
	ctx context.Context, flags *options.GlobalFlags, streams genericiooptions.IOStreams,
	invokedAs string, arg ResourceArg,
) (*resolve.Backend, query.ObjectRef, error) {
	backend, err := resolveBackend(ctx, flags, streams, invokedAs)
	if err != nil {
		return nil, query.ObjectRef{}, err
	}

	ref, err := objectRefFor(flags, streams, backend, arg)
	if err != nil {
		return nil, query.ObjectRef{}, errors.Join(err, backend.Close())
	}
	return backend, ref, nil
}

// resolveBackend runs the resolution chain and opens what it chose.
//
// It is the half of resolveObject that has nothing to do with an object, and it
// is separate because `scopes` needs exactly that half: it asks about what was
// being recorded rather than about one object's history, and there is no address
// for it to resolve. The caller owns the returned backend and must Close it.
func resolveBackend(
	ctx context.Context, flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) (*resolve.Backend, error) {
	resolver, err := resolve.NewBackendResolver(flags, streams, invokedAs)
	if err != nil {
		return nil, err
	}
	return resolver.Resolve(ctx)
}

// objectRefFor resolves the address against the opened backend's cluster
// identity.
func objectRefFor(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, backend *resolve.Backend, arg ResourceArg,
) (query.ObjectRef, error) {
	resolved, err := resolveObjectAddress(flags, streams, arg)
	if err != nil {
		return query.ObjectRef{}, err
	}
	namespace, err := objectNamespace(flags, streams, resolved)
	if err != nil {
		return query.ObjectRef{}, err
	}
	return resolved.ObjectRef(backend.ClusterID, namespace), nil
}

// parseWindow reads a pair of --since/--until values against one instant.
//
// now is threaded through rather than read here so that both ends of one window
// are computed against the same instant: a --since and a --until evaluated a
// microsecond apart would produce a window whose width depended on how fast the
// process started.
// zone is the frame the contradiction below spells its two instants in. A usage
// error is a human-facing instant like any other, and one rendered in a frame the
// rest of the invocation is not in would make the reader convert before they
// could see why the bounds cross (Task 18.9).
func parseWindow(since, until string, now time.Time, zone render.Zone) (from, to time.Time, err error) {
	if since != "" {
		if from, err = options.ParseInstant(since, now); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if until != "" {
		if to, err = options.ParseInstant(until, now); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return time.Time{}, time.Time{}, exit.UsageErrorf(
			"the window ends before it starts: --since resolves to %s and --until to %s",
			zone.Instant(from), zone.Instant(to))
	}
	return from, to, nil
}

// timelineFormat decides which of this command's two renderings an invocation
// asked for, and refuses the one it does not have.
//
// An empty StructuredFormat means the table; anything else is the envelope. `diff`
// is refused by name rather than quietly rendered as a table, for the reason
// `config view` refuses a format it cannot produce: a user who asked for one
// shape and received another has been answered in a form their eye or their
// script cannot read, and finding that out at the `jq` is worse than finding it
// out here. It is refused rather than implemented because the hunk rendering is a
// whole command — `diff` — and a second entrance to it would be a second place
// for the two to drift apart.
func timelineFormat(format options.OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case options.OutputTable, options.OutputWide:
		return "", nil
	case options.OutputDiff:
		return "", exit.UsageErrorf("timeline does not render %s: its rows are one line each by design. "+
			"The `diff` command spends the whole page on the same changes, with the old value beside "+
			"the new one", options.OutputDiff)
	}
	structured, ok := structuredFormat(format)
	if !ok {
		return "", exit.UsageErrorf("timeline cannot render %s", format)
	}
	return structured, nil
}

// normalizeFieldPaths accepts the display spelling of a path as well as the
// filter one.
//
// Without it the tool would print "containers[0].image" and then answer "no
// changes" to a --field spelled exactly the way it had just printed it — an
// empty result manufactured by the tool's own two grammars.
func normalizeFieldPaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(paths))
	for _, path := range paths {
		normalized = append(normalized, render.NormalizeFieldPath(path))
	}
	return normalized
}

// timelineRenderOptions decides how the document will look.
func timelineRenderOptions(
	flags *options.GlobalFlags, local *timelineFlags, streams genericiooptions.IOStreams,
) render.Options {
	return render.Options{
		Width: options.TerminalWidth(streams.Out),
		Color: options.ShouldColorize(flags.Color, streams.Out),
		Wide:  flags.Output == options.OutputWide,
		Full:  local.full,
		Zone:  flags.Zone(),
		// The footer names --events-only under a document that holds both kinds of
		// row, and must not name it to somebody already using it. See eventsHint.
		EventsOnly: local.eventsOnly,
	}
}

// resolveObjectAddress maps the address onto a kind, through the cluster when
// one can be reached and offline when one cannot.
//
// It is shared by every command that names an object, which is why neither it nor
// objectNamespace is spelled after the command that arrived first.
//
// The offline path exists because an archive on a laptop is a supported way to
// read history (D18, and docs/CLI.md's evaluation mode), and the cluster the
// changes happened in may not exist any more. It handles only the address form
// that needs no discovery data: a capitalised Kind, optionally group-qualified,
// which is exactly the identity the schema stores. It cannot expand `deploy` or
// pluralize `deployments`, because doing either without the server's own
// discovery data would be a guess — and a guess here silently reads a different
// object's history.
func resolveObjectAddress(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, arg ResourceArg,
) (ResolvedResource, error) {
	reach, err := clusterResolution(flags, arg)
	if err == nil {
		return reach, nil
	}

	// A cluster that answered and said it does not serve this kind is the user's
	// spelling, not the tool's reach. Falling back here would take a typo and
	// resolve it into a kind nobody has, then report an empty timeline for it.
	var unknown *UnknownResourceError
	if errors.As(err, &unknown) {
		return ResolvedResource{}, err
	}

	resolved, offlineErr := resolveKindOffline(arg, explicitNamespace(flags) != "")
	if offlineErr != nil {
		return ResolvedResource{}, exit.RuntimeErrorf(
			"the cluster could not be reached to resolve %q (%v), and %w",
			arg.Resource, err, offlineErr)
	}
	if writeErr := options.WriteLine(streams.ErrOut, fmt.Sprintf(
		"→ read %s/%s as recorded, without the cluster: %s",
		describeGroupKind(resolved.GVK), resolved.Name, reachFailure(err))); writeErr != nil {
		return ResolvedResource{}, writeErr
	}
	return resolved, nil
}

// clusterResolution asks the cluster what the address names.
//
// The REST mapper cli-runtime builds is *lazy*: constructing it succeeds against
// an unreachable API server and the connection failure surfaces on the first
// lookup. So both steps are taken here and reported as one failure, which is what
// lets the caller decide between "the cluster said no" and "there was no cluster
// to ask" from a single error.
func clusterResolution(flags *options.GlobalFlags, arg ResourceArg) (ResolvedResource, error) {
	mapper, err := flags.ConfigFlags.ToRESTMapper()
	if err != nil {
		return ResolvedResource{}, err
	}
	return NewResolver(mapper).Resolve(arg)
}

// reachFailure trims a discovery failure to the sentence a notice can carry.
//
// The innermost cause is kept and the wrappers around it dropped: client-go's own
// message names a URL, a timeout and a dial error, and after a line that has
// already said the cluster was not reached, "connection refused" is the half that
// tells the reader something. The whole of it is still available at -v, and
// client-go has usually logged it unprompted anyway.
func reachFailure(err error) string {
	message := err.Error()
	if cut := strings.LastIndex(message, ": "); cut >= 0 && cut < len(message)-2 {
		return message[cut+2:]
	}
	return message
}

// describeGroupKind renders a kind the way the document's header does.
func describeGroupKind(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return gvk.Kind
	}
	return gvk.Kind + "." + gvk.Group
}

// objectNamespace resolves the namespace the question is asked in, and says so
// when the answer is discarded.
//
// A cluster-scoped kind has no namespace in the recorded history, so a --namespace
// given for one is dropped. It is announced rather than dropped quietly: the user
// narrowed their question and the tool widened it back, and a result that did not
// obey a flag has to say which flag it did not obey.
func objectNamespace(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, resolved ResolvedResource,
) (string, error) {
	namespace, err := flags.Namespace()
	if err != nil {
		return "", exit.RuntimeErrorf("%w", err)
	}
	if resolved.Namespaced {
		return namespace, nil
	}
	if named := explicitNamespace(flags); named != "" {
		if writeErr := options.WriteLine(streams.ErrOut, fmt.Sprintf(
			"→ %s is cluster-scoped, so --namespace %s is not part of its identity and was ignored",
			resolved.GVK.Kind, named)); writeErr != nil {
			return "", writeErr
		}
	}
	return "", nil
}

// explicitNamespace is the namespace the user typed, as opposed to the one the
// kubeconfig supplies.
//
// The distinction matters twice: a --namespace given for a cluster-scoped kind is
// a flag being ignored and has to be announced, and offline it is the only signal
// there is about whether the address names a namespaced object at all.
func explicitNamespace(flags *options.GlobalFlags) string {
	if flags.ConfigFlags.Namespace == nil {
		return ""
	}
	return *flags.ConfigFlags.Namespace
}

// TimelineRequest is one `timeline` invocation, resolved.
//
// It is exported, and RunTimeline takes an already-opened resolve.Backend, so that the
// whole of the command's behaviour — the incarnation choice, the coverage
// explanation, the capability notices, the rendering — is reachable from a test
// holding a fake QueryEngine. A command whose only entry point went through a
// kubeconfig and a live sink would have its interesting half tested by nothing.
type TimelineRequest struct {
	// Ref is the object whose history is wanted.
	Ref query.ObjectRef

	// From and To bound the window; a zero value is unbounded on that side,
	// subject to the backend's own TimeBoundRequired.
	From time.Time
	To   time.Time

	// Now is the instant this invocation treats as the present, used for the
	// default window a backend forces. Zero means time.Now.
	Now time.Time

	// UID pins the timeline to one incarnation.
	UID string

	// AllIncarnations shows every incarnation in the window instead of the
	// newest.
	AllIncarnations bool

	// AllIncarnationsOffered says the command being run has the
	// --all-incarnations flag, so the banner that names the incarnations it is
	// not showing may point at it.
	//
	// `timeline` is the only one. `diff` and `blame` deliberately have no such
	// flag — one field table or one diff spanning two UIDs is the splice
	// Invariant 7 forbids, and blame.go states why at length — and the banner is
	// shared by all three, so without this it answered "how do I see the others?"
	// on two commands with a flag they reject.
	//
	// Spelled as an offer rather than as a suppression, unlike NoPriorValues,
	// because the failures are not symmetrical: a command that forgets to set
	// this loses one suggestion from a notice, while a command that had to
	// remember to *unset* it would send a reader to a usage error. So the
	// conservative claim is the zero value, and RunTimeline pins the other one on
	// the way in rather than trusting its callers to.
	AllIncarnationsOffered bool

	// Actors, ExcludeActors and FieldPaths are the read plane's predicates,
	// already in its own grammar.
	Actors        []string
	ExcludeActors []string
	FieldPaths    []string

	// DisplayFieldPaths narrows the *rendered* rows without narrowing the query.
	//
	// It exists for `diff`, whose entire output is the value each operation
	// destroyed. Pushing a path predicate into the query would make the returned
	// rows a non-consecutive slice of history, which switches the prior-value
	// replay off (see filtered) and leaves every hunk with its "+" half and no
	// "-" half — the command's whole point, removed by one of its own flags. So
	// the query stays unfiltered, the replay runs over the consecutive run it
	// needs, and the narrowing happens here afterwards. The cost is that the
	// backend reads rows nobody will see, which is stated in a notice rather than
	// absorbed silently.
	DisplayFieldPaths []string

	// NoPriorValues suppresses the state replay that recovers the value each
	// operation destroyed.
	//
	// `blame` sets it. That command replays the same rows itself, forward from the
	// same anchor, to work out which change last wrote each field — so leaving the
	// prior-value replay on would buy a second round trip to fill in values its
	// table never prints, and print a notice about arrows that appear nowhere in
	// it. It is spelled as a suppression rather than as an opt-in because the
	// replay is what `timeline` and `diff` are for, and a field named the other way
	// round would make forgetting it cost the release's flagship column.
	NoPriorValues bool

	// Limit caps the changes rendered; zero means no cap.
	Limit int

	// Reverse displays the same changes newest first, against a default of oldest
	// first. It is a rendering choice and deliberately not the query's own
	// Reverse: the query always asks for the newest first, because that is the
	// shape both backends answer cheaply, and only how they are laid out follows
	// this field.
	Reverse bool

	// WithEvents interleaves the Kubernetes Events recorded about the object.
	WithEvents bool

	// EventsOnly narrows the answer to those Events, showing none of the object's
	// own changes.
	//
	// It implies WithEvents rather than requiring it — see includeEvents, which is
	// how every reader of the pair asks the question — because the two compose and
	// demanding both would be a usage error for a request nobody could misread.
	//
	// What it changes beyond the rows is the *subject*. A document made of Events
	// is explained by the coverage of Events: the header reports that scope, the
	// empty case is explained against it, and the object's own scope is not
	// consulted at all. That last part is the trap this field exists around. A
	// timeline of an object nobody watched is the no-coverage finding, and under
	// this flag it would be an error about state answering a question nobody asked
	// — suppressing a page of perfectly good commentary to report the absence of
	// something the reader had excluded. See gatherChanges.
	EventsOnly bool

	// Scan is the cold-scan safety surface: the confirmation, the circuit breaker
	// and whether either can be shown. It travels with the request rather than
	// being read from the flags where it is used, so that a test can drive the
	// guard without a pseudo-terminal — the same reason render.Options carries the
	// terminal width instead of asking for it.
	Scan coldscan.Options

	// Structured names the serialization of the versioned envelope to write.
	// Empty means the table, which is the default and the rendering this release
	// exists for; anything else routes to the structured path, which streams
	// under `jsonl` rather than gathering the whole answer first.
	Structured render.StructuredFormat
}

// filtered reports whether a predicate makes the rendered rows a non-consecutive
// slice of history.
//
// It gates the prior-value replay. Replaying only the surviving patches over a
// real base state would produce a document the object was never in, and the old
// values read out of it would be confident and wrong — which in an audit
// timeline is worse than their being absent.
func (r TimelineRequest) filtered() bool {
	if r.EventsOnly {
		// The predicates narrow the object's own changes, and there are none to
		// narrow: every row is commentary, which --actor and --field deliberately
		// leave alone (see sawChange). Reporting a filter in force here would send
		// predicateNotice to re-read a window for an emptiness no predicate caused,
		// and would suppress the prior-value replay that is already a no-op. What the
		// reader is owed instead is the flag they passed and its absence of effect,
		// which inertPredicateNotice says once and in those terms.
		return false
	}
	return len(r.Actors) > 0 || len(r.ExcludeActors) > 0 || len(r.FieldPaths) > 0
}

// includeEvents reports whether the answer carries Kubernetes Events at all.
//
// It is the question every reader of the pair asks, and it exists so that
// --events-only implying --with-events is a fact about the request rather than
// something four call sites have to remember. A field the wiring had to set twice
// is one a second entry point — a test, a future command — sets once, and the half
// it forgot is a flag that silently does nothing.
func (r TimelineRequest) includeEvents() bool { return r.WithEvents || r.EventsOnly }

// eventsFlag is the flag an Event notice names.
//
// A notice that named --with-events to somebody who typed --events-only would be
// telling them about a flag they did not pass, which is the affordance sweep's
// complaint in reverse: the route out has to be the one they are on.
func (r TimelineRequest) eventsFlag() string {
	if r.EventsOnly {
		return eventsOnlyFlag
	}
	return withEventsFlag
}

// The two spellings of the Event question, as a reader typed them.
const (
	withEventsFlag = "--with-events"
	eventsOnlyFlag = "--events-only"
)

// RunTimeline answers one timeline request against an opened backend and renders
// the result.
//
// The gathering is shared with `diff` (see gather.go) and the layout is not: the
// two commands ask the backend the identical question, and the whole of their
// difference is how the answer is laid out.
func RunTimeline(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	// Pinned here rather than filled in by the caller, as RunBlame pins Reverse:
	// having the flag is a fact about this command, and a field the wiring had to
	// remember to set is one a second call site — a test, a future entry point —
	// would leave false, silently costing the banner its suggestion.
	request.AllIncarnationsOffered = true

	if request.Structured != "" {
		return runTimelineStructured(ctx, backend, request, streams, opts)
	}

	gathered, err := gatherChanges(ctx, backend, request, streams, opts.Zone)
	if err != nil {
		return err
	}

	document := render.TimelineDocument{
		Kind:           describeKind(request.Ref),
		Object:         describeObject(request.Ref),
		Cluster:        request.Ref.ClusterID,
		UID:            gathered.UID,
		Incarnations:   gathered.Incarnations,
		Coverage:       gathered.Coverage.Summary(opts.Zone),
		CoverageOf:     coverageSubject(request),
		CoverageAbsent: gathered.Coverage.Absent(),
		Rows:           gathered.Rows,
		Notices:        gathered.Notices,
	}
	if writeErr := render.WriteTimeline(streams.Out, streams.ErrOut, document, opts); writeErr != nil {
		return exit.RuntimeErrorf("%w", writeErr)
	}
	return gathered.Empty
}

// coverageSubject names the scope the header's coverage line is about, when it is
// not the object's own.
//
// It is the rendering half of relevantCoverage, and the two are deliberately one
// decision expressed twice: what the command *asked* about and what the header
// *says* it asked about must be the same scope, and the alternative — a renderer
// inferring the subject from the intervals it was handed — would be a second
// reading of an answer the command already has.
//
// "Events" rather than "Kubernetes Events" because the label is a column of a
// header block whose other labels are one word, and the rows under it say Event in
// their own column.
func coverageSubject(request TimelineRequest) string {
	if request.EventsOnly {
		return "Events"
	}
	return ""
}

// timelineBounds completes the window a backend insists on, and says so.
//
// It acts only where it is needed. A backend that can answer an unbounded
// question is asked one, because the change an engineer is hunting at 02:47 is as
// likely to be six weeks old as six hours, and a default window would hide it
// behind a flag they did not know to pass.
//
// A *half* window is completed rather than refused. An engine requiring a bound
// requires both of them, so `--since 3d` alone would otherwise come back as
// ErrTimeBoundRequired naming the flag the user had just used — a message that
// reads as a bug. Completing it and saying which end was supplied answers the
// question they asked.
func timelineBounds(
	request TimelineRequest, capabilities query.Capabilities, zone render.Zone,
) (from, to time.Time, notice render.Notice) {
	from, to = request.From, request.To
	if !capabilities.TimeBoundRequired || (!from.IsZero() && !to.IsZero()) {
		return from, to, render.Notice{}
	}

	now := request.Now
	if now.IsZero() {
		now = time.Now()
	}

	switch {
	case from.IsZero() && to.IsZero():
		from, to = now.Add(-options.DefaultWindow), now
		return from, to, render.Notice{Text: fmt.Sprintf(
			"the %s backend cannot answer an unbounded question, so the window defaults to %s; "+
				"pass --since to widen it", capabilities.Backend, options.DescribeWindow(from, to, zone))}
	case from.IsZero():
		from = to.Add(-options.DefaultWindow)
	default:
		to = now
	}
	return from, to, render.Notice{Text: fmt.Sprintf(
		"the %s backend needs both ends of a window, so this one was completed to %s; "+
			"pass --since and --until to set it yourself", capabilities.Backend,
		options.DescribeWindow(from, to, zone))}
}

// timelineQuery builds the read-plane query.
//
// Reverse is always set, whatever --reverse asked for, and whatever the rows are
// laid out as. The flag reorders the rendered rows; the query always fetches the
// newest first, which is the shape both backends answer cheaply — the object
// archive stops walking partitions once a reverse-limited query's limit is
// filled, and an oldest-first query would forfeit that as well as selecting the
// oldest N rather than the newest.
func (r TimelineRequest) timelineQuery(selection incarnationChoice, from, to time.Time) query.TimelineQuery {
	return query.TimelineQuery{
		Ref:             r.Ref,
		From:            from,
		To:              to,
		UID:             selection.pinned,
		AllIncarnations: r.AllIncarnations,
		Actors:          r.Actors,
		ExcludeActors:   r.ExcludeActors,
		FieldPaths:      r.FieldPaths,
		Limit:           r.Limit,
		Reverse:         true,
		IncludeEvents:   r.includeEvents(),
		EventsOnly:      r.EventsOnly,
	}
}

// scopeQuery asks which scopes covered the object, with ScopeQuery's covering
// reading of a namespace: a cluster-wide rule genuinely was watching an object in
// that namespace, and reporting otherwise would answer "never observed" about an
// object that was observed the whole time.
func (r TimelineRequest) scopeQuery(from, to time.Time) query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: r.Ref.ClusterID,
		APIGroup:  r.Ref.APIGroup,
		Kind:      r.Ref.Kind,
		Namespace: r.Ref.Namespace,
		From:      from,
		To:        to,
	}
}

// collectChanges drains a timeline iterator.
//
// Err is checked after the loop and Close on every path, including the early
// return: skipping either turns a backend that failed halfway into a result that
// looks complete and merely short, which for an audit timeline is the worst
// available outcome.
func collectChanges(
	ctx context.Context, engine query.QueryEngine, q query.TimelineQuery,
) (changes []query.Change, err error) {
	iterator, err := engine.Timeline(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("releasing the change stream: %w", closeErr)
		}
	}()

	for iterator.Next() {
		changes = append(changes, iterator.Change())
	}
	if iterErr := iterator.Err(); iterErr != nil {
		return nil, iterErr
	}
	return changes, nil
}

// timelineQueryError phrases a failed query, naming the flag that fixes the one
// failure a flag can fix.
//
// The scan's own context is consulted first, because a query cancelled by this
// CLI's circuit breaker fails with a context error wherever the scan happened to
// be — a message that names neither the flag that stopped it nor the fact that
// something deliberately did. See coldscan.Stopped.
func timelineQueryError(ctx context.Context, request TimelineRequest, err error) error {
	if stopped := coldscan.Stopped(ctx); stopped != nil {
		return exit.RuntimeErrorf("reading the timeline of %s: %w", describeObject(request.Ref), stopped)
	}
	if errors.Is(err, query.ErrTimeBoundRequired) {
		return exit.RuntimeErrorf("%w; pass --since (and optionally --until) to bound it", err)
	}
	if errors.Is(err, query.ErrNoCoverage) {
		// The sentinel already carries exit code 3 through exit.CodeFor, so it is
		// wrapped for context rather than reclassified.
		return fmt.Errorf("reading the timeline of %s: %w", describeObject(request.Ref), err)
	}
	return exit.RuntimeErrorf("reading the timeline of %s: %w", describeObject(request.Ref), err)
}

// priorValueNotices recovers the value each operation replaced, or explains why
// it did not.
//
// rows arrive in the query's own order — newest first — because this runs before
// the display order is applied, and the replay must run oldest first, so it walks
// a reversed clone. The clone copies the row structs, but each row's Ops field is
// a slice header over the same backing array, so the Op.Old values the replay
// fills in are visible through the original rows — which is what the renderer
// reads. Reversing in place instead would leave the caller holding an order it did
// not ask for, and calling this after the display flip would reverse rows that
// were already ascending and attribute every prior value to the wrong change.
func priorValueNotices(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest, rows []render.TimelineRow,
	zone render.Zone,
) []render.Notice {
	if len(rows) == 0 || request.NoPriorValues {
		return nil
	}
	if request.filtered() {
		return []render.Notice{{Text: "prior values are not shown because a filter is in force: " +
			"the changes below are not consecutive, and replaying only these patches would report " +
			"values the object never held"}}
	}

	ascending := slices.Clone(rows)
	slices.Reverse(ascending)
	return replay.PriorValues(ctx, engine, request.Ref, ascending, zone)
}

// deletionsNotice reports a backend that cannot record deletions, when nothing
// in the result rules one out.
//
// The predicate is written honestly rather than shortened to the capability
// alone: a backend declaring no deletions can never produce a Deleted row today,
// but a renderer whose notice does not actually look at the rows would start
// lying on the day one does.
func deletionsNotice(capabilities query.Capabilities, sawDeleted bool) render.Notice {
	if capabilities.Deletions || sawDeleted {
		return render.Notice{}
	}
	return render.Notice{
		Text: fmt.Sprintf("the %s backend does not record deletions, so this timeline ending is not "+
			"evidence that the object still exists; it may have been deleted while unobserved. "+
			"The `%s` command shows what was being watched", capabilities.Backend, scopesCommand),
	}
}

// sawDeletion reports whether a rendered run holds a deletion.
//
// It is the honest half of deletionsNotice's predicate, kept as a function
// because the streaming path never holds the rows and answers the same question
// with a flag it maintained as they went past. Both callers therefore ask the
// same question of the same enum value, rather than one of them asking the
// capability alone — which would start lying on the day a backend that declares
// no deletions produces one.
func sawDeletion(rows []render.TimelineRow) bool {
	return slices.ContainsFunc(rows, func(row render.TimelineRow) bool {
		return row.Change.EventType == query.EventDeleted
	})
}

// sawEvent reports whether a rendered run holds a merged Kubernetes Event.
//
// It is the predicate --with-events is judged by, and it asks about the rows that
// were *rendered* rather than about the ones the query returned. That is the
// honest reading of "the flag produced no visible effect": a reader looking at a
// document with no Event line in it is owed the explanation whether the Events
// were never recorded or were fetched and then set aside.
//
// It is a function for the reason sawDeletion is: the streaming path never holds
// the rows and answers the same question with a flag maintained as they go past,
// and both callers have to be asking about the same enum value.
func sawEvent(rows []render.TimelineRow) bool {
	return slices.ContainsFunc(rows, func(row render.TimelineRow) bool {
		return row.Change.EventType == query.EventKubernetes
	})
}

// sawChange reports whether a rendered run holds a change to the object itself,
// as opposed to a merged Kubernetes Event.
//
// It is the predicate the query's own filters are judged by, and the distinction
// it draws is the one that makes the judgement honest. --actor, --exclude-actor
// and --field narrow the object's changes and are deliberately not applied to
// Events, whose actors column holds the field managers of the Event object rather
// than of whoever changed the subject — so a `--with-events` document holding
// nothing but Event rows is not an answer to the filter, it is the absence of one.
//
// It is a function for the reason sawDeletion and sawEvent are: the streaming
// path never holds the rows and answers the same question with a flag maintained
// as they go past, and both callers have to be asking about the same enum value.
func sawChange(rows []render.TimelineRow) bool {
	return slices.ContainsFunc(rows, func(row render.TimelineRow) bool {
		return row.Change.EventType != query.EventKubernetes
	})
}

// timelineShape is which kinds of row a rendered answer turned out to hold.
//
// The two questions are carried together because the notice that reads them —
// explainNoChanges — turns on the *combination* rather than on either half. A
// document with a change in it is explained by nothing; one with no row at all is
// explained against coverage; one made entirely of Kubernetes Events is explained
// against the same coverage in different words, because the reader is looking at
// rows and would otherwise conclude the object was watched and quiet. Two
// booleans threaded separately through two call sites is the shape that
// eventually arrives in the wrong order at one of them.
type timelineShape struct {
	// changes reports a row about the object itself: an addition, a
	// modification, a checkpoint, a deletion.
	changes bool
	// events reports a merged Kubernetes Event.
	events bool
}

// shapeOf measures a gathered run.
//
// It asks through sawChange and sawEvent rather than walking the rows itself, so
// that the gathered path and the streaming path — which cannot walk anything,
// and maintains the same two facts as its rows go past — are answering with the
// same reading of the same enum value. See emission.shape.
func shapeOf(rows []render.TimelineRow) timelineShape {
	return timelineShape{changes: sawChange(rows), events: sawEvent(rows)}
}

// appendNotice adds a notice only when there is one, so that callers can build a
// list without a conditional at every site.
func appendNotice(notices []render.Notice, notice render.Notice) []render.Notice {
	if notice.Text == "" {
		return notices
	}
	return append(notices, notice)
}

// describeKind renders an object's group and kind for the header.
func describeKind(ref query.ObjectRef) string {
	if ref.APIGroup == "" {
		return ref.Kind
	}
	return ref.APIGroup + "/" + ref.Kind
}

// describeObject renders an object's namespace and name.
func describeObject(ref query.ObjectRef) string {
	if ref.Namespace == "" {
		return ref.Name
	}
	return ref.Namespace + "/" + ref.Name
}
