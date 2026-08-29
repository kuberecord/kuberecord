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

	"github.com/kuberecord/kuberecord/internal/cli/render"
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
//
// The command writes the document to stdout and every qualification of it to
// stderr. See internal/cli/render for the rule and why it is worth keeping.

// defaultLimit is how many changes a bare invocation renders.
//
// A hundred, newest first, because the question that brings somebody here is
// "what happened to this recently" and because both backends answer a
// reverse-limited query cheaply — the object archive has a short circuit that
// stops walking partitions once the limit is filled, which an unlimited query
// would forfeit.
const defaultLimit = 100

// scopesCommand is the command a notice points a reader at when the answer
// depends on what was being watched.
const scopesCommand = "scopes"

// timelineFlags is one invocation's own flag surface, kept apart from the global
// one so that the command's dependencies are visible in its signature rather
// than reachable through a package-level variable two concurrently-built roots
// would share.
type timelineFlags struct {
	since           string
	until           string
	limit           int
	reverse         bool
	actors          []string
	excludeActors   []string
	fields          []string
	uid             string
	allIncarnations bool
	full            bool
	withEvents      bool
}

// newTimelineCommand builds `timeline`.
func newTimelineCommand(
	flags *GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
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

An empty result is never presented on its own. It is explained against the watch
scopes that were open at the time: "nothing changed" and "nothing was watching"
are different findings, and the second exits ` + fmt.Sprint(ExitNoCoverage) + `.`,
		Example: `  # What happened to this Deployment lately.
  kuberecord timeline deploy/checkout -n payments

  # Only the changes one controller made, over the last two days.
  kuberecord timeline deploy/checkout -n payments --since 2d --actor kube-controller-manager

  # Only the changes that touched the container images, with every operation.
  kuberecord timeline deploy/checkout -n payments --field spec.template.spec.containers --full

  # With the Kubernetes Events that were recorded about it, oldest first.
  kuberecord timeline pod/checkout-7d4f -n payments --with-events --reverse`,

		RunE: func(cmd *cobra.Command, args []string) error {
			return runTimelineCommand(cmd.Context(), flags, local, args, streams, invokedAs)
		},
	}

	command.Flags().StringVar(&local.since, "since", local.since,
		"Only changes at or after this point: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z).")
	command.Flags().StringVar(&local.until, "until", local.until,
		"Only changes at or before this point, in the same forms as --since.")
	command.Flags().IntVar(&local.limit, "limit", local.limit,
		"Show at most this many changes, newest first. Zero means no limit.")
	command.Flags().BoolVar(&local.reverse, "reverse", local.reverse,
		"Show the same changes oldest first. It reorders the rows; it does not select different ones.")
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
	ctx context.Context, flags *GlobalFlags, local *timelineFlags,
	args []string, streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	if formatErr := requireTableFormat(flags.Output); formatErr != nil {
		return formatErr
	}
	arg, err := ParseResourceArg(args)
	if err != nil {
		return err
	}
	if local.uid != "" && local.allIncarnations {
		return UsageErrorf("--uid and --all-incarnations contradict each other: " +
			"one pins the timeline to a single incarnation and the other shows every one of them")
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
	ctx context.Context, flags *GlobalFlags, streams genericiooptions.IOStreams,
	invokedAs string, arg ResourceArg,
) (*Backend, query.ObjectRef, error) {
	resolver, err := NewBackendResolver(flags, streams, invokedAs)
	if err != nil {
		return nil, query.ObjectRef{}, err
	}
	backend, err := resolver.Resolve(ctx)
	if err != nil {
		return nil, query.ObjectRef{}, err
	}

	ref, err := objectRefFor(flags, streams, backend, arg)
	if err != nil {
		return nil, query.ObjectRef{}, errors.Join(err, backend.Close())
	}
	return backend, ref, nil
}

// objectRefFor resolves the address against the opened backend's cluster
// identity.
func objectRefFor(
	flags *GlobalFlags, streams genericiooptions.IOStreams, backend *Backend, arg ResourceArg,
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
func parseWindow(since, until string, now time.Time) (from, to time.Time, err error) {
	if since != "" {
		if from, err = ParseInstant(since, now); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if until != "" {
		if to, err = ParseInstant(until, now); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return time.Time{}, time.Time{}, UsageErrorf(
			"the window ends before it starts: --since resolves to %s and --until to %s",
			render.FormatInstant(from), render.FormatInstant(to))
	}
	return from, to, nil
}

// requireTableFormat refuses the formats this command does not yet render.
//
// Refusing by name rather than rendering a table regardless is the same choice
// `config view` makes: a user who asked for JSON and received a table has been
// answered in a format their script cannot read, and finding out at the `jq` is
// worse than finding out here.
func requireTableFormat(format OutputFormat) error {
	switch format {
	case OutputTable, OutputWide:
		return nil
	}
	return UsageErrorf("timeline renders %s or %s, not %s: the structured formats carry the versioned "+
		"%s envelope and are not wired to this command yet", OutputTable, OutputWide, format, ConfigAPIVersion)
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
	flags *GlobalFlags, local *timelineFlags, streams genericiooptions.IOStreams,
) render.Options {
	return render.Options{
		Width: TerminalWidth(streams.Out),
		Color: ShouldColorize(flags.Color, streams.Out),
		Wide:  flags.Output == OutputWide,
		Full:  local.full,
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
	flags *GlobalFlags, streams genericiooptions.IOStreams, arg ResourceArg,
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
		return ResolvedResource{}, RuntimeErrorf(
			"the cluster could not be reached to resolve %q (%v), and %w",
			arg.Resource, err, offlineErr)
	}
	if writeErr := writeLine(streams.ErrOut, fmt.Sprintf(
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
func clusterResolution(flags *GlobalFlags, arg ResourceArg) (ResolvedResource, error) {
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
	flags *GlobalFlags, streams genericiooptions.IOStreams, resolved ResolvedResource,
) (string, error) {
	namespace, err := flags.Namespace()
	if err != nil {
		return "", RuntimeErrorf("%w", err)
	}
	if resolved.Namespaced {
		return namespace, nil
	}
	if named := explicitNamespace(flags); named != "" {
		if writeErr := writeLine(streams.ErrOut, fmt.Sprintf(
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
func explicitNamespace(flags *GlobalFlags) string {
	if flags.ConfigFlags.Namespace == nil {
		return ""
	}
	return *flags.ConfigFlags.Namespace
}

// TimelineRequest is one `timeline` invocation, resolved.
//
// It is exported, and RunTimeline takes an already-opened Backend, so that the
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

	// Limit caps the changes rendered; zero means no cap.
	Limit int

	// Reverse displays the same changes oldest first. It is a rendering choice
	// and deliberately not the query's own Reverse: the query always asks for
	// the newest first, because that is the shape both backends answer cheaply.
	Reverse bool

	// WithEvents interleaves the Kubernetes Events recorded about the object.
	WithEvents bool
}

// filtered reports whether a predicate makes the rendered rows a non-consecutive
// slice of history.
//
// It gates the prior-value replay. Replaying only the surviving patches over a
// real base state would produce a document the object was never in, and the old
// values read out of it would be confident and wrong — which in an audit
// timeline is worse than their being absent.
func (r TimelineRequest) filtered() bool {
	return len(r.Actors) > 0 || len(r.ExcludeActors) > 0 || len(r.FieldPaths) > 0
}

// RunTimeline answers one timeline request against an opened backend and renders
// the result.
//
// The gathering is shared with `diff` (see gather.go) and the layout is not: the
// two commands ask the backend the identical question, and the whole of their
// difference is how the answer is laid out.
func RunTimeline(
	ctx context.Context, backend *Backend, request TimelineRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	gathered, err := gatherChanges(ctx, backend, request)
	if err != nil {
		return err
	}

	document := render.TimelineDocument{
		Kind:         describeKind(request.Ref),
		Object:       describeObject(request.Ref),
		Cluster:      request.Ref.ClusterID,
		UID:          gathered.UID,
		Incarnations: gathered.Incarnations,
		Coverage:     gathered.Coverage,
		Rows:         gathered.Rows,
		Notices:      gathered.Notices,
	}
	if writeErr := render.WriteTimeline(streams.Out, streams.ErrOut, document, opts); writeErr != nil {
		return RuntimeErrorf("%w", writeErr)
	}
	return gathered.Empty
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
	request TimelineRequest, capabilities query.Capabilities,
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
		from, to = now.Add(-DefaultWindow), now
		return from, to, render.Notice{Text: fmt.Sprintf(
			"the %s backend cannot answer an unbounded question, so the window defaults to %s; "+
				"pass --since to widen it", capabilities.Backend, DescribeWindow(from, to))}
	case from.IsZero():
		from = to.Add(-DefaultWindow)
	default:
		to = now
	}
	return from, to, render.Notice{Text: fmt.Sprintf(
		"the %s backend needs both ends of a window, so this one was completed to %s; "+
			"pass --since and --until to set it yourself", capabilities.Backend, DescribeWindow(from, to))}
}

// timelineQuery builds the read-plane query.
//
// Reverse is always set, whatever --reverse asked for. The flag reorders the
// rendered rows; the query always fetches the newest first, which is the shape
// both backends answer cheaply — the object archive stops walking partitions
// once a reverse-limited query's limit is filled, and an oldest-first query would
// forfeit that.
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
		IncludeEvents:   r.WithEvents,
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
func timelineQueryError(request TimelineRequest, err error) error {
	if errors.Is(err, query.ErrTimeBoundRequired) {
		return RuntimeErrorf("%w; pass --since (and optionally --until) to bound it", err)
	}
	if errors.Is(err, query.ErrNoCoverage) {
		// The sentinel already carries exit code 3 through ExitCodeFor, so it is
		// wrapped for context rather than reclassified.
		return fmt.Errorf("reading the timeline of %s: %w", describeObject(request.Ref), err)
	}
	return RuntimeErrorf("reading the timeline of %s: %w", describeObject(request.Ref), err)
}

// decodeRows turns changes into rows, decoding each patch once.
//
// A patch that will not decode is carried as an error on the row rather than
// dropped: the change still happened, and an audit timeline missing an entry is
// worse than one carrying a cell that says the patch was unreadable.
func decodeRows(changes []query.Change) []render.TimelineRow {
	rows := make([]render.TimelineRow, 0, len(changes))
	for _, change := range changes {
		row := render.TimelineRow{Change: change}
		ops, err := render.PatchOps(change.Diff)
		if err != nil {
			row.PatchErr = err.Error()
		}
		row.Ops = ops
		rows = append(rows, row)
	}
	return rows
}

// priorValueNotices recovers the value each operation replaced, or explains why
// it did not.
//
// rows arrive newest first and the replay must run oldest first, so it walks a
// reversed clone. The clone copies the row structs, but each row's Ops field is a
// slice header over the same backing array, so the Op.Old values the replay fills
// in are visible through the original rows — which is what the renderer reads.
// Reversing in place instead would leave the caller holding the display order it
// did not ask for.
func priorValueNotices(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest, rows []render.TimelineRow,
) []render.Notice {
	if len(rows) == 0 {
		return nil
	}
	if request.filtered() {
		return []render.Notice{{Text: "prior values are not shown because a filter is in force: " +
			"the changes below are not consecutive, and replaying only these patches would report " +
			"values the object never held"}}
	}

	ascending := slices.Clone(rows)
	slices.Reverse(ascending)
	return priorValues(ctx, engine, request.Ref, ascending)
}

// deletionsNotice reports a backend that cannot record deletions, when nothing
// in the result rules one out.
//
// The predicate is written honestly rather than shortened to the capability
// alone: a backend declaring no deletions can never produce a Deleted row today,
// but a renderer whose notice does not actually look at the rows would start
// lying on the day one does.
func deletionsNotice(capabilities query.Capabilities, rows []render.TimelineRow) render.Notice {
	if capabilities.Deletions {
		return render.Notice{}
	}
	if slices.ContainsFunc(rows, func(row render.TimelineRow) bool {
		return row.Change.EventType == query.EventDeleted
	}) {
		return render.Notice{}
	}
	return render.Notice{
		Text: fmt.Sprintf("the %s backend does not record deletions, so this timeline ending is not "+
			"evidence that the object still exists; it may have been deleted while unobserved. "+
			"The `%s` command shows what was being watched", capabilities.Backend, scopesCommand),
		Warning: true,
	}
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
