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

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `--owned`, and the traversal Invariant 9 applies to.
//
// The most-wanted Event query is "show me this Deployment's Pods' Events", and it
// is the one no rule can be written for: the tree is Deployment → ReplicaSet → Pod
// and a Pod's name carries a generated suffix that does not exist until the Pod does
// and changes on every rollout (D52). At read time the names exist, because
// metadata.ownerReferences is in the stored data of every captured object — so the
// tree is a query over rows the archive already holds, and never a question put to
// the cluster.
//
// # What the walk widens, and what it deliberately does not
//
// It widens the Event correlation and nothing else. The rows about the object itself
// stay the object's, because a change carries no identity of its own: a descendant's
// modification would arrive as a patch with nothing on it saying which object it
// patched, and a merged multi-object state stream could not be attributed row by
// row. An Event names its subject in its own data, so an Event row is
// self-describing and the SUBJECT column can be honest about every line
// (query.TimelineQuery.Subjects).
//
// # Why every way the walk can be short has its own sentence
//
// A partial tree presented as a whole one is the traversal form of the empty result
// Invariant 9 forbids. A reader who asked for a Deployment's Pods' Events and got
// none will conclude there were none; the true answer may be that the ReplicaSet
// between them was never captured, or that it was captured before the window opened.
// Those two have different remedies — add a kind to a rule, or widen the window — so
// they are counted apart and named apart, and neither is inferred from the other's
// evidence (D41). Every one of them names the route out (D34).

// ownedFlag is the flag, as a reader typed it.
const ownedFlag = "--owned"

// ownedTree is what the walk contributed to one invocation.
type ownedTree struct {
	// subjects are the descendants whose Events the timeline query will correlate,
	// in the order the walk reached them.
	subjects []query.ObjectRef

	// summary is the header's `Owned` line, empty when there is nothing to say
	// there. It goes on stdout with the rest of the document because it is a fact
	// about the answer rather than a qualification of it.
	summary string

	// notices are every way the walk could not see the whole tree, in the order
	// they are to be written to standard error.
	notices []render.Notice
}

// resolveOwnedTree walks the ownership edges of the archive and reports what it
// could not see.
//
// It is called by both rendering paths, in the same position relative to the
// incarnation choice and the timeline query, for the reason every other shared step
// is shared: a second copy of this sequence is a second place for one of its
// degradation notices to be dropped.
//
// The window is the caller's, already completed by timelineBounds, because the tree
// is read over exactly the window the Events will be read over. A tree assembled
// from a wider window would correlate the Events of Pods whose ownership is not
// evidenced within the window the header states.
func resolveOwnedTree(
	ctx context.Context, backend *resolve.Backend, request TimelineRequest,
	selection incarnationChoice, from, to time.Time, zone render.Zone,
) (ownedTree, error) {
	if !request.Owned {
		return ownedTree{}, nil
	}

	resolver, ok := backend.Engine.(query.OwnershipResolver)
	if !ok {
		// Refused rather than degraded, and it is the one ownership failure that is.
		// Every other one is a property of an archive that a widened window or an
		// edited rule can change; this is a property of the *backend*, permanent for
		// the life of the sink, and a timeline that quietly answered the un-widened
		// question would leave a reader believing they had seen their Pods' Events
		// every time they ran it.
		return ownedTree{}, exit.RuntimeErrorf(
			"the %s backend cannot resolve ownership, so %s has no tree to walk: it reads "+
				"metadata.ownerReferences out of recorded state, which this backend does not "+
				"expose. `%s %s` answers the same question about %s alone",
			backend.Engine.Capabilities().Backend, ownedFlag, timelineCommand,
			withEventsFlag, describeObject(request.Ref))
	}

	objects, err := resolver.Ownership(ctx, ownershipQuery(request, from, to))
	if err != nil {
		// Degraded, for the reason explainNoEvents degrades on a failed scope read: the
		// timeline itself is answerable and ending the flagship command over the half
		// that widens it would trade the whole answer for part of it (Invariant 5).
		// What it must not do is degrade quietly, so the failure is the notice.
		return ownedTree{notices: []render.Notice{{Text: fmt.Sprintf(
			"%s could not read the ownership of %s: %v. The rows below are %s's own Events; "+
				"nothing here is about the objects it owns",
			ownedFlag, describeOwnedScope(request), err, describeObject(request.Ref))}}}, nil
	}

	roots := ownedRoots(request, selection, objects)
	if len(roots) == 0 {
		return ownedTree{notices: []render.Notice{noRootStateNotice(request, from, to, zone)}}, nil
	}

	tree := query.Descendants(roots, objects)
	return ownedTree{
		subjects: tree.Refs(),
		summary:  describeTree(tree),
		notices:  ownedNotices(request, tree, from, to, zone),
	}, nil
}

// ownershipQuery asks about the scope the tree can live in.
//
// A namespaced object's dependents must be in its own namespace, so the walk from
// one is complete within a single namespace — which is what makes the common case a
// bounded read. A cluster-scoped object's dependents may be anywhere, so its query
// carries no namespace and pays for the whole cluster. Narrowing it to the object's
// own (absent) namespace would be the cheaper read and the wrong answer.
func ownershipQuery(request TimelineRequest, from, to time.Time) query.OwnershipQuery {
	return query.OwnershipQuery{
		ClusterID: request.Ref.ClusterID,
		Namespace: request.Ref.Namespace,
		From:      from,
		To:        to,
	}
}

// ownedRoots decides which incarnations of the named object the walk starts from.
//
// A pinned incarnation is the root, because an ownerReference names a UID and the
// caller has said which object they mean. With none pinned — an --all-incarnations
// timeline, or an --events-only one, which resolves no incarnation by construction
// — every UID the name has worn is a root. That is the forgiving key those paths
// already use for the commentary itself, and it is the right one here for the same
// reason: a question spanning a delete-and-recreate spans the trees below both.
func ownedRoots(
	request TimelineRequest, selection incarnationChoice, objects []query.OwnedObject,
) []string {
	if selection.pinned != "" {
		return []string{selection.pinned}
	}
	return query.UIDsOf(objects, request.Ref)
}

// noRootStateNotice explains a walk that could not start.
//
// The object itself has no recorded state in the window, so there is no row to read
// its UID from and nothing any dependent could be matched against. It is a real and
// unremarkable state — the quickstart's own, for a Pod in a cluster whose rules watch
// Events and Deployments — and it is exactly the case the acceptance criteria call a
// descendant "whose state was never captured", one level up.
//
// It says nothing about the Events, deliberately. Those are an independent query
// (D40) and this invocation is still about to answer it; a sentence claiming both
// would be the second assertion made from the first one's evidence (D41).
func noRootStateNotice(
	request TimelineRequest, from, to time.Time, zone render.Zone,
) render.Notice {
	return render.Notice{Text: fmt.Sprintf(
		"%s found no recorded state of %s in %s, so there is nothing to walk from: ownership "+
			"is read from stored rows, and an object with no full state in the window has no "+
			"ownerReferences to match a descendant against. The Events below are the ones "+
			"naming %s itself. Widen the window with --since, or use `%s` to see which kinds "+
			"were being recorded",
		ownedFlag, describeObject(request.Ref), options.DescribeWindow(from, to, zone),
		describeObject(request.Ref), scopesCommand)}
}

// ownedNotices turns everything the walk could not see into sentences.
//
// One for a walk that reached nothing at all, and four for the ways a walk that
// reached something can still be short. They are separate because their remedies
// are: a tree that ran out of depth or breadth was cut by this tool; a missing owner
// is a kind nobody captured, which a rule fixes; an object with no full state in the
// window is a kind that *was* captured, which a wider window fixes. One merged
// sentence would leave a reader holding several possible fixes and no way to choose
// between them.
//
// A complete walk that reached descendants says nothing at all. Every notice this
// CLI writes renders in one tier — the Warning tier, deliberately (D30) — so a line
// reporting a successful traversal would be amber prose about nothing, and a reader
// who meets one of those stops reading the rest. What the reader sees instead is the
// SUBJECT column and the header's `Owned` line, which are the document saying it.
func ownedNotices(
	request TimelineRequest, tree query.OwnershipTree, from, to time.Time, zone render.Zone,
) []render.Notice {
	var notices []render.Notice
	if len(tree.Nodes) == 0 && tree.Complete() {
		return []render.Notice{{Text: fmt.Sprintf(
			"%s found no objects owned by %s recorded in %s. Ownership is read from stored "+
				"rows rather than from the cluster, so a descendant whose state was never "+
				"captured cannot be discovered — `%s` shows which kinds were being recorded, "+
				"and --since widens the window",
			ownedFlag, describeObject(request.Ref), options.DescribeWindow(from, to, zone),
			scopesCommand)}}
	}

	if tree.MissingOwnerObjects > 0 {
		notices = append(notices, render.Notice{Text: fmt.Sprintf(
			"the ownership walk could not see the whole tree: %s in %s name an owner the "+
				"archive does not hold, of %s, so anything beneath them could not be attributed "+
				"to %s. The walk read %s: add the kind to a rule so its state is recorded, or "+
				"pass --since if it was captured before that",
			countObjects(tree.MissingOwnerObjects), describeOwnedScope(request),
			describeKinds(tree.MissingOwnerKinds), describeObject(request.Ref),
			options.DescribeWindow(from, to, zone))})
	}
	if tree.UnreadableObjects > 0 {
		notices = append(notices, render.Notice{Text: fmt.Sprintf(
			"%s in %s have rows in %s but no full state in it, of %s, so their own "+
				"ownerReferences could not be read and the walk could not place them. Pass "+
				"--since so a full state of them falls inside the window",
			countObjects(tree.UnreadableObjects), describeOwnedScope(request),
			options.DescribeWindow(from, to, zone), describeKinds(tree.UnreadableKinds))})
	}
	if tree.DepthLimited {
		notices = append(notices, render.Notice{Text: fmt.Sprintf(
			"the ownership walk stops %d levels below %s and this tree is deeper; the Events "+
				"of anything below that level are not here",
			query.MaxOwnershipDepth, describeObject(request.Ref))})
	}
	if tree.BreadthLimited {
		notices = append(notices, render.Notice{Text: fmt.Sprintf(
			"the ownership walk stops at %d descendants and this tree has more; the Events "+
				"below are those of the %d it reached first. Narrow the window with --since "+
				"and --until to bring the tree under the bound",
			query.MaxOwnedObjects, query.MaxOwnedObjects)})
	}
	return notices
}

// describeOwnedScope names the scope the walk measured its gaps over.
//
// It is the ownership query's scope and not the object's, because that is what was
// counted: a claim about "12 objects" has to say which twelve, and a reader whose
// namespace holds four thousand needs to know the number is not about their tree.
func describeOwnedScope(request TimelineRequest) string {
	if namespace := request.Ref.Namespace; namespace != "" {
		return "namespace " + namespace
	}
	return "this cluster"
}

// describeTree renders the header's `Owned` line.
//
// The count and the kinds, because those are the two questions a reader has about a
// tree they cannot see: how much of the page is not about the object they named, and
// whether the walk got as far as Pods. It is on stdout with the rest of the header,
// since it describes the answer rather than qualifying it.
func describeTree(tree query.OwnershipTree) string {
	if len(tree.Nodes) == 0 {
		return ""
	}
	kinds := make(map[string]struct{}, len(tree.Nodes))
	for _, node := range tree.Nodes {
		kinds[node.Ref.Kind] = struct{}{}
	}
	names := make([]string, 0, len(kinds))
	for kind := range kinds {
		names = append(names, kind)
	}
	slices.Sort(names)
	return fmt.Sprintf("%s (%s)", countObjects(len(tree.Nodes)), strings.Join(names, ", "))
}

// countObjects renders a count of objects with the right number on the noun.
func countObjects(n int) string {
	if n == 1 {
		return "1 object"
	}
	return fmt.Sprintf("%d objects", n)
}

// describeKinds renders a kind list with the right number on the noun.
//
// The kinds are the actionable half of a gap notice — "of kind ReplicaSet" is what
// sends a reader to the rule that is missing one — so they are named rather than
// counted, however many there are.
func describeKinds(kinds []string) string {
	if len(kinds) == 1 {
		return "kind " + kinds[0]
	}
	return "kinds " + strings.Join(kinds, ", ")
}
