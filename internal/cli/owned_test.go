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

// `timeline --owned`: the tree it walks, the rows it attributes, and the four ways
// it says it could not see the whole thing.
//
// # What is under test here and what is not
//
// The subject predicate itself is a backend's business and is proven against both
// of them, over one shared past (D42) — a fake that correlated an Event without
// being asked to would model the contract's *conclusion* rather than its
// mechanism, which is exactly the shape of defect Task 18.6 found. What is under
// test here is the command: that the walk runs from the resolved incarnation, that
// its tree reaches the query, that each row names the object it belongs to, and
// that every way the archive can fail to show a whole tree becomes a sentence a
// reader can act on rather than an emptiness they will misread (Invariant 9).

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The tree every fixture below walks: the fixture Deployment, one ReplicaSet under
// it, and two Pods under that.
const (
	// ownedNamespace is the namespace every object of the tree lives in. A
	// namespaced object's owners must share its namespace, so one is all a tree can
	// span.
	ownedNamespace = "payments"

	ownedRSName   = "checkout-7d4f"
	ownedPodAName = "checkout-7d4f-ldw5j"
	ownedPodBName = "checkout-7d4f-x92kk"

	ownedRSUID   = "3f1c2f2e-0000-4000-8000-00000000rs01"
	ownedPodAUID = "3f1c2f2e-0000-4000-8000-000000pod01"
	ownedPodBUID = "3f1c2f2e-0000-4000-8000-000000pod02"
)

// ownedRequest is `timeline deploy/checkout -n payments --owned`, over the bounded
// window every Event fixture in this package uses.
//
// It sets Owned and deliberately neither of the Event flags: that a bare --owned
// asks the Event question is the implication under test, and a request that spelled
// both would not be exercising it.
func ownedRequest() cli.TimelineRequest {
	request := defaultRequest()
	request.From = at("2026-08-01T00:00:00Z")
	request.To = at("2026-08-28T15:00:00Z")
	request.Owned = true
	return request
}

// ownedRefFor is one identity of the tree.
func ownedRefFor(group, kind, name string) query.ObjectRef {
	return query.ObjectRef{
		ClusterID: fixtureCluster, APIGroup: group, Kind: kind,
		Namespace: ownedNamespace, Name: name,
	}
}

// ownedObject is one entry of an ownership answer, with its state recorded.
func ownedObject(ref query.ObjectRef, uid string, owners ...query.OwnerReference) query.OwnedObject {
	return query.OwnedObject{Ref: ref, UID: uid, StateRecorded: true, Owners: owners}
}

// ownerRef names an owner the way an ownerReference does.
func ownerRef(kind, name, uid string) query.OwnerReference {
	return query.OwnerReference{APIVersion: "apps/v1", Kind: kind, Name: name, UID: uid}
}

// wholeTree is Deployment → ReplicaSet → Pod, every level captured.
func wholeTree() []query.OwnedObject {
	return []query.OwnedObject{
		ownedObject(fixtureRef(), fixtureUID),
		ownedObject(ownedRefFor("apps", "ReplicaSet", ownedRSName), ownedRSUID,
			ownerRef("Deployment", "checkout", fixtureUID)),
		ownedObject(ownedRefFor("", "Pod", ownedPodAName), ownedPodAUID,
			ownerRef("ReplicaSet", ownedRSName, ownedRSUID)),
		ownedObject(ownedRefFor("", "Pod", ownedPodBName), ownedPodBUID,
			ownerRef("ReplicaSet", ownedRSName, ownedRSUID)),
	}
}

// treeWithNoMiddle is the same tree with the ReplicaSet never captured, which is
// the honest-degradation case: the Pods reference an owner no row describes, so
// nothing can attribute them to the Deployment.
func treeWithNoMiddle() []query.OwnedObject {
	return []query.OwnedObject{
		ownedObject(fixtureRef(), fixtureUID),
		ownedObject(ownedRefFor("", "Pod", ownedPodAName), ownedPodAUID,
			ownerRef("ReplicaSet", ownedRSName, ownedRSUID)),
		ownedObject(ownedRefFor("", "Pod", ownedPodBName), ownedPodBUID,
			ownerRef("ReplicaSet", ownedRSName, ownedRSUID)),
	}
}

// cyclicTree is a corrupted archive: the Deployment names one of its own
// descendants as its owner.
func cyclicTree() []query.OwnedObject {
	return []query.OwnedObject{
		ownedObject(fixtureRef(), fixtureUID, ownerRef("Pod", ownedPodAName, ownedPodAUID)),
		ownedObject(ownedRefFor("apps", "ReplicaSet", ownedRSName), ownedRSUID,
			ownerRef("Deployment", "checkout", fixtureUID)),
		ownedObject(ownedRefFor("", "Pod", ownedPodAName), ownedPodAUID,
			ownerRef("ReplicaSet", ownedRSName, ownedRSUID)),
	}
}

// ownedRootEvent is the Event about the Deployment itself, carrying involvedObject
// because the SUBJECT column is read out of exactly that.
func ownedRootEvent() []query.Change {
	return []query.Change{{
		TS: at("2026-08-28T14:03:20.310Z"), EventType: query.EventKubernetes,
		UID: "e1", Actors: []string{"kube-controller-manager"}, APIVersion: "v1",
		Data: `{"type":"Normal","reason":"ScalingReplicaSet",` +
			`"message":"Scaled up replica set checkout-7d4f to 5",` +
			`"involvedObject":{"kind":"Deployment","namespace":"payments","name":"checkout"},` +
			`"source":{"component":"deployment-controller"}}`,
	}}
}

// ownedTreeEvents are the Events at the levels below: the ReplicaSet's quota
// refusal and the Pod's scheduling failure, which is the pair somebody runs this
// flag to find.
func ownedTreeEvents() []query.Change {
	return []query.Change{
		{
			TS: at("2026-08-28T14:06:44.020Z"), EventType: query.EventKubernetes,
			UID: "e2", APIVersion: "events.k8s.io/v1",
			Data: `{"type":"Warning","reason":"FailedCreate",` +
				`"note":"pods \"checkout-7d4f-\" is forbidden: exceeded quota",` +
				`"regarding":{"kind":"ReplicaSet","namespace":"payments","name":"checkout-7d4f"},` +
				`"reportingController":"replicaset-controller"}`,
		},
		{
			TS: at("2026-08-28T14:07:03.771Z"), EventType: query.EventKubernetes,
			UID: "e3", APIVersion: "v1",
			Data: `{"type":"Warning","reason":"FailedScheduling",` +
				`"message":"0/9 nodes are available: insufficient memory",` +
				`"involvedObject":{"kind":"Pod","namespace":"payments",` +
				`"name":"checkout-7d4f-ldw5j"},"source":{"component":"default-scheduler"}}`,
		},
	}
}

// ownedEngine is the ordinary case: the object's own history, a scope that covered
// it, a rule streaming Events, and the tree the walk is to find.
func ownedEngine(tree []query.OwnedObject) *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      shortHistory(),
		incarnations: checkoutIncarnations(),
		events:       ownedRootEvent(),
		treeEvents:   ownedTreeEvents(),
		ownership:    tree,
		intervals: append(deploymentScope(),
			eventsWatchedBy("", "ClusterStreamRule/all-events")),
	}
}

// TestOwnedCorrelatesTheEventsOfTheWholeTree is the flag doing what it exists for:
// a two-level tree, Events at the leaves, one ts order, and every row naming the
// object it belongs to.
func TestOwnedCorrelatesTheEventsOfTheWholeTree(t *testing.T) {
	for mode, color := range map[string]bool{"": false, "-color": true} {
		t.Run("whole tree"+mode, func(t *testing.T) {
			engine := ownedEngine(wholeTree())

			stdout, stderr, err := runTimeline(t, engine, ownedRequest(),
				render.Options{Color: color, Owned: true})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			assertGolden(t, "owned-whole-tree"+mode, stdout, stderr)
		})
	}
}

// TestOwnedImpliesWithEvents pins the composition: --owned on its own asks the
// Event question rather than producing a page identical to its absence (D31).
//
// The assertion is on the query rather than on the page, because a page cannot tell
// "the Events were asked for and the tree is quiet" from "the Events were never
// asked for at all".
func TestOwnedImpliesWithEvents(t *testing.T) {
	engine := ownedEngine(wholeTree())

	if _, _, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if len(engine.queries) != 1 {
		t.Fatalf("the command issued %d timeline queries, want 1: %+v", len(engine.queries), engine.queries)
	}
	if !engine.queries[0].IncludeEvents {
		t.Errorf("--owned did not ask for Events: %+v. Subjects is read only when IncludeEvents is "+
			"set, so a query carrying the tree without it widens nothing", engine.queries[0])
	}
	if got, want := subjectNamesOf(engine.queries[0]), []string{
		ownedRSName, ownedPodAName, ownedPodBName,
	}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("the query carried subjects %v, want %v — breadth first from the root, root excluded",
			got, want)
	}
}

// TestOwnedWalksFromTheResolvedIncarnation is Invariant 7 reaching the walk.
//
// A name that has been reused belongs to several objects, and the tree below one of
// them is not the tree below another. The command resolves the incarnation before
// the walk, so a Deployment recreated under the same name must not have the
// previous object's ReplicaSets attributed to it.
func TestOwnedWalksFromTheResolvedIncarnation(t *testing.T) {
	tree := wholeTree()
	// A ReplicaSet of the *previous* incarnation of this name.
	tree = append(tree, ownedObject(ownedRefFor("apps", "ReplicaSet", "checkout-1111"), "uid-old-rs",
		ownerRef("Deployment", "checkout", priorUID)))
	tree = append(tree, ownedObject(fixtureRef(), priorUID))

	engine := ownedEngine(tree)
	if _, _, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	for _, name := range subjectNamesOf(engine.queries[0]) {
		if name == "checkout-1111" {
			t.Fatalf("the walk reached a ReplicaSet of the previous incarnation: %v. Splicing two "+
				"incarnations' trees is the same falsehood as splicing their timelines (Invariant 7)",
				subjectNamesOf(engine.queries[0]))
		}
	}
}

// TestOwnedAsksAboutTheObjectsOwnNamespace pins the scope of the walk, which is
// invisible in the page: a walk that asked about the wrong namespace returns an
// empty tree, and so does an object that owns nothing.
func TestOwnedAsksAboutTheObjectsOwnNamespace(t *testing.T) {
	engine := ownedEngine(wholeTree())
	request := ownedRequest()

	if _, _, err := runTimeline(t, engine, request, render.Options{Owned: true}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if len(engine.ownershipQueries) != 1 {
		t.Fatalf("the command made %d ownership reads, want 1", len(engine.ownershipQueries))
	}
	asked := engine.ownershipQueries[0]
	switch {
	case asked.ClusterID != fixtureCluster:
		t.Errorf("the walk asked about cluster %q, want %q", asked.ClusterID, fixtureCluster)
	case asked.Namespace != ownedNamespace:
		t.Errorf("the walk asked about namespace %q, want the object's own", asked.Namespace)
	case !asked.From.Equal(request.From) || !asked.To.Equal(request.To):
		t.Errorf("the walk asked over %s → %s, want the timeline's own window %s → %s. A tree read "+
			"over a wider window would correlate Events for objects whose ownership is not evidenced "+
			"in the window the header states", asked.From, asked.To, request.From, request.To)
	}
}

// TestOwnedNamesAnUncapturedMiddleLevel is Invariant 9 applied to a traversal.
//
// The ReplicaSet was never captured, so the Pods below it cannot be discovered. The
// page must not present that as "this Deployment owns nothing", and the sentence has
// to name the kind — which survives only on the reference the Pods themselves carry.
func TestOwnedNamesAnUncapturedMiddleLevel(t *testing.T) {
	engine := ownedEngine(treeWithNoMiddle())

	stdout, stderr, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "owned-uncaptured-middle", stdout, stderr)

	if !strings.Contains(stderr, "kind ReplicaSet") {
		t.Errorf("the notice does not name the kind that could not be seen:\n%s", stderr)
	}
	if len(engine.queries[0].Subjects) != 0 {
		t.Errorf("the walk reached %v through an owner the archive does not hold",
			subjectNamesOf(engine.queries[0]))
	}
}

// TestOwnedTerminatesOnACycle is the reason the walk is bounded: a corrupted
// archive must produce an answer rather than a hang.
func TestOwnedTerminatesOnACycle(t *testing.T) {
	engine := ownedEngine(cyclicTree())

	done := make(chan struct{})
	var subjects []string
	go func() {
		defer close(done)
		if _, _, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true}); err != nil {
			t.Errorf("RunTimeline: %v", err)
			return
		}
		subjects = subjectNamesOf(engine.queries[0])
	}()
	<-done

	if got, want := subjects, []string{ownedRSName, ownedPodAName}; strings.Join(got, ",") !=
		strings.Join(want, ",") {
		t.Fatalf("the walk reached %v, want %v — each object once, and the edge back to the root "+
			"followed nowhere", got, want)
	}
}

// TestOwnedOverAnObjectWithNoDescendantsSaysSo is the fourth acceptance case, and
// the reading of "silence" it takes.
//
// Not an error: the command succeeds and renders the object's own page. Not
// wordless either — a flag that produced no visible effect must say why (D31), and
// "this object owns nothing recorded here" and "the flag was ignored" are the two
// readings an unexplained page leaves open.
func TestOwnedOverAnObjectWithNoDescendantsSaysSo(t *testing.T) {
	engine := ownedEngine([]query.OwnedObject{ownedObject(fixtureRef(), fixtureUID)})

	stdout, stderr, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true})
	if err != nil {
		t.Fatalf("RunTimeline returned %v; an object that owns nothing is a result and not a "+
			"failure", err)
	}
	assertGolden(t, "owned-no-descendants", stdout, stderr)

	if !strings.Contains(stderr, "found no objects owned by") {
		t.Errorf("a walk that reached nothing said nothing about it:\n%s", stderr)
	}
}

// TestOwnedWithEventsOnlyIsTheFlagshipCombination pins the pair the release exists
// for: the tree's commentary, and none of the object's status churn.
func TestOwnedWithEventsOnlyIsTheFlagshipCombination(t *testing.T) {
	engine := ownedEngine(wholeTree())
	request := ownedRequest()
	request.EventsOnly = true

	stdout, stderr, err := runTimeline(t, engine, request,
		render.Options{Owned: true, EventsOnly: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "owned-events-only", stdout, stderr)

	if engine.stateScans != 0 {
		t.Errorf("the backend read the object's own history %d time(s) under --events-only; the "+
			"ownership walk must not have reintroduced the read the flag exists to skip",
			engine.stateScans)
	}
	if !engine.queries[0].EventsOnly || len(engine.queries[0].Subjects) == 0 {
		t.Errorf("the query lost half of the pair: %+v", engine.queries[0])
	}
}

// TestOwnedWithEventsOnlyWalksEveryIncarnationOfTheName covers the roots an
// events-only question supplies.
//
// It resolves no incarnation by construction — that is the read the flag skips — so
// the walk starts from every UID the name has worn, which is the forgiving key the
// commentary itself already uses.
func TestOwnedWithEventsOnlyWalksEveryIncarnationOfTheName(t *testing.T) {
	tree := append(wholeTree(), ownedObject(fixtureRef(), priorUID))
	tree = append(tree, ownedObject(ownedRefFor("apps", "ReplicaSet", "checkout-1111"), "uid-old-rs",
		ownerRef("Deployment", "checkout", priorUID)))

	engine := ownedEngine(tree)
	request := ownedRequest()
	request.EventsOnly = true

	if _, _, err := runTimeline(t, engine, request, render.Options{Owned: true}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	names := subjectNamesOf(engine.queries[0])
	if !slices.Contains(names, "checkout-1111") {
		t.Fatalf("the walk reached %v and not the older incarnation's ReplicaSet. With no "+
			"incarnation resolved, the honest root set is every UID the name has worn", names)
	}
}

// TestOwnedDegradesWhenTheWalkCannotBeRead keeps a failed ownership read from
// costing the reader the timeline.
//
// The Events about the object itself are still answerable, so the command renders
// them and says what it could not add — Invariant 5 with Invariant 4's condition on
// it.
func TestOwnedDegradesWhenTheWalkCannotBeRead(t *testing.T) {
	engine := ownedEngine(wholeTree())
	engine.ownershipErr = errors.New("the archive refused the listing")

	stdout, stderr, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v; a failed walk must not end a command whose main question is "+
			"still answerable", err)
	}
	if !strings.Contains(stderr, "could not read the ownership") ||
		!strings.Contains(stderr, "the archive refused the listing") {
		t.Errorf("the failure was swallowed:\n%s", stderr)
	}
	if !strings.Contains(stdout, "ScalingReplicaSet") {
		t.Errorf("the object's own Events were lost with the tree:\n%s", stdout)
	}
}

// TestOwnedRefusesABackendWithNoOwnershipHalf is the one ownership failure that is
// not degraded, and the reason is that it is permanent.
//
// Every other way a tree can be short is a property of an archive that a widened
// window or an edited rule changes. This is a property of the backend, so a command
// that quietly answered the un-widened question would leave a reader believing they
// had seen their Pods' Events on every invocation, forever. The error names the
// route around it (D34).
func TestOwnedRefusesABackendWithNoOwnershipHalf(t *testing.T) {
	engine := ownedEngine(wholeTree())

	err := runTimelineWith(t, &engineWithoutOwnership{engine}, ownedRequest(),
		render.Options{Owned: true})
	if err == nil {
		t.Fatal("--owned succeeded against a backend that cannot resolve ownership, so the page " +
			"held the object's Events and nothing said the tree was never walked")
	}
	if !strings.Contains(err.Error(), "--with-events") {
		t.Errorf("the refusal names no route around itself: %v", err)
	}
}

// runTimelineWith drives the command against any engine, which runTimeline cannot:
// it takes the fake by its concrete type so that it can assert the fake's own
// counters afterwards, and the point of the engine below is that it is not one.
func runTimelineWith(
	t *testing.T, engine query.QueryEngine, request cli.TimelineRequest, opts render.Options,
) error {
	t.Helper()
	if opts.Width == 0 {
		opts.Width = goldenWidth
	}
	var out, errOut bytes.Buffer
	return cli.RunTimeline(context.Background(),
		&resolve.Backend{Engine: engine, ClusterID: fixtureCluster},
		request, ioStreams(&out, &errOut), opts)
}

// engineWithoutOwnership is a QueryEngine and nothing more.
//
// Embedding the *interface* rather than the fake is what makes it one: a fake
// embedded by value would promote its Ownership method and the type would satisfy
// the optional half it exists to lack.
type engineWithoutOwnership struct{ query.QueryEngine }

// subjectNamesOf lists the subjects a query carried, in order.
func subjectNamesOf(q query.TimelineQuery) []string {
	names := make([]string, 0, len(q.Subjects))
	for _, ref := range q.Subjects {
		names = append(names, ref.Name)
	}
	return names
}

// TestOwnedReachesTheStreamingPath is the second entry point, and the reason the
// walk lives in one function both paths call.
//
// The structured renderings do not gather the answer before writing it, so they are
// a separate sequence with the same obligations — and a field the two had to thread
// separately is one that eventually reaches this one empty, producing a `-o json`
// answer with none of the tree in it and nothing saying so.
func TestOwnedReachesTheStreamingPath(t *testing.T) {
	for _, format := range []render.StructuredFormat{render.StructuredJSON, render.StructuredJSONL} {
		t.Run(string(format), func(t *testing.T) {
			engine := ownedEngine(wholeTree())
			request := ownedRequest()
			request.Structured = format

			stdout, _, err := runTimeline(t, engine, request, render.Options{Owned: true})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if got, want := subjectNamesOf(engine.queries[0]), 3; len(got) != want {
				t.Fatalf("the streaming path carried %d subjects, want %d: %v", len(got), want, got)
			}
			// The envelope carries no subject field of its own: an Event row's data
			// reaches the output as real JSON, so involvedObject names the object the
			// row is about, verbatim and in the spelling docs/QUERIES.md uses in SQL.
			if !strings.Contains(strings.Join(strings.Fields(stdout), ""),
				`"name":"checkout-7d4f-ldw5j"`) {
				t.Errorf("the tree's Events did not reach the envelope, or their subject did not "+
					"travel with them:\n%s", stdout)
			}
		})
	}
}

// TestOwnedReportsTheDepthBound covers the notice the bound exists to make
// unnecessary to guess at.
//
// A bound that silently truncated would turn "this tool stopped looking" into "there
// is nothing below there", which is the traversal form of the unexplained empty
// result. The chain here is one level deeper than the walk will go.
func TestOwnedReportsTheDepthBound(t *testing.T) {
	tree := []query.OwnedObject{ownedObject(fixtureRef(), fixtureUID)}
	owner := ownerRef("Deployment", "checkout", fixtureUID)
	for level := 1; level <= query.MaxOwnershipDepth+1; level++ {
		uid := fmt.Sprintf("uid-level-%d", level)
		name := fmt.Sprintf("level-%d", level)
		tree = append(tree, ownedObject(ownedRefFor("", "Pod", name), uid, owner))
		owner = ownerRef("Pod", name, uid)
	}

	engine := ownedEngine(tree)
	_, stderr, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	if len(engine.queries[0].Subjects) != query.MaxOwnershipDepth {
		t.Errorf("the walk carried %d subjects, want the bound of %d",
			len(engine.queries[0].Subjects), query.MaxOwnershipDepth)
	}
	want := fmt.Sprintf("stops %d levels below", query.MaxOwnershipDepth)
	if !strings.Contains(stderr, want) {
		t.Errorf("a truncated walk did not state its bound; want a notice containing %q:\n%s",
			want, stderr)
	}
}

// TestOwnedWidensTheSentenceAboutAnEmptyEventAnswer keeps the explanation as wide as
// the search behind it.
//
// "no Events for payments/checkout" beneath a walk that also looked at three
// descendants is a claim narrower than its own measurement, and a reader who
// believed it would go and check the Pods by hand.
func TestOwnedWidensTheSentenceAboutAnEmptyEventAnswer(t *testing.T) {
	engine := ownedEngine(wholeTree())
	engine.events = nil
	engine.treeEvents = nil

	_, stderr, err := runTimeline(t, engine, ownedRequest(), render.Options{Owned: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stderr, "or the 3 objects it owns") {
		t.Errorf("the empty-Events notice does not name the tree it searched:\n%s", stderr)
	}
	if !strings.Contains(stderr, ownedFlagName) {
		t.Errorf("the notice names a flag the reader did not pass:\n%s", stderr)
	}
}

// ownedFlagName is the flag as a notice spells it. It is duplicated from the
// package under test on purpose: an assertion that imported the constant would
// agree with a renamed flag rather than catching it.
const ownedFlagName = "--owned"
