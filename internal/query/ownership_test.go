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

package query_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The walk is in the contract rather than in a backend for the reason Replay and
// CoverageOf are: its interesting cases are the quiet ones. A cycle must terminate,
// a bound must be reported only when it actually cut something, and a tree the
// archive cannot show whole must say so — and every one of those is a property of
// the traversal rather than of any storage layout. Two backends implementing it
// apart would be two chances to get the cycle wrong.

const (
	walkCluster   = "prod"
	walkNamespace = "payments"
)

// owned builds one entry of an ownership answer: a captured object of some kind,
// with the UIDs of the owners its recorded state named.
func owned(kind, name, uid string, owners ...query.OwnerReference) query.OwnedObject {
	group := ""
	if kind == "Deployment" || kind == "ReplicaSet" {
		group = "apps"
	}
	return query.OwnedObject{
		Ref: query.ObjectRef{
			ClusterID: walkCluster, APIGroup: group, Kind: kind,
			Namespace: walkNamespace, Name: name,
		},
		UID:           uid,
		StateRecorded: true,
		Owners:        owners,
	}
}

// ownerOf names an owner the way an ownerReference does.
func ownerOf(kind, name, uid string) query.OwnerReference {
	return query.OwnerReference{Kind: kind, Name: name, UID: uid}
}

// TestDescendantsWalksATwoLevelTree is the shape the flag exists for:
// Deployment → ReplicaSet → Pod, where the Pod's name could not have been named in
// advance.
func TestDescendantsWalksATwoLevelTree(t *testing.T) {
	objects := []query.OwnedObject{
		owned("Deployment", "checkout", "uid-deploy"),
		owned("ReplicaSet", "checkout-7d4f", "uid-rs", ownerOf("Deployment", "checkout", "uid-deploy")),
		owned("Pod", "checkout-7d4f-ldw5j", "uid-pod-a",
			ownerOf("ReplicaSet", "checkout-7d4f", "uid-rs")),
		owned("Pod", "checkout-7d4f-x92kk", "uid-pod-b",
			ownerOf("ReplicaSet", "checkout-7d4f", "uid-rs")),
		// A Pod of an unrelated Deployment, to prove the walk follows edges rather
		// than sweeping up the namespace.
		owned("ReplicaSet", "search-4b2c", "uid-other-rs",
			ownerOf("Deployment", "search", "uid-other-deploy")),
		owned("Deployment", "search", "uid-other-deploy"),
	}

	tree := query.Descendants([]string{"uid-deploy"}, objects)

	if !tree.Complete() {
		t.Fatalf("a fully captured tree reported a gap: %s", describeGaps(tree))
	}
	if got, want := namesOf(tree), []string{
		"checkout-7d4f", "checkout-7d4f-ldw5j", "checkout-7d4f-x92kk",
	}; !slices.Equal(got, want) {
		t.Fatalf("the walk reached %v, want %v", got, want)
	}
	if got, want := depthsOf(tree), []int{1, 2, 2}; !slices.Equal(got, want) {
		t.Fatalf("the walk recorded depths %v, want %v — breadth first, root excluded", got, want)
	}
	for _, node := range tree.Nodes {
		if node.OwnerUID == "" {
			t.Errorf("%s was reached through no owner; a node with no edge cannot be rendered as a tree",
				node.Ref.Name)
		}
	}
}

// TestDescendantsNamesAnUncapturedMiddleLevel is Invariant 9 applied to a
// traversal.
//
// The ReplicaSet between the Deployment and its Pods was never captured, so the
// Pods cannot be discovered through it: there is no row to read their owner's UID
// off. The walk must not present the empty result as a complete tree, and the kind
// it names has to be the *missing owner's* — which survives only on the reference
// the Pod itself carries.
func TestDescendantsNamesAnUncapturedMiddleLevel(t *testing.T) {
	objects := []query.OwnedObject{
		owned("Deployment", "checkout", "uid-deploy"),
		owned("Pod", "checkout-7d4f-ldw5j", "uid-pod-a",
			ownerOf("ReplicaSet", "checkout-7d4f", "uid-rs")),
		owned("Pod", "checkout-7d4f-x92kk", "uid-pod-b",
			ownerOf("ReplicaSet", "checkout-7d4f", "uid-rs")),
	}

	tree := query.Descendants([]string{"uid-deploy"}, objects)

	if len(tree.Nodes) != 0 {
		t.Fatalf("the walk reached %v through an owner the archive does not hold", namesOf(tree))
	}
	if tree.Complete() {
		t.Fatal("a tree with an uncaptured middle level reported itself complete, which is the " +
			"partial answer presented as a whole one that Invariant 9 forbids")
	}
	if got, want := tree.MissingOwnerKinds, []string{"ReplicaSet"}; !slices.Equal(got, want) {
		t.Fatalf("the gap named kinds %v, want %v — the kind is on the dangling reference itself",
			got, want)
	}
	if tree.MissingOwnerObjects != 2 {
		t.Fatalf("the gap counted %d objects, want 2", tree.MissingOwnerObjects)
	}
	if tree.UnreadableObjects != 0 {
		t.Fatalf("the gap counted %d objects as unreadable; their state was recorded, and the two "+
			"gaps have different remedies", tree.UnreadableObjects)
	}
}

// TestDescendantsCountsAnObjectWhoseOwnershipCouldNotBeRead separates the second
// gap from the first.
//
// An object with rows in the window but no full state in it has ownerReferences the
// archive holds and this window does not show. The remedy is a wider window rather
// than an edited rule, so it is counted apart (D41) — and it is emphatically not
// reported as owning nothing.
func TestDescendantsCountsAnObjectWhoseOwnershipCouldNotBeRead(t *testing.T) {
	pod := owned("Pod", "checkout-7d4f-ldw5j", "uid-pod-a")
	pod.StateRecorded = false

	tree := query.Descendants([]string{"uid-deploy"}, []query.OwnedObject{
		owned("Deployment", "checkout", "uid-deploy"), pod,
	})

	if tree.Complete() {
		t.Fatal("an object whose ownership could not be read left the tree reported complete")
	}
	if got, want := tree.UnreadableKinds, []string{"Pod"}; !slices.Equal(got, want) {
		t.Fatalf("the unreadable gap named kinds %v, want %v", got, want)
	}
	if tree.UnreadableObjects != 1 {
		t.Fatalf("the unreadable gap counted %d objects, want 1", tree.UnreadableObjects)
	}
	if tree.MissingOwnerObjects != 0 {
		t.Fatalf("the missing-owner gap counted %d objects; this object named no owner at all, and "+
			"reporting it under both headings would give the reader two fixes and no way to choose",
			tree.MissingOwnerObjects)
	}
}

// TestDescendantsTerminatesOnACycle is the reason the walk is bounded at all.
//
// An ownership graph is a DAG in principle and a cycle in a corrupted archive, and
// a read-only tool that hangs on bad data is worse than one that reports what it
// can. Every object here is reachable exactly once, and the test's real assertion
// is that it returns.
func TestDescendantsTerminatesOnACycle(t *testing.T) {
	objects := []query.OwnedObject{
		owned("Deployment", "checkout", "uid-a", ownerOf("Pod", "loop-c", "uid-c")),
		owned("ReplicaSet", "loop-b", "uid-b", ownerOf("Deployment", "checkout", "uid-a")),
		owned("Pod", "loop-c", "uid-c", ownerOf("ReplicaSet", "loop-b", "uid-b")),
	}

	done := make(chan query.OwnershipTree, 1)
	go func() { done <- query.Descendants([]string{"uid-a"}, objects) }()

	tree := <-done
	if got, want := namesOf(tree), []string{"loop-b", "loop-c"}; !slices.Equal(got, want) {
		t.Fatalf("the walk reached %v, want %v — each node once, and the edge back to the root "+
			"followed nowhere", got, want)
	}
	if !tree.Complete() {
		t.Fatalf("a cycle was reported as a gap: %s. Every object was reached, so there is nothing "+
			"the reader has to act on", describeGaps(tree))
	}
}

// TestDescendantsTerminatesOnASelfReference is the degenerate cycle, which a
// visited set keyed on the child rather than on the edge would miss.
func TestDescendantsTerminatesOnASelfReference(t *testing.T) {
	tree := query.Descendants([]string{"uid-a"}, []query.OwnedObject{
		owned("Deployment", "checkout", "uid-a", ownerOf("Deployment", "checkout", "uid-a")),
	})
	if len(tree.Nodes) != 0 {
		t.Fatalf("an object that owns itself was reached as its own descendant: %v", namesOf(tree))
	}
}

// TestDescendantsReportsTheDepthBoundOnlyWhenItCuts pins both halves of the bound.
//
// A chain deeper than MaxOwnershipDepth is truncated and says so; a chain exactly
// that deep is complete, and reporting it as truncated would be a warning about
// nothing — which is how a reader learns to stop reading them.
func TestDescendantsReportsTheDepthBoundOnlyWhenItCuts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		levels       int
		wantNodes    int
		wantLimited  bool
		wantComplete bool
	}{
		{"exactly at the bound", query.MaxOwnershipDepth, query.MaxOwnershipDepth, false, true},
		{"one level past it", query.MaxOwnershipDepth + 1, query.MaxOwnershipDepth, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tree := query.Descendants([]string{"uid-0"}, chainOf(tc.levels))

			if len(tree.Nodes) != tc.wantNodes {
				t.Fatalf("the walk reached %d levels, want %d", len(tree.Nodes), tc.wantNodes)
			}
			if tree.DepthLimited != tc.wantLimited {
				t.Fatalf("DepthLimited is %t, want %t", tree.DepthLimited, tc.wantLimited)
			}
			if tree.Complete() != tc.wantComplete {
				t.Fatalf("Complete is %t, want %t: %s", tree.Complete(), tc.wantComplete,
					describeGaps(tree))
			}
		})
	}
}

// TestDescendantsReportsTheBreadthBound covers the other bound, whose purpose is
// the size of the predicate the tree becomes rather than the size of the answer.
func TestDescendantsReportsTheBreadthBound(t *testing.T) {
	objects := make([]query.OwnedObject, 0, query.MaxOwnedObjects+11)
	objects = append(objects, owned("Deployment", "checkout", "uid-deploy"))
	for i := range query.MaxOwnedObjects + 10 {
		objects = append(objects, owned("Pod", fmt.Sprintf("checkout-%03d", i), fmt.Sprintf("uid-%03d", i),
			ownerOf("Deployment", "checkout", "uid-deploy")))
	}

	tree := query.Descendants([]string{"uid-deploy"}, objects)

	if len(tree.Nodes) != query.MaxOwnedObjects {
		t.Fatalf("the walk reached %d descendants, want the bound of %d",
			len(tree.Nodes), query.MaxOwnedObjects)
	}
	if !tree.BreadthLimited {
		t.Fatal("the breadth bound cut the tree and did not say so")
	}
	if tree.DepthLimited {
		t.Fatal("a walk stopped by breadth also reported the depth bound; one cause must not " +
			"produce two findings")
	}
}

// TestDescendantsOverAnObjectWithNoDescendantsIsSilent is the fourth case the
// acceptance criteria name: not an error, and not a gap either.
//
// Nothing is missing here — the archive holds the whole namespace and the object
// simply owns nothing — so the walk has nothing to report. The sentence the reader
// gets in that state is the command's, and it is asserted where the command builds
// it.
func TestDescendantsOverAnObjectWithNoDescendantsIsSilent(t *testing.T) {
	tree := query.Descendants([]string{"uid-cm"}, []query.OwnedObject{
		owned("ConfigMap", "checkout-config", "uid-cm"),
		owned("ConfigMap", "search-config", "uid-cm-2"),
	})

	if len(tree.Nodes) != 0 {
		t.Fatalf("an object that owns nothing reached %v", namesOf(tree))
	}
	if !tree.Complete() {
		t.Fatalf("an object that owns nothing reported a gap: %s. Nothing is missing — the other "+
			"objects in the scope name no owner at all", describeGaps(tree))
	}
	if len(tree.Refs()) != 0 {
		t.Fatalf("the subject list is %v, want empty", tree.Refs())
	}
}

// TestDescendantsSpansEveryRootIncarnation covers the roots an events-only question
// supplies, which resolves no incarnation and therefore hands over every UID the
// name has worn.
func TestDescendantsSpansEveryRootIncarnation(t *testing.T) {
	objects := []query.OwnedObject{
		owned("Deployment", "checkout", "uid-old"),
		owned("Deployment", "checkout", "uid-new"),
		owned("ReplicaSet", "checkout-1111", "uid-rs-old", ownerOf("Deployment", "checkout", "uid-old")),
		owned("ReplicaSet", "checkout-2222", "uid-rs-new", ownerOf("Deployment", "checkout", "uid-new")),
	}
	ref := query.ObjectRef{
		ClusterID: walkCluster, APIGroup: "apps", Kind: "Deployment",
		Namespace: walkNamespace, Name: "checkout",
	}

	roots := query.UIDsOf(objects, ref)
	if got, want := roots, []string{"uid-old", "uid-new"}; !slices.Equal(got, want) {
		t.Fatalf("UIDsOf returned %v, want %v", got, want)
	}

	tree := query.Descendants(roots, objects)
	if got, want := namesOf(tree), []string{"checkout-1111", "checkout-2222"}; !slices.Equal(got, want) {
		t.Fatalf("the walk reached %v, want %v — a question spanning a delete-and-recreate spans "+
			"the trees below both", got, want)
	}
}

// TestOwnersOfReadsBothWaysIn pins the two decodings against one another: the whole
// document, as an archive line carries it, and the projected array, as the table
// backend fetches it.
func TestOwnersOfReadsBothWaysIn(t *testing.T) {
	const refs = `[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"checkout-7d4f",` +
		`"uid":"uid-rs","controller":true}]`
	document := `{"metadata":{"name":"checkout-7d4f-ldw5j","ownerReferences":` + refs + `}}`

	fromDocument := query.OwnersOf(document)
	fromArray := query.ParseOwnerReferences(refs)
	if !slices.Equal(fromDocument, fromArray) {
		t.Fatalf("the two decodings disagree: %v from the document, %v from the array",
			fromDocument, fromArray)
	}
	if len(fromDocument) != 1 || fromDocument[0].UID != "uid-rs" ||
		fromDocument[0].Kind != "ReplicaSet" {
		t.Fatalf("the owner decoded as %+v", fromDocument)
	}
}

// TestOwnersOfIsSilentOnWhatItCannotRead covers the payloads a real archive holds
// and this decoding must not fail the timeline over.
//
// A reference with no UID is dropped rather than kept: the UID is the join key, so
// such a reference can neither be followed nor be reported as absent, and keeping
// it would make every object that had one count as a gap.
func TestOwnersOfIsSilentOnWhatItCannotRead(t *testing.T) {
	for _, tc := range []struct{ name, data string }{
		{"no data at all", ""},
		{"not an object", `"redacted"`},
		{"truncated", `{"metadata":{"ownerReferences":[`},
		{"no metadata", `{"spec":{}}`},
		{"no references", `{"metadata":{"name":"checkout"}}`},
		{"a reference with no uid", `{"metadata":{"ownerReferences":[{"kind":"ReplicaSet"}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if owners := query.OwnersOf(tc.data); len(owners) != 0 {
				t.Fatalf("read %v owners out of %q", owners, tc.data)
			}
		})
	}
}

// chainOf builds a chain of objects levels deep below uid-0, which is the root.
func chainOf(levels int) []query.OwnedObject {
	objects := []query.OwnedObject{owned("Deployment", "level-0", "uid-0")}
	for level := 1; level <= levels; level++ {
		objects = append(objects, owned("Pod",
			fmt.Sprintf("level-%d", level), fmt.Sprintf("uid-%d", level),
			ownerOf("Pod", fmt.Sprintf("level-%d", level-1), fmt.Sprintf("uid-%d", level-1))))
	}
	return objects
}

// namesOf lists the names the walk reached, in the order it reached them.
func namesOf(tree query.OwnershipTree) []string {
	names := make([]string, 0, len(tree.Nodes))
	for _, node := range tree.Nodes {
		names = append(names, node.Ref.Name)
	}
	return names
}

// depthsOf lists the depths the walk recorded, in the same order.
func depthsOf(tree query.OwnershipTree) []int {
	depths := make([]int, 0, len(tree.Nodes))
	for _, node := range tree.Nodes {
		depths = append(depths, node.Depth)
	}
	return depths
}

// describeGaps renders why a tree is not complete, so a failure says which of the
// four reasons fired rather than only that one did.
func describeGaps(tree query.OwnershipTree) string {
	return strings.Join([]string{
		fmt.Sprintf("depth limited=%t", tree.DepthLimited),
		fmt.Sprintf("breadth limited=%t", tree.BreadthLimited),
		fmt.Sprintf("missing owners=%d %v", tree.MissingOwnerObjects, tree.MissingOwnerKinds),
		fmt.Sprintf("unreadable=%d %v", tree.UnreadableObjects, tree.UnreadableKinds),
	}, ", ")
}
