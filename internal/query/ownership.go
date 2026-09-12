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

package query

import (
	"context"
	"encoding/json"
	"slices"
	"time"
)

// Ownership, read from the archive rather than from the cluster.
//
// # The question this exists for
//
// "Events about this Deployment's Pods" is the most-wanted Event query and the one
// no capture-time filter can express. The tree is Deployment → ReplicaSet → Pod,
// and a Pod's name carries a generated suffix that does not exist until the Pod
// does and changes on every rollout (D52), so a rule authored today cannot name the
// objects an incident tomorrow will be about.
//
// At read time the names exist, because metadata.ownerReferences is in the stored
// data of every captured object. The tree is therefore a query over rows the
// archive already holds.
//
// # Why the live cluster is never consulted
//
// The API server would answer with *today's* tree for a question about a past
// window: the ReplicaSet that was scaled down last Tuesday has since been garbage
// collected, and the Pods that failed to schedule are gone. It would also make the
// answer depend on a cluster still existing, which an archive read on a laptop
// deliberately does not (D18). So the walk reads rows, and where the rows stop, the
// walk stops and says so.
//
// # Why this is an optional half of the read plane
//
// It is a separate interface rather than a method on [QueryEngine], exactly as
// [ScanEstimator] is: an engine whose storage cannot answer it must be detectable
// rather than obliged to invent an empty tree, and an empty tree a caller cannot
// distinguish from "this object owns nothing" is the silent failure Invariant 4
// forbids.

// MaxOwnershipDepth is how many levels below the named object a walk will descend.
//
// Four, against a real tree that is two: Deployment → ReplicaSet → Pod, and
// CronJob → Job → Pod, are the deepest controller chains Kubernetes itself builds,
// and an operator adding one of its own has room for two more. The bound exists
// because an ownership graph is a DAG in principle and a cycle in a corrupted
// archive, and a walk that does not bound itself is a hang — the one failure mode a
// read-only tool has no excuse for.
//
// It is a constant rather than a flag because the number is not a preference. A
// reader who needs a fifth level has an archive whose shape nobody anticipated, and
// the honest response to that is a notice naming the bound (see
// OwnershipTree.DepthLimited) rather than a flag inviting them to raise it until the
// walk hangs.
const MaxOwnershipDepth = 4

// MaxOwnedObjects is how many descendants one walk will collect.
//
// The tree becomes one predicate in one query — the subjects of
// [TimelineQuery.Subjects] — so its size is the size of a statement rather than the
// size of a result set, and an unbounded one would be a Deployment with four
// thousand Pods rendered as a megabyte of SQL. Five hundred is far past any tree a
// person reads the Events of in one page, and a walk that reaches it says so
// (OwnershipTree.BreadthLimited) instead of quietly answering about a prefix.
const MaxOwnedObjects = 500

// OwnerReference is one entry of an object's metadata.ownerReferences, as it was
// recorded.
//
// It is the reference and not the object: UID is the join key the walk uses, and
// Kind is what makes a *missing* owner reportable — an owner that was never captured
// has no row to read a kind off, and the reference the dependent carries is the only
// place its kind survives. That is the whole of how "the middle level is uncovered"
// becomes a sentence naming ReplicaSet rather than a shrug.
//
// controller and blockOwnerDeletion are deliberately not carried. The walk follows
// every owner rather than only the controlling one, because a reader asking what an
// object owns means the tree and not the garbage-collection edge, and a field nothing
// reads is a field that eventually gets read for the wrong reason.
type OwnerReference struct {
	// APIVersion is the group and version of the owner, as the reference spells it.
	// Provenance only: identity here is the UID, for the reason ObjectRef gives.
	APIVersion string `json:"apiVersion"`
	// Kind is the owner's kind.
	Kind string `json:"kind"`
	// Name is the owner's name.
	Name string `json:"name"`
	// UID is the owner's UID, and the key the walk joins on.
	UID string `json:"uid"`
}

// OwnedObject is one captured object and the owners its recorded state named.
//
// One of these per (identity, incarnation) in the window, whether or not it owns or
// is owned by anything: the walk needs the objects with owners to build its edges,
// and it needs the objects *without* them just as much, because the set of captured
// UIDs is what tells a dangling owner reference from a satisfied one.
type OwnedObject struct {
	// Ref is the object's canonical identity.
	Ref ObjectRef
	// UID is the incarnation. Two incarnations of one name are two entries, because
	// an ownerReference names a UID and splicing them would attribute one object's
	// dependents to another (Invariant 7).
	UID string
	// StateRecorded reports whether the window holds a data-bearing row for this
	// object — which is to say, whether Owners could be read at all.
	//
	// False is not the same fact as an empty Owners list, and the difference is the
	// whole reason the field exists. An object whose only rows in the window are a
	// patch and a deletion genuinely owns whatever it owns; the archive simply does
	// not say so within these bounds, and reporting it as owning nothing would turn
	// a window that is too narrow into an ownership tree that is too small
	// (Invariant 9).
	StateRecorded bool
	// Owners is metadata.ownerReferences as recorded, empty for an object that names
	// none and for one whose state was not recorded in the window.
	Owners []OwnerReference
}

// OwnershipQuery asks for the ownership edges of one scope.
//
// It asks for a *scope* rather than for the descendants of one object, and that is
// the design rather than an approximation of it. A reverse walk needs an index from
// owner to dependent, no storage this project writes holds one, and building it per
// level would mean one scan of the window per level of the tree. One scan produces
// the whole index, and it produces the two facts a level-by-level walk could never
// see: which owner references name objects the archive does not hold, and which
// objects' ownership could not be read at all.
type OwnershipQuery struct {
	// ClusterID is the cluster whose rows to read.
	ClusterID string

	// Namespace restricts the scan; empty means every namespace.
	//
	// A namespaced object's owners must live in its own namespace, so a walk from a
	// namespaced root is complete within one — which is what makes the common case
	// affordable. A cluster-scoped root has dependents that may be anywhere, so its
	// caller leaves this empty and pays for the whole cluster.
	Namespace string

	// From and To bound the window, inclusive, and are subject to the same
	// ErrTimeBoundRequired rule as everything else an engine is asked.
	//
	// The window is the honest limit of the answer: ownership is read from rows, so a
	// window that excludes the only full state of a ReplicaSet excludes the
	// ReplicaSet from the tree. A caller must report that rather than absorb it.
	From time.Time
	To   time.Time
}

// OwnershipResolver is the optional half of the read plane for engines that can
// enumerate the ownership edges of a window.
//
// It is a separate interface rather than a method on [QueryEngine] for
// [ScanEstimator]'s reason, one question along: an engine that cannot enumerate
// objects would have to answer with an empty tree, and an empty tree is
// indistinguishable from an object that owns nothing. Detected, it becomes a
// command that says what it cannot do:
//
//	if resolver, ok := engine.(query.OwnershipResolver); ok {
//	        ...
//	}
type OwnershipResolver interface {
	// Ownership returns one entry per captured object in the scope, with the owners
	// its recorded state named.
	//
	// Kubernetes Events are excluded. An Event carries no ownerReferences and is
	// never a member of an ownership tree, while being the most numerous kind in
	// almost every archive — including them would multiply the cost of the scan by
	// the thing the scan has no use for. The exclusion is a cost decision that
	// cannot change the answer, which is the only kind of exclusion permitted here.
	//
	// Errors: whatever prevented the read, and ErrTimeBoundRequired when the engine
	// demands a window and q carries none. A scope holding no objects is not an
	// error — it is an empty slice, and the difference between "owns nothing" and
	// "could not be read" is one the caller must keep (Invariant 4).
	Ownership(ctx context.Context, q OwnershipQuery) ([]OwnedObject, error)
}

// OwnedNode is one object a walk reached, and how it got there.
type OwnedNode struct {
	// Ref is the object's canonical identity.
	Ref ObjectRef
	// UID is the incarnation reached. The edge was matched on it, so this is exact
	// rather than the forgiving (kind, namespace, name) key.
	UID string
	// Depth is how many ownership edges away from the root it is: 1 for a direct
	// dependent.
	Depth int
	// OwnerUID is the UID of the node it was reached through, which is what lets a
	// caller render the tree rather than the set.
	OwnerUID string
}

// OwnershipTree is what a walk reached, and what it could not.
//
// The second half is not diagnostics. A partial tree presented as a whole one is the
// traversal form of the empty result Invariant 9 forbids: a reader who asked for a
// Deployment's Pods' Events and got none concludes there were none, and the true
// answer may be that the ReplicaSet between them was never captured. So every reason
// the walk could be short is measured and named, and none of them is inferred from
// another (D41).
type OwnershipTree struct {
	// Nodes are the descendants, breadth first and then in the order the ownership
	// answer supplied them, with the root excluded — a caller already has the root.
	Nodes []OwnedNode

	// DepthLimited reports that MaxOwnershipDepth stopped the walk with unvisited
	// dependents still below it. It is measured rather than assumed from the walk
	// having used every level: a tree exactly MaxOwnershipDepth deep is complete,
	// and reporting it as truncated would be a warning about nothing.
	DepthLimited bool

	// BreadthLimited reports that MaxOwnedObjects stopped the walk.
	BreadthLimited bool

	// MissingOwnerKinds are the kinds of owner that unreached objects named and the
	// archive does not hold, sorted and distinct.
	//
	// This is the middle-level-uncovered finding, and it is stated about the scope
	// rather than about the tree because that is what was measured. An object whose
	// owner is not in the archive *might* hang below the root — nothing can say,
	// which is exactly the gap — and a claim that it does would be the second thing
	// asserted from the first thing's evidence (D41).
	MissingOwnerKinds []string

	// MissingOwnerObjects is how many unreached objects named such an owner.
	MissingOwnerObjects int

	// UnreadableKinds are the kinds of unreached object whose own ownership could
	// not be read, because the window holds no full state of them. Sorted and
	// distinct.
	UnreadableKinds []string

	// UnreadableObjects is how many such objects there were.
	UnreadableObjects int
}

// Complete reports whether the walk saw everything the archive could show it.
//
// It is the one predicate a caller needs to decide whether to say anything at all,
// and it is a method rather than four comparisons at the call site so that a reason
// added later is a reason every caller already reports.
func (t OwnershipTree) Complete() bool {
	return !t.DepthLimited && !t.BreadthLimited &&
		t.MissingOwnerObjects == 0 && t.UnreadableObjects == 0
}

// Refs returns the nodes' identities, in the order the walk reached them.
//
// It exists because that slice is what a caller hands to [TimelineQuery.Subjects],
// and building it inline at each call site is how one of them comes to include the
// root and correlate its Events twice.
func (t OwnershipTree) Refs() []ObjectRef {
	refs := make([]ObjectRef, 0, len(t.Nodes))
	for _, node := range t.Nodes {
		refs = append(refs, node.Ref)
	}
	return refs
}

// Descendants walks the ownership edges of one scope from a set of root
// incarnations.
//
// roots are UIDs rather than an identity because an ownerReference names a UID: a
// caller that has resolved one incarnation passes that one, and a caller that has
// not — an events-only question, which resolves none by construction — passes every
// UID the name has worn, which is the forgiving key the rest of that path already
// uses. See UIDsOf.
//
// # What bounds it
//
// Three things, and all three are reported rather than absorbed. Depth stops at
// MaxOwnershipDepth. Breadth stops at MaxOwnedObjects. And a UID is visited at most
// once, which is what makes a cycle terminate: an archive holding A owns B owns A is
// corrupt, but a read-only tool meeting corrupt data must return, and it must return
// the part of the tree that is not a lie rather than nothing at all.
//
// # Why the gaps are measured over unreached objects
//
// An object the walk reached is not hiding anything: its dependents were enumerable
// from its own UID, whatever else its owner references say. An object the walk did
// *not* reach and cannot attribute to any recorded owner is the one that might have
// been below the root, and it is the only one worth a sentence. Measuring over every
// object in the scope would report a gap on trees that are complete, which teaches a
// reader to stop reading the notice.
func Descendants(roots []string, objects []OwnedObject) OwnershipTree {
	var tree OwnershipTree

	children := make(map[string][]int, len(objects))
	captured := make(map[string]struct{}, len(objects))
	for i, object := range objects {
		if object.UID == "" {
			// A row with no incarnation cannot be an edge in either direction: nothing
			// can name it as an owner, and nothing can be attributed to it.
			continue
		}
		captured[object.UID] = struct{}{}
		for _, owner := range object.Owners {
			if owner.UID != "" {
				children[owner.UID] = append(children[owner.UID], i)
			}
		}
	}

	visited := make(map[string]struct{}, len(roots))
	frontier := make([]string, 0, len(roots))
	for _, root := range roots {
		if root == "" {
			continue
		}
		if _, seen := visited[root]; seen {
			continue
		}
		visited[root] = struct{}{}
		frontier = append(frontier, root)
	}

	for depth := 1; depth <= MaxOwnershipDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, owner := range frontier {
			for _, index := range children[owner] {
				child := objects[index]
				if _, seen := visited[child.UID]; seen {
					continue
				}
				if len(tree.Nodes) >= MaxOwnedObjects {
					// next is dropped rather than kept: the walk is stopping here, and a
					// frontier left populated would be read by hasUnvisited as depth
					// having cut the tree as well — two findings from one cause, only one
					// of which happened.
					tree.BreadthLimited = true
					next = nil
					break
				}
				visited[child.UID] = struct{}{}
				tree.Nodes = append(tree.Nodes, OwnedNode{
					Ref: child.Ref, UID: child.UID, Depth: depth, OwnerUID: owner,
				})
				next = append(next, child.UID)
			}
			if tree.BreadthLimited {
				break
			}
		}
		frontier = next
	}

	// Measured rather than inferred from the loop having used every level: a tree
	// exactly MaxOwnershipDepth deep is a complete tree, and only an unvisited
	// dependent below the last one visited means the bound actually cut something.
	tree.DepthLimited = hasUnvisited(frontier, children, objects, visited)
	tree.measureGaps(objects, visited, captured)
	return tree
}

// hasUnvisited reports whether anything in the frontier still has a dependent the
// walk has not reached.
func hasUnvisited(
	frontier []string, children map[string][]int, objects []OwnedObject, visited map[string]struct{},
) bool {
	for _, owner := range frontier {
		for _, index := range children[owner] {
			if _, seen := visited[objects[index].UID]; !seen {
				return true
			}
		}
	}
	return false
}

// measureGaps counts the objects the walk could not attribute, and names their
// kinds.
//
// Two gaps, counted apart because they have different remedies and a reader acts on
// the remedy. An owner reference naming a UID the archive does not hold is a kind
// nobody captured — the rule needs the kind added. An object with no full state in
// the window is a kind that *was* captured, whose ownership simply did not appear
// within these bounds — the window needs widening. Merging them into one number
// would leave a reader with two possible fixes and no way to choose.
func (t *OwnershipTree) measureGaps(
	objects []OwnedObject, visited, captured map[string]struct{},
) {
	missing := make(map[string]struct{})
	unreadable := make(map[string]struct{})

	for _, object := range objects {
		if _, reached := visited[object.UID]; reached {
			continue
		}
		if !object.StateRecorded {
			t.UnreadableObjects++
			unreadable[object.Ref.Kind] = struct{}{}
			continue
		}
		for _, owner := range object.Owners {
			if _, held := captured[owner.UID]; held {
				continue
			}
			t.MissingOwnerObjects++
			missing[owner.Kind] = struct{}{}
			break
		}
	}

	t.MissingOwnerKinds = sortedKeys(missing)
	t.UnreadableKinds = sortedKeys(unreadable)
}

// sortedKeys renders a kind set as the sorted, distinct list a notice names.
//
// Sorted because the notice is compared against a golden file and read by a person,
// and a set iterated in map order would produce a different sentence on every run
// for the same archive.
func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// UIDsOf returns every incarnation of one identity in an ownership answer, in the
// order the answer supplied them.
//
// It is how a caller with no resolved incarnation names its roots. That is the
// events-only path, which resolves none by construction
// ([TimelineQuery.EventsOnly]), and the honest root set there is every UID the name
// has worn: the question spans a delete-and-recreate, and so does the tree beneath
// it.
func UIDsOf(objects []OwnedObject, ref ObjectRef) []string {
	var uids []string
	for _, object := range objects {
		if object.UID != "" && object.Ref == ref {
			uids = append(uids, object.UID)
		}
	}
	return uids
}

// ownerReferencesEnvelope is the part of a recorded object that names its owners.
type ownerReferencesEnvelope struct {
	Metadata struct {
		OwnerReferences []OwnerReference `json:"ownerReferences"`
	} `json:"metadata"`
}

// OwnersOf reads metadata.ownerReferences out of a recorded object document.
//
// A document that will not parse yields no owners rather than an error, which is the
// same reading the Event correlation applies to an unreadable payload: an object
// whose ownership cannot be read is not attributed to anything, and it is not a
// reason to fail the timeline it was going to be part of. The object is still
// returned by the engine, with StateRecorded true and no owners, so the walk reports
// it among the objects it could not attribute rather than losing it entirely.
func OwnersOf(data string) []OwnerReference {
	if data == "" {
		return nil
	}
	var envelope ownerReferencesEnvelope
	if err := json.Unmarshal([]byte(data), &envelope); err != nil {
		return nil
	}
	return keepIdentifiedOwners(envelope.Metadata.OwnerReferences)
}

// ParseOwnerReferences reads the ownerReferences array on its own.
//
// It exists for a backend that projects the array server-side rather than fetching
// the whole document to read four fields out of it — which is the difference between
// reading a Deployment's manifest per object in the window and reading a hundred
// bytes. Both spellings end in the same decoding, so the two backends cannot come to
// disagree about what an owner reference is.
func ParseOwnerReferences(raw string) []OwnerReference {
	if raw == "" {
		return nil
	}
	var owners []OwnerReference
	if err := json.Unmarshal([]byte(raw), &owners); err != nil {
		return nil
	}
	return keepIdentifiedOwners(owners)
}

// keepIdentifiedOwners drops references carrying no UID.
//
// The UID is the join key, so a reference without one is an edge to nowhere: it can
// neither be followed nor be reported as missing, since "the archive does not hold
// it" would be indistinguishable from "the reference did not say what it was". A
// reference like that is malformed rather than interesting, and carrying it would
// make every object that had one count as a gap.
func keepIdentifiedOwners(owners []OwnerReference) []OwnerReference {
	kept := make([]OwnerReference, 0, len(owners))
	for _, owner := range owners {
		if owner.UID != "" {
			kept = append(kept, owner)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}
