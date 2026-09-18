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

package objectsource

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// This backend answers the read plane's optional ownership half.
//
// Declared apart from the QueryEngine assertion in engine.go for the reason the
// table backend declares its own apart: the two are different promises, and an
// assertion beside the mandatory one would read as part of it.
var _ query.OwnershipResolver = (*Engine)(nil)

// ownershipMark is what a scan remembers about one line naming one object.
//
// The owners are read at decode time rather than being carried as the whole
// document, which is the difference between holding four strings per object and
// holding every captured manifest in the window at once. ts travels so that the
// newest full state wins when an object was recorded several times — an
// ownerReference is stable in practice, but "in practice" is not a rule this
// archive enforces, and picking whichever line happened to be decoded last would
// make the answer depend on listing order.
type ownershipMark struct {
	ref    query.ObjectRef
	uid    string
	ts     time.Time
	state  bool
	owners []query.OwnerReference
}

// ownershipAccumulator is what one archive object contributed to an ownership scan.
type ownershipAccumulator struct {
	marks []ownershipMark
}

// ownershipScan is the per-line decision an ownership read makes.
type ownershipScan struct {
	q query.OwnershipQuery
}

// decode reads one archive object and keeps one mark per record line in scope.
//
// Events are skipped. An Event carries no ownerReferences and is never a member of
// an ownership tree, while being the most numerous kind in almost every archive:
// keeping them would multiply the marks by the thing the walk has no use for. The
// exclusion cannot change the answer, which is what makes it a cost decision rather
// than a filter.
func (s ownershipScan) decode(acc *ownershipAccumulator, body io.Reader) error {
	return decodeFrame(body, func(line *recordLine) error {
		switch {
		case line.ClusterID != s.q.ClusterID:
			return nil
		case s.q.Namespace != "" && line.Namespace != s.q.Namespace:
			return nil
		case line.UID == "":
			// Nothing can name it as an owner and nothing can be attributed to it, so
			// it is neither an edge nor evidence of one.
			return nil
		case line.isEvent():
			return nil
		case !s.inWindow(line.Timestamp):
			return nil
		}
		acc.marks = append(acc.marks, ownershipMark{
			ref: query.ObjectRef{
				ClusterID: line.ClusterID, APIGroup: line.APIGroup, Kind: line.Kind,
				Namespace: line.Namespace, Name: line.Name,
			},
			uid:   line.UID,
			ts:    line.Timestamp,
			state: line.Data != "",
			// The contract's own decoding rather than a second one: two backends
			// disagreeing about what an owner reference is would be two ownership trees
			// for one archive, both well formed.
			owners: query.OwnersOf(line.Data),
		})
		return nil
	})
}

// inWindow reports whether an instant falls in the scan's window, inclusive.
func (s ownershipScan) inWindow(ts time.Time) bool {
	if !s.q.From.IsZero() && ts.Before(s.q.From) {
		return false
	}
	return s.q.To.IsZero() || !ts.After(s.q.To)
}

// Ownership returns one entry per captured object in the scope, with the owners its
// recorded state named.
//
// It is the read-time half of "Events about this Deployment's Pods". Pod names are
// generated, so the tree cannot be named at capture time (D52); it can be read back,
// because metadata.ownerReferences is in the stored data of every captured object.
// The cluster is never consulted — asking the API server would answer with today's
// tree for a question about a past window, and this tier is read on laptops whose
// cluster no longer exists (D18).
//
// # It is a second scan, and it is one scan
//
// There is no index from owner to dependent, which is what Capabilities.PointQuery
// being false already declares about every question here. So the edges come from a
// pass over the window — the same pass shape a timeline costs, with a much narrower
// thing kept per line. It is a second pass rather than a fold into the timeline's,
// because the tree has to be *known* before the Event predicate that uses it can be
// built. What it is not is a pass per level of the tree, which is what asking for
// one owner's dependents at a time would have cost.
//
// # Why a partial read is a failure here and not a short answer
//
// A timeline delivers what it read and reports the rest, because a missing change is
// a missing change. A missing row here is a missing *edge*, and an ownership walk
// over a tree with an absent edge reports the subtree below it as not existing —
// the traversal form of the empty result Invariant 9 forbids. So a scan that failed
// part way is returned as a failure, and the caller says nothing about the tree.
func (e *Engine) Ownership(
	ctx context.Context, q query.OwnershipQuery,
) ([]query.OwnedObject, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if err := requireWindow(q.From, q.To); err != nil {
		return nil, fmt.Errorf("reading the ownership of %s: %w", describeScope(q), err)
	}

	e.beginScan()

	scan := ownershipScan{q: q}
	var marks []ownershipMark
	err := scanPartitions(ctx, e, e.recordPrefixes(q.ClusterID, q.From, q.To), scan.decode,
		func(acc *ownershipAccumulator) { marks = append(marks, acc.marks...) })
	if err != nil {
		return nil, fmt.Errorf("reading the ownership of %s: %w", describeScope(q), err)
	}
	return ownedObjects(marks), nil
}

// ownedObjects reduces the marks to one entry per incarnation.
//
// The owners come from the newest data-bearing line, which is the same rule the
// table backend's argMaxIf applies — the two must agree about which line's
// references are the object's, or one archive read two ways would produce two trees.
//
// StateRecorded is whether any line carried data at all, and it is deliberately not
// the same fact as an empty owner list. An object whose only lines in the window are
// a patch and a deletion owns whatever it owns; the window simply does not say, and
// reporting it as owning nothing would turn a narrow window into a small tree
// (Invariant 9).
//
// The result is sorted by identity so that one archive produces one ordering
// whatever order its partitions were listed in. The walk is breadth-first over this
// slice, so an unsorted one would render the same tree's rows in a different order
// on every run.
func ownedObjects(marks []ownershipMark) []query.OwnedObject {
	objects := make([]query.OwnedObject, 0, len(marks))
	newest := make(map[string]time.Time, len(marks))
	at := make(map[string]int, len(marks))

	for _, mark := range marks {
		i, seen := at[mark.uid]
		if !seen {
			i = len(objects)
			at[mark.uid] = i
			objects = append(objects, query.OwnedObject{Ref: mark.ref, UID: mark.uid})
		}
		objects[i].StateRecorded = objects[i].StateRecorded || mark.state
		if !mark.state {
			continue
		}
		if when, held := newest[mark.uid]; held && !mark.ts.After(when) {
			continue
		}
		newest[mark.uid] = mark.ts
		objects[i].Owners = mark.owners
	}

	slices.SortFunc(objects, compareOwned)
	return objects
}

// compareOwned orders two objects by identity and then by incarnation, which is the
// order the table backend's ORDER BY produces.
func compareOwned(a, b query.OwnedObject) int {
	return cmp.Or(
		strings.Compare(a.Ref.APIGroup, b.Ref.APIGroup),
		strings.Compare(a.Ref.Kind, b.Ref.Kind),
		strings.Compare(a.Ref.Namespace, b.Ref.Namespace),
		strings.Compare(a.Ref.Name, b.Ref.Name),
		strings.Compare(a.UID, b.UID),
	)
}

// describeScope renders an ownership scope for an error message.
//
// The unrestricted namespace is spelled out rather than left blank, for the reason
// the CLI spells out an Event scope: an answer about a scope is only actionable if
// the reader can see how wide the scope was.
func describeScope(q query.OwnershipQuery) string {
	if q.Namespace == "" {
		return fmt.Sprintf("cluster %q in every namespace", q.ClusterID)
	}
	return fmt.Sprintf("cluster %q namespace %q", q.ClusterID, q.Namespace)
}
