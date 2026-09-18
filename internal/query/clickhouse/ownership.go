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

package clickhouse

import (
	"context"
	"fmt"

	"github.com/kuberecord/kuberecord/internal/query"
)

// This backend answers the read plane's optional ownership half.
//
// Declared here rather than in engine.go beside the QueryEngine assertion, because
// it is the optional half and the two are not the same promise: an engine may
// legitimately be one without being the other, and an assertion sitting beside the
// mandatory one would read as though it were part of it.
var _ query.OwnershipResolver = (*Engine)(nil)

// Ownership returns one entry per captured object in the scope, with the owners its
// recorded state named.
//
// It is the read-time half of "Events about this Deployment's Pods". Pod names are
// generated, so the tree cannot be named at capture time (D52); it can be read back,
// because metadata.ownerReferences is in the stored data of every captured object.
// The scan is over rows this archive already holds and the cluster is never
// consulted — asking the API server would answer with today's tree for a question
// about a past window.
//
// # What it costs
//
// The window, and the namespace when there is one. There is no index from owner to
// dependent — no schema this project writes holds one, and Schema v1 is frozen — so
// the edges come from a grouped read over the window's rows. The projection is
// narrow (see ownershipColumns) and Events are excluded, which is what keeps the
// bytes proportional to the number of objects rather than to the number of changes.
// A caller invoking this is under the same cold-scan guard as any other question,
// and it is one scan for the whole tree rather than one per level.
//
// # What a partial read would mean, and why it is a failure
//
// Unlike a timeline, this cannot deliver a short answer. A missing row here is not a
// missing change; it is a *missing edge*, and an ownership walk over a tree with a
// silently absent edge reports the subtree below it as not existing — the traversal
// form of the empty result Invariant 9 forbids. So a read that failed part-way is
// returned as a failure and the caller says nothing about the tree at all.
func (e *Engine) Ownership(
	ctx context.Context, q query.OwnershipQuery,
) (objects []query.OwnedObject, err error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	stmt := ownershipStatement(q)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the ownership of %s: %w", describeScope(q), err)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		object := query.OwnedObject{Ref: query.ObjectRef{ClusterID: q.ClusterID}}
		var (
			stateRows uint64
			ownerRefs string
		)
		scanErr := rows.Scan(&object.Ref.APIGroup, &object.Ref.Kind, &object.Ref.Namespace,
			&object.Ref.Name, &object.UID, &stateRows, &ownerRefs)
		if scanErr != nil {
			return nil, fmt.Errorf("decoding the ownership of an object in %s: %w",
				describeScope(q), scanErr)
		}
		object.StateRecorded = stateRows > 0
		// The contract's own decoding, not a second one: two backends disagreeing
		// about what an owner reference is would be two ownership trees for one
		// archive, both well formed.
		object.Owners = query.ParseOwnerReferences(ownerRefs)
		objects = append(objects, object)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("streaming the ownership of %s: %w", describeScope(q), rowsErr)
	}
	return objects, nil
}

// describeScope renders an ownership scope for an error message.
//
// The unrestricted namespace is spelled out rather than left blank, for the reason
// the CLI spells out an Event scope: an answer about a scope is only actionable if
// the reader can see how wide the scope was, and a message with an empty pair of
// quotes in it reads as a bug rather than as every namespace.
func describeScope(q query.OwnershipQuery) string {
	if q.Namespace == "" {
		return fmt.Sprintf("cluster %q in every namespace", q.ClusterID)
	}
	return fmt.Sprintf("cluster %q namespace %q", q.ClusterID, q.Namespace)
}
