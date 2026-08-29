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
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Compile-time proof that this backend answers the optional cluster-identity half
// of the read plane, asserted where the implementation lives.
var _ query.ClusterIDLister = (*Engine)(nil)

// ClusterIDs reports every distinct cluster_id this archive holds records for.
//
// It matters most here, and not on an indexed backend. An evaluator who synced an
// archive to a laptop and ran the CLI against a directory has no cluster to ask —
// no kubeconfig, no operator Deployment, nothing but the files — so this is the
// step that makes the zero-infrastructure path zero-*configuration* too. Without
// it, the archive that needs the least setup would be the one demanding a flag
// the indexed backend can work out for itself.
//
// # What it costs, stated plainly
//
// The archive's layout partitions records by cluster
// (format=jsonl-v1/cluster_id=<id>/…), so the answer is in the keys and no object
// is ever opened. But ObjectSource.List is flat and recursive by contract — there
// is no delimiter, because the partition pruning everything else in this package
// does needs the keys and not the folders — so learning *all* the clusters means
// listing every record key in the archive. That is a metadata-only walk, and
// against an object store it is a page of a thousand keys per round trip.
//
// It is bounded by nothing but the archive's size, which is why the caller reaches
// for it last: after the flag, after its configuration, after the operator's own
// Deployment. Read the whole listing rather than stopping at the second distinct
// cluster, because the result is about to be shown to somebody choosing from it —
// a list that stopped early would present a subset as the set.
//
// The scope log is not consulted. Unlike the record keys it is partitioned by date
// alone, with the cluster inside the line rather than in the key, so answering
// from it would mean decompressing objects to learn something the record keys
// already spell out.
func (e *Engine) ClusterIDs(ctx context.Context) (ids []string, err error) {
	if openErr := e.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	// The listing root is everything of this format, one level above the cluster
	// partition. The trailing slash is what keeps a sibling format directory —
	// format=jsonl-v2, one day — out of a walk that would otherwise match it by
	// prefix and read its keys as if they were this contract's.
	root := joinSegments(e.prefix, formatPartition) + "/"

	it := e.src.List(ctx, root)
	defer closeListing(it, &err)

	seen := map[string]bool{}
	for it.Next() {
		key := it.Object().Key
		if !isObjectKey(key) {
			continue
		}
		id, ok := clusterIDOf(key, root)
		if !ok {
			// A key under this root that is not filed beneath a cluster partition is
			// the scope log, which is where it belongs, or something else's object in
			// a shared bucket. Neither is a cluster and neither is an error.
			continue
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	if listErr := it.Err(); listErr != nil {
		return nil, fmt.Errorf("listing %q to find the recorded clusters: %w", root, listErr)
	}

	// Sorted because the contract says so: this list is rendered into a message a
	// person chooses from, and an order that followed the store's own would differ
	// between a bucket and the directory somebody synced it into.
	slices.Sort(ids)
	return ids, nil
}

// clusterIDOf reads the cluster partition out of a record key, or reports that the
// key is not filed under one.
//
// It takes the root it was listed under rather than searching the whole key for
// the segment, because a prefix chosen by whoever owns the bucket may contain
// anything at all — including, quite legally, a directory called cluster_id=x —
// and a reader that scanned for the segment would take that as the answer.
func clusterIDOf(key, root string) (string, bool) {
	rest, found := strings.CutPrefix(key, root)
	if !found {
		return "", false
	}
	segment, _, found := strings.Cut(rest, "/")
	if !found {
		return "", false
	}
	id, found := strings.CutPrefix(segment, clusterSegment)
	if !found || id == "" {
		return "", false
	}
	return id, true
}
