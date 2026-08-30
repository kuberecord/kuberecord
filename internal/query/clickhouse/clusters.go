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

// Compile-time proof that this backend answers the optional cluster-identity half
// of the read plane, asserted where the implementation lives.
var _ query.ClusterIDLister = (*Engine)(nil)

// ClusterIDs reports every distinct cluster_id this sink holds history for.
//
// It is the last step before a command-line client gives up on resolving which
// cluster the user meant, and its result is rendered into that failure: "pass
// --cluster-id: this sink holds prod-eu-1, prod-us-1" is a message somebody can
// act on without opening a SQL client, which "pass --cluster-id" is not.
//
// The scope log is asked first and the record table only if the scope log is
// empty. See clusterIDsFromScopesStatement for why: one is a scan of a tiny plain
// MergeTree, the other is FINAL over the whole of history, and the second is worth
// paying only for an archive whose scopes have been trimmed out from under its
// records.
//
// An empty result is a result. A sink with nothing in it holds no clusters, and
// saying so is different from failing — the caller's own message for the empty
// case names the sink rather than blaming the query (Invariant 4).
func (e *Engine) ClusterIDs(ctx context.Context) ([]string, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	fromScopes, err := e.clusterIDs(ctx, clusterIDsFromScopesStatement(), tableWatchScopes)
	if err != nil {
		return nil, err
	}
	if len(fromScopes) > 0 {
		return fromScopes, nil
	}
	return e.clusterIDs(ctx, clusterIDsFromRecordsStatement(), tableResourceStates)
}

// clusterIDs runs one probe and collects its column.
//
// table is passed only so the error can name which of the two reads failed. A
// caller told "reading the recorded clusters" cannot tell a permission missing on
// one table from a permission missing on the other, and a read-only user granted
// SELECT on half the schema is a configuration people really do arrive with.
func (e *Engine) clusterIDs(ctx context.Context, stmt statement, table string) (ids []string, err error) {
	rows, queryErr := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if queryErr != nil {
		return nil, fmt.Errorf("reading the recorded clusters from %s: %w", table, queryErr)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, fmt.Errorf("decoding a recorded cluster from %s: %w", table, scanErr)
		}
		ids = append(ids, id)
	}
	// A short list here is worse than an error, because it is about to be printed
	// as the set of values a user may choose from: an omission would read as proof
	// that the cluster they were looking for is not in this sink.
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("streaming the recorded clusters from %s: %w", table, rowsErr)
	}
	return ids, nil
}
