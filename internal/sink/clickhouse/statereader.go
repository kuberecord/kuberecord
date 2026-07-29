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
	"time"

	"github.com/yelzhy/kubestream/internal/sink"
)

// Compile-time proof that the ClickHouse sink still satisfies the read half of
// the sink contract after its scope-epoch growth (Task 1.6), asserted where the
// implementation lives rather than at wiring time.
var _ sink.StateReader = (*CHWriter)(nil)

// lastKnownStatesQuery renders the resource_states warm-up query for filter.
//
// Filtering on api_group as well as kind keeps two different resources that
// share a Kind (e.g. batch/v1 Job vs. a CRD example.com/v1 Job) from
// cross-contaminating each other's warm-up history — the schema-v1 identity is
// (cluster_id, api_group, kind, namespace, name). This is the ClickHouse-side
// mirror of the in-process cacheKey builder, which keys on
// (api_group, kind, namespace, name) for the same reason.
//
// A non-empty filter.Namespace narrows to that namespace; an empty one matches
// every namespace (the GVK-wide scope today's warm-up uses), so the emitted SQL
// is identical to the original inline query when Namespace is unset.
//
// The grouping is per *incarnation* — (namespace, name, uid) — not per identity,
// and the HAVING therefore scopes per incarnation too: an incarnation whose own
// latest event is Deleted is closed and excluded, while one whose latest event is
// anything else is still open. A normal object yields exactly one row. Two or
// more rows for one (namespace, name) are the signature of a death nobody
// recorded: a delete-and-recreate that happened while the operator was down, in
// which the successor was seen first and the predecessor's close-out was never
// written. Detecting that from history alone — rather than by comparing history
// against the live cache — is what makes it detectable at all: the successor's
// own first row may already have landed by the time this query runs, at which
// point a per-identity argMax(uid, ts) would return the new UID and show nothing
// wrong. See pipeline's seedScope for what is done with the extra rows.
//
// This read is dedup-safe under resource_states' ReplacingMergeTree engine
// without FINAL: the GROUP BY emits exactly one row per (identity, uid) by
// construction, regardless of how many unmerged duplicate rows the table still
// holds. A ReplacingMergeTree duplicate is byte-identical to its original (the
// at-least-once write path re-inserts the same frozen ts and values — see
// 001_resource_states.sql), so neither the row count nor any of the aggregates —
// argMax(sha256, ts), argMax(api_version, ts), max(ts), argMax(event_type, ts) —
// can change between the pre- and post-merge states. The warm-up therefore never
// double-counts or mis-reads an incarnation from a pre-merge duplicate.
func lastKnownStatesQuery(filter sink.ScopeFilter) (string, []any) {
	query := `
        SELECT namespace, name, uid, argMax(sha256, ts), argMax(api_version, ts), max(ts)
        FROM resource_states
        WHERE api_group = ? AND kind = ? AND cluster_id = ?`
	args := []any{filter.APIGroup, filter.Kind, filter.ClusterID}
	if filter.Namespace != "" {
		query += `
        AND namespace = ?`
		args = append(args, filter.Namespace)
	}
	query += `
        GROUP BY namespace, name, uid
        HAVING argMax(event_type, ts) != 'Deleted'`
	return query, args
}

// scopeWasActiveQuery renders the scope-epoch probe for filter, as of asOf.
//
// Unlike lastKnownStatesQuery, namespace is matched *exactly* — including the
// empty string. Here the filter is a scope identity, not a record filter (see
// sink.ScopeFilter): a ClusterStreamRule watching every namespace and a
// StreamRule pinned to one namespace are two scopes with two independent epochs,
// so letting an empty namespace wildcard over namespaces would make one scope's
// history answer for another's.
//
// `ts < ?` is what keeps the answer race-free. The current epoch's own Started
// row is written asynchronously and may land at any moment; the caller passes the
// instant its epoch began, so only strictly-earlier rows — previous epochs — can
// decide the verdict. Rows share this scope's key prefix in the sort key, so this
// reads one narrow range rather than scanning the table.
//
// argMax(action, ts) is the last action in that range; watch_scopes is a plain
// MergeTree with no deduplication, but a scope's rows are written once each, so
// there is nothing to collapse.
func scopeWasActiveQuery(filter sink.ScopeFilter, asOf time.Time) (string, []any) {
	query := `
        SELECT argMax(action, ts)
        FROM watch_scopes
        WHERE cluster_id = ? AND api_group = ? AND kind = ? AND namespace = ? AND ts < ?`
	// The cutoff is rendered as a UTC datetime string rather than bound as a
	// time.Time, and the asymmetry with scopeInsertArgs (which binds the instant) is
	// deliberate: the driver formats a positional time.Time argument at *second*
	// precision, which would blunt this cutoff, while a quoted string is parsed
	// server-side against the DateTime64(9, 'UTC') column — full precision, no zone
	// ambiguity. Insert columns are the opposite: there the string is parsed
	// client-side in the local zone, so the instant is what must be bound.
	return query, []any{
		filter.ClusterID, filter.APIGroup, filter.Kind, filter.Namespace,
		asOf.UTC().Format(chTimeFormat),
	}
}

// ScopeWasActive implements sink.StateReader's scope-epoch probe: true iff this
// scope's most recent recorded action strictly before asOf is Started, i.e. a
// previous epoch of it was left open.
//
// An empty result (no rows for this scope at all) is reported as false, not as an
// error: a scope with no history is the ordinary brand-new case, and treating it
// as an error would make a first-ever warm-up retry forever.
func (w *CHWriter) ScopeWasActive(ctx context.Context, filter sink.ScopeFilter, asOf time.Time) (bool, error) {
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}

	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return false, fmt.Errorf("chwriter: shutting down, refusing scope epoch read")
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()
	defer w.otherUsers.Done()

	query, args := scopeWasActiveQuery(filter, asOf)
	rows, err := w.conn.Query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	// An aggregate over an empty range still yields one row, holding the empty
	// default for a String — so "no history" and "history ending in an empty
	// action" both read as not-active, which is the same, correct verdict.
	var action string
	if rows.Next() {
		if err := rows.Scan(&action); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return sink.ScopeAction(action) == sink.ScopeActionStarted, nil
}

// activeScopesQuery renders the boot-reconciliation enumeration: every scope in
// clusterID whose most recent recorded action is Started.
//
// It groups by the scope's identity columns — deliberately not api_version, which
// is provenance (Invariant 7) and would split one scope into two rows if two
// versions of a resource ever served it. The HAVING mirrors
// lastKnownStatesQuery's "most recent event decides" shape, so both reads answer
// from the same notion of currency.
func activeScopesQuery(clusterID string) (string, []any) {
	query := `
        SELECT api_group, kind, namespace
        FROM watch_scopes
        WHERE cluster_id = ?
        GROUP BY api_group, kind, namespace
        HAVING argMax(action, ts) = ?`
	return query, []any{clusterID, string(sink.ScopeActionStarted)}
}

// ActiveScopes implements sink.StateReader's boot-reconciliation enumeration:
// the scopes a previous process left open, so the warm/GC coordinator can close
// the ones nothing wants any more with a Stopped row (and never with Deleted
// rows).
//
// A mid-stream read failure surfaces via rows.Err() and is returned as an error
// rather than a short-but-complete list: acting on a partial enumeration would
// leave a genuinely orphaned scope open until the next attempt, and — worse —
// tempt a caller into treating an incomplete answer as authoritative.
func (w *CHWriter) ActiveScopes(ctx context.Context, clusterID string) ([]sink.ScopeFilter, error) {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return nil, fmt.Errorf("chwriter: shutting down, refusing scope enumeration")
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()
	defer w.otherUsers.Done()

	query, args := activeScopesQuery(clusterID)
	rows, err := w.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var scopes []sink.ScopeFilter
	for rows.Next() {
		scope := sink.ScopeFilter{ClusterID: clusterID}
		if err := rows.Scan(&scope.APIGroup, &scope.Kind, &scope.Namespace); err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return scopes, nil
}

// LastKnownStates implements sink.StateReader against the shared ClickHouse
// connection. It reports, per scope, the last-known (sha256, api_version, ts) of
// every *incarnation* whose own most recent event is not a deletion — exactly
// what a cache warm-up needs to reconstruct its dedup baseline without
// re-emitting live objects, plus the evidence it needs to close out an
// incarnation whose death was never recorded (see lastKnownStatesQuery).
//
// The call is registered in otherUsers under the closing check so Start never
// closes conn while a query is in flight (see CHWriter.otherUsers). A
// mid-stream read failure (the connection dropping after some rows) surfaces
// via rows.Err(), not Next(); it is returned as an error rather than silently
// treated as a short-but-complete result, so a caller relying on completeness
// (the warm-up) retries the whole scan instead of trusting a partial one.
func (w *CHWriter) LastKnownStates(ctx context.Context, filter sink.ScopeFilter) ([]sink.KnownState, error) {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return nil, fmt.Errorf("chwriter: shutting down, refusing state read")
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()
	defer w.otherUsers.Done()

	query, args := lastKnownStatesQuery(filter)
	rows, err := w.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var states []sink.KnownState
	for rows.Next() {
		var st sink.KnownState
		if err := rows.Scan(&st.Namespace, &st.Name, &st.UID, &st.SHA256, &st.APIVersion, &st.TS); err != nil {
			return nil, err
		}
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}
