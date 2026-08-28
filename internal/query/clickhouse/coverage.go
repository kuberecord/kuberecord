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

// Coverage reports when the scopes matching a query were actually being watched.
//
// This is the mechanism behind Invariant 9 and the reason an empty timeline is
// explicable at all. "Nothing changed" and "nobody was watching" are different
// facts that look identical from a timeline alone, and they send an engineer at
// 02:47 in opposite directions.
//
// The pairing itself is the contract's (query.CoverageOf) rather than this
// package's, because its interesting cases are quiet ones — an unmatched trailing
// Started that must stay open, an overlapping interval that must not be clipped —
// and two backends reading the same log differently would disagree about when the
// recorder was watching while both looked plausible.
//
// A scope the log holds nothing for comes back as an empty slice and a nil error.
// That is the "nothing was watching" answer, and it is a result rather than a
// failure — collapsing it into an error would make the fact unreportable by the
// one call that exists to report it.
func (e *Engine) Coverage(ctx context.Context, q query.ScopeQuery) ([]query.ScopeInterval, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	transitions, err := e.scopeTransitions(ctx, q)
	if err != nil {
		return nil, err
	}
	return query.CoverageOf(transitions, q.From, q.To), nil
}

// scopeTransitions reads the matching rows of the scope log.
//
// The statement orders by (api_group, kind, namespace, ts), which is the order
// pairing wants; it is kept even though query.CoverageOf orders defensively of its
// own accord, because a server-side sort over a small, indexed table costs
// nothing and a reader of the published statement should see the order the
// procedure is described in.
func (e *Engine) scopeTransitions(
	ctx context.Context, q query.ScopeQuery,
) (transitions []query.ScopeChange, err error) {
	stmt := coverageStatement(q)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the scope log of cluster %q: %w", q.ClusterID, err)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		var t query.ScopeChange
		scanErr := rows.Scan(&t.APIGroup, &t.Kind, &t.Namespace, &t.Action, &t.RuleRef, &t.TS)
		if scanErr != nil {
			return nil, fmt.Errorf("decoding a %s row: %w", tableWatchScopes, scanErr)
		}
		transitions = append(transitions, t)
	}
	// A partial scope log is worse than none: it is the input to the answer that
	// says whether a silence is meaningful, and half of it would report an outage
	// that did not happen.
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("streaming the scope log of cluster %q: %w", q.ClusterID, rowsErr)
	}
	return transitions, nil
}
