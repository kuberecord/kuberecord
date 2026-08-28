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
	"slices"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The actions the scope log records, spelled as its own action column spells
// them.
const (
	scopeStarted = "Started"
	scopeStopped = "Stopped"
)

// scopeKey is one watched scope's identity.
//
// Namespace is part of it, with the scope log's own reading rather than a
// ScopeQuery's: the empty string is the all-namespaces scope itself, not a
// wildcard. A rule pinned to one namespace and a cluster-wide rule over the same
// kind are two scopes with two independent epochs, and pairing their transitions
// together would invent an outage in one from a transition in the other.
type scopeKey struct {
	apiGroup  string
	kind      string
	namespace string
}

// scopeTransition is one row of the scope log.
type scopeTransition struct {
	key     scopeKey
	action  string
	ruleRef string
	ts      time.Time
}

// Coverage reports when the scopes matching a query were actually being watched.
//
// This is the mechanism behind Invariant 9 and the reason an empty timeline is
// explicable at all. "Nothing changed" and "nobody was watching" are different
// facts that look identical from a timeline alone, and they send an engineer at
// 02:47 in opposite directions.
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

	// The statement orders by (api_group, kind, namespace, ts), so each scope's
	// transitions arrive contiguously and in the order they happened — which is
	// exactly the order pairing has to walk them in.
	var intervals []query.ScopeInterval
	for start := 0; start < len(transitions); {
		end := start
		for end < len(transitions) && transitions[end].key == transitions[start].key {
			end++
		}
		for _, interval := range pairScope(transitions[start:end]) {
			if overlapsWindow(interval, q.From, q.To) {
				intervals = append(intervals, interval)
			}
		}
		start = end
	}

	slices.SortStableFunc(intervals, func(a, b query.ScopeInterval) int { return a.From.Compare(b.From) })
	return intervals, nil
}

// scopeTransitions reads the matching rows of the scope log.
func (e *Engine) scopeTransitions(
	ctx context.Context, q query.ScopeQuery,
) (transitions []scopeTransition, err error) {
	stmt := coverageStatement(q)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the scope log of cluster %q: %w", q.ClusterID, err)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		var t scopeTransition
		scanErr := rows.Scan(&t.key.apiGroup, &t.key.kind, &t.key.namespace, &t.action, &t.ruleRef, &t.ts)
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

// pairScope walks one scope's transitions in order, opening on Started and
// closing on Stopped.
//
// An unmatched trailing Started is left open, and that is the load-bearing case:
// a process that exits writes no Stopped row, so a scope being watched right now
// and a scope left open by a process that never came back look the same from
// here, and both of them are genuinely still open in the log. Closing one at the
// last row would turn "we are watching this and nothing has happened" into
// "nobody is watching this" — the opposite conclusion.
//
// A second Started while one is open adds nothing. The scope was already being
// watched, and starting a fresh interval would report a zero-length gap in
// coverage that never occurred.
func pairScope(transitions []scopeTransition) []query.ScopeInterval {
	var intervals []query.ScopeInterval
	open := -1
	for _, t := range transitions {
		switch t.action {
		case scopeStarted:
			if open >= 0 {
				continue
			}
			intervals = append(intervals, query.ScopeInterval{
				APIGroup:  t.key.apiGroup,
				Kind:      t.key.kind,
				Namespace: t.key.namespace,
				// The rule that *opened* the interval. A Stopped row's rule_ref may
				// name a different rule, or none at all when boot reconciliation
				// closed a scope whose rule no longer exists, and the opener is the
				// one a reader can still go and look at.
				RuleRef: t.ruleRef,
				From:    t.ts,
			})
			open = len(intervals) - 1
		case scopeStopped:
			if open < 0 {
				continue
			}
			stop := t.ts
			intervals[open].To = &stop
			open = -1
		}
	}
	return intervals
}

// overlapsWindow reports whether an interval intersects the query's window.
//
// An interval that merely overlaps is returned whole rather than clipped to the
// question. Trimming it would make a scope opened last year and still open look
// as though it opened when the window did, which is a false statement about when
// the recorder started watching — and the one thing a coverage answer exists to
// be right about.
func overlapsWindow(interval query.ScopeInterval, from, to time.Time) bool {
	if !to.IsZero() && interval.From.After(to) {
		return false
	}
	if !from.IsZero() && interval.To != nil && interval.To.Before(from) {
		return false
	}
	return true
}
