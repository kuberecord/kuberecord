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
	"io"

	"github.com/kuberecord/kuberecord/internal/query"
)

// scopeAccumulator is what one scope object contributed to a coverage read.
type scopeAccumulator struct {
	changes []query.ScopeChange
}

// scopeScan is the per-line decision a coverage read makes.
type scopeScan struct {
	q query.ScopeQuery
}

// decode reads one scope object and keeps the transitions the query is about.
func (s scopeScan) decode(acc *scopeAccumulator, body io.Reader) error {
	return decodeScopeFrame(body, func(line *scopeLine) error {
		if s.matches(line) {
			acc.changes = append(acc.changes, line.scopeChange())
		}
		return nil
	})
}

// matches applies the query's predicates to one transition.
//
// The namespace predicate has ScopeQuery's *covering* reading rather than the scope
// log's own: a query for one namespace matches that namespace's own scope and the
// all-namespaces scope, because a cluster-wide rule genuinely was watching the
// object and answering "never observed" about it would be false. The interval that
// comes back therefore reports the empty namespace it was really recorded under,
// which is not a normalization to tidy away — it is which scope did the covering.
func (s scopeScan) matches(line *scopeLine) bool {
	switch {
	case line.ClusterID != s.q.ClusterID:
		return false
	case s.q.APIGroup != "" && line.APIGroup != s.q.APIGroup:
		return false
	case s.q.Kind != "" && line.Kind != s.q.Kind:
		return false
	case s.q.Namespace != "" && line.Namespace != s.q.Namespace && line.Namespace != "":
		return false
	default:
		return true
	}
}

// Coverage reports when the scopes matching a query were actually being watched.
//
// This is the mechanism behind Invariant 9, and on this backend it carries more
// weight than on any other. An archive of this format holds no deletions at all
// (D12) and no record of the periods the recorder was down, so "there are no changes
// for this Deployment after Tuesday" is genuinely ambiguous from the records alone:
// it could mean nothing changed, or that nobody was watching. These objects are what
// disambiguate it, from inside the archive, with no operator and no database to ask.
//
// # Why this read carries no time bound
//
// Every other question this engine answers demands a window (Capabilities.
// TimeBoundRequired), and this one deliberately does not: the whole scope log is
// read and the window is applied afterwards as the overlap filter. That is not an
// oversight in either direction.
//
// It is necessary because pairing needs the transition that *opened* an interval,
// and a scope opened last year and never closed covers this morning. A scan clipped
// to the window would find its Stopped without its Started — or neither — and report
// "nobody was watching" about a scope that was watching the whole time, which is the
// exact inversion this call exists to prevent.
//
// It is affordable because the scope log is the archive's smallest thing by
// construction: partitioned by date alone, holding a handful of tiny objects per day,
// written at rule-lifecycle rate rather than at cluster-change rate.
//
// # What an unmatched Started means here
//
// It means watching began and this epoch's end is unrecorded — not "still watching".
// This archive tier reconciles nothing on boot, so a recorder that died with scopes
// open leaves their Started transitions unmatched permanently. The interval is
// reported open because that is what the log says; a reader must not upgrade "open in
// the log" into "open in the cluster".
func (e *Engine) Coverage(ctx context.Context, q query.ScopeQuery) ([]query.ScopeInterval, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	e.beginScan()

	scan := scopeScan{q: q}
	var transitions []query.ScopeChange
	err := scanPartitions(ctx, e, []string{scopesRoot(e.prefix)}, scan.decode,
		func(acc *scopeAccumulator) { transitions = append(transitions, acc.changes...) })
	if err != nil {
		// A partial scope log is worse than none: it is the input to the answer that
		// says whether a silence is meaningful, and half of it would report an outage
		// that did not happen.
		return nil, fmt.Errorf("reading the scope log of cluster %q: %w", q.ClusterID, err)
	}
	// The pairing is the contract's, not this package's: its interesting cases are
	// quiet ones, and two backends reading the same log differently would disagree
	// about when the recorder was watching while both looked plausible.
	return query.CoverageOf(transitions, q.From, q.To), nil
}
