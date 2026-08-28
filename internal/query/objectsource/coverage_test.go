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
	"slices"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// scopeHistory is one scope watched, dropped, and picked up again long afterwards,
// beside a still-open scope over another kind.
//
// The transitions are days apart on purpose: the scope log is partitioned by date, so
// this is a log spread over several partitions rather than one, which is what a
// coverage read has to gather before it can pair anything.
func scopeHistory() conformance.History {
	start := testEpoch().Add(-72 * time.Hour)
	return conformance.History{Scopes: []conformance.ScopeTransition{
		{Action: conformance.ScopeStarted, APIGroup: "apps", Kind: "Deployment",
			Namespace: "payments", RuleRef: "streamrule/payments/deployments", TS: start},
		{Action: conformance.ScopeStopped, APIGroup: "apps", Kind: "Deployment",
			Namespace: "payments", RuleRef: "streamrule/payments/deployments",
			TS: start.Add(24 * time.Hour)},
		{Action: conformance.ScopeStarted, APIGroup: "apps", Kind: "Deployment",
			Namespace: "payments", RuleRef: "streamrule/payments/deployments-v2",
			TS: start.Add(48 * time.Hour)},
		{Action: conformance.ScopeStarted, APIGroup: "", Kind: "ConfigMap",
			Namespace: "", RuleRef: "clusterstreamrule/configmaps", TS: start.Add(time.Hour)},
	}}
}

// coverageQuery asks about the fixture's twice-watched scope over an unbounded window.
func coverageQuery() query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: conformance.FixtureClusterID,
		APIGroup:  "apps",
		Kind:      "Deployment",
		Namespace: "payments",
	}
}

// TestCoverageReadsOnlyTheScopeLog: a coverage read touches the scopes prefix and
// nothing else.
//
// The archive holds the records and the scope log under one prefix, with different line
// shapes, so a read that globbed both would hand record lines to a scope decoder — and
// on a busy cluster it would pay for the whole archive to answer a question about a
// handful of tiny objects.
func TestCoverageReadsOnlyTheScopeLog(t *testing.T) {
	t.Parallel()

	engine, spy := engineOver(t, scopeHistory(), Options{Prefix: "audit"})

	if _, err := engine.Coverage(context.Background(), coverageQuery()); err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	want := []string{scopesRoot("audit")}
	if got := spy.listed(); !slices.Equal(got, want) {
		t.Errorf("a coverage read listed:\n got: %v\nwant: %v", got, want)
	}
	for _, key := range spy.opened() {
		if !containsAll(key, scopesPartition) {
			t.Errorf("a coverage read fetched %q, which is not a scope object", key)
		}
	}
}

// TestCoverageNeedsNoTimeBound: the whole scope log is read, and an interval that
// opened before the window is returned whole.
//
// This is the one question this engine answers unbounded, and it has to be. Pairing
// needs the transition that *opened* an interval, and a scope opened last year and
// never closed covers this morning: a scan clipped to the window would find a Stopped
// with no Started, or neither, and report "nobody was watching" about a scope that was
// watching the whole time — the exact inversion Coverage exists to prevent.
func TestCoverageNeedsNoTimeBound(t *testing.T) {
	t.Parallel()

	history := scopeHistory()
	engine, _ := engineOver(t, history, Options{Prefix: "audit"})
	start := history.Scopes[0].TS

	got, err := engine.Coverage(context.Background(), coverageQuery())
	if err != nil {
		t.Fatalf("an unbounded Coverage was refused: %v; the scope log is the archive's smallest "+
			"partition and pairing needs all of it", err)
	}
	if len(got) != 2 {
		t.Fatalf("Coverage returned %d intervals, want 2 — the scope was watched, dropped, and picked "+
			"up again: %v", len(got), got)
	}
	switch {
	case got[0].To == nil:
		t.Errorf("the first interval is open, but the log holds a Stopped for it")
	case !got[0].From.Equal(start):
		t.Errorf("the first interval opens at %s, want %s", formatInstant(got[0].From),
			formatInstant(start))
	case got[1].To != nil:
		t.Errorf("the second interval was closed at %s, but its Started has no matching Stopped. On "+
			"this archive tier an unmatched Started is permanent — nothing reconciles it — and closing "+
			"it would turn \"this epoch's end is unrecorded\" into \"nobody is watching\"",
			formatInstant(*got[1].To))
	case got[1].RuleRef != "streamrule/payments/deployments-v2":
		t.Errorf("the second interval names the rule %q, want the one that opened it", got[1].RuleRef)
	}

	// A window that starts after the first interval closed still reports that interval
	// with its real bounds, unclipped: trimming would make a scope opened three days ago
	// look as though it opened when the window did.
	windowed := coverageQuery()
	windowed.From = start.Add(12 * time.Hour)
	windowed.To = testEpoch()
	clipped, err := engine.Coverage(context.Background(), windowed)
	if err != nil {
		t.Fatalf("Coverage over a window: %v", err)
	}
	if len(clipped) != 2 || !clipped[0].From.Equal(start) {
		t.Errorf("a windowed coverage read returned %v; an overlapping interval is returned whole, "+
			"with the instant the scope really opened", clipped)
	}
}

// TestCoverageAnswersTheCoveringQuestion: a query for one namespace matches the
// all-namespaces scope, and a kind nobody watched is an empty result rather than a
// failure.
//
// Both are the difference between "nothing changed" and "nobody was looking", which is
// the whole of Invariant 9. A cluster-wide rule genuinely was watching the object in
// that namespace, and the interval comes back reporting the empty namespace it was
// really recorded under — which is not a normalization to tidy away, it is which scope
// did the covering.
func TestCoverageAnswersTheCoveringQuestion(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, scopeHistory(), Options{Prefix: "audit"})

	covering := query.ScopeQuery{
		ClusterID: conformance.FixtureClusterID,
		APIGroup:  "",
		Kind:      "ConfigMap",
		Namespace: "kube-system",
	}
	got, err := engine.Coverage(context.Background(), covering)
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("coverage for a namespace watched by a cluster-wide rule returned %d intervals, "+
			"want 1: %v", len(got), got)
	}
	if got[0].Namespace != "" {
		t.Errorf("the covering interval reports namespace %q, want the empty all-namespaces scope it "+
			"was recorded under; rewriting it would hide which scope did the covering", got[0].Namespace)
	}

	unwatched := coverageQuery()
	unwatched.Kind = "StatefulSet"
	none, err := engine.Coverage(context.Background(), unwatched)
	if err != nil {
		t.Fatalf("Coverage for an unwatched kind: %v; \"nothing was watching\" is a result, not a "+
			"failure", err)
	}
	if len(none) != 0 {
		t.Errorf("coverage for a kind the fixture never watches returned %v", none)
	}
}

// TestCoverageFiltersByCluster: one archive may hold two clusters' scope logs, and a
// query about one must not be answered from the other's.
//
// The scope log is partitioned by date alone, outside cluster_id=, so the cluster is a
// field of the line rather than a segment of the key: there is no prefix to prune with
// and the filter is the only thing separating them.
func TestCoverageFiltersByCluster(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, scopeHistory(), Options{Prefix: "audit"})

	other := coverageQuery()
	other.ClusterID = "some-other-cluster"
	got, err := engine.Coverage(context.Background(), other)
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("coverage for another cluster returned %v from this cluster's scope log", got)
	}
}
