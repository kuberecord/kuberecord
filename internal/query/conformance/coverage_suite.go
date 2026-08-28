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

package conformance

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

const (
	propCoverageOpen      = "Coverage/OpenInterval"
	propCoverageClosed    = "Coverage/ClosedIntervalsInOrder"
	propCoverageNamespace = "Coverage/CoveringNamespace"

	// unwatchedKind is a kind the fixture never watches, so a query for it produces
	// the "nothing was watching" answer — which is a result, not a failure.
	unwatchedKind = "StatefulSet"
)

// Coverage is the mechanism behind Invariant 9 and the reason an empty timeline is
// explicable at all. A caller that renders "no changes" without consulting it cannot
// tell an object that sat untouched from an object nobody was recording, and those
// two answers send an engineer at 02:47 in opposite directions.
//
// Like reconstruction, it is not capability-gated: there is no flag for "has no
// scope log", so a backend that lacked one could not declare it, and a gate must not
// certify an omission nobody wrote down.

// scopeQuery builds a bounded coverage query.
func scopeQuery(group, kind, namespace string) query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: FixtureClusterID,
		APIGroup:  group,
		Kind:      kind,
		Namespace: namespace,
		From:      windowFrom(),
		To:        windowTo(),
	}
}

// coverageOf asks for coverage and fails the property if the engine could not
// answer.
func coverageOf(t conformanceT, h Harness, q query.ScopeQuery) []query.ScopeInterval {
	t.Helper()
	got, err := h.Engine.Coverage(context.Background(), q)
	if err != nil {
		t.Fatalf("conformance: Coverage(%s/%s ns=%q) returned %v; the seeded scope log holds transitions "+
			"for it", q.APIGroup, q.Kind, q.Namespace, err)
	}
	return got
}

// describeInterval renders one interval for a failure message, spelling an open one
// as open rather than as a zero timestamp — the distinction the pointer exists for.
func describeInterval(iv query.ScopeInterval) string {
	to := "open"
	if iv.To != nil {
		to = iv.To.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("{%s/%s ns=%q from=%s to=%s}",
		iv.APIGroup, iv.Kind, iv.Namespace, iv.From.UTC().Format(time.RFC3339Nano), to)
}

// describeIntervals renders a whole coverage answer.
func describeIntervals(intervals []query.ScopeInterval) string {
	if len(intervals) == 0 {
		return "(no intervals)"
	}
	var b strings.Builder
	for i, iv := range intervals {
		fmt.Fprintf(&b, "\n  [%d] %s", i, describeInterval(iv))
	}
	return b.String()
}

// deploymentCoverage is the fixture's twice-watched scope, which every property here
// starts from.
func deploymentCoverage(t conformanceT, h Harness) []query.ScopeInterval {
	t.Helper()
	got := coverageOf(t, h, scopeQuery(fixtureGroup, fixtureKind, fixtureNS))
	if len(got) != 2 {
		t.Fatalf("conformance: coverage for %s/%s in %q came back as %d interval(s), want 2 — the scope "+
			"was watched, dropped, and picked up again by a second rule.%s",
			fixtureGroup, fixtureKind, fixtureNS, len(got), describeIntervals(got))
	}
	return got
}

// coverageOpenInterval: a scope still being watched comes back with a nil To.
//
// The pointer is the whole point of the field. A plain timestamp would force a reader
// to guess whether a zero value meant "still open" or "closed at the zero instant",
// and the end of an interval is the load-bearing fact here: it says the recorder
// stopped watching, and emphatically not that the objects in the scope were deleted.
func coverageOpenInterval(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, coverageHistory())

	got := deploymentCoverage(t, h)
	open := got[1]
	switch {
	case open.To != nil:
		t.Errorf("conformance: the newest interval was closed at %s, but its Started has no matching "+
			"Stopped in the scope log — the scope is still being watched. A wrongly closed interval is "+
			"the difference between \"nobody is watching this now\" and \"we are watching it and nothing "+
			"has happened\": %s", open.To.UTC().Format(time.RFC3339Nano), describeInterval(open))
	case !open.From.Equal(at(scopeSecondStart)):
		t.Errorf("conformance: the open interval starts at %s, want %s",
			open.From.UTC().Format(time.RFC3339Nano), at(scopeSecondStart).UTC().Format(time.RFC3339Nano))
	}
}

// coverageClosedIntervalsInOrder: closed intervals come back paired and oldest
// first, and a scope nobody ever watched comes back as an empty result rather than
// as a failure.
func coverageClosedIntervalsInOrder(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, coverageHistory())

	got := deploymentCoverage(t, h)
	for i := 1; i < len(got); i++ {
		if got[i].From.Before(got[i-1].From) {
			t.Errorf("conformance: intervals %d and %d are out of order (%s then %s); coverage is oldest "+
				"first%s", i-1, i, describeInterval(got[i-1]), describeInterval(got[i]),
				describeIntervals(got))
			return
		}
	}

	closed := got[0]
	switch {
	case closed.To == nil:
		t.Errorf("conformance: the first interval is open, but the scope log holds a Stopped for it at "+
			"%s: %s", at(scopeFirstStop).UTC().Format(time.RFC3339Nano), describeInterval(closed))
	case !closed.From.Equal(at(scopeFirstStart)) || !closed.To.Equal(at(scopeFirstStop)):
		t.Errorf("conformance: the closed interval is %s, want from=%s to=%s", describeInterval(closed),
			at(scopeFirstStart).UTC().Format(time.RFC3339Nano),
			at(scopeFirstStop).UTC().Format(time.RFC3339Nano))
	}
	for i, iv := range got {
		if iv.APIGroup != fixtureGroup || iv.Kind != fixtureKind || iv.Namespace != fixtureNS {
			t.Errorf("conformance: interval %d reports the scope %s, but the query asked about %s/%s in "+
				"%q", i, describeInterval(iv), fixtureGroup, fixtureKind, fixtureNS)
		}
	}

	// The other answer coverage has to be able to give. An engine with a scope log
	// that holds no matching interval returns an empty slice and a nil error: that
	// is the "nothing was watching" fact, and it is a result rather than a failure.
	if none := coverageOf(t, h, scopeQuery(fixtureGroup, unwatchedKind, fixtureNS)); len(none) != 0 {
		t.Errorf("conformance: coverage for %s/%s — a kind the fixture never watches — came back with "+
			"%d interval(s)%s", fixtureGroup, unwatchedKind, len(none), describeIntervals(none))
	}
}

// coverageCoveringNamespace: a query for one namespace matches the all-namespaces
// scope too, and the interval is reported with the scope's own namespace.
//
// This is not a normalization a backend may tidy away. A cluster-wide rule genuinely
// was watching the object in kube-system, so answering "never observed" about it
// would be false; and the interval reports the scope that actually covered the
// object, which is why its Namespace comes back empty in reply to a query for a
// specific one.
func coverageCoveringNamespace(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, coverageHistory())

	got := coverageOf(t, h, scopeQuery("", coveringKind, coveringNS))
	if len(got) != 1 {
		t.Fatalf("conformance: coverage for %s in %q came back as %d interval(s), want 1. The fixture "+
			"watches that kind with an all-namespaces scope, and a cluster-wide rule really was watching "+
			"objects in that namespace — reporting otherwise answers \"never observed\" about something "+
			"that was observed the whole time.%s", coveringKind, coveringNS, len(got), describeIntervals(got))
	}
	iv := got[0]
	switch {
	case iv.Namespace != "":
		t.Errorf("conformance: the covering interval reports namespace %q, want the empty all-namespaces "+
			"scope it was actually recorded under: %s. Rewriting it to the queried namespace would hide "+
			"which scope did the covering", iv.Namespace, describeInterval(iv))
	case iv.To != nil:
		t.Errorf("conformance: the covering interval was closed at %s, but its scope is still open: %s",
			iv.To.UTC().Format(time.RFC3339Nano), describeInterval(iv))
	case !iv.From.Equal(at(scopeWideStart)):
		t.Errorf("conformance: the covering interval starts at %s, want %s",
			iv.From.UTC().Format(time.RFC3339Nano), at(scopeWideStart).UTC().Format(time.RFC3339Nano))
	}
}
