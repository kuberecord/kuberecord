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

package watch

import (
	"slices"
	"testing"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/plan"
)

// podsInNamespace is the informer key these tests key most of their interests on.
func podsInNamespace(namespace string) informerKey {
	return informerKey{GVR: podGVR, Namespace: namespace}
}

// interestFor builds one interest the way reconcilePool does, failing the test if
// the selectors do not parse (which is what the dedicated error case asserts
// instead).
func interestFor(t *testing.T, sink, namespace string, selectors, ruleKeys []string) *scopeInterest {
	t.Helper()
	key := plan.TargetKey{Sink: sink, GVK: podGVK, Namespace: namespace}
	in, err := newScopeInterest(key, podsInNamespace(namespace), selectors, ruleKeys)
	if err != nil {
		t.Fatalf("newScopeInterest(%q, %q, %v): %v", sink, namespace, selectors, err)
	}
	return in
}

// TestNewScopeInterestSelectors covers how a target's merged selector set becomes
// a matcher: the union semantics, the "select everything" short-circuit, and the
// refusal to accept a selector the registry should have canonicalized already.
func TestNewScopeInterestSelectors(t *testing.T) {
	cases := []struct {
		name      string
		selectors []string
		wantErr   bool
		// matches maps a label set to whether it must be in scope.
		matches map[string]struct {
			labels map[string]string
			want   bool
		}
	}{
		{
			name:      "no selectors matches everything",
			selectors: nil,
			matches: map[string]struct {
				labels map[string]string
				want   bool
			}{
				"unlabelled": {labels: nil, want: true},
				"labelled":   {labels: map[string]string{"app": "web"}, want: true},
			},
		},
		{
			name:      "the empty selector makes the rest redundant",
			selectors: []string{"", "app=web"},
			matches: map[string]struct {
				labels map[string]string
				want   bool
			}{
				"non-matching label is still in scope": {labels: map[string]string{"app": "db"}, want: true},
			},
		},
		{
			name:      "distinct selectors are a union, not an intersection",
			selectors: []string{"app=web", "tier=cache"},
			matches: map[string]struct {
				labels map[string]string
				want   bool
			}{
				"matches the first":  {labels: map[string]string{"app": "web"}, want: true},
				"matches the second": {labels: map[string]string{"tier": "cache"}, want: true},
				"matches both":       {labels: map[string]string{"app": "web", "tier": "cache"}, want: true},
				"matches neither":    {labels: map[string]string{"app": "db"}, want: false},
				"unlabelled":         {labels: nil, want: false},
			},
		},
		{
			name:      "set-based selectors are honoured",
			selectors: []string{"env in (prod,staging)"},
			matches: map[string]struct {
				labels map[string]string
				want   bool
			}{
				"in the set":     {labels: map[string]string{"env": "prod"}, want: true},
				"not in the set": {labels: map[string]string{"env": "dev"}, want: false},
			},
		},
		{
			name:      "an unparseable selector is reported, not swallowed",
			selectors: []string{"!!!"},
			wantErr:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := plan.TargetKey{Sink: "sink-a", GVK: podGVK, Namespace: "ns-a"}
			in, err := newScopeInterest(key, podsInNamespace("ns-a"), tc.selectors, []string{"rule-1"})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected a selector parse error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for name, want := range tc.matches {
				if got := in.matches(want.labels); got != want.want {
					t.Errorf("%s: matches(%v) = %t, want %t", name, want.labels, got, want.want)
				}
			}
		})
	}
}

// TestScopeInterestDerivedIdentity pins the translation from a plan target to the
// data-plane identities the rest of the package keys on: the scope the pipeline
// evicts by, and the work key an event produces. Both must be version-agnostic
// (Invariant 7) even though the interest itself carries a versioned GVR.
func TestScopeInterestDerivedIdentity(t *testing.T) {
	key := plan.TargetKey{Sink: "sink-a", GVK: deploymentGVK, Namespace: "ns-a"}
	in, err := newScopeInterest(key, informerKey{GVR: deploymentGVR, Namespace: "ns-a"}, nil, []string{"rule-1"})
	if err != nil {
		t.Fatalf("newScopeInterest: %v", err)
	}

	wantScope := pipeline.ScopeKey{Group: "apps", Kind: "Deployment", Namespace: "ns-a"}
	if in.scope != wantScope {
		t.Errorf("scope = %+v, want %+v", in.scope, wantScope)
	}

	wantKey := pipeline.Key{Sink: "sink-a", Group: "apps", Kind: "Deployment", Namespace: "ns-a", Name: "web"}
	if got := in.keyFor("ns-a", "web"); got != wantKey {
		t.Errorf("keyFor = %+v, want %+v", got, wantKey)
	}

	wantIdentity := identityKey{Group: "apps", Kind: "Deployment", Namespace: "ns-a", Sink: "sink-a"}
	if got := in.identity(); got != wantIdentity {
		t.Errorf("identity = %+v, want %+v", got, wantIdentity)
	}
}

// TestScopeInterestMatchesEither covers the scope-exit rule: an update is in scope
// if the object matches *either* before or after, so that losing a label produces
// one final work item instead of silently freezing the sink's last-known state.
func TestScopeInterestMatchesEither(t *testing.T) {
	in := interestFor(t, "sink-a", "ns-a", []string{"app=web"}, []string{"rule-1"})

	cases := []struct {
		name     string
		current  map[string]string
		previous map[string]string
		want     bool
	}{
		{name: "still matching", current: map[string]string{"app": "web"}, want: true},
		{
			name:     "left the scope",
			current:  map[string]string{"app": "db"},
			previous: map[string]string{"app": "web"},
			want:     true,
		},
		{
			name:     "entered the scope",
			current:  map[string]string{"app": "web"},
			previous: map[string]string{"app": "db"},
			want:     true,
		},
		{
			name:     "never in scope",
			current:  map[string]string{"app": "db"},
			previous: map[string]string{"app": "db"},
			want:     false,
		},
		{name: "not in scope, no previous", current: map[string]string{"app": "db"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := in.matchesEither(tc.current, tc.previous); got != tc.want {
				t.Errorf("matchesEither(%v, %v) = %t, want %t", tc.current, tc.previous, got, tc.want)
			}
		})
	}
}

// TestInterestTableReplaceReportsRemovals is the table's core contract: installing
// a new desired set reports exactly the interests that vanished, and reports
// nothing for interests that merely changed.
//
// The "changed" case is the selector-edit path in miniature: same identity, new
// matcher, so nothing may be evicted and no scope transition may be reported —
// which is what keeps a selector edit from tearing down an informer.
func TestInterestTableReplaceReportsRemovals(t *testing.T) {
	table := newInterestTable()

	first := interestFor(t, "sink-a", "ns-a", nil, []string{"rule-1"})
	second := interestFor(t, "sink-b", "ns-a", nil, []string{"rule-2"})
	if removed := table.replace(map[interestID]*scopeInterest{
		first.id():  first,
		second.id(): second,
	}); len(removed) != 0 {
		t.Fatalf("first replace removed %d interests, want 0", len(removed))
	}
	if table.size() != 2 {
		t.Fatalf("table size = %d, want 2", table.size())
	}

	// Same two identities, but sink-a's selector changed and sink-b is gone.
	narrowed := interestFor(t, "sink-a", "ns-a", []string{"app=web"}, []string{"rule-1"})
	removed := table.replace(map[interestID]*scopeInterest{narrowed.id(): narrowed})
	if len(removed) != 1 {
		t.Fatalf("second replace removed %d interests, want 1: %+v", len(removed), removed)
	}
	if removed[0].sink != "sink-b" {
		t.Errorf("removed sink = %q, want %q", removed[0].sink, "sink-b")
	}
	if got := table.interestsFor(podsInNamespace("ns-a")); len(got) != 1 || got[0] != narrowed {
		t.Errorf("interestsFor returned %+v, want only the narrowed interest", got)
	}
}

// TestInterestTableFanOutIsSortedBySink pins the fan-out order so a test asserting
// "one event, two keys" can name the keys in a stable order.
func TestInterestTableFanOutIsSortedBySink(t *testing.T) {
	table := newInterestTable()
	zeta := interestFor(t, "zeta", "ns-a", nil, []string{"rule-1"})
	alpha := interestFor(t, "alpha", "ns-a", nil, []string{"rule-2"})
	table.replace(map[interestID]*scopeInterest{zeta.id(): zeta, alpha.id(): alpha})

	interests := table.interestsFor(podsInNamespace("ns-a"))
	sinks := make([]string, 0, len(interests))
	for _, in := range interests {
		sinks = append(sinks, in.sink)
	}
	if !slices.Equal(sinks, []string{"alpha", "zeta"}) {
		t.Errorf("fan-out order = %v, want [alpha zeta]", sinks)
	}
}

// TestInterestTableLookupIdentity covers the pipeline-facing lookup: an exact
// namespace match, the cluster-wide fallback that a ClusterStreamRule produces,
// the per-sink isolation that makes scopeActive a per-(sink, scope) answer, and the
// empty result that means "this scope is not being watched".
func TestInterestTableLookupIdentity(t *testing.T) {
	nsScoped := interestFor(t, "sink-a", "ns-a", nil, []string{"rule-ns"})
	clusterWide := interestFor(t, "sink-a", "", nil, []string{"rule-cluster"})
	otherSink := interestFor(t, "sink-b", "ns-a", nil, []string{"rule-other"})

	table := newInterestTable()
	table.replace(map[interestID]*scopeInterest{
		nsScoped.id():    nsScoped,
		clusterWide.id(): clusterWide,
		otherSink.id():   otherSink,
	})

	podKey := func(sink, namespace string) pipeline.Key {
		return pipeline.Key{Sink: sink, Kind: "Pod", Namespace: namespace, Name: "web"}
	}

	cases := []struct {
		name string
		ref  pipeline.Key
		// want lists the namespaces of the expected interests, most specific first.
		want []string
	}{
		{
			name: "exact namespace first, then the cluster-wide scope",
			ref:  podKey("sink-a", "ns-a"),
			want: []string{"ns-a", ""},
		},
		{
			name: "a namespace only the cluster-wide scope covers",
			ref:  podKey("sink-a", "ns-b"),
			want: []string{""},
		},
		{
			name: "another sink sees only its own interest",
			ref:  podKey("sink-b", "ns-a"),
			want: []string{"ns-a"},
		},
		{
			name: "an unknown sink sees nothing",
			ref:  podKey("sink-c", "ns-a"),
			want: nil,
		},
		{
			name: "a different kind sees nothing",
			ref:  pipeline.Key{Sink: "sink-a", Group: "apps", Kind: "Deployment", Namespace: "ns-a", Name: "web"},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := table.lookupIdentity(tc.ref)
			namespaces := make([]string, 0, len(got))
			for _, in := range got {
				namespaces = append(namespaces, in.informer.Namespace)
			}
			if !slices.Equal(namespaces, tc.want) {
				t.Errorf("lookupIdentity(%+v) namespaces = %v, want %v", tc.ref, namespaces, tc.want)
			}
		})
	}
}

// TestInterestTableLookupIdentityClusterScoped guards the degenerate case: for a
// cluster-scoped object the exact-namespace lookup and the cluster-wide fallback
// are the same lookup, and the candidate must not be returned twice.
func TestInterestTableLookupIdentityClusterScoped(t *testing.T) {
	key := plan.TargetKey{Sink: "sink-a", GVK: namespaceGVK, Namespace: ""}
	in, err := newScopeInterest(key, informerKey{GVR: namespaceGVR}, nil, []string{"rule-1"})
	if err != nil {
		t.Fatalf("newScopeInterest: %v", err)
	}
	table := newInterestTable()
	table.replace(map[interestID]*scopeInterest{in.id(): in})

	got := table.lookupIdentity(pipeline.Key{Sink: "sink-a", Kind: "Namespace", Name: "kube-system"})
	if len(got) != 1 {
		t.Fatalf("lookupIdentity returned %d interests, want 1", len(got))
	}
}

// TestInterestTableConcurrentAccess is the -race guard for the table: the write
// side (a pool diff) and both read sides (event fan-out, pipeline lookups) run
// concurrently in production, and the read paths deliberately publish the table's
// own slices rather than copies.
func TestInterestTableConcurrentAccess(t *testing.T) {
	table := newInterestTable()
	ref := pipeline.Key{Sink: "sink-a", Kind: "Pod", Namespace: "ns-a", Name: "web"}

	// Both interests are built on the test goroutine: interestFor calls Fatalf,
	// which is only legal there.
	alternating := []*scopeInterest{
		interestFor(t, "sink-a", "ns-a", nil, []string{"rule-1"}),
		interestFor(t, "sink-a", "ns-b", nil, []string{"rule-1"}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			in := alternating[i%2]
			table.replace(map[interestID]*scopeInterest{in.id(): in})
		}
	}()

	for range 200 {
		for _, in := range table.interestsFor(podsInNamespace("ns-a")) {
			_ = in.matches(map[string]string{"app": "web"})
		}
		for _, in := range table.lookupIdentity(ref) {
			_ = in.keyFor("ns-a", "web")
		}
	}
	<-done
}
