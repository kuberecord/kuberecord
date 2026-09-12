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

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/plan"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// podsInNamespace is the informer key these tests key most of their interests on.
func podsInNamespace(namespace string) informerKey {
	return informerKey{GVR: podGVR, Namespace: namespace}
}

// interestFor builds one interest the way reconcilePool does, failing the test if
// the selectors do not parse (which is what the dedicated error case asserts
// instead).
func interestFor(t *testing.T, sinkID sink.ID, namespace string,
	selectors, ruleKeys []string) *scopeInterest {
	t.Helper()
	key := plan.TargetKey{Sink: sinkID, GVK: podGVK, Namespace: namespace}
	in, err := newScopeInterest(
		plan.TargetState{Key: key, RuleKeys: ruleKeys, Selectors: selectors},
		podsInNamespace(namespace))
	if err != nil {
		t.Fatalf("newScopeInterest(%s, %q, %v): %v", sinkID, namespace, selectors, err)
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
			key := plan.TargetKey{Sink: sinkA, GVK: podGVK, Namespace: "ns-a"}
			in, err := newScopeInterest(
				plan.TargetState{Key: key, RuleKeys: []string{"rule-1"}, Selectors: tc.selectors},
				podsInNamespace("ns-a"))
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
	key := plan.TargetKey{Sink: sinkA, GVK: deploymentGVK, Namespace: "ns-a"}
	in, err := newScopeInterest(
		plan.TargetState{Key: key, RuleKeys: []string{"rule-1"}},
		informerKey{GVR: deploymentGVR, Namespace: "ns-a"})
	if err != nil {
		t.Fatalf("newScopeInterest: %v", err)
	}

	wantScope := pipeline.ScopeKey{Group: "apps", Kind: "Deployment", Namespace: "ns-a"}
	if in.scope != wantScope {
		t.Errorf("scope = %+v, want %+v", in.scope, wantScope)
	}

	wantKey := pipeline.Key{Sink: sinkA, Group: "apps", Kind: "Deployment", Namespace: "ns-a", Name: "web"}
	if got := in.keyFor("ns-a", "web"); got != wantKey {
		t.Errorf("keyFor = %+v, want %+v", got, wantKey)
	}

	wantIdentity := identityKey{Group: "apps", Kind: "Deployment", Namespace: "ns-a", Sink: sinkA}
	if got := in.identity(); got != wantIdentity {
		t.Errorf("identity = %+v, want %+v", got, wantIdentity)
	}
}

// TestScopeInterestMatchesEither covers the scope-exit rule: an update is in scope
// if the object matches *either* before or after, so that losing a label produces
// one final work item instead of silently freezing the sink's last-known state.
func TestScopeInterestMatchesEither(t *testing.T) {
	in := interestFor(t, sinkA, "ns-a", []string{"app=web"}, []string{"rule-1"})

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

	first := interestFor(t, sinkA, "ns-a", nil, []string{"rule-1"})
	second := interestFor(t, sinkB, "ns-a", nil, []string{"rule-2"})
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
	narrowed := interestFor(t, sinkA, "ns-a", []string{"app=web"}, []string{"rule-1"})
	removed := table.replace(map[interestID]*scopeInterest{narrowed.id(): narrowed})
	if len(removed) != 1 {
		t.Fatalf("second replace removed %d interests, want 1: %+v", len(removed), removed)
	}
	if removed[0].sink != sinkB {
		t.Errorf("removed sink = %s, want %s", removed[0].sink, sinkB)
	}
	if got := table.interestsFor(podsInNamespace("ns-a")); len(got) != 1 || got[0] != narrowed {
		t.Errorf("interestsFor returned %+v, want only the narrowed interest", got)
	}
}

// TestInterestTableFanOutIsSortedBySink pins the fan-out order so a test asserting
// "one event, two keys" can name the keys in a stable order.
func TestInterestTableFanOutIsSortedBySink(t *testing.T) {
	table := newInterestTable()
	zeta := interestFor(t, clickHouseSink("zeta"), "ns-a", nil, []string{"rule-1"})
	alpha := interestFor(t, clickHouseSink("alpha"), "ns-a", nil, []string{"rule-2"})
	table.replace(map[interestID]*scopeInterest{zeta.id(): zeta, alpha.id(): alpha})

	interests := table.interestsFor(podsInNamespace("ns-a"))
	sinks := make([]sink.ID, 0, len(interests))
	for _, in := range interests {
		sinks = append(sinks, in.sink)
	}
	if !slices.Equal(sinks, []sink.ID{alpha.sink, zeta.sink}) {
		t.Errorf("fan-out order = %v, want [%s %s]", sinks, alpha.sink, zeta.sink)
	}
}

// TestInterestTableLookupIdentity covers the pipeline-facing lookup: an exact
// namespace match, the cluster-wide fallback that a ClusterStreamRule produces,
// the per-sink isolation that makes scopeActive a per-(sink, scope) answer, and the
// empty result that means "this scope is not being watched".
func TestInterestTableLookupIdentity(t *testing.T) {
	nsScoped := interestFor(t, sinkA, "ns-a", nil, []string{"rule-ns"})
	clusterWide := interestFor(t, sinkA, "", nil, []string{"rule-cluster"})
	otherSink := interestFor(t, sinkB, "ns-a", nil, []string{"rule-other"})

	table := newInterestTable()
	table.replace(map[interestID]*scopeInterest{
		nsScoped.id():    nsScoped,
		clusterWide.id(): clusterWide,
		otherSink.id():   otherSink,
	})

	podKey := func(sinkID sink.ID, namespace string) pipeline.Key {
		return pipeline.Key{Sink: sinkID, Kind: "Pod", Namespace: namespace, Name: "web"}
	}

	cases := []struct {
		name string
		ref  pipeline.Key
		// want lists the namespaces of the expected interests, most specific first.
		want []string
	}{
		{
			name: "exact namespace first, then the cluster-wide scope",
			ref:  podKey(sinkA, "ns-a"),
			want: []string{"ns-a", ""},
		},
		{
			name: "a namespace only the cluster-wide scope covers",
			ref:  podKey(sinkA, "ns-b"),
			want: []string{""},
		},
		{
			name: "another sink sees only its own interest",
			ref:  podKey(sinkB, "ns-a"),
			want: []string{"ns-a"},
		},
		{
			name: "an unknown sink sees nothing",
			ref:  podKey(clickHouseSink("sink-c"), "ns-a"),
			want: nil,
		},
		{
			name: "a different kind sees nothing",
			ref:  pipeline.Key{Sink: sinkA, Group: "apps", Kind: "Deployment", Namespace: "ns-a", Name: "web"},
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
	key := plan.TargetKey{Sink: sinkA, GVK: namespaceGVK, Namespace: ""}
	in, err := newScopeInterest(
		plan.TargetState{Key: key, RuleKeys: []string{"rule-1"}},
		informerKey{GVR: namespaceGVR})
	if err != nil {
		t.Fatalf("newScopeInterest: %v", err)
	}
	table := newInterestTable()
	table.replace(map[interestID]*scopeInterest{in.id(): in})

	got := table.lookupIdentity(pipeline.Key{Sink: sinkA, Kind: "Namespace", Name: "kube-system"})
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
	ref := pipeline.Key{Sink: sinkA, Kind: "Pod", Namespace: "ns-a", Name: "web"}

	// Both interests are built on the test goroutine: interestFor calls Fatalf,
	// which is only legal there.
	alternating := []*scopeInterest{
		interestFor(t, sinkA, "ns-a", nil, []string{"rule-1"}),
		interestFor(t, sinkA, "ns-b", nil, []string{"rule-1"}),
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

// TestNewScopeInterestCompilesRedaction covers the Task 3.3 translation step: a
// target's per-rule redaction sets become one compiled policy, merged as a union,
// and a set nobody can parse degrades the target rather than the pass.
func TestNewScopeInterestCompilesRedaction(t *testing.T) {
	builtin := pipeline.AnnotationRedactionPath(pipeline.LastAppliedConfigAnnotation)

	tests := []struct {
		name       string
		redactions []string
		wantPaths  []string
		wantErr    bool
	}{
		{
			name:       "no rule configured any",
			redactions: nil,
			// nil, i.e. the data plane's built-in scrubs and nothing else.
			wantPaths: nil,
		},
		{
			name:       "an empty set contributes nothing",
			redactions: []string{""},
			wantPaths:  nil,
		},
		{
			name:       "one rule's set",
			redactions: []string{"data.password\ndata.token"},
			wantPaths:  []string{builtin, "data.password", "data.token"},
		},
		{
			name: "two rules union, duplicates collapse",
			redactions: []string{
				"data.password",
				"data.password\nspec.containers[*].env[*].value",
			},
			wantPaths: []string{builtin, "data.password", "spec.containers[*].env[*].value"},
		},
		{
			name:       "a malformed path degrades the target",
			redactions: []string{"spec.containers[0].name"},
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := plan.TargetKey{Sink: sinkA, GVK: podGVK, Namespace: "ns-a"}
			in, err := newScopeInterest(
				plan.TargetState{Key: key, RuleKeys: []string{"rule-1"}, Redactions: tc.redactions},
				podsInNamespace("ns-a"))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected a redaction compile error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("newScopeInterest: %v", err)
			}
			if tc.wantPaths == nil {
				if in.redaction != nil {
					t.Errorf("redaction = %v, want nil (built-in scrubs only)", in.redaction.Paths())
				}
				return
			}
			want := slices.Clone(tc.wantPaths)
			slices.Sort(want)
			if got := in.redaction.Paths(); !slices.Equal(got, want) {
				t.Errorf("redaction paths = %v, want %v", got, want)
			}
		})
	}
}

// TestInterestTableSeparatesSinksOfDifferentKinds is the interest-map half of the
// collision typed sink identity exists to prevent (the pipeline's dedup half is
// TestPipelineSameNameDifferentKindsAreSeparateSinks).
//
// Two sinks named "default", one a ClickHouseSink and one an S3Sink, are both
// legal at once. Keyed on the name alone they would collide on one byIdentity
// entry, and the consequences run through both of this index's readers: Get would
// report the *other* sink's scope as active — writing rows for a sink no rule
// currently targets — and RedactionFor would hand out the other sink's policy,
// which is how an object one rule asked to have scrubbed reaches a backend
// unredacted.
func TestInterestTableSeparatesSinksOfDifferentKinds(t *testing.T) {
	clickhouse := clickHouseSink("default")
	s3 := sink.ID{Kind: "S3Sink", Name: "default"}

	// Only the ClickHouseSink is interested, and only in ns-a. Distinct redaction
	// paths so a leak between the two is visible rather than merely possible.
	inClickHouse, err := newScopeInterest(
		plan.TargetState{
			Key:        plan.TargetKey{Sink: clickhouse, GVK: podGVK, Namespace: "ns-a"},
			RuleKeys:   []string{"rule-ch"},
			Redactions: []string{"data.clickhouse-only"},
		},
		podsInNamespace("ns-a"))
	if err != nil {
		t.Fatalf("newScopeInterest(ClickHouseSink): %v", err)
	}
	inS3, err := newScopeInterest(
		plan.TargetState{
			Key:        plan.TargetKey{Sink: s3, GVK: podGVK, Namespace: "ns-b"},
			RuleKeys:   []string{"rule-s3"},
			Redactions: []string{"data.s3-only"},
		},
		podsInNamespace("ns-b"))
	if err != nil {
		t.Fatalf("newScopeInterest(S3Sink): %v", err)
	}

	m := &WatchManager{table: newInterestTable()}
	m.table.replace(map[interestID]*scopeInterest{
		inClickHouse.id(): inClickHouse,
		inS3.id():         inS3,
	})

	// Each sink's own scope resolves to its own interest, and neither answers for
	// the other's scope — which is exactly the scopeActive=false the pipeline drops
	// a work item on.
	cases := []struct {
		name string
		ref  pipeline.Key
		want *scopeInterest
	}{
		{"the ClickHouseSink's own scope", pipeline.Key{
			Sink: clickhouse, Kind: "Pod", Namespace: "ns-a", Name: "web"}, inClickHouse},
		{"the S3Sink's own scope", pipeline.Key{
			Sink: s3, Kind: "Pod", Namespace: "ns-b", Name: "web"}, inS3},
		{"the S3Sink does not see the ClickHouseSink's scope", pipeline.Key{
			Sink: s3, Kind: "Pod", Namespace: "ns-a", Name: "web"}, nil},
		{"the ClickHouseSink does not see the S3Sink's scope", pipeline.Key{
			Sink: clickhouse, Kind: "Pod", Namespace: "ns-b", Name: "web"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := m.table.lookupIdentity(tc.ref)
			if tc.want == nil {
				if len(got) != 0 {
					t.Fatalf("lookupIdentity(%s) answered with %d interest(s), want none: "+
						"a same-named sink of another kind shares this index entry", tc.ref, len(got))
				}
				// And the policy lookup fails closed for the same reason.
				if _, ok := m.RedactionFor(tc.ref); ok {
					t.Error("RedactionFor answered for a scope this sink has no interest in")
				}
				return
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("lookupIdentity(%s) = %v, want exactly the one interest for %s",
					tc.ref, got, tc.ref.Sink)
			}
			policy, ok := m.RedactionFor(tc.ref)
			if !ok {
				t.Fatal("RedactionFor reported no policy for a live scope")
			}
			if policy != tc.want.redaction {
				t.Errorf("redaction paths = %v, want this sink's own policy %v",
					policy.Paths(), tc.want.redaction.Paths())
			}
		})
	}

	// The scope-level lookup (ScopeDesired / ScopeSynced) has no cluster-wide
	// fallback and must separate the two just as strictly.
	scope := pipeline.ScopeKey{Kind: "Pod", Namespace: "ns-a"}
	if !m.ScopeDesired(clickhouse, scope) {
		t.Errorf("ScopeDesired(%s, %+v) = false, want true", clickhouse, scope)
	}
	if m.ScopeDesired(s3, scope) {
		t.Errorf("ScopeDesired(%s, %+v) = true; it borrowed %s's interest", s3, scope, clickhouse)
	}
}

// TestWatchManagerRedactionForUnionsInterests covers the lookup the pipeline
// makes per work item, in the ambiguous case that makes merging mandatory: one
// object answered for by both a namespaced interest and a cluster-wide one.
//
// Both land on the same hashCache entry, so there is one payload and one hash for
// the two of them. Picking either policy over the other would let one rule's
// existence unredact the other's stream; the union cannot.
func TestWatchManagerRedactionForUnionsInterests(t *testing.T) {
	namespaced := plan.TargetKey{Sink: sinkA, GVK: podGVK, Namespace: "ns-a"}
	clusterWide := plan.TargetKey{Sink: sinkA, GVK: podGVK, Namespace: ""}

	inNamespace, err := newScopeInterest(
		plan.TargetState{Key: namespaced, RuleKeys: []string{"rule-ns"}, Redactions: []string{"data.password"}},
		podsInNamespace("ns-a"))
	if err != nil {
		t.Fatalf("newScopeInterest(namespaced): %v", err)
	}
	inCluster, err := newScopeInterest(
		plan.TargetState{
			Key:        clusterWide,
			RuleKeys:   []string{"rule-cluster"},
			Redactions: []string{"spec.containers[*].env[*].value"},
		},
		podsInNamespace(""))
	if err != nil {
		t.Fatalf("newScopeInterest(cluster-wide): %v", err)
	}

	m := &WatchManager{table: newInterestTable()}
	m.table.replace(map[interestID]*scopeInterest{
		inNamespace.id(): inNamespace,
		inCluster.id():   inCluster,
	})

	ref := pipeline.Key{Sink: sinkA, Kind: "Pod", Namespace: "ns-a", Name: "web"}
	policy, ok := m.RedactionFor(ref)
	if !ok {
		t.Fatal("RedactionFor reported no policy for a live scope")
	}
	want := []string{
		pipeline.AnnotationRedactionPath(pipeline.LastAppliedConfigAnnotation),
		"data.password",
		"spec.containers[*].env[*].value",
	}
	slices.Sort(want)
	if got := policy.Paths(); !slices.Equal(got, want) {
		t.Errorf("merged paths = %v, want %v", got, want)
	}

	// A single interest answers with its own compiled policy, unmerged.
	single, ok := m.RedactionFor(pipeline.Key{Sink: sinkA, Kind: "Pod", Namespace: "ns-b", Name: "web"})
	if !ok {
		t.Fatal("RedactionFor reported no policy for the cluster-wide scope")
	}
	if single != inCluster.redaction {
		t.Errorf("paths = %v, want the cluster-wide interest's own policy %v",
			single.Paths(), inCluster.redaction.Paths())
	}

	// A sink nothing is registered for is the fail-closed answer the pipeline
	// refuses to write through.
	if _, ok := m.RedactionFor(pipeline.Key{Sink: sinkB, Kind: "Pod", Namespace: "ns-a", Name: "web"}); ok {
		t.Error("RedactionFor answered for a sink with no interests")
	}
}

// eventNamespace is the namespace every Event-filter interest in these tests is
// pinned to, and eventInformer the one informer serving them. One namespace
// rather than a parameter because the property under test is per-*interest*
// independence on a shared informer, and interests in two namespaces are served
// by two informers and never meet.
const eventNamespace = "ns-a"

var eventInformer = informerKey{GVR: eventGVR, Namespace: eventNamespace}

// eventInterestFor builds one interest in the Event informer carrying a merged
// filter set, the way reconcilePool does.
func eventInterestFor(t *testing.T, sinkID sink.ID, eventFilters, ruleKeys []string) *scopeInterest {
	t.Helper()
	key := plan.TargetKey{Sink: sinkID, GVK: eventGVK, Namespace: eventNamespace}
	in, err := newScopeInterest(
		plan.TargetState{Key: key, RuleKeys: ruleKeys, EventFilters: eventFilters},
		eventInformer)
	if err != nil {
		t.Fatalf("newScopeInterest(%s, %v): %v", sinkID, eventFilters, err)
	}
	return in
}

// TestNewScopeInterestEventFilters covers the compile half of Task 19.2: a
// target's merged filter set becomes one matcher when the interest is built, not
// one per Event, and a set that cannot be compiled degrades the target rather
// than widening it.
func TestNewScopeInterestEventFilters(t *testing.T) {
	warning := mustCanonical(t, EventFilterSpec{Types: []string{"Warning"}})

	tests := []struct {
		name         string
		eventFilters []string
		wantErr      bool
		wantAll      bool
		// matches maps an Event to whether it must be recorded.
		matches map[string]struct {
			event map[string]any
			want  bool
		}
	}{
		{
			name:         "no filters records every Event",
			eventFilters: nil,
			wantAll:      true,
		},
		{
			name:         "the empty filter makes the rest redundant",
			eventFilters: []string{"", warning},
			wantAll:      true,
		},
		{
			name:         "a filter narrows the stream",
			eventFilters: []string{warning},
			matches: map[string]struct {
				event map[string]any
				want  bool
			}{
				"a Warning is recorded": {event: coreEvent(nil), want: true},
				"a Normal is not": {
					event: coreEvent(map[string]any{"type": "Normal"}),
					want:  false,
				},
			},
		},
		{
			name: "a filter that cannot be compiled degrades the target",
			// Bytes CanonicalEventFilter could not have produced: the two tiers
			// disagree, which must not resolve into recording everything.
			eventFilters: []string{`{"types":`},
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := plan.TargetKey{Sink: sinkA, GVK: eventGVK, Namespace: eventNamespace}
			in, err := newScopeInterest(
				plan.TargetState{Key: key, RuleKeys: []string{"rule-1"}, EventFilters: tc.eventFilters},
				eventInformer)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an event filter compile error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("newScopeInterest: %v", err)
			}
			if got := in.recordsEveryEvent(); got != tc.wantAll {
				t.Errorf("recordsEveryEvent() = %t, want %t", got, tc.wantAll)
			}
			for name, want := range tc.matches {
				if got := in.matchesEvent(want.event); got != want.want {
					t.Errorf("%s: matchesEvent() = %t, want %t", name, got, want.want)
				}
			}
		})
	}
}

// TestNewScopeInterestCompilesTheFilterOnce is the Invariant 1 half of the
// acceptance criterion, asserted structurally rather than by timing: the matcher
// is a field of the interest, so it survives across events and cannot be rebuilt
// per notification. Two evaluations must see the same compiled instance.
func TestNewScopeInterestCompilesTheFilterOnce(t *testing.T) {
	in := eventInterestFor(t, sinkA,
		[]string{mustCanonical(t, EventFilterSpec{Types: []string{"Warning"}})}, []string{"rule-1"})

	first := in.events
	if first == nil {
		t.Fatal("newScopeInterest left the matcher nil; every Event would be compiled against nothing")
	}
	in.matchesEvent(coreEvent(nil))
	if in.events != first {
		t.Error("the matcher was replaced by an evaluation; it must be compiled once, at pool-diff time")
	}
}
