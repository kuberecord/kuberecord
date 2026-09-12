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
	"sync"
	"testing"
)

// coreEvent builds a core v1 Event of the shape client-go's legacy tools/record
// recorder writes: the subject under `involvedObject`, the emitter under
// `source.component`, and the modern reporting fields absent.
func coreEvent(fields map[string]any) map[string]any {
	event := map[string]any{
		"type":   "Warning",
		"reason": "BackOff",
		"source": map[string]any{"component": "kubelet", "host": "node-7"},
		"involvedObject": map[string]any{
			"kind":      "Pod",
			"namespace": "production",
			"name":      "checkout-api-69dfc5f67d-ldw5j",
		},
	}
	for key, value := range fields {
		if value == nil {
			delete(event, key)
			continue
		}
		event[key] = value
	}
	return event
}

// modernEvent builds an events.k8s.io/v1 Event of the shape client-go's
// tools/events recorder writes: the subject under `regarding`, the emitter under
// `reportingController`, and `deprecatedSource` absent.
func modernEvent(fields map[string]any) map[string]any {
	event := map[string]any{
		"type":                "Warning",
		"reason":              "FailedScheduling",
		"reportingController": "default-scheduler",
		"regarding": map[string]any{
			"kind":      "Pod",
			"namespace": "production",
			"name":      "checkout-api-69dfc5f67d-ldw5j",
		},
	}
	for key, value := range fields {
		if value == nil {
			delete(event, key)
			continue
		}
		event[key] = value
	}
	return event
}

// mustCanonical renders one filter, failing the test rather than returning an
// error nobody would read.
func mustCanonical(t *testing.T, spec EventFilterSpec) string {
	t.Helper()
	canonical, err := CanonicalEventFilter(spec)
	if err != nil {
		t.Fatalf("CanonicalEventFilter(%+v): %v", spec, err)
	}
	return canonical
}

// mustCompile compiles one filter into the matcher, through the canonical form
// the data plane actually receives — never straight from the spec, so every test
// below exercises the round-trip a rule's filter really makes.
func mustCompile(t *testing.T, specs ...EventFilterSpec) *eventMatcher {
	t.Helper()
	canonical := make([]string, 0, len(specs))
	for _, spec := range specs {
		canonical = append(canonical, mustCanonical(t, spec))
	}
	m, err := compileEventFilters(canonical)
	if err != nil {
		t.Fatalf("compileEventFilters(%q): %v", canonical, err)
	}
	return m
}

// TestCanonicalEventFilter covers the property the registry's ref-counting rests
// on: two rules that express the same filter must produce the same bytes, so the
// merged set holds one entry rather than two and a reordered list is not a change.
func TestCanonicalEventFilter(t *testing.T) {
	tests := []struct {
		name string
		spec EventFilterSpec
		want string
	}{
		{
			name: "an empty filter is the same sentinel an empty selector is",
			spec: EventFilterSpec{},
			want: "",
		},
		{
			name: "a filter of empty lists constrains nothing either",
			spec: EventFilterSpec{Types: []string{}, Reasons: []string{}},
			want: "",
		},
		{
			name: "entries are sorted",
			spec: EventFilterSpec{Reasons: []string{"Unhealthy", "BackOff", "FailedScheduling"}},
			want: `{"reasons":["BackOff","FailedScheduling","Unhealthy"]}`,
		},
		{
			name: "duplicates collapse",
			spec: EventFilterSpec{Reasons: []string{"BackOff", "BackOff"}},
			want: `{"reasons":["BackOff"]}`,
		},
		{
			name: "an empty entry is dropped, never compiled into a matcher for absence",
			spec: EventFilterSpec{Reasons: []string{"", "BackOff", ""}},
			want: `{"reasons":["BackOff"]}`,
		},
		{
			name: "an axis left with nothing but empty entries constrains nothing",
			spec: EventFilterSpec{Reasons: []string{"", ""}},
			want: "",
		},
		{
			name: "every axis renders in a fixed order",
			spec: EventFilterSpec{
				SubjectNames:     []string{"postgres-0"},
				SubjectKinds:     []string{"Pod"},
				SourceComponents: []string{"kubelet"},
				ExcludeReasons:   []string{"Pulled"},
				Reasons:          []string{"BackOff"},
				Types:            []string{"Warning"},
			},
			want: `{"types":["Warning"],"reasons":["BackOff"],"excludeReasons":["Pulled"],` +
				`"sourceComponents":["kubelet"],"subjectKinds":["Pod"],"subjectNames":["postgres-0"]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustCanonical(t, tc.spec); got != tc.want {
				t.Errorf("CanonicalEventFilter() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCanonicalEventFilterDoesNotMutateItsArgument guards the one way this
// function could damage something outside itself: the slices it is handed belong
// to the CR the reconciler is holding, and sorting one in place would reorder the
// object a later status write patches.
func TestCanonicalEventFilterDoesNotMutateItsArgument(t *testing.T) {
	reasons := []string{"Unhealthy", "BackOff"}
	if _, err := CanonicalEventFilter(EventFilterSpec{Reasons: reasons}); err != nil {
		t.Fatalf("CanonicalEventFilter: %v", err)
	}
	if reasons[0] != "Unhealthy" || reasons[1] != "BackOff" {
		t.Errorf("the caller's slice was reordered to %v", reasons)
	}
}

// TestCompileEventFiltersRejectsMalformed covers the anomaly path. These bytes
// were written by CanonicalEventFilter, so a decode failure means the two tiers
// disagree — the target must degrade (Invariant 5), never fall back to recording
// the whole stream its author asked to narrow.
func TestCompileEventFiltersRejectsMalformed(t *testing.T) {
	if _, err := compileEventFilters([]string{`{"types":`}); err == nil {
		t.Fatal("expected a decode error for a truncated filter, got nil")
	}
}

// TestEventMatcherFields is the per-axis acceptance criterion: each field in
// isolation, OR within a list, AND across fields, and an Event missing a field
// the filter names failing to match — absent is not empty.
func TestEventMatcherFields(t *testing.T) {
	tests := []struct {
		name  string
		spec  EventFilterSpec
		event map[string]any
		want  bool
	}{
		{
			name:  "an empty filter matches everything",
			spec:  EventFilterSpec{},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "type matches",
			spec:  EventFilterSpec{Types: []string{"Warning"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "type does not match",
			spec:  EventFilterSpec{Types: []string{"Normal"}},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name:  "an absent type does not match a filter naming one",
			spec:  EventFilterSpec{Types: []string{"Warning"}},
			event: coreEvent(map[string]any{"type": nil}),
			want:  false,
		},
		{
			name:  "a type of the wrong JSON type is absence, not a match",
			spec:  EventFilterSpec{Types: []string{"Warning"}},
			event: coreEvent(map[string]any{"type": int64(7)}),
			want:  false,
		},
		{
			name:  "reason matches",
			spec:  EventFilterSpec{Reasons: []string{"BackOff"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "a list is an OR: the second entry matches",
			spec:  EventFilterSpec{Reasons: []string{"FailedScheduling", "BackOff"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "a list is an OR: no entry matches",
			spec:  EventFilterSpec{Reasons: []string{"FailedScheduling", "Unhealthy"}},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name:  "an absent reason does not match a filter naming one",
			spec:  EventFilterSpec{Reasons: []string{"BackOff"}},
			event: coreEvent(map[string]any{"reason": nil}),
			want:  false,
		},
		{
			name:  "excludeReasons drops a named reason",
			spec:  EventFilterSpec{ExcludeReasons: []string{"Pulled", "BackOff"}},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name:  "excludeReasons keeps everything it does not name",
			spec:  EventFilterSpec{ExcludeReasons: []string{"Pulled", "Started"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name: "an Event carrying no reason is not excluded by a list of reasons",
			spec: EventFilterSpec{ExcludeReasons: []string{"Pulled"}},
			// Absence is not one of the named reasons, so the exclusion has
			// nothing to say about it — the mirror image of the include rule.
			event: coreEvent(map[string]any{"reason": nil}),
			want:  true,
		},
		{
			name:  "sourceComponents matches the legacy source.component",
			spec:  EventFilterSpec{SourceComponents: []string{"kubelet"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name: "sourceComponents matches core's reportingComponent",
			spec: EventFilterSpec{SourceComponents: []string{"kubernetes.io/kubelet"}},
			event: coreEvent(map[string]any{
				"source":             nil,
				"reportingComponent": "kubernetes.io/kubelet",
			}),
			want: true,
		},
		{
			name:  "sourceComponents matches events.k8s.io's reportingController",
			spec:  EventFilterSpec{SourceComponents: []string{"default-scheduler"}},
			event: modernEvent(nil),
			want:  true,
		},
		{
			name: "sourceComponents matches events.k8s.io's deprecatedSource.component",
			spec: EventFilterSpec{SourceComponents: []string{"kubelet"}},
			event: modernEvent(map[string]any{
				"reportingController": nil,
				"deprecatedSource":    map[string]any{"component": "kubelet"},
			}),
			want: true,
		},
		{
			name:  "sourceComponents does not match a component nobody emitted",
			spec:  EventFilterSpec{SourceComponents: []string{"deployment-controller"}},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name:  "an Event naming no component at all does not match",
			spec:  EventFilterSpec{SourceComponents: []string{"kubelet"}},
			event: coreEvent(map[string]any{"source": nil}),
			want:  false,
		},
		{
			name:  "subjectKinds reads involvedObject",
			spec:  EventFilterSpec{SubjectKinds: []string{"Pod"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "subjectKinds reads regarding",
			spec:  EventFilterSpec{SubjectKinds: []string{"Pod"}},
			event: modernEvent(nil),
			want:  true,
		},
		{
			name:  "subjectKinds does not match another Kind",
			spec:  EventFilterSpec{SubjectKinds: []string{"ReplicaSet"}},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name:  "subjectNames reads involvedObject",
			spec:  EventFilterSpec{SubjectNames: []string{"checkout-api-69dfc5f67d-ldw5j"}},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "subjectNames reads regarding",
			spec:  EventFilterSpec{SubjectNames: []string{"checkout-api-69dfc5f67d-ldw5j"}},
			event: modernEvent(nil),
			want:  true,
		},
		{
			name:  "an Event with no subject at all does not match a filter naming one",
			spec:  EventFilterSpec{SubjectNames: []string{"checkout-api-69dfc5f67d-ldw5j"}},
			event: coreEvent(map[string]any{"involvedObject": nil}),
			want:  false,
		},
		{
			name: "across fields is an AND: both satisfied",
			spec: EventFilterSpec{
				Types:        []string{"Warning"},
				Reasons:      []string{"BackOff"},
				SubjectKinds: []string{"Pod"},
			},
			event: coreEvent(nil),
			want:  true,
		},
		{
			name: "across fields is an AND: one unsatisfied fails the whole filter",
			spec: EventFilterSpec{
				Types:        []string{"Warning"},
				Reasons:      []string{"BackOff"},
				SubjectKinds: []string{"ReplicaSet"},
			},
			event: coreEvent(nil),
			want:  false,
		},
		{
			name: "an axis left empty constrains nothing next to one that does not",
			spec: EventFilterSpec{Types: []string{"Warning"}, Reasons: nil},
			// The reason is one no include list would name; the filter still
			// matches, because it named no reasons at all.
			event: coreEvent(map[string]any{"reason": "SomethingNobodyNamed"}),
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustCompile(t, tc.spec).matches(tc.event); got != tc.want {
				t.Errorf("matches() = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestEventMatcherUnionsContributingFilters covers the merge: two rules wanting
// one (sink, scope) are served one stream, so their filters add up. Honouring
// only their intersection would let one rule's existence silence the other's.
func TestEventMatcherUnionsContributingFilters(t *testing.T) {
	warnings := EventFilterSpec{Types: []string{"Warning"}}
	scheduler := EventFilterSpec{SourceComponents: []string{"default-scheduler"}}

	m := mustCompile(t, warnings, scheduler)

	cases := []struct {
		name  string
		event map[string]any
		want  bool
	}{
		{
			name:  "matched by the first contributor only",
			event: coreEvent(nil),
			want:  true,
		},
		{
			name:  "matched by the second contributor only",
			event: modernEvent(map[string]any{"type": "Normal"}),
			want:  true,
		},
		{
			name:  "matched by neither",
			event: coreEvent(map[string]any{"type": "Normal"}),
			want:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := m.matches(tc.event); got != tc.want {
				t.Errorf("matches() = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestEventMatcherUnfilteredContributorWidensTheUnion is the counterpart: one
// rule that named no eventFilter wants every Event, which makes every other
// filter in the merged set redundant — exactly as one rule with no label selector
// widens the merged selector set.
func TestEventMatcherUnfilteredContributorWidensTheUnion(t *testing.T) {
	m, err := compileEventFilters([]string{"", mustCanonical(t, EventFilterSpec{Types: []string{"Warning"}})})
	if err != nil {
		t.Fatalf("compileEventFilters: %v", err)
	}
	if !m.matchesAll() {
		t.Error("matchesAll() = false; an unfiltered contributor must widen the union to everything")
	}
	if !m.matches(coreEvent(map[string]any{"type": "Normal"})) {
		t.Error("a Normal Event was rejected by a union one contributor left unfiltered")
	}
}

// TestEventMatcherEmptySetMatchesEverything covers the shape a target assembled
// by hand carries. A merged set nobody contributed to must mean "record
// everything", never "record nothing" — the same direction the selector union
// takes, and the only one that cannot silence a stream by accident.
func TestEventMatcherEmptySetMatchesEverything(t *testing.T) {
	m, err := compileEventFilters(nil)
	if err != nil {
		t.Fatalf("compileEventFilters(nil): %v", err)
	}
	if !m.matchesAll() || !m.matches(coreEvent(nil)) {
		t.Error("an empty merged filter set did not match everything")
	}
}

// TestEventMatcherNilMatchesEverything covers the nil receiver, which is what an
// interest built by a test or a future caller without compiling a filter holds.
func TestEventMatcherNilMatchesEverything(t *testing.T) {
	var m *eventMatcher
	if !m.matchesAll() || !m.matches(coreEvent(nil)) {
		t.Error("a nil matcher did not match everything")
	}
}

// TestEventMatcherIsSafeForConcurrentUse is the -race guard. One compiled matcher
// is shared by every informer goroutine in the process — Events are the
// highest-volume kind, so this is the most concurrently-read structure the filter
// introduces — and it must be immutable in fact, not merely by convention.
func TestEventMatcherIsSafeForConcurrentUse(t *testing.T) {
	m := mustCompile(t,
		EventFilterSpec{Types: []string{"Warning"}, Reasons: []string{"BackOff"}},
		EventFilterSpec{SourceComponents: []string{"default-scheduler"}, SubjectKinds: []string{"Pod"}},
	)

	// Distinct event maps per goroutine: a shared one would make the race
	// detector's subject the test's own fixture rather than the matcher.
	const readers = 16
	const rounds = 200

	var wg sync.WaitGroup
	for i := range readers {
		wg.Go(func() {
			matching := coreEvent(nil)
			rejected := coreEvent(map[string]any{"type": "Normal", "reason": "Pulled"})
			for range rounds {
				if !m.matches(matching) {
					t.Errorf("reader %d: a matching Event was rejected", i)
					return
				}
				if m.matches(rejected) {
					t.Errorf("reader %d: a non-matching Event was accepted", i)
					return
				}
			}
		})
	}
	wg.Wait()
}
