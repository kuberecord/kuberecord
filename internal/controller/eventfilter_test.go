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

package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/watch"
)

// TestCanonicalEventFilter covers the conversion the control plane owns: a CR's
// eventFilter becomes the one canonical string a plan.WatchTarget carries, and
// two rules expressing the same intent produce the same bytes — which is what
// makes the registry's ref-counting collapse them onto one entry.
func TestCanonicalEventFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *v1alpha1.EventFilter
		want   string
	}{
		{
			name:   "no filter records every Event in scope",
			filter: nil,
			want:   "",
		},
		{
			name:   "a filter of empty lists narrows nothing either",
			filter: &v1alpha1.EventFilter{},
			want:   "",
		},
		{
			name:   "the enum widens to the plain strings the data plane compares against",
			filter: &v1alpha1.EventFilter{Types: []v1alpha1.EventType{v1alpha1.EventTypeWarning}},
			want:   `{"types":["Warning"]}`,
		},
		{
			name: "both enum members render in sorted order",
			filter: &v1alpha1.EventFilter{
				Types: []v1alpha1.EventType{v1alpha1.EventTypeWarning, v1alpha1.EventTypeNormal},
			},
			want: `{"types":["Normal","Warning"]}`,
		},
		{
			name: "every axis is carried",
			filter: &v1alpha1.EventFilter{
				Types:            []v1alpha1.EventType{v1alpha1.EventTypeWarning},
				Reasons:          []string{"Unhealthy", "BackOff"},
				SourceComponents: []string{"kubelet"},
				SubjectKinds:     []string{"Pod"},
				SubjectNames:     []string{"postgres-0"},
			},
			want: `{"types":["Warning"],"reasons":["BackOff","Unhealthy"],` +
				`"sourceComponents":["kubelet"],"subjectKinds":["Pod"],"subjectNames":["postgres-0"]}`,
		},
		{
			name:   "excludeReasons is carried on its own axis",
			filter: &v1alpha1.EventFilter{ExcludeReasons: []string{"Started", "Pulled"}},
			want:   `{"excludeReasons":["Pulled","Started"]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalEventFilter(tc.filter)
			if err != nil {
				t.Fatalf("canonicalEventFilter: %v", err)
			}
			if got != tc.want {
				t.Errorf("canonicalEventFilter() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCanonicalEventFilterIsOrderInsensitive pins the property the data plane's
// churn depends on: a rule that merely reorders or repeats its lists produces
// byte-identical output, so the registry's target diff is empty and no informer,
// epoch or compiled matcher churns.
func TestCanonicalEventFilterIsOrderInsensitive(t *testing.T) {
	first, err := canonicalEventFilter(&v1alpha1.EventFilter{
		Reasons:      []string{"Unhealthy", "BackOff", "FailedScheduling"},
		SubjectKinds: []string{"ReplicaSet", "Pod"},
	})
	if err != nil {
		t.Fatalf("canonicalEventFilter(first): %v", err)
	}
	second, err := canonicalEventFilter(&v1alpha1.EventFilter{
		SubjectKinds: []string{"Pod", "ReplicaSet", "Pod"},
		Reasons:      []string{"FailedScheduling", "BackOff", "Unhealthy", "BackOff"},
	})
	if err != nil {
		t.Fatalf("canonicalEventFilter(second): %v", err)
	}
	if first != second {
		t.Errorf("two spellings of one filter rendered differently:\n%s\n%s", first, second)
	}
}

// TestEventFilterSpecMirrorsTheCRDType is the drift guard on a conversion the
// compiler cannot check. canonicalEventFilter copies field by field, so a new
// axis added to v1alpha1.EventFilter and not mapped here would be accepted at
// admission, reported in the rule's status, and silently ignored by the data
// plane — a filter that says it narrows and does not.
//
// It compares JSON tags rather than Go field names because the tag is the
// contract: the wire form watch.CanonicalEventFilter emits is what the data plane
// decodes, and the two declarations have to agree on it exactly.
func TestEventFilterSpecMirrorsTheCRDType(t *testing.T) {
	crd := jsonFieldNames(reflect.TypeFor[v1alpha1.EventFilter]())
	spec := jsonFieldNames(reflect.TypeFor[watch.EventFilterSpec]())

	if !reflect.DeepEqual(crd, spec) {
		t.Errorf("v1alpha1.EventFilter and watch.EventFilterSpec have drifted:\n"+
			"  CRD:  %v\n  spec: %v\n"+
			"add the missing axis to both, and map it in canonicalEventFilter", crd, spec)
	}
}

// jsonFieldNames lists a struct's JSON field names in declaration order, which
// for the two filter types is also the order they serialize in.
func jsonFieldNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		tag := t.Field(i).Tag.Get("json")
		names = append(names, strings.Split(tag, ",")[0])
	}
	return names
}

// TestRuleEventFilterReachesTheRegistry is the end of the control-plane chain, the
// Event-filter counterpart of TestRuleRedactionReachesTheRegistry: a rule's
// eventFilter arrives in the desired-state registry as one canonical string on
// every target that entry contributes, which is the only thing that makes the CRD
// field more than documentation.
func TestRuleEventFilterReachesTheRegistry(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("filtersink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)

	events := resourceEntry("", "Event")
	events.EventFilter = &v1alpha1.EventFilter{
		Types:        []v1alpha1.EventType{v1alpha1.EventTypeWarning},
		SubjectKinds: []string{"Pod"},
	}

	rule := h.newStreamRule(namespace, "filtering", sinkName, events)

	ruleKey := RuleKey(kindStreamRule, namespace, "filtering")
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("Event"), namespace)})

	want := `{"types":["Warning"],"subjectKinds":["Pod"]}`
	waitFor(t, fmt.Sprintf("rule %q event filter %q", ruleKey, want), func() (bool, string) {
		got := eventFiltersFor(h, ruleKey)
		return len(got) == 1 && got[0] == want, fmt.Sprintf("%q", got)
	})

	// Removing the filter widens the entry back to every Event in scope, and ""
	// is what the registry stores for that — a member of the merged set rather
	// than an absence from it.
	if err := h.Client.Get(context.Background(),
		client.ObjectKeyFromObject(rule), rule); err != nil {
		t.Fatalf("re-read StreamRule: %v", err)
	}
	rule.Spec.Resources[0].EventFilter = nil
	if err := h.Client.Update(context.Background(), rule); err != nil {
		t.Fatalf("update StreamRule: %v", err)
	}
	waitFor(t, fmt.Sprintf("rule %q event filter %q", ruleKey, ""), func() (bool, string) {
		got := eventFiltersFor(h, ruleKey)
		return len(got) == 1 && got[0] == "", fmt.Sprintf("%q", got)
	})
}

// eventFiltersFor returns the Event filters the registry currently holds for every
// target one rule contributes.
func eventFiltersFor(h *harness, ruleKey string) []string {
	var got []string
	for _, state := range h.Registry.Snapshot() {
		for _, rule := range state.RuleKeys {
			if rule == ruleKey {
				got = append(got, state.EventFilters...)
			}
		}
	}
	return got
}
