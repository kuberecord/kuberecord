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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/internal/sink"
)

// TestCheckPolicy covers the admission decision that keeps D8 enforceable in code:
// the hard Secret deny must win over every possible policy, and the permissive
// default must stay permissive for everything else.
func TestCheckPolicy(t *testing.T) {
	resource := func(group, version, kind string) v1alpha1.WatchedResource {
		return v1alpha1.WatchedResource{Group: group, Version: version, Kind: kind}
	}
	selector := func(group, version string, kinds ...string) v1alpha1.GVKSelector {
		return v1alpha1.GVKSelector{Group: group, Version: version, Kinds: kinds}
	}

	tests := []struct {
		name        string
		resources   []v1alpha1.WatchedResource
		policy      v1alpha1.SinkPolicy
		wantReason  string
		wantMessage string
	}{
		{
			name:      "empty allow list admits anything but Secrets",
			resources: []v1alpha1.WatchedResource{resource("apps", "v1", "Deployment"), resource("", "v1", "Pod")},
		},
		{
			name:       "core Secret is denied with no policy at all",
			resources:  []v1alpha1.WatchedResource{resource("", "v1", "Secret")},
			wantReason: ReasonSecretsDenied,
			// The message must name the resource as the author wrote it, so the
			// condition is greppable against the CR.
			wantMessage: "v1/Secret",
		},
		{
			name:      "a policy that explicitly admits Secrets still cannot",
			resources: []v1alpha1.WatchedResource{resource("", "v1", "Secret")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("", "v1", "Secret")},
			},
			wantReason: ReasonSecretsDenied,
		},
		{
			name:      "a wildcard policy over the core group still cannot admit Secrets",
			resources: []v1alpha1.WatchedResource{resource("", "v1", "Secret")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("", "v1", "*")},
			},
			wantReason: ReasonSecretsDenied,
		},
		{
			name:      "a future Secret version is denied too",
			resources: []v1alpha1.WatchedResource{resource("", "v2", "Secret")},
			// The deny matches on group and kind alone, so a new version cannot be
			// a way around it.
			wantReason: ReasonSecretsDenied,
		},
		{
			name:      "a Secret-named kind in another group is not the denied one",
			resources: []v1alpha1.WatchedResource{resource("secrets.example.com", "v1", "Secret")},
		},
		{
			name:      "the deny is checked before the allow list",
			resources: []v1alpha1.WatchedResource{resource("apps", "v1", "StatefulSet"), resource("", "v1", "Secret")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("", "v1", "Pod")},
			},
			// StatefulSet is not in the allow list either, but Secrets are the more
			// serious verdict and must be the one reported.
			wantReason: ReasonSecretsDenied,
		},
		{
			name:      "a named kind in the allow list is admitted",
			resources: []v1alpha1.WatchedResource{resource("apps", "v1", "Deployment")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("apps", "v1", "Deployment", "StatefulSet")},
			},
		},
		{
			name:      "a wildcard admits every kind in its group and version",
			resources: []v1alpha1.WatchedResource{resource("apps", "v1", "DaemonSet")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("apps", "v1", "*")},
			},
		},
		{
			name:      "a wildcard does not cross versions",
			resources: []v1alpha1.WatchedResource{resource("apps", "v1beta1", "Deployment")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("apps", "v1", "*")},
			},
			wantReason:  ReasonNotInAllowList,
			wantMessage: "apps/v1beta1/Deployment",
		},
		{
			name:      "a wildcard does not cross groups",
			resources: []v1alpha1.WatchedResource{resource("batch", "v1", "Job")},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("apps", "v1", "*")},
			},
			wantReason: ReasonNotInAllowList,
		},
		{
			name: "one refused resource refuses the whole rule",
			resources: []v1alpha1.WatchedResource{
				resource("apps", "v1", "Deployment"),
				resource("batch", "v1", "Job"),
			},
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{selector("apps", "v1", "*")},
			},
			wantReason:  ReasonNotInAllowList,
			wantMessage: "batch/v1/Job",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkPolicy(tc.resources, tc.policy)
			if tc.wantReason == "" {
				if got != nil {
					t.Fatalf("expected the rule to be admitted, got %s: %s", got.reason, got.message)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected a %s denial, but the rule was admitted", tc.wantReason)
			}
			if got.reason != tc.wantReason {
				t.Errorf("reason = %q, want %q (message: %s)", got.reason, tc.wantReason, got.message)
			}
			if tc.wantMessage != "" && !strings.Contains(got.message, tc.wantMessage) {
				t.Errorf("message %q does not contain %q", got.message, tc.wantMessage)
			}
		})
	}
}

// TestRuleKeyRoundTrip pins the registry key format, which three components have to
// agree on: the reconcilers write it, the registry stores it, and the sink runtime
// hands it back for parking.
func TestRuleKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		namespace string
		object    string
		wantKey   string
	}{
		{
			name:      "namespaced rule",
			kind:      kindStreamRule,
			namespace: "team-a",
			object:    "audit",
			wantKey:   "streamrule/team-a/audit",
		},
		{
			name:    "cluster rule has an empty namespace segment",
			kind:    kindClusterStreamRule,
			object:  "everything",
			wantKey: "clusterstreamrule//everything",
		},
		{
			name:      "two kinds sharing a name are different keys",
			kind:      kindClusterStreamRule,
			namespace: "",
			object:    "audit",
			wantKey:   "clusterstreamrule//audit",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := RuleKey(tc.kind, tc.namespace, tc.object)
			if key != tc.wantKey {
				t.Fatalf("RuleKey = %q, want %q", key, tc.wantKey)
			}
			gotKind, ref, ok := parseRuleKey(key)
			if !ok {
				t.Fatalf("parseRuleKey(%q) reported the key as unrecognised", key)
			}
			if gotKind != tc.kind {
				t.Errorf("kind = %q, want %q", gotKind, tc.kind)
			}
			want := types.NamespacedName{Namespace: tc.namespace, Name: tc.object}
			if ref != want {
				t.Errorf("ref = %v, want %v", ref, want)
			}
		})
	}
}

// TestParseRuleKeyRejects covers the inputs the parker must refuse rather than turn
// into a reconcile request for the wrong object.
func TestParseRuleKeyRejects(t *testing.T) {
	for _, key := range []string{
		"",
		"streamrule",
		"streamrule/ns",
		"streamrule/ns/name/extra",
		"streamrule/ns/",
		"/ns/name",
		"deployment/ns/name",
	} {
		t.Run(key, func(t *testing.T) {
			if _, _, ok := parseRuleKey(key); ok {
				t.Fatalf("parseRuleKey(%q) accepted a key RuleKey never produces", key)
			}
		})
	}
}

// TestReadyFor covers the roll-up rule that keeps "True means healthy" mechanically
// true: any non-True specific condition must make the roll-up non-True and carry
// that condition's own reason forward.
func TestReadyFor(t *testing.T) {
	const readyType = v1alpha1.ConditionReady

	tests := []struct {
		name        string
		set         []metav1.Condition
		order       []string
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		{
			name: "everything true rolls up to true",
			set: []metav1.Condition{
				{Type: v1alpha1.ConditionPolicyAllowed, Status: metav1.ConditionTrue, Reason: "A"},
				{Type: v1alpha1.ConditionRBACGranted, Status: metav1.ConditionTrue, Reason: "B"},
			},
			order:      []string{v1alpha1.ConditionPolicyAllowed, v1alpha1.ConditionRBACGranted},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonStreaming,
		},
		{
			name: "a false condition carries its own reason up",
			set: []metav1.Condition{
				{Type: v1alpha1.ConditionPolicyAllowed, Status: metav1.ConditionTrue, Reason: "A"},
				{
					Type: v1alpha1.ConditionRBACGranted, Status: metav1.ConditionFalse,
					Reason: ReasonMissingPermissions, Message: "pods: missing watch",
				},
			},
			order:       []string{v1alpha1.ConditionPolicyAllowed, v1alpha1.ConditionRBACGranted},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonMissingPermissions,
			wantMessage: "pods: missing watch",
		},
		{
			name: "the order decides which failure is reported",
			set: []metav1.Condition{
				{Type: v1alpha1.ConditionPolicyAllowed, Status: metav1.ConditionFalse, Reason: ReasonSecretsDenied},
				{Type: v1alpha1.ConditionRBACGranted, Status: metav1.ConditionFalse, Reason: ReasonMissingPermissions},
			},
			order:      []string{v1alpha1.ConditionPolicyAllowed, v1alpha1.ConditionRBACGranted},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretsDenied,
		},
		{
			name: "an unknown condition rolls up as unknown, not false",
			set: []metav1.Condition{
				{Type: v1alpha1.ConditionSchemaValid, Status: metav1.ConditionUnknown, Reason: ReasonProbePending},
			},
			order:      []string{v1alpha1.ConditionSchemaValid},
			wantStatus: metav1.ConditionUnknown,
			wantReason: ReasonProbePending,
		},
		{
			name: "an unreachable backend is the one unknown that rolls up false",
			set: []metav1.Condition{
				{
					Type: v1alpha1.ConditionSchemaValid, Status: metav1.ConditionUnknown,
					Reason: sink.ProbeReasonUnreachable,
				},
			},
			order:      []string{v1alpha1.ConditionSchemaValid},
			wantStatus: metav1.ConditionFalse,
			wantReason: sink.ProbeReasonUnreachable,
		},
		{
			name:       "a condition the pass never decided is not a failure",
			set:        nil,
			order:      []string{v1alpha1.ConditionSchemaValid},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonStreaming,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status := &statusWriter{generation: 7}
			for _, c := range tc.set {
				status.set(c.Type, c.Status, c.Reason, c.Message)
			}

			got := readyFor(status, readyType, tc.order, ReasonStreaming, "all good")
			if got.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if tc.wantMessage != "" && got.Message != tc.wantMessage {
				t.Errorf("message = %q, want %q", got.Message, tc.wantMessage)
			}
			if got.ObservedGeneration != 7 {
				t.Errorf("observedGeneration = %d, want 7", got.ObservedGeneration)
			}
		})
	}
}

// TestStatusWriterApplyPreservesTransitionTime checks the property that makes
// conditions readable over time: a condition re-decided to the same status must keep
// its original lastTransitionTime, so "since when has this been broken?" survives a
// resync.
func TestStatusWriterApplyPreservesTransitionTime(t *testing.T) {
	existing := []metav1.Condition{{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonSinkMissing,
		Message:            "gone",
		LastTransitionTime: metav1.NewTime(metav1.Now().Add(-time.Hour)),
		ObservedGeneration: 1,
	}}
	originalTransition := existing[0].LastTransitionTime

	status := &statusWriter{generation: 2}
	status.set(v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing, "still gone")
	status.apply(&existing)

	got := findCondition(existing, v1alpha1.ConditionReady)
	if got == nil {
		t.Fatal("the Ready condition disappeared")
	}
	if !got.LastTransitionTime.Equal(&originalTransition) {
		t.Errorf("lastTransitionTime changed for an unchanged status: %v -> %v",
			originalTransition, got.LastTransitionTime)
	}
	if got.Message != "still gone" {
		t.Errorf("message = %q, want the refreshed one", got.Message)
	}
	if got.ObservedGeneration != 2 {
		t.Errorf("observedGeneration = %d, want 2", got.ObservedGeneration)
	}

	// A genuine status change must move it.
	status = &statusWriter{generation: 3}
	status.set(v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming, "back")
	status.apply(&existing)
	got = findCondition(existing, v1alpha1.ConditionReady)
	if got.LastTransitionTime.Equal(&originalTransition) {
		t.Error("lastTransitionTime did not move on a real status transition")
	}
}

// TestTruncateMessage checks that a rule with a pathological number of failures
// cannot produce a status the API server refuses to store.
func TestTruncateMessage(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
	}{
		{name: "short messages pass through", in: "pods: missing watch", want: len("pods: missing watch")},
		{name: "an over-long message is capped", in: strings.Repeat("x", maxConditionMessage*2), want: maxConditionMessage},
		{name: "a message exactly at the cap is untouched", in: strings.Repeat("x", maxConditionMessage), want: maxConditionMessage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateMessage(tc.in)
			if len(got) != tc.want {
				t.Errorf("length = %d, want %d", len(got), tc.want)
			}
			if len(tc.in) > maxConditionMessage && !strings.HasSuffix(got, "truncated]") {
				t.Error("a truncated message does not say so")
			}
		})
	}
}
