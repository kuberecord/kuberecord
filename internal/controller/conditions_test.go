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
	"k8s.io/client-go/tools/record"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/sink"
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

			got := readyFor(status, tc.order, ReasonStreaming, "all good")
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

// TestEmitCapabilityLimitEvent is Task 6.5's "a Warning event is emitted **once**
// on first registration".
//
// Once is the entire property, and it is worth a dedicated test because the
// failure modes are opposite and both are bad. Emit level-triggered and a
// permanent limit becomes a permanent stream of identical Warnings on every
// resync, which is how an event log stops being read. Emit never and an operator
// who did not know to look for an inverted condition learns from row counts
// months later that their archive has no deletions in it.
func TestEmitCapabilityLimitEvent(t *testing.T) {
	writerOnly := metav1.Condition{
		Type:    v1alpha1.ConditionHistoryUnavailable,
		Status:  metav1.ConditionTrue,
		Reason:  ReasonWriterOnlySink,
		Message: writerOnlySinkMessage,
	}

	tests := []struct {
		name      string
		previous  *metav1.Condition
		current   metav1.Condition
		wantEvent bool
	}{
		{
			name:      "first registration warns",
			previous:  nil,
			current:   writerOnly,
			wantEvent: true,
		},
		{
			name: "a resync of the same verdict says nothing",
			// The condition is already on the object, so this pass is re-deciding
			// what is already true. This is the case that runs every two minutes
			// for the life of the sink.
			previous:  &writerOnly,
			current:   writerOnly,
			wantEvent: false,
		},
		{
			name: "a sink that gained a read half is not warned about",
			previous: &metav1.Condition{
				Type: v1alpha1.ConditionHistoryUnavailable, Status: metav1.ConditionTrue,
				Reason: ReasonWriterOnlySink,
			},
			current: metav1.Condition{
				Type: v1alpha1.ConditionHistoryUnavailable, Status: metav1.ConditionFalse,
				Reason: ReasonHistoryReadable, Message: historyReadableMessage,
			},
			wantEvent: false,
		},
		{
			name:     "an undetected sink is not warned about either",
			previous: nil,
			current: metav1.Condition{
				Type: v1alpha1.ConditionHistoryUnavailable, Status: metav1.ConditionUnknown,
				Reason: ReasonCapabilitiesUnknown, Message: "no running instance yet",
			},
			// Unknown is "nobody has looked". Warning about it would tell an
			// operator something about their backend at the one moment nothing
			// about it is known — and it clears within a second of the first probe.
			wantEvent: false,
		},
		{
			name: "an undetected sink that becomes writer-only still warns",
			previous: &metav1.Condition{
				Type: v1alpha1.ConditionHistoryUnavailable, Status: metav1.ConditionUnknown,
				Reason: ReasonCapabilitiesUnknown,
			},
			current:   writerOnly,
			wantEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := record.NewFakeRecorder(4)
			obj := &v1alpha1.S3Sink{ObjectMeta: metav1.ObjectMeta{Name: "archive"}}

			emitCapabilityLimitEvent(recorder, obj, tt.previous, tt.current)
			close(recorder.Events)

			var got []string
			for event := range recorder.Events {
				got = append(got, event)
			}
			if !tt.wantEvent {
				if len(got) != 0 {
					t.Fatalf("emitted %d events, want none: %v", len(got), got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("emitted %d events, want exactly 1: %v", len(got), got)
			}
			// A Warning, and filed under its own reason rather than Degraded: an
			// operator grepping events for a degrade must not find a healthy sink.
			for _, want := range []string{"Warning", EventReasonHistoryUnavailable, ReasonWriterOnlySink} {
				if !strings.Contains(got[0], want) {
					t.Errorf("event %q does not carry %q", got[0], want)
				}
			}
			if strings.Contains(got[0], EventReasonDegraded) {
				t.Errorf("event %q is filed as a degrade; a declared capability limit is not one", got[0])
			}
			// The three behaviours and both consequences travel with the event, so
			// `kubectl describe` alone is enough to understand it.
			for _, want := range []string{
				"cache warm-up", "zombie garbage collection", "boot reconciliation of scope epochs",
				"permanent Snapshot", "while the operator is down are never recorded",
			} {
				if !strings.Contains(got[0], want) {
					t.Errorf("event %q does not name %q", got[0], want)
				}
			}
		})
	}
}

// TestWriterOnlyMessagesNameEveryDisabledBehaviour pins the wording Task 6.5
// requires, on both the sink's message and the rule's.
//
// It is a test about *text*, which is unusual and deliberate. This condition's
// entire job is to be read by a human who did not know to look for it, so the
// message is the feature: a future edit that tightened it into "history is
// unavailable" would leave the condition technically present and practically
// useless, and nothing else in the suite would notice.
func TestWriterOnlyMessagesNameEveryDisabledBehaviour(t *testing.T) {
	behaviours := []string{"cache warm-up", "zombie garbage collection", "boot reconciliation of scope epochs"}

	t.Run("the sink's message", func(t *testing.T) {
		for _, want := range behaviours {
			if !strings.Contains(writerOnlySinkMessage, want) {
				t.Errorf("writerOnlySinkMessage does not name the disabled behaviour %q", want)
			}
		}
		// Both consequences. The second is the one that cannot be inferred from the
		// archive: an object store with no Deleted records looks exactly like an
		// archive of a cluster where nothing was deleted.
		for _, want := range []string{"permanent Snapshot", "while the operator is down are never recorded"} {
			if !strings.Contains(writerOnlySinkMessage, want) {
				t.Errorf("writerOnlySinkMessage does not state the consequence %q", want)
			}
		}
		// And the metric, which is where the same fact is observable without the API.
		if !strings.Contains(writerOnlySinkMessage, "kuberecord_safe_mode") {
			t.Error("writerOnlySinkMessage does not point at the gauge that observes it")
		}
	})

	t.Run("the rule's message", func(t *testing.T) {
		message := writerOnlySinkRuleMessage("S3Sink/archive")
		// The rule's author may never read the sink, so the message has to name it.
		if !strings.Contains(message, "S3Sink/archive") {
			t.Errorf("rule message %q does not name the sink", message)
		}
		for _, want := range []string{"permanent Snapshot", "while the operator is down is never recorded"} {
			if !strings.Contains(message, want) {
				t.Errorf("rule message %q does not state the consequence %q", message, want)
			}
		}
		// And it must not read as the rule's own fault, since the rule is fine.
		if !strings.Contains(message, "not a fault of this rule") {
			t.Errorf("rule message %q does not say the limit belongs to the sink", message)
		}
	})
}

// TestHistoryUnavailableIsNeverInAReadyOrder is the structural half of "Ready
// remains True when everything else is healthy".
//
// The condition-by-condition tests could all pass while a later edit quietly added
// HistoryUnavailable to one of these lists, at which point every S3Sink in every
// cluster would report Ready=False and every rule bound to one would look broken.
// readyFor consults exactly these slices, so asserting on them forecloses that
// whole class of regression in one place.
func TestHistoryUnavailableIsNeverInAReadyOrder(t *testing.T) {
	orders := map[string][]string{
		"sinkReadyOrder":   sinkReadyOrder,
		"s3SinkReadyOrder": s3SinkReadyOrder,
		"ruleReadyOrder":   ruleReadyOrder,
	}
	for name, order := range orders {
		for _, condType := range order {
			if condType == v1alpha1.ConditionHistoryUnavailable {
				t.Errorf("%s contains %s: a declared capability limit would drag Ready to False",
					name, v1alpha1.ConditionHistoryUnavailable)
			}
		}
	}
}
