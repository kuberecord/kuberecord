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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/watch"
)

// auditLabel is the namespace label key the selector tests select on. Its *value* is
// made unique per test, because envtest has no namespace controller: a deleted
// namespace stays Terminating forever, so namespaces accumulate across the package's
// tests and a shared label value would let one test's namespaces satisfy another
// test's selector.
const auditLabel = "kuberecord.test/audit"

// resourceEntry is a shorthand for one entry of a rule's spec.resources. Every kind
// these tables name is served at v1 (the version is exercised on its own in
// TestCheckPolicy, which does not need an apiserver).
func resourceEntry(group, kind string) v1alpha1.WatchedResource {
	return v1alpha1.WatchedResource{Group: group, Version: "v1", Kind: kind}
}

// newStreamRule builds a namespaced rule in a fresh namespace, streaming to the
// ClickHouseSink of the given name.
//
// The sink reference's kind is left unset on purpose: these rules are created
// through a real apiserver, so omitting it exercises the CRD default every rule
// in a ClickHouse-only cluster relies on.
func (h *harness) newStreamRule(namespace, name, sinkName string,
	resources ...v1alpha1.WatchedResource) *v1alpha1.StreamRule {
	h.t.Helper()
	return h.newStreamRuleWithSink(namespace, name, v1alpha1.SinkReference{Name: sinkName}, resources...)
}

// newStreamRuleWithSink is newStreamRule with the sink reference spelled out, for
// the tests that are *about* the reference: the kind an author wrote is the input
// under test there, so leaving it to the CRD default would test nothing.
func (h *harness) newStreamRuleWithSink(namespace, name string, ref v1alpha1.SinkReference,
	resources ...v1alpha1.WatchedResource) *v1alpha1.StreamRule {
	h.t.Helper()
	rule := &v1alpha1.StreamRule{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1alpha1.StreamRuleSpec{
			Sink:      ref,
			Resources: resources,
		},
	}
	if err := h.Client.Create(context.Background(), rule); err != nil {
		h.t.Fatalf("create StreamRule %s/%s: %v", namespace, name, err)
	}
	h.t.Cleanup(func() { h.deleteIfExists(rule) })
	return rule
}

// stageLegacyRule stores a rule of either kind exactly as an upgrade from v0.1.0
// leaves one behind: `sinkRef` named the sink, and after the rename (D10) that field
// is pruned as unknown and `sink` is simply absent.
//
// The legacy name is still written even though the apiserver drops it, because that
// is the document an operator actually has in etcd — and asserting the reconciler's
// verdict on a *fabricated* shape rather than on the real one would prove less.
func (h *harness) stageLegacyRule(ruleKind, namespace, name, legacySinkRef string,
	resources ...v1alpha1.WatchedResource) client.Object {
	h.t.Helper()
	return h.stageRule(ruleKind, namespace, name, map[string]any{
		"sinkRef":   legacySinkRef,
		"resources": unstructuredResources(resources),
	}, dropRequiredSink)
}

// stageRuleNamingSinkKind stores a rule naming a sink kind no reconciler in this
// build serves.
//
// It relaxes the CRD's kind enum on the way in, which is what a kind no *release*
// serves needs — such a rule could only have been written by a newer operator, or by
// one this binary was downgraded from. Every kind the enum admits now has a
// reconciler behind it (Task 6.4 registered the last one), so the relaxation is what
// makes this helper able to stage the unserved case at all.
func (h *harness) stageRuleNamingSinkKind(ruleKind, namespace, name, sinkKind, sinkName string,
	resources ...v1alpha1.WatchedResource) client.Object {
	h.t.Helper()
	return h.stageRule(ruleKind, namespace, name, map[string]any{
		"sink":      map[string]any{"kind": sinkKind, "name": sinkName},
		"resources": unstructuredResources(resources),
	}, dropSinkKindEnum)
}

// newClusterStreamRule builds a cluster-scoped rule. The sink reference's kind is
// left to the CRD default, exactly as in newStreamRule.
func (h *harness) newClusterStreamRule(name, sinkName string, selector *metav1.LabelSelector,
	resources ...v1alpha1.WatchedResource) *v1alpha1.ClusterStreamRule {
	h.t.Helper()
	rule := &v1alpha1.ClusterStreamRule{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ClusterStreamRuleSpec{
			StreamRuleSpec: v1alpha1.StreamRuleSpec{
				Sink:      v1alpha1.SinkReference{Name: sinkName},
				Resources: resources,
			},
			NamespaceSelector: selector,
		},
	}
	if err := h.Client.Create(context.Background(), rule); err != nil {
		h.t.Fatalf("create ClusterStreamRule %s: %v", name, err)
	}
	h.t.Cleanup(func() { h.deleteIfExists(rule) })
	return rule
}

// TestRulePolicyGates is acceptance criteria (a) and (b): a rule naming v1/Secret is
// refused with reason SecretsDenied, a rule outside a non-empty allowedGVKs is
// refused with NotInAllowList, and in both cases *nothing* reaches the registry —
// the deny is not merely reported, it is enforced.
func TestRulePolicyGates(t *testing.T) {
	tests := []struct {
		name       string
		policy     v1alpha1.SinkPolicy
		resources  []v1alpha1.WatchedResource
		wantStatus metav1.ConditionStatus
		wantReason string
		// wantTargets is the registry contribution the rule is allowed to make.
		wantTargets []string
	}{
		{
			name:       "a rule naming Secrets is refused",
			resources:  []v1alpha1.WatchedResource{resourceEntry("", "Secret")},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretsDenied,
		},
		{
			name: "a rule naming Secrets alongside a legal kind is refused entirely",
			resources: []v1alpha1.WatchedResource{
				resourceEntry("", "ConfigMap"),
				resourceEntry("", "Secret"),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretsDenied,
		},
		{
			name: "a rule outside a non-empty allow list is refused",
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{{Group: "apps", Version: "v1", Kinds: []string{"*"}}},
			},
			resources:  []v1alpha1.WatchedResource{resourceEntry("", "ConfigMap")},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNotInAllowList,
		},
		{
			name: "a rule inside the allow list is admitted",
			policy: v1alpha1.SinkPolicy{
				AllowedGVKs: []v1alpha1.GVKSelector{{Group: "", Version: "v1", Kinds: []string{"ConfigMap"}}},
			},
			resources:   []v1alpha1.WatchedResource{resourceEntry("", "ConfigMap")},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  ReasonAllResourcesPermitted,
			wantTargets: []string{coreGVK("ConfigMap")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{allowAll: true})
			sinkName := uniqueName("psink")
			h.createReadySink(sinkName, tc.policy)

			namespace := uniqueName("ns")
			h.createNamespace(namespace, nil)
			rule := h.newStreamRule(namespace, "policy", sinkName, tc.resources...)

			h.waitForRuleCondition(rule, v1alpha1.ConditionPolicyAllowed, tc.wantStatus, tc.wantReason)

			ruleKey := RuleKey(kindStreamRule, namespace, "policy")
			want := tc.wantTargets
			for i, target := range want {
				want[i] = fmt.Sprintf("%s@%s", target, namespace)
			}
			h.waitForTargets(ruleKey, want)

			if tc.wantStatus == metav1.ConditionFalse {
				// A refused rule must say so on its roll-up too, and must report
				// the two gates that never ran as Unknown rather than implying they
				// passed.
				h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, tc.wantReason)
				h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted,
					metav1.ConditionUnknown, ReasonNotEvaluated)
			}
		})
	}
}

// TestRuleRBACDenialIsPerTarget is acceptance criterion (c): an SSAR denial must name
// the resource and the missing verbs in the condition message, and must not stop the
// rule's *other* targets from activating.
func TestRuleRBACDenialIsPerTarget(t *testing.T) {
	// configmaps are granted; secrets would be, but are denied by policy anyway, so
	// the denied-but-legal kind here is pods.
	h := newHarness(t, harnessOptions{allowed: []string{"configmaps"}})
	sinkName := uniqueName("rbacsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "partial", sinkName,
		resourceEntry("", "ConfigMap"),
		resourceEntry("", "Pod"),
	)

	got := h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted,
		metav1.ConditionFalse, ReasonMissingPermissions)

	// The message has to be actionable on its own: an administrator reading it must
	// know which resource and which verbs to grant.
	for _, want := range []string{"pods", "get", "list", "watch", namespace} {
		if !strings.Contains(got.Message, want) {
			t.Errorf("RBACGranted message %q does not mention %q", got.Message, want)
		}
	}
	if strings.Contains(got.Message, "configmaps") {
		t.Errorf("RBACGranted message %q blames a resource that was granted", got.Message)
	}

	// The granted half of the rule still streams — this is the property that keeps
	// one missing grant from silencing a whole rule.
	ruleKey := RuleKey(kindStreamRule, namespace, "partial")
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	// activeWatches reflects the registry, not the spec: one of two resources is
	// installed.
	waitFor(t, "activeWatches to report the one installed target", func() (bool, string) {
		var fresh v1alpha1.StreamRule
		if err := h.Client.Get(context.Background(), client.ObjectKeyFromObject(rule), &fresh); err != nil {
			return false, err.Error()
		}
		return fresh.Status.ActiveWatches == 1, fmt.Sprintf("activeWatches=%d", fresh.Status.ActiveWatches)
	})
}

// TestRuleRBACGrantSelfHeals is acceptance criterion (e): a rule parked on
// RBACGranted=False must come back on its own once the grant appears, within one
// resync and with no restart.
func TestRuleRBACGrantSelfHeals(t *testing.T) {
	// A short resync is the whole point of the test: it is the only mechanism that
	// re-asks a question nothing watchable answers.
	h := newHarness(t, harnessOptions{resyncPeriod: 250 * time.Millisecond})
	sinkName := uniqueName("healsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "heal", sinkName, resourceEntry("", "ConfigMap"))

	h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted, metav1.ConditionFalse, ReasonMissingPermissions)
	h.waitForTargets(RuleKey(kindStreamRule, namespace, "heal"), nil)

	// The administrator applies the grant. Nothing about the rule changes, and no
	// event is delivered — only the resync can notice.
	h.Reviewer.allow("configmaps")

	h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted, metav1.ConditionTrue, ReasonAllVerbsGranted)
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(RuleKey(kindStreamRule, namespace, "heal"),
		[]string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})
}

// TestRuleAccessReviewFailureIsNotAVerdict checks the difference between "the API
// server said no" and "the API server could not answer": the second must not park
// the rule on a conclusion nobody reached, and must not withdraw targets the rule
// already had.
func TestRuleAccessReviewFailureIsNotAVerdict(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true, resyncPeriod: 250 * time.Millisecond})
	sinkName := uniqueName("failsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "reviewfail", sinkName, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "reviewfail")

	installed := []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)}
	h.waitForTargets(ruleKey, installed)

	h.Reviewer.fail(errors.New("etcdserver: request timed out"))
	h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted, metav1.ConditionUnknown, ReasonAccessReviewFailed)

	// The targets stay: a transient API failure is not a reason to stop watching,
	// which would write scope epochs for a rule nothing is wrong with.
	if got := h.targetsFor(ruleKey); !slicesEqual(got, installed) {
		t.Errorf("targets = %v, want them left alone at %v", got, installed)
	}
}

// TestNamespacedRuleRefusesClusterScopedKind covers the scope rule that keeps a
// StreamRule from being an escalation path: a namespaced rule naming a cluster-scoped
// kind is refused permanently, and the message points at ClusterStreamRule.
func TestNamespacedRuleRefusesClusterScopedKind(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("scopesink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "escalate", sinkName,
		resourceEntry("", "Node"),
		resourceEntry("", "ConfigMap"),
	)

	got := h.waitForRuleCondition(rule, v1alpha1.ConditionResourceResolved,
		metav1.ConditionFalse, ReasonKindsUnresolved)
	if !strings.Contains(got.Message, "ClusterStreamRule") {
		t.Errorf("ResourceResolved message %q does not point the author at ClusterStreamRule", got.Message)
	}

	// The legal half of the rule still streams.
	h.waitForTargets(RuleKey(kindStreamRule, namespace, "escalate"),
		[]string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})
}

// TestRuleUnknownKindResolvesLater covers the self-healing half of resolution: a rule
// naming a kind no CRD provides is parked on ResourceResolved, with the resolver's own
// reason, and the verdict is transient by construction.
func TestRuleUnknownKindResolvesLater(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("crdsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "notyet", sinkName,
		resourceEntry("example.com", "Widget"))

	got := h.waitForRuleCondition(rule, v1alpha1.ConditionResourceResolved,
		metav1.ConditionFalse, ReasonKindsUnresolved)
	if !strings.Contains(got.Message, "not installed") {
		t.Errorf("ResourceResolved message %q does not read as a transient missing kind", got.Message)
	}
	h.waitForTargets(RuleKey(kindStreamRule, namespace, "notyet"), nil)
}

// TestClusterRuleWithoutSelectorWatchesEverywhere pins the nil-selector semantics: one
// all-namespaces target, not one target per namespace.
func TestClusterRuleWithoutSelectorWatchesEverywhere(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("allsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	name := uniqueName("everywhere")
	rule := h.newClusterStreamRule(name, sinkName, nil, resourceEntry("", "ConfigMap"))

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(RuleKey(kindClusterStreamRule, "", name),
		[]string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), "")})
}

// TestClusterRuleClusterScopedKindIgnoresSelector pins the documented interaction
// between a namespace selector and a cluster-scoped kind: the kind has no namespace to
// select on, so it yields a single all-namespaces target however the selector is
// written.
func TestClusterRuleClusterScopedKindIgnoresSelector(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("nodesink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	// The label *value* is unique per test: namespaces outlive a test (envtest has
	// no namespace controller to finish their deletion), so a shared value would let
	// one test's namespaces match another test's selector.
	audit := uniqueName("audit")
	included := uniqueName("in")
	h.createNamespace(included, map[string]string{auditLabel: audit})

	name := uniqueName("nodes")
	rule := h.newClusterStreamRule(name, sinkName,
		&metav1.LabelSelector{MatchLabels: map[string]string{auditLabel: audit}},
		resourceEntry("", "Node"),
		resourceEntry("", "ConfigMap"),
	)

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(RuleKey(kindClusterStreamRule, "", name), []string{
		// The cluster-scoped Node lands on the all-namespaces target; the namespaced
		// ConfigMap is expanded onto the one selected namespace.
		fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), included),
		fmt.Sprintf("%s@%s", coreGVK("Node"), ""),
	})
}

// TestClusterRuleNamespaceSelectorTracksLabels is acceptance criterion (h): labelling a
// namespace into a ClusterStreamRule's selector adds its target, and unlabelling it
// removes that target — live, with no restart and no edit to the rule.
//
// The registry is the right place to assert this. A removed target is exactly what the
// WatchManager turns into a stopped informer and a `Stopped` scope row (and never into
// `Deleted` rows for the objects that were in scope) — that translation is covered by
// internal/watch's own tests and end-to-end by the e2e suite; what this task owns is
// that the target set follows the labels.
func TestClusterRuleNamespaceSelectorTracksLabels(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("selsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	audit := uniqueName("audit")
	included := uniqueName("inc")
	candidate := uniqueName("cand")
	h.createNamespace(included, map[string]string{auditLabel: audit})
	h.createNamespace(candidate, nil)

	name := uniqueName("selector")
	rule := h.newClusterStreamRule(name, sinkName,
		&metav1.LabelSelector{MatchLabels: map[string]string{auditLabel: audit}},
		resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindClusterStreamRule, "", name)

	target := func(namespace string) string {
		return fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)
	}
	h.waitForTargets(ruleKey, []string{target(included)})
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)

	// Labelling the candidate in adds its target: the Namespace watch is what makes
	// this happen without touching the rule.
	h.labelNamespace(candidate, map[string]string{auditLabel: audit})
	want := []string{target(candidate), target(included)}
	if candidate > included {
		want = []string{target(included), target(candidate)}
	}
	h.waitForTargets(ruleKey, want)

	waitFor(t, "activeWatches to follow the expanded selector", func() (bool, string) {
		var fresh v1alpha1.ClusterStreamRule
		if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &fresh); err != nil {
			return false, err.Error()
		}
		return fresh.Status.ActiveWatches == 2, fmt.Sprintf("activeWatches=%d", fresh.Status.ActiveWatches)
	})

	// Unlabelling it removes exactly that one target, leaving the other alone.
	h.labelNamespace(candidate, nil)
	h.waitForTargets(ruleKey, []string{target(included)})

	// And a selector that matches nothing is a legal, Ready state with no targets —
	// not a degrade. It is the normal transient state before the namespaces that will
	// match exist.
	h.labelNamespace(included, nil)
	h.waitForTargets(ruleKey, nil)
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
}

// TestClusterRuleAccessReviewShortCircuit checks the review-count optimisation that
// keeps a wide ClusterStreamRule affordable: a target allowed at cluster scope must
// not also be reviewed per namespace.
func TestClusterRuleAccessReviewShortCircuit(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("ssarsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	audit := uniqueName("audit")
	first := uniqueName("a")
	second := uniqueName("b")
	h.createNamespace(first, map[string]string{auditLabel: audit})
	h.createNamespace(second, map[string]string{auditLabel: audit})

	name := uniqueName("wide")
	h.newClusterStreamRule(name, sinkName,
		&metav1.LabelSelector{MatchLabels: map[string]string{auditLabel: audit}},
		resourceEntry("", "ConfigMap"))

	target := func(namespace string) string {
		return fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)
	}
	want := []string{target(first), target(second)}
	if first > second {
		want = []string{target(second), target(first)}
	}
	h.waitForTargets(RuleKey(kindClusterStreamRule, "", name), want)

	// Two namespaces, three verbs: without the short-circuit that is six reviews per
	// pass and every one of them namespaced. With it, every review is at cluster
	// scope.
	for _, question := range h.Reviewer.questions() {
		if !strings.HasSuffix(question, "||get") && !strings.HasSuffix(question, "||list") &&
			!strings.HasSuffix(question, "||watch") {
			t.Errorf("review %q was asked per namespace even though cluster scope allowed it", question)
		}
	}
}

// TestRuleTargetsCarryTheTypedSinkIdentity is Task 4.4 criterion (a): a rule with a
// valid `sink` activates, and the targets it installs are keyed on the typed
// identity the author wrote — kind included.
//
// The kind matters here and not merely the name: everything the data plane owns per
// sink (the dedup cache, warm state, the metric series) hangs off this key, so a
// target keyed on the name alone would be the collision this phase exists to close.
// The second half of the assertion is the negative one — the same name under another
// kind names nothing — because that is what "the kind is part of the key" actually
// means.
func TestRuleTargetsCarryTheTypedSinkIdentity(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("typedsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRuleWithSink(namespace, "typed",
		v1alpha1.SinkReference{Kind: "ClickHouseSink", Name: sinkName}, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "typed")

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	want := []sink.ID{{Kind: "ClickHouseSink", Name: sinkName}}
	if got := h.targetSinksFor(ruleKey); !slices.Equal(got, want) {
		t.Errorf("target sink identities = %v, want %v", got, want)
	}
	if got := h.Registry.RulesForSink(sink.ID{Kind: "S3Sink", Name: sinkName}); len(got) != 0 {
		t.Errorf("a sink of another kind with the same name claims %v as dependents", got)
	}
}

// TestLegacyRuleIsReportedAndRegistersNothing is Task 4.4 criterion (b): a rule whose
// decoded sink reference is the zero value — a rule written against v0.1.0's
// `sinkRef` — is parked on Ready=False/LegacySinkRef and contributes nothing.
//
// This is the failure mode the whole guard exists for, and it is unreachable through
// admission by construction: the rule was stored under a schema that had no `sink`
// field, and CRD validation only ever runs on write. So the object is staged the way
// an upgrade produces it (see harness.stageRule) and the shipped schema is back in
// place before the verdict is read — which also proves the more subtle half, that the
// operator can still *write status* onto an object the current schema would reject.
//
// A healthy sink of the right name exists throughout. That is the point: the rule
// must not resolve to it. A guard that merely reported the problem while quietly
// streaming to the obvious candidate would be worse than no guard, because the
// stream would look correct and nobody would read the condition.
func TestLegacyRuleIsReportedAndRegistersNothing(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("legacysink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})
	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)

	for _, tc := range []struct {
		name     string
		ruleKind string
		// namespace is empty for the cluster-scoped kind.
		namespace string
	}{
		{name: "namespaced rule", ruleKind: kindStreamRule, namespace: namespace},
		{name: "cluster rule", ruleKind: kindClusterStreamRule},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ruleName := uniqueName("legacy")
			rule := h.stageLegacyRule(tc.ruleKind, tc.namespace, ruleName, sinkName,
				resourceEntry("", "ConfigMap"))
			ruleKey := RuleKey(tc.ruleKind, tc.namespace, ruleName)

			ready := h.waitForRuleCondition(rule, v1alpha1.ConditionReady,
				metav1.ConditionFalse, ReasonLegacySinkRef)
			// The message has to carry the author's own vocabulary and the one action
			// that fixes it; a reason alone is not actionable for a field that no
			// longer appears in `kubectl explain`.
			for _, want := range []string{"sinkRef", "spec.sink", "delete this rule"} {
				if !strings.Contains(ready.Message, want) {
					t.Errorf("LegacySinkRef message %q does not mention %q", ready.Message, want)
				}
			}

			// No gate could run, so none of the three claims a verdict.
			for _, condType := range []string{
				v1alpha1.ConditionPolicyAllowed,
				v1alpha1.ConditionResourceResolved,
				v1alpha1.ConditionRBACGranted,
			} {
				h.waitForRuleCondition(rule, condType, metav1.ConditionUnknown, ReasonNotEvaluated)
			}

			if got := h.targetsFor(ruleKey); len(got) != 0 {
				t.Errorf("a legacy rule installed targets %v; it must contribute none", got)
			}
			if got := h.Registry.RulesForSink(clickHouseSinkID(sinkName)); slices.Contains(got, ruleKey) {
				t.Errorf("a legacy rule bound to sink %q anyway (dependents: %v)", sinkName, got)
			}
		})
	}
}

// TestSinkResolutionComparesTypedIdentities is Task 4.4 criteria (c) and (d): a rule
// naming a kind/name pair with no matching sink CR parks on SinkMissing, and two
// references sharing a *name* across two kinds resolve independently — the one naming
// the served kind streams, the other parks without ever touching that backend.
//
// The second half is the corruption this phase closes, tested from the control plane:
// a ClickHouseSink and an S3Sink named "default" are both legal in etcd, and a
// reconciler that resolved by name alone would bind the S3Sink's rules to the
// ClickHouse instance, handing them a dedup baseline and warm state built for someone
// else. The lower tiers guard the same property on their own keys (sink.ID, the
// pipeline's per-sink state, plan.Registry.RulesForSink); this is the last hop.
//
// Since Task 6.4 both halves are *live*: two backends of different kinds share one
// name, both are reconciled, and each streams to its own. That is a strictly stronger
// test than the earlier version, where the second kind had no reconciler and parked
// for a reason indistinguishable from the collision this asserts against — a rule
// bound to the wrong backend and a rule bound to none both install no targets. Only
// two running sinks can tell the two apart. The unserved-kind branch is still covered
// (a kind no release serves is storable in etcd but reconciled by nobody), with a
// kind that is genuinely absent from this build.
func TestSinkResolutionComparesTypedIdentities(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sharedName := uniqueName("shared")
	h.createReadySink(sharedName, v1alpha1.SinkPolicy{})
	h.createReadyS3Sink(sharedName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	configMaps := []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)}

	// One kind, under the shared name: streams to the ClickHouse backend.
	bound := h.newStreamRuleWithSink(namespace, "bound",
		v1alpha1.SinkReference{Kind: clickHouseSinkKind, Name: sharedName}, resourceEntry("", "ConfigMap"))
	boundKey := RuleKey(kindStreamRule, namespace, "bound")
	h.waitForRuleCondition(bound, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(boundKey, configMaps)

	// The other kind, the *same* name: streams to the archive backend, independently.
	archived := h.newStreamRuleWithSink(namespace, "archived",
		v1alpha1.SinkReference{Kind: s3SinkKind, Name: sharedName}, resourceEntry("", "ConfigMap"))
	archivedKey := RuleKey(kindStreamRule, namespace, "archived")
	h.waitForRuleCondition(archived, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(archivedKey, configMaps)

	// A kind no release serves — storable in etcd (a rule written by a newer
	// operator, or one this binary was downgraded out of serving) and reconciled by
	// nobody. It parks, and the message names the kind rather than only the name,
	// since the name alone describes two sinks that do exist.
	elsewhere := h.stageRuleNamingSinkKind(kindStreamRule, namespace, "elsewhere",
		"PostgresSink", sharedName, resourceEntry("", "ConfigMap"))
	elsewhereKey := RuleKey(kindStreamRule, namespace, "elsewhere")
	parked := h.waitForRuleCondition(elsewhere, v1alpha1.ConditionReady,
		metav1.ConditionFalse, ReasonSinkMissing)
	if !strings.Contains(parked.Message, "PostgresSink") {
		t.Errorf("SinkMissing message %q does not name the kind that is missing", parked.Message)
	}
	if got := h.targetsFor(elsewhereKey); len(got) != 0 {
		t.Errorf("a rule naming an unserved sink kind installed targets %v", got)
	}

	// Each sink's dependents are exactly the rule that named *its* kind — which is
	// what decides whose rules get parked when one of them is deleted, and the last
	// hop where a name-keyed resolution would silently merge the two.
	if got := h.Registry.RulesForSink(clickHouseSinkID(sharedName)); !slicesEqual(got, []string{boundKey}) {
		t.Errorf("RulesForSink(%s) = %v, want [%s]",
			clickHouseSinkID(sharedName), got, boundKey)
	}
	if got := h.Registry.RulesForSink(s3SinkID(sharedName)); !slicesEqual(got, []string{archivedKey}) {
		t.Errorf("RulesForSink(%s) = %v, want [%s]",
			s3SinkID(sharedName), got, archivedKey)
	}
	// And each rule's targets name its own backend, not the one that happens to share
	// the name.
	for _, tc := range []struct {
		rule string
		want []sink.ID
	}{
		{boundKey, []sink.ID{{Kind: clickHouseSinkKind, Name: sharedName}}},
		{archivedKey, []sink.ID{{Kind: s3SinkKind, Name: sharedName}}},
	} {
		if got := h.targetSinksFor(tc.rule); !slices.Equal(got, tc.want) {
			t.Errorf("rule %s streams to %v, want %v", tc.rule, got, tc.want)
		}
	}
	if got := h.targetsFor(boundKey); !slicesEqual(got, configMaps) {
		t.Errorf("targets = %v, want the bound rule still streaming at %v", got, configMaps)
	}

	// Criterion (c) in its plainest form: the served kind, a name nothing answers to.
	missing := h.newStreamRuleWithSink(namespace, "nosink",
		v1alpha1.SinkReference{Kind: clickHouseSinkKind, Name: uniqueName("absent")},
		resourceEntry("", "ConfigMap"))
	missingKey := RuleKey(kindStreamRule, namespace, "nosink")
	h.waitForRuleCondition(missing, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing)
	if got := h.targetsFor(missingKey); len(got) != 0 {
		t.Errorf("a rule naming no existing sink installed targets %v", got)
	}
}

// TestRuleSinkMissingParksAndWithdraws is acceptance criterion (g): deleting a sink
// parks its dependent rules with Ready=False/SinkMissing and withdraws their targets —
// a watch whose records have nowhere to go is not a watch worth running.
func TestRuleSinkMissingParksAndWithdraws(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("goingsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "orphan", sinkName, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "orphan")

	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	// The registry is the sink runtime's dependent-rule oracle, so it must name this
	// rule while the rule is streaming — that is what makes the park callback able
	// to reach it.
	if got := h.Registry.RulesForSink(clickHouseSinkID(sinkName)); !slicesEqual(got, []string{ruleKey}) {
		t.Errorf("RulesForSink(%q) = %v, want [%s]", sinkName, got, ruleKey)
	}

	var chSink v1alpha1.ClickHouseSink
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: sinkName}, &chSink); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if err := h.Client.Delete(context.Background(), &chSink); err != nil {
		t.Fatalf("delete the sink: %v", err)
	}

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing)
	h.waitForTargets(ruleKey, nil)
}

// TestRuleSinkNotReadyKeepsWatches is acceptance criterion (f) for the rule half, and
// pins the one deliberate deviation from "degraded means withdrawn": an unreachable
// sink parks the rule's *condition* but leaves its watches running.
//
// Withdrawing them would evict every dedup baseline the sink serves — forcing a full
// re-emission once it recovers — and would write a pair of false scope epochs per
// scope, which the sink could not even accept while it is the thing that is down.
func TestRuleSinkNotReadyKeepsWatches(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("flapsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "resilient", sinkName, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "resilient")

	installed := []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)}
	h.waitForTargets(ruleKey, installed)
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)

	// The sink goes unreachable. The rule's own gates all still pass, so its
	// specific conditions stay True and only the roll-up degrades.
	h.pushProbe(sink.ProbeResult{
		Sink:   clickHouseSinkID(sinkName),
		At:     time.Now().UTC(),
		Err:    errors.New("connection refused"),
		Reason: sink.ProbeReasonUnreachable,
	})
	h.waitForSinkCondition(sinkName, v1alpha1.ConditionReady, metav1.ConditionFalse, sink.ProbeReasonUnreachable)
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkNotReady)

	if got := h.targetsFor(ruleKey); !slicesEqual(got, installed) {
		t.Errorf("targets = %v, want the watches left running at %v", got, installed)
	}
	h.waitForRuleCondition(rule, v1alpha1.ConditionRBACGranted, metav1.ConditionTrue, ReasonAllVerbsGranted)

	// Recovery flips both back.
	h.pushProbe(sink.ProbeResult{Sink: clickHouseSinkID(sinkName), At: time.Now().UTC()})
	h.waitForSinkCondition(sinkName, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonConnected)
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	if got := h.targetsFor(ruleKey); !slicesEqual(got, installed) {
		t.Errorf("targets = %v, want %v", got, installed)
	}
}

// TestRuleDeletionWithdrawsTargets checks the finalizer-free deletion path: a deleted
// rule's targets disappear from the registry, which is what the WatchManager turns
// into a `Stopped` scope row rather than a flood of `Deleted` rows.
func TestRuleDeletionWithdrawsTargets(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("delsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "transient", sinkName, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "transient")
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	var live v1alpha1.StreamRule
	if err := h.Client.Get(context.Background(), client.ObjectKeyFromObject(rule), &live); err != nil {
		t.Fatalf("get the rule: %v", err)
	}
	if len(live.Finalizers) != 0 {
		t.Errorf("the rule carries finalizers %v; the design is deliberately finalizer-free", live.Finalizers)
	}
	if err := h.Client.Delete(context.Background(), &live); err != nil {
		t.Fatalf("delete the rule: %v", err)
	}

	h.waitForTargets(ruleKey, nil)
}

// TestRuleGaugeCountsDegradedRules is the metric half of the "degraded rules"
// panel and the Ready=False alert: the count the dashboard reads has to follow a
// real rule through degrading, recovering and being deleted.
//
// It runs against the reconcilers rather than against RuleMetrics directly
// because the interesting part is the wiring — a gauge that is never observed, or
// never forgotten, is unit-test-green and operationally useless.
func TestRuleGaugeCountsDegradedRules(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("gaugesink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)

	// A rule naming v1/Secret is refused outright (D8), which is the cheapest way
	// to get a rule that is genuinely Ready=False rather than merely slow.
	denied := h.newStreamRule(namespace, "denied", sinkName, resourceEntry("", "Secret"))
	h.waitForRuleCondition(denied, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSecretsDenied)
	h.waitForReadyGauge("false", 1)

	// A healthy rule alongside it counts into the other series, and does not
	// disturb the degraded count.
	healthy := h.newStreamRule(namespace, "healthy", sinkName, resourceEntry("", "ConfigMap"))
	h.waitForRuleCondition(healthy, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForReadyGauge("true", 1)
	h.waitForReadyGauge("false", 1)

	// Deleting the degraded rule is how an operator makes the alert stop; if the
	// count survived the delete, it never would.
	var live v1alpha1.StreamRule
	if err := h.Client.Get(context.Background(), client.ObjectKeyFromObject(denied), &live); err != nil {
		t.Fatalf("get the denied rule: %v", err)
	}
	if err := h.Client.Delete(context.Background(), &live); err != nil {
		t.Fatalf("delete the denied rule: %v", err)
	}
	h.waitForReadyGauge("false", 0)
	h.waitForReadyGauge("true", 1)
}

// TestParkerWakesDependentRules covers the sink runtime's park callback end to end: a
// rule key handed back by the runtime must turn into a reconcile of that exact rule.
func TestParkerWakesDependentRules(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("parksink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "parked", sinkName, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "parked")
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	// Delete the sink out from under the rule *without* letting the rule's own sink
	// watch see it first: the sink is removed and the runtime's callback is what
	// reports it. (Both paths lead to the same verdict; this asserts the callback
	// path exists and carries a parseable key.)
	var chSink v1alpha1.ClickHouseSink
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: sinkName}, &chSink); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if err := h.Client.Delete(context.Background(), &chSink); err != nil {
		t.Fatalf("delete the sink: %v", err)
	}
	h.Parker.SinkGone(clickHouseSinkID(sinkName), []string{ruleKey})

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing)
	h.waitForTargets(ruleKey, nil)
}

// TestParkerRejectsUnknownKeys checks that a key the reconcilers never produced is
// refused rather than turned into a reconcile of some other object. It runs without a
// manager: the Parker is pure translation.
func TestParkerRejectsUnknownKeys(t *testing.T) {
	reconciler := NewStreamRuleReconciler(RuleReconciler{})
	reconciler.events = make(chan event.GenericEvent, 4)
	parker := NewParker(reconciler)

	parker.SinkGone(clickHouseSinkID("sink"), []string{
		"garbage",                // not a key at all
		"deployment/ns/name",     // a well-formed key naming a kind we do not serve
		"clusterstreamrule//csr", // a kind this Parker has no reconciler for
		"streamrule/ns/ok",       // the only one that should get through
	})

	if got := len(reconciler.events); got != 1 {
		t.Fatalf("the parker enqueued %d wake-ups, want exactly the one it could serve", got)
	}
	delivered := <-reconciler.events
	if delivered.Object.GetName() != "ok" || delivered.Object.GetNamespace() != "ns" {
		t.Errorf("delivered %s/%s, want ns/ok",
			delivered.Object.GetNamespace(), delivered.Object.GetName())
	}
}

// TestRulesForNamespaceOnlySelectingRules checks the map function's filter: waking
// every cluster rule on every namespace event would be churn proportional to the
// namespace creation rate times the rule count.
func TestRulesForNamespaceOnlySelectingRules(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("mapsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	selecting := uniqueName("selecting")
	h.newClusterStreamRule(selecting, sinkName,
		&metav1.LabelSelector{MatchLabels: map[string]string{auditLabel: uniqueName("audit")}},
		resourceEntry("", "ConfigMap"))
	everywhere := uniqueName("everywhere")
	h.newClusterStreamRule(everywhere, sinkName, nil, resourceEntry("", "ConfigMap"))

	h.waitForRuleCondition(&v1alpha1.ClusterStreamRule{ObjectMeta: metav1.ObjectMeta{Name: everywhere}},
		v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)

	reconciler := NewClusterStreamRuleReconciler(RuleReconciler{Client: h.Client})
	requests := reconciler.rulesForNamespace(context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "whatever"}})

	names := make([]string, 0, len(requests))
	for _, req := range requests {
		names = append(names, req.Name)
	}
	if !slices.Contains(names, selecting) {
		t.Errorf("the selector-carrying rule %q was not re-enqueued (got %v)", selecting, names)
	}
	if slices.Contains(names, everywhere) {
		t.Errorf("the all-namespaces rule %q was re-enqueued for a namespace change it cannot care about", everywhere)
	}
}

// TestResolverIsSharedWithTheDataPlane documents the wiring property the resolver's
// backoff gate depends on: both reconcilers and the WatchManager must consult one
// Resolver, so a kind that is not installed is retried on one gate rather than two.
func TestResolverIsSharedWithTheDataPlane(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	resolver := watch.NewResolver(nil)
	base := RuleReconciler{Client: h.Client, Registry: h.Registry, Resolver: resolver, Access: h.Reviewer}
	if NewStreamRuleReconciler(base).Resolver != NewClusterStreamRuleReconciler(base).Resolver {
		t.Error("the two rule reconcilers were built with different resolvers")
	}
}

// TestClusterRuleInvalidNamespaceSelector covers the one namespace-expansion failure
// the CRD schema cannot catch: `matchExpressions` carries a free-form operator, so an
// unknown one is only rejected in code. It must degrade the rule (permanently, until
// its author edits it) rather than retry forever, and it must be a distinct reason
// from a namespace listing that merely failed.
func TestClusterRuleInvalidNamespaceSelector(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("badselsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	name := uniqueName("badsel")
	rule := h.newClusterStreamRule(name, sinkName,
		&metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "audit", Operator: "Sideways", Values: []string{"yes"}},
		}},
		resourceEntry("", "ConfigMap"))

	got := h.waitForRuleCondition(rule, v1alpha1.ConditionResourceResolved,
		metav1.ConditionFalse, ReasonNamespaceSelectorInvalid)
	if !strings.Contains(got.Message, "Sideways") {
		t.Errorf("ResourceResolved message %q does not name the operator that could not be honoured", got.Message)
	}
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonNamespaceSelectorInvalid)
	h.waitForTargets(RuleKey(kindClusterStreamRule, "", name), nil)
}
