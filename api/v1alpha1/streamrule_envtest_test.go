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

package v1alpha1

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// deploymentResource is the canonical valid WatchedResource the rule tables
// start from. Cases testing a GVK rule swap in a deliberately broken variant so
// a failure names the rule that fired, not "something in this object is wrong".
func deploymentResource() WatchedResource {
	return WatchedResource{Group: "apps", Version: "v1", Kind: "Deployment"}
}

// serviceResource is a second valid resource, used to prove that the resources
// list stays mutable even though the sink reference does not.
func serviceResource() WatchedResource {
	return WatchedResource{Group: "", Version: "v1", Kind: "Service"}
}

// ruleSpec returns a StreamRuleSpec the apiserver must accept. Called with no
// arguments it yields an explicitly *empty* (not nil) resources list, so the
// emitted JSON is `[]` and the MinItems rule is what rejects it — a nil slice
// would serialise to `null` and trip the weaker "Required value" check instead,
// leaving MinItems untested.
func ruleSpec(resources ...WatchedResource) StreamRuleSpec {
	if resources == nil {
		resources = []WatchedResource{}
	}
	return StreamRuleSpec{Sink: SinkReference{Name: defaultSinkName}, Resources: resources}
}

// unstructuredRule builds a rule of the given kind directly as unstructured
// JSON, with sink as its `spec.sink` — or with no `sink` key at all when sink is
// nil.
//
// It exists for the cases the typed client cannot express. SinkReference marshals
// both of its fields, so a typed object always submits a `sink` object carrying a
// name, which is exactly what the required-field and MinLength rules are there to
// reject. Only a document that omits the key, or spells `name: ""`, reaches them —
// and those are the documents a YAML author actually writes: an omitted `sink` is
// what a rule migrated from v0.1.0's `sinkRef` looks like on the way in.
func unstructuredRule(kind, namespace string, sink map[string]any) clientObject {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind(kind))
	if namespace != "" {
		u.SetNamespace(namespace)
	}
	spec := map[string]any{
		"resources": []any{map[string]any{
			"group": "apps", "version": "v1", "kind": "Deployment",
		}},
	}
	if sink != nil {
		spec["sink"] = sink
	}
	if err := unstructured.SetNestedMap(u.Object, spec, "spec"); err != nil {
		panic("building unstructured rule: " + err.Error())
	}
	return u
}

// The sink names and kinds the table below references.
//
// defaultSinkKind is the kind an unqualified reference defaults to; otherSinkKind
// is the second kind this build serves (Task 6.1), which is what makes a *kind*
// change a legal edit to attempt and therefore what finally exercises the sink
// reference's immutability rule rather than its enum. unknownSinkKind is a kind
// only a later release serves (D6/D13 put Postgres in v0.3.0), used here to prove
// that this one refuses to admit it.
const (
	defaultSinkName = "default"
	otherSinkName   = "other-sink"
	defaultSinkKind = "ClickHouseSink"
	otherSinkKind   = "S3Sink"
	unknownSinkKind = "PostgresSink"
)

// ruleEditor bundles the two CRD-specific operations the shared rule table
// needs: how to build the concrete CRD around a StreamRuleSpec, and how to
// perform each in-place edit on it.
//
// The table below is shared between StreamRule and ClusterStreamRule rather
// than duplicated because "a ClusterStreamRule validates its inlined spec
// exactly like a StreamRule does" is precisely the property that inlining
// StreamRuleSpec is supposed to guarantee. Asserting it from one table makes a
// regression in either CRD impossible to miss.
type ruleEditor struct {
	// kind and namespace let the table build the same rule as unstructured
	// JSON where the typed client cannot express the case under test.
	kind           string
	namespace      string
	build          func(StreamRuleSpec) clientObject
	setSinkName    func(clientObject)
	setSinkKind    func(clientObject)
	appendResource func(clientObject)
}

// ruleValidationCases builds the admission expectations both rule CRDs must
// satisfy identically.
func ruleValidationCases(e ruleEditor) []apiCase {
	withResource := func(r WatchedResource) clientObject { return e.build(ruleSpec(r)) }
	return []apiCase{
		{
			name: "minimal-valid-rule-is-accepted",
			obj:  e.build(ruleSpec(deploymentResource())),
		},
		{
			name: "core-group-resource-is-accepted",
			obj:  e.build(ruleSpec(WatchedResource{Group: "", Version: "v1", Kind: "ConfigMap"})),
		},
		{
			name: "alpha-version-is-accepted",
			obj:  withResource(WatchedResource{Group: "kuberecord.io", Version: "v1alpha1", Kind: "StreamRule"}),
		},
		{
			name:    "empty-resources-is-rejected",
			obj:     e.build(ruleSpec()),
			wantErr: "should have at least 1 items",
		},
		{
			name:    "lowercase-kind-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "v1", Kind: "deployment"}),
			wantErr: "should match",
		},
		{
			name:    "plural-resource-name-as-kind-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "v1", Kind: "deployments"}),
			wantErr: "should match",
		},
		{
			name:    "bad-version-string-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "apps/v1", Kind: "Deployment"}),
			wantErr: "should match",
		},
		{
			name:    "non-dns-group-is-rejected",
			obj:     withResource(WatchedResource{Group: "Apps", Version: "v1", Kind: "Deployment"}),
			wantErr: "should match",
		},
		// The sink reference (Task 4.3). A rule with no sink at all is what a
		// v0.1.0 rule looks like once `sinkRef` is an unknown field, so rejecting
		// it on write is half of the migration story — the other half is the
		// reconciler guard for the ones already in etcd, which admission cannot
		// reach.
		{
			name:    "missing-sink-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, nil),
			wantErr: "spec.sink: Required value",
		},
		{
			name:    "sink-without-a-name-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, map[string]any{}),
			wantErr: "spec.sink.name: Required value",
		},
		{
			name:    "explicitly-empty-sink-name-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, map[string]any{"name": ""}),
			wantErr: "should be at least 1 chars long",
		},
		{
			// The enum is what keeps a kind this build does not serve from being
			// admitted and then parking forever with no backend behind it.
			name: "unknown-sink-kind-is-rejected",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": unknownSinkKind, "name": defaultSinkName,
			}),
			wantErr: "Unsupported value",
		},
		{
			// The accepting direction of the same rule, which is what stops the
			// enum from being widened in the Go marker and nowhere else: every kind
			// this build serves must be spellable.
			name: "the-second-served-sink-kind-is-accepted",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": otherSinkKind, "name": defaultSinkName,
			}),
		},
		{
			name: "fully-spelled-sink-is-accepted",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": defaultSinkKind, "name": otherSinkName,
			}),
		},
		{
			// The kind may be omitted; the name may not. What the omission is
			// defaulted *to* is asserted by TestStreamRuleLabelSelectorAndDefaults.
			name: "sink-name-alone-is-accepted",
			obj:  unstructuredRule(e.kind, e.namespace, map[string]any{"name": otherSinkName}),
		},
		{
			name:    "sink-name-mutation-is-rejected",
			obj:     e.build(ruleSpec(deploymentResource())),
			mutate:  e.setSinkName,
			wantErr: "sink is immutable",
		},
		{
			// Now that Task 6.1 has added a second served kind, this edit is
			// structurally valid and the *immutability* rule is what refuses it —
			// which is the expectation that was unreachable while the enum held one
			// value and the apiserver rejected the spelling before ever evaluating a
			// transition rule.
			//
			// It is also the edit with the worst consequences if it were allowed:
			// re-pointing a live rule from a ClickHouseSink to an S3Sink would strand
			// the dedup baseline the pipeline built for every object in scope, and
			// silently move that rule onto a backend that can never reconstruct
			// history (D12).
			name:    "sink-kind-mutation-is-rejected",
			obj:     e.build(ruleSpec(deploymentResource())),
			mutate:  e.setSinkKind,
			wantErr: "sink is immutable",
		},
		{
			name:   "resources-remain-mutable",
			obj:    e.build(ruleSpec(deploymentResource())),
			mutate: e.appendResource,
		},
		// Redaction path syntax (Task 3.3). The rejections are what stops a
		// malformed policy from reaching the data plane, where the only remaining
		// options would be to degrade the rule silently or to stream unredacted.
		{
			name: "redaction-field-path-is-accepted",
			obj:  e.build(redactingSpec(RedactionRule{FieldPath: "data.password"})),
		},
		{
			name: "redaction-array-wildcard-is-accepted",
			obj: e.build(redactingSpec(RedactionRule{
				FieldPath: "spec.template.spec.containers[*].env[*].value",
			})),
		},
		{
			name: "redaction-annotation-shorthand-is-accepted",
			obj:  e.build(redactingSpec(RedactionRule{Annotation: "my.company.io/api-token"})),
		},
		{
			name: "redaction-annotation-shorthand-accepts-a-dotted-key",
			obj: e.build(redactingSpec(RedactionRule{
				Annotation: "kubectl.kubernetes.io/last-applied-configuration",
			})),
		},
		{
			name:    "redaction-with-both-fields-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "data.password", Annotation: "token"})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "redaction-with-neither-field-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "redaction-indexed-path-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "spec.containers[0].name"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-leading-dot-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: ".data.password"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-trailing-dot-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "data.password."})),
			wantErr: "should match",
		},
		{
			name:    "redaction-jsonpath-syntax-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "$.data.password"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-quoted-segment-is-rejected-in-a-field-path",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: `metadata.annotations["token"]`})),
			wantErr: "should match",
		},
		{
			// A key that could close the quote the data plane renders it into
			// (see pipeline.AnnotationRedactionPath) would let an author express
			// a path they did not write.
			name:    "redaction-annotation-with-a-quote-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{Annotation: `to"ken`})),
			wantErr: "should match",
		},
		{
			name:    "redaction-empty-field-path-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: ""})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
	}
}

// redactingSpec is a valid rule spec whose extraRedaction is exactly rule, so a
// rejection names the redaction rule under test and nothing else.
func redactingSpec(rule RedactionRule) StreamRuleSpec {
	spec := ruleSpec(deploymentResource())
	spec.ExtraRedaction = []RedactionRule{rule}
	return spec
}

func TestStreamRuleValidation(t *testing.T) {
	runAPICases(t, ruleValidationCases(ruleEditor{
		kind:      "StreamRule",
		namespace: testNamespace,
		build: func(spec StreamRuleSpec) clientObject {
			return &StreamRule{ObjectMeta: objectMeta(testNamespace), Spec: spec}
		},
		setSinkName: func(o clientObject) {
			o.(*StreamRule).Spec.Sink.Name = otherSinkName
		},
		setSinkKind: func(o clientObject) {
			o.(*StreamRule).Spec.Sink.Kind = otherSinkKind
		},
		appendResource: func(o clientObject) {
			r := o.(*StreamRule)
			r.Spec.Resources = append(r.Spec.Resources, serviceResource())
		},
	}))
}

// TestStreamRuleLabelSelectorAndDefaults proves the two things a pattern check
// cannot: that the optional per-resource label selector is actually wired
// through to storage, and that sink.kind defaults to ClickHouseSink when a rule
// names only a sink name — which is what keeps rules in a ClickHouse-only
// cluster free of a kind they have no alternative for.
func TestStreamRuleLabelSelectorAndDefaults(t *testing.T) {
	ctx := context.Background()
	rule := &StreamRule{
		ObjectMeta: objectMeta(testNamespace),
		Spec: StreamRuleSpec{
			// sink.kind deliberately omitted: the apiserver must default it.
			Sink: SinkReference{Name: defaultSinkName},
			Resources: []WatchedResource{{
				Group: "", Version: "v1", Kind: "ConfigMap",
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"kuberecord.io/audit": "true"},
				},
			}},
		},
	}
	rule.SetName("selector-and-defaults-rule")
	if err := k8sClient.Create(ctx, rule); err != nil {
		t.Fatalf("creating rule: %v", err)
	}
	defer deleteObject(ctx, t, rule)

	got := &StreamRule{}
	key := types.NamespacedName{Name: rule.Name, Namespace: rule.Namespace}
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatalf("reading rule back: %v", err)
	}
	if got.Spec.Sink.Kind != defaultSinkKind {
		t.Errorf("sink.kind defaulted to %q, want %q", got.Spec.Sink.Kind, defaultSinkKind)
	}
	if got.Spec.Sink.Name != defaultSinkName {
		t.Errorf("sink.name round-tripped as %q, want %q", got.Spec.Sink.Name, defaultSinkName)
	}
	sel := got.Spec.Resources[0].LabelSelector
	if sel == nil || sel.MatchLabels["kuberecord.io/audit"] != "true" {
		t.Errorf("labelSelector did not round-trip: %+v", sel)
	}
}
