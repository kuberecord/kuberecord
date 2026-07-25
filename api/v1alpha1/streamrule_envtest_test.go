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
// list stays mutable even though sinkRef does not.
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
	return StreamRuleSpec{SinkRef: defaultSinkRef, Resources: resources}
}

// unstructuredRule builds a rule of the given kind directly as unstructured
// JSON. It exists for the one case the typed client cannot express: sinkRef is
// `omitempty`, so a typed object with SinkRef=="" omits the key entirely and
// gets defaulted. Only a literal `sinkRef: ""` in the submitted document — what
// a YAML author would actually write — reaches the MinLength rule.
func unstructuredRule(kind, namespace, sinkRef string) clientObject {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind(kind))
	if namespace != "" {
		u.SetNamespace(namespace)
	}
	if err := unstructured.SetNestedMap(u.Object, map[string]any{
		"sinkRef": sinkRef,
		"resources": []any{map[string]any{
			"group": "apps", "version": "v1", "kind": "Deployment",
		}},
	}, "spec"); err != nil {
		panic("building unstructured rule: " + err.Error())
	}
	return u
}

// defaultSinkRef is the shipped sinkRef default; otherSinkRef is what the
// immutability cases try (and must fail) to move a rule to.
const (
	defaultSinkRef = "default"
	otherSinkRef   = "other-sink"
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
	setSinkRef     func(clientObject)
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
			obj:  withResource(WatchedResource{Group: "kubestream.io", Version: "v1alpha1", Kind: "StreamRule"}),
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
		{
			name:    "explicitly-empty-sinkref-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, ""),
			wantErr: "should be at least 1 chars long",
		},
		{
			name: "explicit-sinkref-is-accepted",
			obj:  unstructuredRule(e.kind, e.namespace, otherSinkRef),
		},
		{
			name:    "sinkref-mutation-is-rejected",
			obj:     e.build(ruleSpec(deploymentResource())),
			mutate:  e.setSinkRef,
			wantErr: "sinkRef is immutable",
		},
		{
			name:   "resources-remain-mutable",
			obj:    e.build(ruleSpec(deploymentResource())),
			mutate: e.appendResource,
		},
	}
}

func TestStreamRuleValidation(t *testing.T) {
	runAPICases(t, ruleValidationCases(ruleEditor{
		kind:      "StreamRule",
		namespace: testNamespace,
		build: func(spec StreamRuleSpec) clientObject {
			return &StreamRule{ObjectMeta: objectMeta(testNamespace), Spec: spec}
		},
		setSinkRef: func(o clientObject) {
			o.(*StreamRule).Spec.SinkRef = otherSinkRef
		},
		appendResource: func(o clientObject) {
			r := o.(*StreamRule)
			r.Spec.Resources = append(r.Spec.Resources, serviceResource())
		},
	}))
}

// TestStreamRuleLabelSelectorAndDefaults proves the two things a pattern check
// cannot: that the optional per-resource label selector is actually wired
// through to storage, and that sinkRef defaults to "default" when omitted —
// which is what keeps rules in a single-sink cluster boilerplate-free.
func TestStreamRuleLabelSelectorAndDefaults(t *testing.T) {
	ctx := context.Background()
	rule := &StreamRule{
		ObjectMeta: objectMeta(testNamespace),
		Spec: StreamRuleSpec{
			// sinkRef deliberately omitted: the apiserver must default it.
			Resources: []WatchedResource{{
				Group: "", Version: "v1", Kind: "ConfigMap",
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"kubestream.io/audit": "true"},
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
	if got.Spec.SinkRef != defaultSinkRef {
		t.Errorf("sinkRef defaulted to %q, want %q", got.Spec.SinkRef, defaultSinkRef)
	}
	sel := got.Spec.Resources[0].LabelSelector
	if sel == nil || sel.MatchLabels["kubestream.io/audit"] != "true" {
		t.Errorf("labelSelector did not round-trip: %+v", sel)
	}
}
