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
	"k8s.io/apimachinery/pkg/types"
)

// TestClusterStreamRuleValidation runs the shared rule table against
// ClusterStreamRule. Passing it is the proof that inlining StreamRuleSpec
// carried every field rule — including the sink reference's oldSelf transition
// rule — into this CRD's generated schema.
func TestClusterStreamRuleValidation(t *testing.T) {
	runAPICases(t, ruleValidationCases(ruleEditor{
		kind: "ClusterStreamRule",
		build: func(spec StreamRuleSpec) clientObject {
			// Cluster-scoped: no namespace.
			return &ClusterStreamRule{
				ObjectMeta: objectMeta(""),
				Spec:       ClusterStreamRuleSpec{StreamRuleSpec: spec},
			}
		},
		setSinkName: func(o clientObject) {
			o.(*ClusterStreamRule).Spec.Sink.Name = otherSinkName
		},
		setSinkKind: func(o clientObject) {
			o.(*ClusterStreamRule).Spec.Sink.Kind = otherSinkKind
		},
		appendResource: func(o clientObject) {
			r := o.(*ClusterStreamRule)
			r.Spec.Resources = append(r.Spec.Resources, serviceResource())
		},
	}))
}

// TestClusterStreamRuleNamespaceSelector covers the one field this CRD adds.
// It asserts both directions: the selector round-trips when set, and stays nil
// when omitted — nil is load-bearing, it means "every namespace, including ones
// created later", which is a different statement from an empty selector.
func TestClusterStreamRuleNamespaceSelector(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		selector *metav1.LabelSelector
		wantNil  bool
	}{
		{
			name:    "omitted-selector-stays-nil",
			wantNil: true,
		},
		{
			name: "matchexpressions-selector-round-trips",
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "kubernetes.io/metadata.name",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{"kube-system"},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ClusterStreamRule{
				ObjectMeta: objectMeta(""),
				Spec: ClusterStreamRuleSpec{
					// A cluster-scoped Kind: only this CRD may name one.
					StreamRuleSpec:    ruleSpec(WatchedResource{Group: "", Version: "v1", Kind: "Node"}),
					NamespaceSelector: tt.selector,
				},
			}
			rule.SetName(objectNameFor(tt.name))
			if err := k8sClient.Create(ctx, rule); err != nil {
				t.Fatalf("creating rule: %v", err)
			}
			defer deleteObject(ctx, t, rule)

			got := &ClusterStreamRule{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: rule.Name}, got); err != nil {
				t.Fatalf("reading rule back: %v", err)
			}
			if tt.wantNil {
				if got.Spec.NamespaceSelector != nil {
					t.Errorf("namespaceSelector should stay nil, got %+v", got.Spec.NamespaceSelector)
				}
				return
			}
			if got.Spec.NamespaceSelector == nil || len(got.Spec.NamespaceSelector.MatchExpressions) != 1 {
				t.Fatalf("namespaceSelector did not round-trip: %+v", got.Spec.NamespaceSelector)
			}
		})
	}
}
