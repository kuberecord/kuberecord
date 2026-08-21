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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterStreamRuleSpec is a StreamRuleSpec that additionally chooses which
// namespaces it applies to.
//
// StreamRuleSpec is embedded inline rather than copied so the two rule CRDs
// cannot drift apart field-by-field, and so every validation rule written on
// StreamRuleSpec's fields (including the sink reference's immutability) is
// inherited verbatim by this CRD's generated schema.
type ClusterStreamRuleSpec struct {
	StreamRuleSpec `json:",inline"`

	// NamespaceSelector chooses the namespaces this rule applies to. Nil means
	// every namespace, including namespaces created after the rule.
	//
	// It selects on *namespace* labels, not object labels — use
	// resources[].labelSelector for the latter. A selector matching no
	// namespace is legal and leaves the rule Ready with activeWatches=0; it is
	// a normal transient state while the namespaces that will match are still
	// being created.
	//
	// Cluster-scoped kinds named by this rule ignore the selector entirely:
	// they have no namespace to select on. ClusterStreamRule is the only type
	// permitted to name them.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}

// ClusterStreamRule declares that the resources it names, across the
// namespaces its selector matches, should be streamed to a ClickHouseSink.
//
// It is cluster-scoped (D6) and is the only rule type permitted to name
// cluster-scoped kinds (Nodes, Namespaces, CRDs …), because doing so requires
// cluster-level authority the namespaced StreamRule deliberately does not
// assume. Its status uses the same StreamRuleStatus shape as StreamRule, so a
// single set of condition semantics covers both.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="READY",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="SINK",type=string,JSONPath=`.spec.sink.name`
// +kubebuilder:printcolumn:name="SINK-KIND",type=string,JSONPath=`.spec.sink.kind`
// +kubebuilder:printcolumn:name="WATCHES",type=integer,JSONPath=`.status.activeWatches`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type ClusterStreamRule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ClusterStreamRule.
	// +required
	Spec ClusterStreamRuleSpec `json:"spec"`

	// status defines the observed state of ClusterStreamRule.
	// +optional
	Status StreamRuleStatus `json:"status,omitempty"`
}

// ClusterStreamRuleList contains a list of ClusterStreamRule.
// +kubebuilder:object:root=true
type ClusterStreamRuleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterStreamRule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterStreamRule{}, &ClusterStreamRuleList{})
}
