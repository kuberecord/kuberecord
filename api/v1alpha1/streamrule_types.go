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

// StreamRule declares that the resources it names, *in its own namespace*,
// should be streamed to a ClickHouseSink.
//
// It is namespaced so that streaming intent is delegable: a team with
// write access only to their namespace can opt their own workloads into the
// audit trail without cluster-level privileges, and the watch it creates can
// never reach beyond that namespace. Naming a cluster-scoped kind here is
// therefore invalid and degrades the rule with
// ResourceResolved=False/ClusterScopedKind — use ClusterStreamRule for those.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="READY",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="SINK",type=string,JSONPath=`.spec.sink.name`
// +kubebuilder:printcolumn:name="SINK-KIND",type=string,JSONPath=`.spec.sink.kind`
// +kubebuilder:printcolumn:name="WATCHES",type=integer,JSONPath=`.status.activeWatches`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type StreamRule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of StreamRule.
	// +required
	Spec StreamRuleSpec `json:"spec"`

	// status defines the observed state of StreamRule.
	// +optional
	Status StreamRuleStatus `json:"status,omitempty"`
}

// StreamRuleList contains a list of StreamRule.
// +kubebuilder:object:root=true
type StreamRuleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamRule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StreamRule{}, &StreamRuleList{})
}
