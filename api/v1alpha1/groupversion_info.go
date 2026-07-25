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

// Package v1alpha1 contains the kubestream.io/v1alpha1 API types: the three
// CRDs (D6) that replace the operator's env-var configuration with declarative,
// two-tier intent — a ClickHouseSink says *where* state goes, a StreamRule /
// ClusterStreamRule says *what* to stream there.
//
// These types are the operator's public UX and are validated entirely by CRD
// structural schemas and CEL (`x-kubernetes-validations`) — never by an
// admission webhook (D4), so kubestream installs with zero external
// dependencies and no cert-manager. Every rule that a webhook would normally
// enforce either lives on a field marker in this package or is explicitly
// documented as reconciler-validated.
//
// The package deliberately holds no behavior beyond generated deepcopy and
// scheme registration: reconcilers (internal/controller), the desired-state
// registry (internal/plan), and the data plane (internal/watch) consume these
// types but never the reverse.
// +kubebuilder:object:generate=true
// +groupName=kubestream.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "kubestream.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
