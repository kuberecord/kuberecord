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

package cli_test

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/restmapper"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/query"
)

// testMapper builds the same shape of REST mapper the CLI uses in production: a
// discovery-backed mapper wrapped in restmapper.ShortcutExpander.
//
// The expander is not a convenience here, it is the subject. Short names are not
// a hard-coded table in this codebase or in client-go's — they come from each
// resource's ShortNames in the server's own discovery data — so a test that
// resolved `deploy` against a hand-written alias map would prove nothing about
// what happens against a cluster. Declaring the short names on the fake's
// APIResources is what makes these assertions transferable.
func testMapper(t *testing.T) meta.RESTMapper {
	t.Helper()

	resources := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{
					Name: "configmaps", SingularName: "configmap", Namespaced: true,
					Kind: "ConfigMap", ShortNames: []string{"cm"},
				},
				{
					Name: "nodes", SingularName: "node", Namespaced: false,
					Kind: "Node", ShortNames: []string{"no"},
				},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{
					Name: "deployments", SingularName: "deployment", Namespaced: true,
					Kind: "Deployment", ShortNames: []string{"deploy"},
				},
				{
					Name: "statefulsets", SingularName: "statefulset", Namespaced: true,
					Kind: "StatefulSet", ShortNames: []string{"sts"},
				},
			},
		},
		{
			GroupVersion: "networking.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{
					Name: "ingresses", SingularName: "ingress", Namespaced: true,
					Kind: "Ingress", ShortNames: []string{"ing"},
				},
			},
		},
		{
			GroupVersion: "kuberecord.io/v1alpha1",
			APIResources: []metav1.APIResource{
				{
					Name: "clusterstreamrules", SingularName: "clusterstreamrule", Namespaced: false,
					Kind: "ClusterStreamRule",
				},
			},
		},
	}

	discovery := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
	groups, err := restmapper.GetAPIGroupResources(discovery)
	if err != nil {
		t.Fatalf("build API group resources: %v", err)
	}
	return restmapper.NewShortcutExpander(
		restmapper.NewDiscoveryRESTMapper(groups), discovery, func(string) {})
}

// TestParseResourceArg covers every address spelling the CLI accepts and the
// shapes it refuses.
//
// Parsing is offline on purpose: a malformed address is a usage error a user
// should get instantly, not after a discovery round-trip.
func TestParseResourceArg(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantResource string
		wantName     string
		wantErr      bool
	}{
		{name: "slash form", args: []string{"deploy/nginx"}, wantResource: "deploy", wantName: "nginx"},
		{name: "space form", args: []string{"deploy", "nginx"}, wantResource: "deploy", wantName: "nginx"},
		{
			name: "group-qualified slash form", args: []string{"deployments.apps/nginx"},
			wantResource: "deployments.apps", wantName: "nginx",
		},
		{
			name: "group-qualified space form", args: []string{"deployments.apps", "nginx"},
			wantResource: "deployments.apps", wantName: "nginx",
		},
		{
			name: "a name may contain dots", args: []string{"ing/www.example.com"},
			wantResource: "ing", wantName: "www.example.com",
		},
		{name: "no arguments", args: nil, wantErr: true},
		{name: "kind with no name", args: []string{"deploy"}, wantErr: true},
		{name: "empty kind", args: []string{"/nginx"}, wantErr: true},
		{name: "empty name", args: []string{"deploy/"}, wantErr: true},
		{name: "two slashes", args: []string{"deploy/nginx/extra"}, wantErr: true},
		{name: "mixed forms", args: []string{"deploy/nginx", "other"}, wantErr: true},
		{name: "three arguments", args: []string{"deploy", "a", "b"}, wantErr: true},
		{name: "empty strings", args: []string{"", ""}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := cli.ParseResourceArg(test.args)

			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseResourceArg(%q) = %+v, want a usage error", test.args, got)
				}
				// A malformed address must never be a runtime error: a script
				// that retries on failure should not retry a typo.
				if code := cli.ExitCodeFor(err); code != cli.ExitUsageError {
					t.Errorf("ParseResourceArg(%q) error carries exit code %d, want %d (%v)",
						test.args, code, cli.ExitUsageError, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseResourceArg(%q): %v", test.args, err)
			}
			if got.Resource != test.wantResource || got.Name != test.wantName {
				t.Errorf("ParseResourceArg(%q) = {%q, %q}, want {%q, %q}",
					test.args, got.Resource, got.Name, test.wantResource, test.wantName)
			}
		})
	}
}

// TestResolve covers resolution against discovery data: short names,
// group-qualified resources, singular and plural spellings, kinds, and both
// scopes.
func TestResolve(t *testing.T) {
	mapper := testMapper(t)
	resolver := cli.NewResolver(mapper)

	tests := []struct {
		name           string
		resource       string
		wantGVK        schema.GroupVersionKind
		wantResource   string
		wantNamespaced bool
	}{
		{
			name: "short name", resource: "deploy",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			name: "short name sts", resource: "sts",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"},
			wantResource: "statefulsets", wantNamespaced: true,
		},
		{
			name: "short name cm in the core group", resource: "cm",
			wantGVK:      schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			wantResource: "configmaps", wantNamespaced: true,
		},
		{
			name: "short name ing in a dotted group", resource: "ing",
			wantGVK: schema.GroupVersionKind{
				Group: "networking.k8s.io", Version: "v1", Kind: "Ingress",
			},
			wantResource: "ingresses", wantNamespaced: true,
		},
		{
			name: "plural resource", resource: "deployments",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			name: "singular resource", resource: "deployment",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			name: "group-qualified", resource: "deployments.apps",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			name: "fully specified resource.version.group", resource: "deployments.v1.apps",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			// kubectl accepts a kind spelled as a user thinks of it, and so must
			// this: an engineer reading a StreamRule sees "Deployment", not
			// "deployments".
			name: "kind spelling, capitalised", resource: "Deployment",
			wantGVK:      schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantResource: "deployments", wantNamespaced: true,
		},
		{
			name: "cluster-scoped kind", resource: "no",
			wantGVK:      schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
			wantResource: "nodes", wantNamespaced: false,
		},
		{
			name: "a kuberecord CRD, cluster-scoped", resource: "clusterstreamrules.kuberecord.io",
			wantGVK: schema.GroupVersionKind{
				Group: "kuberecord.io", Version: "v1alpha1", Kind: "ClusterStreamRule",
			},
			wantResource: "clusterstreamrules", wantNamespaced: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolver.Resolve(cli.ResourceArg{Resource: test.resource, Name: "example"})
			if err != nil {
				t.Fatalf("Resolve(%q): %v", test.resource, err)
			}

			if got.GVK != test.wantGVK {
				t.Errorf("Resolve(%q).GVK = %v, want %v", test.resource, got.GVK, test.wantGVK)
			}
			if got.GVR.Resource != test.wantResource {
				t.Errorf("Resolve(%q).GVR.Resource = %q, want %q",
					test.resource, got.GVR.Resource, test.wantResource)
			}
			if got.Namespaced != test.wantNamespaced {
				t.Errorf("Resolve(%q).Namespaced = %t, want %t",
					test.resource, got.Namespaced, test.wantNamespaced)
			}
			if got.Name != "example" {
				t.Errorf("Resolve(%q).Name = %q, want %q", test.resource, got.Name, "example")
			}
		})
	}
}

// TestResolveShortNameMatchesTheLongForm is the "resolves identically on both
// sides" criterion stated as a property rather than as two hard-coded
// expectations: whatever `deploy` means, it must mean exactly what
// `deployments.apps` means.
func TestResolveShortNameMatchesTheLongForm(t *testing.T) {
	resolver := cli.NewResolver(testMapper(t))

	short, err := resolver.Resolve(cli.ResourceArg{Resource: "deploy", Name: "nginx"})
	if err != nil {
		t.Fatalf("Resolve(deploy): %v", err)
	}
	long, err := resolver.Resolve(cli.ResourceArg{Resource: "deployments.apps", Name: "nginx"})
	if err != nil {
		t.Fatalf("Resolve(deployments.apps): %v", err)
	}

	if short != long {
		t.Errorf("deploy resolved to %+v but deployments.apps resolved to %+v", short, long)
	}
}

// TestResolveUnknownResource covers the kind this cluster does not serve.
//
// Both shapes matter and they arrive differently from the REST mapper: an
// unknown resource inside a known group, and an entirely unknown group. The
// second is the common case — a rule or a query naming a CRD that is not
// installed — and it is the one a type assertion on *meta.NoKindMatchError would
// misclassify as a discovery outage, which is why the operator's resolver routes
// through meta.IsNoMatchError and why this copy does too.
func TestResolveUnknownResource(t *testing.T) {
	resolver := cli.NewResolver(testMapper(t))

	tests := []struct {
		name     string
		resource string
	}{
		{name: "unknown resource in a known group", resource: "widgets"},
		{name: "unknown group", resource: "widgets.example.com"},
		{name: "unknown kind spelling", resource: "Widget"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolver.Resolve(cli.ResourceArg{Resource: test.resource, Name: "x"})
			if err == nil {
				t.Fatalf("Resolve(%q) succeeded, want a not-served error", test.resource)
			}

			var unknown *cli.UnknownResourceError
			if !errors.As(err, &unknown) {
				t.Fatalf("Resolve(%q) = %v, want an *cli.UnknownResourceError", test.resource, err)
			}
			// A kind the cluster does not serve is a well-formed request that
			// could not be carried out, which is exit 1 and not exit 2 — the
			// same classification kubectl makes.
			if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
				t.Errorf("exit code for %q = %d, want %d", test.resource, code, cli.ExitRuntimeError)
			}
			if unknown.Unwrap() == nil {
				t.Error("the mapper's own verdict was dropped, so -v can show nothing useful")
			}
		})
	}
}

// TestObjectRef covers the translation into the read plane's canonical identity,
// and in particular that a cluster-scoped kind carries no namespace.
//
// A cluster-scoped reference carrying a namespace would match nothing in
// recorded history, and "matched nothing" is the answer this project treats as
// most dangerous — it reads as "nothing happened" (Invariant 9).
func TestObjectRef(t *testing.T) {
	resolver := cli.NewResolver(testMapper(t))

	tests := []struct {
		name      string
		resource  string
		namespace string
		want      query.ObjectRef
	}{
		{
			name: "namespaced kind keeps the namespace", resource: "deploy", namespace: "payments",
			want: query.ObjectRef{
				ClusterID: "prod-eu", APIGroup: "apps", Kind: "Deployment",
				Namespace: "payments", Name: "nginx",
			},
		},
		{
			name: "core group is the empty string", resource: "cm", namespace: "payments",
			want: query.ObjectRef{
				ClusterID: "prod-eu", APIGroup: "", Kind: "ConfigMap",
				Namespace: "payments", Name: "nginx",
			},
		},
		{
			name: "cluster-scoped kind drops the namespace", resource: "no", namespace: "payments",
			want: query.ObjectRef{
				ClusterID: "prod-eu", APIGroup: "", Kind: "Node",
				Namespace: "", Name: "nginx",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolver.Resolve(cli.ResourceArg{Resource: test.resource, Name: "nginx"})
			if err != nil {
				t.Fatalf("Resolve(%q): %v", test.resource, err)
			}
			if got := resolved.ObjectRef("prod-eu", test.namespace); got != test.want {
				t.Errorf("ObjectRef = %+v, want %+v", got, test.want)
			}
		})
	}
}
