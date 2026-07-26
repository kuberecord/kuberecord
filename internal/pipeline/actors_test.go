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

package pipeline

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// managedField builds a single managedFields entry with the given manager name,
// mirroring the shape the API server produces (only the manager key matters to
// ExtractActors, but the surrounding keys keep the fixtures realistic).
func managedField(manager string) map[string]any {
	return map[string]any{
		"manager":    manager,
		"operation":  "Update",
		"apiVersion": "v1",
	}
}

func TestExtractActors(t *testing.T) {
	tests := []struct {
		name          string
		managedFields []any // nil means the key is absent entirely
		absent        bool
		want          []string
	}{
		{
			name:   "absent managedFields yields empty slice",
			absent: true,
			want:   []string{},
		},
		{
			name:          "nil managedFields yields empty slice",
			managedFields: nil,
			want:          []string{},
		},
		{
			name:          "empty managedFields yields empty slice",
			managedFields: []any{},
			want:          []string{},
		},
		{
			name: "single manager",
			managedFields: []any{
				managedField("argocd-controller"),
			},
			want: []string{"argocd-controller"},
		},
		{
			name: "duplicate managers are deduped",
			managedFields: []any{
				managedField("kubectl-client-side-apply"),
				managedField("kubectl-client-side-apply"),
				managedField("kubectl-client-side-apply"),
			},
			want: []string{"kubectl-client-side-apply"},
		},
		{
			name: "mixed managers are sorted",
			managedFields: []any{
				managedField("kube-controller-manager"),
				managedField("argocd-controller"),
				managedField("kubectl-client-side-apply"),
			},
			want: []string{"argocd-controller", "kube-controller-manager", "kubectl-client-side-apply"},
		},
		{
			name: "empty manager maps to unknown",
			managedFields: []any{
				managedField(""),
			},
			want: []string{"unknown"},
		},
		{
			name: "missing manager key maps to unknown",
			managedFields: []any{
				map[string]any{"operation": "Update"},
			},
			want: []string{"unknown"},
		},
		{
			name: "non-string manager maps to unknown",
			managedFields: []any{
				map[string]any{"manager": 42},
			},
			want: []string{"unknown"},
		},
		{
			name: "empty and named managers coexist",
			managedFields: []any{
				managedField("argocd-controller"),
				managedField(""),
			},
			want: []string{"argocd-controller", "unknown"},
		},
		{
			name: "malformed non-map entry is skipped, others harvested",
			managedFields: []any{
				managedField("argocd-controller"),
				"i am not a map",
				managedField("kube-controller-manager"),
			},
			want: []string{"argocd-controller", "kube-controller-manager"},
		},
		{
			name: "only malformed entries yield empty slice",
			managedFields: []any{
				"not a map",
				12345,
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata":   map[string]any{"name": "example"},
			}}
			if !tt.absent {
				meta := obj.Object["metadata"].(map[string]any)
				meta["managedFields"] = tt.managedFields
			}

			got := ExtractActors(obj)
			if got == nil {
				t.Fatalf("ExtractActors returned nil, want non-nil slice")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractActors() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestExtractActorsDoesNotMutate proves ExtractActors only reads the object:
// managedFields must still be present afterwards, so the caller — which relies
// on stripping it itself, immediately after — sees an unperturbed object.
func TestExtractActorsDoesNotMutate(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": "example",
			"managedFields": []any{
				managedField("argocd-controller"),
			},
		},
	}}

	_ = ExtractActors(obj)

	if _, found, _ := unstructured.NestedSlice(obj.Object, "metadata", "managedFields"); !found {
		t.Fatalf("ExtractActors removed managedFields; it must only read the object")
	}
}

// normalizedHash runs the production normalization + hashing, so the regression
// tests below assert on exactly the bytes the pipeline hashes rather than on a
// test-local re-implementation that could drift away from it.
func normalizedHash(t *testing.T, obj *unstructured.Unstructured) string {
	t.Helper()
	norm, err := normalizeObject(obj)
	if err != nil {
		t.Fatalf("normalizeObject: %v", err)
	}
	return norm.Hash
}

// TestManagedFieldsDoNotPerturbHash is the regression guard for the core
// invariant of this task: harvesting actors must not change what gets hashed.
// An object with managedFields (after extraction + stripping) must hash
// identically to the same object that never carried them.
func TestManagedFieldsDoNotPerturbHash(t *testing.T) {
	withManagedFields := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      "example",
			"namespace": "default",
			"managedFields": []any{
				managedField("kubectl-client-side-apply"),
				managedField("kube-controller-manager"),
			},
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}}
	without := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      "example",
			"namespace": "default",
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}}

	// Extraction runs first on the managedFields variant, exactly as the pipeline
	// orders it — proving the extraction step itself introduces no perturbation.
	if actors := ExtractActors(withManagedFields); len(actors) != 2 {
		t.Fatalf("expected 2 actors from fixture, got %v", actors)
	}

	hashWith := normalizedHash(t, withManagedFields)
	hashWithout := normalizedHash(t, without)
	if hashWith != hashWithout {
		t.Errorf("hash differs with vs. without managedFields: extraction perturbed normalization")
	}
}

// TestActorsAnnotationDoesNotPerturbHash is the Task 1.5 regression guard for the
// informer transform's side effect: because the transform stashes actors in an
// annotation on the *cached copy*, an object's hash would otherwise change the
// moment a transform started running — turning a cosmetic pipeline change into a
// cluster-wide flood of spurious Modified rows. The annotation must therefore be
// read into the record and stripped before hashing, and the annotations map
// itself removed when that leaves it empty.
func TestActorsAnnotationDoesNotPerturbHash(t *testing.T) {
	tests := []struct {
		name string
		// annotated is the object as the informer transform leaves it; bare is the
		// same object as it would look with no transform in the picture.
		annotated, bare map[string]any
	}{
		{
			name:      "annotation is the only annotation",
			annotated: map[string]any{ActorsAnnotation: "argocd-controller"},
			bare:      nil,
		},
		{
			name:      "annotation alongside a user annotation",
			annotated: map[string]any{ActorsAnnotation: "argocd-controller", "team": "platform"},
			bare:      map[string]any{"team": "platform"},
		},
		{
			name:      "multiple actors",
			annotated: map[string]any{ActorsAnnotation: "argocd-controller,kube-controller-manager"},
			bare:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotated := newHashFixture(tt.annotated)
			bare := newHashFixture(tt.bare)

			if got, want := normalizedHash(t, annotated), normalizedHash(t, bare); got != want {
				t.Errorf("hash differs with vs. without the actors annotation:\n with: %s\n without: %s", got, want)
			}
		})
	}
}

// TestNormalizeReadsActorsFromAnnotationOrManagedFields asserts both sources of
// the actors column: the transform's annotation is authoritative when present,
// and managedFields is the fallback for an object that reached the pipeline
// untransformed (so the column is never silently empty).
func TestNormalizeReadsActorsFromAnnotationOrManagedFields(t *testing.T) {
	fromAnnotation := newHashFixture(map[string]any{ActorsAnnotation: "argocd-controller,kubectl"})
	norm, err := normalizeObject(fromAnnotation)
	if err != nil {
		t.Fatalf("normalizeObject: %v", err)
	}
	if want := []string{"argocd-controller", "kubectl"}; !reflect.DeepEqual(norm.Actors, want) {
		t.Errorf("actors from annotation = %v, want %v", norm.Actors, want)
	}

	fromManagedFields := newHashFixture(nil)
	meta := fromManagedFields.Object["metadata"].(map[string]any)
	meta["managedFields"] = []any{managedField("kube-controller-manager"), managedField("kubectl")}
	norm, err = normalizeObject(fromManagedFields)
	if err != nil {
		t.Fatalf("normalizeObject: %v", err)
	}
	if want := []string{"kube-controller-manager", "kubectl"}; !reflect.DeepEqual(norm.Actors, want) {
		t.Errorf("actors from managedFields fallback = %v, want %v", norm.Actors, want)
	}

	// The annotation wins when both are present: it is what the transform
	// harvested *before* managedFields was dropped, so it is at least as complete.
	both := newHashFixture(map[string]any{ActorsAnnotation: "argocd-controller"})
	meta = both.Object["metadata"].(map[string]any)
	meta["managedFields"] = []any{managedField("kubectl")}
	norm, err = normalizeObject(both)
	if err != nil {
		t.Fatalf("normalizeObject: %v", err)
	}
	if want := []string{"argocd-controller"}; !reflect.DeepEqual(norm.Actors, want) {
		t.Errorf("actors = %v, want the annotation's value %v", norm.Actors, want)
	}
}

// newHashFixture builds an otherwise-identical Pod carrying the given
// annotations (nil for none), so two fixtures differ in exactly one dimension.
func newHashFixture(annotations map[string]any) *unstructured.Unstructured {
	meta := map[string]any{
		"name":            "example",
		"namespace":       "default",
		"uid":             "uid-1",
		"resourceVersion": "7",
	}
	if annotations != nil {
		meta["annotations"] = annotations
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   meta,
		"spec":       map[string]any{"nodeName": "node-1"},
	}}
}
