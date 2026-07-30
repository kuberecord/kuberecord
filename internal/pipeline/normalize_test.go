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
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The specs in this file pin normalizeObject's two load-bearing properties, both
// of which became easier to break in Task 2.3 when the function stopped taking a
// full deep copy of every object it normalizes:
//
//  1. The bytes it produces are *exactly* what the deep-copy-and-remove version
//     produced. Those bytes are hashed into the sha256 column and stored as the
//     diff baseline, so a single byte of drift would make every object in every
//     cluster look changed at once — a cluster-wide re-write storm and a break in
//     every stored history's diff chain.
//  2. It does not mutate the object it is given. That object is the informer's own
//     cached instance, shared with every other reader in the process.

// referenceNormalizedJSON is the pre-Task-2.3 normalization, verbatim: deep-copy
// the object, remove the volatile fields from the copy, marshal it. It exists as
// the oracle for the equivalence test — the point is to compare against the
// algorithm that produced every hash already stored in a real deployment, not
// against a restatement of the current one.
func referenceNormalizedJSON(t *testing.T, obj *unstructured.Unstructured) []byte {
	t.Helper()
	copied := obj.DeepCopy()
	annotations := copied.GetAnnotations()
	unstructured.RemoveNestedField(copied.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(copied.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(copied.Object, "metadata", "generation")
	if _, ok := annotations[ActorsAnnotation]; ok {
		if len(annotations) == 1 {
			unstructured.RemoveNestedField(copied.Object, "metadata", "annotations")
		} else {
			unstructured.RemoveNestedField(copied.Object, "metadata", "annotations", ActorsAnnotation)
		}
	}
	out, err := json.Marshal(copied.Object)
	if err != nil {
		t.Fatalf("reference marshal: %v", err)
	}
	return out
}

// normalizeShapes are the object shapes normalization has to handle, including
// the malformed ones that must degrade rather than fail.
func normalizeShapes() []struct {
	name string
	obj  *unstructured.Unstructured
} {
	return []struct {
		name string
		obj  *unstructured.Unstructured
	}{
		{
			name: "transformed: actors annotation only",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"name":            "only-ours",
					"namespace":       "default",
					"resourceVersion": "12",
					"generation":      int64(4),
					"annotations":     map[string]any{ActorsAnnotation: "argocd-controller"},
				},
				"spec": map[string]any{"containers": []any{map[string]any{"name": "app"}}},
			}},
		},
		{
			name: "transformed: actors annotation alongside real ones",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"name":      "ours-and-theirs",
					"namespace": "default",
					"annotations": map[string]any{
						ActorsAnnotation:               "argocd-controller,kubectl",
						"kubectl.kubernetes.io/x":      "y",
						"deployment.kubernetes.io/rev": "3",
					},
				},
				"status": map[string]any{"phase": "Running"},
			}},
		},
		{
			name: "untransformed: managedFields present, no actors annotation",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"name":            "raw",
					"namespace":       "prod",
					"resourceVersion": "99",
					"generation":      int64(7),
					"annotations":     map[string]any{"note": "keep me"},
					"managedFields": []any{
						map[string]any{"manager": "kube-controller-manager", "operation": "Update"},
					},
				},
				"spec": map[string]any{"replicas": int64(3)},
			}},
		},
		{
			name: "no annotations at all",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "bare", "namespace": "default"},
				"data":       map[string]any{"k": "v"},
			}},
		},
		{
			name: "empty annotations map",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "empty-annotations", "namespace": "default",
					"annotations": map[string]any{},
				},
			}},
		},
		{
			name: "annotations hold a non-string value",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "weird-annotations", "namespace": "default",
					// The typed accessor reports no annotations for this shape, so
					// nothing may be stripped from it — not even a key that looks
					// like the operator's own.
					"annotations": map[string]any{ActorsAnnotation: int64(1)},
				},
			}},
		},
		{
			name: "metadata is not a map",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   "not-a-map",
			}},
		},
		{
			name: "no metadata at all",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"data":       map[string]any{"k": "v"},
			}},
		},
	}
}

// TestNormalizeObjectMatchesTheDeepCopyReference is the hash-stability guarantee:
// for every shape, and for every realistic object in testdata/, the normalized
// bytes must be byte-identical to what deep-copy-and-remove produced.
func TestNormalizeObjectMatchesTheDeepCopyReference(t *testing.T) {
	for _, shape := range normalizeShapes() {
		t.Run(shape.name, func(t *testing.T) {
			norm, err := normalizeObject(shape.obj)
			if err != nil {
				t.Fatalf("normalizeObject: %v", err)
			}
			if want := referenceNormalizedJSON(t, shape.obj); string(norm.JSON) != string(want) {
				t.Errorf("normalized JSON drifted from the deep-copy reference\n got: %s\nwant: %s",
					norm.JSON, want)
			}
		})
	}

	// The realistic corpus, in both the transformed and untransformed shapes, so
	// the equivalence covers real object depth and not just hand-built fixtures.
	for _, transformed := range []bool{true, false} {
		for _, object := range loadBenchCorpus(t, transformed) {
			name := object.name
			if transformed {
				name += "/transformed"
			} else {
				name += "/raw-managedfields"
			}
			t.Run(name, func(t *testing.T) {
				norm, err := normalizeObject(object.obj)
				if err != nil {
					t.Fatalf("normalizeObject: %v", err)
				}
				if want := referenceNormalizedJSON(t, object.obj); string(norm.JSON) != string(want) {
					t.Errorf("normalized JSON drifted from the deep-copy reference for %s", name)
				}
			})
		}
	}
}

// TestNormalizeObjectLeavesTheInputUntouched is the watch-cache-integrity
// guarantee, asserted at the level the sharing actually happens: the whole object
// must serialize identically before and after, for every shape.
func TestNormalizeObjectLeavesTheInputUntouched(t *testing.T) {
	for _, shape := range normalizeShapes() {
		t.Run(shape.name, func(t *testing.T) {
			before, err := json.Marshal(shape.obj.Object)
			if err != nil {
				t.Fatalf("marshal before: %v", err)
			}
			if _, err := normalizeObject(shape.obj); err != nil {
				t.Fatalf("normalizeObject: %v", err)
			}
			after, err := json.Marshal(shape.obj.Object)
			if err != nil {
				t.Fatalf("marshal after: %v", err)
			}
			if string(before) != string(after) {
				t.Errorf("normalizeObject mutated the object it was given\nbefore: %s\nafter:  %s", before, after)
			}
		})
	}
}

// TestNormalizeObjectIsRepeatable is the practical consequence of the two specs
// above: normalizing the same object twice must produce the same hash. A function
// that mutated its input would pass the first call and quietly change the answer
// on the second — which, in production, is a hash that differs between the write
// path and a re-read for no reason at all.
func TestNormalizeObjectIsRepeatable(t *testing.T) {
	for _, shape := range normalizeShapes() {
		t.Run(shape.name, func(t *testing.T) {
			first, err := normalizeObject(shape.obj)
			if err != nil {
				t.Fatalf("normalizeObject (first): %v", err)
			}
			second, err := normalizeObject(shape.obj)
			if err != nil {
				t.Fatalf("normalizeObject (second): %v", err)
			}
			if first.Hash != second.Hash {
				t.Errorf("hash changed between two normalizations of one object: %s then %s",
					first.Hash, second.Hash)
			}
		})
	}
}

// TestStripVolatileFieldsSharesWhatItDoesNotEdit pins the optimization itself, not
// just its output. Without this, a future "let's be safe" deep copy could be
// reintroduced with every test still green and half the data plane's allocation
// budget silently back — the regression Task 2.3 exists to remove.
func TestStripVolatileFieldsSharesWhatItDoesNotEdit(t *testing.T) {
	spec := map[string]any{"replicas": int64(3), "template": map[string]any{"x": "y"}}
	object := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":            "shared",
			"resourceVersion": "5",
			"managedFields":   []any{map[string]any{"manager": "kubectl"}},
			"annotations":     map[string]any{ActorsAnnotation: "kubectl", "keep": "me"},
		},
		"spec": spec,
	}

	stripped := stripVolatileFields(object, true)

	// Untouched subtrees are the *same* maps, not copies.
	if !sameMap(stripped["spec"], spec) {
		t.Error("spec was copied; only the maps normalization edits may be copied")
	}
	// Edited levels are fresh, so the caller's own object is unaffected.
	if sameMap(stripped["metadata"], object["metadata"]) {
		t.Error("metadata was shared; stripping it would then mutate the caller's object")
	}

	strippedMeta, ok := stripped["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("stripped metadata is a %T, want map[string]any", stripped["metadata"])
	}
	for _, gone := range []string{"managedFields", "resourceVersion"} {
		if _, present := strippedMeta[gone]; present {
			t.Errorf("metadata.%s survived stripping", gone)
		}
	}
	annotations, ok := strippedMeta["annotations"].(map[string]any)
	if !ok {
		t.Fatalf("stripped annotations is a %T, want map[string]any", strippedMeta["annotations"])
	}
	if _, present := annotations[ActorsAnnotation]; present {
		t.Error("the operator's own annotation survived stripping")
	}
	if annotations["keep"] != "me" {
		t.Errorf("annotations = %v, want the object's own annotations kept", annotations)
	}

	// The caller's object still has everything.
	originalMeta := object["metadata"].(map[string]any)
	for _, field := range []string{"managedFields", "resourceVersion"} {
		if _, present := originalMeta[field]; !present {
			t.Errorf("stripping removed metadata.%s from the caller's own object", field)
		}
	}
	originalAnnotations := originalMeta["annotations"].(map[string]any)
	if _, present := originalAnnotations[ActorsAnnotation]; !present {
		t.Error("stripping removed the actors annotation from the caller's own object")
	}
}

// sameMap reports whether two values are the same map instance.
func sameMap(a, b any) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.Kind() != reflect.Map || bv.Kind() != reflect.Map {
		return false
	}
	return av.UnsafePointer() == bv.UnsafePointer()
}
