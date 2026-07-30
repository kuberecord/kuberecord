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

package loadgen

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// revisionAnnotation and payloadAnnotation are where the harness stores the two
// things every churned object needs: a monotonically increasing revision (so
// each mutation genuinely changes the object's content hash and can never be
// deduplicated away) and the size filler that makes the object realistically
// large.
//
// They are plain annotations under a harness-owned prefix, which the pipeline
// treats as ordinary object content — unlike internal.kubestream.io/actors,
// which normalizeObject strips before hashing. That is the point: filler that
// the hash ignored would measure the wrong thing.
const (
	revisionAnnotation = "loadgen.kubestream.io/revision"
	payloadAnnotation  = "loadgen.kubestream.io/payload"
)

// churnKind is one kind the harness can create, mutate and delete.
//
// The three shipped kinds are all built-ins present in a bare envtest apiserver
// (no controller-manager, so nothing reconciles them behind the harness's back)
// and between them span the two axes that matter to the pipeline: two API groups
// (core and apps, so identity keys and scopes are genuinely mixed rather than
// three flavours of the same group) and two object shapes (a flat one and a
// deeply nested one, which is what makes the normalize/diff cost realistic).
//
// Two built-ins are deliberately absent. v1/Secret is hard-denied as a watchable
// kind (D8) and must never appear in a benchmark that claims to describe the real
// data plane. v1/Service allocates a cluster IP per object, and envtest's default
// service CIDR runs out after a couple of hundred — a 20,000-object profile would
// fail on address exhaustion rather than on anything this harness measures.
type churnKind struct {
	// GVK is what the informer watches and what the work key's Group/Kind come
	// from.
	GVK schema.GroupVersionKind

	// build returns a fresh object of this kind at the given revision, padded
	// with roughly payloadBytes of filler.
	build func(namespace, name string, revision, payloadBytes int) *unstructured.Unstructured
}

// churnKinds is the kind table, keyed by Kind name — which is exactly how a
// profile names them and how pipeline.Key identifies them.
var churnKinds = map[string]churnKind{
	// ConfigMap: the flat case. Its arbitrary string data is where the filler
	// goes, which keeps this kind's payload dial identical to the one Task 0.8's
	// baseline was measured with.
	"ConfigMap": {
		GVK: schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		build: func(namespace, name string, revision, payloadBytes int) *unstructured.Unstructured {
			obj := newObject("v1", "ConfigMap", namespace, name, revision)
			obj.Object["data"] = map[string]any{
				"payload":  filler(payloadBytes),
				"revision": strconv.Itoa(revision),
			}
			return obj
		},
	},

	// Deployment: the nested case, and the second API group. Replicas are 0 and
	// no controller-manager runs, so the object is inert — the harness churns its
	// annotations, and the pipeline still pays the full normalize + diff cost of
	// a pod template.
	"Deployment": {
		GVK: schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		build: func(namespace, name string, revision, payloadBytes int) *unstructured.Unstructured {
			obj := newObject("apps/v1", "Deployment", namespace, name, revision)
			padAnnotations(obj, payloadBytes)
			obj.Object["spec"] = map[string]any{
				"replicas": int64(0),
				"selector": map[string]any{
					"matchLabels": map[string]any{"app": name},
				},
				"template": map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{"app": name},
					},
					"spec": map[string]any{
						"containers": []any{
							map[string]any{
								"name":  "pause",
								"image": "registry.k8s.io/pause:3.10",
							},
						},
					},
				},
			}
			return obj
		},
	},

	// ServiceAccount: the metadata-only case. It contributes a third scope with
	// almost no body, which is what makes per-object overhead (identity keys,
	// cache entries, work items) visible in the massive profile rather than
	// hidden behind payload bytes.
	"ServiceAccount": {
		GVK: schema.GroupVersionKind{Version: "v1", Kind: "ServiceAccount"},
		build: func(namespace, name string, revision, payloadBytes int) *unstructured.Unstructured {
			obj := newObject("v1", "ServiceAccount", namespace, name, revision)
			padAnnotations(obj, payloadBytes)
			return obj
		},
	},
}

// churnableKinds returns the kind names this harness supports, sorted, for error
// messages that tell a profile author what they could have written.
func churnableKinds() []string {
	return slices.Sorted(maps.Keys(churnKinds))
}

// resolveKinds turns a profile's kind names into their table entries, preserving
// the profile's order so object naming and worker partitioning are reproducible.
func resolveKinds(names []string) ([]churnKind, error) {
	kinds := make([]churnKind, 0, len(names))
	for _, name := range names {
		kind, ok := churnKinds[name]
		if !ok {
			return nil, fmt.Errorf("kind %q is not churnable by this harness", name)
		}
		kinds = append(kinds, kind)
	}
	return kinds, nil
}

// churnTarget is one object in the churn pool: which kind it is and what it is
// called.
type churnTarget struct {
	kind churnKind
	name string
}

// planObjects spreads the profile's object count evenly across its kinds, in a
// deterministic order, so two runs of a profile churn the same object set.
func planObjects(kinds []churnKind, total int) []churnTarget {
	objects := make([]churnTarget, 0, total)
	for i := range total {
		kind := kinds[i%len(kinds)]
		objects = append(objects, churnTarget{
			kind: kind,
			// The kind is part of the name so no two kinds' objects ever collide
			// on one name, which would make a delete-and-recreate of one look
			// like a reincarnation of another.
			name: fmt.Sprintf("loadgen-%s-%05d", strings.ToLower(kind.GVK.Kind), i),
		})
	}
	return objects
}

// newObject builds the metadata every churned object shares.
func newObject(apiVersion, kind, namespace, name string, revision int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"namespace": namespace,
			"name":      name,
			"annotations": map[string]any{
				revisionAnnotation: strconv.Itoa(revision),
			},
			// A label every churned object carries, so a rule with a selector
			// could be pointed at exactly this harness's objects.
			"labels": map[string]any{"app.kubernetes.io/managed-by": "kubestream-loadgen"},
		},
	}}
}

// padAnnotations adds the size filler for kinds that have no free-form field of
// their own to put it in.
func padAnnotations(obj *unstructured.Unstructured, payloadBytes int) {
	if payloadBytes <= 0 {
		return
	}
	annotations := obj.Object["metadata"].(map[string]any)["annotations"].(map[string]any)
	annotations[payloadAnnotation] = filler(payloadBytes)
}

// setRevision stamps a new revision onto an existing object, in both places a
// kind might carry one, so one mutation function serves every kind.
//
// It writes the revision annotation for every kind and additionally refreshes
// ConfigMap data's copy, because the annotation alone would already change the
// hash — the data copy exists so a ConfigMap's *payload-bearing* field moves too,
// which is what a real workload's updates look like.
func setRevision(obj *unstructured.Unstructured, revision int) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string, 1)
	}
	annotations[revisionAnnotation] = strconv.Itoa(revision)
	obj.SetAnnotations(annotations)

	// NestedFieldNoCopy, so the data map is mutated in place: this is the
	// harness's own freshly-read object, and copying the whole map back would
	// make the generator allocate more per mutation than the pipeline does.
	// A kind with no data field, or one whose data is not a map, simply keeps the
	// annotation-only revision above — which has already changed the hash, so
	// the mutation is real either way and there is nothing to report.
	data, found, err := unstructured.NestedFieldNoCopy(obj.Object, "data")
	if err != nil || !found {
		return
	}
	if entries, ok := data.(map[string]any); ok {
		entries["revision"] = strconv.Itoa(revision)
	}
}

// filler returns n bytes of deterministic, non-compressible-by-accident text.
// Deterministic so two runs of a profile churn identical bytes; a repeating
// alphabet rather than random data so the zstd baseline compression the pipeline
// applies behaves like it does on real Kubernetes JSON instead of on noise.
func filler(n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	return string(buf)
}
