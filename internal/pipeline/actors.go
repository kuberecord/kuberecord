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
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ActorsAnnotation is where the informer transform (Task 1.4) stashes the actor
// names it harvested from metadata.managedFields before deleting that field
// from the cached copy. Dropping managedFields in the transform is the
// informer-memory half of D2 — it is by far the largest chunk of a typical
// object — but the actors signal has to survive it, and an annotation is the
// only place on the object itself that both sides can agree on.
//
// The pipeline therefore treats this annotation as internal transport, not as
// object content: Process reads it into the Record and then strips it *before*
// hashing, so the annotation can never perturb an object's hash or show up in a
// stored diff (see normalize). The "internal." prefix marks it as
// operator-owned; it is never written back to the apiserver.
const ActorsAnnotation = "internal.kuberecord.io/actors"

// actorsSeparator joins actor names inside ActorsAnnotation. A comma is safe
// because field-manager names are apiserver-validated and cannot contain one.
const actorsSeparator = ","

// EncodeActors renders actors as an ActorsAnnotation value. It is exported for
// the informer transform (Task 1.4), which is the only writer of that
// annotation; keeping the encoding and decoding in one file is what guarantees
// the two halves cannot drift apart. Input order is preserved — ExtractActors
// already returns a sorted, de-duplicated slice, and determinism matters here:
// a re-ordered annotation on an otherwise-unchanged object would look like a
// change to anything hashing it.
func EncodeActors(actors []string) string {
	return strings.Join(actors, actorsSeparator)
}

// decodeActors parses an ActorsAnnotation value back into actor names, skipping
// empty segments so a stray or trailing separator degrades to a shorter list
// rather than an empty-string "actor". The result is always non-nil, matching
// ExtractActors, because the sink's actors column is a non-nullable array.
func decodeActors(value string) []string {
	actors := []string{}
	for part := range strings.SplitSeq(value, actorsSeparator) {
		if part != "" {
			actors = append(actors, part)
		}
	}
	return actors
}

// ExtractActors harvests the distinct field-manager names from an object's
// metadata.managedFields — the cheapest available "who probably changed this"
// signal and the backbone of the GitOps-drift story (kubectl-client-side-apply,
// argocd-controller, kube-controller-manager, …). It must be called before
// managedFields is stripped — by the informer transform (Task 1.4) in
// production, or by normalize for any object that reaches the pipeline
// untransformed — and it only reads the object: it never mutates obj.Object, so
// the subsequent normalization + hashing is unaffected.
//
// The returned slice is de-duplicated and sorted for determinism (so an
// unchanged actor set never produces a spurious diff downstream), with empty
// manager names mapped to "unknown". A non-map entry in managedFields is
// skipped rather than failing the whole extraction; if any are seen they are
// logged once per call (never once per entry) so a malformed object degrades to
// a partial actor set instead of a silent error or a log storm. The result is
// always non-nil (empty slice when there is nothing to harvest).
//
//nolint:logcheck
func ExtractActors(obj *unstructured.Unstructured) []string {
	// NestedFieldNoCopy, not NestedSlice: the latter deep-copies the whole
	// slice (needless work on the hot path) and panics on any non-JSON value
	// it encounters, which would turn a single malformed managedFields entry
	// into a crash rather than the graceful skip this function guarantees. We
	// only read, so a no-copy view is both cheaper and safer.
	raw, found, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "managedFields")
	if err != nil || !found {
		return []string{}
	}
	managedFields, ok := raw.([]any)
	if !ok || len(managedFields) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(managedFields))
	malformed := 0
	for _, entry := range managedFields {
		m, ok := entry.(map[string]any)
		if !ok {
			malformed++
			continue
		}
		// A missing, non-string, or empty manager all collapse to "unknown":
		// the row still records that *something* touched the object even when
		// the field manager can't be named.
		manager, _ := m["manager"].(string)
		if manager == "" {
			manager = "unknown"
		}
		seen[manager] = struct{}{}
	}

	if malformed > 0 {
		logf.Log.WithName("actors").Error(nil, "skipped malformed managedFields entries while extracting actors",
			"kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName(), "skipped", malformed)
	}

	if len(seen) == 0 {
		return []string{}
	}
	return slices.Sorted(maps.Keys(seen))
}
