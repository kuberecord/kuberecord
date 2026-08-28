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

package query_test

import (
	"testing"

	"github.com/kuberecord/kuberecord/internal/query"
)

// TestMatchesActors covers the predicate and the two readings of it that are easy to
// get backwards.
//
// ExcludeActors is applied after Actors and wins on conflict, which is the narrower and
// safer reading when a caller has contradicted itself. And a change made by two actors
// is dropped when *either* is excluded, because the question an exclusion asks is "did
// this actor touch it", not "was this actor alone".
func TestMatchesActors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		actors  []string
		include []string
		exclude []string
		want    bool
		why     string
	}{
		{
			name: "no predicate keeps everything", actors: []string{"kubectl"}, want: true,
			why: "an empty filter is not a filter",
		},
		{
			name: "an included actor is kept", actors: []string{"kubectl", "helm"},
			include: []string{"helm"}, want: true,
			why: "one actor in the list is enough",
		},
		{
			name: "an actor nobody asked for is dropped", actors: []string{"kubectl"},
			include: []string{"helm"}, want: false,
		},
		{
			name: "a deletion cannot satisfy an include list", actors: nil,
			include: []string{"kubectl"}, want: false,
			why: "a deletion records no actors, so any non-empty include list excludes every " +
				"deletion — arithmetic rather than policy, and surprising enough that a caller " +
				"applying one should say so in its output",
		},
		{
			name: "a deletion survives an exclude list", actors: nil,
			exclude: []string{"kubectl"}, want: true,
		},
		{
			name: "an excluded actor is dropped", actors: []string{"kube-controller-manager"},
			exclude: []string{"kube-controller-manager"}, want: false,
		},
		{
			name:    "one excluded actor out of two is enough to drop the change",
			actors:  []string{"kubectl", "kube-controller-manager"},
			exclude: []string{"kube-controller-manager"}, want: false,
			why: "an exclusion asks whether the actor touched the object, not whether it acted alone",
		},
		{
			name: "exclusion wins over inclusion", actors: []string{"kubectl"},
			include: []string{"kubectl"}, exclude: []string{"kubectl"}, want: false,
			why: "the narrower reading when a caller has named the same actor twice",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := query.MatchesActors(query.Change{Actors: tc.actors}, tc.include, tc.exclude)
			if got != tc.want {
				t.Errorf("MatchesActors(actors=%v, include=%v, exclude=%v) = %t, want %t. %s",
					tc.actors, tc.include, tc.exclude, got, tc.want, tc.why)
			}
		})
	}
}

// TestMatchesFieldPaths covers the dotted grammar, the pointer unescaping and the two
// rows a field-path filter must keep even though it cannot match them.
//
// The escape order is the case worth having a test for. RFC 6901 mandates ~1 before ~0,
// and reversing them turns the encoded sequence for a literal "~1" into a slash — a
// segment silently split in two, and a path that matches nothing for a reason nobody
// would find by reading the filter.
func TestMatchesFieldPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		diff  string
		paths []string
		want  bool
		why   string
	}{
		{
			name: "no predicate keeps everything",
			diff: `[{"op":"replace","path":"/spec/replicas","value":2}]`, want: true,
		},
		{
			name: "a row carrying no patch is kept", diff: "", paths: []string{"spec.replicas"},
			want: true,
			why: "a first sighting, a snapshot and a deletion are the boundaries of the object's " +
				"existence, and a filtered timeline without them implies it had neither",
		},
		{
			name:  "an exact path matches",
			diff:  `[{"op":"replace","path":"/spec/replicas","value":2}]`,
			paths: []string{"spec.replicas"}, want: true,
		},
		{
			name:  "a prefix matches everything beneath it",
			diff:  `[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"v2"}]`,
			paths: []string{"spec.template.spec.containers"}, want: true,
		},
		{
			name:  "a sibling path does not match",
			diff:  `[{"op":"replace","path":"/status/readyReplicas","value":2}]`,
			paths: []string{"spec.replicas"}, want: false,
		},
		{
			name:  "a prefix must end on a segment boundary",
			diff:  `[{"op":"replace","path":"/spec/replicasHistory","value":2}]`,
			paths: []string{"spec.replicas"}, want: false,
			why: "otherwise a filter on spec.replicas would quietly match spec.replicasHistory",
		},
		{
			name: "any operation in the patch is enough",
			diff: `[{"op":"replace","path":"/status/readyReplicas","value":2},` +
				`{"op":"replace","path":"/spec/replicas","value":3}]`,
			paths: []string{"spec.replicas"}, want: true,
		},
		{
			name: "an escaped slash stays one segment",
			diff: `[{"op":"add","path":"/metadata/annotations/kubectl.kubernetes.io~1last-applied",` +
				`"value":"{}"}]`,
			paths: []string{"metadata.annotations.kubectl.kubernetes.io/last-applied"}, want: true,
			why: "~1 is a slash inside a segment, not a separator between two",
		},
		{
			name:  "the escapes are undone in the order the RFC mandates",
			diff:  `[{"op":"add","path":"/metadata/labels/a~01","value":"x"}]`,
			paths: []string{"metadata.labels.a~1"}, want: true,
			why: "~0 then the literal 1 is a tilde followed by a one; undoing ~0 first would turn " +
				"it into ~1 and then into a slash, splitting the segment",
		},
		{
			name: "an undecodable patch is kept", diff: `not json`, paths: []string{"spec.replicas"},
			want: true,
			why: "dropping a row because its diff would not parse turns a rendering problem into a " +
				"missing entry in an audit timeline",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := query.MatchesFieldPaths(query.Change{Diff: tc.diff}, tc.paths)
			if got != tc.want {
				t.Errorf("MatchesFieldPaths(diff=%s, paths=%v) = %t, want %t. %s",
					tc.diff, tc.paths, got, tc.want, tc.why)
			}
		})
	}
}
