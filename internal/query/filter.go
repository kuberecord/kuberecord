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

package query

import (
	"encoding/json"
	"slices"
	"strings"
)

// The predicates of [TimelineQuery], as applied to a change already read.
//
// They live in the contract for the same reason [Replay] does. A backend that
// cannot push a predicate into its storage has to evaluate it here, and a second
// evaluation of the same rule is a second reading of it: the field-path filter
// alone has to unescape RFC 6901 in the mandated order, convert to the dotted
// grammar, prefix-match it, and keep a row that carries no patch at all. Each of
// those steps is somewhere two backends could disagree about which rows a filter
// keeps — and the contract requires them not to, because two engines answering
// one question differently would have an engineer comparing two stores conclude
// that one of them lost rows.
//
// A backend that *can* push a predicate down is expected to, and to keep the two
// forms in agreement; the conformance suite's agreement property is what proves
// it. What these functions guarantee is that the client-side reading has exactly
// one definition to disagree with.

// MatchesActors reports whether a change survives the actor predicates.
//
// The documented order is applied: Actors narrows, and ExcludeActors then wins on
// conflict, so an actor named in both lists excludes the change. That is the
// narrower, safer reading when a caller has contradicted itself.
//
// Note what a non-empty include list necessarily does to a deletion: a deletion
// records no actors, so it can never satisfy one. That is arithmetic rather than
// policy, but it is surprising enough that a caller applying an actor filter
// should say so in its output rather than let the deletion vanish unremarked.
func MatchesActors(c Change, include, exclude []string) bool {
	held := func(names []string) bool {
		return slices.ContainsFunc(c.Actors, func(a string) bool { return slices.Contains(names, a) })
	}
	if len(include) > 0 && !held(include) {
		return false
	}
	return !held(exclude)
}

// patchOp is the part of an RFC 6902 operation a field-path filter reads.
type patchOp struct {
	Path string `json:"path"`
}

// MatchesFieldPaths reports whether a change touches one of the requested paths.
//
// # Why a row with no patch is kept
//
// A first sighting, a snapshot and a deletion carry no diff, so no field-path
// filter can match them. They are kept regardless because they are the boundaries
// of the object's existence: a filtered timeline that dropped them would show a
// history with no beginning and no end, and imply the object had neither.
//
// An undecodable patch is kept for a related reason. Dropping a row because its
// diff would not parse turns a rendering problem into a missing entry in an audit
// timeline — the one outcome worse than showing a row the filter did not ask for.
func MatchesFieldPaths(c Change, paths []string) bool {
	if len(paths) == 0 || c.Diff == "" {
		return true
	}
	var ops []patchOp
	if err := json.Unmarshal([]byte(c.Diff), &ops); err != nil {
		return true
	}
	for _, op := range ops {
		if MatchesFieldPath(op.Path, paths) {
			return true
		}
	}
	return false
}

// MatchesFieldPath reports whether one RFC 6901 pointer lies at or beneath one of
// the requested paths.
//
// It is the single-pointer half of the rule [MatchesFieldPaths] applies to a whole
// change, and it is exported because not every caller is selecting changes. A
// caller attributing *fields* — which recorded change last wrote each path — asks
// this question of one pointer at a time, and it must get the same answer a
// filtered timeline would have given for the change that pointer came from.
// Spelling the rule twice is how two commands come to disagree about what
// `--field spec.template` covers, which reads to a user as one of them having lost
// rows.
//
// An empty list matches everything, because that is what "no field restriction"
// means at every other call site in this package.
func MatchesFieldPath(pointer string, paths []string) bool {
	if len(paths) == 0 {
		return true
	}
	dotted := dottedPath(pointer)
	for _, want := range paths {
		if dotted == want || strings.HasPrefix(dotted, want+".") {
			return true
		}
	}
	return false
}

// dottedPath converts an RFC 6901 JSON Pointer into the dotted grammar
// TimelineQuery.FieldPaths uses, so "/spec/template/spec/containers/0/image"
// becomes "spec.template.spec.containers.0.image" and a filter on
// "spec.template.spec.containers" matches it by prefix.
//
// The two escapes are undone in the order the RFC mandates: ~1 first, then ~0.
// Reversing them would turn the encoded sequence for a literal "~1" into a slash
// — a segment silently split in two, and a path that matches nothing for a reason
// nobody would find by reading the filter.
func dottedPath(pointer string) string {
	segments := make([]string, 0, 8)
	for segment := range strings.SplitSeq(strings.TrimPrefix(pointer, "/"), "/") {
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")
		segments = append(segments, segment)
	}
	return strings.Join(segments, ".")
}
