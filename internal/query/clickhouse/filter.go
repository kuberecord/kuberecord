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

package clickhouse

import (
	"encoding/json"
	"strings"

	"github.com/kuberecord/kuberecord/internal/query"
)

// patchOp is the part of an RFC 6902 operation a field-path filter reads.
type patchOp struct {
	Path string `json:"path"`
}

// matchesFieldPaths reports whether a change touches one of the requested paths.
//
// # Why this is not SQL
//
// Every other predicate this backend applies is pushed into WHERE. This one is
// applied to rows already read, and the reason is that pushing it down would buy
// nothing and cost correctness.
//
// It would buy nothing because the diff column is returned on every row of a
// timeline regardless: the caller renders it. So the same bytes are read off disk
// either way, and a server-side form would only transfer fewer rows over the
// wire — a saving on the smaller of the two costs, in the one query shape whose
// result set a limit is already there to bound.
//
// It would cost correctness because the SQL form is a reimplementation of RFC
// 6901 in a query language: each operation's path has to be unescaped (~1 before
// ~0, in that order, or a path containing a literal tilde is silently mangled),
// converted to the dotted grammar, and prefix-matched — and a row carrying no
// patch at all has to survive the filter anyway. Every one of those steps is a
// place for a subtle disagreement with the client-side reading, and a
// disagreement between two backends about which rows a filter keeps is exactly
// the failure the conformance suite's agreement property exists to catch. Having
// one implementation is how it cannot happen.
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
func matchesFieldPaths(change query.Change, paths []string) bool {
	if len(paths) == 0 || change.Diff == "" {
		return true
	}
	var ops []patchOp
	if err := json.Unmarshal([]byte(change.Diff), &ops); err != nil {
		return true
	}
	for _, op := range ops {
		dotted := dottedPath(op.Path)
		for _, want := range paths {
			if dotted == want || strings.HasPrefix(dotted, want+".") {
				return true
			}
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
