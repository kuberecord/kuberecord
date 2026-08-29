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

package render

import (
	"strings"
)

// Field paths appear in three grammars in this project, and this file is where
// they are reconciled.
//
//   - RFC 6901 JSON Pointer is what a recorded patch stores:
//     "/spec/template/spec/containers/0/image".
//   - The *display* grammar is the one an engineer reads and the one the
//     acceptance criteria fix: "spec.template.spec.containers[0].image".
//   - The *filter* grammar is what query.TimelineQuery.FieldPaths matches
//     against, and it is dotted throughout, indices included:
//     "spec.template.spec.containers.0.image". It is dotted because it is the
//     grammar redaction policies already use, and one path language per project
//     is worth more than either spelling.
//
// The gap between the second and third is a trap with a user's name on it: they
// read a path out of the CHANGE column, type it into --field, and match nothing.
// NormalizeFieldPath closes it by accepting either spelling.

// DisplayPath converts an RFC 6901 JSON Pointer into the dotted grammar with
// bracketed array indices.
//
// The two escapes are undone in the order the RFC mandates — ~1 first, then ~0.
// Reversing them turns the encoded sequence for a literal "~1" into a slash, so
// a segment silently splits in two and the path names a field that does not
// exist. query.MatchesFieldPaths does the same thing for the same reason; the
// duplication is two readings of one RFC in two packages, which is a smaller
// cost than the read plane's contract importing a rendering package.
//
// # The numeric-segment ambiguity, and why it is accepted
//
// A pointer segment that is entirely digits is rendered as an array index,
// because in a Kubernetes object it almost always is one. It is not always: a
// ConfigMap may hold a data key spelled "1", and this renders "data[1]" for it.
// Telling the two apart needs the document the pointer addresses, which is not
// available on every row — a patch survives its object's retention window — so
// the choice is between being right about container indices on every row and
// being right about numeric map keys on some. The first is chosen, and the cost
// is recorded here rather than discovered.
func DisplayPath(pointer string) string {
	segments := pointerSegments(pointer)
	if len(segments) == 0 {
		// The whole-document pointer. Rendering it as an empty cell would make a
		// root-level operation look like a row with no path at all.
		return "/"
	}

	var built strings.Builder
	for i, segment := range segments {
		switch {
		case i > 0 && isIndex(segment):
			built.WriteString("[" + segment + "]")
		case i > 0:
			built.WriteString("." + segment)
		default:
			// A leading index cannot happen for a recorded object — the recorded
			// state is always a JSON object — so the first segment is always a
			// member name, and writing it bare is what keeps "spec" from becoming
			// ".spec".
			built.WriteString(segment)
		}
	}
	return built.String()
}

// NormalizeFieldPath converts a path in either display or filter grammar into
// the filter grammar query.TimelineQuery.FieldPaths expects.
//
// It exists so that copying a path out of the output and pasting it into --field
// works. Without it the tool would show "containers[0].image" and then answer
// "no changes" to a filter spelled exactly the way it had just printed it — a
// silence produced by the tool's own two grammars, which is precisely the class
// of empty answer Invariant 9 exists to forbid.
//
// Anything already dotted passes through unchanged, so a user who learned the
// filter grammar from docs/SCHEMA.md is not asked to unlearn it.
func NormalizeFieldPath(path string) string {
	if !strings.ContainsAny(path, "[]") {
		return path
	}
	var built strings.Builder
	for _, r := range path {
		switch r {
		case '[':
			built.WriteByte('.')
		case ']':
			// The closing bracket is dropped rather than replaced: "a[0]b" is not
			// a path anybody writes, and emitting a trailing dot for it would turn
			// a malformed input into a path with an empty segment, which matches
			// nothing and says nothing about why.
		default:
			built.WriteRune(r)
		}
	}
	return built.String()
}

// Elide shortens a display path to width by removing whole segments from its
// middle, keeping the first segment and as much of the tail as fits.
//
// The tail is what is kept because it is what identifies the field: the leaf and
// the container it sits in are the answer to "what changed", while the interior
// of "spec.template.spec.template…" is scaffolding that every path of that shape
// shares. The head is kept too, and only one segment of it, because it says
// which half of the object was touched — a change under `status` and a change
// under `spec` are different news, and a path elided from the left would read
// identically for both.
//
// The marker is glued to the leading segment's dot, so "spec" plus the tail
// "containers[0].image" renders "spec.…containers[0].image". A width too small
// for even the leaf is answered by truncating rather than by returning a path
// with nothing in it.
func Elide(path string, width int) string {
	if width <= 0 || displayWidth(path) <= width {
		return path
	}

	segments := splitDisplayPath(path)
	if len(segments) <= 1 {
		return truncate(path, width)
	}

	head := segments[0]
	// One segment of head, one dot, one marker. Anything less and there is no
	// room to say a middle was removed.
	fixed := displayWidth(head) + 1 + displayWidth(Ellipsis)

	// Take trailing segments while they fit, longest tail first. The loop walks
	// backwards so that the leaf is the last thing given up.
	tail := ""
	for i := len(segments) - 1; i >= 1; i-- {
		candidate := joinDisplaySegments(segments[i:])
		if fixed+displayWidth(candidate) > width {
			break
		}
		tail = candidate
	}
	if tail == "" {
		// Not even the leaf fits beside the head. The leaf is the informative
		// half, so the head is given up rather than the leaf.
		return Ellipsis + truncate(joinDisplaySegments(segments[len(segments)-1:]), width-displayWidth(Ellipsis))
	}
	return head + "." + Ellipsis + tail
}

// pointerSegments splits an RFC 6901 pointer and unescapes each token.
func pointerSegments(pointer string) []string {
	trimmed := strings.TrimPrefix(pointer, "/")
	if pointer == "" || trimmed == "" && pointer == "/" {
		// "" addresses the whole document; "/" addresses the member named by the
		// empty string, which internal/pipeline refuses to record a patch for at
		// all (see errEmptyInteriorToken). Both are returned as no segments and
		// rendered as "/" by the caller.
		return nil
	}
	segments := make([]string, 0, 8)
	for segment := range strings.SplitSeq(trimmed, "/") {
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")
		segments = append(segments, segment)
	}
	return segments
}

// splitDisplayPath breaks a rendered path back into the segments Elide moves,
// keeping a bracketed index attached to the member it indexes.
//
// Keeping "containers[0]" together is what stops an elision from producing
// "spec.…[0].image", which names an index into nothing.
func splitDisplayPath(path string) []string {
	var (
		segments []string
		current  strings.Builder
	)
	for _, r := range path {
		if r == '.' {
			segments = append(segments, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	segments = append(segments, current.String())
	return segments
}

// joinDisplaySegments reassembles what splitDisplayPath took apart.
func joinDisplaySegments(segments []string) string { return strings.Join(segments, ".") }

// isIndex reports whether a pointer segment is an array subscript.
//
// A leading zero is rejected because RFC 6901 spells array indices without one,
// so "01" is a member name that happens to look numeric — the one case where the
// ambiguity DisplayPath documents can be resolved from the token alone.
func isIndex(segment string) bool {
	if segment == "" || (len(segment) > 1 && segment[0] == '0') {
		return false
	}
	for i := range len(segment) {
		if segment[i] < '0' || segment[i] > '9' {
			return false
		}
	}
	return true
}
