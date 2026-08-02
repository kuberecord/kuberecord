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
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// RedactionSentinel replaces every redacted value. Structure is preserved and
// only the leaf is rewritten, so a redacted object is still a valid object with
// the same shape — a reader sees that a field existed and was scrubbed, rather
// than being unable to tell an absent field from a hidden one.
//
// It is deliberately a fixed literal rather than a per-value digest: a digest
// would be a stable oracle an attacker could grind against a guessed value,
// which is exactly the property redaction exists to remove.
const RedactionSentinel = "[REDACTED]"

// LastAppliedConfigAnnotation is scrubbed on every object, under every policy,
// including an entirely empty one.
//
// kubectl writes the complete submitted object into it on every `kubectl apply`,
// so it embeds a full prior copy of the very fields a policy redacts. Leaving it
// alone would make every other rule in this file cosmetic: an operator could
// redact `data.password` and still find the password in the annotation blob of
// the same row. It is not configurable for the same reason — a policy that
// re-enabled it would be a policy that silently defeats itself.
const LastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// redactionSegmentPattern is what one dot-separated path segment may look like:
// a leading letter or underscore, then letters, digits, underscores and hyphens.
// It is the parser's half of the CRD's `fieldPath` pattern (see
// v1alpha1.RedactionFieldPathPattern), and the controller test cross-checks that
// the two agree so a path the API server admits can never fail to compile here.
var redactionSegmentPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)

// errEmptyRedactionPath is returned for an empty path string. It is a sentinel
// so a caller collecting compile failures can classify the "someone passed an
// empty string" case apart from a genuine syntax error.
var errEmptyRedactionPath = errors.New("redaction path is empty")

// AnnotationRedactionPath renders the canonical path of one annotation key.
//
// It exists because annotation keys routinely contain dots and slashes
// (`kubectl.kubernetes.io/last-applied-configuration`), which the dot-segment
// grammar cannot express — `metadata.annotations.kubectl.kubernetes.io/...`
// would parse as six nested maps that do not exist. The CRD's `annotation:`
// shorthand is therefore not sugar; it is the only way to name such a key, and
// this function is the single place its expansion is defined so the control
// plane's rendering and the data plane's parsing cannot drift.
func AnnotationRedactionPath(key string) string {
	return "metadata.annotations[" + strconv.Quote(key) + "]"
}

// redactionSegment is one step of a compiled path: descend into this map key,
// and — if wildcard — treat what is found as an array and apply the rest of the
// path to every element.
type redactionSegment struct {
	name     string
	wildcard bool
}

// redactionPath is one compiled path plus the text it was compiled from, kept so
// policies can be merged and deduplicated by their canonical spelling without
// re-deriving it from the segments.
type redactionPath struct {
	raw      string
	segments []redactionSegment
}

// RedactionPolicy is a compiled, immutable set of paths to scrub.
//
// Immutability is what makes it safe to hand one pointer to every pipeline
// worker: the watch manager compiles a policy once per pool diff and publishes
// it, workers only ever read it, and a policy edit installs a *new* value rather
// than mutating the live one — the same discipline scopeInterest follows for
// selectors, and for the same reason.
//
// The zero value is not usable; build one with CompileRedaction. A nil
// *RedactionPolicy is meaningful and safe: it means "the built-in scrubs only",
// which is what an un-configured stream gets.
type RedactionPolicy struct {
	paths []redactionPath
}

// builtinRedaction is the policy every other policy contains: the last-applied
// annotation and nothing else. It is also what a nil *RedactionPolicy resolves
// to at apply time, so "no configuration" can never mean "no redaction".
var builtinRedaction = mustCompileRedaction(AnnotationRedactionPath(LastAppliedConfigAnnotation))

// mustCompileRedaction compiles paths that are constants in this package, and
// panics on failure. It runs at package init, so a malformed built-in path is a
// build-time-visible programming error rather than a stream that silently leaks.
func mustCompileRedaction(paths ...string) *RedactionPolicy {
	policy, err := CompileRedaction(paths)
	if err != nil {
		panic("pipeline: built-in redaction path does not compile: " + err.Error())
	}
	return policy
}

// CompileRedaction parses paths into a policy, always including the built-in
// scrubs (see LastAppliedConfigAnnotation).
//
// Duplicate paths collapse, and the result is sorted by path text, so two
// spellings of the same policy produce byte-identical redaction — which is what
// keeps a hash a stable function of the *policy*, not of the order rules
// happened to be merged in.
//
// An unparseable path fails the whole call rather than being skipped. A skipped
// path is a silent leak: the stream would keep running and keep writing the
// value its author asked to have scrubbed. Callers degrade the target instead
// (see WatchManager.translate), which writes nothing at all — the safe
// direction.
func CompileRedaction(paths []string) (*RedactionPolicy, error) {
	compiled := make([]redactionPath, 0, len(paths)+len(builtinPaths()))
	seen := make(map[string]struct{}, len(paths)+1)

	for _, raw := range slices.Concat(paths, builtinPaths()) {
		if _, done := seen[raw]; done {
			continue
		}
		path, err := parseRedactionPath(raw)
		if err != nil {
			return nil, err
		}
		seen[raw] = struct{}{}
		compiled = append(compiled, path)
	}

	slices.SortFunc(compiled, func(a, b redactionPath) int { return strings.Compare(a.raw, b.raw) })
	return &RedactionPolicy{paths: compiled}, nil
}

// builtinPaths returns the always-on path set. It is a function rather than a
// package-level slice because builtinRedaction is itself compiled from it during
// init, and a var initialized from another var in the same package is an
// ordering hazard nobody should have to reason about.
func builtinPaths() []string {
	return []string{AnnotationRedactionPath(LastAppliedConfigAnnotation)}
}

// MergeRedaction returns the union of every policy given.
//
// Union — never intersection — is the only defensible merge. Two rules can
// stream the same object to the same sink, and there is exactly one hash and one
// stored payload per (sink, identity); if the two disagree about what to scrub,
// honouring only what both asked for would let one rule's presence *unredact*
// the other's stream. Over-redacting the rule that asked for less is the safe
// direction, and it is what makes `extraRedaction` strictly additive as
// designed.
//
// nil entries contribute the built-in scrubs, which every compiled policy
// already carries. All-nil (or empty) returns nil, i.e. built-ins only.
func MergeRedaction(policies ...*RedactionPolicy) *RedactionPolicy {
	var merged []redactionPath
	seen := make(map[string]struct{})
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		for _, path := range policy.paths {
			if _, done := seen[path.raw]; done {
				continue
			}
			seen[path.raw] = struct{}{}
			merged = append(merged, path)
		}
	}
	if len(merged) == 0 {
		return nil
	}
	slices.SortFunc(merged, func(a, b redactionPath) int { return strings.Compare(a.raw, b.raw) })
	return &RedactionPolicy{paths: merged}
}

// Paths returns the canonical text of every path this policy scrubs, sorted.
// It is what log lines and tests read; nothing about redaction behaviour depends
// on it.
func (p *RedactionPolicy) Paths() []string {
	if p == nil {
		return builtinRedaction.Paths()
	}
	out := make([]string, len(p.paths))
	for i, path := range p.paths {
		out[i] = path.raw
	}
	return out
}

// Apply returns object with every configured path replaced by RedactionSentinel.
//
// It never mutates object, and it does not deep-copy it either. The object
// reaching this function shares its spec, status and every nested value with the
// informer's cached instance (see stripVolatileFields), so mutation would
// corrupt the watch cache for every other reader in the process — while a full
// copy would reintroduce, on every event, exactly the allocation Task 2.3
// removed. Instead this is copy-on-write: only the maps and slices *on a path
// that actually changed something* are copied, and every untouched subtree is
// shared with the argument by reference. An object no path matches is returned
// as-is, allocating nothing, which is the overwhelmingly common case.
//
// The result therefore aliases object and must be treated as read-only by the
// caller, exactly like the value it was built from.
//
// A nil policy applies the built-in scrubs (see LastAppliedConfigAnnotation), so
// there is no way to call this and get an unredacted object back.
func (p *RedactionPolicy) Apply(object map[string]any) map[string]any {
	if p == nil {
		p = builtinRedaction
	}
	current := object
	for _, path := range p.paths {
		if next, changed := redactValue(current, path.segments); changed {
			// A change at the root can only produce a map: the first segment is
			// consumed by a map lookup, so redactValue never rewrites the root
			// itself into a scalar.
			current = next.(map[string]any)
		}
	}
	return current
}

// redactValue applies the remaining segments to value, returning the rewritten
// value and whether anything actually changed. changed=false means the caller
// must keep its own value untouched — that is what stops a copy from
// propagating up a path that matched nothing.
func redactValue(value any, segments []redactionSegment) (any, bool) {
	if len(segments) == 0 {
		// The leaf. Any type is replaced, not just a string: a policy naming a
		// map or an array means "this subtree must not be stored", and the
		// sentinel string is what it collapses to (documented in
		// docs/SCHEMA.md). Replacing an already-redacted value is skipped so
		// re-applying a policy is free rather than allocating a fresh copy of
		// every parent.
		if existing, isString := value.(string); isString && existing == RedactionSentinel {
			return value, false
		}
		return RedactionSentinel, true
	}

	object, isObject := value.(map[string]any)
	if !isObject {
		// A path that does not match the object's actual shape is a no-op, not
		// an error: policies are written once and applied to every object of a
		// kind, and half of them legitimately will not carry the field.
		return value, false
	}
	segment := segments[0]
	child, present := object[segment.name]
	if !present {
		return value, false
	}

	var replacement any
	var changed bool
	if segment.wildcard {
		replacement, changed = redactElements(child, segments[1:])
	} else {
		replacement, changed = redactValue(child, segments[1:])
	}
	if !changed {
		return value, false
	}

	// Copy-on-write: this map is on a path that changed, so it — and only it —
	// is copied before the new child is installed.
	out := make(map[string]any, len(object))
	maps.Copy(out, object)
	out[segment.name] = replacement
	return out, true
}

// redactElements applies the remaining segments to every element of an array,
// which is what `[*]` means. A value that is not an array is a no-op for the
// same reason a missing key is.
func redactElements(value any, segments []redactionSegment) (any, bool) {
	items, isArray := value.([]any)
	if !isArray {
		return value, false
	}

	var out []any
	changed := false
	for i, item := range items {
		replacement, itemChanged := redactValue(item, segments)
		if !itemChanged {
			continue
		}
		if !changed {
			out = slices.Clone(items)
			changed = true
		}
		out[i] = replacement
	}
	if !changed {
		return value, false
	}
	return out, true
}

// parseRedactionPath compiles one path.
//
// The grammar is deliberately tiny — dot-separated names, an optional `[*]`
// array wildcard per segment, and a bracket-quoted segment for keys the dot
// grammar cannot spell (see AnnotationRedactionPath). It is not JSONPath and
// will not grow into it: every construct a bigger grammar would add (filters,
// recursive descent, slices) is one whose match set depends on the object's
// contents, which would make "what does this policy redact?" unanswerable by
// reading the policy.
func parseRedactionPath(raw string) (redactionPath, error) {
	if raw == "" {
		return redactionPath{}, errEmptyRedactionPath
	}

	var segments []redactionSegment
	rest := raw
	for {
		name, remainder, err := parseSegmentName(raw, rest)
		if err != nil {
			return redactionPath{}, err
		}
		segment := redactionSegment{name: name}
		rest = remainder

		if strings.HasPrefix(rest, "[") {
			end := strings.Index(rest, "]")
			if end < 0 {
				return redactionPath{}, fmt.Errorf("redaction path %q: unterminated %q subscript", raw, "[")
			}
			subscript := rest[1:end]
			rest = rest[end+1:]

			switch {
			case subscript == "*":
				segment.wildcard = true
				segments = append(segments, segment)
			case strings.HasPrefix(subscript, `"`):
				key, unquoteErr := strconv.Unquote(subscript)
				if unquoteErr != nil {
					return redactionPath{}, fmt.Errorf("redaction path %q: %s is not a quoted key: %w",
						raw, subscript, unquoteErr)
				}
				if key == "" {
					return redactionPath{}, fmt.Errorf("redaction path %q: quoted key is empty", raw)
				}
				// A quoted subscript is just a map key whose name the dot
				// grammar cannot spell, so it becomes an ordinary segment of
				// its own rather than a third kind of step.
				segments = append(segments, segment, redactionSegment{name: key})
			default:
				return redactionPath{}, fmt.Errorf(
					"redaction path %q: subscript [%s] must be [*] or a quoted key", raw, subscript)
			}
		} else {
			segments = append(segments, segment)
		}

		switch {
		case rest == "":
			return redactionPath{raw: raw, segments: segments}, nil
		case strings.HasPrefix(rest, "."):
			rest = rest[1:]
			if rest == "" {
				return redactionPath{}, fmt.Errorf("redaction path %q: trailing %q", raw, ".")
			}
		default:
			return redactionPath{}, fmt.Errorf("redaction path %q: expected %q or end of path at %q", raw, ".", rest)
		}
	}
}

// parseSegmentName reads one field name off the front of rest, returning it and
// what follows. raw is threaded in only so the error names the whole path the
// author wrote rather than the suffix the parser happens to be looking at.
func parseSegmentName(raw, rest string) (name, remainder string, err error) {
	name = rest
	remainder = ""
	if cut := strings.IndexAny(rest, ".["); cut >= 0 {
		name = rest[:cut]
		remainder = rest[cut:]
	}
	if !redactionSegmentPattern.MatchString(name) {
		return "", "", fmt.Errorf("redaction path %q: %q is not a valid field name", raw, name)
	}
	return name, remainder, nil
}
