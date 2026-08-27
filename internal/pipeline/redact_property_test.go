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
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The specs in this file are the property-based half of the redaction engine's
// proof (Task 8.2). redact_test.go and process_test.go already pin the security
// property rigorously, but over hand-written objects under hand-written
// policies, which generalizes to "these cases hold" rather than "the function
// holds". Redaction is the one property in this repository whose failure is a
// disclosure, so it also gets the stronger form: the same assertions over a
// generated corpus of nested objects, under policies drawn from the paths those
// objects actually contain.
//
// Three properties, one corpus:
//
//   - two objects hash identically if and only if their non-redacted subtrees
//     are equal (TestRedactionHashesIdenticallyIffNonRedactedSubtreesAreEqual),
//   - applying a policy twice changes nothing and allocates nothing
//     (TestRedactionIsIdempotentOnGeneratedObjects),
//   - only the containers on a path that changed are copied
//     (TestRedactionSharesWhatItDoesNotEditOnGeneratedObjects).
//
// "Hash" means the value that reaches the sink's sha256 column, so the corpus is
// hashed through ObjectHash — the production path — rather than through a
// SHA-256 the test computes for itself. The generated object is planted as the
// `spec` of a minimal pod because stripVolatileFields edits `metadata` and
// nothing else: a generated `managedFields` or `resourceVersion` key at the root
// would be stripped before hashing and read as a counterexample when it is
// nothing of the kind.
//
// Difference is defined throughout as a difference in canonical JSON, not in Go
// types. The hash is a SHA-256 over json.Marshal output, so int64(1) and
// float64(1) are one value to it, and an object that turns one into the other
// genuinely has not changed. That is a property of hashing JSON rather than a
// weakness of redaction, and the mutators below draw replacements whose encoding
// differs rather than pretending otherwise. The generated strings are all valid
// UTF-8 for the same reason: the encoder rewrites invalid bytes to U+FFFD, which
// would make two different objects encode identically for a reason that has
// nothing to do with redaction.
//
// A failing case prints its seed and its index. Rerun exactly that corpus with
//
//	KUBERECORD_REDACTION_SEED=<seed> go test ./internal/pipeline/ -run <TestName>
//
// Per the task's acceptance criteria, a counterexample is a finding: it is
// reported, not generated around.

const (
	// propertyCases is the corpus size. The generated objects are small and all
	// three properties together run in well under a second, which is the budget
	// that matters — `make test` gates every pull request.
	propertyCases = 300

	// propertyMinDepth is the acceptance criterion's floor. Every generated
	// object carries at least one forced chain this many containers deep, so the
	// corpus cannot quietly degenerate into flat maps the day someone retunes
	// the branching probabilities.
	propertyMinDepth = 4
	// propertyMaxDepth bounds the random nesting layered on top of that chain.
	propertyMaxDepth = 6
	// propertyMaxFanout bounds how many keys a generated map and how many
	// elements a generated array may carry.
	propertyMaxFanout = 4

	// propertyDirectionFloor is how many of the propertyCases cases each
	// direction of the iff property must actually exercise. A corpus that
	// happened to generate only policies matching nothing would satisfy every
	// assertion vacuously, which is the one way a property test can be green and
	// worthless.
	propertyDirectionFloor = 40
	// propertyShareFloor is the same guard for the copy-on-write spec's
	// "nothing matched, so nothing was copied" branch, which only absent-only
	// policies reach.
	propertyShareFloor = 15
)

// specRoot is the key the generated object is planted under. Every generated
// policy path starts here, which keeps the corpus clear of the metadata fields
// stripVolatileFields removes before hashing.
const specRoot = "spec"

// propertySeedEnv overrides the fixed seed, for local exploration of corpora
// this file does not ship with.
const propertySeedEnv = "KUBERECORD_REDACTION_SEED"

// defaultPropertySeed keeps the corpus deterministic. A property test that fails
// on one pull request in fifty, for reasons unrelated to that pull request, is
// deleted by the third person it blocks — so the seed is a constant, CI is
// reproducible from the commit alone, and the exploration story lives in the
// environment override rather than in the clock.
const defaultPropertySeed = 0x6B75626572656301

// propertySeed resolves the corpus seed, rejecting an unusable override rather
// than silently falling back to the default — an operator who set the variable
// wants that corpus, and quietly running a different one would waste the run.
func propertySeed(t *testing.T) uint64 {
	t.Helper()
	raw, set := os.LookupEnv(propertySeedEnv)
	if !set {
		return defaultPropertySeed
	}
	seed, err := strconv.ParseUint(raw, 0, 64)
	if err != nil {
		t.Fatalf("%s=%q is not a uint64: %v", propertySeedEnv, raw, err)
	}
	return seed
}

// caseRNG gives each case its own stream, so a counterexample at index N is
// reproducible without replaying the N-1 cases before it — which is what makes
// the printed seed worth printing.
func caseRNG(seed uint64, index int) *rand.Rand {
	return rand.New(rand.NewPCG(seed, uint64(index)))
}

// asciiKeyNames are spellable by the dot grammar: they match
// redactionSegmentPattern, which is the parser's half of the CRD's fieldPath
// pattern.
var asciiKeyNames = []string{
	"data", "spec", "env", "value", "name", "items", "config", "a", "b-c", "_x", "tls",
}

// awkwardKeyNames are not, and are reachable only through the ["quoted"]
// subscript — which is the whole reason that subscript exists (see
// AnnotationRedactionPath).
//
// `a]b` is in the pool on purpose and is reachable by nothing at all:
// parseRedactionPath takes the first `]` in the remainder as the end of the
// subscript, so a `]` inside the quotes truncates it. collectPathCandidates has
// to skip such a key, and TestRedactionPathGrammarCannotNameTheseShapes records
// why leaving it unreachable is acceptable rather than a gap to widen the
// grammar for.
var awkwardKeyNames = []string{
	"ключ", "配置", "🔑", "a.b", "app.kubernetes.io/name",
	"with space", "0leading", "-dash", `quote"key`, "a]b",
}

// stringValues is deliberately not all ASCII: multi-byte keys and values are the
// case a byte-oriented path walker or a naive canonical encoder gets wrong, and
// they are the case an operator running a non-English cluster hits first.
// RedactionSentinel itself is in the pool because sentinel-valued *input* is the
// branch both the idempotence short circuit and the copy-on-write accounting
// turn on.
var stringValues = []string{
	"hunter2", "", "correct horse", "значення", "秘密の値", "🔒🔑",
	"égalité", "line\nbreak", `quote"inside`, "[REDACTED]-ish", RedactionSentinel,
}

// spineKey names the forced nesting chain. It is dot-grammar-legal so the chain
// is nameable end to end; an unnameable spine would put the deepest part of
// every generated object out of reach of every generated policy, and the depth
// floor would buy nothing. It is absent from asciiKeyNames so a random draw can
// never overwrite the spine and shorten the object below the floor.
const spineKey = "spine"

// genKey draws a map key, one in four of them unspellable by the dot grammar.
func genKey(rng *rand.Rand) string {
	if rng.IntN(4) == 0 {
		return awkwardKeyNames[rng.IntN(len(awkwardKeyNames))]
	}
	return asciiKeyNames[rng.IntN(len(asciiKeyNames))]
}

// genScalar draws a leaf from the value domain unstructured content actually
// holds: the types client-go's converter produces from decoded JSON, and nothing
// else. NaN and +Inf are absent because json.Marshal refuses them, so no real
// object can carry one either.
func genScalar(rng *rand.Rand) any {
	switch rng.IntN(8) {
	case 0:
		return nil
	case 1:
		return rng.IntN(2) == 0
	case 2:
		return int64(rng.IntN(2001) - 1000)
	case 3:
		// Eighths are exactly representable, so the encoding is stable across
		// platforms. The integral ones collide with int64 on purpose: that
		// collision is the canonical-JSON equality this file documents.
		return float64(rng.IntN(2001)-1000) / 8
	default:
		return stringValues[rng.IntN(len(stringValues))]
	}
}

// genValue draws any value, biased towards leaves so the random part of an
// object stays small; the guaranteed depth comes from the spine, not from luck.
func genValue(rng *rand.Rand, maxDepth int) any {
	if maxDepth <= 0 {
		return genScalar(rng)
	}
	switch rng.IntN(6) {
	case 0, 1:
		return genObject(rng, 0, maxDepth)
	case 2:
		return genArray(rng, maxDepth)
	default:
		return genScalar(rng)
	}
}

// genArray may return an empty array, which is precisely the case
// redactElements must not clone: no element changed, so nothing may be copied.
func genArray(rng *rand.Rand, maxDepth int) []any {
	items := make([]any, rng.IntN(propertyMaxFanout+1))
	for i := range items {
		items[i] = genValue(rng, maxDepth-1)
	}
	return items
}

// genObject builds one map. minDepth forces a chain of that many further
// containers beneath it, which is how every generated object is guaranteed to
// reach propertyMinDepth rather than merely tending to.
func genObject(rng *rand.Rand, minDepth, maxDepth int) map[string]any {
	object := make(map[string]any, propertyMaxFanout+1)
	for range 1 + rng.IntN(propertyMaxFanout) {
		object[genKey(rng)] = genValue(rng, maxDepth-1)
	}
	if minDepth > 0 {
		object[spineKey] = genSpine(rng, minDepth-1, maxDepth-1)
	}
	return object
}

// genSpine returns the next link of the forced chain: a map, or an array of maps
// so the chain runs through the `[*]` wildcard about as often as through plain
// map descent. An array link adds a level the depth budget does not charge for,
// which only ever makes an object deeper than the floor.
func genSpine(rng *rand.Rand, minDepth, maxDepth int) any {
	if rng.IntN(3) != 0 {
		return genObject(rng, minDepth, maxDepth)
	}
	items := make([]any, 1+rng.IntN(propertyMaxFanout-1))
	for i := range items {
		items[i] = genObject(rng, minDepth, maxDepth)
	}
	return items
}

// podWithGeneratedSpec wraps a generated object in the smallest object the
// pipeline will hash. The metadata is fixed and carries no annotations, so the
// built-in scrub every policy contains matches nothing and the corpus measures
// the generated paths alone.
func podWithGeneratedSpec(spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":            "generated",
			"namespace":       "default",
			"uid":             testUID,
			"resourceVersion": "1",
		},
		specRoot: spec,
	}}
}

// location is one concrete position inside an object: map keys (string) and
// array indices (int) interleaved, e.g. ["spec","containers",0,"env",1,"value"].
//
// A redaction path is a *pattern*; a location is what one pattern resolved to in
// one particular object. Every mutation and every pointer-identity assertion
// below is expressed in terms of locations, because "mutate only what the policy
// redacts" and "this map is on a path that changed" are both statements about
// resolved positions rather than about patterns.
type location []any

func (l location) String() string {
	if len(l) == 0 {
		return "<root>"
	}
	parts := make([]string, len(l))
	for i, step := range l {
		parts[i] = fmt.Sprint(step)
	}
	return strings.Join(parts, "/")
}

// child returns l extended by one step, never aliasing l's backing array —
// sibling locations are built from the same prefix and would otherwise overwrite
// each other's last step.
func (l location) child(step any) location {
	return append(slices.Clone(l), step)
}

// covers reports whether l is at or above other.
func (l location) covers(other location) bool {
	if len(l) > len(other) {
		return false
	}
	for i, step := range l {
		if step != other[i] {
			return false
		}
	}
	return true
}

// equals reports whether l and other are the same position.
func (l location) equals(other location) bool {
	return len(l) == len(other) && l.covers(other)
}

// valueAt resolves a location, reporting whether it still exists. A location
// computed against one object and replayed against a rewritten one may not.
func valueAt(root any, loc location) (any, bool) {
	current := root
	for _, step := range loc {
		switch step := step.(type) {
		case string:
			object, isObject := current.(map[string]any)
			if !isObject {
				return nil, false
			}
			child, present := object[step]
			if !present {
				return nil, false
			}
			current = child
		case int:
			items, isArray := current.([]any)
			if !isArray || step < 0 || step >= len(items) {
				return nil, false
			}
			current = items[step]
		default:
			return nil, false
		}
	}
	return current, true
}

// setValueAt writes value at loc, in place. A location that no longer resolves
// is a bug in this file rather than in the engine, so it fails the test loudly
// instead of being skipped.
func setValueAt(t *testing.T, root any, loc location, value any) {
	t.Helper()
	if len(loc) == 0 {
		t.Fatal("setValueAt: the root has no parent to write through")
	}
	parent, ok := valueAt(root, loc[:len(loc)-1])
	if !ok {
		t.Fatalf("setValueAt: %s does not resolve", loc)
	}
	switch step := loc[len(loc)-1].(type) {
	case string:
		object, isObject := parent.(map[string]any)
		if !isObject {
			t.Fatalf("setValueAt: the parent of %s is not a map", loc)
		}
		object[step] = value
	case int:
		items, isArray := parent.([]any)
		if !isArray || step < 0 || step >= len(items) {
			t.Fatalf("setValueAt: the parent of %s is not an array of that length", loc)
		}
		items[step] = value
	default:
		t.Fatalf("setValueAt: %s ends in a step that is neither a key nor an index", loc)
	}
}

// deepCopyValue shares nothing with its argument. It is what the oracle and the
// mutators build on, so an oracle result can never accidentally satisfy a
// pointer-identity assertion that Apply's copy-on-write result would fail.
func deepCopyValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, child := range value {
			out[key] = deepCopyValue(child)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return value
	}
}

// canonicalJSON is the encoding the hash is taken over. Marshalling is the whole
// definition of equality in this file, so it lives in one function rather than
// being spelled out at each call site.
func canonicalJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

// matchLocations resolves one compiled path against value, appending every
// position it selects.
//
// It is the oracle the properties below are measured against, and it is
// deliberately a different shape from redactValue: it returns positions instead
// of a rewritten object, it has no copy-on-write bookkeeping, no
// already-redacted short circuit and no partially rewritten root threaded
// through it. Where the two agree across the whole corpus they agree for
// reasons; where they disagree, the disagreement is a finding rather than the
// same mistake made twice. It shares the parser, because parsing is pinned by
// TestParseRedactionPath and re-deriving the grammar here would only add a
// second place for the test itself to be wrong.
func matchLocations(value any, segments []redactionSegment, loc location, out *[]location) {
	if len(segments) == 0 {
		*out = append(*out, slices.Clone(loc))
		return
	}
	object, isObject := value.(map[string]any)
	if !isObject {
		return
	}
	child, present := object[segments[0].name]
	if !present {
		return
	}
	next := loc.child(segments[0].name)
	if !segments[0].wildcard {
		matchLocations(child, segments[1:], next, out)
		return
	}
	items, isArray := child.([]any)
	if !isArray {
		return
	}
	for i, item := range items {
		matchLocations(item, segments[1:], next.child(i), out)
	}
}

// maskDeep is the independent redactor: a full deep copy with the sentinel
// written at every position the policy selects, sharing no code and no structure
// with Apply.
//
// matches must be ordered deepest position first, so that a shallower path which
// swallows a deeper one wins. That is what Apply's traversal does too, and not
// by coincidence: whenever one path names an ancestor of another's target it is
// necessarily the shorter string, so CompileRedaction's sort runs it first and
// it rewrites the subtree the deeper path would have descended into.
func maskDeep(t *testing.T, object map[string]any, matches []location) map[string]any {
	t.Helper()
	out, isObject := deepCopyValue(object).(map[string]any)
	if !isObject {
		t.Fatal("maskDeep: the deep copy of a map is not a map")
	}
	for _, loc := range matches {
		if _, resolves := valueAt(out, loc); !resolves {
			t.Fatalf("maskDeep: %s does not resolve; matches are not ordered deepest first", loc)
		}
		setValueAt(t, out, loc, RedactionSentinel)
	}
	return out
}

// quotableKey reports whether key can appear inside a ["..."] subscript. A key
// holding a `]` cannot: parseRedactionPath takes the first `]` in the remainder
// as the end of the subscript, so the quoted text is truncated and the path
// fails to compile (see TestRedactionPathGrammarCannotNameTheseShapes).
func quotableKey(key string) bool {
	return key != "" && !strings.Contains(key, "]")
}

// pathCandidate is one path the grammar can express for a generated object.
//
// subscriptable records whether a `[...]` may still be attached, because the
// parser allows a subscript only immediately after a plain name segment: `a[*]`
// and `a["k"]` exist, `a[*][*]`, `a["k"][*]` and `a["k"]["j"]` do not. arrays
// counts how many of the positions the path resolved to are arrays, which is what
// lets choosePaths pick a wildcard guaranteed to match nothing.
type pathCandidate struct {
	raw           string
	subscriptable bool
	arrays        int
}

// collectPathCandidates walks a generated object and returns every path the
// grammar can name for it, in a deterministic order (map keys are visited
// sorted, because Go's map iteration order would otherwise make a seed
// unreproducible).
//
// The root path itself is excluded. A policy that redacts the whole generated
// object leaves nothing outside the redaction closure to mutate, so every such
// case would be vacuous for the converse half of the iff property — and the
// forward half is already covered by every other case.
//
// A key the grammar cannot name takes its whole subtree out of the pool with it:
// a path has to pass through the key to reach anything below it.
func collectPathCandidates(root map[string]any, rootPath string) []pathCandidate {
	index := make(map[string]*pathCandidate)
	var order []string

	var walk func(value any, path string, subscriptable bool)
	walk = func(value any, path string, subscriptable bool) {
		if path != rootPath {
			candidate, seen := index[path]
			if !seen {
				candidate = &pathCandidate{raw: path, subscriptable: subscriptable}
				index[path] = candidate
				order = append(order, path)
			}
			if _, isArray := value.([]any); isArray {
				candidate.arrays++
			}
		}
		switch value := value.(type) {
		case map[string]any:
			for _, key := range slices.Sorted(maps.Keys(value)) {
				switch {
				case redactionSegmentPattern.MatchString(key):
					walk(value[key], path+"."+key, true)
				case subscriptable && quotableKey(key):
					walk(value[key], path+"["+strconv.Quote(key)+"]", false)
				}
			}
		case []any:
			// `[*]` attaches to the name that precedes it, so an array reached
			// by a subscript has unreachable elements.
			if !subscriptable {
				return
			}
			for _, item := range value {
				walk(item, path+"[*]", false)
			}
		}
	}
	walk(root, rootPath, true)

	candidates := make([]pathCandidate, 0, len(order))
	for _, path := range order {
		candidates = append(candidates, *index[path])
	}
	return candidates
}

// collectPositions returns every position inside object, deterministically
// ordered, excluding the root. It is the pool the converse half of the iff
// property draws its mutation target from.
func collectPositions(object map[string]any) []location {
	var out []location
	var walk func(value any, loc location)
	walk = func(value any, loc location) {
		if len(loc) > 0 {
			out = append(out, loc)
		}
		switch value := value.(type) {
		case map[string]any:
			for _, key := range slices.Sorted(maps.Keys(value)) {
				walk(value[key], loc.child(key))
			}
		case []any:
			for i, item := range value {
				walk(item, loc.child(i))
			}
		}
	}
	walk(object, nil)
	return out
}

// maximalLocations drops any position lying beneath another in the set, so a
// mutation writes at the top of each redacted subtree rather than into one that
// is about to be replaced wholesale.
func maximalLocations(locs []location) []location {
	out := make([]location, 0, len(locs))
	for _, loc := range locs {
		beneathAnother := slices.ContainsFunc(locs, func(other location) bool {
			return len(other) < len(loc) && other.covers(loc)
		})
		if !beneathAnother {
			out = append(out, loc)
		}
	}
	return out
}

// choosePaths assembles one policy's worth of paths for a generated object.
//
// Every policy carries at least one path that matches nothing, because that is
// the common case in production rather than an edge: a policy is written once
// and applied to every object of a kind, and half of them legitimately do not
// carry the field.
func choosePaths(rng *rand.Rand, candidates []pathCandidate, absentOnly bool) []string {
	// A key the generator never emits, so this path is unmatchable by
	// construction rather than by luck.
	paths := []string{specRoot + ".absent_" + strconv.Itoa(rng.IntN(1000))}
	if len(candidates) == 0 {
		return paths
	}
	order := rng.Perm(len(candidates))
	head := candidates[order[0]]

	// A path that shares a real prefix and then leaves the object: the case
	// where a policy is nearly right, which a matcher that stopped one segment
	// early would pass and a correct one must not.
	paths = append(paths, head.raw+".absent_leaf")

	// A wildcard over something that is never an array. The engine treats a
	// subscript against the wrong shape as a no-op rather than an error, which
	// is what lets one policy cover a kind whose field is sometimes absent.
	for _, i := range order {
		if candidates[i].subscriptable && candidates[i].arrays == 0 {
			paths = append(paths, candidates[i].raw+"[*]")
			break
		}
	}
	if absentOnly {
		return paths
	}

	for _, i := range order[:min(1+rng.IntN(3), len(order))] {
		paths = append(paths, candidates[i].raw)
	}

	// An overlapping pair: a path and a path beneath it. Sorted by text the
	// shallower one runs first, so the deeper one finds a sentinel string where
	// it expected a map — the swallowing the whole design leans on.
	for _, candidate := range candidates {
		if strings.HasPrefix(candidate.raw, head.raw+".") {
			paths = append(paths, head.raw, candidate.raw)
			break
		}
	}
	return paths
}

// redactionCase is one generated (object, policy) pair, plus everything the
// properties need to know about how the two meet.
type redactionCase struct {
	seed  uint64
	index int

	// object is what the Apply-level specs run on; pod is the same generated
	// content planted where the hash-level specs need it.
	object map[string]any
	pod    *unstructured.Unstructured

	paths      []string
	policy     *RedactionPolicy
	absentOnly bool

	// matches is every position the policy selects, deepest first.
	matches []location
	// rewrites is the subset Apply actually changes. A position already holding
	// the sentinel is left alone, so it is not a reason for any parent to be
	// copied, and predicting copy-on-write from matches rather than from these
	// would fail on exactly the objects redaction has already been applied to.
	rewrites []location
	// outside is every position no policy path touches in either direction:
	// nothing at or above it is selected, and nothing beneath it is either.
	// Mutating one of these is the only mutation that is honestly "a difference
	// somewhere else".
	outside []location
}

// newRedactionCase generates case index of the corpus seeded by seed.
func newRedactionCase(t *testing.T, seed uint64, index int) *redactionCase {
	t.Helper()
	rng := caseRNG(seed, index)

	spec := genObject(rng, propertyMinDepth, propertyMaxDepth)
	c := &redactionCase{
		seed:   seed,
		index:  index,
		object: map[string]any{specRoot: spec},
		pod:    podWithGeneratedSpec(spec),
		// One policy in eight names nothing the object carries, which is how the
		// corpus reaches the branch where Apply returns its argument untouched
		// and allocates nothing at all.
		absentOnly: rng.IntN(8) == 0,
	}
	c.paths = choosePaths(rng, collectPathCandidates(spec, specRoot), c.absentOnly)

	policy, err := CompileRedaction(c.paths)
	if err != nil {
		t.Fatalf("%s\nthe path collector produced a path the parser rejects: %v", c.describe(t), err)
	}
	c.policy = policy

	var podMatches []location
	for _, raw := range policy.Paths() {
		parsed, parseErr := parseRedactionPath(raw)
		if parseErr != nil {
			t.Fatalf("%s\nre-parsing %q: %v", c.describe(t), raw, parseErr)
		}
		matchLocations(c.object, parsed.segments, nil, &c.matches)
		matchLocations(c.pod.Object, parsed.segments, nil, &podMatches)
	}
	deepestFirst := func(a, b location) int { return len(b) - len(a) }
	slices.SortStableFunc(c.matches, deepestFirst)
	slices.SortStableFunc(podMatches, deepestFirst)

	// The pod's metadata carries no annotations, so the built-in scrub every
	// policy contains matches nothing and every selected position lies under
	// `spec`. That is what lets one location set serve both the Apply-level
	// specs and the hash-level ones, so it is checked here rather than assumed.
	if len(podMatches) != len(c.matches) {
		t.Fatalf("%s\nthe policy selects %d positions in the pod and %d in the bare object",
			c.describe(t), len(podMatches), len(c.matches))
	}
	for i := range podMatches {
		if !podMatches[i].equals(c.matches[i]) {
			t.Fatalf("%s\nposition %d differs between the pod and the bare object: %s vs %s",
				c.describe(t), i, podMatches[i], c.matches[i])
		}
	}

	for _, loc := range c.matches {
		value, resolves := valueAt(c.object, loc)
		if !resolves {
			t.Fatalf("%s\nthe policy selected %s, which does not resolve", c.describe(t), loc)
		}
		if existing, isString := value.(string); isString && existing == RedactionSentinel {
			continue
		}
		c.rewrites = append(c.rewrites, loc)
	}

	for _, loc := range collectPositions(c.object) {
		touched := slices.ContainsFunc(c.matches, func(m location) bool {
			return m.covers(loc) || loc.covers(m)
		})
		if !touched {
			c.outside = append(c.outside, loc)
		}
	}
	return c
}

// describe is what a counterexample is reported with: the seed and index that
// regenerate it, the policy that found it, and the object it was found in.
func (c *redactionCase) describe(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("seed=%#x case=%d (rerun with %s=%#x go test ./internal/pipeline/ -run <TestName>)\npaths: %v\nobject: %s",
		c.seed, c.index, propertySeedEnv, c.seed, c.paths, canonicalJSON(t, c.object))
}

// mutate returns a fresh copy of the case's pod with a different value at every
// target. It never touches the case's own object, so the specs may call it
// repeatedly and in any order.
func (c *redactionCase) mutate(t *testing.T, rng *rand.Rand, targets []location) *unstructured.Unstructured {
	t.Helper()
	object, isObject := deepCopyValue(c.pod.Object).(map[string]any)
	if !isObject {
		t.Fatal("mutate: the deep copy of a map is not a map")
	}
	for _, loc := range targets {
		before, resolves := valueAt(object, loc)
		if !resolves {
			t.Fatalf("%s\nmutate: %s does not resolve", c.describe(t), loc)
		}
		setValueAt(t, object, loc, differentValue(t, rng, before))
	}
	return &unstructured.Unstructured{Object: object}
}

// differentValue draws a replacement whose canonical JSON differs from that of
// the value it replaces.
//
// "Differs" is measured on the encoding rather than with reflect.DeepEqual on
// purpose: the hash under test is a SHA-256 over json.Marshal output, so
// int64(1) and float64(1) are the same value to it, and a mutation that swapped
// one for the other would be asserting a difference the design never claimed.
func differentValue(t *testing.T, rng *rand.Rand, before any) any {
	t.Helper()
	encoded := canonicalJSON(t, before)
	for range 64 {
		candidate := genValue(rng, 3)
		if !bytes.Equal(canonicalJSON(t, candidate), encoded) {
			return candidate
		}
	}
	t.Fatalf("no replacement differing from %s after 64 draws", encoded)
	return nil
}

// mutationSalt keeps the stream a case's mutations are drawn from independent of
// the stream that generated the case, so a change to the generator does not
// silently change which positions get mutated.
const mutationSalt = 0x9E3779B97F4A7C15

// sameSlice reports whether two values are the same array. An empty array has no
// addressable element to compare, and cannot have been changed either —
// redactElements clones only when an element changed — so length is the whole
// question for it.
func sameSlice(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	return &a[0] == &b[0]
}

// TestRedactionHashesIdenticallyIffNonRedactedSubtreesAreEqual is Task 8.2's
// property, asserted by name over generated objects and generated policies:
// **two objects hash identically if and only if their non-redacted subtrees are
// equal.**
//
// Both directions are exercised, and they fail for opposite reasons, which is
// why neither alone would do. Two states differing only under a redacted path
// must collide — otherwise the sha256 column is an oracle an attacker can grind
// candidate values against, and the pipeline writes a second row announcing that
// a secret changed. Two states differing anywhere else must not collide —
// otherwise "stable" would mean "constant" and the pipeline would stop recording
// real changes, which is a quieter failure and a worse one.
//
// The hash is ObjectHash, the function the write path itself uses, so what is
// asserted here is the value that reaches the sink rather than a re-derivation
// of it.
func TestRedactionHashesIdenticallyIffNonRedactedSubtreesAreEqual(t *testing.T) {
	seed := propertySeed(t)
	collisions, distinctions := 0, 0

	for index := range propertyCases {
		c := newRedactionCase(t, seed, index)
		rng := caseRNG(seed^mutationSalt, index)

		base, err := ObjectHash(c.pod, c.policy)
		if err != nil {
			t.Fatalf("%s\nObjectHash: %v", c.describe(t), err)
		}

		// Apply agrees with the independent masker across the corpus. This is
		// what keeps the two directions below statements about redaction rather
		// than statements about SHA-256: the positions the mutators treat as
		// redacted are the positions the engine actually redacts.
		applied := canonicalJSON(t, c.policy.Apply(c.object))
		masked := canonicalJSON(t, maskDeep(t, c.object, c.matches))
		if !bytes.Equal(applied, masked) {
			t.Fatalf("%s\nApply and the independent masker disagree\n  Apply: %s\n masker: %s",
				c.describe(t), applied, masked)
		}

		// Differing only under a redacted path must collide.
		if targets := maximalLocations(c.matches); len(targets) > 0 {
			mutant := c.mutate(t, rng, targets)
			hash, hashErr := ObjectHash(mutant, c.policy)
			if hashErr != nil {
				t.Fatalf("%s\nObjectHash(redacted mutant): %v", c.describe(t), hashErr)
			}
			if hash != base {
				t.Fatalf("%s\ntwo states differing only at redacted positions %v hashed differently: %q vs %q\nmutant: %s",
					c.describe(t), targets, base, hash, canonicalJSON(t, mutant.Object[specRoot]))
			}
			collisions++
		}

		// Differing anywhere else must not.
		if len(c.outside) > 0 {
			target := c.outside[rng.IntN(len(c.outside))]
			mutant := c.mutate(t, rng, []location{target})
			hash, hashErr := ObjectHash(mutant, c.policy)
			if hashErr != nil {
				t.Fatalf("%s\nObjectHash(unredacted mutant): %v", c.describe(t), hashErr)
			}
			if hash == base {
				t.Fatalf("%s\na change at %s, which no policy path touches, left the hash at %q\nmutant: %s",
					c.describe(t), target, base, canonicalJSON(t, mutant.Object[specRoot]))
			}
			distinctions++
		}
	}

	// A corpus whose policies happened to match nothing would satisfy both
	// directions vacuously, which is the one way a property test can be green
	// and worthless.
	if collisions < propertyDirectionFloor || distinctions < propertyDirectionFloor {
		t.Fatalf("the corpus exercised %d collisions and %d distinctions over %d cases, want at least %d of each",
			collisions, distinctions, propertyCases, propertyDirectionFloor)
	}
}

// TestRedactionIsIdempotentOnGeneratedObjects is the generated form of
// TestRedactionIsIdempotent. A value is redacted on every event for its object,
// and warm-up re-reads state the write path already scrubbed, so re-applying a
// policy has to be a no-op — not a second rewrite that allocates a fresh copy of
// every parent map on an object that was already clean.
//
// "No-op" is asserted twice over: byte-identical output, and the same containers
// rather than equal ones.
func TestRedactionIsIdempotentOnGeneratedObjects(t *testing.T) {
	seed := propertySeed(t)

	for index := range propertyCases {
		c := newRedactionCase(t, seed, index)

		once := c.policy.Apply(c.object)
		twice := c.policy.Apply(once)

		if first, second := canonicalJSON(t, once), canonicalJSON(t, twice); !bytes.Equal(first, second) {
			t.Fatalf("%s\nre-applying the policy changed the object\n first: %s\nsecond: %s",
				c.describe(t), first, second)
		}
		if where, violated := firstSharingViolation(once, twice, nil); violated {
			t.Fatalf("%s\nre-applying the policy allocated a fresh container at %s", c.describe(t), where)
		}
	}
}

// firstSharingViolation walks two results side by side and reports the first
// container that is not literally the same container.
//
// It deliberately does not lean on the fact that a copy-on-write copy propagates
// all the way to the root, so that checking the root alone would do. That is
// true of the engine today; checking only the root would quietly stop meaning
// anything the day it stops being true.
func firstSharingViolation(before, after any, loc location) (location, bool) {
	switch before := before.(type) {
	case map[string]any:
		afterMap, isMap := after.(map[string]any)
		if !isMap || !sameMap(before, afterMap) {
			return loc, true
		}
		for _, key := range slices.Sorted(maps.Keys(before)) {
			if where, violated := firstSharingViolation(before[key], afterMap[key], loc.child(key)); violated {
				return where, true
			}
		}
	case []any:
		afterItems, isArray := after.([]any)
		if !isArray || !sameSlice(before, afterItems) {
			return loc, true
		}
		for i := range before {
			if where, violated := firstSharingViolation(before[i], afterItems[i], loc.child(i)); violated {
				return where, true
			}
		}
	}
	return nil, false
}

// TestRedactionSharesWhatItDoesNotEditOnGeneratedObjects is the generated form
// of TestRedactionSharesWhatItDoesNotEdit, and it pins both halves of the
// copy-on-write bargain rather than only the cheap one.
//
// Apply runs on every object the data plane observes. A redactor that deep-copied
// would hand back the per-event allocation Task 2.3 removed; a redactor that
// mutated in place would corrupt the informer's cache for every other reader in
// the process. So a container must be copied exactly when a rewrite happened at
// or beneath it, and shared otherwise — and an object nothing rewrote must come
// back as the very same map.
func TestRedactionSharesWhatItDoesNotEditOnGeneratedObjects(t *testing.T) {
	seed := propertySeed(t)
	shared, copied := 0, 0

	for index := range propertyCases {
		c := newRedactionCase(t, seed, index)

		before := deepCopyValue(c.object)
		out := c.policy.Apply(c.object)

		if !reflect.DeepEqual(c.object, before) {
			t.Fatalf("%s\nApply mutated the object it was given, which the informer's cache shares with every other reader in the process\nnow: %s",
				c.describe(t), canonicalJSON(t, c.object))
		}

		if len(c.rewrites) == 0 {
			if !sameMap(out, c.object) {
				t.Fatalf("%s\nthe policy rewrote nothing, so the object must come back as-is, but it was copied", c.describe(t))
			}
			shared++
			continue
		}
		copied++
		if where, why := firstCopyViolation(c.rewrites, c.object, out, nil); why != "" {
			t.Fatalf("%s\nat %s: %s", c.describe(t), where, why)
		}
	}

	if shared < propertyShareFloor || copied < propertyDirectionFloor {
		t.Fatalf("the corpus exercised %d untouched objects and %d rewritten ones over %d cases, want at least %d and %d",
			shared, copied, propertyCases, propertyShareFloor, propertyDirectionFloor)
	}
}

// firstCopyViolation walks a generated object and the result of applying a
// policy to it side by side, reporting the first container that was copied when
// it should have been shared, or shared when it must have been copied.
//
// The rule is the whole of the copy-on-write design: a container is copied
// exactly when a rewrite happened at or beneath it. The prediction is made from
// the case's rewrites rather than its matches, because a position already
// holding the sentinel is left alone and is therefore not a reason for any
// parent to be copied.
//
// The generator never aliases one container into two positions, so pointer
// identity here is a statement about the position under examination rather than
// about two that happen to share.
func firstCopyViolation(rewrites []location, before, after any, loc location) (location, string) {
	if slices.ContainsFunc(rewrites, loc.equals) {
		// The subtree ends here: the policy rewrites this position, so the
		// result holds the sentinel and there is nothing below it to compare.
		if after != RedactionSentinel {
			return loc, fmt.Sprintf("the policy rewrites this position, but the result holds %#v rather than the sentinel", after)
		}
		return nil, ""
	}
	expectCopy := slices.ContainsFunc(rewrites, loc.covers)

	switch before := before.(type) {
	case map[string]any:
		afterMap, isMap := after.(map[string]any)
		if !isMap {
			return loc, fmt.Sprintf("a map became a %T", after)
		}
		if sameMap(before, afterMap) == expectCopy {
			return loc, copyVerdict(expectCopy, "map")
		}
		for _, key := range slices.Sorted(maps.Keys(before)) {
			if where, why := firstCopyViolation(rewrites, before[key], afterMap[key], loc.child(key)); why != "" {
				return where, why
			}
		}
	case []any:
		afterItems, isArray := after.([]any)
		if !isArray || len(afterItems) != len(before) {
			return loc, fmt.Sprintf("an array of %d became a %T of %d", len(before), after, len(afterItems))
		}
		if sameSlice(before, afterItems) == expectCopy {
			return loc, copyVerdict(expectCopy, "array")
		}
		for i := range before {
			if where, why := firstCopyViolation(rewrites, before[i], afterItems[i], loc.child(i)); why != "" {
				return where, why
			}
		}
	}
	return nil, ""
}

// copyVerdict names which half of the bargain was broken, since the two failures
// have opposite causes and opposite fixes.
func copyVerdict(expectCopy bool, kind string) string {
	if expectCopy {
		return "a rewrite happened at or beneath this " + kind +
			", so it must have been copied, but the result shares it with the caller's object"
	}
	return "nothing at or beneath this " + kind +
		" was rewritten, so it must be shared, but the result holds a copy"
}

// TestRedactionPathGrammarCannotNameTheseShapes records, as executable text,
// the four positions the path grammar cannot reach — so that the generated
// corpus skipping them is a documented limit rather than a silent hole, and so
// that a future widening of the grammar has to come here and say so.
//
// The parser allows at most one subscript per segment and requires `.` or the
// end of the path after it (see parseRedactionPath). That is a deliberate
// consequence of keeping the grammar tiny rather than growing it into JSONPath,
// and none of these shapes is reachable in practice: Kubernetes annotation and
// label keys are qualified names, so the one construct the subscript exists for
// — AnnotationRedactionPath — can never produce any of them.
func TestRedactionPathGrammarCannotNameTheseShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		why  string
	}{
		{
			name: "a quoted key beneath a quoted key",
			path: `spec["ключ"]["вкладений"]`,
			why:  "a subscript must follow a plain name segment, and a quoted key is not one",
		},
		{
			name: "a wildcard after a quoted key",
			path: `spec["ключ"][*]`,
			why:  "same rule: an array whose own key needs quoting has unreachable elements",
		},
		{
			name: "nested array wildcards",
			path: `spec.items[*][*]`,
			why:  "an array of arrays is redactable only whole, at its outermost level",
		},
		{
			name: "a key holding a closing bracket",
			path: `spec["a]b"]`,
			why:  "the parser takes the first ] in the remainder as the end of the subscript, truncating the quoted key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CompileRedaction([]string{tc.path}); err == nil {
				t.Fatalf("CompileRedaction(%q) succeeded; the grammar was widened without updating this spec (%s)", tc.path, tc.why)
			}
		})
	}
}
