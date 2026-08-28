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
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Two independent RFC 6902 implementations sit on either side of the diff
// column, and the specs in this file are the only test that holds them to the
// same reading of the spec.
//
// The write path computes a row's diff with github.com/wI2L/jsondiff (see
// ComputeDiff). The read path reconstructs an object's state at an instant by
// applying those diffs with github.com/evanphx/json-patch/v5. Nothing else pairs
// them: the reconstruction fixtures in the query conformance suite are patches a
// test author wrote by hand, so what they prove is that the consumer applies
// *hand-written* patches. Only the other claim — that the consumer applies the
// patches the producer really emits — is the product.
//
// The divergence to worry about is JSON Pointer escaping. A key containing `/`
// must be escaped `~1` and a `~` must be escaped `~0`, and Kubernetes keys carry
// `/` constantly: `app.kubernetes.io/name`,
// `kubectl.kubernetes.io/last-applied-configuration`. A patch touching one
// renders as `/metadata/annotations/app.kubernetes.io~1name`, and the two
// libraries have to agree on that spelling in both directions. Array index
// ordering, `-` as an append token, and the distinction between a value set to
// null and a key removed are the other places two readings can differ.
//
// A mismatch would not crash. It would produce a reconstruction that is
// plausible and wrong: `get --at` handing an auditor a document the cluster was
// never in, under a header asserting it was rebuilt from N patches. For a
// product whose value is evidentiary integrity, that is the worst available
// failure and the one nobody notices.
//
// Per the task's acceptance criteria, a counterexample here is a finding. It is
// reported and a human decides how to resolve it; it is never worked around by
// retuning the generator, the corpus or the producer's options.

// The producer emits no move and no copy operations, and this is the reason:
// jsondiff gates both behind the jsondiff.Factorize() option, which ComputeDiff
// does not pass. TestProducerEmitsNoMoveOrCopyOps asserts the consequence over
// every pair in this file, so an options change that started emitting them
// fails here rather than becoming a shape no test has ever applied.
const factorizeOption = "jsondiff.Factorize()"

// patchCase is one (before, after) pair, plus what the pair is meant to prove.
type patchCase struct {
	name   string
	before []byte
	after  []byte
	// wantInPatch are substrings the produced patch must contain. They are the
	// per-case non-vacuity guard: a case named for `~1` escaping that produced an
	// unescaped pointer, or none at all, would otherwise round-trip green while
	// testing nothing it was written for.
	wantInPatch []string
	// reversalMustDiffer marks a pair whose operations must be applied in the
	// order given. The assertion is made by applying the patch backwards and
	// requiring a different answer, which is what makes "index ordering is
	// load-bearing" a fact about this patch rather than a claim about patches.
	reversalMustDiffer bool
	// check is an extra assertion over the reconstruction.
	check func(t *testing.T, got []byte)
	// wantRefused marks a pair the producer must decline to diff, because the
	// consumer could not apply the patch faithfully. See errEmptyInteriorToken.
	wantRefused bool
}

// TestProducedPatchesApplyThroughTheConsumer is the round trip over named
// shapes: producer, then consumer, then the target document back.
func TestProducedPatchesApplyThroughTheConsumer(t *testing.T) {
	for _, tc := range namedPatchCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			patch, refused := roundTrip(t, tc.name, tc.before, tc.after)
			if refused != tc.wantRefused {
				t.Fatalf("the producer refused this pair = %v, want %v; a refusal means the write path "+
					"records full state instead of a diff, so which of the two happens is part of what "+
					"this case pins", refused, tc.wantRefused)
			}
			if patch == nil {
				return
			}
			for _, want := range tc.wantInPatch {
				if !strings.Contains(string(patch), want) {
					t.Errorf("the produced patch does not contain %q, so this case is not exercising the "+
						"shape it is named for:\n  before %s\n  after  %s\n  patch  %s",
						want, tc.before, tc.after, patch)
				}
			}
			if tc.reversalMustDiffer {
				assertOrderIsLoadBearing(t, tc.before, tc.after, patch)
			}
			if tc.check != nil {
				got, err := applyProducedPatch(tc.before, patch)
				if err != nil {
					t.Fatalf("applying the patch a second time: %v", err)
				}
				tc.check(t, got)
			}
		})
	}
}

// namedPatchCases is the corpus of shapes a hand-written fixture would not
// reach.
func namedPatchCases(t *testing.T) []patchCase {
	cases := []patchCase{{
		name: "an annotation key containing a slash, escaped ~1",
		before: object(t, annotated(map[string]any{
			"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{"replicas":1}}`,
		})),
		after: object(t, annotated(map[string]any{
			"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{"replicas":3}}`,
		})),
		wantInPatch: []string{"/metadata/annotations/kubectl.kubernetes.io~1last-applied-configuration"},
	}, {
		name:        "a key containing a tilde, escaped ~0",
		before:      object(t, annotated(map[string]any{"tilde~key": "before"})),
		after:       object(t, annotated(map[string]any{"tilde~key": "after"})),
		wantInPatch: []string{"/metadata/annotations/tilde~0key"},
	}, {
		// The `~1` already in the key is what makes this more than the two cases
		// above put together: escaping is not a substitution that can be done in
		// either order. A `~` must become `~0` before a `/` becomes `~1`, or the
		// `1` of a freshly written `~1` is itself escaped and the pointer names a
		// key that does not exist.
		name:   "a key containing both, and a literal ~1 to escape after them",
		before: object(t, annotated(map[string]any{"both~and/slash~1and🔑": "значення"})),
		after:  object(t, annotated(map[string]any{"both~and/slash~1and🔑": "秘密の値"})),
		wantInPatch: []string{
			"/metadata/annotations/both~0and~1slash~01and🔑",
		},
	}, {
		name:        "an element inserted into an array",
		before:      object(t, withArgs("--a", "--c")),
		after:       object(t, withArgs("--a", "--b", "--c")),
		wantInPatch: []string{`"op":"replace"`, `"op":"add"`},
	}, {
		name:        "an element removed from an array",
		before:      object(t, withArgs("--a", "--b", "--c")),
		after:       object(t, withArgs("--a", "--c")),
		wantInPatch: []string{`"op":"remove"`},
	}, {
		name:        "an array reordered",
		before:      object(t, withArgs("--a", "--b", "--c")),
		after:       object(t, withArgs("--c", "--a", "--b")),
		wantInPatch: []string{`"op":"replace"`},
	}, {
		name:        "an element appended, addressed by the - token",
		before:      object(t, withArgs("--a", "--b")),
		after:       object(t, withArgs("--a", "--b", "--c")),
		wantInPatch: []string{`"path":"/spec/args/-"`},
	}, {
		// One array shrinks and a sibling grows, so the patch is two removes
		// followed by two appends. Both removes name the same index — the second
		// is meaningful only after the first has run — and the appends must land
		// after them, which is the ordering the reversal assertion pins.
		name:   "a removal followed by an addition, where index ordering decides the answer",
		before: object(t, withMatrix([]any{"a", "b", "c"}, []any{"z"})),
		after:  object(t, withMatrix([]any{"a"}, []any{"z", "y", "x"})),
		wantInPatch: []string{
			`{"op":"remove","path":"/spec/matrix/0/1"},{"op":"remove","path":"/spec/matrix/0/1"}`,
			`"path":"/spec/matrix/1/-"`,
		},
		reversalMustDiffer: true,
	}, {
		name:   "a nested map created where the parent did not exist",
		before: object(t, map[string]any{"spec": map[string]any{"replicas": int64(1)}}),
		after: object(t, map[string]any{"spec": map[string]any{
			"replicas": int64(1),
			"template": map[string]any{"metadata": map[string]any{
				"labels": map[string]any{"app.kubernetes.io/name": "checkout"},
			}},
		}}),
		wantInPatch: []string{`"op":"add","path":"/spec/template"`},
	}, {
		// The two cases below are the pair a naive implementation conflates. A key
		// whose value is null is present; a key that was removed is not, and
		// `remove` is the only operation that expresses the second. An
		// implementation that treated null as absence would render one as the
		// other and a reconstruction would gain or lose a field.
		name:        "a value replaced with null, which leaves the key present",
		before:      object(t, map[string]any{"spec": map[string]any{"paused": true, "replicas": int64(2)}}),
		after:       object(t, map[string]any{"spec": map[string]any{"paused": nil, "replicas": int64(2)}}),
		wantInPatch: []string{`{"value":null,"op":"replace","path":"/spec/paused"}`},
		check: func(t *testing.T, got []byte) {
			t.Helper()
			if !strings.Contains(string(got), `"paused":null`) {
				t.Errorf("the reconstruction dropped the key rather than nulling it: %s", got)
			}
		},
	}, {
		name:        "a key removed, which is not the same as nulling it",
		before:      object(t, map[string]any{"spec": map[string]any{"paused": true, "replicas": int64(2)}}),
		after:       object(t, map[string]any{"spec": map[string]any{"replicas": int64(2)}}),
		wantInPatch: []string{`{"op":"remove","path":"/spec/paused"}`},
		check: func(t *testing.T, got []byte) {
			t.Helper()
			if strings.Contains(string(got), `"paused"`) {
				t.Errorf("the reconstruction kept the removed key: %s", got)
			}
		},
	}, {
		// The boundary the guard draws. An empty key that is the *last* token of a
		// pointer is addressed directly by both libraries and agrees, so it is still
		// diffed — refusing it too would trade an unreachable bug for a real loss of
		// compression on every object that has one.
		name:        "a key that is the empty string, addressed as the last token",
		before:      object(t, map[string]any{"spec": map[string]any{"": "before"}}),
		after:       object(t, map[string]any{"spec": map[string]any{"": "after"}}),
		wantInPatch: []string{`"path":"/spec/"`},
	}, {
		name:        "an empty reference token walked through, which the producer must refuse",
		before:      object(t, map[string]any{"spec": map[string]any{"": map[string]any{"k": "before"}}}),
		after:       object(t, map[string]any{"spec": map[string]any{"": map[string]any{"k": "after"}}}),
		wantRefused: true,
	}, {
		name:        "an empty reference token walked through on the way to an append",
		before:      object(t, map[string]any{"spec": map[string]any{"": []any{"a"}}}),
		after:       object(t, map[string]any{"spec": map[string]any{"": []any{"a", "b"}}}),
		wantRefused: true,
	}, realisticPatchCase(t), redactedPatchCase(t)}

	return cases
}

// realisticPatchCase diffs a real Deployment against an edited copy of itself.
//
// The synthetic shapes above each isolate one property; this one is the shape
// the producer actually meets — 6KB of nested status, three annotations whose
// keys contain `/`, and an edit spread across all of them at once. A pointer
// escaping bug that only appears when a patch carries several operations at
// different depths has nowhere to hide here.
func realisticPatchCase(t *testing.T) patchCase {
	t.Helper()

	before := loadTestdataObject(t, "deployment.json")
	after := deepCopyObject(t, before)

	spec, ok := after["spec"].(map[string]any)
	if !ok {
		t.Fatalf("testdata/deployment.json has no spec object, so this case cannot edit one")
	}
	spec["replicas"] = float64(5)

	meta, ok := after["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("testdata/deployment.json has no metadata object")
	}
	annotations, ok := meta["annotations"].(map[string]any)
	if !ok {
		t.Fatalf("testdata/deployment.json has no metadata.annotations, which is the case's whole subject")
	}
	const lastApplied = "kubectl.kubernetes.io/last-applied-configuration"
	if _, present := annotations[lastApplied]; !present {
		t.Fatalf("testdata/deployment.json no longer carries the %s annotation, so this case has "+
			"stopped covering the ~1 escaping it was written for", lastApplied)
	}
	annotations[lastApplied] = `{"apiVersion":"apps/v1","kind":"Deployment","spec":{"replicas":5}}`
	annotations["deployment.kubernetes.io/revision"] = "9"
	delete(annotations, "argocd.argoproj.io/tracking-id")
	annotations["kuberecord.io/edited~by"] = "the round-trip corpus"

	return patchCase{
		name:   "a realistic Deployment from testdata, edited at several depths at once",
		before: marshal(t, before),
		after:  marshal(t, after),
		wantInPatch: []string{
			"/metadata/annotations/kubectl.kubernetes.io~1last-applied-configuration",
			"/metadata/annotations/argocd.argoproj.io~1tracking-id",
			"/metadata/annotations/kuberecord.io~1edited~0by",
			`"path":"/spec/replicas"`,
		},
	}
}

// redactedPatchCase produces the patch through the whole write path — normalize,
// redact, diff — for a change that adds a field the policy scrubs.
//
// The sentinel is a value like any other to both libraries, and that is exactly
// the claim worth pinning: it must arrive in the patch verbatim and come back out
// of the reconstruction verbatim. A sentinel mangled in transit would read as a
// recorded value rather than as a redaction, which is a disclosure-shaped
// failure in the one direction nobody checks.
func redactedPatchCase(t *testing.T) patchCase {
	t.Helper()

	policy := mustCompile(t, "spec.dbPassword")
	base := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "checkout", "namespace": "payments", "uid": "uid-a"},
		"spec":       map[string]any{"replicas": int64(2)},
	}
	before := normalized(t, base, policy)

	withSecret := deepCopyObject(t, base)
	spec, _ := withSecret["spec"].(map[string]any)
	spec["dbPassword"] = "hunter2"
	after := normalized(t, withSecret, policy)

	if !bytes.Contains(after, []byte(RedactionSentinel)) {
		t.Fatalf("the redaction policy did not fire, so this case would prove nothing: %s", after)
	}

	return patchCase{
		name:        "a change touching only a redacted value",
		before:      before,
		after:       after,
		wantInPatch: []string{RedactionSentinel, `"path":"/spec/dbPassword"`},
		check: func(t *testing.T, got []byte) {
			t.Helper()
			var reconstructed map[string]any
			if err := json.Unmarshal(got, &reconstructed); err != nil {
				t.Fatalf("decoding the reconstruction: %v", err)
			}
			spec, ok := reconstructed["spec"].(map[string]any)
			if !ok {
				t.Fatalf("the reconstruction has no spec: %s", got)
			}
			if spec["dbPassword"] != RedactionSentinel {
				t.Errorf("the reconstruction carries %v at spec.dbPassword, want the sentinel %q exactly; "+
					"a sentinel altered in transit reads as a recorded value rather than as a redaction",
					spec["dbPassword"], RedactionSentinel)
			}
		},
	}
}

// roundTrip runs one pair through the real producer and the real consumer and
// asserts the target document comes back. It returns the patch so a caller can
// assert over it, or nil if the round trip already failed.
//
// Equality is over canonical JSON — keys sorted, numbers kept as the literals
// they were written as — rather than over the raw bytes. The consumer preserves
// a document's own key order (partialDoc.keys), while the producer's inputs are
// json.Marshal output with keys sorted, so a patch that adds a key returns a
// correct document whose bytes differ in key order alone. Canonicalizing hides
// that and nothing else: whitespace and key order are the only differences it
// can absorb, and a number is compared as the literal text it arrived as, so a
// value that changed in any way the schema records is still a failure here.
func roundTrip(t *testing.T, context string, before, after []byte) (patch []byte, refused bool) {
	t.Helper()

	patch, err := ComputeDiff(before, after)
	switch {
	case errors.Is(err, errEmptyInteriorToken):
		// Not a failure: the producer refused to record a patch the consumer
		// cannot apply, and the write path records full state instead. There is no
		// patch to round-trip, and that is the correct answer for this pair.
		return nil, true
	case err != nil:
		t.Errorf("%s: the producer could not diff the pair: %v\n  before %s\n  after  %s",
			context, err, before, after)
		return nil, false
	}
	got, err := applyProducedPatch(before, patch)
	if err != nil {
		t.Errorf("%s: %v\n  before %s\n  after  %s\n  patch  %s", context, err, before, after, patch)
		return nil, false
	}
	if want := canonicalBytes(t, after); !bytes.Equal(canonicalBytes(t, got), want) {
		t.Errorf("%s: the reconstruction is not the document the patch was produced from.\n"+
			"  before        %s\n  after         %s\n  patch         %s\n"+
			"  reconstructed %s\n"+
			"This is a divergence between the two RFC 6902 implementations, not a test failure to "+
			"work around: the write path records patches the read path cannot apply faithfully, and "+
			"every reconstruction over such a patch is wrong in a way that looks right.",
			context, before, after, patch, canonicalBytes(t, got))
	}
	return patch, false
}

// applyProducedPatch is the consumer half, in the same order state.go runs it.
func applyProducedPatch(before, patch []byte) ([]byte, error) {
	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, fmt.Errorf("the consumer could not decode the produced patch: %w", err)
	}
	got, err := decoded.Apply(before)
	if err != nil {
		return nil, fmt.Errorf("the consumer could not apply the produced patch: %w", err)
	}
	return got, nil
}

// assertOrderIsLoadBearing applies the patch backwards and requires a different
// answer.
//
// Without it, "index ordering is load-bearing" would be an assertion about the
// author's intent. With it, the case proves that this particular sequence of
// operations is order-sensitive — so a future producer change that made the
// ordering incidental would surface here rather than leaving a case that reads
// like a guard and is not one.
func assertOrderIsLoadBearing(t *testing.T, before, after, patch []byte) {
	t.Helper()

	var ops []json.RawMessage
	if err := json.Unmarshal(patch, &ops); err != nil {
		t.Fatalf("decoding the produced patch as an operation list: %v", err)
	}
	if len(ops) < 2 {
		t.Fatalf("the produced patch has %d operation(s), so there is no ordering to be load-bearing:\n%s",
			len(ops), patch)
	}
	slices.Reverse(ops)
	reversed, err := json.Marshal(ops)
	if err != nil {
		t.Fatalf("re-marshalling the reversed operation list: %v", err)
	}

	got, err := applyProducedPatch(before, reversed)
	if err != nil {
		return // Refusing the reversed patch outright is order-sensitivity too.
	}
	if bytes.Equal(canonicalBytes(t, got), canonicalBytes(t, after)) {
		t.Errorf("applying the patch backwards produced the same document, so this case is not "+
			"exercising the index ordering it is named for:\n  patch    %s\n  reversed %s", patch, reversed)
	}
}

// TestProducerEmitsNoMoveOrCopyOps records, over every pair in this file, that
// the pipeline's options produce only add, remove and replace.
//
// The value is in what it makes visible. move and copy are the two operations
// with no round-trip coverage anywhere, because the producer cannot currently
// emit them; the day someone passes the factorize option, this test names the
// change rather than letting an untested operation shape reach the diff column
// in silence.
func TestProducerEmitsNoMoveOrCopyOps(t *testing.T) {
	pairs := make([]patchCase, 0, 32)
	pairs = append(pairs, namedPatchCases(t)...)
	pairs = append(pairs, generatedPatchCases(t)...)

	for _, pair := range pairs {
		patch, err := ComputeDiff(pair.before, pair.after)
		if errors.Is(err, errEmptyInteriorToken) {
			continue // No patch was produced, so there are no operations to inspect.
		}
		if err != nil {
			t.Fatalf("%s: ComputeDiff: %v", pair.name, err)
		}
		for _, op := range decodeOps(t, patch) {
			if op == "move" || op == "copy" {
				t.Errorf("%s: the producer emitted a %q operation, which no round-trip case covers. "+
					"Both are gated behind %s, so this means the pipeline's diff options changed: give "+
					"the operation its own named case in namedPatchCases before shipping the change.\n%s",
					pair.name, op, factorizeOption, patch)
			}
		}
	}
}

// decodeOps returns the `op` of every operation in a patch.
func decodeOps(t *testing.T, patch []byte) []string {
	t.Helper()

	var ops []struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(patch, &ops); err != nil {
		t.Fatalf("decoding the produced patch: %v\n%s", err, patch)
	}
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		out = append(out, op.Op)
	}
	return out
}

// emptyInteriorTokenPairs are the shapes the producer must decline, and the one
// it must not.
//
// They are spelled as pointers rather than as documents because the guard is a
// property of the pointer: what makes a patch unapplicable is where the empty
// token sits, not what the document around it looks like.
var emptyInteriorTokenPairs = []struct {
	pointer string
	refused bool
}{
	{"", false},
	{"/", false},
	{"/spec", false},
	{"/spec/", false},
	{"/spec/replicas", false},
	{"//k", true},
	{"//-", true},
	{"//0", true},
	{"/spec//k", true},
	{"/spec//", true},
	{"/a//b//c", true},
	{"/spec/app.kubernetes.io~1name", false},
}

// TestTheGuardMatchesOnlyAnEmptyInteriorToken pins where the line is drawn.
//
// The guard costs compression on every object it fires for, so drawing it one
// token too wide is a real cost paid for nothing — and drawing it one token too
// narrow puts back exactly the silent wrong reconstruction it exists to prevent.
func TestTheGuardMatchesOnlyAnEmptyInteriorToken(t *testing.T) {
	for _, tc := range emptyInteriorTokenPairs {
		if got := hasEmptyInteriorToken(tc.pointer); got != tc.refused {
			t.Errorf("hasEmptyInteriorToken(%q) = %v, want %v", tc.pointer, got, tc.refused)
		}
	}
}

// TestTheConsumerStillCannotApplyAnEmptyInteriorToken pins the upstream defect
// the guard exists for.
//
// It asserts a bug rather than a feature, deliberately. The guard in ComputeDiff
// costs compression, and the only honest justification for keeping it is that the
// consumer still gets these pointers wrong. When this test starts failing,
// github.com/evanphx/json-patch has fixed partialDoc.get and the guard can be
// removed — which is a far better signal than a comment nobody re-checks.
//
// The patches here are written out rather than produced, because the producer now
// refuses to emit them. That is the point: this is the shape that would have been
// recorded, and this is what the read plane would have done with it.
func TestTheConsumerStillCannotApplyAnEmptyInteriorToken(t *testing.T) {
	tests := []struct {
		name    string
		before  string
		patch   string
		silent  bool
		wantGot string
	}{{
		name:    "add walks into the wrong node and reports success",
		before:  `{"":{"k":"1"}}`,
		patch:   `[{"op":"add","path":"//n","value":"2"}]`,
		silent:  true,
		wantGot: `{"":{"k":"1"}}`,
	}, {
		name:    "append walks into the wrong node and reports success",
		before:  `{"":["a"]}`,
		patch:   `[{"op":"add","path":"//-","value":"b"}]`,
		silent:  true,
		wantGot: `{"":["a"]}`,
	}, {
		name:   "replace fails loudly",
		before: `{"":{"k":"1"}}`,
		patch:  `[{"op":"replace","path":"//k","value":"2"}]`,
	}, {
		name:   "remove fails loudly",
		before: `{"":{"k":"1","n":"2"}}`,
		patch:  `[{"op":"remove","path":"//n"}]`,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := applyProducedPatch([]byte(tc.before), []byte(tc.patch))
			if !tc.silent {
				if err == nil {
					t.Errorf("the consumer applied %s to %s without error, returning %s. If it is now "+
						"correct, the guard in ComputeDiff has outlived its reason and should be removed "+
						"along with this test", tc.patch, tc.before, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("the consumer now rejects %s rather than silently ignoring it (%v), which is a "+
					"change in upstream behaviour worth re-reading errEmptyInteriorToken over",
					tc.patch, err)
			}
			if string(got) != tc.wantGot {
				t.Errorf("the consumer applied %s to %s and returned %s, want %s unchanged. Upstream "+
					"behaviour has changed: re-check whether the guard in ComputeDiff is still needed",
					tc.patch, tc.before, got, tc.wantGot)
			}
		})
	}
}

// The generated half of the proof. The named corpus above covers the shapes
// somebody thought of; this covers the ones nobody did.
const (
	// patchSeedEnv overrides the corpus seed, for local exploration of corpora
	// this file does not ship with.
	patchSeedEnv = "KUBERECORD_PATCH_SEED"

	// defaultPatchSeed keeps the corpus deterministic, for the reason
	// redact_property_test.go spells out: a property test that fails on one pull
	// request in fifty, for reasons unrelated to that pull request, is deleted by
	// the third person it blocks.
	defaultPatchSeed = 0x70617463685f7631

	// patchPropertyCases is the corpus size. The pairs are small and the whole
	// file runs in well under a second, which is the budget that matters —
	// `make test` gates every pull request.
	patchPropertyCases = 200

	// patchMinDepth is the acceptance criterion's floor: every generated object
	// carries a forced chain at least this many containers deep, so the corpus
	// cannot degenerate into flat maps the day the branching is retuned.
	patchMinDepth = 4
	// patchMaxDepth bounds the random nesting layered on top of that chain.
	patchMaxDepth = 6
	// patchMaxFanout bounds a generated map's keys and an array's elements.
	patchMaxFanout = 4
	// patchMaxEdits bounds how many mutations separate a pair.
	patchMaxEdits = 5
)

// The floors below are what keep the generated corpus from passing vacuously. A
// corpus that happened to produce no escaped pointer, or no removal, would
// satisfy every assertion in this file while covering none of the shapes it
// exists for, and the failure mode of a property test is silence rather than
// noise.
const (
	patchEscapedSlashFloor = 20
	patchEscapedTildeFloor = 20
	patchRemoveFloor       = 20
	// Lower than the others because appends are the rarest of the shapes the
	// mutator produces (20 of 200 under the shipped seed); a floor set at the
	// observed count would fail on any retune rather than on a real loss of
	// coverage.
	patchAppendFloor   = 10
	patchAddFloor      = 20
	patchNonEmptyFloor = 130

	// The corpus must keep reaching the guard in ComputeDiff. An alphabet that
	// stopped generating the empty key would leave errEmptyInteriorToken pinned
	// only by the hand-written cases, which is the coverage this corpus exists to
	// go beyond.
	patchRefusalFloor = 1
)

// patchKeyNames is the key alphabet, and it is chosen for the pointer grammar
// rather than for realism.
//
// `/` and `~` are the two characters RFC 6901 escapes; `~1` in a key is the case
// that catches an implementation escaping in the wrong order; `-` is the token
// that means "append" inside an array and an ordinary key inside a map, so a
// consumer that decided by spelling rather than by container type gets it wrong;
// `""` is the empty reference token, which is legal and renders as a bare `/`.
// The multi-byte names are the case a byte-oriented pointer walker gets wrong,
// and the case an operator running a non-English cluster hits first.
var patchKeyNames = []string{
	"spec", "data", "items", "value", "name",
	"app.kubernetes.io/name",
	"kubectl.kubernetes.io/last-applied-configuration",
	"tilde~key", "already~1escaped", "both~and/slash",
	"-", "", "0", "1",
	"ключ", "配置", "🔑",
}

// patchStringValues carries multi-byte text, an empty string and the redaction
// sentinel, which is a value the pipeline really writes.
var patchStringValues = []string{
	"hunter2", "", "correct horse", "значення", "秘密の値", "🔒🔑",
	"égalité", "line\nbreak", `quote"inside`, "a/b~c", RedactionSentinel,
}

// patchSpineKey names the forced nesting chain. It is absent from patchKeyNames
// so a random draw can never overwrite the spine and shorten the object below
// the depth floor.
const patchSpineKey = "spine"

// patchSeed resolves the corpus seed, rejecting an unusable override rather than
// silently running a different corpus than the one that was asked for.
func patchSeed(t *testing.T) uint64 {
	t.Helper()
	raw, set := os.LookupEnv(patchSeedEnv)
	if !set {
		return defaultPatchSeed
	}
	seed, err := strconv.ParseUint(raw, 0, 64)
	if err != nil {
		t.Fatalf("%s=%q is not a uint64: %v", patchSeedEnv, raw, err)
	}
	return seed
}

// TestProducedPatchesApplyThroughTheConsumerOnGeneratedObjects is the same round
// trip over a generated corpus.
//
// A counterexample prints its seed and its index, and rerunning exactly that
// corpus is
//
//	KUBERECORD_PATCH_SEED=<seed> go test ./internal/pipeline/ -run <TestName>
//
// Per the acceptance criteria it is a finding and is reported as one. It is not
// resolved by moving the seed, narrowing the alphabet or changing the producer's
// options.
func TestProducedPatchesApplyThroughTheConsumerOnGeneratedObjects(t *testing.T) {
	seed := patchSeed(t)
	var escapedSlash, escapedTilde, removes, appends, adds, nonEmpty, refusals int

	for index, pair := range generatedPatchCases(t) {
		context := fmt.Sprintf("seed %#x case %d (%s)", seed, index, pair.name)
		patch, refused := roundTrip(t, context, pair.before, pair.after)
		if refused {
			refusals++
			continue
		}
		if patch == nil {
			continue
		}
		text := string(patch)
		if len(decodeOps(t, patch)) > 0 {
			nonEmpty++
		}
		if strings.Contains(text, "~1") {
			escapedSlash++
		}
		if strings.Contains(text, "~0") {
			escapedTilde++
		}
		if strings.Contains(text, `"op":"remove"`) {
			removes++
		}
		if strings.Contains(text, `"op":"add"`) {
			adds++
		}
		if strings.Contains(text, `/-"`) {
			appends++
		}
	}

	for _, floor := range []struct {
		what string
		got  int
		want int
	}{
		{"a patch at all", nonEmpty, patchNonEmptyFloor},
		{"a pointer with a ~1-escaped slash", escapedSlash, patchEscapedSlashFloor},
		{"a pointer with a ~0-escaped tilde", escapedTilde, patchEscapedTildeFloor},
		{"a remove operation", removes, patchRemoveFloor},
		{"an add operation", adds, patchAddFloor},
		{"an append addressed by the - token", appends, patchAppendFloor},
		{"a producer refusal over an empty interior token", refusals, patchRefusalFloor},
	} {
		if floor.got < floor.want {
			t.Errorf("only %d of %d generated cases produced %s, want at least %d: the corpus has "+
				"drifted away from the shapes this test exists to cover, so its silence no longer "+
				"means they hold",
				floor.got, patchPropertyCases, floor.what, floor.want)
		}
	}
}

// generatedPatchCases builds the corpus: a random object, and a copy of it with
// a handful of edits applied.
//
// Mutation rather than two independent objects, because a diff between unrelated
// documents is one enormous replace at the root and exercises no pointer at all.
// The interesting patches are the ones that reach into the structure, which is
// what a real update produces.
func generatedPatchCases(t *testing.T) []patchCase {
	t.Helper()

	seed := patchSeed(t)
	cases := make([]patchCase, 0, patchPropertyCases)
	for index := range patchPropertyCases {
		rng := patchCaseRNG(seed, index)
		before := genPatchObject(rng, patchMinDepth, patchMaxDepth)
		edits := 1 + rng.IntN(patchMaxEdits)
		after, _ := mutateForPatch(rng, deepCopyValue(before), &edits).(map[string]any)
		if after == nil {
			t.Fatalf("case %d: mutation returned a non-object root, which the pointer grammar cannot "+
				"address", index)
		}
		cases = append(cases, patchCase{
			name:   fmt.Sprintf("generated case %d", index),
			before: marshal(t, before),
			after:  marshal(t, after),
		})
	}
	return cases
}

// patchCaseRNG gives each case its own stream, so a counterexample at index N is
// reproducible without replaying the N-1 cases before it — which is what makes
// the printed seed worth printing.
func patchCaseRNG(seed uint64, index int) *rand.Rand {
	return rand.New(rand.NewPCG(seed, uint64(index)))
}

// genPatchKey draws a map key, weighted so a pointer-escaping key appears often
// enough to clear the floors above.
func genPatchKey(rng *rand.Rand) string {
	return patchKeyNames[rng.IntN(len(patchKeyNames))]
}

// genPatchScalar draws a leaf.
//
// The numeric domain stops short of integers beyond float64's exact range, and
// that is a stated gap rather than an oversight. jsondiff decodes both documents
// into `interface{}` with a plain json.Unmarshal, so every number in a produced
// patch is a float64 re-marshalled from that decoding: an integer larger than
// 2^53 that changed would be recorded in the diff column rounded, and a
// reconstruction would differ from the state that was recorded — while the data
// column, which is normalized JSON written directly, would still be exact.
// Kubernetes numerics sit far inside that range (quantities are strings), so the
// gap is latent rather than live. Naming it here makes widening the domain a
// decision somebody takes rather than something that happens.
func genPatchScalar(rng *rand.Rand) any {
	switch rng.IntN(8) {
	case 0:
		return nil
	case 1:
		return rng.IntN(2) == 0
	case 2:
		return int64(rng.IntN(2001) - 1000)
	case 3:
		// Eighths are exactly representable, so the literal is stable across
		// platforms and identical whether it arrives as an int64 or a float64.
		return float64(rng.IntN(2001)-1000) / 8
	default:
		return patchStringValues[rng.IntN(len(patchStringValues))]
	}
}

// genPatchValue draws any value, biased towards leaves so the random part of an
// object stays small; the guaranteed depth comes from the spine, not from luck.
func genPatchValue(rng *rand.Rand, maxDepth int) any {
	if maxDepth <= 0 {
		return genPatchScalar(rng)
	}
	switch rng.IntN(6) {
	case 0, 1:
		return genPatchObject(rng, 0, maxDepth)
	case 2, 3:
		return genPatchArray(rng, maxDepth)
	default:
		return genPatchScalar(rng)
	}
}

// genPatchArray may return an empty array, which is the case an append has to
// address as `/-` against nothing at all.
func genPatchArray(rng *rand.Rand, maxDepth int) []any {
	items := make([]any, rng.IntN(patchMaxFanout+1))
	for i := range items {
		items[i] = genPatchValue(rng, maxDepth-1)
	}
	return items
}

// genPatchObject builds one map. minDepth forces a chain of that many further
// containers beneath it, which is how every generated object reaches
// patchMinDepth rather than merely tending to.
func genPatchObject(rng *rand.Rand, minDepth, maxDepth int) map[string]any {
	object := make(map[string]any, patchMaxFanout+1)
	for range 1 + rng.IntN(patchMaxFanout) {
		object[genPatchKey(rng)] = genPatchValue(rng, maxDepth-1)
	}
	if minDepth > 0 {
		object[patchSpineKey] = genPatchSpine(rng, minDepth-1, maxDepth-1)
	}
	return object
}

// genPatchSpine returns the next link of the forced chain: a map, or an array of
// maps so the chain runs through an array index about as often as through plain
// map descent.
func genPatchSpine(rng *rand.Rand, minDepth, maxDepth int) any {
	if rng.IntN(3) != 0 {
		return genPatchObject(rng, minDepth, maxDepth)
	}
	items := make([]any, 1+rng.IntN(patchMaxFanout-1))
	for i := range items {
		items[i] = genPatchObject(rng, minDepth, maxDepth)
	}
	return items
}

// mutateForPatch applies up to *edits changes to a copy of a generated value.
//
// Keys are chosen from a sorted list rather than by ranging over the map,
// because Go randomizes map iteration and a corpus that depended on it would not
// be reproducible from the printed seed — which is the whole of what makes a
// counterexample actionable.
func mutateForPatch(rng *rand.Rand, value any, edits *int) any {
	if *edits <= 0 {
		return value
	}
	switch container := value.(type) {
	case map[string]any:
		return mutateMapForPatch(rng, container, edits)
	case []any:
		return mutateArrayForPatch(rng, container, edits)
	default:
		*edits--
		return genPatchScalar(rng)
	}
}

// mutateMapForPatch edits one map: a key removed, a key added, a value nulled,
// or a descent into a child.
func mutateMapForPatch(rng *rand.Rand, container map[string]any, edits *int) any {
	keys := slices.Sorted(maps.Keys(container))
	switch rng.IntN(6) {
	case 0:
		if len(keys) > 0 {
			delete(container, keys[rng.IntN(len(keys))])
			*edits--
		}
	case 1:
		container[genPatchKey(rng)] = genPatchValue(rng, 2)
		*edits--
	case 2:
		if len(keys) > 0 {
			// Nulling rather than removing, so the corpus carries both halves of
			// the distinction the named cases pin by hand.
			container[keys[rng.IntN(len(keys))]] = nil
			*edits--
		}
	default:
		if len(keys) > 0 {
			key := keys[rng.IntN(len(keys))]
			container[key] = mutateForPatch(rng, container[key], edits)
		}
	}
	return container
}

// mutateArrayForPatch edits one array: an append, an insert, a removal, a swap,
// or a descent into an element.
func mutateArrayForPatch(rng *rand.Rand, container []any, edits *int) any {
	switch rng.IntN(6) {
	case 0:
		*edits--
		return append(container, genPatchValue(rng, 2))
	case 1:
		if len(container) > 0 {
			at := rng.IntN(len(container))
			*edits--
			return slices.Insert(container, at, genPatchValue(rng, 2))
		}
	case 2:
		if len(container) > 0 {
			*edits--
			return slices.Delete(container, rng.IntN(len(container)), rng.IntN(len(container))+1)
		}
	case 3:
		if len(container) > 1 {
			i, j := rng.IntN(len(container)), rng.IntN(len(container))
			container[i], container[j] = container[j], container[i]
			*edits--
		}
	default:
		if len(container) > 0 {
			at := rng.IntN(len(container))
			container[at] = mutateForPatch(rng, container[at], edits)
		}
	}
	return container
}

// canonicalBytes re-encodes a document with its map keys sorted and its numbers
// kept as the literals they arrived as.
//
// json.Number rather than float64 is the load-bearing part. Decoding numbers
// into float64 and re-encoding them would make the comparison unable to see a
// value the producer had rounded, which is one of the divergences these specs
// exist to catch — the comparison would equalise it on both sides and report
// agreement.
func canonicalBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decoding %s: %v", data, err)
	}
	var out bytes.Buffer
	writeCanonical(t, &out, value)
	return out.Bytes()
}

// writeCanonical is canonicalBytes' recursive half.
func writeCanonical(t *testing.T, out *bytes.Buffer, value any) {
	t.Helper()

	switch typed := value.(type) {
	case map[string]any:
		out.WriteByte('{')
		for i, key := range slices.Sorted(maps.Keys(typed)) {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCanonical(t, out, key)
			out.WriteByte(':')
			writeCanonical(t, out, typed[key])
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for i, item := range typed {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCanonical(t, out, item)
		}
		out.WriteByte(']')
	case json.Number:
		out.WriteString(typed.String())
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			t.Fatalf("marshalling %v: %v", typed, err)
		}
		out.Write(encoded)
	}
}

// marshal encodes a value the way the write path encodes normalized JSON: plain
// json.Marshal, so map keys are sorted and numbers are rendered by the same
// encoder the pipeline uses.
//
// Every document in this file is built this way rather than written out as a JSON
// literal, and that is deliberate: a hand-written literal could carry a number
// spelling (`1.50`, `1e2`) that json.Marshal never emits, and the resulting
// mismatch would be an artefact of the fixture rather than a divergence between
// the two libraries.
func marshal(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshalling the fixture: %v", err)
	}
	return encoded
}

// object wraps a fragment in the smallest Kubernetes shell that carries an
// identity, so the named cases read as changes to an object rather than as
// changes to a bag of keys.
func object(t *testing.T, fragment map[string]any) []byte {
	t.Helper()

	metadata := map[string]any{"name": "checkout", "namespace": "payments", "uid": "uid-a"}
	shell := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata}
	for key, value := range fragment {
		if key != "metadata" {
			shell[key] = value
			continue
		}
		overlay, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("the fragment's metadata is %T, want a map", value)
		}
		maps.Copy(metadata, overlay)
	}
	return marshal(t, shell)
}

// annotated is a fragment carrying annotations, which is where a Kubernetes
// object's `/`-containing keys actually live.
func annotated(annotations map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"annotations": annotations}}
}

// withArgs is a fragment carrying one array, for the insert, remove, reorder and
// append cases.
func withArgs(args ...string) map[string]any {
	items := make([]any, 0, len(args))
	for _, arg := range args {
		items = append(items, arg)
	}
	return map[string]any{"spec": map[string]any{"args": items}}
}

// withMatrix is a fragment carrying an array of arrays, so one inner array can
// shrink while another grows within a single deterministic traversal order.
func withMatrix(rows ...[]any) map[string]any {
	items := make([]any, 0, len(rows))
	for _, row := range rows {
		items = append(items, row)
	}
	return map[string]any{"spec": map[string]any{"matrix": items}}
}

// loadTestdataObject reads a fixture and normalizes it the way the write path
// does — decode, then re-encode — so the bytes under test are the bytes a row's
// data column would really hold rather than the file's own formatting.
func loadTestdataObject(t *testing.T, name string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", filepath.Clean(name)))
	if err != nil {
		t.Fatalf("reading testdata/%s: %v", name, err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decoding testdata/%s: %v", name, err)
	}
	return object
}

// deepCopyObject copies a decoded object so a case can edit one side of a pair
// without touching the other.
func deepCopyObject(t *testing.T, object map[string]any) map[string]any {
	t.Helper()

	copied, ok := deepCopyValue(object).(map[string]any)
	if !ok {
		t.Fatalf("the deep copy of an object is %T, want a map", deepCopyValue(object))
	}
	return copied
}

// normalized runs an object through the write path's own normalization and
// redaction, so the redaction case's documents are produced the way production
// produces them rather than by writing the sentinel out by hand.
func normalized(t *testing.T, object map[string]any, policy *RedactionPolicy) []byte {
	t.Helper()

	encoded, err := NormalizedJSON(&unstructured.Unstructured{Object: deepCopyObject(t, object)}, policy)
	if err != nil {
		t.Fatalf("NormalizedJSON: %v", err)
	}
	return encoded
}
