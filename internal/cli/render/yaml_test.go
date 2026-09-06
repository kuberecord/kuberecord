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

package render_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The key order of every structured document, pinned per kind.
//
// # Why this file exists rather than only the commands' golden files
//
// The commands' golden files under internal/cli/testdata pin whole pages, and
// they would have caught this defect if anything had ever looked at the first
// four lines of one. Nothing did, for two phases, because a sorted document is
// not wrong in any way a reader of a diff notices — it is only wrong to the
// person opening the file expecting a Kubernetes document.
//
// So the assertion lives next to the encoder and states the claim in words:
// apiVersion, kind, metadata, items, in that order, for every kind. A library
// that changed its mind about sorting fails here, at the one place that could
// have caused it, rather than in five end-to-end fixtures whose diffs would be
// read as churn.
//
// The JSON goldens beside them are the other half of the criterion, and they are
// goldens rather than an argument: encoding/json emits struct fields in
// declaration order and always has, but "JSON is unaffected" is a claim about
// output, and the way to know a claim about output is true is to have written the
// output down.

// wantEnvelopeKeys is the order every envelope must open in.
//
// It is the order of every Kubernetes document a reader has met, which is the
// entire justification: identity first, then provenance, then the payload. A
// document that opens `apiVersion, items` reads as malformed even when it is not,
// because the two fields that say what it is have been pushed below a
// several-hundred-line array.
var wantEnvelopeKeys = []string{"apiVersion", "kind", "metadata", "items"}

// topLevelKey matches a key at the left margin, which is what a top-level key is
// in a block-style YAML document.
//
// Scanning the text rather than parsing it is deliberate. A parse would go
// through the same library the encoder uses, so a library that sorted would be
// asked to report on its own sorting; and the property under test is what a
// person sees when they open the file, which is text.
var topLevelKey = regexp.MustCompile(`(?m)^([A-Za-z][A-Za-z0-9_.]*):`)

// envelopeFixture is one kind's document, built the way its command builds it.
type envelopeFixture struct {
	// name is the golden file's basename, which is the kind: one file per kind is
	// what makes a re-sort show up as a diff against the kind it broke.
	name  string
	head  render.EnvelopeHead
	items []any
}

// envelopeFixtures is one envelope per kind the CLI produces.
//
// Every kind is here, including Blame. The acceptance criteria enumerate five and
// Blame is not among them, but it travels through the same Envelope as the rest,
// so it was fixed by the same change — and a kind covered by the fix and not by
// the pin is the kind a later refactor breaks silently.
//
// The items are small but not empty. An envelope with no items pins the head and
// nothing else, and the head is not where the surprising ordering was: `object`
// is the last key of an Object item now and was the fourth of seven before, which
// is the difference between a reconstructed state a reader finds at the end of
// the document and one they have to hunt for among the provenance.
func envelopeFixtures(t *testing.T) []envelopeFixture {
	t.Helper()

	head := func(kind string) render.EnvelopeHead {
		return render.EnvelopeHead{
			APIVersion: render.EnvelopeAPIVersion,
			Kind:       kind,
			Metadata: render.EnvelopeMetadata{
				ClusterID: "prod-eu-1",
				Backend:   "clickhouse",
				Coverage: render.CoverageReport{
					Available: true,
					Summary:   "2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)",
					Intervals: []query.ScopeInterval{{
						APIGroup: "apps",
						Kind:     "Deployment",
						RuleRef:  "ClusterStreamRule/all-workloads",
						From:     mustInstant("2026-07-02T09:14:00Z"),
					}},
				},
			},
		}
	}

	change, err := render.NewChangeItem(query.Change{
		TS:              mustInstant("2026-08-28T14:03:11.482Z"),
		EventType:       query.EventModified,
		Actors:          []string{"kubectl-client-side-apply"},
		UID:             "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		ResourceVersion: "1002",
		APIVersion:      "apps/v1",
		Diff:            `[{"op":"replace","path":"/spec/replicas","value":5}]`,
	})
	if err != nil {
		t.Fatalf("NewChangeItem: %v", err)
	}

	// An Object envelope carries the reconstruction marker, which the command
	// stamps rather than the caller. Setting it here keeps the fixture the shape
	// WriteObject actually produces (see render.ReconstructionOf).
	objectHead := head(render.KindObject)
	reconstruction := render.ReconstructionOf(render.ObjectDocument{
		At:        mustInstant("2026-08-28T15:00:00Z"),
		BaseTS:    mustInstant("2026-08-28T14:05:02.117Z"),
		BaseEvent: query.EventCheckpoint,
	})
	objectHead.Metadata.Reconstruction = &reconstruction

	return []envelopeFixture{
		{name: render.KindTimeline, head: head(render.KindTimeline), items: []any{change}},
		{
			name: render.KindDiff,
			head: head(render.KindDiff),
			items: []any{render.DiffItem{
				ChangeItem: change,
				Hunks: []render.Hunk{{
					Op:       "replace",
					Path:     "spec.replicas",
					Pointer:  "/spec/replicas",
					Old:      float64(3),
					OldKnown: true,
					New:      []byte("5"),
				}},
			}},
		},
		{
			name: render.KindObject,
			head: objectHead,
			items: []any{render.ObjectItem{
				At:             mustInstant("2026-08-28T15:00:00Z"),
				UID:            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
				BaseTS:         mustInstant("2026-08-28T14:05:02.117Z"),
				BaseEvent:      query.EventCheckpoint,
				PatchesApplied: 2,
				SHA256:         "283f5a59",
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata":   map[string]any{"name": "checkout", "namespace": "payments"},
				},
			}},
		},
		{
			name:  render.KindCoverage,
			head:  head(render.KindCoverage),
			items: []any{head(render.KindCoverage).Metadata.Coverage.Intervals[0]},
		},
		{
			name: render.KindBlame,
			head: head(render.KindBlame),
			items: []any{render.BlameItem{
				Path:            "spec.replicas",
				Pointer:         "/spec/replicas",
				Attributed:      true,
				TS:              instantPointer("2026-08-28T14:03:11.482Z"),
				Actors:          []string{"kubectl-client-side-apply"},
				UID:             "7c9e6679-7425-40de-944b-e07fc1f90ae7",
				ResourceVersion: "1002",
				EventType:       query.EventModified,
				Fields:          1,
			}},
		},
	}
}

// instantPointer is mustInstant for the one field that carries a pointer.
func instantPointer(clock string) *time.Time {
	parsed := mustInstant(clock)
	return &parsed
}

// TestYAMLOpensLikeAKubernetesDocument is the criterion, stated as a claim rather
// than as a diff.
//
// A golden file alone would pin the order and say nothing about what the order
// is, so a regenerated golden would carry a re-sort along with whatever change
// prompted the regeneration. This names the four keys.
func TestYAMLOpensLikeAKubernetesDocument(t *testing.T) {
	for _, fixture := range envelopeFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			document := renderEnvelope(t, fixture, render.StructuredYAML)

			if got := topLevelKeys(document); !slicesEqual(got, wantEnvelopeKeys) {
				t.Errorf("a %s document opens %v, want %v — a Kubernetes document says what it "+
					"is before it says what is in it, and `kind` below a long `items` array reads "+
					"as malformed even when it is not\n%s", fixture.name, got, wantEnvelopeKeys, document)
			}
			assertRenderGolden(t, "envelope", fixture.name+".yaml", document)
		})
	}
}

// TestJSONKeyOrderIsUnchanged is the other half of the criterion.
//
// encoding/json emits struct fields in declaration order, so this format was
// never affected — but that is an assumption about a library, and the criterion
// asks for it to be verified rather than assumed. The golden is the verification;
// the key-order assertion is what makes the golden readable as a claim.
func TestJSONKeyOrderIsUnchanged(t *testing.T) {
	for _, fixture := range envelopeFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			document := renderEnvelope(t, fixture, render.StructuredJSON)

			// The same four keys, at the same two-space margin `json.MarshalIndent`
			// puts them at.
			var keys []string
			for line := range strings.SplitSeq(document, "\n") {
				if strings.HasPrefix(line, `  "`) {
					keys = append(keys, strings.Split(line, `"`)[1])
				}
			}
			if !slicesEqual(keys, wantEnvelopeKeys) {
				t.Errorf("a %s JSON document opens %v, want %v\n%s",
					fixture.name, keys, wantEnvelopeKeys, document)
			}
			assertRenderGolden(t, "envelope", fixture.name+".json", document)
		})
	}
}

// TestYAMLChangesNothingButKeyOrder is the safety half of the change.
//
// Key order is not a semantic property, and the whole argument for making this
// change was that nothing else moves with it. That is a claim about two
// encoders, so it is asserted against both: the document is rendered the way it
// is rendered now and the way sigs.k8s.io/yaml would have rendered it, and the
// two are normalized through one parser and compared byte for byte.
//
// The inequality check is not decoration. Deep equality alone would pass just as
// happily against an encoder that had quietly gone back to sorting, which is the
// state this whole task exists to leave behind.
func TestYAMLChangesNothingButKeyOrder(t *testing.T) {
	for _, fixture := range envelopeFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			ordered := renderEnvelope(t, fixture, render.StructuredYAML)

			// The old path, called here and nowhere else: sigs.k8s.io/yaml.Marshal
			// is json.Marshal followed by a parse into a Go map, and the map is
			// what sorted the keys.
			sorted, err := yaml.Marshal(render.Envelope{EnvelopeHead: fixture.head, Items: fixture.items})
			if err != nil {
				t.Fatalf("marshalling a %s envelope the old way: %v", fixture.name, err)
			}

			if ordered == string(sorted) {
				t.Fatalf("the %s document is byte-identical to the sorted one, so either the "+
					"encoder is sorting again or this test is measuring nothing:\n%s",
					fixture.name, ordered)
			}
			if got, want := canonical(t, ordered), canonical(t, string(sorted)); !bytes.Equal(got, want) {
				t.Errorf("re-ordering the %s document changed what it means.\n--- was ---\n%s\n"+
					"--- is ---\n%s", fixture.name, want, got)
			}
		})
	}
}

// TestYAMLAndJSONAreOneDocument keeps the promise the two formats make together.
//
// They are one document in two syntaxes, and the syntax a reader picked must
// never decide what they are told. Both are normalized through the same parser,
// so any disagreement here is a disagreement about content rather than about how
// either format spells a scalar.
func TestYAMLAndJSONAreOneDocument(t *testing.T) {
	for _, fixture := range envelopeFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			asYAML := canonical(t, renderEnvelope(t, fixture, render.StructuredYAML))
			asJSON := canonical(t, renderEnvelope(t, fixture, render.StructuredJSON))
			if !bytes.Equal(asYAML, asJSON) {
				t.Errorf("the two serializations of a %s are different documents.\n"+
					"--- yaml ---\n%s\n--- json ---\n%s", fixture.name, asYAML, asJSON)
			}
		})
	}
}

// TestTheReconstructedStateIsTheLastThingInAnObjectItem is the half of the
// criterion a reader feels rather than parses.
//
// Sorted, an Object item read `at, base_event, base_ts, object, patches_applied,
// sha256, uid`: the reconstruction — the reason anybody ran the command — sat
// fourth, with three provenance fields after it, and on a real object it buried
// them under several hundred lines. In declaration order the six facts that let a
// reader judge the reconstruction come first and the reconstruction itself is
// last, where a document ends.
func TestTheReconstructedStateIsTheLastThingInAnObjectItem(t *testing.T) {
	for _, fixture := range envelopeFixtures(t) {
		if fixture.name != render.KindObject {
			continue
		}
		document := renderEnvelope(t, fixture, render.StructuredYAML)

		// The item's keys are the ones indented two spaces under the `items`
		// sequence; `object:` is the last of them, and everything below it belongs
		// to the state itself.
		var keys []string
		for line := range strings.SplitSeq(document, "\n") {
			trimmed := strings.TrimPrefix(line, "- ")
			if len(line)-len(strings.TrimLeft(line, " ")) != 2 && !strings.HasPrefix(line, "- ") {
				continue
			}
			if key, _, found := strings.Cut(strings.TrimLeft(trimmed, " "), ":"); found {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 || keys[len(keys)-1] != "object" {
			t.Errorf("an Object item's keys are %v; the reconstructed state must be the last of "+
				"them, or the provenance a reader judges it by sits below it\n%s", keys, document)
		}
	}
}

// renderEnvelope writes one fixture through the Stream every command writes
// through, so what is pinned is what a command emits rather than what a direct
// call to the encoder would.
func renderEnvelope(t *testing.T, fixture envelopeFixture, format render.StructuredFormat) string {
	t.Helper()

	var out bytes.Buffer
	stream, err := render.NewStream(&out, format, fixture.head)
	if err != nil {
		t.Fatalf("NewStream(%s): %v", format, err)
	}
	for _, item := range fixture.items {
		if err := stream.Write(item); err != nil {
			t.Fatalf("writing a %s item: %v", fixture.name, err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("closing the %s envelope: %v", fixture.name, err)
	}
	return out.String()
}

// canonical reduces a document to a form in which only its content survives.
//
// YAMLToJSON parses with the YAML parser and re-encodes through a Go map, so
// every key is sorted and every scalar is spelled one way. Both syntaxes go
// through it — JSON is valid YAML — so the two sides of every comparison here are
// handled by identical code, and a difference cannot be an artifact of having
// normalized them differently.
func canonical(t *testing.T, document string) []byte {
	t.Helper()

	normalized, err := yaml.YAMLToJSON([]byte(document))
	if err != nil {
		t.Fatalf("the document does not parse as YAML, so nothing else about it can be "+
			"asserted: %v\n%s", err, document)
	}
	return normalized
}

// topLevelKeys reads the keys at the left margin, in the order they appear.
func topLevelKeys(document string) []string {
	matches := topLevelKey.FindAllStringSubmatch(document, -1)
	keys := make([]string, 0, len(matches))
	for _, match := range matches {
		keys = append(keys, match[1])
	}
	return keys
}

// slicesEqual compares two key sequences.
//
// slices.Equal would do, and this is here because the failure it guards is order
// rather than membership: naming the comparison at the call site is what makes
// the assertion above read as "in that order" rather than as "these four".
func slicesEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// assertRenderGolden compares a rendering against its checked-in file.
//
// It is the one -update path in this package, shared with the severity goldens,
// for the reason internal/cli's own harness gives for sharing its own: two copies
// are two places for the write path to drift from the compare path, and a golden
// test whose halves disagree is a test that passes after rewriting the thing it
// was meant to pin.
func assertRenderGolden(t *testing.T, dir, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", dir, name+".golden")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/render/ -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}
