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
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The header that stops somebody deploying a reconstruction.
//
// Every fact the acceptance criteria list is asserted by name below, and so is
// the sentence that matters most. Someone will try to `kubectl apply -f` this
// document, and the header is the only thing between them and a Deployment whose
// password field is the literal string "[REDACTED]".

// reconstructedDocument is the fixture every test in this file renders.
func reconstructedDocument() render.ObjectDocument {
	return render.ObjectDocument{
		Kind:           "apps/Deployment",
		Ref:            "payments/checkout",
		Cluster:        "prod-eu-1",
		UID:            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		At:             mustInstant("2026-08-28T14:04:00Z"),
		BaseTS:         mustInstant("2026-08-28T14:02:58Z"),
		BaseEvent:      query.EventAdded,
		PatchesApplied: 2,
		State: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "checkout", "namespace": "payments"},
			"spec":       map[string]any{"replicas": float64(5)},
		},
	}
}

// reconstructedHead is the envelope head the document is written inside.
//
// The provenance a reader judges the reconstruction by travels on the item; what
// is here is the provenance of the *answer* — which cluster, which engine, and
// what the watch scopes said about the period being reconstructed.
//
// It deliberately carries no Reconstruction marker. WriteObject stamps that from
// the document itself, and a fixture that pre-set it would be asserting its own
// value back rather than the writer's.
func reconstructedHead() render.EnvelopeHead {
	return render.EnvelopeHead{
		APIVersion: render.EnvelopeAPIVersion,
		Kind:       render.KindObject,
		Metadata: render.EnvelopeMetadata{
			ClusterID: "prod-eu-1",
			Backend:   "clickhouse",
			Coverage: render.CoverageReport{
				Available: true,
				Summary:   "2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)",
				Intervals: []query.ScopeInterval{},
			},
		},
	}
}

// mustInstant parses a fixture timestamp.
func mustInstant(clock string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, clock)
	if err != nil {
		panic(err)
	}
	return parsed
}

// TestObjectYAMLCarriesTheMandatoryHeader asserts each fact the criteria name.
//
// By substring rather than by golden file, because a golden file regenerated
// after somebody trimmed the header would still pass. These will not.
func TestObjectYAMLCarriesTheMandatoryHeader(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := render.WriteObject(&out, &errOut, reconstructedDocument(), reconstructedHead(),
		render.StructuredYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	got := out.String()

	for _, want := range []struct{ fact, text string }{
		{"that it is reconstructed", "# Reconstructed state"},
		{"that it must not be deployed", "NOT A DEPLOYABLE MANIFEST"},
		{"the object", "# object:          apps/Deployment payments/checkout"},
		{"the cluster", "# cluster:         prod-eu-1"},
		{"the uid", "# uid:             7c9e6679-7425-40de-944b-e07fc1f90ae7"},
		{"the instant", "# at:              2026-08-28T14:04:00Z"},
		{"the base row", "# base row:        2026-08-28T14:02:58Z (Added)"},
		{"how many patches were applied", "# patches applied: 2"},
		{"why applying it is wrong", "`kubectl apply -f`"},
		{"what was stripped at capture", "metadata.managedFields, metadata.resourceVersion and"},
		{"that generation went too", "metadata.generation were stripped at capture"},
		{"that redacted fields carry a sentinel", "carries the sentinel " + render.RedactionSentinel},
	} {
		if !strings.Contains(got, want.text) {
			t.Errorf("the header does not state %s.\nwant it to contain %q\ngot:\n%s",
				want.fact, want.text, got)
		}
	}

	// The header must be a header: a comment block that appears after the
	// document is a comment block a reader has already scrolled past.
	if !strings.HasPrefix(got, "# Reconstructed state") {
		t.Errorf("the header is not the first thing in the document:\n%s", got)
	}
}

// TestObjectYAMLRemainsParseable is the other half of the header being comments.
//
// A header that broke the document would have people strip it, which is exactly
// the outcome it exists to prevent.
func TestObjectYAMLRemainsParseable(t *testing.T) {
	var out, errOut bytes.Buffer
	doc := reconstructedDocument()
	if err := render.WriteObject(
		&out, &errOut, doc, reconstructedHead(), render.StructuredYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	var decoded map[string]any
	if err := yaml.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("the rendered document is not valid YAML: %v\n%s", err, out.String())
	}
	spec, ok := reconstructedState(t, decoded)["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the decoded document has no spec: %#v", decoded)
	}
	if spec["replicas"] != float64(5) {
		t.Errorf("the document did not survive the header: spec.replicas is %#v, want 5", spec["replicas"])
	}
}

// reconstructedState digs the object out of a decoded envelope.
//
// It asserts the envelope on the way past, because every one of those fields is
// part of the contract a script binds to: an apiVersion it branches on, a kind
// that says what the items are, and an items list rather than a bare document.
func reconstructedState(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()

	if decoded["apiVersion"] != render.EnvelopeAPIVersion {
		t.Fatalf("apiVersion is %#v, want %q", decoded["apiVersion"], render.EnvelopeAPIVersion)
	}
	if decoded["kind"] != render.KindObject {
		t.Fatalf("kind is %#v, want %q", decoded["kind"], render.KindObject)
	}
	items, ok := decoded["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("a reconstruction is one item, and this envelope holds %#v", decoded["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("the item is not an object: %#v", items[0])
	}
	state, ok := item["object"].(map[string]any)
	if !ok {
		t.Fatalf("the item carries no reconstructed object: %#v", item)
	}
	return state
}

// TestObjectJSONPutsTheHeaderOnStandardError covers the format with no comment
// syntax.
//
// The warning still has to reach whoever ran the command, and stdout still has to
// be something `jq` can read — so the identical block goes to the stream every
// other qualification in this CLI goes to. An invented field would have been the
// other option, and it would change the document a --verify hashes.
func TestObjectJSONPutsTheHeaderOnStandardError(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := render.WriteObject(&out, &errOut, reconstructedDocument(), reconstructedHead(),
		render.StructuredJSON, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	if !strings.Contains(errOut.String(), "NOT A DEPLOYABLE MANIFEST") {
		t.Errorf("the warning never reached stderr:\n%s", errOut.String())
	}
	if strings.Contains(out.String(), "NOT A DEPLOYABLE MANIFEST") {
		t.Errorf("the header reached stdout, where it would corrupt a pipe:\n%s", out.String())
	}

	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("the rendered document is not valid JSON: %v\n%s", err, out.String())
	}
	if state := reconstructedState(t, decoded); state["kind"] != "Deployment" {
		t.Errorf("the decoded document is not the object: %#v", state)
	}
}

// TestObjectReportsAbsentProvenanceAsAbsent keeps a blank from reading as a
// value.
//
// A UID the history did not carry is a real state; a header line reading "uid:"
// with nothing after it says the object's UID was the empty string.
func TestObjectReportsAbsentProvenanceAsAbsent(t *testing.T) {
	doc := reconstructedDocument()
	doc.UID = ""
	doc.BaseEvent = ""

	var out, errOut bytes.Buffer
	if err := render.WriteObject(
		&out, &errOut, doc, reconstructedHead(), render.StructuredYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	for _, want := range []string{"# uid:             not recorded", "(not recorded)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("an absent fact is rendered as a blank.\nwant %q\ngot:\n%s", want, out.String())
		}
	}
}

// The machine-readable half of the same warning.
//
// The header above solves the problem for a person. It does not solve it for a
// script: for JSON and JSONL it travels on standard error, which `2>/dev/null`
// discards and which a pipe never reads at all, so
// `get … -o json | jq '.items[0].object'` receives a reconstruction with nothing
// in its input saying so. metadata.reconstruction is that fact as fields, on
// stdout, in every format — and the tests below are what keep it there.

// markerOf digs metadata.reconstruction out of a decoded envelope.
//
// It fails rather than returning an absence, because every caller below is
// asserting that the marker is present: a helper that quietly returned an empty
// map would turn "the marker is gone" into a run of confusing field mismatches.
func markerOf(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()

	metadata, ok := decoded["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("the envelope carries no metadata: %#v", decoded)
	}
	marker, ok := metadata["reconstruction"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.reconstruction is absent or not an object, so a consumer reading only "+
			"stdout cannot tell this document from a manifest: %#v", metadata)
	}
	return marker
}

// assertMarkerDescribesTheFixture checks the marker against reconstructedDocument.
//
// Every field the acceptance criteria name is asserted by its wire spelling, so a
// rename shows up here as the diff a consumer's `jq` would have hit.
func assertMarkerDescribesTheFixture(t *testing.T, marker map[string]any) {
	t.Helper()

	for _, want := range []struct {
		field string
		value any
	}{
		{"reconstructed", true},
		{"not_deployable", true},
		{"at", "2026-08-28T14:04:00Z"},
		{"base_ts", "2026-08-28T14:02:58Z"},
		{"base_event", query.EventAdded},
		{"patches_applied", float64(2)},
	} {
		if got := marker[want.field]; got != want.value {
			t.Errorf("metadata.reconstruction.%s is %#v, want %#v", want.field, got, want.value)
		}
	}
}

// TestObjectRoutesTheHeaderAndStillMarksTheDocument is the routing criterion for
// the two formats with no comment syntax.
//
// Three things have to hold at once, and each one breaks a different reader if it
// does not: the header reaches whoever ran the command, stdout is something `jq`
// can read with nothing prepended to it, and the document itself says it is a
// reconstruction.
func TestObjectRoutesTheHeaderAndStillMarksTheDocument(t *testing.T) {
	for _, test := range []struct {
		name   string
		format render.StructuredFormat
		// head extracts the envelope head from stdout, which is the whole
		// document for json and the first line for jsonl.
		head func(t *testing.T, stdout string) map[string]any
	}{
		{
			name:   "json",
			format: render.StructuredJSON,
			head: func(t *testing.T, stdout string) map[string]any {
				t.Helper()

				if !strings.HasPrefix(stdout, "{") {
					t.Fatalf("something was prepended to the JSON document:\n%s", stdout)
				}
				var decoded map[string]any
				if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
					t.Fatalf("stdout is not valid JSON, so a script cannot read it: %v\n%s", err, stdout)
				}
				return decoded
			},
		},
		{
			name:   "jsonl",
			format: render.StructuredJSONL,
			head: func(t *testing.T, stdout string) map[string]any {
				t.Helper()

				lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
				if len(lines) != 2 {
					t.Fatalf("a reconstruction is a head line and one item line, and stdout holds "+
						"%d lines:\n%s", len(lines), stdout)
				}
				decoded := make([]map[string]any, len(lines))
				for i, line := range lines {
					if err := json.Unmarshal([]byte(line), &decoded[i]); err != nil {
						t.Fatalf("line %d is not valid JSON, so a line-at-a-time consumer fails on "+
							"it: %v\n%s", i+1, err, line)
					}
				}
				if _, present := decoded[1]["object"]; !present {
					t.Errorf("the second line is not the item: %#v", decoded[1])
				}
				return decoded[0]
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doc := reconstructedDocument()

			var out, errOut bytes.Buffer
			if err := render.WriteObject(
				&out, &errOut, doc, reconstructedHead(), test.format, render.Options{}); err != nil {
				t.Fatalf("WriteObject: %v", err)
			}

			// The provenance block reaches standard error whole, rather than as a
			// summary of itself: it is the same bytes YAML carries as comments.
			if errOut.String() != render.ObjectProvenance(doc) {
				t.Errorf("stderr is not the provenance block.\n--- want ---\n%s\n--- got ---\n%s",
					render.ObjectProvenance(doc), errOut.String())
			}
			if strings.Contains(out.String(), "NOT A DEPLOYABLE MANIFEST") {
				t.Errorf("the header reached stdout, where it would corrupt a pipe:\n%s", out.String())
			}
			assertMarkerDescribesTheFixture(t, markerOf(t, test.head(t, out.String())))
		})
	}
}

// TestReconstructionIsIdentifiableFromStdoutAlone is the property this marker
// exists to create.
//
// `kuberecord get … -o json 2>/dev/null | jq …` is not a misuse; it is how a
// script is written by somebody who does not want diagnostics in their log. With
// the warning only on standard error, that pipeline receives a document that
// looks exactly like a manifest and says nothing about being one. So every format
// is written here with standard error thrown away, and every one of them must
// still answer the question from stdout.
func TestReconstructionIsIdentifiableFromStdoutAlone(t *testing.T) {
	for _, test := range []struct {
		name   string
		format render.StructuredFormat
		decode func(t *testing.T, stdout string) map[string]any
	}{
		{"json", render.StructuredJSON, decodeJSONDocument},
		{"jsonl", render.StructuredJSONL, decodeJSONLHead},
		{"yaml", render.StructuredYAML, decodeYAMLDocument},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := render.WriteObject(&out, io.Discard, reconstructedDocument(),
				reconstructedHead(), test.format, render.Options{}); err != nil {
				t.Fatalf("WriteObject: %v", err)
			}

			marker := markerOf(t, test.decode(t, out.String()))
			if marker["reconstructed"] != true {
				t.Errorf("with stderr discarded, nothing on stdout says this document was "+
					"assembled rather than recorded: %#v", marker)
			}
			if marker["not_deployable"] != true {
				t.Errorf("with stderr discarded, nothing on stdout says this document must not be "+
					"applied: %#v", marker)
			}
			assertMarkerDescribesTheFixture(t, marker)
		})
	}
}

// decodeJSONDocument parses a whole JSON envelope.
func decodeJSONDocument(t *testing.T, stdout string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, stdout)
	}
	return decoded
}

// decodeJSONLHead parses the first line of a jsonl stream, which is the head.
func decodeJSONLHead(t *testing.T, stdout string) map[string]any {
	t.Helper()

	head, _, found := strings.Cut(stdout, "\n")
	if !found {
		t.Fatalf("stdout carries no complete line:\n%s", stdout)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(head), &decoded); err != nil {
		t.Fatalf("the head line is not valid JSON: %v\n%s", err, head)
	}
	return decoded
}

// decodeYAMLDocument parses a whole YAML envelope, comment header and all.
func decodeYAMLDocument(t *testing.T, stdout string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := yaml.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("stdout is not valid YAML: %v\n%s", err, stdout)
	}
	return decoded
}

// TestObjectYAMLKeepsItsHeaderAboveTheEnvelope is the unchanged-path criterion.
//
// YAML gains the marker like every other format, and loses nothing for it: the
// comment block is still the first thing in the document, still byte-for-byte
// what ObjectProvenance renders, and still followed immediately by the envelope.
// A reader gets both halves; a parser gets one.
func TestObjectYAMLKeepsItsHeaderAboveTheEnvelope(t *testing.T) {
	doc := reconstructedDocument()

	var out, errOut bytes.Buffer
	if err := render.WriteObject(
		&out, &errOut, doc, reconstructedHead(), render.StructuredYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	header := render.ObjectProvenance(doc)
	body, found := strings.CutPrefix(out.String(), header)
	if !found {
		t.Fatalf("the document does not open with the provenance block.\n--- want prefix ---\n%s"+
			"\n--- got ---\n%s", header, out.String())
	}
	if !strings.HasPrefix(body, "apiVersion:") {
		t.Errorf("the envelope does not follow the header immediately:\n%s", body)
	}
	if errOut.Len() != 0 {
		t.Errorf("YAML wrote to stderr as well, so the header would be printed twice:\n%s",
			errOut.String())
	}
	assertMarkerDescribesTheFixture(t, markerOf(t, decodeYAMLDocument(t, out.String())))
}

// TestReconstructionMarkerCannotDriftFromTheHeader keeps one warning from
// becoming two.
//
// The header and the marker describe the same reconstruction, and the only reason
// they cannot disagree is that ReconstructionOf derives both. Editing the fixture
// and requiring *both* renderings to follow is what would catch a future change
// that read one of them from somewhere else.
func TestReconstructionMarkerCannotDriftFromTheHeader(t *testing.T) {
	doc := reconstructedDocument()
	doc.BaseTS = mustInstant("2026-05-01T08:30:00Z")
	doc.BaseEvent = query.EventCheckpoint
	doc.PatchesApplied = 417

	var out bytes.Buffer
	if err := render.WriteObject(&out, io.Discard, doc, reconstructedHead(),
		render.StructuredYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	marker := markerOf(t, decodeYAMLDocument(t, out.String()))
	for _, want := range []struct {
		field string
		value any
	}{
		{"base_ts", "2026-05-01T08:30:00Z"},
		{"base_event", query.EventCheckpoint},
		{"patches_applied", float64(417)},
	} {
		if got := marker[want.field]; got != want.value {
			t.Errorf("metadata.reconstruction.%s is %#v, want %#v — the marker is not being built "+
				"from the document the header is built from", want.field, got, want.value)
		}
	}

	// The same three facts, in the header, in the words a person reads them in.
	// A base three months old and four hundred patches is exactly the case the
	// provenance exists for, and both halves have to be saying it.
	for _, want := range []string{
		"# base row:        2026-05-01T08:30:00Z (Checkpoint)",
		"# patches applied: 417",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the header does not carry %q, so it and the marker describe different "+
				"reconstructions:\n%s", want, out.String())
		}
	}
}
