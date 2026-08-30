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
