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
	if err := render.WriteObject(
		&out, &errOut, reconstructedDocument(), render.ObjectYAML, render.Options{}); err != nil {
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
	if err := render.WriteObject(&out, &errOut, doc, render.ObjectYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	var decoded map[string]any
	if err := yaml.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("the rendered document is not valid YAML: %v\n%s", err, out.String())
	}
	spec, ok := decoded["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the decoded document has no spec: %#v", decoded)
	}
	if spec["replicas"] != float64(5) {
		t.Errorf("the document did not survive the header: spec.replicas is %#v, want 5", spec["replicas"])
	}
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
	if err := render.WriteObject(
		&out, &errOut, reconstructedDocument(), render.ObjectJSON, render.Options{}); err != nil {
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
	if decoded["kind"] != "Deployment" {
		t.Errorf("the decoded document is not the object: %#v", decoded)
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
	if err := render.WriteObject(&out, &errOut, doc, render.ObjectYAML, render.Options{}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	for _, want := range []string{"# uid:             not recorded", "(not recorded)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("an absent fact is rendered as a blank.\nwant %q\ngot:\n%s", want, out.String())
		}
	}
}
