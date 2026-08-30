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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The blame table's own layout decisions, driven directly rather than through a
// command.
//
// The elastic column here is the *first* one, which is the one difference from
// every other table in this package, so the cases that matter are the ones where
// the budget runs out: a path long enough to be elided, and a removal marker that
// must survive the elision because losing it would report a deleted field as
// present.

// blameRow builds one attributed row for the cases below.
func blameRow(path, pointer, actor string) render.BlameRow {
	return render.BlameRow{
		Path: path, Pointer: pointer, Attributed: true,
		TS: mustInstant("2026-08-28T14:05:02Z"), Actors: []string{actor}, Fields: 1,
	}
}

// TestBlameKeepsTheRemovalMarkerWhenThePathIsElided is the narrow-terminal case.
//
// A path elided to fit still identifies something; a row that lost its
// "(removed)" says the field is part of the object, which is a claim about the
// cluster rather than about the width of a window.
func TestBlameKeepsTheRemovalMarkerWhenThePathIsElided(t *testing.T) {
	row := blameRow(
		"spec.template.spec.containers[0].resources.limits.ephemeral-storage",
		"/spec/template/spec/containers/0/resources/limits/ephemeral-storage",
		"kube-controller-manager")
	row.Removed = true

	out := writeBlame(t, render.BlameDocument{Rows: []render.BlameRow{row}}, render.Options{Width: 100})

	if !strings.Contains(out, render.RemovedMarker) {
		t.Errorf("the removal marker was elided away, so a deleted field reads as present:\n%s", out)
	}
	if !strings.Contains(out, render.Ellipsis) {
		t.Errorf("a path too long for the column was not elided:\n%s", out)
	}
	if !strings.Contains(out, "limits.ephemeral-storage") {
		t.Errorf("the elision gave up the leaf, which is the half that names the field:\n%s", out)
	}
}

// TestBlameNamesAnUnattributedFieldTwice checks the pair of cells that carry the
// same fact.
//
// The timestamp cell says the write is older than the window; the actor cell must
// not say "unknown", which is the answer for a change that was read and recorded
// no field managers. Two different absences, and a reader has to be able to tell
// which one they have.
func TestBlameNamesAnUnattributedFieldTwice(t *testing.T) {
	out := writeBlame(t, render.BlameDocument{Rows: []render.BlameRow{{
		Path: "metadata.name", Pointer: "/metadata/name", Fields: 1,
	}}}, render.Options{Width: 120})

	if !strings.Contains(out, render.BeforeWindow) {
		t.Errorf("a field older than the window does not say so:\n%s", out)
	}
	if strings.Contains(out, render.UnknownActor) {
		t.Errorf("a field nobody read a change for is attributed to an unknown actor, which is what "+
			"an actorless change renders as:\n%s", out)
	}
}

// TestBlameCountsCollapsedFieldsOnlyWhenSomethingCollapsed keeps the FIELDS
// column from appearing as a column of ones.
func TestBlameCountsCollapsedFieldsOnlyWhenSomethingCollapsed(t *testing.T) {
	single := render.BlameDocument{Rows: []render.BlameRow{
		blameRow("spec.replicas", "/spec/replicas", "kube-controller-manager"),
	}}
	if out := writeBlame(t, single, render.Options{Width: 120}); strings.Contains(out, "FIELDS") {
		t.Errorf("a table where nothing collapsed grew a FIELDS column:\n%s", out)
	}

	collapsed := blameRow("spec.template", "/spec/template", "argocd-application-controller")
	collapsed.Fields = 12
	out := writeBlame(t, render.BlameDocument{
		Rows: []render.BlameRow{collapsed, single.Rows[0]},
	}, render.Options{Width: 120})

	if !strings.Contains(out, "FIELDS") {
		t.Errorf("a collapsed row was rendered without saying how many fields it stands for:\n%s", out)
	}
	if !strings.Contains(out, "12") {
		t.Errorf("the collapsed row's count is missing:\n%s", out)
	}
}

// TestBlameWritesNoTableForNoFields keeps an empty result from reading as an
// object with no fields, the same choice every other document here makes.
func TestBlameWritesNoTableForNoFields(t *testing.T) {
	var out, errOut bytes.Buffer
	doc := render.BlameDocument{
		Kind: "apps/Deployment", Object: "payments/checkout", Cluster: "prod-eu-1",
		Window: "all recorded history", Base: "not established",
		Coverage: "none recorded for this scope",
		Notices:  []render.Notice{{Text: "nothing was ever watching this", Warning: true}},
	}
	if err := render.WriteBlame(&out, &errOut, doc, render.Options{Width: 120}); err != nil {
		t.Fatalf("WriteBlame: %v", err)
	}

	if strings.Contains(out.String(), "FIELD") {
		t.Errorf("an empty attribution printed a heading row:\n%s", out.String())
	}
	for _, want := range []string{"all recorded history", "not established"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the header does not carry %q, which is what a (before window) cell is read "+
				"against:\n%s", want, out.String())
		}
	}
	if !strings.Contains(errOut.String(), "nothing was ever watching this") {
		t.Errorf("the notice did not reach stderr:\n%s", errOut.String())
	}
	if strings.Contains(out.String(), "nothing was ever watching this") {
		t.Errorf("a notice reached stdout, where a pipe would receive it:\n%s", out.String())
	}
}

// TestBlameColoursSurviveTheLayout is the property every table in this package
// has to keep: escapes carry no display width, so they must be applied after the
// widths are settled or every coloured cell is padded by the length of its escape
// codes.
func TestBlameColoursSurviveTheLayout(t *testing.T) {
	removed := blameRow("spec.minReadySeconds", "/spec/minReadySeconds", "kube-controller-manager")
	removed.Removed = true
	doc := render.BlameDocument{Rows: []render.BlameRow{
		blameRow("spec.template.spec.containers[0].image", "/spec/template/spec/containers/0/image",
			"argocd-application-controller"),
		removed,
	}}

	plain := writeBlame(t, doc, render.Options{Width: 120})
	painted := writeBlame(t, doc, render.Options{Width: 120, Color: true})

	if !strings.Contains(painted, "\x1b[31m") {
		t.Errorf("a removed field is not painted, so the one row that says a field is gone reads "+
			"like the rest:\n%q", painted)
	}
	if stripped := stripANSI(painted); stripped != plain {
		t.Errorf("colour changed the layout.\n--- uncoloured ---\n%s\n--- painted, stripped ---\n%s",
			plain, stripped)
	}
}

// writeBlame renders one document to a string.
func writeBlame(t *testing.T, doc render.BlameDocument, opts render.Options) string {
	t.Helper()

	var out bytes.Buffer
	if err := render.WriteBlame(&out, nil, doc, opts); err != nil {
		t.Fatalf("WriteBlame: %v", err)
	}
	return out.String()
}
