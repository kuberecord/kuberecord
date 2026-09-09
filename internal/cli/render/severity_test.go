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
	"flag"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The severity vocabulary, asserted as a reader meets it: every register next to
// each other, in both colour modes.
//
// The half of the property that says colour changes nothing but colour is not
// here. It is one property over the whole CLI's output, and it is asserted in one
// place — TestColourIsNothingButColour in internal/cli/resolve — over the
// diagnostic and every tier together, so that a second copy of it cannot drift
// away from the first.

// updateGolden rewrites the golden files instead of comparing against them.
//
//	go test ./internal/cli/render/ -update
//
// One flag for every golden suite in this package — the severity block here and
// the per-kind envelope documents in yaml_test.go — because a second -update path
// is a second place for the write half to drift from the compare half.
var updateGolden = flag.Bool("update", false, "rewrite the golden files")

// The lines the block is built from, in the shape their call sites spend them:
// three from the documents Tasks 15.5 and 15.6 render, and one from the exit path
// (Task 18.2), which is the only tier a reader meets instead of a result rather
// than beside one.
const (
	fixtureEmphasis    = "NOT A DEPLOYABLE MANIFEST"
	fixtureProvenance  = "Cluster    prod-eu-1"
	fixtureProvenance2 = "Base       2026-08-14T09:12:44.317Z  Modified"
	fixtureWarning     = "this backend does not record deletions, so a timeline that stops is not " +
		"proof the object was deleted"
	fixtureFailure = "error: cannot reach ClickHouseSink/default at clickhouse.kuberecord-system.svc:9000"
)

// severityBlock renders one line per tier, in the shape each is destined for.
//
// One file per colour mode rather than one per tier, because the question a
// reader of these files is answering is a question about all of them together: is
// a warning distinguishable from provenance, is a failure distinguishable from a
// warning, is one emphasised line enough, does the block still read when every
// escape is gone.
//
// The failure line comes last because that is where a reader meets it — beneath
// whatever the invocation had already printed, which is the arrangement that made
// its old default weight a finding.
func severityBlock(color bool) string {
	severity := render.NewSeverity(color)
	return strings.Join([]string{
		severity.Emphasis(fixtureEmphasis),
		severity.Provenance(fixtureProvenance),
		severity.Provenance(fixtureProvenance2),
		render.WarningMarker + " " + severity.Warning(fixtureWarning),
		severity.Failure(fixtureFailure),
	}, "\n") + "\n"
}

// TestSeverityRendersEachTierInBothColourModes, against golden files.
//
// Golden files rather than assembled expectations because what is under test is
// how the tiers read against one another, which is a property of the block and
// not of any line in it — and because a change to the register a tier paints in
// should arrive in review as the diff a reader would have seen.
func TestSeverityRendersEachTierInBothColourModes(t *testing.T) {
	for name, color := range map[string]bool{"plain": false, "color": true} {
		t.Run(name, func(t *testing.T) {
			assertSeverityGolden(t, name, severityBlock(color))
		})
	}
}

// TestSeverityWithoutColourIsTheTextItself.
//
// Under --color=never and NO_COLOR every tier has to render as exactly its text:
// not trimmed, not prefixed, not marked in any way the caller did not ask for.
// This is what lets a call site spend the vocabulary unconditionally — there is no
// colour check at any of them, so a tier that added a character when colour was
// off would add it to every redirected stream in the CLI.
func TestSeverityWithoutColourIsTheTextItself(t *testing.T) {
	plain := render.NewSeverity(false)

	for name, rendered := range map[string]func(string) string{
		"warning":    plain.Warning,
		"provenance": plain.Provenance,
		"emphasis":   plain.Emphasis,
		"failure":    plain.Failure,
	} {
		t.Run(name, func(t *testing.T) {
			if got := rendered(fixtureWarning); got != fixtureWarning {
				t.Errorf("the uncoloured tier changed its text:\n--- want ---\n%s\n--- got ---\n%s",
					fixtureWarning, got)
			}
			// Empty text stays empty in both modes: a tier that painted nothing
			// would emit a bare pair of escapes, which is a cell a terminal draws
			// and a golden file records for no reason.
			if got := rendered(""); got != "" {
				t.Errorf("the tier painted an empty string: %q", got)
			}
		})
	}
}

// TestSeverityWarningsKeepTheirMarker, in both colour modes.
//
// The marker is the whole of what survives NO_COLOR, so it is asserted separately
// from the tier that carries it. A warning rendered without it is a sentence in a
// stream of sentences, and the reader has nothing to tell them which one they
// cannot skip.
func TestSeverityWarningsKeepTheirMarker(t *testing.T) {
	for name, block := range map[string]string{"plain": severityBlock(false), "color": severityBlock(true)} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(block, render.WarningMarker+" ") {
				t.Errorf("the warning line carries no %q marker, so its severity does not survive "+
					"a stream with no colour:\n%s", render.WarningMarker, block)
			}
		})
	}
}

// TestSeverityTiersAreDistinguishable.
//
// Three registers that painted the same thing would be one register with three
// names, and the failure would be invisible: every call site would still compile,
// every golden file would still be generated from the code that produced it, and
// the only symptom would be a reader unable to tell a notice from a header. It is
// asserted here so that a later pass at making the output quieter has to change a
// test that says why the tiers differ, rather than a colour constant.
func TestSeverityTiersAreDistinguishable(t *testing.T) {
	colored := render.NewSeverity(true)

	rendered := map[string]string{
		"warning":    colored.Warning(fixtureWarning),
		"provenance": colored.Provenance(fixtureWarning),
		"emphasis":   colored.Emphasis(fixtureWarning),
	}
	for name, one := range rendered {
		for otherName, other := range rendered {
			if name < otherName && one == other {
				t.Errorf("the %s and %s tiers render identically, so the vocabulary has "+
					"%d names for one register", name, otherName, len(rendered))
			}
		}
	}
}

// assertSeverityGolden compares a rendering against its checked-in file.
func assertSeverityGolden(t *testing.T, name, got string) {
	t.Helper()

	assertRenderGolden(t, "severity", name, got)
}
