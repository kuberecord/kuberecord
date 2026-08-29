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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// warningRow is a merged Kubernetes Event that earns the glyph.
func warningRow() render.TimelineRow {
	return render.TimelineRow{Change: query.Change{
		EventType: query.EventKubernetes,
		Data: `{"type":"Warning","reason":"FailedScheduling",` +
			`"message":"0/6 nodes are available","reportingController":"default-scheduler"}`,
	}}
}

// TestAWarningEventIsGlyphedAndPainted covers the one piece of colour inside the
// CHANGE column.
//
// The glyph is painted *after* the cell has been fitted, so it must cost exactly
// the one column it occupies on screen. A red sequence counted into the width
// would shorten the message beside it, which is the failure this asserts against
// by comparing the two renderings rune for rune.
func TestAWarningEventIsGlyphedAndPainted(t *testing.T) {
	doc := render.TimelineDocument{Rows: []render.TimelineRow{warningRow()}}

	plain := renderRows(t, doc, render.Options{Width: 120})
	painted := renderRows(t, doc, render.Options{Width: 120, Color: true})

	if !strings.Contains(plain, render.WarningGlyph+" FailedScheduling: 0/6 nodes are available") {
		t.Errorf("the warning was not glyphed:\n%s", plain)
	}
	if !strings.Contains(painted, "\x1b[31m"+render.WarningGlyph+"\x1b[0m") {
		t.Errorf("the glyph was not painted:\n%s", painted)
	}
	if stripped := stripANSI(painted); stripped != plain {
		t.Errorf("painting the glyph changed the layout.\n--- plain ---\n%s\n--- stripped ---\n%s",
			plain, stripped)
	}
}

// TestTheReporterAnswersForAnEventWithNoFieldManagers covers the ACTOR column's
// fallback.
//
// "default-scheduler" is a better answer to "who" than "unknown" is, for a row
// that is entirely about what the scheduler had to say.
func TestTheReporterAnswersForAnEventWithNoFieldManagers(t *testing.T) {
	doc := render.TimelineDocument{Rows: []render.TimelineRow{warningRow()}}
	if rendered := renderRows(t, doc, render.Options{Width: 120}); !strings.Contains(rendered, "default-scheduler") {
		t.Errorf("the reporting controller was not used as the actor:\n%s", rendered)
	}
}

// TestNoticesReachOnlyTheErrorStream is the stream split, asserted at the one
// place it is decided.
//
// stdout is the data and stderr explains it. A notice on stdout would corrupt a
// pipe; a notice nowhere would be the silence Invariants 4 and 5 forbid.
func TestNoticesReachOnlyTheErrorStream(t *testing.T) {
	doc := render.TimelineDocument{
		Kind: "apps/Deployment", Object: "payments/checkout", Cluster: "prod-eu-1",
		Coverage: "none recorded for this scope",
		Notices: []render.Notice{
			{Text: "an ordinary notice"},
			{Text: "a warning", Warning: true},
		},
	}

	var out, errOut strings.Builder
	if err := render.WriteTimeline(&out, &errOut, doc, render.Options{Color: true}); err != nil {
		t.Fatalf("WriteTimeline: %v", err)
	}

	for _, notice := range []string{"an ordinary notice", "a warning"} {
		if !strings.Contains(errOut.String(), notice) {
			t.Errorf("%q did not reach stderr:\n%s", notice, errOut.String())
		}
		if strings.Contains(out.String(), notice) {
			t.Errorf("%q reached stdout, where a pipe would receive it:\n%s", notice, out.String())
		}
	}
	if !strings.Contains(errOut.String(), "\x1b[31m!\x1b[0m a warning") {
		t.Errorf("a warning was not marked differently from a notice:\n%s", errOut.String())
	}
	if strings.Contains(out.String(), "TIME (UTC)") {
		t.Errorf("an empty document printed a table header with nothing under it:\n%s", out.String())
	}
}

// TestWriteTimelineToleratesAMissingStream covers a caller that wants only one
// half of the output, which is what every test in the sibling package that
// inspects a table does.
func TestWriteTimelineToleratesAMissingStream(t *testing.T) {
	doc := render.TimelineDocument{Notices: []render.Notice{{Text: "nowhere to go"}}}
	if err := render.WriteTimeline(nil, nil, doc, render.Options{}); err != nil {
		t.Errorf("WriteTimeline with no streams: %v", err)
	}
}

// stripANSI removes the escape sequences colour adds.
func stripANSI(text string) string {
	var built strings.Builder
	for i := 0; i < len(text); {
		if text[i] != 0x1b {
			built.WriteByte(text[i])
			i++
			continue
		}
		for i < len(text) && text[i] != 'm' {
			i++
		}
		i++
	}
	return built.String()
}
