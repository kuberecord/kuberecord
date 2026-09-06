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
	"encoding/json"
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
			{Text: "the first notice"},
			{Text: "the second notice"},
		},
	}

	var out, errOut strings.Builder
	if err := render.WriteTimeline(&out, &errOut, doc, render.Options{Color: true}); err != nil {
		t.Fatalf("WriteTimeline: %v", err)
	}

	for _, notice := range []string{"the first notice", "the second notice"} {
		if !strings.Contains(errOut.String(), notice) {
			t.Errorf("%q did not reach stderr:\n%s", notice, errOut.String())
		}
		if strings.Contains(out.String(), notice) {
			t.Errorf("%q reached stdout, where a pipe would receive it:\n%s", notice, out.String())
		}
	}
	if want := render.WarningMarker + " " + render.NewSeverity(true).Warning("the second notice"); !strings.Contains(
		errOut.String(), want) {
		t.Errorf("a notice did not render as a marked warning:\n--- want ---\n%s\n--- got ---\n%s",
			want, errOut.String())
	}
	if strings.Contains(out.String(), "TIME (UTC)") {
		t.Errorf("an empty document printed a table header with nothing under it:\n%s", out.String())
	}
}

// TestANoticeIsNeverDimmed is D30, asserted as a property of the two tiers rather
// than of a colour constant.
//
// A notice exists because the data on its own misleads — a timeline that stops
// against a backend recording no deletions, a window with no state before it —
// and provenance exists because a fact has to be available without being read
// again. Rendering the first in the register of the second is not a change of
// taste, it is the CLI saying "you may skip this" about the line it printed
// because you may not.
//
// It is worth a test of its own because the failure is silent: every call site
// would still compile, every golden file would still be regenerated from the code
// that produced it, and the only symptom would be a reader missing the
// qualification on an answer they then act on.
func TestANoticeIsNeverDimmed(t *testing.T) {
	const text = "this backend does not record deletions"

	var errOut strings.Builder
	if err := render.WriteNotices(
		&errOut, []render.Notice{{Text: text}}, render.Options{Color: true}); err != nil {
		t.Fatalf("WriteNotices: %v", err)
	}

	severity := render.NewSeverity(true)
	if !strings.Contains(errOut.String(), severity.Warning(text)) {
		t.Errorf("a notice did not render in the warning tier:\n%s", errOut.String())
	}
	if strings.Contains(errOut.String(), severity.Provenance(text)) {
		t.Errorf("a notice rendered in the provenance tier, so a reader is being told "+
			"they may skip it:\n%s", errOut.String())
	}
}

// TestTheFullHintIsEmittedOnlyWhenARowWasCollapsed.
//
// Both halves matter and the absent half matters more. A footer that appears
// under every timeline is a footer readers learn to skip, and it would be
// skipped on the one invocation where it is true — so the assertion that it is
// *missing* from a document where nothing was shortened is the one protecting the
// hint's value, not the one asserting it is present.
func TestTheFullHintIsEmittedOnlyWhenARowWasCollapsed(t *testing.T) {
	for name, test := range map[string]struct {
		rows []render.TimelineRow
		opts render.Options
		want bool
	}{
		"a row summarized as a count names the flag": {
			rows: []render.TimelineRow{collapsedRow()}, opts: render.Options{Width: 120}, want: true,
		},
		"a row shown whole names nothing": {
			rows: []render.TimelineRow{wholeRow()}, opts: render.Options{Width: 120}, want: false,
		},
		"--full is already on, so there is nothing to name": {
			rows: []render.TimelineRow{collapsedRow()},
			opts: render.Options{Width: 120, Full: true}, want: false,
		},
		"an empty document has no table to have collapsed anything": {
			rows: nil, opts: render.Options{Width: 120}, want: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var out, errOut strings.Builder
			doc := render.TimelineDocument{Coverage: "open", Rows: test.rows}
			if err := render.WriteTimeline(&out, &errOut, doc, test.opts); err != nil {
				t.Fatalf("WriteTimeline: %v", err)
			}

			if got := strings.Contains(errOut.String(), "--full"); got != test.want {
				t.Errorf("the footer names --full: %t, want %t\n--- stdout ---\n%s--- stderr ---\n%s",
					got, test.want, out.String(), errOut.String())
			}
			if strings.Contains(out.String(), "--full") {
				t.Errorf("the hint reached stdout, where a pipe would receive it:\n%s", out.String())
			}
		})
	}
}

// TestTheFullHintIsOneLineHoweverManyRowsCollapsed.
//
// The alternative was a marker on every collapsed row, and a hundred of those is
// a worse noise than the one this hint answers. One line, and it counts.
func TestTheFullHintIsOneLineHoweverManyRowsCollapsed(t *testing.T) {
	doc := render.TimelineDocument{
		Coverage: "open",
		Rows:     []render.TimelineRow{collapsedRow(), wholeRow(), collapsedRow()},
	}

	var errOut strings.Builder
	if err := render.WriteTimeline(nil, &errOut, doc, render.Options{Width: 120}); err != nil {
		t.Fatalf("WriteTimeline: %v", err)
	}

	if got := strings.Count(errOut.String(), "--full"); got != 1 {
		t.Errorf("--full is named %d times, want once:\n%s", got, errOut.String())
	}
	if !strings.Contains(errOut.String(), "2 rows are") {
		t.Errorf("the footer did not count the rows it is about:\n%s", errOut.String())
	}
}

// collapsedRow is a change the CHANGE column has to summarize.
func collapsedRow() render.TimelineRow {
	return render.TimelineRow{
		Change: query.Change{EventType: query.EventModified, Diff: "x", Actors: []string{"kubectl"}},
		Ops: []render.Op{
			{Type: render.OpReplace, Path: "/spec/replicas", Value: json.RawMessage("5")},
			{Type: render.OpAdd, Path: "/spec/paused", Value: json.RawMessage("true")},
		},
	}
}

// wholeRow is a change whose single short operation the column shows entire, so
// that --full would add nothing to it.
func wholeRow() render.TimelineRow {
	return render.TimelineRow{
		Change: query.Change{EventType: query.EventModified, Diff: "x", Actors: []string{"kubectl"}},
		Ops:    []render.Op{{Type: render.OpReplace, Path: "/spec/replicas", Value: json.RawMessage("5")}},
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
