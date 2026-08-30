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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The two things a hunk must never do: present a redaction sentinel as a value,
// and let one value take the terminal.
//
// Both are tested here rather than only through a golden file, because a golden
// file records what the renderer does and these record what it must do. A golden
// file regenerated after a regression is a passing test; these are not.

// diffAt is the instant every fixture in this file uses.
const diffAt = "2026-08-28T14:03:11.482Z"

// renderHunks renders one change and returns the stdout half.
func renderHunks(t *testing.T, ops []render.Op, opts render.Options) string {
	t.Helper()

	ts, err := time.Parse(time.RFC3339Nano, diffAt)
	if err != nil {
		t.Fatalf("parsing the fixture instant: %v", err)
	}
	if opts.Width == 0 {
		opts.Width = 120
	}

	var out, errOut bytes.Buffer
	doc := render.DiffDocument{
		Kind:    "apps/Deployment",
		Object:  "payments/checkout",
		Cluster: "prod-eu-1",
		Changes: []render.TimelineRow{{
			Change: query.Change{
				TS:        ts,
				EventType: query.EventModified,
				Actors:    []string{"kubectl-client-side-apply"},
			},
			Ops: ops,
		}},
	}
	if err := render.WriteDiff(&out, &errOut, doc, opts); err != nil {
		t.Fatalf("WriteDiff: %v", err)
	}
	return out.String()
}

// replaceOp builds a replace whose prior value the replay established.
func replaceOp(path string, old any, newValue string) render.Op {
	return render.Op{
		Type: render.OpReplace, Path: path,
		Value: json.RawMessage(newValue), Old: old, OldKnown: true,
	}
}

// TestDiffMarksTheRedactionSentinel is the acceptance criterion: a value equal to
// the sentinel must never render as though the object held that string.
//
// The sentinel is what is *stored* — redaction happens on the way in, before
// hashing — so the renderer cannot tell it from a ConfigMap whose value genuinely
// reads "[REDACTED]". Marking it is the only honest rendering of that ambiguity,
// and leaving it bare would let somebody conclude a password field was set to a
// literal string.
func TestDiffMarksTheRedactionSentinel(t *testing.T) {
	sentinel, err := json.Marshal(render.RedactionSentinel)
	if err != nil {
		t.Fatalf("encoding the sentinel: %v", err)
	}

	out := renderHunks(t, []render.Op{{
		Type: render.OpReplace, Path: "/data/password",
		Value: sentinel, Old: render.RedactionSentinel, OldKnown: true,
	}}, render.Options{})

	const want = render.RedactionSentinel + "  (redacted by policy)"
	if strings.Count(out, want) != 2 {
		t.Errorf("both halves of the hunk should carry the redaction marker.\nwant two of %q\ngot:\n%s",
			want, out)
	}

	// And nowhere may the sentinel appear as a bare value: a line ending at the
	// sentinel is a line a reader takes for content.
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasSuffix(line, render.RedactionSentinel) {
			t.Errorf("a value line ends at the sentinel, so it reads as a literal string: %q", line)
		}
	}
}

// TestDiffDimsTheRedactionMarker checks the marker is the renderer speaking
// rather than part of the value.
func TestDiffDimsTheRedactionMarker(t *testing.T) {
	sentinel, err := json.Marshal(render.RedactionSentinel)
	if err != nil {
		t.Fatalf("encoding the sentinel: %v", err)
	}

	out := renderHunks(t,
		[]render.Op{{Type: render.OpAdd, Path: "/data/token", Value: sentinel}},
		render.Options{Color: true})

	const dimMarker = "\x1b[2m  (redacted by policy)\x1b[0m"
	if !strings.Contains(out, dimMarker) {
		t.Errorf("the redaction marker is not dimmed, so it reads as part of the value.\n"+
			"want %q\ngot:\n%q", dimMarker, out)
	}
}

// TestDiffTruncatesLongValues covers the criterion a fat PodTemplate exists to
// break: a value over MaxValueRunes is cut, and the cut is announced with the
// size of what was withheld and the flag that shows it.
func TestDiffTruncatesLongValues(t *testing.T) {
	const overflow = 300
	long := strings.Repeat("x", render.MaxValueRunes+overflow)
	encoded, err := json.Marshal(long)
	if err != nil {
		t.Fatalf("encoding the fixture value: %v", err)
	}

	out := renderHunks(t,
		[]render.Op{{Type: render.OpAdd, Path: "/spec/template", Value: encoded}},
		render.Options{})

	want := fmt.Sprintf("…(%d more bytes, --full)", overflow)
	if !strings.Contains(out, want) {
		t.Errorf("a long value was not cut with the marker the criteria fix.\nwant %q\ngot:\n%s", want, out)
	}
	if strings.Contains(out, strings.Repeat("x", render.MaxValueRunes+1)) {
		t.Errorf("more than %d characters of the value were printed:\n%s", render.MaxValueRunes, out)
	}
}

// TestDiffFullPrintsWholeValues is the other half: --full is what the truncation
// marker points at, so it has to actually work.
func TestDiffFullPrintsWholeValues(t *testing.T) {
	long := strings.Repeat("x", render.MaxValueRunes+300)
	encoded, err := json.Marshal(long)
	if err != nil {
		t.Fatalf("encoding the fixture value: %v", err)
	}

	out := renderHunks(t,
		[]render.Op{{Type: render.OpAdd, Path: "/spec/template", Value: encoded}},
		render.Options{Full: true})

	if !strings.Contains(out, long) {
		t.Errorf("--full did not print the whole value:\n%s", out)
	}
	if strings.Contains(out, "--full)") {
		t.Errorf("--full still printed a truncation marker:\n%s", out)
	}
}

// TestDiffCapsOperationsPerChange covers the second half of the truncation
// criterion: a bulk rewrite must not scroll the screen, and the remainder is
// counted rather than dropped.
func TestDiffCapsOperationsPerChange(t *testing.T) {
	const extra = 5
	ops := make([]render.Op, 0, render.MaxOpsPerChange+extra)
	for i := range render.MaxOpsPerChange + extra {
		ops = append(ops, render.Op{
			Type:  render.OpAdd,
			Path:  fmt.Sprintf("/metadata/labels/key-%02d", i),
			Value: json.RawMessage(`"v"`),
		})
	}

	out := renderHunks(t, ops, render.Options{})

	want := fmt.Sprintf("… and %d more operations (--full)", extra)
	if !strings.Contains(out, want) {
		t.Errorf("the withheld operations were not counted.\nwant %q\ngot:\n%s", want, out)
	}
	if strings.Contains(out, "key-20") {
		t.Errorf("more than %d operations were shown:\n%s", render.MaxOpsPerChange, out)
	}
	if !strings.Contains(out, "key-19") {
		t.Errorf("fewer than %d operations were shown:\n%s", render.MaxOpsPerChange, out)
	}

	full := renderHunks(t, ops, render.Options{Full: true})
	if !strings.Contains(full, "key-24") {
		t.Errorf("--full did not print every operation:\n%s", full)
	}
}

// TestDiffColorsOperationsByMeaning pins the criterion's colours.
//
// By meaning rather than by aesthetics, and asserted by escape sequence because
// that is what a terminal reads: something arrived is green, something went is
// red, something moved in place is yellow.
func TestDiffColorsOperationsByMeaning(t *testing.T) {
	out := renderHunks(t, []render.Op{
		{Type: render.OpAdd, Path: "/spec/paused", Value: json.RawMessage(`true`)},
		{Type: render.OpRemove, Path: "/spec/minReadySeconds", Old: float64(10), OldKnown: true},
		replaceOp("/spec/replicas", float64(3), `5`),
	}, render.Options{Color: true})

	for _, want := range []struct{ what, sequence string }{
		{"an add", "\x1b[32m+ spec.paused\x1b[0m"},
		{"a remove", "\x1b[31m- spec.minReadySeconds\x1b[0m"},
		{"a replace", "\x1b[33m~ spec.replicas\x1b[0m"},
		{"the old value of a replace", "\x1b[31m- 3\x1b[0m"},
		{"the new value of a replace", "\x1b[32m+ 5\x1b[0m"},
	} {
		if !strings.Contains(out, want.sequence) {
			t.Errorf("%s is not coloured as the criteria fix.\nwant %q\ngot:\n%q",
				want.what, want.sequence, out)
		}
	}
}

// TestDiffStatesAnUnestablishedPriorValue covers Invariant 4 in the one place
// this renderer is tempted to break it.
//
// A hunk with only a "+" reads as a field that had no value before, which is a
// claim about the object. The absence has to be stated as an absence.
func TestDiffStatesAnUnestablishedPriorValue(t *testing.T) {
	out := renderHunks(t, []render.Op{{
		Type: render.OpReplace, Path: "/spec/replicas", Value: json.RawMessage(`5`),
	}}, render.Options{})

	if !strings.Contains(out, "- (prior value not established)") {
		t.Errorf("a replace with no established prior value renders without saying so:\n%s", out)
	}
}

// TestDiffRendersRowsThatCarryNoPatch checks that no block is ever empty.
//
// Every one of these rows is something happening, and a change header with
// nothing under it reads as a change that did nothing.
func TestDiffRendersRowsThatCarryNoPatch(t *testing.T) {
	ts, err := time.Parse(time.RFC3339Nano, diffAt)
	if err != nil {
		t.Fatalf("parsing the fixture instant: %v", err)
	}

	tests := []struct {
		name string
		row  render.TimelineRow
		want string
	}{
		{
			name: "a deletion",
			row:  render.TimelineRow{Change: query.Change{TS: ts, EventType: query.EventDeleted}},
			want: "object deleted",
		},
		{
			name: "a first sighting",
			row: render.TimelineRow{Change: query.Change{
				TS: ts, EventType: query.EventAdded, Data: `{"kind":"Deployment"}`}},
			want: "full state recorded",
		},
		{
			name: "a snapshot",
			row: render.TimelineRow{Change: query.Change{
				TS: ts, EventType: query.EventSnapshot, Data: `{"kind":"Deployment"}`}},
			want: "full state recorded (snapshot)",
		},
		{
			name: "a patch that will not decode",
			row: render.TimelineRow{
				Change:   query.Change{TS: ts, EventType: query.EventModified, Diff: `{`},
				PatchErr: "unexpected end of JSON input",
			},
			want: "unreadable patch: unexpected end of JSON input",
		},
		{
			name: "a patch with no operations",
			row: render.TimelineRow{Change: query.Change{
				TS: ts, EventType: query.EventModified, Diff: `[]`}},
			want: "patch recorded with no operations",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			doc := render.DiffDocument{Kind: "apps/Deployment", Changes: []render.TimelineRow{test.row}}
			if err := render.WriteDiff(&out, &errOut, doc, render.Options{Width: 120}); err != nil {
				t.Fatalf("WriteDiff: %v", err)
			}
			if !strings.Contains(out.String(), test.want) {
				t.Errorf("the block does not say what the row is.\nwant %q\ngot:\n%s", test.want, out.String())
			}
		})
	}
}

// TestDiffNoticesGoToStandardError keeps the split every command in this CLI
// depends on: stdout is the data, stderr explains it.
func TestDiffNoticesGoToStandardError(t *testing.T) {
	var out, errOut bytes.Buffer
	doc := render.DiffDocument{
		Kind:    "apps/Deployment",
		Notices: []render.Notice{{Text: "a qualification", Warning: true}},
	}
	if err := render.WriteDiff(&out, &errOut, doc, render.Options{Width: 120}); err != nil {
		t.Fatalf("WriteDiff: %v", err)
	}
	if strings.Contains(out.String(), "a qualification") {
		t.Errorf("a notice reached stdout, where a pipe would receive it:\n%s", out.String())
	}
	if !strings.Contains(errOut.String(), "! a qualification") {
		t.Errorf("the notice is missing from stderr:\n%s", errOut.String())
	}
}
