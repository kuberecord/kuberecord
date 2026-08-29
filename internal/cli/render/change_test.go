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

// TestPatchOps covers reading the diff column, including the two ways it can be
// unusable.
func TestPatchOps(t *testing.T) {
	tests := []struct {
		name    string
		diff    string
		want    []render.Op
		wantErr bool
	}{
		{name: "an empty diff is no operations and no error", diff: ""},
		{name: "whitespace is no operations either", diff: "   "},
		{
			name: "a single replace",
			diff: `[{"op":"replace","path":"/spec/replicas","value":5}]`,
			want: []render.Op{{Type: "replace", Path: "/spec/replicas", Value: json.RawMessage("5")}},
		},
		{
			name: "a remove carries no value",
			diff: `[{"op":"remove","path":"/spec/paused"}]`,
			want: []render.Op{{Type: "remove", Path: "/spec/paused"}},
		},
		{name: "an undecodable diff is reported, not swallowed", diff: `{"op":"replace"}`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := render.PatchOps(test.diff)
			if test.wantErr {
				if err == nil {
					t.Fatalf("PatchOps(%q) returned no error; an unreadable patch must be reported so "+
						"the row can say so rather than vanish", test.diff)
				}
				return
			}
			if err != nil {
				t.Fatalf("PatchOps(%q): %v", test.diff, err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("PatchOps(%q) returned %d operations, want %d", test.diff, len(got), len(test.want))
			}
			for i := range got {
				if got[i].Type != test.want[i].Type || got[i].Path != test.want[i].Path ||
					string(got[i].Value) != string(test.want[i].Value) {
					t.Errorf("PatchOps(%q)[%d] = %+v, want %+v", test.diff, i, got[i], test.want[i])
				}
			}
		})
	}
}

// TestChangeCellRendersOneOperation is the flagship row, exercised through the
// public entry point rather than through an unexported helper, so that what is
// asserted is what a user sees.
func TestChangeCellRendersOneOperation(t *testing.T) {
	tests := []struct {
		name  string
		op    render.Op
		width int
		want  string
	}{
		{
			name: "a replace with a known prior value",
			op: render.Op{
				Type: "replace", Path: "/spec/replicas",
				Value: json.RawMessage("5"), Old: float64(3), OldKnown: true,
			},
			width: 60,
			want:  "~ spec.replicas: 3 → 5",
		},
		{
			name: "a replace whose prior value could not be established",
			op: render.Op{
				Type: "replace", Path: "/spec/replicas", Value: json.RawMessage("5"),
			},
			width: 60,
			// No arrow and no dash. The reader is told separately, on stderr,
			// that the past could not be recovered.
			want: "~ spec.replicas: 5",
		},
		{
			name:  "an add shows what appeared",
			op:    render.Op{Type: "add", Path: "/spec/paused", Value: json.RawMessage("true")},
			width: 60,
			want:  "+ spec.paused: true",
		},
		{
			name:  "a remove shows what went",
			op:    render.Op{Type: "remove", Path: "/spec/paused", Old: true, OldKnown: true},
			width: 60,
			want:  "- spec.paused: true",
		},
		{
			name:  "a remove with no prior value has nothing to show",
			op:    render.Op{Type: "remove", Path: "/spec/paused"},
			width: 60,
			want:  "- spec.paused",
		},
		{
			name: "a string value loses its quotes",
			op: render.Op{
				Type: "replace", Path: "/spec/template/spec/containers/0/image",
				Value: json.RawMessage(`"nginx:1.26"`), Old: "nginx:1.25", OldKnown: true,
			},
			width: 80,
			want:  "~ spec.template.spec.containers[0].image: nginx:1.25 → nginx:1.26",
		},
		{
			name: "an object value is compacted onto one line",
			op: render.Op{
				Type: "add", Path: "/spec/selector",
				Value: json.RawMessage("{\n  \"app\": \"checkout\"\n}"),
			},
			width: 80,
			want:  `+ spec.selector: {"app":"checkout"}`,
		},
		{
			name: "the acceptance criteria's own row",
			op: render.Op{
				Type: "replace", Path: "/spec/template/spec/containers/0/resources/limits/memory",
				Value: json.RawMessage(`"512Mi"`), Old: "2Gi", OldKnown: true,
			},
			width: 58,
			want:  "~ spec.…containers[0].resources.limits.memory: 2Gi → 512Mi",
		},
		{
			name: "an operation this project never emits keeps its own name",
			op: render.Op{
				Type: "move", Path: "/spec/b", From: "/spec/a",
			},
			width: 60,
			want:  "move spec.b: spec.a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := cellFor(t, query.Change{EventType: query.EventModified, Diff: "x"},
				[]render.Op{test.op}, test.width)
			if got != test.want {
				t.Errorf("the CHANGE cell was\n  %q\nwant\n  %q", got, test.want)
			}
		})
	}
}

// TestChangeCellShortensValuesTowardsEachOther pins which half of "old → new"
// survives a narrow column.
//
// The new value is what the reader came for, so it keeps its budget and the old
// one gives ground. A tail truncation of the pair would do the opposite.
func TestChangeCellShortensValuesTowardsEachOther(t *testing.T) {
	op := render.Op{
		Type:  "replace",
		Path:  "/spec/x",
		Value: json.RawMessage(`"nnnnnnnnnnnnnnnnnnnn"`),
		Old:   "oooooooooooooooooooo", OldKnown: true,
	}
	// Wider than minChangeWidth, so that what is being measured is the fitting
	// rule rather than the floor the layout puts under the column.
	const budget = 40
	got := cellFor(t, query.Change{EventType: query.EventModified, Diff: "x"}, []render.Op{op}, budget)

	if len([]rune(got)) > budget {
		t.Fatalf("the cell overflowed its column: %q", got)
	}
	if !strings.Contains(got, "→") {
		t.Fatalf("the arrow was dropped, so the row no longer reads as a transition: %q", got)
	}
	if !strings.HasPrefix(got, "~ spec.x: o") {
		t.Errorf("the old value lost its head rather than its tail: %q", got)
	}
}

// TestChangeCellDescribesRowsWithoutPatches covers the rows that bracket an
// object's existence, which carry no diff and must never render as a blank cell.
func TestChangeCellDescribesRowsWithoutPatches(t *testing.T) {
	tests := []struct {
		name   string
		change query.Change
		want   string
	}{
		{"a first sighting", query.Change{EventType: query.EventAdded, Data: "{}"}, "full state recorded"},
		{
			name:   "a pre-warm snapshot says which it is",
			change: query.Change{EventType: query.EventSnapshot, Data: "{}"},
			want:   "full state recorded (snapshot)",
		},
		{
			name:   "a modification that could not produce a patch says so",
			change: query.Change{EventType: query.EventModified, Data: "{}"},
			want:   "full state recorded (no patch produced)",
		},
		{"a deletion", query.Change{EventType: query.EventDeleted}, "object deleted"},
		{
			name:   "a row with neither state nor patch",
			change: query.Change{EventType: query.EventModified},
			want:   "no state and no patch recorded",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cellFor(t, test.change, nil, 80); got != test.want {
				t.Errorf("the CHANGE cell was %q, want %q", got, test.want)
			}
		})
	}
}

// TestChangeCellCountsALargerPatch is the multi-operation summary.
func TestChangeCellCountsALargerPatch(t *testing.T) {
	ops := []render.Op{
		{Type: "replace", Path: "/spec/replicas", Value: json.RawMessage("5")},
		{Type: "add", Path: "/spec/paused", Value: json.RawMessage("true")},
		{Type: "remove", Path: "/spec/minReadySeconds"},
	}
	if got := cellFor(t, query.Change{EventType: query.EventModified, Diff: "x"}, ops, 80); got != "~3 ops" {
		t.Errorf("the CHANGE cell was %q, want %q", got, "~3 ops")
	}
}

// TestChangeCellKeepsAnUnreadablePatchVisible asserts the row survives its own
// patch being unusable.
//
// Dropping it would take an entry out of an audit timeline to spare a reader an
// ugly cell, which is the wrong way round.
func TestChangeCellKeepsAnUnreadablePatchVisible(t *testing.T) {
	doc := render.TimelineDocument{Rows: []render.TimelineRow{{
		Change:   query.Change{EventType: query.EventModified, Diff: "{"},
		PatchErr: "decoding the recorded patch: unexpected end of JSON input",
	}}}
	got := renderRows(t, doc, render.Options{Width: 200})
	if !strings.Contains(got, "unreadable patch") {
		t.Errorf("the row did not say its patch was unreadable:\n%s", got)
	}
}

// cellFor renders one row and returns its CHANGE cell.
//
// It goes through WriteTimeline rather than reaching for an unexported helper, so
// that every assertion above is made against the characters a user actually gets
// — including the column arithmetic, which is where a rendering bug hides.
func cellFor(t *testing.T, change query.Change, ops []render.Op, changeWidth int) string {
	t.Helper()

	doc := render.TimelineDocument{Rows: []render.TimelineRow{{Change: change, Ops: ops}}}
	// The columns to the left of CHANGE, at their minimum widths for a row whose
	// timestamp is the zero time and whose event type is the longest here.
	line := lastLine(t, renderRows(t, doc, render.Options{Width: fixedColumns(change) + changeWidth}))
	return strings.TrimRight(line[fixedColumns(change):], " ")
}

// fixedColumns is the width the columns left of CHANGE take for a single row.
//
// TIME is 23 columns of timestamp plus its gutter, EVENT is the event type or its
// heading, whichever is longer, and ACTOR is "unknown" or its heading — both
// widened to their headings for the short values these tests use.
func fixedColumns(change query.Change) int {
	event := max(len(change.EventType), len("EVENT"))
	return 23 + 2 + event + 2 + len("unknown") + 2
}

func renderRows(t *testing.T, doc render.TimelineDocument, opts render.Options) string {
	t.Helper()

	var out strings.Builder
	if err := render.WriteTimeline(&out, nil, doc, opts); err != nil {
		t.Fatalf("WriteTimeline: %v", err)
	}
	return out.String()
}

func lastLine(t *testing.T, rendered string) string {
	t.Helper()

	lines := strings.Split(strings.TrimRight(rendered, "\n"), "\n")
	if len(lines) == 0 {
		t.Fatal("nothing was rendered")
	}
	return lines[len(lines)-1]
}
