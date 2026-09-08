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

package render

import (
	"encoding/json"
	"testing"
)

// The one split the CHANGE column's vocabulary is allowed, asserted where it is
// made.
//
// Everything else in this package is tested from outside it, through the
// characters a user gets. This is here because what has to hold is a statement
// about two functions rather than about a rendering: --full paints one half of an
// operation and leaves the other alone, and the halves have to be exactly the
// line opText would have produced. Seen from outside, a marker that drifted would
// show up as a golden file that needed regenerating — which is the review that
// waves it through.

// TestOpTextPartsRecomposeTheUnlimitedRendering.
//
// The recomposition is the property, and it is what keeps the uncoloured
// rendering byte for byte the one it has always been: under --color=never and
// NO_COLOR the provenance tier is the identity function, so marker+detail is
// literally what the line was before any of this existed.
func TestOpTextPartsRecomposeTheUnlimitedRendering(t *testing.T) {
	tests := map[string]Op{
		"an add": {
			Type: OpAdd, Path: "/spec/paused", Value: json.RawMessage(`true`),
		},
		"a replace with a prior value": {
			Type: OpReplace, Path: "/spec/replicas", Value: json.RawMessage(`5`),
			Old: json.RawMessage(`3`), OldKnown: true,
		},
		"a replace whose prior value could not be established": {
			Type: OpReplace, Path: "/spec/replicas", Value: json.RawMessage(`5`),
		},
		// A remove with no prior value renders as the path alone, so the detail
		// half carries no separator and no value — the case where a naive split on
		// ": " would have found nothing to split on.
		"a remove with no prior value": {
			Type: OpRemove, Path: "/spec/minReadySeconds",
		},
		"a remove with a prior value": {
			Type: OpRemove, Path: "/spec/minReadySeconds", Old: json.RawMessage(`10`), OldKnown: true,
		},
		// Not an operation this project records. It keeps its own name as its
		// marker rather than borrowing a glyph (see glyph), and the split has to
		// hold for it too: the marker is wherever opText's prefix ends, not
		// wherever a single character does.
		"an operation this project never emits": {
			Type: "move", Path: "/spec/paused", From: "/spec/suspended",
		},
	}

	for name, op := range tests {
		t.Run(name, func(t *testing.T) {
			marker, detail := opTextParts(op)
			if got := marker + detail; got != opText(op, 0) {
				t.Errorf("the halves do not recompose.\nwant %q\ngot  %q", opText(op, 0), got)
			}
			if marker != glyph(op.Type)+" " {
				t.Errorf("the marker is not the operation's glyph and a space: %q", marker)
			}
			if detail == "" {
				t.Error("the detail half is empty, so the whole line would stay at full intensity")
			}
		})
	}
}

// TestTheGlyphIsTheWholeMarkerForEveryOperationThisProjectRecords.
//
// The marker is left at full intensity inside a dimmed line because +, - and ~
// are how a reader scans a block for the kind of change in it. That only works
// while the marker *is* the glyph: a marker that had swallowed the path's first
// characters would leave the reader a bright fragment of a field name to scan
// instead, which is worse than dimming the line whole.
func TestTheGlyphIsTheWholeMarkerForEveryOperationThisProjectRecords(t *testing.T) {
	for opType, want := range map[string]string{
		OpAdd:     glyphAdd,
		OpRemove:  glyphRemove,
		OpReplace: glyphReplace,
	} {
		marker, _ := opTextParts(Op{Type: opType, Path: "/spec/replicas", Value: json.RawMessage(`5`)})
		if marker != want+" " {
			t.Errorf("%s renders as %q, not %q", opType, marker, want+" ")
		}
	}
}
