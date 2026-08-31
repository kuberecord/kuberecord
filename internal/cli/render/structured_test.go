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

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The envelope writer itself: the shapes it produces, and the mistakes it
// refuses.
//
// The commands' own golden files cover what a real answer looks like. What is
// here is the half a golden file cannot show — that an empty answer is an empty
// list rather than a null, that a still-open interval survives the round trip as
// a null rather than as a zero timestamp, and that the writer says so when it is
// misused rather than producing a document nothing can parse.

// testHead is a minimal envelope head.
func testHead(kind string) render.EnvelopeHead {
	return render.EnvelopeHead{
		APIVersion: render.EnvelopeAPIVersion,
		Kind:       kind,
		Metadata: render.EnvelopeMetadata{
			ClusterID: "prod-eu-1",
			Backend:   "clickhouse",
			Coverage: render.CoverageReport{
				Available: true,
				Summary:   "none recorded for this scope",
				Intervals: []query.ScopeInterval{},
			},
		},
	}
}

// TestEmptyAnswerIsAnEmptyList is the property a consumer's first `jq` depends
// on.
//
// `.items[]` over a null fails; over an empty list it yields nothing, which is
// what "no changes" should do to a pipeline. The whole-document formats are the
// ones at risk, because a nil slice is what an unpopulated envelope holds.
func TestEmptyAnswerIsAnEmptyList(t *testing.T) {
	for _, format := range []render.StructuredFormat{render.StructuredJSON, render.StructuredYAML} {
		t.Run(string(format), func(t *testing.T) {
			var out bytes.Buffer
			stream, err := render.NewStream(&out, format, testHead(render.KindTimeline))
			if err != nil {
				t.Fatalf("NewStream: %v", err)
			}
			if err := stream.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !strings.Contains(out.String(), "items") {
				t.Fatalf("the envelope carries no items key:\n%s", out.String())
			}
			if strings.Contains(out.String(), "null") {
				t.Errorf("an empty answer serialized a null, which breaks `.items[]`:\n%s", out.String())
			}
		})
	}
}

// TestJSONLWritesTheHeadBeforeAnyItem is what makes the streaming form usable by
// a consumer reading it as it arrives.
func TestJSONLWritesTheHeadBeforeAnyItem(t *testing.T) {
	var out bytes.Buffer
	stream, err := render.NewStream(&out, render.StructuredJSONL, testHead(render.KindCoverage))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// The head is on the wire before anything has been written to the stream,
	// which is the whole distinction between this format and the other two.
	if lines := strings.Count(out.String(), "\n"); lines != 1 {
		t.Fatalf("%d lines were written before the first item, want 1", lines)
	}

	stop := mustInstant("2026-08-11T17:31:22Z")
	for _, interval := range []query.ScopeInterval{
		{APIGroup: "apps", Kind: "Deployment", RuleRef: "ClusterStreamRule/all", From: mustInstant(
			"2026-07-02T09:14:00Z")},
		{Kind: "ConfigMap", Namespace: "payments", From: mustInstant("2026-07-02T09:14:00Z"), To: &stop},
	} {
		if err := stream.Write(interval); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("%d lines were written, want 3 (a head and two items):\n%s", len(lines), out.String())
	}

	// A still-open interval must survive as a null rather than as a zero
	// timestamp: the contract makes To a pointer precisely so the two cannot be
	// confused, and a serialization that lost the distinction would undo that.
	var open map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &open); err != nil {
		t.Fatalf("the first item is not valid JSON: %v\n%s", err, lines[1])
	}
	if open["to"] != nil {
		t.Errorf("a still-open interval's `to` is %#v, want null", open["to"])
	}
}

// TestStreamRefusesMisuse covers the writer's own error paths.
//
// Each of them would otherwise produce a document nothing can parse — a second
// envelope appended to the first, or an item after the closing brace — and a
// silent corruption of structured output is the failure this whole file exists to
// prevent.
func TestStreamRefusesMisuse(t *testing.T) {
	t.Run("an unknown serialization", func(t *testing.T) {
		var out bytes.Buffer
		if _, err := render.NewStream(&out, "toml", testHead(render.KindTimeline)); err == nil {
			t.Fatal("a serialization this package does not have was accepted")
		}
	})

	t.Run("a second close", func(t *testing.T) {
		var out bytes.Buffer
		stream, err := render.NewStream(&out, render.StructuredJSON, testHead(render.KindTimeline))
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := stream.Close(); err == nil {
			t.Error("a second Close was accepted, which would append a second document to the first")
		}
	})

	t.Run("an item after the close", func(t *testing.T) {
		var out bytes.Buffer
		stream, err := render.NewStream(&out, render.StructuredJSON, testHead(render.KindTimeline))
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := stream.Write(query.Change{}); err == nil {
			t.Error("an item was accepted after the envelope had been closed")
		}
	})
}

// TestHunksCarryBothPathSpellings pins the two grammars a Diff item speaks.
//
// The dotted path is the one --field accepts and the table prints; the pointer is
// the one a JSON Patch library takes. Both are carried because a consumer reading
// a path out of structured output should be able to paste it back into a query
// *and* feed it to a patch library, without learning a conversion.
func TestHunksCarryBothPathSpellings(t *testing.T) {
	hunks := render.Hunks([]render.Op{
		{
			Type: render.OpReplace,
			Path: "/metadata/annotations/deployment.kubernetes.io~1revision",
			// A replay established the previous value, so it is an answer.
			Old: "1", OldKnown: true, Value: json.RawMessage(`"2"`),
		},
		{
			Type: render.OpAdd, Path: "/spec/paused", Value: json.RawMessage("true"),
		},
	})

	if len(hunks) != 2 {
		t.Fatalf("%d hunks, want 2", len(hunks))
	}
	if want := "metadata.annotations.deployment.kubernetes.io/revision"; hunks[0].Path != want {
		t.Errorf("the dotted path is %q, want %q: RFC 6901's ~1 escape must be undone before the "+
			"path is joined with dots", hunks[0].Path, want)
	}
	if want := "/metadata/annotations/deployment.kubernetes.io~1revision"; hunks[0].Pointer != want {
		t.Errorf("the pointer is %q, want it exactly as recorded (%q)", hunks[0].Pointer, want)
	}
	if hunks[1].OldKnown {
		t.Error("an added field reports a known prior value; nothing was there to be destroyed")
	}
}

// TestEmptyHunksAreAListNotANull keeps a row with no patch readable.
//
// A first sighting, a snapshot and a deletion all carry no operations, and they
// are ordinary rows rather than edge cases — `.hunks[]` over them must yield
// nothing rather than fail.
func TestEmptyHunksAreAListNotANull(t *testing.T) {
	encoded, err := json.Marshal(render.DiffItem{Hunks: render.Hunks(nil)})
	if err != nil {
		t.Fatalf("encoding a patchless item: %v", err)
	}
	if !strings.Contains(string(encoded), `"hunks":[]`) {
		t.Errorf("a patchless item's hunks are not an empty list:\n%s", encoded)
	}
}

// TestOnlyAReconstructionIsMarkedAsOne keeps the marker meaningful.
//
// metadata.reconstruction says a document was assembled from recorded history
// rather than read back whole, and that is only true of a KindObject envelope.
// A Timeline carrying the key — even as a null — would teach a consumer to test
// it for null rather than for presence, and the first backend to return a null
// coverage report is a reminder of how that ends. The honest spelling of "there
// is no reconstruction in this answer" is no key at all.
func TestOnlyAReconstructionIsMarkedAsOne(t *testing.T) {
	for _, kind := range []string{
		render.KindTimeline, render.KindDiff, render.KindCoverage, render.KindBlame,
	} {
		t.Run(kind, func(t *testing.T) {
			var out bytes.Buffer
			stream, err := render.NewStream(&out, render.StructuredJSON, testHead(kind))
			if err != nil {
				t.Fatalf("NewStream: %v", err)
			}
			if err := stream.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			var decoded map[string]any
			if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
				t.Fatalf("the envelope is not valid JSON: %v\n%s", err, out.String())
			}
			metadata, ok := decoded["metadata"].(map[string]any)
			if !ok {
				t.Fatalf("the envelope carries no metadata: %#v", decoded)
			}
			if _, present := metadata["reconstruction"]; present {
				t.Errorf("a %s envelope carries metadata.reconstruction, which says its items were "+
					"assembled rather than recorded: %#v", kind, metadata)
			}
		})
	}
}
