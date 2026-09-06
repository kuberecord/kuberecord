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

// The two columns the schema stores as strings and the envelope carries as
// structures.
//
// What is asserted here is the property the change exists for — a consumer reads
// a path out of `diff` or `data` with one parse rather than two — and the three
// ways it could have been given away: an empty column serialized as something a
// consumer has to branch on, a corrupt column flattened back into a string, or a
// patch that survived the round trip as valid JSON while ceasing to be the patch
// that was recorded.

// changeWithPatch is the fixture both halves of the round trip use: a first
// sighting carrying full state, then the modification whose patch is applied to
// it.
func changeWithPatch() (base, patched query.Change) {
	const state = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":3,"minReadySeconds":10}}`
	const patch = `[{"op":"replace","path":"/spec/replicas","value":5},` +
		`{"op":"remove","path":"/spec/minReadySeconds"},` +
		`{"op":"add","path":"/spec/paused","value":true}]`

	base = query.Change{
		TS: mustInstant("2026-08-28T14:02:58Z"), EventType: query.EventAdded,
		UID: "7c9e6679-7425-40de-944b-e07fc1f90ae7", ResourceVersion: "1001", Data: state,
	}
	patched = query.Change{
		TS: mustInstant("2026-08-28T14:05:02Z"), EventType: query.EventModified,
		UID: "7c9e6679-7425-40de-944b-e07fc1f90ae7", ResourceVersion: "1002", Diff: patch,
	}
	return base, patched
}

// TestRecordedColumnsAreStructuresNotStrings is the defect this shape fixes.
//
// A document containing JSON inside a string is not machine-readable without a
// second parse, which for a format whose purpose is to be machine-readable is a
// defect rather than a preference. The assertion is deliberately made against the
// decoded document rather than against its text: what a consumer gets from
// `.diff[0].op` is the property, and a substring check would pass on a cleverly
// escaped string.
func TestRecordedColumnsAreStructuresNotStrings(t *testing.T) {
	base, patched := changeWithPatch()

	var out bytes.Buffer
	stream, err := render.NewStream(&out, render.StructuredJSON, testHead(render.KindTimeline))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	for _, change := range []query.Change{base, patched} {
		item, itemErr := render.NewChangeItem(change)
		if itemErr != nil {
			t.Fatalf("NewChangeItem: %v", itemErr)
		}
		if err := stream.Write(item); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var decoded struct {
		Items []struct {
			Data map[string]any   `json:"data"`
			Diff []map[string]any `json:"diff"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("the envelope did not decode with data as an object and diff as an array: %v\n%s",
			err, out.String())
	}
	if len(decoded.Items) != 2 {
		t.Fatalf("%d items, want 2", len(decoded.Items))
	}
	if got := decoded.Items[0].Data["kind"]; got != "Deployment" {
		t.Errorf("`.items[0].data.kind` is %#v, want \"Deployment\": a full-state row's object must "+
			"be reachable without a second parse", got)
	}
	if got := len(decoded.Items[1].Diff); got != 3 {
		t.Fatalf("`.items[1].diff` holds %d operations, want 3", got)
	}
	if got := decoded.Items[1].Diff[0]["path"]; got != "/spec/replicas" {
		t.Errorf("`.items[1].diff[0].path` is %#v, want \"/spec/replicas\"", got)
	}
}

// TestAbsentColumnsAreEmptyStructures pins the shape of the two ordinary
// absences.
//
// A deletion carries neither column and a first sighting carries no patch. Both
// are ordinary rows rather than edge cases, so `.diff[]` must yield nothing and
// `.data.spec` must be absent rather than fail — and neither key may be dropped,
// because a consumer branching on presence would be doing so to learn something
// the value already says.
func TestAbsentColumnsAreEmptyStructures(t *testing.T) {
	item, err := render.NewChangeItem(query.Change{
		TS: mustInstant("2026-08-28T14:11:00Z"), EventType: query.EventDeleted,
		UID: "7c9e6679-7425-40de-944b-e07fc1f90ae7",
	})
	if err != nil {
		t.Fatalf("NewChangeItem: %v", err)
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("encoding a deletion: %v", err)
	}
	for _, want := range []string{`"data":{}`, `"diff":[]`} {
		if !strings.Contains(string(encoded), want) {
			t.Errorf("a deletion does not carry %s:\n%s", want, encoded)
		}
	}

	// And in YAML, which is the syntax the two absences are read in most often.
	document, err := yaml.Marshal(item)
	if err != nil {
		t.Fatalf("encoding a deletion as YAML: %v", err)
	}
	for _, want := range []string{"data: {}", "diff: []"} {
		if !strings.Contains(string(document), want) {
			t.Errorf("a deletion does not carry %q in YAML:\n%s", want, document)
		}
	}
}

// TestACorruptColumnIsAFindingNotAFallback is the rule that makes the parse safe
// to rely on.
//
// A stored column that will not parse is corrupt evidence. Emitting the raw
// string in its place would hide exactly the corruption an audit tool exists to
// surface, and emitting an empty structure would say the change touched nothing,
// which is a stronger lie than the one it replaced. So the row is named — by ts
// and by uid, the two fields that identify it — and nothing is emitted for it.
func TestACorruptColumnIsAFindingNotAFallback(t *testing.T) {
	const marker = "CORRUPTION-MARKER"
	ts := mustInstant("2026-08-28T14:05:02Z")
	const uid = "7c9e6679-7425-40de-944b-e07fc1f90ae7"

	for _, tc := range []struct {
		name    string
		change  query.Change
		wantFor string
	}{
		{
			name:    "a patch that is not JSON at all",
			change:  query.Change{TS: ts, UID: uid, Diff: `[{"op":"replace"` + marker},
			wantFor: "diff",
		},
		{
			name:    "a patch that is JSON of the wrong shape",
			change:  query.Change{TS: ts, UID: uid, Diff: `{"op":"replace","path":"/` + marker + `"}`},
			wantFor: "diff",
		},
		{
			name:    "a state that is not JSON at all",
			change:  query.Change{TS: ts, UID: uid, Data: `{"kind":"Deployment"` + marker},
			wantFor: "data",
		},
		{
			name:    "a state that is JSON of the wrong shape",
			change:  query.Change{TS: ts, UID: uid, Data: `["` + marker + `"]`},
			wantFor: "data",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item, err := render.NewChangeItem(tc.change)
			if err == nil {
				encoded, _ := json.Marshal(item)
				t.Fatalf("a corrupt %s column was accepted and emitted as:\n%s", tc.wantFor, encoded)
			}

			// The row has to be identifiable, or the finding is a rumour: an
			// engineer reading it must be able to go to the backend and look at the
			// value themselves.
			for _, want := range []string{ts.Format(time.RFC3339Nano), uid, tc.wantFor} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the finding does not name %q, so the row cannot be located: %v", want, err)
				}
			}
			// And the stored value must not be in it. Printing it is the fallback
			// this refusal exists to avoid, one stream over.
			if strings.Contains(err.Error(), marker) {
				t.Errorf("the finding quotes the recorded value back, which is the fallback it "+
					"exists to refuse: %v", err)
			}
		})
	}
}

// TestACorruptRowIsNamedEvenWithoutAUID keeps the message honest about the one
// identifier it may not have.
//
// A row carrying no UID is a real state rather than a defect, and rendering the
// blank as `uid ""` would read as a quoting accident in a message whose whole job
// is to be believed.
func TestACorruptRowIsNamedEvenWithoutAUID(t *testing.T) {
	_, err := render.NewChangeItem(query.Change{
		TS: mustInstant("2026-08-28T14:05:02Z"), Diff: "not json",
	})
	if err == nil {
		t.Fatal("a corrupt patch was accepted")
	}
	if !strings.Contains(err.Error(), "no uid recorded") {
		t.Errorf("a row with no UID is not described as such: %v", err)
	}
}

// TestTheEmittedPatchIsStillThePatchThatWasRecorded is the round trip.
//
// Producing valid JSON is not the property that matters; producing the *same*
// patch is. So the emitted array is serialized back and replayed over the same
// base through the same procedure a reconstruction uses, and the two states must
// be identical. A parse that reordered operations, coerced a number or dropped an
// entry would pass every assertion above and fail this one.
func TestTheEmittedPatchIsStillThePatchThatWasRecorded(t *testing.T) {
	base, patched := changeWithPatch()

	item, err := render.NewChangeItem(patched)
	if err != nil {
		t.Fatalf("NewChangeItem: %v", err)
	}
	emitted, err := json.Marshal(item.Diff)
	if err != nil {
		t.Fatalf("re-serializing the emitted patch: %v", err)
	}

	history := []query.ReplayRow{
		{TS: base.TS, EventType: base.EventType, Data: base.Data},
		{TS: patched.TS, EventType: patched.EventType, Diff: patched.Diff},
	}
	recorded, err := query.Replay(history, query.BaseRow(history))
	if err != nil {
		t.Fatalf("replaying the recorded patch: %v", err)
	}

	// The same history with the emitted array standing in for the stored string.
	history[1].Diff = string(emitted)
	roundTripped, err := query.Replay(history, query.BaseRow(history))
	if err != nil {
		t.Fatalf("replaying the emitted patch: %v", err)
	}

	want, err := json.Marshal(recorded.Object)
	if err != nil {
		t.Fatalf("encoding the recorded state: %v", err)
	}
	got, err := json.Marshal(roundTripped.Object)
	if err != nil {
		t.Fatalf("encoding the round-tripped state: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("replaying the emitted patch produced a different state.\nrecorded: %s\nemitted:  %s",
			want, got)
	}
	if roundTripped.PatchesApplied != 1 {
		t.Errorf("%d patches were applied, want 1: the emitted array must still be a patch",
			roundTripped.PatchesApplied)
	}
}
