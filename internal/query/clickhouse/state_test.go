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

package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The instants the reconstruction fixtures record at.
const (
	stateAdded      = 0
	statePatch      = time.Minute
	stateCheckpoint = 2 * time.Minute
	statePastCP     = 3 * time.Minute
)

// checkpointFixture walks one incarnation through a first sighting, a patch, a
// checkpoint and a final patch.
//
// The checkpoint's own diff **appends to an array**, and that is the whole design
// of the fixture. A checkpoint carries both the patch and the state that patch
// produced, so applying it a second time must be *visible*: an append run twice
// leaves a duplicate element, whereas a replace run twice is indistinguishable
// from a replace run once and would let a double-applying backend pass every
// assertion here.
func checkpointFixture() conformance.History {
	return conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(stateAdded, query.EventAdded, uidA, []string{actorKubectl}),
			`{"spec":{"replicas":1,"history":["r1"]}}`),
		withDiff(fixtureRow(statePatch, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":2}]`),
		withDiff(withData(fixtureRow(stateCheckpoint, query.EventCheckpoint, uidA, []string{actorController}),
			`{"spec":{"replicas":2,"history":["r1","r2"]}}`),
			`[{"op":"add","path":"/spec/history/-","value":"r2"}]`),
		withDiff(fixtureRow(statePastCP, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":5}]`),
	}}
}

// TestCheckpointDiffIsNotAppliedOverItsOwnData is the trap docs/SCHEMA.md names
// explicitly, given a test of its own because the failure is quiet.
//
// A Checkpoint's data is the state *after* its diff. A replay that based itself on
// the data and then applied the diff anyway would produce a document the object
// was never in — and for a replace op the mistake is invisible, which is why this
// fixture's checkpoint patch appends.
func TestCheckpointDiffIsNotAppliedOverItsOwnData(t *testing.T) {
	engine, _ := seededEngine(t, checkpointFixture())

	got, err := engine.StateAt(context.Background(), testRef(), after(stateCheckpoint), "")
	if err != nil {
		t.Fatalf("StateAt at the checkpoint: %v", err)
	}

	history := historyOf(t, got.Object)
	if len(history) != 2 {
		t.Fatalf("the reconstructed history array is %v, want two elements. A third is the checkpoint's "+
			"own append replayed on top of the state it had already produced", history)
	}
	if got.BaseEvent != query.EventCheckpoint {
		t.Errorf("the replay based itself on a %q row, want the checkpoint — which is what caps the walk "+
			"back and what a reader judges the answer by", got.BaseEvent)
	}
	if !got.BaseTS.Equal(after(stateCheckpoint)) {
		t.Errorf("the replay reports a base at %s, want %s",
			got.BaseTS.Format(time.RFC3339Nano), after(stateCheckpoint).Format(time.RFC3339Nano))
	}
	if got.PatchesApplied != 0 {
		t.Errorf("the replay applied %d patches over a checkpoint that is its own base, want 0",
			got.PatchesApplied)
	}
}

// TestReplayResumesAfterACheckpoint: the row past the checkpoint is applied, and
// exactly once.
//
// The mirror of the test above. Skipping the base row's diff must not turn into
// skipping the *next* row's, which would leave the answer one change stale — the
// most plausible-looking way to be wrong about the past.
func TestReplayResumesAfterACheckpoint(t *testing.T) {
	engine, _ := seededEngine(t, checkpointFixture())

	got, err := engine.StateAt(context.Background(), testRef(), after(statePastCP), "")
	if err != nil {
		t.Fatalf("StateAt one patch past the checkpoint: %v", err)
	}
	if got.PatchesApplied != 1 {
		t.Errorf("the replay applied %d patches, want 1 (the one row after the checkpoint)",
			got.PatchesApplied)
	}
	if replicas := replicasOf(t, got.Object); replicas != 5 {
		t.Errorf("the reconstructed object has %d replicas, want 5", replicas)
	}
	if len(historyOf(t, got.Object)) != 2 {
		t.Errorf("the history array is %v, want the checkpoint's two elements",
			historyOf(t, got.Object))
	}
}

// TestReconstructionBaseIsTheNewestFullStateRow: the base is the last data-bearing
// row at or before the instant, and everything before it is irrelevant.
func TestReconstructionBaseIsTheNewestFullStateRow(t *testing.T) {
	engine, _ := seededEngine(t, checkpointFixture())

	got, err := engine.StateAt(context.Background(), testRef(), after(statePatch), "")
	if err != nil {
		t.Fatalf("StateAt one patch past the first sighting: %v", err)
	}
	switch {
	case got.BaseEvent != query.EventAdded:
		t.Errorf("the replay based itself on a %q row, want the first sighting", got.BaseEvent)
	case got.PatchesApplied != 1:
		t.Errorf("the replay applied %d patches, want 1", got.PatchesApplied)
	case replicasOf(t, got.Object) != 2:
		t.Errorf("the reconstructed object has %d replicas, want 2", replicasOf(t, got.Object))
	}
}

// TestFullStateRowReplacesRatherThanPatches: a Modified that fell back to full
// state is a new base, not a patch source.
//
// The write path degrades to full state whenever a diff could not be produced,
// and a replay that looked only at the diff column would skip such a row's content
// entirely — silently dropping whatever change the diff had failed to describe.
func TestFullStateRowReplacesRatherThanPatches(t *testing.T) {
	history := conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(stateAdded, query.EventAdded, uidA, []string{actorKubectl}),
			`{"spec":{"replicas":1}}`),
		// A Modified carrying data and no diff: the documented degradation.
		withData(fixtureRow(statePatch, query.EventModified, uidA, []string{actorKubectl}),
			`{"spec":{"replicas":42}}`),
	}}
	engine, _ := seededEngine(t, history)

	got, err := engine.StateAt(context.Background(), testRef(), after(statePatch), "")
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if replicas := replicasOf(t, got.Object); replicas != 42 {
		t.Errorf("the reconstructed object has %d replicas, want 42; a full-state row replaces the "+
			"document rather than patching it", replicas)
	}
	if got.PatchesApplied != 0 {
		t.Errorf("the replay reports %d patches applied over a row that carried none", got.PatchesApplied)
	}
}

// TestReconstructionReportsTheDigestOfTheLastRowConsumed: the hash is the last
// row's, not the base's.
//
// It is what turns "the replay ran without errors" into "the replay produced the
// state that was recorded": a reader canonicalizes the object, hashes it, and the
// two must match. Reporting the base's digest would make that check pass only when
// nothing had been replayed.
func TestReconstructionReportsTheDigestOfTheLastRowConsumed(t *testing.T) {
	history := checkpointFixture()
	last := history.Rows[len(history.Rows)-1]
	last.Change.SHA256 = "deadbeef"
	history.Rows[len(history.Rows)-1] = last

	engine, _ := seededEngine(t, history)
	got, err := engine.StateAt(context.Background(), testRef(), after(statePastCP), "")
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got.SHA256 != "deadbeef" {
		t.Errorf("the reconstruction reports sha256 %q, want the last row consumed", got.SHA256)
	}
}

// TestStateAtRefusesAnInstantBeforeTheHistory: no state, said plainly.
//
// Substituting the nearest state is the tempting failure, because the nearest
// state is nearly always nearly right. It is also the one that ends an
// investigation with the wrong answer: an engineer asking what an object looked
// like before an incident and being shown what it looked like after one has been
// told something false, with nothing marking the approximation.
func TestStateAtRefusesAnInstantBeforeTheHistory(t *testing.T) {
	engine, _ := seededEngine(t, checkpointFixture())

	got, err := engine.StateAt(context.Background(), testRef(), testEpoch().Add(-time.Hour), "")
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Errorf("StateAt before the object existed returned %v, want query.ErrObjectNotFound", err)
	}
	if got != nil {
		t.Errorf("StateAt returned both an error and a reconstruction: %+v", got)
	}
}

// TestStateAtStopsAtADeletion: a deletion is terminal for its incarnation.
func TestStateAtStopsAtADeletion(t *testing.T) {
	engine, _ := seededEngine(t, incarnationFixture())

	// Pinned to the incarnation that was deleted, at an instant after its deletion.
	_, err := engine.StateAt(context.Background(), testRef(), after(4*time.Minute), uidA)
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Errorf("StateAt for a deleted incarnation returned %v, want query.ErrObjectNotFound", err)
	}
}

// TestStateAtPinnedToAnUnknownIncarnationFindsNothing: a UID that never wore this
// name has no state, and must not be answered with the state of the UID that did.
//
// A (namespace, name) pair may span several incarnations, and answering with a
// different one's state would reconstruct an object that never existed
// (Invariant 7).
func TestStateAtPinnedToAnUnknownIncarnationFindsNothing(t *testing.T) {
	engine, _ := seededEngine(t, checkpointFixture())

	_, err := engine.StateAt(context.Background(), testRef(), after(statePastCP), uidB)
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Errorf("StateAt pinned to an incarnation that never wore this name returned %v, want "+
			"query.ErrObjectNotFound", err)
	}
}

// TestStateAtDistinguishesAnAgedOutBaseFromAnAbsentObject: two facts, one
// sentinel, two messages.
//
// History holding rows but no full-state row means the base has aged out of the
// retention window, which is a different fact from the object never having
// existed. What a caller does about it is the same — report that no state can be
// produced — so the sentinel is shared, but the message has to say which, or a
// reader will go looking for an object that was there all along.
func TestStateAtDistinguishesAnAgedOutBaseFromAnAbsentObject(t *testing.T) {
	history := conformance.History{Rows: []conformance.Row{
		withDiff(fixtureRow(stateAdded, query.EventModified, uidA, []string{actorKubectl}),
			`[{"op":"replace","path":"/spec/replicas","value":2}]`),
	}}
	engine, _ := seededEngine(t, history)

	_, err := engine.StateAt(context.Background(), testRef(), after(statePatch), "")
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Fatalf("StateAt over a history with no full-state row returned %v, want query.ErrObjectNotFound",
			err)
	}
	if !strings.Contains(err.Error(), "retention window") {
		t.Errorf("the error reads %q; it must say the base predates the retention window rather than "+
			"letting a reader conclude the object was absent", err)
	}
}

// replicasOf reads spec.replicas out of a reconstructed object.
func replicasOf(t *testing.T, object map[string]any) int {
	t.Helper()
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the reconstructed object has no spec: %v", object)
	}
	replicas, ok := spec["replicas"].(float64)
	if !ok {
		t.Fatalf("spec.replicas is %T, not a number: %v", spec["replicas"], spec)
	}
	return int(replicas)
}

// historyOf reads spec.history out of a reconstructed object.
func historyOf(t *testing.T, object map[string]any) []any {
	t.Helper()
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the reconstructed object has no spec: %v", object)
	}
	entries, ok := spec["history"].([]any)
	if !ok {
		t.Fatalf("spec.history is %T, not an array: %v", spec["history"], spec)
	}
	return entries
}
