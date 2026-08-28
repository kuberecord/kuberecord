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

package conformance

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

const (
	propReconstructBase       = "Reconstruction/Base"
	propReconstructCheckpoint = "Reconstruction/CheckpointNotDoubleApplied"
	propReconstructFidelity   = "Reconstruction/Fidelity"
	propReconstructPreHistory = "Reconstruction/BeforeHistory"
)

// Reconstruction is not capability-gated, and the omission is deliberate rather
// than an oversight. query.Capabilities carries no flag for "cannot reconstruct
// state", so a backend that could not would have no way to declare it — and an
// undeclared omission is precisely what a conformance gate must not certify. Every
// engine reconstructs; a backend for which that is genuinely impossible is a
// contract change and not a harness setting.

// stateAt reconstructs and fails the property if the engine could not.
func stateAt(t conformanceT, h Harness, after time.Duration, uid string) *query.Reconstruction {
	t.Helper()
	got, err := h.Engine.StateAt(context.Background(), fixtureRef(), at(after), uid)
	if err != nil {
		t.Fatalf("conformance: StateAt(%s, uid=%q) returned %v; the seeded history holds a full-state row "+
			"before that instant", at(after).UTC().Format(time.RFC3339Nano), uid, err)
	}
	if got == nil {
		t.Fatalf("conformance: StateAt(%s) returned a nil Reconstruction and a nil error",
			at(after).UTC().Format(time.RFC3339Nano))
	}
	return got
}

// assertReconstruction checks a reconstruction against the state it should have
// produced and the evidence it should have reported for how.
//
// The provenance fields are asserted as hard as the document is, because they are
// not diagnostics. A reconstruction is an assertion about the past somebody may act
// on, and a state assembled from a base an hour old and two patches invites a
// different amount of confidence than the same state assembled from a base three
// months old and four hundred. A backend that reported the wrong base has produced
// the right answer for a reason the reader cannot check.
func assertReconstruction(
	t conformanceT, got *query.Reconstruction,
	wantState string, wantBaseAfter time.Duration, wantBaseEvent string, wantPatches int, when string,
) {
	t.Helper()

	gotDoc, err := canonicalValue(got.Object)
	if err != nil {
		t.Fatalf("conformance: %s: the reconstructed object could not be re-encoded: %v", when, err)
	}
	if want := mustCanonicalJSON(wantState); !bytes.Equal(gotDoc, want) {
		t.Errorf("conformance: %s: the reconstructed state is not the recorded one.\ngot:  %s\nwant: %s",
			when, gotDoc, want)
	}
	if wantBase := at(wantBaseAfter); !got.BaseTS.Equal(wantBase) {
		t.Errorf("conformance: %s: the replay reports it based itself on the row at %s, want %s. The base "+
			"is what a reader judges the answer by, so reporting the wrong one produces a right answer "+
			"nobody can check", when, got.BaseTS.UTC().Format(time.RFC3339Nano),
			wantBase.UTC().Format(time.RFC3339Nano))
	}
	if got.BaseEvent != wantBaseEvent {
		t.Errorf("conformance: %s: the replay reports a base event of %q, want %q",
			when, got.BaseEvent, wantBaseEvent)
	}
	if got.PatchesApplied != wantPatches {
		t.Errorf("conformance: %s: the replay reports %d patches applied, want %d",
			when, got.PatchesApplied, wantPatches)
	}
}

// reconstructionBase: the replay starts from the newest data-bearing row at or
// before the instant, and applies every patch after it in order.
//
// The two instants it asks about have different bases on purpose. One lands after
// two patches over the original first sighting; the other lands on a checkpoint,
// which is what caps the walk — a reconstruction never applies more patches than the
// checkpoint cadence allows, and a backend that ignored checkpoints would replay the
// object's entire history to answer the same question.
func reconstructionBase(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, reconstructionHistory())

	assertReconstruction(t, stateAt(t, h, reconSecondPatch, ""),
		reconState2, reconAdded, query.EventAdded, 2,
		"state at an instant two patches past the first sighting")

	assertReconstruction(t, stateAt(t, h, reconCheckpoint, ""),
		reconState3, reconCheckpoint, query.EventCheckpoint, 0,
		"state at the instant of a checkpoint, which is its own base")

	// The uid argument pins the reconstruction to one incarnation, and an empty one
	// means the newest alive at or before the instant — never a blend of two. The
	// fixture records a single incarnation, so pinning it must not change the
	// answer, and pinning one that never wore this name must produce no state
	// rather than the state of the one that did.
	assertReconstruction(t, stateAt(t, h, reconLastPatch, uidA),
		reconState4, reconCheckpoint, query.EventCheckpoint, 1,
		"state pinned to the incarnation the history actually records")

	_, err := h.Engine.StateAt(context.Background(), fixtureRef(), at(reconLastPatch), uidB)
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Errorf("conformance: StateAt pinned to incarnation %s — which never wore this name — returned "+
			"%v, want query.ErrObjectNotFound. A (namespace, name) pair may span several UIDs, and "+
			"answering with a different incarnation's state would reconstruct an object that never "+
			"existed (Invariant 7)", uidB, err)
	}
}

// reconstructionCheckpointNotDoubleApplied: a checkpoint's own patch describes the
// transition its data already reflects, and must not be applied on top of it.
//
// This is the trap docs/SCHEMA.md names explicitly and it is worth its own property
// because the failure is quiet. A double-applied replace is indistinguishable from a
// single one, so a backend can get this wrong and pass every other assertion here;
// the fixture's checkpoint patch appends to an array precisely so the mistake shows
// up as an extra element rather than as nothing at all.
func reconstructionCheckpointNotDoubleApplied(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, reconstructionHistory())

	assertReconstruction(t, stateAt(t, h, reconCheckpoint, ""),
		reconState3, reconCheckpoint, query.EventCheckpoint, 0,
		"state at a checkpoint, whose data is the state after its own diff")

	assertReconstruction(t, stateAt(t, h, reconLastPatch, ""),
		reconState4, reconCheckpoint, query.EventCheckpoint, 1,
		"state one patch past a checkpoint (the checkpoint's own diff must not be replayed)")
}

// reconstructionFidelity: the reconstructed state is byte-identical to the state
// that was recorded, and hashing it reproduces the digest history holds.
//
// The hash check is the one that turns "the replay ran without errors" into "the
// replay produced the right state". A mismatch is a chain-of-custody finding rather
// than a rounding error: it means the history and the replay disagree about what
// this object looked like, and an audit trail whose two accounts of the same instant
// differ is not an audit trail.
func reconstructionFidelity(t conformanceT, h Harness) {
	t.Helper()
	history := reconstructionHistory()
	seed(t, h, history)

	got := stateAt(t, h, reconLastPatch, "")

	gotDoc, err := canonicalValue(got.Object)
	if err != nil {
		t.Fatalf("conformance: the reconstructed object could not be re-encoded: %v", err)
	}
	if want := mustCanonicalJSON(reconState4); !bytes.Equal(gotDoc, want) {
		t.Errorf("conformance: the reconstruction of the final state is not byte-identical to the state "+
			"that was recorded.\ngot:  %s\nwant: %s", gotDoc, want)
	}

	last := history.Rows[len(history.Rows)-1].Change
	if got.SHA256 != last.SHA256 {
		t.Errorf("conformance: the reconstruction reports sha256 %q, but the last row consumed recorded "+
			"%q; the digest is the digest of the row the replay finished on", got.SHA256, last.SHA256)
	}
	if recomputed := sha256Hex(gotDoc); recomputed != got.SHA256 {
		t.Errorf("conformance: hashing the reconstructed state gives %q but the reconstruction reports "+
			"%q. History and replay disagree about what this object looked like, which is a "+
			"chain-of-custody finding and not a rounding error", recomputed, got.SHA256)
	}
}

// reconstructionBeforeHistory: an instant before anything was recorded has no state,
// and the engine says so rather than substituting a neighbouring instant's.
//
// Substituting is the tempting failure, because the nearest state is nearly always
// nearly right. It is also the one that ends an investigation with the wrong answer:
// an engineer asking what an object looked like before an incident and being shown
// what it looked like after one has been told something false with no indication
// that anything was approximated.
func reconstructionBeforeHistory(t conformanceT, h Harness) {
	t.Helper()
	seed(t, h, reconstructionHistory())

	before := suiteEpoch.Add(-time.Minute)
	got, err := h.Engine.StateAt(context.Background(), fixtureRef(), before, "")
	switch {
	case err == nil:
		t.Fatalf("conformance: StateAt(%s) returned a state, but the earliest recorded row for %s/%s is "+
			"at %s. A caller must be told no state can be produced, never handed a neighbouring "+
			"instant's", before.UTC().Format(time.RFC3339Nano), fixtureNS, fixtureName,
			suiteEpoch.UTC().Format(time.RFC3339Nano))
	case !errors.Is(err, query.ErrObjectNotFound):
		t.Errorf("conformance: StateAt before the object existed returned %v, which is not "+
			"query.ErrObjectNotFound. The sentinel is what lets a caller give this its own exit code "+
			"and its own message instead of matching on error text", err)
	case got != nil:
		t.Errorf("conformance: StateAt returned both an error and a non-nil Reconstruction; a caller " +
			"checking the error first would be right to ignore the value, and one checking the value " +
			"first would render a state the engine has just said it cannot produce")
	}
}
