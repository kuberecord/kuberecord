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

package objectsource

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// spreadHistory is one incarnation whose changes are spread across three days, so a
// reconstruction has to walk back through partitions rather than find everything in
// the one the instant lands in.
//
// The base is the oldest row and carries full state; everything after it is a patch.
// That is the shape a reconstruction is hardest on: an engine that stopped walking at
// the first partition holding anything would find patches with nothing to apply them
// to and report the object as absent.
func spreadHistory() conformance.History {
	base := testEpoch().Add(-48 * time.Hour)
	row := func(at time.Time, event, data, diff string) conformance.Row {
		return conformance.Row{
			Ref: testRef(),
			Change: query.Change{
				TS: at, EventType: event, UID: uidOld, ResourceVersion: "1",
				APIVersion: "apps/v1", Actors: []string{actorKubectl},
				Data: data, Diff: diff, SHA256: strings.Repeat("a", 64),
			},
		}
	}
	return conformance.History{Rows: []conformance.Row{
		row(base, query.EventAdded, `{"kind":"Deployment","spec":{"replicas":1,"paused":false}}`, ""),
		row(base.Add(24*time.Hour), query.EventModified, "",
			`[{"op":"replace","path":"/spec/replicas","value":4}]`),
		row(base.Add(47*time.Hour), query.EventModified, "",
			`[{"op":"replace","path":"/spec/paused","value":true}]`),
	}}
}

// TestStateAtWalksBackThroughPartitions: a reconstruction finds a base in an older
// partition and replays everything after it.
//
// The two instants asked about have different answers on purpose. One lands between
// two patches and must not include the later one; the other lands after both. An
// engine that read only the partition the instant falls in would answer the first
// question with "no state" and the second with the state from a day earlier.
func TestStateAtWalksBackThroughPartitions(t *testing.T) {
	t.Parallel()

	history := spreadHistory()
	engine, spy := engineOver(t, history, Options{Prefix: "audit"})
	base := history.Rows[0].Change.TS

	tests := []struct {
		name         string
		at           time.Time
		wantReplicas float64
		wantPaused   bool
		wantPatches  int
	}{
		{
			name: "between the two patches", at: base.Add(30 * time.Hour),
			wantReplicas: 4, wantPaused: false, wantPatches: 1,
		},
		{
			name: "after both patches", at: base.Add(47 * time.Hour),
			wantReplicas: 4, wantPaused: true, wantPatches: 2,
		},
		{
			name: "at the base itself", at: base,
			wantReplicas: 1, wantPaused: false, wantPatches: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := engine.StateAt(context.Background(), testRef(), tc.at, "")
			if err != nil {
				t.Fatalf("StateAt(%s): %v", formatInstant(tc.at), err)
			}
			spec, ok := got.Object["spec"].(map[string]any)
			if !ok {
				t.Fatalf("the reconstruction has no spec: %v", got.Object)
			}
			switch {
			case spec["replicas"] != tc.wantReplicas:
				t.Errorf("spec.replicas = %v, want %v", spec["replicas"], tc.wantReplicas)
			case spec["paused"] != tc.wantPaused:
				t.Errorf("spec.paused = %v, want %v", spec["paused"], tc.wantPaused)
			case got.PatchesApplied != tc.wantPatches:
				t.Errorf("%d patches applied, want %d", got.PatchesApplied, tc.wantPatches)
			case !got.BaseTS.Equal(base):
				t.Errorf("the replay based itself on %s, want %s. The base is what a reader judges the "+
					"answer by, so reporting the wrong one produces a right answer nobody can check",
					formatInstant(got.BaseTS), formatInstant(base))
			case got.BaseEvent != query.EventAdded:
				t.Errorf("base event = %q, want %q", got.BaseEvent, query.EventAdded)
			}
		})
	}

	// The walk stops once the base is settled rather than running the whole lookback:
	// three days of history must not cost thirty days of listings.
	if listed := len(spy.listed()); listed > 60 {
		t.Errorf("the backward walk listed %d prefixes for a three-day history; it is meant to stop "+
			"once it holds a base and has read one object span below it", listed)
	}
}

// TestStateAtContinuesPastTheBase pins the rule that keeps a replay from stopping a
// patch short.
//
// An object's partition comes from its *first* record, so a change can sit in an
// earlier partition than its own hour — which means a newer full-state row, or a patch
// belonging between the base and the instant, can turn up after the base has already
// been found. The walk therefore continues one object span below the base's own
// partition before it settles.
//
// It is asserted against the predicate rather than against a hand-built archive because
// the fixture writer files each record under its own hour by construction: the case only
// arises from a rotation this fixture cannot produce, and the rule is what has to be
// right either way.
func TestStateAtContinuesPastTheBase(t *testing.T) {
	t.Parallel()

	local, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("opening a source: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })

	baseTS := time.Date(2026, 3, 14, 8, 30, 0, 0, time.UTC)
	history := []query.Change{
		{TS: baseTS, EventType: query.EventAdded, UID: uidOld, Data: `{"spec":{}}`},
	}

	tests := []struct {
		name       string
		span       time.Duration
		day        time.Time
		wantSettle bool
	}{
		{
			name: "the base's own day is not enough, whatever the span",
			span: NoObjectSpan, day: dayStart(baseTS), wantSettle: false,
		},
		{
			name: "one day below the base settles it with no widening",
			span: NoObjectSpan, day: dayStart(baseTS).AddDate(0, 0, -1), wantSettle: true,
		},
		{
			name: "an hour of widening is still inside the base's own day",
			span: time.Hour, day: dayStart(baseTS).AddDate(0, 0, -1), wantSettle: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := NewEngine(local, Options{ObjectSpan: tc.span})
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			if got := engine.baseIsSettled(tc.day, history, uidOld); got != tc.wantSettle {
				t.Errorf("after reading %s the walk settles = %t, want %t",
					tc.day.Format(dateLayout), got, tc.wantSettle)
			}
		})
	}

	// And with no base in hand it never settles, however far down the walk has got:
	// stopping there would report a retention problem as an absent object.
	engine, err := NewEngine(local, Options{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	patchOnly := []query.Change{{TS: baseTS, EventType: query.EventModified, UID: uidOld, Diff: "[]"}}
	if engine.baseIsSettled(dayStart(baseTS).AddDate(0, 0, -5), patchOnly, uidOld) {
		t.Error("the walk settled on a history holding no full-state row to replay from")
	}
}

// TestStateAtExhaustingTheLookbackSaysSo: a walk that ran out of lookback reports that,
// not that the object was absent.
//
// Substituting the wrong explanation is the tempting failure. An engineer told "this
// object was never recorded" stops looking; told "no full-state row within thirty
// days", they widen the window or reach for the other backend. The sentinel is shared
// because the caller's action is the same — produce no state — but the message has to
// distinguish them.
func TestStateAtExhaustingTheLookbackSaysSo(t *testing.T) {
	t.Parallel()

	history := spreadHistory()
	// A lookback far shorter than the history is old: the base is two days behind the
	// instant, and the engine is told to look back an hour.
	engine, _ := engineOver(t, history, Options{Prefix: "audit", StateLookback: time.Hour})

	got, err := engine.StateAt(context.Background(), testRef(), testEpoch(), "")
	if got != nil {
		t.Errorf("StateAt returned a state as well as %v; a caller checking the value first would "+
			"render a state the engine has just said it cannot produce", err)
	}
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Fatalf("StateAt = %v, want query.ErrObjectNotFound", err)
	}
	if !containsAll(err.Error(), "walks back", "1h0m0s") {
		t.Errorf("StateAt reported %q; it has to name the lookback it exhausted, or the message reads "+
			"as \"this object never existed\"", err)
	}
}

// TestStateAtWithoutAFullStateRowIsNotAnAbsentObject: history with patches but no base
// is reported as a base that has aged out.
//
// This is the other half of the message above, and the distinction is the same one:
// "we hold changes for this object but cannot rebuild it" is a retention finding, and
// reporting it as absence would have an engineer conclude the object never existed.
func TestStateAtWithoutAFullStateRowIsNotAnAbsentObject(t *testing.T) {
	t.Parallel()

	history := spreadHistory()
	// Drop the only full-state row, leaving the two patches.
	history.Rows = history.Rows[1:]
	engine, _ := engineOver(t, history, Options{Prefix: "audit"})

	_, err := engine.StateAt(context.Background(), testRef(), testEpoch(), "")
	if !errors.Is(err, query.ErrObjectNotFound) {
		t.Fatalf("StateAt = %v, want query.ErrObjectNotFound", err)
	}
	if !containsAll(err.Error(), "no full-state row", "rather than the object being absent") {
		t.Errorf("StateAt reported %q; it has to say the base aged out rather than that the object "+
			"was never there", err)
	}
}

// TestStateAtRequiresAnInstant: a zero instant is refused rather than read as "now".
//
// The zero time is the year 1, so a walk from it finds nothing and would answer "this
// object was never recorded" — a false statement produced by a caller's missing field.
func TestStateAtRequiresAnInstant(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, spreadHistory(), Options{Prefix: "audit"})
	if _, err := engine.StateAt(context.Background(), testRef(), time.Time{}, ""); err == nil {
		t.Error("StateAt accepted a zero instant")
	}
}

// TestStateAtReconstructionIsTheRecordedState: the state that comes back is what was
// recorded, byte for byte after canonicalization.
//
// The reconstruction goes through the contract's own replay, which is what this asserts
// indirectly: a package that reimplemented the procedure would be free to get the
// checkpoint rule wrong, and the fixture here would not notice — but the conformance
// suite's checkpoint property would. What is checked here is the plainer claim that the
// document is the document.
func TestStateAtReconstructionIsTheRecordedState(t *testing.T) {
	t.Parallel()

	history := spreadHistory()
	engine, _ := engineOver(t, history, Options{Prefix: "audit"})

	got, err := engine.StateAt(context.Background(), testRef(), testEpoch(), "")
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	want := map[string]any{
		"kind": "Deployment",
		"spec": map[string]any{"replicas": float64(4), "paused": true},
	}
	gotJSON, err := json.Marshal(got.Object)
	if err != nil {
		t.Fatalf("re-encoding the reconstruction: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("encoding the expectation: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("the reconstruction is\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}
