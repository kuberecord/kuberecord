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

package cli_test

import (
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The value on the left of the arrow, and the ways it can go missing.
//
// A recorded patch carries only the value it wrote — internal/pipeline records
// what wI2L/jsondiff emits, and that library's OldValue field is tagged
// `json:"-"` — so "2Gi → 512Mi" is reconstructed rather than read. These tests
// pin both halves of that: that the reconstruction is anchored once and walked
// forward, and that every way it can fail leaves the arrow off rather than
// putting a plausible wrong number in an audit timeline.

// TestPriorValuesAnchorOnAReconstructionWhenTheWindowHasNoBase covers the
// round-trip path: the rows shown start mid-history, so there is no full-state
// row among them to replay from.
func TestPriorValuesAnchorOnAReconstructionWhenTheWindowHasNoBase(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		state:        fixtureState,
	}

	request := defaultRequest()
	request.Limit = 2
	stdout, _, err := runTimeline(t, engine, request, render.Options{Full: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	// The oldest row shown is the three-operation patch, whose prior values can
	// only have come from the reconstruction.
	for _, want := range []string{"~ spec.replicas: 3 → 5", "- spec.minReadySeconds: 10"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q; the anchored replay did not establish it.\n%s", want, stdout)
		}
	}
	// And the row after it must have been replayed *forward* over that anchor
	// rather than re-fetched: the annotation was 1 at the anchor and is 2 after.
	if !strings.Contains(stdout, ": 1 → 2") {
		t.Errorf("the replay did not carry forward past the first row.\n%s", stdout)
	}
}

// TestPriorValuesSeedFromTheRowsWhenTheyHoldFullState is the cheap path: a
// timeline reaching back to a first sighting already holds the object's whole
// state, and asking the backend to reconstruct what it just handed over would be
// spending a query on arithmetic.
func TestPriorValuesSeedFromTheRowsWhenTheyHoldFullState(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		// No state at all: a reconstruction would fail, so the arrows below can
		// only have come from the Added row already in the result.
		state: "",
	}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stdout, "2Gi → 512Mi") {
		t.Errorf("the replay did not seed from the full-state row in the result.\n%s", stdout)
	}
	if strings.Contains(stderr, "prior values") {
		t.Errorf("a reconstruction was attempted when the rows already held one:\n%s", stderr)
	}
}

// TestPriorValuesDegradeWhenStateCannotBeReconstructed covers the two ways the
// anchor can fail, and the requirement that both are said out loud.
func TestPriorValuesDegradeWhenStateCannotBeReconstructed(t *testing.T) {
	tests := []struct {
		name     string
		stateErr error
		want     string
	}{
		{
			name:     "the backend cannot reconstruct at all",
			stateErr: query.ErrCapabilityUnsupported,
			want:     "this backend cannot reconstruct state",
		},
		{
			name:     "the base has aged out of the retention window",
			stateErr: query.ErrObjectNotFound,
			want:     "no full-state row survives before the oldest change shown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := &fakeEngine{
				caps:         clickHouseCapabilities(),
				changes:      checkoutHistory(),
				incarnations: checkoutIncarnations(),
				intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
				stateErr:     test.stateErr,
			}

			request := defaultRequest()
			request.Limit = 2
			stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if !strings.Contains(stderr, test.want) {
				t.Errorf("the degradation was not explained.\nwant a notice containing %q\ngot:\n%s",
					test.want, stderr)
			}
			// The header's coverage line carries an arrow of its own, so the
			// assertion names the transition that must not be there rather than
			// the glyph.
			if strings.Contains(stdout, "1 → 2") {
				t.Errorf("a prior value was rendered although none could be established:\n%s", stdout)
			}
			if !strings.Contains(stdout, "deployment.kubernetes.io/revision: 2") {
				t.Errorf("the new value was lost along with the prior one:\n%s", stdout)
			}
			// The new values are still exact, and must still be there.
			if !strings.Contains(stdout, "~3 ops") {
				t.Errorf("the rows themselves were lost along with their prior values:\n%s", stdout)
			}
		})
	}
}

// TestPriorValuesStopWhenAPatchWillNotApply covers a replay that starts and then
// disagrees with history.
//
// It stops and says where, rather than carrying a state the object was never in
// forward through every row after it.
func TestPriorValuesStopWhenAPatchWillNotApply(t *testing.T) {
	changes := []query.Change{
		{
			TS: at("2026-08-28T14:02:58.001Z"), EventType: query.EventAdded, UID: fixtureUID,
			Actors: []string{"kubectl-client-side-apply"}, Data: `{"spec":{"replicas":3}}`,
		},
		{
			TS: at("2026-08-28T14:03:11.482Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"kubectl-client-side-apply"},
			// Removing a member the state does not hold.
			Diff: `[{"op":"remove","path":"/spec/strategy/rollingUpdate"}]`,
		},
		{
			TS: at("2026-08-28T14:05:02.117Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"kube-controller-manager"},
			Diff:   `[{"op":"replace","path":"/spec/replicas","value":5}]`,
		},
	}
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      changes,
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	_, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stderr, "prior values stop at 2026-08-28T14:03:11Z") {
		t.Errorf("the replay did not report where it stopped:\n%s", stderr)
	}
}

// TestPriorValuesIgnoreMergedEvents is why the replay skips Kubernetes Event
// rows.
//
// Every field of such a row describes the Event object rather than the object
// whose timeline it was merged into, so seeding state from its data would splice
// two objects together and lose the arrow on the row after it.
func TestPriorValuesIgnoreMergedEvents(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		events: []query.Change{{
			// Between the first sighting and the change that needs its state.
			TS: at("2026-08-28T14:03:05.000Z"), EventType: query.EventKubernetes, UID: "e0",
			Data: `{"type":"Normal","reason":"ScalingReplicaSet","message":"Scaled up",
			        "source":{"component":"deployment-controller"}}`,
		}},
	}

	request := defaultRequest()
	request.WithEvents = true
	stdout, _, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stdout, "2Gi → 512Mi") {
		t.Errorf("an interleaved Event disturbed the replay of the object's own state.\n%s", stdout)
	}
}

// TestPriorValuesAreGroupedByIncarnation is Invariant 7 applied to the replay.
//
// The fixture gives the older incarnation a full-state row and the newer one
// none, so an ungrouped walk would seed the newer object's replay from the older
// object's state and report a prior value that belonged to a different object
// wearing the same name.
func TestPriorValuesAreGroupedByIncarnation(t *testing.T) {
	changes := []query.Change{
		{
			TS: at("2026-08-28T14:01:10.004Z"), EventType: query.EventAdded, UID: priorUID,
			Actors: []string{"kubectl-client-side-apply"}, Data: `{"spec":{"replicas":9}}`,
		},
		{
			TS: at("2026-08-28T14:03:11.482Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"kube-controller-manager"},
			Diff:   `[{"op":"replace","path":"/spec/replicas","value":5}]`,
		},
	}
	engine := &fakeEngine{
		caps:      clickHouseCapabilities(),
		changes:   changes,
		intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		incarnations: []query.Incarnation{
			{UID: priorUID, FirstSeen: at("2026-08-28T14:01:10.004Z"), LastSeen: at("2026-08-28T14:01:10.004Z")},
			{UID: fixtureUID, FirstSeen: at("2026-08-28T14:03:11.482Z"), LastSeen: at("2026-08-28T14:03:11.482Z")},
		},
		// The newer incarnation's own state, which only a grouped replay reaches.
		state: `{"spec":{"replicas":3}}`,
	}

	request := defaultRequest()
	request.AllIncarnations = true
	stdout, _, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if strings.Contains(stdout, "9 → 5") {
		t.Fatalf("the replay carried a deleted object's state into its replacement's history.\n%s", stdout)
	}
	if !strings.Contains(stdout, "3 → 5") {
		t.Errorf("the newer incarnation's own state was not used.\n%s", stdout)
	}
}

// TestBackendCloseIsJoinedRatherThanSwallowed is a small guarantee with a large
// consequence: the reason a command ended must survive the tidying up.
func TestBackendCloseIsJoinedRatherThanSwallowed(t *testing.T) {
	backend := &cli.Backend{Engine: &fakeEngine{caps: clickHouseCapabilities()}}
	if err := backend.Close(); err != nil {
		t.Fatalf("closing a backend with nothing to release: %v", err)
	}
}
