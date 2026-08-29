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
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// fakeEngine is a QueryEngine over fixed slices, and it is what every rendering
// test in this package runs against.
//
// A fake rather than a live backend for the reason the acceptance criteria give:
// what is under test here is the command — which incarnation it chooses, how it
// explains an absence, which notices it prints — and none of that is a property
// of ClickHouse or of an object archive. The backends have a conformance suite of
// their own for the half that is (internal/query/conformance).
//
// It answers the contract faithfully enough for those questions and no further.
// In particular it applies the predicates through query.MatchesActors and
// query.MatchesFieldPaths rather than re-deriving them, so a test that turns a
// filter on is exercising the same rule a real backend applies client-side.
type fakeEngine struct {
	caps query.Capabilities

	// changes is the object's whole recorded history, oldest first.
	changes []query.Change
	// events are the Kubernetes Events correlated to it, oldest first, already
	// stamped with query.EventKubernetes as an engine would stamp them on merge.
	events []query.Change
	// incarnations, intervals and state are what the three supporting calls
	// answer with.
	incarnations []query.Incarnation
	intervals    []query.ScopeInterval
	state        string

	// The failures a test injects, to drive the degradation paths that no
	// shipped backend currently takes.
	timelineErr     error
	incarnationsErr error
	coverageErr     error
	stateErr        error

	// queries records what was asked, so a test can assert the query the command
	// built rather than only the output it produced.
	queries []query.TimelineQuery
	// opened and closed count the iterators handed out and released, which is how
	// the drain's Close-on-every-path discipline is checked. A query that failed
	// hands out no iterator, so the two counters are not the same thing as the
	// number of queries.
	opened int
	closed int
}

func (f *fakeEngine) Capabilities() query.Capabilities { return f.caps }

func (f *fakeEngine) Close() error { return nil }

// Timeline selects, filters, orders and limits, in that order.
func (f *fakeEngine) Timeline(_ context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	f.queries = append(f.queries, q)
	if f.timelineErr != nil {
		return nil, f.timelineErr
	}
	if f.caps.TimeBoundRequired && q.From.IsZero() && q.To.IsZero() {
		return nil, query.ErrTimeBoundRequired
	}

	var selected []query.Change
	for _, change := range f.changes {
		switch {
		case !inWindow(change.TS, q.From, q.To):
		case q.UID != "" && change.UID != q.UID:
		case !query.MatchesActors(change, q.Actors, q.ExcludeActors):
		case !query.MatchesFieldPaths(change, q.FieldPaths):
		default:
			selected = append(selected, change)
		}
	}
	if q.IncludeEvents {
		for _, event := range f.events {
			if inWindow(event.TS, q.From, q.To) {
				selected = append(selected, event)
			}
		}
	}

	slices.SortStableFunc(selected, func(a, b query.Change) int { return a.TS.Compare(b.TS) })
	if q.Reverse {
		slices.Reverse(selected)
	}
	if q.Limit > 0 && len(selected) > q.Limit {
		selected = selected[:q.Limit]
	}
	f.opened++
	return &fakeIterator{changes: selected, engine: f}, nil
}

// StateAt hands back the one document the fixture holds, whatever instant is
// asked for.
//
// The command anchors its replay once, so one document is all a rendering test
// needs; a fake that indexed by instant would be asserting the anchor arithmetic
// twice, once here and once in the test that exists for it.
func (f *fakeEngine) StateAt(
	_ context.Context, _ query.ObjectRef, at time.Time, _ string,
) (*query.Reconstruction, error) {
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	if f.state == "" {
		return nil, query.ErrObjectNotFound
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(f.state), &object); err != nil {
		return nil, err
	}
	return &query.Reconstruction{Object: object, BaseTS: at, BaseEvent: query.EventAdded}, nil
}

func (f *fakeEngine) Coverage(_ context.Context, _ query.ScopeQuery) ([]query.ScopeInterval, error) {
	if f.coverageErr != nil {
		return nil, f.coverageErr
	}
	return f.intervals, nil
}

func (f *fakeEngine) Incarnations(
	_ context.Context, _ query.ObjectRef, _, _ time.Time,
) ([]query.Incarnation, error) {
	if f.incarnationsErr != nil {
		return nil, f.incarnationsErr
	}
	return f.incarnations, nil
}

// inWindow applies an inclusive window with the contract's reading of a zero
// bound.
func inWindow(ts, from, to time.Time) bool {
	if !from.IsZero() && ts.Before(from) {
		return false
	}
	return to.IsZero() || !ts.After(to)
}

// fakeIterator is a cursor over a slice that records having been closed.
type fakeIterator struct {
	changes []query.Change
	engine  *fakeEngine
	at      int
	done    bool
}

func (i *fakeIterator) Next() bool {
	if i.at >= len(i.changes) {
		return false
	}
	i.at++
	return true
}

func (i *fakeIterator) Change() query.Change { return i.changes[i.at-1] }

func (i *fakeIterator) Err() error { return nil }

// Close is idempotent and counted once, matching the contract's promise that
// calling it more than once is safe.
func (i *fakeIterator) Close() error {
	if !i.done {
		i.done = true
		i.engine.closed++
	}
	return nil
}

// assertDrained is the check that the command released what it opened.
//
// Skipping Close on an early return leaks driver rows against a real backend, and
// a leak is invisible in output — which is exactly the kind of defect a test has
// to be asked to look for.
func assertDrained(t *testing.T, engine *fakeEngine) {
	t.Helper()

	if engine.closed != engine.opened {
		t.Errorf("%d change iterators were opened but %d were closed; an unreleased iterator leaks "+
			"driver rows against a real backend and shows up nowhere in the output",
			engine.opened, engine.closed)
	}
}
