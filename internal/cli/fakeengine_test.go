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
	"fmt"
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

	// replay makes StateAt reconstruct from changes through query.Replay instead
	// of handing back the fixed document in state.
	//
	// It exists so that a test of `get` exercises the reconstruction procedure
	// rather than a stand-in for it. The rule that a Checkpoint's own diff must
	// not be applied over the state that diff already produced lives in
	// query.Replay, and a fake that returned a document of its own would assert
	// the fake's arithmetic and certify nothing about the rule.
	replay bool

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

	// stateCalls counts the reconstructions asked for. It exists because the cost
	// of a command is part of its behaviour: `blame` runs one replay of its own
	// and must not also pay for the prior-value replay it does not render, and a
	// second round trip is invisible in output.
	stateCalls int
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
	_ context.Context, _ query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	f.stateCalls++
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	if f.replay {
		return f.replayState(at, uid)
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

// replayState reconstructs from the fixture's own history, through the contract's
// reconstruction procedure.
//
// It follows the procedure docs/SCHEMA.md specifies as far as a rendering test
// needs: rows at or before the instant, one incarnation, a deletion terminal for
// its uid, the newest full-state row as the base, and query.Replay for the rest.
// What it deliberately does not do is decide anything query.Replay decides.
func (f *fakeEngine) replayState(at time.Time, uid string) (*query.Reconstruction, error) {
	if uid == "" {
		uid = newestUIDAt(f.changes, at)
	}

	history := make([]query.ReplayRow, 0, len(f.changes))
	for _, change := range f.changes {
		switch {
		case change.TS.After(at), change.UID != uid:
			continue
		case change.EventType == query.EventDeleted:
			// Terminal for its incarnation: everything before it describes an
			// object that no longer existed at the instant asked about.
			history = nil
			continue
		}
		history = append(history, query.ReplayRow{
			TS: change.TS, EventType: change.EventType,
			Data: change.Data, Diff: change.Diff, SHA256: change.SHA256,
		})
	}
	if len(history) == 0 {
		return nil, query.ErrObjectNotFound
	}
	base := query.BaseRow(history)
	if base < 0 {
		return nil, fmt.Errorf("no full-state row survives before %s: %w",
			at.UTC().Format(time.RFC3339Nano), query.ErrObjectNotFound)
	}
	return query.Replay(history, base)
}

// newestUIDAt is the incarnation StateAt picks when none was pinned: the newest
// one alive at the instant, never a blend of two.
func newestUIDAt(changes []query.Change, at time.Time) string {
	uid := ""
	for _, change := range changes {
		if !change.TS.After(at) {
			uid = change.UID
		}
	}
	return uid
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
