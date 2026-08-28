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

// The engines the non-vacuity tests drive: one that is correct, and one per way of
// being wrong.
//
// Every one of them is the package's own referenceEngine with a single deliberate
// flaw layered over it, which is why they live in a test file. A flaw switch inside
// reference.go would be a hazard: it would ship, and the first person to reach for
// it would find a supported way to make the reference lie. Here the flaws cannot
// leave the test binary, and each one is small enough that what it breaks — and
// therefore what the property it provokes is really asserting — can be read in one
// place.

package conformance

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// fakeBackend is the identifier the fake engines report. It is a real, non-empty
// name because the capability property insists on one, and naming no storage
// technology because this package must not.
const fakeBackend = "reference"

// flaws is one deliberate violation each. A fixture sets exactly one, so that the
// property it provokes can be said to have rejected *that* violation rather than
// some incidental damage.
type flaws struct {
	// reverseOrder flips the requested direction, so an unreversed query comes back
	// newest first and a limit takes from the wrong end of the window.
	reverseOrder bool
	// coarseTimestamps rounds every emitted timestamp to the second, which does not
	// lose the changes — it loses the order of everything inside each second.
	coarseTimestamps bool
	// mergeIncarnations answers "the history of this name" instead of "the history
	// of this incarnation", blanks the UID a reader would key on, and reports the
	// two incarnations as one.
	mergeIncarnations bool
	// fabricateDeletion synthesizes a Deleted row to close a timeline that merely
	// ended, on a backend whose storage never receives one.
	fabricateDeletion bool
	// ignoreCheckpoints treats a checkpoint as an ordinary modification, so a replay
	// walks back to the object's first sighting and reports a base that is not the
	// one a reader would have to check.
	ignoreCheckpoints bool
	// doubleApplyCheckpoint applies a checkpoint's own diff on top of the state that
	// diff already produced.
	doubleApplyCheckpoint bool
	// dropLastPatch stops one patch short while still reporting the digest the full
	// replay would have produced.
	dropLastPatch bool
	// stateBeforeHistory substitutes the earliest recorded state for an instant that
	// predates it, instead of admitting no state can be produced.
	stateBeforeHistory bool
	// mangleCoverage closes intervals that are still open, returns them newest
	// first, and drops the all-namespaces scope from a namespaced query.
	mangleCoverage bool
	// scanUnbounded declares that a time bound is required and then answers an
	// unbounded query anyway.
	scanUnbounded bool
	// truncateOnError ends the loop quietly when the backend dies mid-stream,
	// presenting a partial history as a whole one.
	truncateOnError bool
	// leakOnClose leaves a goroutine running after the iterator is closed.
	leakOnClose bool
	// ignoreActorInclude drops the Actors predicate.
	ignoreActorInclude bool
	// ignoreExcludeActors drops the ExcludeActors predicate.
	ignoreExcludeActors bool
	// ignoreFieldPaths drops the FieldPaths predicate.
	ignoreFieldPaths bool
	// unstableCapabilities renames the backend on every call.
	unstableCapabilities bool
}

// fakeEngine is a referenceEngine seen through one flaw.
type fakeEngine struct {
	flaws flaws
	// report is what Capabilities() says; behave is what the underlying reference
	// engine actually does. They are the same for every fixture but scanUnbounded,
	// whose whole point is that they differ.
	report query.Capabilities
	behave query.Capabilities

	mu      sync.Mutex
	calls   int
	fault   *StreamFault
	history History
	ref     *referenceEngine
	// alt is the engine a reconstruction flaw answers from: a reference over a
	// history that has been damaged in the shape the flaw describes. Building the
	// damage into the history rather than into the replay keeps every flaw out of
	// reference.go.
	alt *referenceEngine

	release chan struct{}
	leaked  sync.WaitGroup
}

func newFakeEngine(f flaws) *fakeEngine {
	report := query.Capabilities{
		Backend:          fakeBackend,
		Deletions:        true,
		ServerSideFilter: true,
		PointQuery:       true,
	}
	if f.fabricateDeletion {
		// The fabrication is only a violation against a backend that says it cannot
		// record deletions; against one that can, the row would simply be history.
		report.Deletions = false
	}
	behave := report
	if f.scanUnbounded {
		report.TimeBoundRequired = true
	}
	return &fakeEngine{flaws: f, report: report, behave: behave, release: make(chan struct{})}
}

// reducedEngine is a truthfully declared, minimal backend: no deletions, no
// pushdown, no point queries, and a time bound demanded on every query.
//
// It exists so the compliant run proves the suite green for both shapes of backend.
// A suite that only ever passed a full-capability engine would be one whose
// capability-conditional expectations had never been executed, and the archive tier
// is exactly the backend those expectations exist for.
func reducedEngine() *fakeEngine {
	caps := query.Capabilities{Backend: fakeBackend, TimeBoundRequired: true}
	return &fakeEngine{report: caps, behave: caps, release: make(chan struct{})}
}

func (e *fakeEngine) seed(h History) error {
	e.history = h
	e.ref = newReferenceEngine(h, e.behave)
	switch {
	case e.flaws.ignoreCheckpoints:
		e.alt = newReferenceEngine(historyWithoutCheckpoints(h), e.behave)
	case e.flaws.doubleApplyCheckpoint:
		e.alt = newReferenceEngine(historyWithDoubledCheckpoint(h), e.behave)
	case e.flaws.dropLastPatch:
		e.alt = newReferenceEngine(historyWithoutLastPatch(h), e.behave)
	}
	return nil
}

func (e *fakeEngine) setStreamFault(f *StreamFault) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fault = f
}

func (e *fakeEngine) takeFault() *StreamFault {
	e.mu.Lock()
	defer e.mu.Unlock()
	f := e.fault
	e.fault = nil
	return f
}

func (e *fakeEngine) Capabilities() query.Capabilities {
	if !e.flaws.unstableCapabilities {
		return e.report
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	out := e.report
	out.Backend = fakeBackend + "-" + strconv.Itoa(e.calls)
	return out
}

// Close releases the reference engine. It deliberately does not release the
// goroutine leakOnClose started: that is the flaw.
func (e *fakeEngine) Close() error { return e.ref.Close() }

func (e *fakeEngine) Timeline(ctx context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	asked := q
	if e.flaws.reverseOrder {
		q.Reverse = !q.Reverse
	}
	if e.flaws.mergeIncarnations && q.UID == "" {
		q.AllIncarnations = true
	}
	if e.flaws.ignoreActorInclude {
		q.Actors = nil
	}
	if e.flaws.ignoreExcludeActors {
		q.ExcludeActors = nil
	}
	if e.flaws.ignoreFieldPaths {
		q.FieldPaths = nil
	}

	it, err := e.ref.Timeline(ctx, q)
	if err != nil {
		return nil, err
	}
	var changes []query.Change
	for it.Next() {
		changes = append(changes, it.Change())
	}
	if err := it.Close(); err != nil {
		return nil, err
	}

	changes = e.damage(changes, asked)
	if e.flaws.leakOnClose {
		e.leaked.Go(func() { <-e.release })
	}
	stream := query.ChangeIterator(newSliceIterator(changes, e.takeFault()))
	if e.flaws.truncateOnError {
		stream = truncatingIterator{stream}
	}
	return stream, nil
}

// damage applies the flaws that act on the emitted rows rather than on the query.
func (e *fakeEngine) damage(changes []query.Change, q query.TimelineQuery) []query.Change {
	if e.flaws.coarseTimestamps {
		for i := range changes {
			changes[i].TS = changes[i].TS.Truncate(time.Second)
		}
	}
	if e.flaws.mergeIncarnations {
		for i := range changes {
			changes[i].UID = ""
		}
	}
	if e.flaws.fabricateDeletion && len(changes) > 0 && !q.Reverse {
		last := changes[len(changes)-1]
		changes = append(changes, query.Change{
			TS:         last.TS.Add(time.Second),
			EventType:  query.EventDeleted,
			UID:        last.UID,
			APIVersion: last.APIVersion,
		})
	}
	return changes
}

func (e *fakeEngine) StateAt(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	got, err := e.ref.StateAt(ctx, ref, at, uid)
	if err != nil {
		if e.flaws.stateBeforeHistory {
			return e.earliestState()
		}
		return nil, err
	}
	if e.alt == nil {
		return got, nil
	}
	damaged, altErr := e.alt.StateAt(ctx, ref, at, uid)
	if altErr != nil {
		return nil, altErr
	}
	if e.flaws.dropLastPatch {
		// The realistic shape of the bug: the replay stopped short but still reported
		// the digest of the row it believed it had finished on.
		damaged.SHA256 = got.SHA256
	}
	return damaged, nil
}

// earliestState substitutes the oldest recorded state for an instant that predates
// every row — the "nearly always nearly right" answer the contract forbids.
func (e *fakeEngine) earliestState() (*query.Reconstruction, error) {
	rows := slices.Clone(e.history.Rows)
	slices.SortStableFunc(rows, func(a, b Row) int { return a.Change.TS.Compare(b.Change.TS) })
	for _, r := range rows {
		if r.Change.Data == "" {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(r.Change.Data), &doc); err != nil {
			panic("fixture: earliest state is not a JSON object: " + err.Error())
		}
		return &query.Reconstruction{
			Object: doc, BaseTS: r.Change.TS, BaseEvent: r.Change.EventType, SHA256: r.Change.SHA256,
		}, nil
	}
	return nil, query.ErrObjectNotFound
}

func (e *fakeEngine) Coverage(ctx context.Context, q query.ScopeQuery) ([]query.ScopeInterval, error) {
	got, err := e.ref.Coverage(ctx, q)
	if err != nil || !e.flaws.mangleCoverage {
		return got, err
	}
	// The literal reading of namespace, which answers "never observed" about an
	// object a cluster-wide rule was watching the whole time.
	got = slices.DeleteFunc(got, func(iv query.ScopeInterval) bool {
		return q.Namespace != "" && iv.Namespace != q.Namespace
	})
	for i := range got {
		if got[i].To == nil {
			closed := got[i].From.Add(time.Minute)
			got[i].To = &closed
		}
	}
	slices.Reverse(got)
	return got, nil
}

func (e *fakeEngine) Incarnations(
	ctx context.Context, ref query.ObjectRef, from, to time.Time,
) ([]query.Incarnation, error) {
	got, err := e.ref.Incarnations(ctx, ref, from, to)
	if err != nil || !e.flaws.mergeIncarnations || len(got) < 2 {
		return got, err
	}
	// One name, one entry: the evidence that a second object ever wore it, gone.
	merged := got[0]
	merged.LastSeen = got[len(got)-1].LastSeen
	return []query.Incarnation{merged}, nil
}

// truncatingIterator reports a mid-stream failure as the end of the result set.
type truncatingIterator struct{ query.ChangeIterator }

func (t truncatingIterator) Err() error { return nil }

// ---------------------------------------------------------------------------
// Damaged histories
// ---------------------------------------------------------------------------

// historyWithoutCheckpoints demotes every checkpoint to an ordinary modification,
// which is what a backend that did not know about them would see.
func historyWithoutCheckpoints(h History) History {
	out := History{Scopes: h.Scopes, Rows: make([]Row, 0, len(h.Rows))}
	for _, r := range h.Rows {
		if r.Change.EventType == query.EventCheckpoint {
			r.Change.EventType = query.EventModified
			r.Change.Data = ""
		}
		out.Rows = append(out.Rows, r)
	}
	return out
}

// historyWithDoubledCheckpoint rewrites each checkpoint's data as its own diff
// applied a second time, which is what a replay that re-applied it would produce.
func historyWithDoubledCheckpoint(h History) History {
	out := History{Scopes: h.Scopes, Rows: make([]Row, 0, len(h.Rows))}
	for _, r := range h.Rows {
		if r.Change.EventType == query.EventCheckpoint && r.Change.Diff != "" {
			r.Change.Data = mustReapply(r.Change.Data, r.Change.Diff)
		}
		out.Rows = append(out.Rows, r)
	}
	return out
}

// mustReapply applies a patch to a document and returns the result, panicking on
// failure: the inputs are this package's own fixtures, so a failure is a bug in the
// suite rather than a finding about a backend.
func mustReapply(data, diff string) string {
	var doc any
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		panic("fixture: checkpoint data is not JSON: " + err.Error())
	}
	next, err := applyPatch(doc, diff)
	if err != nil {
		panic("fixture: checkpoint diff does not apply to its own data: " + err.Error())
	}
	raw, err := json.Marshal(next)
	if err != nil {
		panic("fixture: doubled checkpoint state will not encode: " + err.Error())
	}
	return string(raw)
}

// historyWithoutLastPatch drops the newest patch-only row, so a replay stops one
// step short of where it should.
func historyWithoutLastPatch(h History) History {
	rows := slices.Clone(h.Rows)
	slices.SortStableFunc(rows, func(a, b Row) int { return a.Change.TS.Compare(b.Change.TS) })
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].Change.Data == "" && rows[i].Change.Diff != "" {
			return History{Scopes: h.Scopes, Rows: slices.Delete(rows, i, i+1)}
		}
	}
	return History{Scopes: h.Scopes, Rows: rows}
}

// ---------------------------------------------------------------------------
// Harnesses
// ---------------------------------------------------------------------------

// newFakeHarness builds a harness over an engine carrying these flaws, and the
// cleanup that releases anything the leak fixture started.
//
// The cleanup is returned rather than registered on a *testing.T because the
// non-vacuity runner drives properties through a recorder and has no T to register
// on. Releasing the goroutine explicitly, rather than letting it time out, keeps the
// leak fixture from making a later property's baseline flaky.
func newFakeHarness(f flaws) (Harness, func()) {
	e := newFakeEngine(f)
	return harnessOver(e), func() {
		close(e.release)
		e.leaked.Wait()
	}
}

// newReducedHarness is the truthfully declared minimal backend.
func newReducedHarness() (Harness, func()) {
	e := reducedEngine()
	return harnessOver(e), func() {
		close(e.release)
		e.leaked.Wait()
	}
}

// harnessOver wires an engine into a Harness whose declaration mirrors what that
// engine reports.
//
// Mirroring is what makes the fixtures test what they are meant to. A fixture whose
// declaration disagreed with its engine would be rejected by Harness.validate before
// any property ran, and every non-vacuity test would then be proving that the
// capability gate works rather than that the property does.
func harnessOver(e *fakeEngine) Harness {
	var declared []Capability
	for _, c := range declarableCapabilities() {
		if reports(e.report, c) {
			declared = append(declared, c)
		}
	}
	return Harness{
		Engine:         e,
		Seed:           e.seed,
		SetStreamFault: e.setStreamFault,
		Capabilities:   DeclareCapabilities(declared...),
	}
}
