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

package pipeline

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The doubles in this file complete the warm/GC coordinator's dependency set, so
// every test in this package still runs with no apiserver, no informer and no
// ClickHouse: fakeStateReader stands in for the sink's history, fakeScopeEvents for
// its scope log, fakeSinkBackends for Task 1.8's SinkManager, and fakeScopes for
// Task 1.4's WatchManager in its scope-level role.
//
// They record calls as well as answering them, because several acceptance criteria
// are about what the coordinator did *not* do — no StateReader call for another
// scope, no GC before the informer synced, no Deleted row for an orphaned scope.

// scopeActiveCall is one ScopeWasActive probe: which scope, and as of when.
type scopeActiveCall struct {
	filter sink.ScopeFilter
	asOf   time.Time
}

// fakeStateReader is a map-backed sink.StateReader. Errors are scripted rather
// than random: each of the three methods consumes one queued error per call, so a
// test states exactly which attempt fails.
type fakeStateReader struct {
	mu sync.Mutex

	// states answers LastKnownStates, keyed by the exact filter queried. The
	// values are *per-incarnation* rows (see sink.KnownState), so a test states a
	// delete-and-recreate the operator missed by listing two entries with the same
	// Namespace/Name and different UIDs and timestamps.
	states map[sink.ScopeFilter][]sink.KnownState
	// wasActive answers ScopeWasActive; a missing entry means false, which is the
	// brand-new-scope case.
	wasActive map[sink.ScopeFilter]bool
	// active answers ActiveScopes — the scopes some earlier process left open.
	active []sink.ScopeFilter

	lastKnownCalls   []sink.ScopeFilter
	scopeActiveCalls []scopeActiveCall
	activeScopeCalls int

	lastKnownErrs   []error
	wasActiveErrs   []error
	activeScopeErrs []error

	// block, when non-nil, holds every LastKnownStates call until it is closed —
	// the hook the cancellation tests use to catch a warm mid-flight.
	block chan struct{}
}

func newFakeStateReader() *fakeStateReader {
	return &fakeStateReader{
		states:    make(map[sink.ScopeFilter][]sink.KnownState),
		wasActive: make(map[sink.ScopeFilter]bool),
	}
}

func (f *fakeStateReader) LastKnownStates(ctx context.Context, filter sink.ScopeFilter) ([]sink.KnownState, error) {
	f.mu.Lock()
	f.lastKnownCalls = append(f.lastKnownCalls, filter)
	states := slices.Clone(f.states[filter])
	err := takeErr(&f.lastKnownErrs)
	block := f.block
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return states, nil
}

func (f *fakeStateReader) ScopeWasActive(_ context.Context, filter sink.ScopeFilter, asOf time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopeActiveCalls = append(f.scopeActiveCalls, scopeActiveCall{filter: filter, asOf: asOf})
	if err := takeErr(&f.wasActiveErrs); err != nil {
		return false, err
	}
	return f.wasActive[filter], nil
}

func (f *fakeStateReader) ActiveScopes(_ context.Context, clusterID string) ([]sink.ScopeFilter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeScopeCalls++
	if err := takeErr(&f.activeScopeErrs); err != nil {
		return nil, err
	}
	var out []sink.ScopeFilter
	for _, scope := range f.active {
		if scope.ClusterID == clusterID {
			out = append(out, scope)
		}
	}
	return out, nil
}

// setStates makes states the history for filter, and records them as the scope's
// last-known objects.
func (f *fakeStateReader) setStates(filter sink.ScopeFilter, states ...sink.KnownState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[filter] = states
}

// setWasActive makes filter's scope look like it was watched in a previous epoch.
func (f *fakeStateReader) setWasActive(filter sink.ScopeFilter, active bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wasActive[filter] = active
}

// setActiveScopes makes scopes the set some earlier process left open.
func (f *fakeStateReader) setActiveScopes(scopes ...sink.ScopeFilter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = scopes
}

// failNextLastKnownStates queues a one-shot LastKnownStates failure.
func (f *fakeStateReader) failNextLastKnownStates(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastKnownErrs = append(f.lastKnownErrs, errs...)
}

// failNextActiveScopes queues a one-shot ActiveScopes failure.
func (f *fakeStateReader) failNextActiveScopes(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeScopeErrs = append(f.activeScopeErrs, errs...)
}

// blockLastKnownStates makes every LastKnownStates call wait, returning the
// release function.
func (f *fakeStateReader) blockLastKnownStates() (release func()) {
	block := make(chan struct{})
	f.mu.Lock()
	f.block = block
	f.mu.Unlock()
	return sync.OnceFunc(func() { close(block) })
}

// historyReads returns the scopes LastKnownStates was called for, in order.
func (f *fakeStateReader) historyReads() []sink.ScopeFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lastKnownCalls)
}

// epochProbes returns the ScopeWasActive calls, in order.
func (f *fakeStateReader) epochProbes() []scopeActiveCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.scopeActiveCalls)
}

func (f *fakeStateReader) activeScopesCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeScopeCalls
}

// takeErr pops the next scripted error, if any. The caller holds the fake's lock.
func takeErr(queue *[]error) error {
	if len(*queue) == 0 {
		return nil
	}
	err := (*queue)[0]
	*queue = (*queue)[1:]
	return err
}

// fakeScopeEvents is a sink.ScopeEventWriter that records every accepted scope
// event. Failures are scripted one-shot, like fakeWriter's.
type fakeScopeEvents struct {
	mu       sync.Mutex
	events   []sink.ScopeEvent
	failures []error
}

func (f *fakeScopeEvents) EnqueueScopeEvent(_ context.Context, event sink.ScopeEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := takeErr(&f.failures); err != nil {
		return err
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakeScopeEvents) recorded() []sink.ScopeEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// awaitEvents waits for at least n scope events, failing the test on timeout
// rather than deadlocking it.
func (f *fakeScopeEvents) awaitEvents(t *testing.T, n int) []sink.ScopeEvent {
	t.Helper()
	var got []sink.ScopeEvent
	waitFor(t, func() bool {
		got = f.recorded()
		return len(got) >= n
	}, func() string { return "at least one scope event" })
	return got
}

// fakeSinkBackends is a StateReaderRouter plus ScopeEventRouter — the two halves
// of Task 1.8's SinkManager the coordinator consumes. A sink present in one map
// but not the other reproduces a real condition: a sink that writes records but
// cannot read its history back.
// It is keyed by sink.ID, as the real SinkManager's registry is, so a lookup
// carrying an unexpected kind misses rather than being answered by the sink that
// shares its name. The setters take names, which is what the coordinator itself
// still holds (see sinkIDFor).
type fakeSinkBackends struct {
	mu      sync.Mutex
	readers map[sink.ID]sink.StateReader
	events  map[sink.ID]sink.ScopeEventWriter
}

func newFakeSinkBackends() *fakeSinkBackends {
	return &fakeSinkBackends{
		readers: make(map[sink.ID]sink.StateReader),
		events:  make(map[sink.ID]sink.ScopeEventWriter),
	}
}

func (f *fakeSinkBackends) StateReaderFor(id sink.ID) (sink.StateReader, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.readers[id]
	return r, ok
}

func (f *fakeSinkBackends) SinkIDs() []sink.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]sink.ID, 0, len(f.readers)+len(f.events))
	for id := range f.readers {
		ids = append(ids, id)
	}
	for id := range f.events {
		if _, dup := f.readers[id]; !dup {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b sink.ID) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name))
	})
	return ids
}

func (f *fakeSinkBackends) ScopeEventWriterFor(id sink.ID) (sink.ScopeEventWriter, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.events[id]
	return w, ok
}

func (f *fakeSinkBackends) setReader(name string, r sink.StateReader) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readers[sinkIDFor(name)] = r
}

func (f *fakeSinkBackends) removeReader(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.readers, sinkIDFor(name))
}

func (f *fakeSinkBackends) setEvents(name string, w sink.ScopeEventWriter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events[sinkIDFor(name)] = w
}

// fakeScopes is a ScopeStates whose answers default to the conservative side:
// a scope is neither synced nor desired until a test says so. That default is what
// makes the HasSynced-gating assertion meaningful — a fake that defaulted to
// "synced" would let the gate be removed without any test noticing.
type fakeScopes struct {
	mu      sync.Mutex
	synced  map[scopeRef]struct{}
	desired map[scopeRef]struct{}
	// syncChecks counts ScopeSynced calls, so a test can assert a path consulted
	// the informer's readiness *not at all* rather than merely not waiting long.
	syncChecks int
	// settled is nil by default, which the contract defines as "no gating needed",
	// so most tests need not think about it; the gate test supplies a real channel.
	settled chan struct{}
}

func newFakeScopes() *fakeScopes {
	return &fakeScopes{
		synced:  make(map[scopeRef]struct{}),
		desired: make(map[scopeRef]struct{}),
	}
}

func (f *fakeScopes) Settled() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.settled == nil {
		return nil
	}
	return f.settled
}

// withSettleGate makes the coordinator wait, returning the function that opens the
// gate.
func (f *fakeScopes) withSettleGate() (open func()) {
	gate := make(chan struct{})
	f.mu.Lock()
	f.settled = gate
	f.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

func (f *fakeScopes) ScopeSynced(sinkName string, scope ScopeKey) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncChecks++
	_, ok := f.synced[scopeRef{sink: sinkName, scope: scope}]
	return ok
}

// syncChecked reports how many times the informer's readiness was consulted.
func (f *fakeScopes) syncChecked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncChecks
}

func (f *fakeScopes) ScopeDesired(sinkName string, scope ScopeKey) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.desired[scopeRef{sink: sinkName, scope: scope}]
	return ok
}

// markDesired and markSynced operate on testSink, the one sink these tests use;
// the per-sink separation itself is asserted against the real WatchManager in
// internal/watch, where the answers actually come from.
func (f *fakeScopes) markDesired(scope ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desired[scopeRef{sink: testSink, scope: scope}] = struct{}{}
}

func (f *fakeScopes) markSynced(scope ScopeKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced[scopeRef{sink: testSink, scope: scope}] = struct{}{}
}

// waitFor polls cond until it holds or the deadline passes, failing with the
// message describe returns. describe is a function so the failure message reflects
// the state at the moment of the timeout rather than at the start of the wait.
func waitFor(t *testing.T, cond func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", describe())
		}
		time.Sleep(time.Millisecond)
	}
}

// stayFalse asserts cond does not become true within a short window — the shape
// every "it must not do that yet" assertion needs (no GC before the informer
// syncs, no Deleted row for an orphaned scope).
func stayFalse(t *testing.T, cond func() bool, describe string) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("%s", describe)
		}
		time.Sleep(time.Millisecond)
	}
}
