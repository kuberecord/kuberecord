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
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/goleak"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// testClusterID matches the harness's ClusterID, so a filter built here is the one
// the coordinator actually queries with. oldUID names the incarnation history
// remembers when a reincarnation test needs one (newUID is its live counterpart, see
// hashcache_test.go).
const (
	testClusterID = "test-cluster"
	oldUID        = "uid-old"
)

// podScope is the (group, kind, namespace) triple every test in this file warms.
var podScope = ScopeKey{Group: "", Kind: "Pod", Namespace: "default"}

// scopeFilterFor renders a scope as the sink-side filter the coordinator queries
// with, so a test's expectations and the production call path cannot drift.
func scopeFilterFor(scope ScopeKey) sink.ScopeFilter {
	return scopeRef{sink: testSink, scope: scope}.filter(testClusterID)
}

// warmHarness is a Pipeline plus a WarmCoordinator and the doubles behind both.
type warmHarness struct {
	*testHarness

	reader   *fakeStateReader
	events   *fakeScopeEvents
	backends *fakeSinkBackends
	scopes   *fakeScopes
	coord    *WarmCoordinator
}

// newWarmHarness builds a coordinator over the pipeline harness's doubles, with
// pacing shortened so nothing waits on production backoff. The sink is wired with
// both a reader and a scope-log writer, which is the ordinary case; tests that want
// a Writer-only sink remove the reader.
func newWarmHarness(t *testing.T) *warmHarness {
	t.Helper()

	base := newHarness(t)
	h := &warmHarness{
		testHarness: base,
		reader:      newFakeStateReader(),
		events:      &fakeScopeEvents{},
		backends:    newFakeSinkBackends(),
		scopes:      newFakeScopes(),
	}
	h.backends.setReader(testSink, h.reader)
	h.backends.setEvents(testSink, h.events)

	opts := WarmOptions{
		Pipeline:         base.pipeline,
		Scopes:           h.scopes,
		Readers:          h.backends,
		ScopeEvents:      h.backends,
		RetryMaxInterval: 5 * time.Millisecond,
		SyncPollInterval: time.Millisecond,
		BootInterval:     5 * time.Millisecond,
	}
	coord, err := NewWarmCoordinator(opts)
	if err != nil {
		t.Fatalf("NewWarmCoordinator: %v", err)
	}
	h.coord = coord
	return h
}

// run starts the coordinator and returns a stop function that cancels it and waits
// for a clean exit.
func (h *warmHarness) run(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- h.coord.Start(ctx) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Start returned %v, want nil", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("the warm coordinator did not stop within 10s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// warmNow starts a warm for scope with the current instant as its epoch, after
// telling the scope fake that the scope is desired (which production guarantees:
// a warm is only ever requested for a scope an interest was just installed for).
func (h *warmHarness) warmNow(scope ScopeKey) time.Time {
	epoch := time.Now().UTC()
	h.scopes.markDesired(scope)
	h.coord.WarmScope(WarmTarget{Sink: testSink, Scope: scope, EpochStart: epoch})
	return epoch
}

// awaitWarm waits until the scope has been marked warm (SafeMode off).
func (h *warmHarness) awaitWarm(t *testing.T, scope ScopeKey) {
	t.Helper()
	waitFor(t, func() bool {
		st := h.pipeline.sinks.get(testSink)
		return st.scopeWarm(Key{Sink: testSink, Group: scope.Group, Kind: scope.Kind, Namespace: scope.Namespace})
	}, func() string { return "the scope to be marked warm" })
}

// writerOnlyAnnouncements returns the coordinator's "this sink cannot read its own
// history" lines, at the default verbosity an operator actually sees.
//
// It counts rather than merely detects because the requirement is about volume: one
// line per sink, however many scopes discover the same thing (see
// WarmCoordinator.announceWriterOnly).
func (h *warmHarness) writerOnlyAnnouncements() []string {
	var out []string
	for _, msg := range h.logs.infoLines() {
		if strings.Contains(msg, "cannot read its own history") {
			out = append(out, msg)
		}
	}
	return out
}

// deletedRecords returns the Deleted rows accepted by the sink — the thing most of
// these tests are counting.
func (h *warmHarness) deletedRecords() []sink.Record {
	var out []sink.Record
	for _, rec := range h.writer.recorded() {
		if rec.EventType == "Deleted" {
			out = append(out, rec)
		}
	}
	return out
}

// knownState renders one history row for a named Pod — one incarnation of it, in
// the per-incarnation reading LastKnownStates now answers in (see
// sink.KnownState). Its TS is the zero instant, which is all a single-incarnation
// identity needs: the classification only compares timestamps *within* one
// identity.
func knownState(name, uid, hash string) sink.KnownState {
	return sink.KnownState{Namespace: "default", Name: name, UID: uid, SHA256: hash}
}

// incarnation renders one history row carrying everything a close-out has to be
// derivable from: the api_version last recorded for that incarnation and the
// instant of its last recorded event.
func incarnation(name, uid, hash, apiVersion string, ts time.Time) sink.KnownState {
	return sink.KnownState{
		Namespace:  "default",
		Name:       name,
		UID:        uid,
		SHA256:     hash,
		APIVersion: apiVersion,
		TS:         ts,
	}
}

// The two instants every reincarnation-recovery test dates its history from: the
// prior incarnation's last recorded event, and the successor's first. priorAPIVersion
// is the version recorded against the prior incarnation, deliberately different
// from the successor's so a close-out that took the wrong one is visible.
const priorAPIVersion = "v1beta1"

var (
	priorTS     = time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	successorTS = priorTS.Add(time.Hour)
)

// TestNewWarmCoordinatorValidatesRequiredOptions covers the eager dependency
// checks: a missing dependency must fail at construction, not as a nil dereference
// inside a warm goroutine whose whole job is to be trustworthy about deletions.
func TestNewWarmCoordinatorValidatesRequiredOptions(t *testing.T) {
	base := newHarness(t)
	scopes := newFakeScopes()
	backends := newFakeSinkBackends()

	cases := []struct {
		name string
		opts WarmOptions
	}{
		{name: "no pipeline", opts: WarmOptions{Scopes: scopes, Readers: backends, ScopeEvents: backends}},
		{name: "no scopes", opts: WarmOptions{Pipeline: base.pipeline, Readers: backends, ScopeEvents: backends}},
		{name: "no readers", opts: WarmOptions{Pipeline: base.pipeline, Scopes: scopes, ScopeEvents: backends}},
		{name: "no scope events", opts: WarmOptions{Pipeline: base.pipeline, Scopes: scopes, Readers: backends}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewWarmCoordinator(tc.opts); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}

	c, err := NewWarmCoordinator(WarmOptions{
		Pipeline: base.pipeline, Scopes: scopes, Readers: backends, ScopeEvents: backends,
	})
	if err != nil {
		t.Fatalf("NewWarmCoordinator with every dependency: %v", err)
	}
	if c.retryMaxInterval != defaultWarmRetryMaxInterval ||
		c.syncPollInterval != defaultSyncPollInterval || c.bootInterval != defaultBootInterval {
		t.Error("pacing was not defaulted to the documented values")
	}
	if !c.NeedLeaderElection() {
		t.Error("NeedLeaderElection = false, want true: GC claims are per-process, so two replicas would double every Deleted row")
	}
}

// TestBootReconciliationRuleDeletedWhileOperatorDownEmitsStoppedAndNeverDeleted is
// the single most important test in Phase 1.
//
// A rule was deleted while the operator was down. Its scope is still recorded as
// Started in the sink's scope log, and the objects it covered are still recorded as
// live. The only truthful reconciliation is one Stopped row for the scope: the
// operator stopped watching, it did not witness any deletion. Emitting Deleted rows
// here would turn every rule deletion during a restart into a fabricated mass
// deletion event — the exact audit lie scope epochs exist to prevent.
func TestBootReconciliationRuleDeletedWhileOperatorDownEmitsStoppedAndNeverDeleted(t *testing.T) {
	h := newWarmHarness(t)

	orphan := scopeFilterFor(podScope)
	// The scope's epoch was left open, and its objects are still on record as live —
	// everything a naive implementation would need to "helpfully" delete them.
	h.reader.setActiveScopes(orphan)
	h.reader.setWasActive(orphan, true)
	h.reader.setStates(orphan, knownState("web-1", "uid-1", "hash-1"), knownState("web-2", "uid-2", "hash-2"))
	// No rule wants it any more: h.scopes has nothing marked desired.

	h.run(t)

	events := h.events.awaitEvents(t, 1)
	if len(events) != 1 {
		t.Fatalf("recorded %d scope events, want exactly 1: %+v", len(events), events)
	}
	got := events[0]
	if got.Action != sink.ScopeActionStopped {
		t.Errorf("scope event action = %q, want %q", got.Action, sink.ScopeActionStopped)
	}
	if got.Scope != orphan {
		t.Errorf("scope event scope = %+v, want %+v", got.Scope, orphan)
	}
	if got.RuleRef != "" {
		t.Errorf("scope event rule_ref = %q, want empty: the rule that held this scope is gone", got.RuleRef)
	}
	if got.TS.IsZero() {
		t.Error("scope event carries no timestamp")
	}

	// The whole point: not one Deleted row, and in fact not one row at all.
	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"boot reconciliation wrote resource_states rows; a closed scope must never be recorded as deletions")

	// It also must not warm the orphan: there is nothing watching it, so seeding a
	// dedup baseline for it would be pure memory with no reader.
	if reads := h.reader.historyReads(); len(reads) != 0 {
		t.Errorf("boot reconciliation read object history for %d scopes, want 0: %+v", len(reads), reads)
	}

	// And it happens once per sink, not once per tick.
	stayFalse(t, func() bool { return len(h.events.recorded()) > 1 },
		"boot reconciliation re-emitted a Stopped row for an already-reconciled sink")
}

// TestBootReconciliationLeavesDesiredScopesOpen is the other half of the same
// judgement: an ordinary restart, where the rule still exists. The scope's epoch was
// left open because a process exiting deliberately writes no Stopped row, and it
// must stay open — closing and immediately reopening it would put a spurious epoch
// boundary in the audit trail on every restart.
func TestBootReconciliationLeavesDesiredScopesOpen(t *testing.T) {
	h := newWarmHarness(t)

	stillWanted := scopeFilterFor(podScope)
	h.reader.setActiveScopes(stillWanted)
	h.scopes.markDesired(podScope)

	h.run(t)

	waitFor(t, func() bool { return h.reader.activeScopesCallCount() > 0 },
		func() string { return "the boot pass to enumerate open scopes" })
	stayFalse(t, func() bool { return len(h.events.recorded()) > 0 },
		"boot reconciliation closed a scope a live rule still wants")
}

// TestBootReconciliationWaitsForTheSettleGate proves the gate is honoured. Judging
// orphans against a desired state that has not been populated yet would close every
// scope in the cluster on startup.
func TestBootReconciliationWaitsForTheSettleGate(t *testing.T) {
	h := newWarmHarness(t)
	openGate := h.scopes.withSettleGate()

	h.reader.setActiveScopes(scopeFilterFor(podScope))
	h.run(t)

	stayFalse(t, func() bool { return h.reader.activeScopesCallCount() > 0 },
		"boot reconciliation ran before the desired state settled")

	openGate()
	h.events.awaitEvents(t, 1)
}

// TestBootReconciliationRetriesAfterAFailedEnumeration covers Invariant 5 on this
// path: a sink whose history could not be read is retried on the next tick rather
// than being marked done, and other sinks are unaffected.
func TestBootReconciliationRetriesAfterAFailedEnumeration(t *testing.T) {
	h := newWarmHarness(t)

	h.reader.setActiveScopes(scopeFilterFor(podScope))
	h.reader.failNextActiveScopes(errors.New("clickhouse unavailable"))

	h.run(t)

	events := h.events.awaitEvents(t, 1)
	if events[0].Action != sink.ScopeActionStopped {
		t.Errorf("scope event action = %q, want %q", events[0].Action, sink.ScopeActionStopped)
	}
	if calls := h.reader.activeScopesCallCount(); calls < 2 {
		t.Errorf("ActiveScopes was called %d times, want at least 2 (one failure plus one retry)", calls)
	}
}

// TestBootReconciliationSkipsASinkWithNoStateReader covers the Writer-only sink:
// there is no history to reconcile against, so the pass is a no-op rather than an
// error loop.
func TestBootReconciliationSkipsASinkWithNoStateReader(t *testing.T) {
	h := newWarmHarness(t)
	h.backends.removeReader(testSink)

	h.run(t)

	stayFalse(t, func() bool { return len(h.events.recorded()) > 0 },
		"a sink that cannot read its own history had scope epochs reconciled anyway")
}

// TestWarmScopeSeedsHistoryAndCollectsGenuineZombies is the ordinary restart path:
// the scope was watched before, one of its objects disappeared while the operator
// was down, and exactly one Deleted row must be written for it — no more (the claim
// is exactly-once) and no fewer (the disappearance is real).
func TestWarmScopeSeedsHistoryAndCollectsGenuineZombies(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)

	// History says two Pods were live; reality has only one of them.
	h.reader.setStates(filter, knownState("gone", "uid-gone", "hash-gone"), knownState("alive", "uid-alive", "hash-alive"))
	h.reader.setWasActive(filter, true)
	h.lister.set(podKey("alive"), newPod("alive", "uid-alive", "7", "nginx"))
	h.scopes.markSynced(podScope)

	h.run(t)
	h.warmNow(podScope)
	h.awaitWarm(t, podScope)

	waitFor(t, func() bool { return len(h.deletedRecords()) >= 1 },
		func() string { return "one Deleted row for the zombie" })

	deleted := h.deletedRecords()
	if len(deleted) != 1 {
		t.Fatalf("wrote %d Deleted rows, want exactly 1: %+v", len(deleted), deleted)
	}
	if deleted[0].Name != "gone" || deleted[0].UID != "uid-gone" {
		t.Errorf("Deleted row = %s (uid %s), want gone (uid uid-gone)", deleted[0].Name, deleted[0].UID)
	}

	// The surviving object keeps its seeded baseline, so its next unchanged
	// observation deduplicates instead of being re-recorded.
	st := h.pipeline.sinks.get(testSink)
	if entry, ok := st.cache.Load(podKey("alive").cacheKey()); !ok || entry.Hash != "hash-alive" {
		t.Errorf("the live object's seeded baseline = %+v (present %v), want hash-alive", entry, ok)
	}
	// The zombie's entry is gone: its Deleted row was confirmed by the fake writer.
	if _, ok := st.cache.Load(podKey("gone").cacheKey()); ok {
		t.Error("the zombie's cache entry survived a confirmed Deleted write")
	}
	// A repeated warm for the same epoch must not re-run the sweep.
	stayFalse(t, func() bool { return len(h.deletedRecords()) > 1 },
		"the GC pass emitted a second Deleted row for one disappearance")
}

// TestWarmScopeGCHonoursTheEpochCheck is the brand-new-scope case: a rule appears
// over a kind whose history was written by some earlier, properly-closed epoch. The
// seeded objects are pre-history — this process never watched them disappear — so
// the sweep must write nothing, while still seeding the baselines that keep the
// scope from re-emitting everything it sees.
func TestWarmScopeGCHonoursTheEpochCheck(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)

	// The history row carries the object's real content hash, exactly as a genuine
	// earlier epoch would have recorded it — that is what makes the "no duplicate
	// Added flood" assertion at the end of this test mean something.
	pod := newPod("ancient", "uid-ancient", "9", "nginx")
	norm, err := normalizeObject(pod, nil)
	if err != nil {
		t.Fatalf("normalizeObject: %v", err)
	}

	// Prior history exists, but the scope's last recorded action is not Started.
	h.reader.setStates(filter, knownState("ancient", "uid-ancient", norm.Hash))
	h.reader.setWasActive(filter, false)
	h.scopes.markSynced(podScope)

	h.run(t)
	epoch := h.warmNow(podScope)
	h.awaitWarm(t, podScope)

	waitFor(t, func() bool { return len(h.reader.epochProbes()) > 0 },
		func() string { return "the epoch check to run" })
	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"a scope with no previous open epoch had its pre-history recorded as deletions")

	// The probe is anchored to the epoch's own start, so this epoch's Started row —
	// written asynchronously, possibly during the warm — cannot answer for a previous
	// one.
	probe := h.reader.epochProbes()[0]
	if probe.filter != filter {
		t.Errorf("epoch probe filter = %+v, want %+v", probe.filter, filter)
	}
	if !probe.asOf.Equal(epoch) {
		t.Errorf("epoch probe asOf = %s, want the epoch start %s", probe.asOf, epoch)
	}

	// Seeding happened via StoreIfAbsent, so the object the history described
	// deduplicates on its next observation rather than flooding the sink with Added
	// rows for everything the earlier epoch already recorded.
	st := h.pipeline.sinks.get(testSink)
	if entry, ok := st.cache.Load(podKey("ancient").cacheKey()); !ok || entry.Hash != norm.Hash {
		t.Fatalf("seeded baseline = %+v (present %v), want the history's hash", entry, ok)
	}

	h.lister.set(podKey("ancient"), pod)
	if err := h.pipeline.Process(h.ctx, podKey("ancient")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if records := h.writer.recorded(); len(records) != 0 {
		t.Errorf("a seeded, unchanged object produced %d rows, want 0 (dedup): %v", len(records), h.writer.eventTypes())
	}
}

// TestWarmScopeSeedingDoesNotClobberLiveState covers the StoreIfAbsent contract at
// the warm level: a work item processed while the scope was still cold (and
// therefore tagged Snapshot) has already reserved the authoritative entry, and the
// historical baseline must not overwrite it.
func TestWarmScopeSeedingDoesNotClobberLiveState(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", oldUID, "hash-from-history"))
	h.reader.setWasActive(filter, false)
	h.scopes.markSynced(podScope)

	// A live observation lands first, while the scope is cold.
	h.lister.set(podKey("web"), newPod("web", newUID, "3", "nginx"))
	if err := h.pipeline.Process(h.ctx, podKey("web")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if got := h.writer.eventTypes(); len(got) != 1 || got[0] != "Snapshot" {
		t.Fatalf("event types before warm = %v, want exactly [Snapshot]", got)
	}

	h.run(t)
	h.warmNow(podScope)
	h.awaitWarm(t, podScope)

	st := h.pipeline.sinks.get(testSink)
	entry, ok := st.cache.Load(podKey("web").cacheKey())
	if !ok {
		t.Fatal("the live entry disappeared during warm-up")
	}
	if entry.UID != newUID || entry.Hash == "hash-from-history" {
		t.Errorf("warm-up clobbered live state: entry = %+v, want the uid-new observation", entry)
	}
}

// TestWarmScopeWaitsForTheInformerToSync is the HasSynced gating criterion. An
// informer that has not finished its initial List holds an empty indexer, so a GC
// pass running then would find every seeded object absent and delete the whole
// scope. Seeding itself is *not* gated — it needs no cluster state at all.
func TestWarmScopeWaitsForTheInformerToSync(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("gone", "uid-gone", "hash-gone"))
	h.reader.setWasActive(filter, true)
	// Deliberately not synced yet.

	h.run(t)
	h.warmNow(podScope)

	// Seeding and the SafeMode flip happen regardless of sync state.
	h.awaitWarm(t, podScope)
	// The sweep does not.
	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"the GC pass ran before the informer reported synced")
	if probes := h.reader.epochProbes(); len(probes) != 0 {
		t.Errorf("the epoch check ran before the informer synced: %+v", probes)
	}

	h.scopes.markSynced(podScope)
	waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
		func() string { return "the Deleted row once the informer synced" })
}

// TestWarmScopeAbandonsGCWhenTheScopeStopsWaitingForSync covers the other exit from
// the sync wait: the rule went away while its informer was still listing, so there
// is nothing to reconcile and nothing to record beyond the scope's Stopped row.
func TestWarmScopeAbandonsGCWhenTheScopeStopsWaitingForSync(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("gone", "uid-gone", "hash-gone"))
	h.reader.setWasActive(filter, true)

	h.run(t)
	h.warmNow(podScope)
	h.awaitWarm(t, podScope)

	// Undesire the scope without cancelling the run, so the abort is decided by the
	// desire check rather than by StopScope's cancellation.
	h.scopes.mu.Lock()
	delete(h.scopes.desired, scopeRef{sink: testSink, scope: podScope})
	h.scopes.mu.Unlock()

	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"the GC pass proceeded for a scope no rule wants any more")
}

// TestWarmScopeGCRefusesAReincarnatedObject ports the UID-mismatch refusal from the
// original GC tests. The pass's belief that an object is gone comes from a
// point-in-time read of history, so it must be checked against the cache's current
// UID before claiming — otherwise a reincarnation that a worker already recorded
// would have its live entry deleted by name alone.
func TestWarmScopeGCRefusesAReincarnatedObject(t *testing.T) {
	t.Run("refuses when a worker already recorded the new incarnation", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter, knownState("web", oldUID, "hash-old"))
		h.reader.setWasActive(filter, true)
		h.scopes.markSynced(podScope)

		// The object came back under a new UID and a worker already reserved it.
		st := h.pipeline.sinks.get(testSink)
		st.cache.Reserve(podKey("web").cacheKey(), CacheEntry{Hash: "hash-new", UID: newUID})
		h.lister.set(podKey("web"), newPod("web", newUID, "11", "nginx"))

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)
		waitFor(t, func() bool { return len(h.reader.epochProbes()) > 0 },
			func() string { return "the GC pass to reach the epoch check" })

		stayFalse(t, func() bool { return len(h.deletedRecords()) > 0 },
			"the GC pass deleted a currently-existing object by name after a reincarnation")

		entry, ok := st.cache.Load(podKey("web").cacheKey())
		if !ok || entry.UID != newUID || entry.PendingDelete {
			t.Errorf("the live entry was disturbed by a refused claim: %+v (present %v)", entry, ok)
		}
	})

	t.Run("closes out the old UID when the cache still holds it", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter, knownState("web", oldUID, "hash-old"))
		h.reader.setWasActive(filter, true)
		h.scopes.markSynced(podScope)

		// The object was recreated while nobody was watching: history and the live
		// cluster disagree on its UID, and no worker has caught up yet.
		h.lister.set(podKey("web"), newPod("web", newUID, "11", "nginx"))

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)

		waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
			func() string { return "one close-out row for the old incarnation" })
		if got := h.deletedRecords()[0].UID; got != oldUID {
			t.Errorf("close-out row uid = %q, want %q", got, oldUID)
		}
	})
}

// TestWarmScopeClassifiesIncarnationsFromHistory covers the close-out recovery a
// restart across a delete-and-recreate depends on (Task 1.12).
//
// The live reincarnation branch cannot fire here: the successor is observed and
// Reserved before the warm-up finishes, so the dedup cache never holds the old
// UID, and gcPass's UID-gated claim is then correctly refused. The evidence
// survives only in history, as two incarnations of one identity where the older
// has no Deleted row of its own — and that is what the seed-time classification
// reads it from.
func TestWarmScopeClassifiesIncarnationsFromHistory(t *testing.T) {
	t.Run("the newest incarnation is seeded and swept, the prior is neither", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter,
			incarnation("web", oldUID, "hash-old", priorAPIVersion, priorTS),
			incarnation("web", newUID, "hash-new", "v1", successorTS),
		)

		seeded, priors, err := h.coord.seedScope(h.ctx, logr.Discard(), scopeRef{sink: testSink, scope: podScope})
		if err != nil {
			t.Fatalf("seedScope: %v", err)
		}

		wantSeeded := []gcTarget{{namespace: "default", name: "web", uid: newUID}}
		if !reflect.DeepEqual(seeded, wantSeeded) {
			t.Errorf("GC targets = %+v, want only the current incarnation %+v", seeded, wantSeeded)
		}
		wantPriors := []reincarnation{
			{namespace: "default", name: "web", uid: oldUID, apiVersion: priorAPIVersion, ts: priorTS},
		}
		if !reflect.DeepEqual(priors, wantPriors) {
			t.Errorf("unclosed priors = %+v, want %+v", priors, wantPriors)
		}

		// The cache holds the current incarnation. The prior is deliberately not
		// seeded: the key belongs to the successor, and a historical baseline for a
		// dead UID would suppress the successor's next genuine change.
		st := h.pipeline.sinks.get(testSink)
		entry, ok := st.cache.Load(podKey("web").cacheKey())
		if !ok || entry.UID != newUID || entry.Hash != "hash-new" {
			t.Errorf("seeded entry = %+v (present %v), want the current incarnation (uid %s, hash-new)",
				entry, ok, newUID)
		}
	})

	t.Run("the prior is closed out from its own history row", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter,
			incarnation("web", oldUID, "hash-old", priorAPIVersion, priorTS),
			incarnation("web", newUID, "hash-new", "v1", successorTS),
		)
		h.reader.setWasActive(filter, true)
		h.scopes.markSynced(podScope)
		// The successor is alive, so the GC pass finds nothing to collect and the
		// only Deleted row that can appear is the recovered close-out.
		h.lister.set(podKey("web"), newPod("web", newUID, "11", "nginx"))

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)

		waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
			func() string { return "one close-out row for the unclosed prior incarnation" })

		got := h.deletedRecords()[0]
		if got.UID != oldUID {
			t.Errorf("close-out uid = %q, want the prior incarnation's %q", got.UID, oldUID)
		}
		if got.APIVersion != priorAPIVersion {
			t.Errorf("close-out api_version = %q, want the prior incarnation's own %q", got.APIVersion, priorAPIVersion)
		}
		if !got.Timestamp.Equal(priorTS) {
			t.Errorf("close-out ts = %s, want the prior incarnation's own %s: a now-stamp would sort after"+
				" the successor's first row and exclude the live successor from every later warm",
				got.Timestamp, priorTS)
		}
		if got.Data != "" || got.Diff != "" || got.SHA256 != "" {
			t.Errorf("close-out carries data/diff/sha256 (%q/%q/%q); event_type alone marks a deletion in schema v1",
				got.Data, got.Diff, got.SHA256)
		}
		if got.Namespace != "default" || got.Name != "web" || got.Kind != "Pod" {
			t.Errorf("close-out identity = %s/%s (%s), want default/web (Pod)", got.Namespace, got.Name, got.Kind)
		}

		// Exactly one: the successor is alive, so nothing else may be recorded as
		// dead, and the recovery must not double-emit.
		stayFalse(t, func() bool { return len(h.deletedRecords()) > 1 },
			"close-out recovery emitted more than one Deleted row for one unrecorded death")
	})
}

// TestWarmScopeClosesOutAReincarnationTheGCPassCouldNotClaim covers the other
// ordering of the same restart, and the one the e2e suite actually produces.
//
// Here the warm's history read wins the race against the successor's own first
// row reaching the sink, so history returns a *single* incarnation: the old UID
// is seeded like any ordinary object and swept like one. The sweep then finds a
// different UID live under that name and its claim is refused — correctly, since
// the cache entry belongs to the successor — and with only the seed-time
// classification nobody would ever record that the old incarnation died.
//
// The refusal is the proof. The close-out still waits for history to catch up, so
// that the row it writes is dated from the old incarnation's own last event and
// stays byte-identical across re-emissions.
func TestWarmScopeClosesOutAReincarnationTheGCPassCouldNotClaim(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)

	// History as the seed read sees it: one incarnation, because the successor's
	// Snapshot row has not been flushed to the sink yet.
	h.reader.setStates(filter, incarnation("web", oldUID, "hash-old", priorAPIVersion, priorTS))
	h.reader.setWasActive(filter, true)
	h.scopes.markSynced(podScope)

	// A worker got to the successor first: the cache entry is the new
	// incarnation's, so the seed's StoreIfAbsent declines and the sweep's
	// UID-gated claim will be refused.
	st := h.pipeline.sinks.get(testSink)
	st.cache.Reserve(podKey("web").cacheKey(), CacheEntry{Hash: "hash-new", UID: newUID})
	h.lister.set(podKey("web"), newPod("web", newUID, "11", "nginx"))

	h.run(t)
	h.warmNow(podScope)
	waitFor(t, func() bool { return len(h.reader.historyReads()) > 0 },
		func() string { return "the seed read of a history that holds only the old incarnation" })

	// The successor's own first row reaches the sink now — the evidence the
	// close-out has to be dated from.
	h.reader.setStates(filter,
		incarnation("web", oldUID, "hash-old", priorAPIVersion, priorTS),
		incarnation("web", newUID, "hash-new", "v1", successorTS),
	)

	waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
		func() string { return "the close-out for the reincarnation the sweep could not claim" })

	got := h.deletedRecords()[0]
	if got.UID != oldUID {
		t.Errorf("close-out uid = %q, want the refused incarnation's %q", got.UID, oldUID)
	}
	if got.APIVersion != priorAPIVersion || !got.Timestamp.Equal(priorTS) {
		t.Errorf("close-out = (api_version %q, ts %s), want the history row's (%s, %s)",
			got.APIVersion, got.Timestamp, priorAPIVersion, priorTS)
	}
	if got.Data != "" || got.Diff != "" || got.SHA256 != "" {
		t.Errorf("close-out carries data/diff/sha256 (%q/%q/%q), want all empty", got.Data, got.Diff, got.SHA256)
	}

	// The refusal stands: recording the old incarnation's death must not disturb
	// the live successor's entry, which no close-out ever claims (Invariant 3).
	entry, ok := st.cache.Load(podKey("web").cacheKey())
	if !ok || entry.UID != newUID || entry.PendingDelete {
		t.Errorf("the live successor's entry was disturbed by the close-out: %+v (present %v)", entry, ok)
	}
	stayFalse(t, func() bool { return len(h.deletedRecords()) > 1 },
		"the refused reincarnation was closed out more than once")
}

// gcSweep drives one zombie sweep over targets directly, with no seeding, so a test
// can put the dedup cache into a state seedScope would otherwise overwrite (or
// decline to produce at all — an absent entry cannot survive a seed).
func (h *warmHarness) gcSweep(t *testing.T, targets ...gcTarget) gcResult {
	t.Helper()
	result, err := h.coord.gcPass(h.ctx, logr.Discard(), scopeRef{sink: testSink, scope: podScope}, targets)
	if err != nil {
		t.Fatalf("gcPass: %v", err)
	}
	return result
}

// webTarget is the GC target every refusal-classification test below sweeps: the
// old incarnation of default/web, as a point-in-time read of history believed it.
var webTarget = gcTarget{namespace: "default", name: "web", uid: oldUID}

// TestGCPassDoesNotRecoverADeleteClaimAlreadyInFlight is the refusal-reason guard.
//
// hashCache.ReserveDelete refuses for three materially different reasons, and the
// sweep must recover only one of them. Here an earlier attempt of this very sweep
// already claimed the old incarnation's deletion — the pass errored on a later
// target and was retried, so nothing has released the claim and its Deleted row is
// still on its way to the sink. The claim is refused with deleteClaimInFlight.
//
// The live indexer cannot tell that apart from the case that *does* need
// recovering: awaitScopeSync gates the sweep on HasSynced, so the successor is
// guaranteed to be visible under a different UID either way. Classifying on the
// indexer alone therefore hands this target to recoverRefusedReincarnations by
// default, which re-reads history, sees an incarnation with no landed Deleted row
// (an unwritten row is invisible to history), and emits a *second* close-out for
// one UID — one stamped time.Now() by emitDelete, one dated from history, so
// resource_states' ReplacingMergeTree cannot collapse them.
func TestGCPassDoesNotRecoverADeleteClaimAlreadyInFlight(t *testing.T) {
	h := newWarmHarness(t)
	key := podKey("web")

	// An earlier sweep attempt owns this deletion; its write has not settled.
	st := h.pipeline.sinks.get(testSink)
	st.cache.Reserve(key.cacheKey(), CacheEntry{Hash: "hash-old", UID: oldUID})
	_, claimVersion, outcome := st.cache.ReserveDelete(key.cacheKey(), oldUID)
	if outcome != deleteClaimed {
		t.Fatalf("seeding the in-flight claim: outcome = %s, want claimed", outcome)
	}

	// The successor is live under a new UID, exactly as the synced indexer reports
	// it in the case this must not be confused with.
	h.lister.set(key, newPod("web", newUID, "11", "nginx"))

	result := h.gcSweep(t, webTarget)

	if len(result.reincarnated) != 0 {
		t.Errorf("reincarnated = %+v, want empty: this deletion's row is already in flight, and recovering"+
			" it would write a second, differently-timestamped Deleted row for one UID", result.reincarnated)
	}
	if result.zombies != 0 {
		t.Errorf("zombies = %d, want 0: the claim was refused, so this pass enqueued nothing", result.zombies)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Errorf("the sweep enqueued %+v, want nothing", got)
	}

	// The pre-existing claim belongs to the earlier attempt and must be left
	// exactly as it was — neither released nor re-versioned (Invariant 3).
	entry, ok := st.cache.Load(key.cacheKey())
	if !ok || !entry.PendingDelete || entry.Version != claimVersion || entry.UID != oldUID {
		t.Errorf("the in-flight claim was disturbed: %+v (present %v), want %s still pending at version %d",
			entry, ok, oldUID, claimVersion)
	}
}

// TestGCPassStillRecoversAGenuineUIDMismatch is the other side of that guard: the
// narrowed classification must not over-correct and drop the case Task 1.12 exists
// for.
//
// The cache holds the successor (a worker Reserved it), so the claim is refused
// with deleteClaimUIDMismatch — the key changed hands, nobody else owns the old
// UID's deletion, and its death would otherwise never reach the audit trail. The
// recovered row is still dated from history, not from the recovery.
func TestGCPassStillRecoversAGenuineUIDMismatch(t *testing.T) {
	h := newWarmHarness(t)
	key := podKey("web")
	filter := scopeFilterFor(podScope)

	// A worker got to the successor first: the key belongs to UID-B, unclaimed.
	st := h.pipeline.sinks.get(testSink)
	st.cache.Reserve(key.cacheKey(), CacheEntry{Hash: "hash-new", UID: newUID})
	h.lister.set(key, newPod("web", newUID, "11", "nginx"))

	// History has caught up by now: both incarnations are on record, so the
	// close-out has a prior row to be dated from.
	h.reader.setStates(filter,
		incarnation("web", oldUID, "hash-old", priorAPIVersion, priorTS),
		incarnation("web", newUID, "hash-new", "v1", successorTS),
	)

	result := h.gcSweep(t, webTarget)

	wantRefused := []gcTarget{webTarget}
	if !reflect.DeepEqual(result.reincarnated, wantRefused) {
		t.Fatalf("reincarnated = %+v, want %+v: a UID mismatch is the one refusal that leaves a death unrecorded",
			result.reincarnated, wantRefused)
	}

	recovered := h.coord.recoverRefusedReincarnations(h.ctx, logr.Discard(),
		scopeRef{sink: testSink, scope: podScope}, result.reincarnated)
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}

	deleted := h.deletedRecords()
	if len(deleted) != 1 {
		t.Fatalf("Deleted rows = %+v, want exactly one close-out for the old incarnation", deleted)
	}
	got := deleted[0]
	if got.UID != oldUID {
		t.Errorf("close-out uid = %q, want the refused incarnation's %q", got.UID, oldUID)
	}
	if !got.Timestamp.Equal(priorTS) || got.APIVersion != priorAPIVersion {
		t.Errorf("close-out = (ts %s, api_version %q), want the history row's (%s, %q)",
			got.Timestamp, got.APIVersion, priorTS, priorAPIVersion)
	}
	if got.Data != "" || got.Diff != "" || got.SHA256 != "" {
		t.Errorf("close-out carries data/diff/sha256 (%q/%q/%q), want all empty", got.Data, got.Diff, got.SHA256)
	}

	// The refusal stands: the live successor's entry is never claimed.
	entry, ok := st.cache.Load(key.cacheKey())
	if !ok || entry.UID != newUID || entry.PendingDelete {
		t.Errorf("the live successor's entry was disturbed: %+v (present %v)", entry, ok)
	}
}

// TestGCPassDoesNotRecoverAnAbsentCacheEntry covers the third refusal reason. With
// no entry for the key there is nothing to claim and, crucially, no cache-side
// evidence that a successor owns it: the indexer's live UID alone says only that
// *something* answers to that name now, which is equally true after this key's
// Deleted row already landed and removed the entry. Recovering on that would
// re-emit a close-out for a UID whose death is already recorded.
func TestGCPassDoesNotRecoverAnAbsentCacheEntry(t *testing.T) {
	h := newWarmHarness(t)
	key := podKey("web")

	// Nothing is seeded into the cache, and a different UID is live under the name.
	h.lister.set(key, newPod("web", newUID, "11", "nginx"))

	result := h.gcSweep(t, webTarget)

	if len(result.reincarnated) != 0 {
		t.Errorf("reincarnated = %+v, want empty: an absent entry is no evidence that a successor owns the key",
			result.reincarnated)
	}
	if result.zombies != 0 {
		t.Errorf("zombies = %d, want 0", result.zombies)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Errorf("the sweep enqueued %+v, want nothing", got)
	}
	if entry, ok := h.pipeline.sinks.get(testSink).cache.Load(key.cacheKey()); ok {
		t.Errorf("the refused claim created a cache entry: %+v", entry)
	}
}

// TestCloseOutsAreNeverEmittedForPreHistoryWhenTheScopeWasNeverActive is the epoch
// gate on close-out recovery, and the most damaging failure this component has.
//
// A brand-new rule over a kind that carries older history from some other scope
// will find unclosed incarnations in that history. This process never watched
// them, so recording their deaths would fabricate deletions for pre-history —
// exactly what the same gate stops the zombie GC pass from doing. The gate is not
// optional and is not a heuristic: no previous open epoch, no Deleted rows of any
// kind.
func TestCloseOutsAreNeverEmittedForPreHistoryWhenTheScopeWasNeverActive(t *testing.T) {
	// A different name from the other recovery tests, so this scope's history
	// cannot be confused with theirs when both run in the same package.
	const object = "api"
	history := []sink.KnownState{
		incarnation(object, oldUID, "hash-old", priorAPIVersion, priorTS),
		incarnation(object, newUID, "hash-new", "v1", successorTS),
	}

	t.Run("no previous open epoch: nothing is written at all", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter, history...)
		h.reader.setWasActive(filter, false)
		h.scopes.markSynced(podScope)
		h.lister.set(podKey(object), newPod(object, newUID, "11", "nginx"))

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)
		waitFor(t, func() bool { return len(h.reader.epochProbes()) > 0 },
			func() string { return "the epoch check to run" })

		stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
			"an unclosed incarnation in pre-history was fabricated into a deletion")
	})

	t.Run("a previous open epoch: exactly one Deleted row", func(t *testing.T) {
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter, history...)
		h.reader.setWasActive(filter, true)
		h.scopes.markSynced(podScope)
		h.lister.set(podKey(object), newPod(object, newUID, "11", "nginx"))

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)

		waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
			func() string { return "the close-out row once the epoch check passes" })
		if got := h.deletedRecords()[0].UID; got != oldUID {
			t.Errorf("Deleted row uid = %q, want the prior incarnation's %q", got, oldUID)
		}
		stayFalse(t, func() bool { return len(h.deletedRecords()) > 1 },
			"more than one Deleted row was written for one unrecorded death")
	})
}

// TestCloseOutRecordIsDeterministicFromHistory proves the idempotency the retry
// path depends on: every field of a close-out comes from history, so re-emitting
// one (the write failed, the process died mid-retry, the scope was re-warmed
// before the row landed) produces a byte-identical row that resource_states'
// ReplacingMergeTree collapses on merge.
//
// The equivalent assertion one layer down is TestInsertArgsTimestampFrozen in
// internal/sink/clickhouse, which proves insertArgs is a pure function of the
// record — including the timestamp, the one field a "fix" to time.Now() would
// make vary. Because that holds, an identical record renders to identical
// positional args, and this test asserts the record half here rather than
// importing an unexported helper from a package that already imports this one.
func TestCloseOutRecordIsDeterministicFromHistory(t *testing.T) {
	prior := reincarnation{
		namespace:  "default",
		name:       "web",
		uid:        oldUID,
		apiVersion: priorAPIVersion,
		ts:         priorTS,
	}
	key := Key{Sink: testSink, Group: "", Kind: "Pod", Namespace: "default", Name: "web"}

	first := prior.closeOutRecord(testClusterID, key)
	second := prior.closeOutRecord(testClusterID, key)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("close-out records differ between builds:\n first  = %+v\n second = %+v", first, second)
	}
	if !first.Timestamp.Equal(priorTS) {
		t.Errorf("close-out ts = %s, want the history instant %s; a time.Now() stamp would make every"+
			" re-emission a new row instead of a duplicate ReplacingMergeTree can collapse",
			first.Timestamp, priorTS)
	}
	if first.EventType != "Deleted" || first.UID != oldUID || first.APIVersion != priorAPIVersion {
		t.Errorf("close-out = %+v, want a Deleted row for %s at api_version %s", first, oldUID, priorAPIVersion)
	}
	if first.Data != "" || first.Diff != "" || first.SHA256 != "" {
		t.Errorf("close-out carries data/diff/sha256, want all empty: %+v", first)
	}
}

// TestEmitCloseOutsNeedsNoInformerSync covers the close-outs-without-GC-targets
// path: the recovery compares nothing against the live cache — its evidence is
// entirely historical — so it neither consults the informer's readiness nor waits
// for it. That is why warm gates only the GC pass on awaitScopeSync
// (`len(seeded) > 0 && !c.awaitScopeSync(...)`), and a warm with nothing to sweep
// proceeds straight to the epoch check.
//
// It drives emitCloseOuts directly because seedScope always yields a current
// incarnation alongside any prior (one identity's newest row is the seed by
// definition), so an empty target list with a non-empty prior list is a state the
// classification cannot produce — the guard is defensive, and this is the
// behaviour it protects.
func TestEmitCloseOutsNeedsNoInformerSync(t *testing.T) {
	h := newWarmHarness(t)
	// Deliberately never synced, and no live object for the key at all.
	h.run(t)

	priors := []reincarnation{
		{namespace: "default", name: "web", uid: oldUID, apiVersion: priorAPIVersion, ts: priorTS},
	}
	recovered := h.coord.emitCloseOuts(h.ctx, logr.Discard(), scopeRef{sink: testSink, scope: podScope}, priors)
	if recovered != 1 {
		t.Fatalf("emitCloseOuts returned %d, want 1", recovered)
	}

	waitFor(t, func() bool { return len(h.deletedRecords()) == 1 },
		func() string { return "the close-out row without the informer ever reporting synced" })
	if got := h.deletedRecords()[0].UID; got != oldUID {
		t.Errorf("Deleted row uid = %q, want %q", got, oldUID)
	}
	if n := h.scopes.syncChecked(); n != 0 {
		t.Errorf("close-out recovery consulted the informer's readiness %d times, want 0", n)
	}
}

// TestForgetSinkRestoresBootReconciliationAndCancelsWarms covers the second half
// of Task 1.12: Pipeline.RemoveSink discards a deleted sink's caches, but the
// coordinator used to keep the sink marked boot-reconciled forever. A sink deleted
// and re-created under the same name would then never have its boot pass run
// again, so scopes orphaned during its absence would stay open in watch_scopes
// indefinitely — a self-heal that silently stops working.
func TestForgetSinkRestoresBootReconciliationAndCancelsWarms(t *testing.T) {
	h := newWarmHarness(t)

	// An orphan nothing desires, distinct from the scope warmed below so the
	// second boot pass still has something to close.
	orphanScope := ScopeKey{Group: "", Kind: "ConfigMap", Namespace: "default"}
	orphan := scopeFilterFor(orphanScope)
	h.reader.setActiveScopes(orphan)
	h.reader.setStates(scopeFilterFor(podScope), knownState("web", "uid-web", "hash-web"))

	h.run(t)

	h.events.awaitEvents(t, 1)
	stayFalse(t, func() bool { return len(h.events.recorded()) > 1 },
		"the boot pass re-ran for a sink it had already reconciled")
	passesBefore := h.reader.activeScopesCallCount()

	// A warm for this sink, caught mid-read.
	release := h.reader.blockLastKnownStates()
	defer release()
	h.warmNow(podScope)
	waitFor(t, func() bool { return len(h.reader.historyReads()) > 0 },
		func() string { return "the warm to reach the blocked history read" })

	// The sink is deleted: the SinkManager evicts its pipeline state and, next to
	// that call, forgets it here.
	h.coord.ForgetSink(testSink)
	h.coord.ForgetSink(clickHouseSink("a-sink-that-never-existed")) // safe by contract
	release()

	// The boot pass runs again for the re-created sink, closing the scope that was
	// orphaned while it was gone.
	h.events.awaitEvents(t, 2)
	if n := h.reader.activeScopesCallCount(); n <= passesBefore {
		t.Errorf("ActiveScopes was called %d times, want more than the %d before ForgetSink", n, passesBefore)
	}
	for _, event := range h.events.recorded() {
		if event.Action != sink.ScopeActionStopped || event.Scope != orphan {
			t.Errorf("scope event = %+v, want a Stopped row for the orphan %+v", event, orphan)
		}
	}

	// The in-flight warm was cancelled with the sink, so it neither marked its
	// scope warm nor wrote anything for a sink that is gone.
	st := h.pipeline.sinks.get(testSink)
	if st.scopeWarm(Key{Sink: testSink, Group: "", Kind: "Pod", Namespace: "default"}) {
		t.Error("a warm cancelled by ForgetSink still marked its scope warm")
	}
	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"a warm cancelled by ForgetSink still wrote resource_states rows")
}

// TestWarmScopeWarmsOnlyItsOwnScope is the "new rule on a live cluster" criterion:
// warm-up is per-scope and incremental, so a rule created hours after boot must not
// touch any other scope's history, cache or readiness.
func TestWarmScopeWarmsOnlyItsOwnScope(t *testing.T) {
	h := newWarmHarness(t)

	otherScope := ScopeKey{Group: "apps", Kind: "Deployment", Namespace: "default"}
	mine, theirs := scopeFilterFor(podScope), scopeFilterFor(otherScope)
	h.reader.setStates(mine, knownState("web", "uid-web", "hash-web"))
	h.reader.setStates(theirs, knownState("api", "uid-api", "hash-api"))
	h.reader.setWasActive(mine, true)
	h.reader.setWasActive(theirs, true)
	h.scopes.markSynced(podScope)
	h.scopes.markSynced(otherScope)
	h.lister.set(podKey("web"), newPod("web", "uid-web", "2", "nginx"))

	h.run(t)
	h.warmNow(podScope)
	h.awaitWarm(t, podScope)

	// No StateReader call for any other scope — neither for its objects nor for its
	// epoch history.
	for _, filter := range h.reader.historyReads() {
		if filter != mine {
			t.Errorf("warm-up read history for a scope it was not asked about: %+v", filter)
		}
	}
	for _, probe := range h.reader.epochProbes() {
		if probe.filter != mine {
			t.Errorf("warm-up probed the epoch of a scope it was not asked about: %+v", probe.filter)
		}
	}

	// And the other scope stays cold, so its own cache misses still tag Snapshot.
	st := h.pipeline.sinks.get(testSink)
	if st.scopeWarm(Key{Sink: testSink, Group: "apps", Kind: "Deployment", Namespace: "default"}) {
		t.Error("warming one scope marked another scope warm")
	}
	if _, ok := st.cache.Load(Key{Sink: testSink, Group: "apps", Kind: "Deployment",
		Namespace: "default", Name: "api"}.cacheKey()); ok {
		t.Error("warming one scope seeded another scope's baselines")
	}
}

// TestWarmScopeStaysColdUntilTheSinkAnswers covers the degradation Invariant 5
// requires: while the sink cannot be read, the scope is *not* marked warm, so cache
// misses tag Snapshot instead of flooding the sink with Added rows — and the warm
// keeps retrying rather than giving up.
func TestWarmScopeStaysColdUntilTheSinkAnswers(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", "uid-web", "hash-web"))
	h.reader.failNextLastKnownStates(
		errors.New("clickhouse unavailable"),
		errors.New("clickhouse unavailable"),
		errors.New("clickhouse unavailable"),
	)

	h.run(t)
	h.warmNow(podScope)

	h.awaitWarm(t, podScope)
	if reads := len(h.reader.historyReads()); reads < 4 {
		t.Errorf("LastKnownStates was called %d times, want at least 4 (three failures plus the success)", reads)
	}
	if h.logs.countOf(nil) != 0 {
		t.Error("a nil error was logged")
	}
}

// TestWarmScopeDisabledForASinkThatCannotReadItsHistory covers the Writer-only
// sink: warm-up and GC are disabled, the scope stays permanently in Snapshot mode
// (the safe direction), and the goroutine does not spin retrying forever.
//
// Three scopes, not one, and that is the point of the shape. The coordinator asks
// for history once per scope, so the naive implementation announces the missing
// read half once per scope too — and a real cluster has hundreds of scopes per
// sink. A storm of expected lines is how the *unexpected* ones stop being read, so
// Task 6.5 requires one informational line per sink and nothing at Error.
func TestWarmScopeDisabledForASinkThatCannotReadItsHistory(t *testing.T) {
	h := newWarmHarness(t)
	h.backends.removeReader(testSink)

	scopes := []ScopeKey{
		podScope,
		{Group: "", Kind: "Pod", Namespace: "other"},
		{Group: "apps", Kind: "Deployment", Namespace: "default"},
	}

	h.run(t)
	for _, scope := range scopes {
		h.warmNow(scope)
	}

	for _, scope := range scopes {
		stayFalse(t, func() bool {
			st := h.pipeline.sinks.get(testSink)
			return st.scopeWarm(Key{Sink: testSink, Group: scope.Group, Kind: scope.Kind, Namespace: scope.Namespace})
		}, "a sink with no StateReader marked its scope warm, which would let a cache miss claim an object is genuinely new")
	}

	// Not one call, for any scope: the reader is never resolved, so nothing even
	// attempts to read an archive that cannot answer.
	if reads := h.reader.historyReads(); len(reads) != 0 {
		t.Errorf("history was read for a sink with no StateReader: %+v", reads)
	}
	if calls := h.reader.activeScopesCallCount(); calls != 0 {
		t.Errorf("ActiveScopes was called %d times for a sink with no StateReader, want 0", calls)
	}
	if calls := h.reader.epochProbes(); len(calls) != 0 {
		t.Errorf("ScopeWasActive was called for a sink with no StateReader: %+v", calls)
	}

	// Exactly one line, for three scopes plus the boot pass — waited for rather
	// than read, for the same reason as in the restart test: the announcement is
	// made on a warm goroutine. This one survives a single read today only because
	// the stayFalse loops above happen to give it a second first, which makes it a
	// property of the assertion order rather than of the code under test.
	waitFor(t, func() bool { return len(h.writerOnlyAnnouncements()) == 1 },
		func() string {
			return fmt.Sprintf("exactly one announcement across %d scopes, got %d",
				len(scopes), len(h.writerOnlyAnnouncements()))
		})
	stayFalse(t, func() bool { return len(h.writerOnlyAnnouncements()) > 1 },
		"the missing read half was announced more than once, so it is being announced per scope")
	announced := h.writerOnlyAnnouncements()
	// And it names all three behaviours it switches off, so the line is worth the
	// one time it is printed.
	for _, want := range []string{"cache warm-up", "zombie garbage collection", "boot reconciliation"} {
		if !strings.Contains(announced[0], want) {
			t.Errorf("the announcement does not name %q:\n%s", want, announced[0])
		}
	}
	// Never at Error. Nothing has gone wrong: the backend is doing what it was
	// built to do, and an Error here would put a permanent fault in every
	// operator's log for a declared design decision (D12).
	if errs := h.logs.loggedErrors(); len(errs) != 0 {
		t.Errorf("a Writer-only sink logged %d errors, want none: %v", len(errs), errs)
	}
}

// TestWriterOnlySinkIsAnnouncedAgainAfterItIsForgotten pairs with ForgetSink's
// clearing of the boot-reconciliation mark, for the same reason that exists.
//
// A sink deleted and re-created under one identity is a new detection. If the
// announcement latched for the life of the process, the operator who created the
// replacement would get no line at all — and "I applied a sink and the log said
// nothing about it" is indistinguishable from "the sink was never picked up".
func TestWriterOnlySinkIsAnnouncedAgainAfterItIsForgotten(t *testing.T) {
	h := newWarmHarness(t)
	h.backends.removeReader(testSink)

	h.run(t)
	h.warmNow(podScope)
	waitFor(t, func() bool { return len(h.writerOnlyAnnouncements()) == 1 },
		func() string { return "the first announcement" })

	h.coord.ForgetSink(testSink)
	h.warmNow(podScope)

	waitFor(t, func() bool { return len(h.writerOnlyAnnouncements()) == 2 },
		func() string {
			return fmt.Sprintf("a second announcement after ForgetSink, got %d",
				len(h.writerOnlyAnnouncements()))
		})
}

// TestWriterOnlySinkProducesNoDeletedRowsFromTheGCPath is Task 6.5's test (d), and
// it is deliberately a *paired* test: one fixture, run twice, differing only in
// whether the sink can read its own history back.
//
// The fixture is the textbook zombie — history holds two objects, reality has one,
// the scope's log says it was watched in a previous epoch, the informer reports
// synced — which on a ClickHouseSink is exactly the case that must produce one
// Deleted row. Asserting zero rows against a Writer-only sink means nothing on its
// own (a fixture that produces nothing anywhere would pass); asserting that the
// *same* fixture produces one row on a readable sink is what makes the zero
// meaningful.
//
// And the zero is the property that makes the archive's silence explicable. An S3
// archive contains no Deleted records at all, so a reader must be able to trust
// that this is a documented, total absence rather than a lossy one — a sink that
// emitted deletions sometimes would be far worse than one that never does.
func TestWriterOnlySinkProducesNoDeletedRowsFromTheGCPath(t *testing.T) {
	// setup installs the identical zombie fixture on a fresh harness.
	setup := func(t *testing.T) *warmHarness {
		t.Helper()
		h := newWarmHarness(t)
		filter := scopeFilterFor(podScope)
		h.reader.setStates(filter,
			knownState("gone", "uid-gone", "hash-gone"),
			knownState("alive", "uid-alive", "hash-alive"))
		h.reader.setWasActive(filter, true)
		h.lister.set(podKey("alive"), newPod("alive", "uid-alive", "7", "nginx"))
		h.scopes.markSynced(podScope)
		return h
	}

	t.Run("a sink that can read its history writes exactly one Deleted row", func(t *testing.T) {
		h := setup(t)

		h.run(t)
		h.warmNow(podScope)
		h.awaitWarm(t, podScope)

		waitFor(t, func() bool { return len(h.deletedRecords()) >= 1 },
			func() string { return "one Deleted row for the zombie" })
		if deleted := h.deletedRecords(); len(deleted) != 1 {
			t.Fatalf("wrote %d Deleted rows, want exactly 1: %+v", len(deleted), deleted)
		}
	})

	t.Run("the same fixture on a Writer-only sink writes none", func(t *testing.T) {
		h := setup(t)
		// The only difference. Everything else — the history, the epoch, the live
		// object, the synced informer — is identical.
		h.backends.removeReader(testSink)

		h.run(t)
		h.warmNow(podScope)

		stayFalse(t, func() bool { return len(h.deletedRecords()) > 0 },
			"a Writer-only sink was sent a Deleted row by the GC path: the archive's silence about deletions "+
				"must be total, since a reader cannot tell a rare fabricated deletion from a real one")
		// Nor a close-out, which is the *other* Deleted-row producer on this path:
		// the fixture has no reincarnation, but a pass that ran at all would have
		// had to read history to find that out.
		if records := h.writer.recorded(); len(records) != 0 {
			t.Errorf("the GC path wrote %d records to a Writer-only sink, want none: %+v", len(records), records)
		}
	})
}

// TestWarmScopeRetriesWhileTheSinkIsNotLiveYet covers the other absent-reader case:
// a rule applied before its ClickHouseSink became ready must be warmed once the sink
// arrives, not written off.
func TestWarmScopeRetriesWhileTheSinkIsNotLiveYet(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", "uid-web", "hash-web"))

	// Neither a reader nor a live writer: the sink simply does not exist yet.
	h.backends.removeReader(testSink)
	h.router.remove(testSink)

	h.run(t)
	h.warmNow(podScope)

	stayFalse(t, func() bool { return len(h.reader.historyReads()) > 0 },
		"history was read for a sink that is not live")

	h.router.set(testSink, h.writer)
	h.backends.setReader(testSink, h.reader)
	h.awaitWarm(t, podScope)
}

// TestWarmScopeIsIdempotentPerEpoch proves the recorder does not have to remember
// which scopes it has handed over: a repeated request for an epoch already warmed is
// dropped, while a genuinely new epoch (the scope stopped and started again) is
// warmed afresh.
func TestWarmScopeIsIdempotentPerEpoch(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", "uid-web", "hash-web"))
	h.lister.set(podKey("web"), newPod("web", "uid-web", "2", "nginx"))
	h.scopes.markSynced(podScope)

	h.run(t)
	epoch := h.warmNow(podScope)
	h.awaitWarm(t, podScope)
	waitFor(t, func() bool { return len(h.reader.historyReads()) == 1 },
		func() string { return "the first warm's history read" })

	// Same epoch again: nothing to do.
	h.coord.WarmScope(WarmTarget{Sink: testSink, Scope: podScope, EpochStart: epoch})
	stayFalse(t, func() bool { return len(h.reader.historyReads()) > 1 },
		"a repeated warm request for the same epoch re-read the sink's history")

	// A newer epoch supersedes it.
	h.coord.WarmScope(WarmTarget{Sink: testSink, Scope: podScope, EpochStart: epoch.Add(time.Second)})
	waitFor(t, func() bool { return len(h.reader.historyReads()) == 2 },
		func() string { return "the new epoch's history read" })
}

// TestStopScopeCancelsAnInFlightWarm proves a stopped scope's warm is abandoned
// mid-flight. Without it, a warm that was already past its epoch check could emit
// Deleted rows for a scope whose Stopped row had just been written — the audit lie
// in its subtlest form.
func TestStopScopeCancelsAnInFlightWarm(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("gone", "uid-gone", "hash-gone"))
	h.reader.setWasActive(filter, true)
	h.scopes.markSynced(podScope)

	release := h.reader.blockLastKnownStates()
	defer release()

	h.run(t)
	h.warmNow(podScope)
	waitFor(t, func() bool { return len(h.reader.historyReads()) > 0 },
		func() string { return "the warm to reach the blocked history read" })

	h.coord.StopScope(testSink, podScope)
	release()

	stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
		"a cancelled warm still wrote rows for its stopped scope")
	st := h.pipeline.sinks.get(testSink)
	if st.scopeWarm(Key{Sink: testSink, Group: "", Kind: "Pod", Namespace: "default"}) {
		t.Error("a cancelled warm marked its scope warm")
	}
}

// TestWarmScopeRequestedBeforeStartIsNotLost covers the runnable ordering the
// manager gives no guarantee about: the WatchManager may reconcile — and therefore
// request a warm — before this coordinator's Start runs.
func TestWarmScopeRequestedBeforeStartIsNotLost(t *testing.T) {
	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", "uid-web", "hash-web"))
	h.lister.set(podKey("web"), newPod("web", "uid-web", "2", "nginx"))
	h.scopes.markSynced(podScope)

	// Requested while the coordinator is not running yet.
	h.warmNow(podScope)
	stayFalse(t, func() bool { return len(h.reader.historyReads()) > 0 },
		"a warm ran before the coordinator started")

	h.run(t)
	h.awaitWarm(t, podScope)
}

// TestWarmCoordinatorStopsCleanly asserts the shutdown contract: Start returns only
// once every warm goroutine it spawned has exited, so the process leaves nothing
// behind (a goleak-verified property).
func TestWarmCoordinatorStopsCleanly(t *testing.T) {
	snapshot := goleak.IgnoreCurrent()

	h := newWarmHarness(t)
	filter := scopeFilterFor(podScope)
	h.reader.setStates(filter, knownState("web", "uid-web", "hash-web"))
	release := h.reader.blockLastKnownStates()
	defer release()

	stop := h.run(t)
	h.warmNow(podScope)
	waitFor(t, func() bool { return len(h.reader.historyReads()) > 0 },
		func() string { return "the warm to be in flight" })

	// Cancelling with a warm blocked mid-read is the case that would strand a
	// goroutine if shutdown did not cancel and wait for each run.
	stop()

	// A warm requested after shutdown is dropped rather than starting a goroutine
	// nobody will ever wait for.
	h.coord.WarmScope(WarmTarget{Sink: testSink, Scope: podScope, EpochStart: time.Now()})

	// The Pipeline's own delaying queue owns a background goroutine that outlives
	// the coordinator by design (the harness shuts it down in a cleanup, which runs
	// after this check), so it is retired explicitly before the snapshot comparison.
	h.pipeline.queue.ShutDown()
	goleak.VerifyNone(t, snapshot)
}

// --- Events mode (Task 3.1) ---
//
// An Events scope takes exactly step 1 of the warm and none of the rest: the seed
// runs, so a restart deduplicates against Events already on record, but nothing is
// ever reconciled *away* from history. The two specs below are the two halves of
// that claim — no claims made, and the seed actually suppressing re-emission.

// eventScopeFor renders an Events watch scope in one of the two accepted groups.
func eventScopeFor(group string) ScopeKey {
	return ScopeKey{Group: group, Kind: "Event", Namespace: "default"}
}

// restartWarm stands in for an operator restart for a test that needs the warm
// coordinator on the other side of it. testHarness.restart alone is not enough: the
// coordinator holds its Pipeline by reference, so a restart that replaced only the
// pipeline would leave the old coordinator seeding a dead process's caches. The
// doubles (lister, sink history, scope desire) survive, exactly as the API server
// and ClickHouse do.
func (h *warmHarness) restartWarm(t *testing.T) {
	t.Helper()
	h.restart(t)
	coord, err := NewWarmCoordinator(WarmOptions{
		Pipeline:         h.pipeline,
		Scopes:           h.scopes,
		Readers:          h.backends,
		ScopeEvents:      h.backends,
		RetryMaxInterval: 5 * time.Millisecond,
		SyncPollInterval: time.Millisecond,
		BootInterval:     5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWarmCoordinator after restart: %v", err)
	}
	h.coord = coord
}

// TestWarmEventsScopeSeedsButClaimsNoDeletes is the GC half. The fixture is
// deliberately the one that makes a Pod scope emit a Deleted row — history holds an
// object reality does not, the epoch says the scope was watched before, the informer
// reports synced — and for Events it must produce nothing at all: that object is an
// Event that aged out, which is not a deletion and is never recorded as one.
func TestWarmEventsScopeSeedsButClaimsNoDeletes(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newWarmHarness(t)
			scope := eventScopeFor(gvk.group)
			filter := scopeFilterFor(scope)

			// History knows two Events; only one is still live. Under Pod semantics
			// "expired" is a textbook zombie.
			h.reader.setStates(filter,
				knownState("expired.17aaa", "uid-expired", "hash-expired"),
				knownState("alive.17bbb", "uid-alive", "hash-alive"))
			h.reader.setWasActive(filter, true)
			h.scopes.markSynced(scope)
			aliveKey := eventKey(gvk.group, "alive.17bbb")
			h.lister.set(aliveKey, newEvent(gvk.group, aliveKey.Name, "uid-alive", "7", 1))

			h.run(t)
			h.warmNow(scope)
			h.awaitWarm(t, scope)

			// Both baselines are seeded — that is the whole purpose of warming an
			// Events scope.
			st := h.pipeline.sinks.get(testSink)
			for _, want := range []struct{ key, hash string }{
				{eventKey(gvk.group, "alive.17bbb").cacheKey(), "hash-alive"},
				{eventKey(gvk.group, "expired.17aaa").cacheKey(), "hash-expired"},
			} {
				entry, ok := st.cache.Load(want.key)
				if !ok || entry.Hash != want.hash {
					t.Errorf("seeded entry for %s = %+v (present %v), want hash %s",
						want.key, entry, ok, want.hash)
				}
			}

			// And nothing was reconciled away: no Deleted row, and in fact no row.
			stayFalse(t, func() bool { return len(h.writer.recorded()) > 0 },
				"an Events scope wrote rows during warm-up; an expired Event is not a deletion")

			// The two gates the GC pass needs are never consulted, because the pass
			// never runs — which is what "zero delete claims" means structurally
			// rather than merely by outcome.
			if probes := h.reader.epochProbes(); len(probes) != 0 {
				t.Errorf("an Events scope probed the scope epoch %d times, want 0: %+v", len(probes), probes)
			}
			if n := h.scopes.syncChecked(); n != 0 {
				t.Errorf("an Events scope waited on informer sync %d times, want 0", n)
			}
		})
	}
}

// TestWarmEventsRestartDoesNotReEmitUnchangedEvents is the seeding half, and the
// reason warm-up runs for Events at all: after a restart, every live Event is a
// cache miss, and an Events scope never Snapshot-tags — so without the primed hashes
// the operator would re-emit an Added row for every Event still inside its TTL.
func TestWarmEventsRestartDoesNotReEmitUnchangedEvents(t *testing.T) {
	h := newWarmHarness(t)
	scope := eventScopeFor("")
	filter := scopeFilterFor(scope)
	key := eventKey("", "crasher.17abc")
	event := newEvent("", key.Name, "event-uid", "3", 2)

	h.scopes.markSynced(scope)
	h.reader.setWasActive(filter, true)
	h.lister.set(key, event)

	// Before the restart: the Event is observed once and recorded once.
	h.pipeline.MarkScopeWarm(testSink, scope)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(pre-restart): %v", err)
	}
	if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
		t.Fatalf("pre-restart event types = %v, want %v", got, want)
	}

	// The sink's history now holds that row. Its hash is computed through the same
	// function the write path used, so the seeded baseline is the real one rather
	// than a literal that would stop matching the first time normalization changed.
	hash, err := ObjectHash(event, nil)
	if err != nil {
		t.Fatalf("ObjectHash: %v", err)
	}
	h.reader.setStates(filter, knownState(key.Name, "event-uid", hash))

	// The operator restarts. Every in-memory cache is gone; the Event is still live
	// and still unchanged.
	h.restartWarm(t)
	h.run(t)
	h.warmNow(scope)
	h.awaitWarm(t, scope)

	// The informer's initial list re-delivers it.
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(post-restart): %v", err)
	}

	if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v — the seeded hash must deduplicate the re-delivery", got, want)
	}
	stayFalse(t, func() bool { return len(h.writer.recorded()) > 1 },
		"a restart re-emitted a row for a live, unchanged Event")
}

// TestWriterOnlySinkTagsEveryFirstSightingSnapshotAcrossARestart is Task 6.5's
// test (c): a sink that cannot read its own history can never claim an object is
// genuinely new, so no record it receives is ever tagged Added — not on the first
// process's watch, and not on the next one's.
//
// The restart is the half that matters. Within one process a Writer-only sink looks
// almost normal: the first sighting is a Snapshot and subsequent changes diff as
// Modified off the in-memory baseline, exactly as they would anywhere. It is the
// restart that exposes the limit — the baseline dies with the process and cannot be
// rebuilt from the archive, so every live object is a first sighting again and is
// re-snapshotted in full. That is the documented cost of the archive tier, and this
// test is what makes it a property rather than a claim.
//
// The counterpart is TestPipelineSnapshotTaggingUntilScopeWarm, where a scope that
// *does* warm goes on to tag its next miss Added. Without it the "never Added" here
// would be satisfiable by a pipeline that never emitted Added at all.
func TestWriterOnlySinkTagsEveryFirstSightingSnapshotAcrossARestart(t *testing.T) {
	h := newWarmHarness(t)
	h.backends.removeReader(testSink)

	web, api := podKey("web"), podKey("api")
	h.lister.set(web, newPod(web.Name, "uid-web", "1", "nginx:1"))
	h.lister.set(api, newPod(api.Name, "uid-api", "1", "envoy:1"))

	stop := h.run(t)
	h.warmNow(podScope)
	// The first process's announcement, waited for rather than assumed. It lands on
	// the warm goroutine warmNow starts, and it is what the count after the restart
	// is measured against, so reading it later without a barrier would make the
	// two-process claim below depend on goroutine scheduling.
	waitFor(t, func() bool { return len(h.writerOnlyAnnouncements()) == 1 },
		func() string { return "the first process to announce the missing read half" })

	// Two first sightings, both Snapshot: the scope is never marked warm, so the
	// hedge never lifts.
	for _, key := range []Key{web, api} {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(%s): %v", key.Name, err)
		}
	}
	// A change within one process still diffs, which is correct and is the reason
	// the claim is "never Added" rather than "only ever Snapshot": the archive holds
	// the Snapshot this diff is relative to, because this process wrote it.
	h.lister.set(web, newPod(web.Name, "uid-web", "2", "nginx:2"))
	if err := h.pipeline.Process(h.ctx, web); err != nil {
		t.Fatalf("Process(web after the edit): %v", err)
	}

	if got, want := h.writer.eventTypes(), []string{"Snapshot", "Snapshot", "Modified"}; !slices.Equal(got, want) {
		t.Fatalf("event types before the restart = %v, want %v", got, want)
	}
	// safe_mode is pinned at 1 for the scope, which is the metrics-side observation
	// of exactly this. Task 6.5 requires it stay there and forbids a second metric
	// saying the same thing.
	if got := testutil.ToFloat64(h.pipeline.metrics.safeMode.WithLabelValues(
		testSink.String(), podScope.Group, podScope.Kind, podScope.Namespace)); got != 1 {
		t.Errorf("safe_mode = %v on a Writer-only sink, want 1", got)
	}

	// The restart. The lister and the sink survive it, exactly as the API server and
	// the bucket do; every in-memory baseline does not.
	stop()
	h.restartWarm(t)
	h.run(t)
	h.warmNow(podScope)

	// Both live objects are first sightings again, and both are re-snapshotted in
	// full. This is the "full re-snapshot on every restart" the release summary
	// names as the visible cost of D12.
	for _, key := range []Key{web, api} {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(%s) after the restart: %v", key.Name, err)
		}
	}

	got := h.writer.eventTypes()
	want := []string{"Snapshot", "Snapshot", "Modified", "Snapshot", "Snapshot"}
	if !slices.Equal(got, want) {
		t.Fatalf("event types across the restart = %v, want %v", got, want)
	}
	// The invariant, stated over everything the sink ever received: Added is the one
	// event type that asserts "this object did not exist before", and nothing that
	// cannot read its own history is entitled to say it.
	for _, eventType := range got {
		if eventType == "Added" {
			t.Errorf("a Writer-only sink received an Added record; event types were %v", got)
		}
	}
	// Still pinned after the restart, on the new process's gauge.
	if got := testutil.ToFloat64(h.pipeline.metrics.safeMode.WithLabelValues(
		testSink.String(), podScope.Group, podScope.Kind, podScope.Namespace)); got != 1 {
		t.Errorf("safe_mode = %v after the restart, want 1", got)
	}
	// And the announcement is still one line per sink: the restarted coordinator is
	// a new object, so this also proves the dedup is per coordinator rather than
	// accidentally global — the second process is entitled to say it once too.
	//
	// Waited for and then held, rather than read once. The announcement is made on
	// the warm goroutine that the fire-and-forget warmNow above started, so a single
	// read here races it; the two halves together are what keep the claim exact —
	// waitFor establishes that each process said it, stayFalse that neither said it
	// twice.
	waitFor(t, func() bool { return len(h.writerOnlyAnnouncements()) == 2 },
		func() string {
			return fmt.Sprintf("one announcement from each of the two processes, got %d:\n%v",
				len(h.writerOnlyAnnouncements()), h.writerOnlyAnnouncements())
		})
	stayFalse(t, func() bool { return len(h.writerOnlyAnnouncements()) > 2 },
		"the missing read half was announced more than once in a process, so the per-sink dedup is not holding")
}
