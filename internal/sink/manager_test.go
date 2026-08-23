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

package sink

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/goleak"
)

// managerHarness is one test's sink runtime: a manager, the fakes behind it, and
// the shared clock its ordering assertions are made against.
type managerHarness struct {
	t *testing.T

	clock    *atomic.Int64
	mgr      *SinkManager
	pipe     *fakePipeline
	parks    *parkLog
	cancel   context.CancelFunc
	started  chan error
	instance func(label string) *fakeInstance

	mu sync.Mutex
	// built records every instance the factory produced, in order, so a test can
	// address "the instance before the rotation" and "the one after".
	built []*fakeInstance
	// buildErr, when non-nil, makes the next factory call fail.
	buildErr error
}

// newManagerHarness builds a manager whose factory hands out fakeInstances, with
// pacing shortened so no assertion waits on production timings.
func newManagerHarness(t *testing.T, tune func(*ManagerOptions)) *managerHarness {
	t.Helper()

	clock := &atomic.Int64{}
	h := &managerHarness{t: t, clock: clock, pipe: newFakePipeline(clock), parks: newParkLog(clock)}
	h.instance = func(label string) *fakeInstance { return newFakeInstance(label, clock) }

	opts := ManagerOptions{
		Pipeline:   h.pipe,
		Dependents: fakeDependents{rules: map[ID][]string{testID("primary"): {"team-a/audit", "team-b/audit"}}},
		OnSinkGone: h.parks.park,
		// Long enough that no test is re-probed by accident, short enough that the
		// probe test can drive the cadence itself by overriding these.
		ProbeInterval:   time.Hour,
		ProbeMinBackoff: time.Hour,
		ProbeMaxBackoff: time.Hour,
		ProbeTimeout:    time.Second,
		DrainTimeout:    2 * time.Second,
		Factory: func(id ID, cfg InstanceConfig) (Writer, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.buildErr != nil {
				err := h.buildErr
				h.buildErr = nil
				return nil, err
			}
			inst := h.instance(fmt.Sprintf("%s#%s", id.Name, cfg.Fingerprint()))
			h.built = append(h.built, inst)
			return inst, nil
		},
	}
	if tune != nil {
		tune(&opts)
	}

	mgr, err := NewSinkManager(opts)
	if err != nil {
		t.Fatalf("NewSinkManager: %v", err)
	}
	h.mgr = mgr
	return h
}

// start runs the manager, waits until it has installed its lifetime context, and
// registers its shutdown with the test.
//
// The wait is what makes the tests deterministic: Start is a runnable on its own
// goroutine, and an Ensure that lands before it has begun is legitimately held as
// pending (see TestEnsureBeforeStartIsAppliedWhenTheManagerRuns) rather than
// applied immediately. Every test that is not *about* that ordering wants the
// manager running first. Because Start installs the context and applies the
// pending set under one hold of mu, observing a non-nil ctx also means the pending
// set has been applied.
func (h *managerHarness) start() {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.started = make(chan error, 1)
	go func() { h.started <- h.mgr.Start(ctx) }()
	h.t.Cleanup(h.stop)

	waitFor(h.t, "the manager to install its lifetime context", func() bool {
		h.mgr.mu.Lock()
		defer h.mgr.mu.Unlock()
		return h.mgr.ctx != nil
	})
}

// stop cancels the manager and waits for Start to return. It is idempotent so a
// test may stop explicitly and still rely on the cleanup.
func (h *managerHarness) stop() {
	h.t.Helper()
	if h.cancel == nil {
		return
	}
	h.cancel()
	select {
	case err := <-h.started:
		if err != nil {
			h.t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		h.t.Fatal("Start did not return after cancellation")
	}
	h.cancel = nil
}

// instances returns the instances the factory has built so far, in order.
func (h *managerHarness) instances() []*fakeInstance {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*fakeInstance(nil), h.built...)
}

// writerFor resolves name through the manager, failing the test if no instance is
// routed for it.
func (h *managerHarness) writerFor(name string) Writer {
	h.t.Helper()
	w, ok := h.mgr.WriterFor(testID(name))
	if !ok {
		h.t.Fatalf("WriterFor(%q) reported no live writer", name)
	}
	return w
}

// TestSecretRotationSwapsInstancesWithoutLosingJobs covers AC (a): a credential
// rotation replaces the running instance, and every job enqueued before the swap
// settles exactly once on the *old* instance while post-swap jobs land on the new
// one.
//
// This is Invariant 3 across a recycle: the commit callbacks of in-flight jobs
// carry cache versions reserved against the sink's hashCache, so they must fire —
// once each — on the instance that accepted them, not be re-routed, dropped, or
// double-settled by the swap.
func TestSecretRotationSwapsInstancesWithoutLosingJobs(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "pass-v1"}); err != nil {
		t.Fatalf("Ensure(primary, pass-v1): %v", err)
	}
	old := h.writerFor("primary")
	oldInst := h.instances()[0]

	const jobs = 5
	for i := range jobs {
		enqueue(t, old, oldInst.fakeWriter, fmt.Sprintf("pre-%d", i))
	}

	// The rotation: same sink, new credential, hence a new fingerprint.
	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "pass-v2"}); err != nil {
		t.Fatalf("Ensure(primary, pass-v2): %v", err)
	}

	built := h.instances()
	if len(built) != 2 {
		t.Fatalf("factory built %d instances, want 2", len(built))
	}
	newInst := built[1]

	newWriter := h.writerFor("primary")
	if newWriter == old {
		t.Fatal("WriterFor still routes to the pre-rotation instance; the swap did not happen")
	}

	// Every pre-swap job settles on the old instance, exactly once, as part of its
	// drain — which the manager started right after the swap.
	waitFor(t, "the pre-rotation instance to drain and close", func() bool {
		closed, _, _ := oldInst.isClosed()
		return closed
	})
	total, trues, maxPerName := oldInst.counts()
	if total != jobs || trues != jobs || maxPerName != 1 {
		t.Errorf("old instance commits: total=%d trues=%d maxPerName=%d, want %d/%d/1",
			total, trues, maxPerName, jobs, jobs)
	}

	// Post-swap jobs go to the new instance and are still in flight there: the
	// rotation must not settle them early.
	for i := range jobs {
		enqueue(t, newWriter, newInst.fakeWriter, fmt.Sprintf("post-%d", i))
	}
	if total, _, _ := newInst.counts(); total != 0 {
		t.Errorf("new instance settled %d jobs before its own shutdown, want 0", total)
	}

	// Shutting the manager down drains the new instance in turn.
	h.stop()
	total, trues, maxPerName = newInst.counts()
	if total != jobs || trues != jobs || maxPerName != 1 {
		t.Errorf("new instance commits: total=%d trues=%d maxPerName=%d, want %d/%d/1",
			total, trues, maxPerName, jobs, jobs)
	}

	// No job crossed instances — the swap routed work, it did not migrate it.
	for i := range jobs {
		if n := newInst.committed(fmt.Sprintf("pre-%d", i)); n != 0 {
			t.Errorf("pre-swap job pre-%d settled %d times on the new instance, want 0", i, n)
		}
		if n := oldInst.committed(fmt.Sprintf("post-%d", i)); n != 0 {
			t.Errorf("post-swap job post-%d settled %d times on the old instance, want 0", i, n)
		}
	}

	// A recycle keeps the sink's pipeline state: it is the same sink with the same
	// durable history, so discarding its dedup baselines would re-emit every object
	// in every scope it serves.
	if removed, _ := h.pipe.removals(); len(removed) != 0 {
		t.Errorf("a credential rotation evicted pipeline state for %v, want no eviction", removed)
	}
	if parked, _ := h.parks.snapshot(); len(parked) != 0 {
		t.Errorf("a credential rotation parked rules for %v, want no parking", parked)
	}
}

// TestEnsureIsIdempotentForAnUnchangedConfig proves an unchanged fingerprint is a
// no-op — the property that makes Ensure safe to call from every reconcile pass,
// including controller-runtime's periodic resyncs.
func TestEnsureIsIdempotentForAnUnchangedConfig(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()

	for range 3 {
		if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "same"}); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}
	if n := len(h.instances()); n != 1 {
		t.Fatalf("factory built %d instances for three identical Ensure calls, want 1", n)
	}
	if closed, _, _ := h.instances()[0].isClosed(); closed {
		t.Error("the running instance was drained by an idempotent Ensure")
	}
}

// TestEnsureKeepsThePreviousInstanceWhenTheFactoryFails is Invariant 5 on the
// update path: a sink whose new configuration cannot be built must keep streaming
// through the instance that works, with the failure reported to the caller (which
// Task 1.7 turns into a condition) rather than swallowed.
func TestEnsureKeepsThePreviousInstanceWhenTheFactoryFails(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "good"}); err != nil {
		t.Fatalf("Ensure(good): %v", err)
	}
	live := h.writerFor("primary")

	h.mu.Lock()
	h.buildErr = errFactory
	h.mu.Unlock()

	err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "bad"})
	if !errors.Is(err, errFactory) {
		t.Fatalf("Ensure(bad) error = %v, want it to wrap errFactory", err)
	}
	if got := h.writerFor("primary"); got != live {
		t.Error("a failed rebuild replaced the working instance")
	}
	if closed, _, _ := h.instances()[0].isClosed(); closed {
		t.Error("a failed rebuild drained the working instance")
	}
}

// TestDeleteDrainsThenEvictsThenParks covers AC (b): deleting a sink settles its
// in-flight jobs, closes it, evicts its pipeline state, and parks the rules that
// streamed to it — strictly in that order.
//
// The order is not cosmetic. Commits settling during the drain still resolve
// against the sink's hashCache, so evicting it first would let a confirmed write
// revert into state that no longer exists (Invariant 3). Routing, by contrast, is
// withdrawn immediately, so no new job is handed to a sink on its way out.
func TestDeleteDrainsThenEvictsThenParks(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writer := h.writerFor("primary")
	inst := h.instances()[0]

	const jobs = 3
	for i := range jobs {
		enqueue(t, writer, inst.fakeWriter, fmt.Sprintf("job-%d", i))
	}

	h.mgr.Delete(testID("primary"))

	// Routing is gone at once, before any draining has finished.
	if _, ok := h.mgr.WriterFor(testID("primary")); ok {
		t.Error("WriterFor still routes to a deleted sink")
	}

	waitFor(t, "the deleted sink to be parked", func() bool {
		parked, _ := h.parks.snapshot()
		_, ok := parked[testID("primary")]
		return ok
	})

	total, trues, maxPerName := inst.counts()
	if total != jobs || trues != jobs || maxPerName != 1 {
		t.Errorf("commits: total=%d trues=%d maxPerName=%d, want %d/%d/1", total, trues, maxPerName, jobs, jobs)
	}

	closed, lastCommitAt, closedAt := inst.isClosed()
	if !closed {
		t.Fatal("the deleted sink's instance was never closed")
	}
	if lastCommitAt >= closedAt {
		t.Errorf("the instance closed at %d before its last commit at %d; the drain did not precede the close",
			closedAt, lastCommitAt)
	}

	removed, removeAt := h.pipe.removals()
	if !slices.Contains(removed, testID("primary")) {
		t.Fatalf("RemoveSink was not called for the deleted sink; removals=%v", removed)
	}
	if removeAt[testID("primary")] <= closedAt {
		t.Errorf("pipeline state was evicted at %d, before the instance closed at %d",
			removeAt[testID("primary")], closedAt)
	}

	parked, parkAt := h.parks.snapshot()
	if want := []string{"team-a/audit", "team-b/audit"}; !slices.Equal(parked[testID("primary")], want) {
		t.Errorf("parked rules = %v, want %v", parked[testID("primary")], want)
	}
	if parkAt[testID("primary")] <= removeAt[testID("primary")] {
		t.Errorf("rules were parked at %d, before the pipeline eviction at %d",
			parkAt[testID("primary")], removeAt[testID("primary")])
	}
}

// TestDeleteClearsTheWarmCoordinatorsBookkeeping covers the optional WarmHooks
// half of the teardown (Task 1.12). The coordinator's per-sink state — most of all
// its "already boot-reconciled" mark — has to go the moment the pipeline's caches
// do, or a sink re-created under the same name inherits a stale mark and its boot
// pass never runs again, leaving scopes orphaned during the absence open forever.
func TestDeleteClearsTheWarmCoordinatorsBookkeeping(t *testing.T) {
	var warm *fakeWarmHooks
	h := newManagerHarness(t, func(opts *ManagerOptions) {
		warm = newFakeWarmHooks(opts.Pipeline.(*fakePipeline).clock)
		opts.Warm = warm
	})
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	h.mgr.Delete(testID("primary"))

	waitFor(t, "the deleted sink to be forgotten by the warm coordinator", func() bool {
		forgotten, _ := warm.forgets()
		return slices.Contains(forgotten, testID("primary"))
	})

	// Ordering: the coordinator is cleared immediately after the pipeline
	// eviction, so no window exists in which one half of the teardown has happened
	// and the other has not been scheduled.
	_, removeAt := h.pipe.removals()
	_, forgetAt := warm.forgets()
	if forgetAt[testID("primary")] <= removeAt[testID("primary")] {
		t.Errorf("the warm coordinator was cleared at %d, before the pipeline eviction at %d",
			forgetAt[testID("primary")], removeAt[testID("primary")])
	}
}

// TestDeleteTearsDownOnlyTheIdentifiedKind is the teardown half of typed identity:
// two sinks may legitimately share a name across kinds (a ClickHouseSink and an
// S3Sink called "audit" are two unrelated backends, D6), so deleting one must evict
// exactly one sink's state and park exactly one sink's rules.
//
// The failure this closes is silent and expensive. Keyed on the name alone, deleting
// the archive would discard the ClickHouse sink's hashCache — and the next
// observation of every object in every scope it serves would be treated as new, so
// the "deletion" of one sink would re-emit the entire cluster into another.
func TestDeleteTearsDownOnlyTheIdentifiedKind(t *testing.T) {
	const shared = "audit"
	archive := ID{Kind: "S3Sink", Name: shared}
	timeline := ID{Kind: DefaultSinkKind, Name: shared}

	var warm *fakeWarmHooks
	h := newManagerHarness(t, func(opts *ManagerOptions) {
		warm = newFakeWarmHooks(opts.Pipeline.(*fakePipeline).clock)
		opts.Warm = warm
		opts.Dependents = fakeDependents{rules: map[ID][]string{
			archive:  {"streamrule/team-a/archive"},
			timeline: {"streamrule/team-a/timeline"},
		}}
	})
	h.start()

	for _, id := range []ID{timeline, archive} {
		if err := h.mgr.Ensure(id, fakeConfig{fingerprint: "v1"}); err != nil {
			t.Fatalf("Ensure(%s): %v", id, err)
		}
	}

	h.mgr.Delete(archive)

	waitFor(t, "the deleted archive sink to be parked", func() bool {
		parked, _ := h.parks.snapshot()
		_, ok := parked[archive]
		return ok
	})

	// Everything the teardown touches names the S3Sink identity and nothing else.
	removed, _ := h.pipe.removals()
	if !slices.Contains(removed, archive) {
		t.Errorf("RemoveSink was not called for %s; removals=%v", archive, removed)
	}
	if slices.Contains(removed, timeline) {
		t.Errorf("RemoveSink evicted %s, whose CR was never deleted", timeline)
	}
	forgotten, _ := warm.forgets()
	if !slices.Contains(forgotten, archive) {
		t.Errorf("ForgetSink was not called for %s; forgotten=%v", archive, forgotten)
	}
	if slices.Contains(forgotten, timeline) {
		t.Errorf("ForgetSink cleared %s, whose CR was never deleted", timeline)
	}
	parked, _ := h.parks.snapshot()
	if want := []string{"streamrule/team-a/archive"}; !slices.Equal(parked[archive], want) {
		t.Errorf("parked rules for %s = %v, want %v", archive, parked[archive], want)
	}
	if got, ok := parked[timeline]; ok {
		t.Errorf("the rules of %s were parked (%v) by another kind's deletion", timeline, got)
	}

	// And the sink that shares the name is still routed, so its writes never paused.
	if _, ok := h.mgr.WriterFor(timeline); !ok {
		t.Errorf("%s stopped being routed when %s was deleted", timeline, archive)
	}
}

// TestDeleteWithoutWarmHooksStillEvicts proves the hook is genuinely optional: a
// deployment (or a test) that runs no warm coordinator must not need the wiring,
// and must not panic for the want of it.
func TestDeleteWithoutWarmHooksStillEvicts(t *testing.T) {
	h := newManagerHarness(t, nil) // ManagerOptions.Warm left nil
	h.start()

	h.mgr.Delete(testID("primary"))

	waitFor(t, "the deleted sink to be evicted with no warm hooks wired", func() bool {
		removed, _ := h.pipe.removals()
		return slices.Contains(removed, testID("primary"))
	})
}

// TestDeleteOfAnUnknownSinkStillEvictsAndParks covers the rule that references a
// sink whose CR never existed (or was deleted before this process started): it
// needs the same SinkMissing parking as one whose instance was running, and the
// pipeline eviction is documented as safe for a name it never saw.
func TestDeleteOfAnUnknownSinkStillEvictsAndParks(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()

	h.mgr.Delete(testID("primary"))

	waitFor(t, "the unknown sink to be parked", func() bool {
		parked, _ := h.parks.snapshot()
		_, ok := parked[testID("primary")]
		return ok
	})
	removed, _ := h.pipe.removals()
	if !slices.Contains(removed, testID("primary")) {
		t.Errorf("RemoveSink was not called for an unknown sink; removals=%v", removed)
	}
	if n := len(h.instances()); n != 0 {
		t.Errorf("deleting an unknown sink built %d instances, want 0", n)
	}
}

// TestDeleteDoesNotEvictAStateRecreatedMidDrain guards the delete-then-recreate
// race: if a sink is re-applied while its predecessor is still draining, the
// delete's tail must not evict the *new* instance's freshly-warmed state, nor park
// rules that are working again.
func TestDeleteDoesNotEvictAStateRecreatedMidDrain(t *testing.T) {
	h := newManagerHarness(t, nil)
	// The first instance's drain blocks until the test releases it, which is the
	// window the recreate has to land in.
	hold := make(chan struct{})
	h.instance = func(label string) *fakeInstance {
		inst := newFakeInstance(label, h.clock)
		if label == "primary#v1" {
			inst.holdDrain = hold
		}
		return inst
	}
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure(v1): %v", err)
	}
	h.mgr.Delete(testID("primary"))

	// Recreated while the old instance is still inside its (blocked) drain.
	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v2"}); err != nil {
		t.Fatalf("Ensure(v2): %v", err)
	}
	close(hold)

	waitFor(t, "the held instance to finish draining", func() bool {
		closed, _, _ := h.instances()[0].isClosed()
		return closed
	})
	// Give the delete tail a chance to (wrongly) evict before asserting it did not.
	time.Sleep(50 * time.Millisecond)

	if removed, _ := h.pipe.removals(); len(removed) != 0 {
		t.Errorf("the delete tail evicted pipeline state for the recreated sink: %v", removed)
	}
	if parked, _ := h.parks.snapshot(); len(parked) != 0 {
		t.Errorf("the delete tail parked rules for a recreated sink: %v", parked)
	}
	if _, ok := h.mgr.WriterFor(testID("primary")); !ok {
		t.Error("the recreated sink is not routed")
	}
}

// TestEnsureBeforeStartIsAppliedWhenTheManagerRuns covers the ordering the
// controller-runtime manager does not guarantee: a SinkReconciler may reconcile a
// ClickHouseSink before this runnable starts, and that sink must still come up
// rather than be silently dropped.
func TestEnsureBeforeStartIsAppliedWhenTheManagerRuns(t *testing.T) {
	h := newManagerHarness(t, nil)

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure before Start: %v", err)
	}
	if _, ok := h.mgr.WriterFor(testID("primary")); ok {
		t.Error("a pending sink is routed before the manager started")
	}
	if n := len(h.instances()); n != 0 {
		t.Errorf("a pending sink built %d instances before Start, want 0", n)
	}

	h.start()
	waitFor(t, "the pending sink to be routed", func() bool {
		_, ok := h.mgr.WriterFor(testID("primary"))
		return ok
	})
	if n := len(h.instances()); n != 1 {
		t.Errorf("factory built %d instances, want 1", n)
	}
}

// TestDeleteBeforeStartCancelsThePendingSink proves the pending queue is not a
// one-way street: a sink created and deleted before the manager ran must never be
// started.
func TestDeleteBeforeStartCancelsThePendingSink(t *testing.T) {
	h := newManagerHarness(t, nil)

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure before Start: %v", err)
	}
	h.mgr.Delete(testID("primary"))
	h.start()

	waitFor(t, "the deleted-before-start sink to be parked", func() bool {
		parked, _ := h.parks.snapshot()
		_, ok := parked[testID("primary")]
		return ok
	})
	if n := len(h.instances()); n != 0 {
		t.Errorf("a sink deleted before Start still built %d instances, want 0", n)
	}
	if _, ok := h.mgr.WriterFor(testID("primary")); ok {
		t.Error("a sink deleted before Start is routed")
	}
}

// TestRoutingReportsTheOptionalHalvesHonestly proves the three routers agree about
// what a backend can do: a fully-featured instance answers all of them, a
// write-only backend answers only WriterFor, and an unknown name answers none.
//
// The write-only case is the one that matters for D6: a future backend that cannot
// read its own history back must still route writes, with warm-up and scope epochs
// disabled for it rather than the operator refusing to use it.
func TestRoutingReportsTheOptionalHalvesHonestly(t *testing.T) {
	h := newManagerHarness(t, func(opts *ManagerOptions) {
		clock := &atomic.Int64{}
		opts.Factory = func(id ID, _ InstanceConfig) (Writer, error) {
			if id.Name == "write-only" {
				return newFakeWriter(id.Name, clock), nil
			}
			return newFakeInstance(id.Name, clock), nil
		}
	})
	h.start()

	for _, name := range []string{"full", "write-only"} {
		if err := h.mgr.Ensure(testID(name), fakeConfig{fingerprint: "v1"}); err != nil {
			t.Fatalf("Ensure(%q): %v", name, err)
		}
		// Both are routed for writes, whatever else they can or cannot do.
		if w := h.writerFor(name); w == nil {
			t.Fatalf("WriterFor(%q) returned a nil writer", name)
		}
	}

	tests := []struct {
		sink        ID
		wantWriter  bool
		wantReader  bool
		wantEvents  bool
		description string
	}{
		{sink: testID("full"), wantWriter: true, wantReader: true, wantEvents: true,
			description: "a backend implementing every half answers every router"},
		{sink: testID("write-only"), wantWriter: true, wantReader: false, wantEvents: false,
			description: "a write-only backend routes writes and reports no reader"},
		{sink: testID("absent"), wantWriter: false, wantReader: false, wantEvents: false,
			description: "an unknown sink is transiently absent from every router"},
		// A live ClickHouseSink named "full" must not answer for an S3Sink of the
		// same name: the routers key on the whole identity, which is what stops one
		// backend's writer from serving another's records (Task 4.1).
		{sink: ID{Kind: "S3Sink", Name: "full"}, wantWriter: false, wantReader: false, wantEvents: false,
			description: "a different kind sharing a live sink's name is absent from every router"},
	}
	for _, tc := range tests {
		t.Run(tc.sink.String(), func(t *testing.T) {
			if _, ok := h.mgr.WriterFor(tc.sink); ok != tc.wantWriter {
				t.Errorf("WriterFor(%q) ok = %t, want %t (%s)", tc.sink, ok, tc.wantWriter, tc.description)
			}
			if _, ok := h.mgr.StateReaderFor(tc.sink); ok != tc.wantReader {
				t.Errorf("StateReaderFor(%q) ok = %t, want %t (%s)", tc.sink, ok, tc.wantReader, tc.description)
			}
			if _, ok := h.mgr.ScopeEventWriterFor(tc.sink); ok != tc.wantEvents {
				t.Errorf("ScopeEventWriterFor(%q) ok = %t, want %t (%s)", tc.sink, ok, tc.wantEvents, tc.description)
			}
		})
	}

	want := []ID{testID("full"), testID("write-only")}
	if got := h.mgr.SinkIDs(); !slices.Equal(got, want) {
		t.Errorf("SinkIDs() = %v, want %v", got, want)
	}
}

// TestProbeFailsWithBackoffThenRecovers is the probe-failure acceptance criterion:
// an unreachable backend produces repeated failure results, spaced by a growing
// backoff, and its recovery produces a success — all of it on the result channel,
// with no CR status touched anywhere in this package (that wiring is Task 1.7's).
func TestProbeFailsWithBackoffThenRecovers(t *testing.T) {
	const minBackoff = 20 * time.Millisecond
	const failures = 3

	dialErr := errors.New("dial tcp 10.0.0.1:9000: connect: connection refused")

	h := newManagerHarness(t, func(opts *ManagerOptions) {
		opts.ProbeInterval = time.Hour // one success is enough; don't re-probe
		opts.ProbeMinBackoff = minBackoff
		opts.ProbeMaxBackoff = 200 * time.Millisecond
	})
	h.instance = func(label string) *fakeInstance {
		inst := newFakeInstance(label, h.clock)
		inst.probe = func(attempt int) error {
			if attempt <= failures {
				return dialErr
			}
			return nil
		}
		return inst
	}
	h.start()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// The fake consumer: read results until the recovery lands.
	var results []ProbeResult
	for {
		select {
		case res := <-h.mgr.ProbeResults():
			results = append(results, res)
			if res.Err == nil {
				goto settled
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for a successful probe result; got %d results so far", len(results))
		}
	}

settled:
	if len(results) != failures+1 {
		t.Fatalf("got %d results, want %d failures followed by one success: %+v", len(results), failures+1, results)
	}
	for i, res := range results[:failures] {
		if res.Sink != testID("primary") {
			t.Errorf("result %d is for sink %q, want %q", i, res.Sink, testID("primary"))
		}
		if !errors.Is(res.Err, dialErr) {
			t.Errorf("result %d error = %v, want the dial error", i, res.Err)
		}
		if res.Reason != ProbeReasonUnreachable {
			t.Errorf("result %d reason = %q, want %q", i, res.Reason, ProbeReasonUnreachable)
		}
		if res.At.IsZero() {
			t.Errorf("result %d carries no timestamp", i)
		}
	}

	recovery := results[failures]
	if recovery.Err != nil || recovery.Reason != "" {
		t.Errorf("recovery result = %+v, want a nil error and an empty reason", recovery)
	}

	// Every retry waited: consecutive attempts are at least one minimum backoff
	// apart, so a failing sink is retried rather than hammered.
	attempts := h.instances()[0].probeTimes()
	if len(attempts) < failures+1 {
		t.Fatalf("probe ran %d times, want at least %d", len(attempts), failures+1)
	}
	for i := 1; i < len(attempts); i++ {
		if gap := attempts[i].Sub(attempts[i-1]); gap < minBackoff {
			t.Errorf("probe attempts %d and %d are %s apart, want at least %s", i-1, i, gap, minBackoff)
		}
	}
}

// TestProbeClassifiesItsFailures proves the two classifications the manager makes,
// and the default everything else falls into.
//
// Both classified cases are "this will not fix itself with time" verdicts that need
// a human, and they need *different* humans: a schema mismatch needs a migration,
// and a credential that could not be obtained needs whoever owns the workload's
// identity. Everything else is reachability, which the manager keeps retrying on
// its own. Collapsing any two of the three would leave the reconciler unable to
// write a condition that says what to do next.
func TestProbeClassifiesItsFailures(t *testing.T) {
	tests := []struct {
		name       string
		probeErr   error
		wantReason string
	}{
		{
			name:       "a wrapped schema mismatch is permanent",
			probeErr:   fmt.Errorf("%w: table %q is missing", ErrSchemaInvalid, "resource_states"),
			wantReason: ProbeReasonSchemaInvalid,
		},
		{
			name: "a wrapped credential failure is about identity, not reachability",
			probeErr: fmt.Errorf("write probe object to bucket %q: %w: no EC2 IMDS role found",
				"audit", ErrCredentialsUnavailable),
			wantReason: ProbeReasonCredentialsInvalid,
		},
		{
			name:       "any other failure is unreachable",
			probeErr:   errors.New("i/o timeout"),
			wantReason: ProbeReasonUnreachable,
		},
		{
			name:       "a successful probe carries no reason",
			probeErr:   nil,
			wantReason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newManagerHarness(t, nil)
			h.instance = func(label string) *fakeInstance {
				inst := newFakeInstance(label, h.clock)
				inst.probe = func(int) error { return tc.probeErr }
				return inst
			}
			h.start()

			if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); err != nil {
				t.Fatalf("Ensure: %v", err)
			}

			select {
			case res := <-h.mgr.ProbeResults():
				if res.Reason != tc.wantReason {
					t.Errorf("reason = %q, want %q", res.Reason, tc.wantReason)
				}
				if (res.Err == nil) != (tc.probeErr == nil) {
					t.Errorf("result error = %v, want nil == %t", res.Err, tc.probeErr == nil)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for a probe result")
			}
		})
	}
}

// TestNextProbeDelaySchedule pins the schedule the probe loop follows, which the
// timing test can only bound from below: failures step up geometrically to the
// cap, and a success both returns to the steady interval and clears the
// accumulated backoff so a recovered sink does not inherit its outage's penalty.
func TestNextProbeDelaySchedule(t *testing.T) {
	m := &SinkManager{
		probeInterval:   time.Minute,
		probeMinBackoff: 20 * time.Millisecond,
		probeMaxBackoff: 45 * time.Millisecond,
	}
	newSchedule := func() *backoff.ExponentialBackOff {
		eb := backoff.NewExponentialBackOff()
		eb.InitialInterval = m.probeMinBackoff
		eb.MaxInterval = m.probeMaxBackoff
		eb.RandomizationFactor = 0
		eb.MaxElapsedTime = 0
		eb.Reset()
		return eb
	}

	tests := []struct {
		name      string
		outcomes  []bool // true = the probe failed
		wantDelay []time.Duration
	}{
		{
			name:     "consecutive failures step up to the cap",
			outcomes: []bool{true, true, true, true},
			// 20ms, ×1.5 → 30ms, ×1.5 → 45ms, capped at MaxInterval.
			wantDelay: []time.Duration{20 * time.Millisecond, 30 * time.Millisecond,
				45 * time.Millisecond, 45 * time.Millisecond},
		},
		{
			name:      "a success returns to the steady interval",
			outcomes:  []bool{true, false},
			wantDelay: []time.Duration{20 * time.Millisecond, time.Minute},
		},
		{
			name:     "a recovery clears the accumulated backoff",
			outcomes: []bool{true, true, false, true},
			wantDelay: []time.Duration{20 * time.Millisecond, 30 * time.Millisecond,
				time.Minute, 20 * time.Millisecond},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eb := newSchedule()
			for i, failed := range tc.outcomes {
				if got := m.nextProbeDelay(eb, failed); got != tc.wantDelay[i] {
					t.Errorf("delay after outcome %d (failed=%t) = %s, want %s", i, failed, got, tc.wantDelay[i])
				}
			}
		})
	}
}

// TestNoProbeLoopWithoutAProber proves a backend that cannot be probed is simply
// not probed: no results, and above all no fabricated success that would let a CR
// claim Ready on the strength of a check that never ran.
func TestNoProbeLoopWithoutAProber(t *testing.T) {
	h := newManagerHarness(t, func(opts *ManagerOptions) {
		clock := &atomic.Int64{}
		opts.ProbeInterval = time.Millisecond
		opts.Factory = func(id ID, _ InstanceConfig) (Writer, error) {
			return newFakeWriter(id.Name, clock), nil
		}
	})
	h.start()

	if err := h.mgr.Ensure(testID("write-only"), fakeConfig{fingerprint: "v1"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	waitFor(t, "the write-only sink to be routed", func() bool {
		_, ok := h.mgr.WriterFor(testID("write-only"))
		return ok
	})

	select {
	case res := <-h.mgr.ProbeResults():
		t.Fatalf("a backend without a Prober posted a probe result: %+v", res)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestEnsureAfterShutdownIsRefused proves the manager stops accepting work once it
// is shutting down, rather than starting an instance nothing will ever drain.
func TestEnsureAfterShutdownIsRefused(t *testing.T) {
	h := newManagerHarness(t, nil)
	h.start()
	h.stop()

	if err := h.mgr.Ensure(testID("primary"), fakeConfig{fingerprint: "v1"}); !errors.Is(err, errManagerStopped) {
		t.Errorf("Ensure after shutdown error = %v, want errManagerStopped", err)
	}
	if n := len(h.instances()); n != 0 {
		t.Errorf("Ensure after shutdown built %d instances, want 0", n)
	}
}

// TestManagerShutdownDrainsEveryInstance is the goleak shutdown guard: once Start
// returns, every instance has been drained and closed and no manager goroutine —
// writer, probe loop, or delete tail — is left behind.
//
// Shutdown deliberately does *not* evict pipeline state or park rules: a process
// exiting is not a sink going away, and doing either would write a Degraded
// condition describing an operator that no longer exists.
func TestManagerShutdownDrainsEveryInstance(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	h := newManagerHarness(t, func(opts *ManagerOptions) {
		opts.ProbeInterval = 5 * time.Millisecond
		opts.ProbeMinBackoff = 5 * time.Millisecond
	})
	h.start()

	for _, name := range []string{"primary", "audit"} {
		if err := h.mgr.Ensure(testID(name), fakeConfig{fingerprint: "v1"}); err != nil {
			t.Fatalf("Ensure(%q): %v", name, err)
		}
	}
	waitFor(t, "both sinks to be routed", func() bool { return len(h.mgr.SinkIDs()) == 2 })

	// Drain the probe results so no probe goroutine is parked on a full channel
	// when shutdown begins; a consumer is what production has (Task 1.7).
	consumerDone := make(chan struct{})
	consumerStop := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-h.mgr.ProbeResults():
			case <-consumerStop:
				return
			}
		}
	}()

	h.stop()
	close(consumerStop)
	<-consumerDone

	for _, inst := range h.instances() {
		if closed, _, _ := inst.isClosed(); !closed {
			t.Errorf("instance %s was not closed by shutdown", inst.label)
		}
	}
	if ids := h.mgr.SinkIDs(); len(ids) != 0 {
		t.Errorf("SinkIDs() after shutdown = %v, want empty", ids)
	}
	if removed, _ := h.pipe.removals(); len(removed) != 0 {
		t.Errorf("shutdown evicted pipeline state for %v, want no eviction", removed)
	}

	goleak.VerifyNone(t, leaked)
}

// TestNewSinkManagerValidatesItsDependencies proves the two mandatory
// dependencies are rejected eagerly: either of them missing would otherwise
// surface as a nil-pointer panic on a lifecycle goroutine, in the middle of a
// drain whose whole job is to not lose writes.
func TestNewSinkManagerValidatesItsDependencies(t *testing.T) {
	factory := func(ID, InstanceConfig) (Writer, error) { return nil, nil }

	tests := []struct {
		name string
		opts ManagerOptions
	}{
		{name: "no factory", opts: ManagerOptions{Pipeline: newFakePipeline(&atomic.Int64{})}},
		{name: "no pipeline", opts: ManagerOptions{Factory: factory}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSinkManager(tc.opts); err == nil {
				t.Error("NewSinkManager accepted an incomplete configuration")
			}
		})
	}

	m, err := NewSinkManager(ManagerOptions{Factory: factory, Pipeline: newFakePipeline(&atomic.Int64{})})
	if err != nil {
		t.Fatalf("NewSinkManager with the mandatory dependencies: %v", err)
	}
	if !m.NeedLeaderElection() {
		t.Error("NeedLeaderElection() = false; two replicas holding sink connections would double every row")
	}
	// An incomplete ID is refused rather than completed. Defaulting the kind here
	// is the silent collision typed identity exists to prevent, so a caller that
	// cannot name the kind gets an error instead of another backend's routing slot.
	incomplete := []ID{{}, {Name: "primary"}, {Kind: DefaultSinkKind}}
	for _, id := range incomplete {
		if err := m.Ensure(id, fakeConfig{fingerprint: "v1"}); err == nil {
			t.Errorf("Ensure(%+v) accepted an incomplete sink ID", id)
		}
	}
	if err := m.Ensure(testID("primary"), nil); err == nil {
		t.Error("Ensure accepted a nil configuration")
	}
}
