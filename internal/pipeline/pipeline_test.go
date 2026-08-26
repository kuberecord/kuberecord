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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/goleak"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// warm marks a key's scope as warmed, which is what most specs want: without it
// every cache-miss is tagged Snapshot rather than Added (see the dedicated
// Snapshot spec below for that behaviour).
func (h *testHarness) warm(key Key) {
	h.pipeline.MarkScopeWarm(key.Sink, key.Scope())
}

func TestNewRequiresListerAndRouter(t *testing.T) {
	if _, err := New(Options{Router: newFakeRouter()}); err == nil {
		t.Fatal("New must reject a nil Lister rather than panicking inside a worker later")
	}
	if _, err := New(Options{Lister: newFakeLister()}); err == nil {
		t.Fatal("New must reject a nil Router rather than panicking inside a worker later")
	}

	p, err := New(Options{Lister: newFakeLister(), Router: newFakeRouter()})
	if err != nil {
		t.Fatalf("New with both dependencies: %v", err)
	}
	t.Cleanup(p.queue.ShutDown)
	if p.workers != DefaultWorkers {
		t.Errorf("worker count defaulted to %d, want DefaultWorkers (%d)", p.workers, DefaultWorkers)
	}
}

// TestPipelineSerializationPerKey is the Invariant 2 proof, and the single most
// important spec in this package: the entire hashCache version-gating design
// documents itself as depending on "no two workers ever process the same key
// concurrently". The workqueue provides that contractually; this test refuses to
// take it on faith.
//
// It is the direct port of the old controller-runtime-era serialization guard
// (which asserted the property indirectly, via emitted diff shapes, because
// controller-runtime offered no way to observe concurrency). Now that the
// pipeline owns the guarantee, it is asserted head-on with per-key in-flight
// counters — and the complementary half is asserted too: *distinct* keys must
// genuinely run in parallel, or the guarantee would be trivially satisfied by a
// pipeline that is simply single-threaded.
func TestPipelineSerializationPerKey(t *testing.T) {
	t.Run("one key never runs on two workers at once", func(t *testing.T) {
		h := newHarness(t)

		var mu sync.Mutex
		inFlight := make(map[Key]int)
		maxInFlight := make(map[Key]int)
		var calls atomic.Int64

		// An artificially slow Process widens the window in which a second
		// worker could pick up the same key, so rapid re-adds actually have a
		// chance to break the contract if it were not upheld.
		h.pipeline.processFn = func(_ context.Context, key Key) error {
			mu.Lock()
			inFlight[key]++
			if inFlight[key] > maxInFlight[key] {
				maxInFlight[key] = inFlight[key]
			}
			mu.Unlock()

			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			inFlight[key]--
			if inFlight[key] == 0 {
				delete(inFlight, key)
			}
			mu.Unlock()
			calls.Add(1)
			return nil
		}

		stop := h.run(t)

		keyA, keyB := podKey("a"), podKey("b")
		var adders sync.WaitGroup
		for range 8 {
			adders.Go(func() {
				for range 200 {
					h.pipeline.Add(keyA)
					h.pipeline.Add(keyB)
				}
			})
		}
		adders.Wait()

		// Let the queue drain, then stop the workers so no call is in flight
		// while the counters are read.
		deadline := time.Now().Add(5 * time.Second)
		for h.pipeline.queue.Len() > 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		stop()

		mu.Lock()
		defer mu.Unlock()
		for _, key := range []Key{keyA, keyB} {
			if got := maxInFlight[key]; got != 1 {
				t.Errorf("key %s reached %d concurrent Process calls, want exactly 1 (Invariant 2 violated)", key, got)
			}
		}
		if calls.Load() == 0 {
			t.Fatal("no work was processed; the test proved nothing")
		}
		// Coalescing is the flip side of the contract: 3200 adds must not mean
		// 3200 Process calls, or every update storm would cost a write per event.
		if calls.Load() >= 3200 {
			t.Errorf("expected the queue to coalesce re-adds of a pending key, got %d calls for 3200 adds", calls.Load())
		}
	})

	t.Run("distinct keys do run concurrently", func(t *testing.T) {
		h := newHarness(t)

		entered := make(chan Key, 2)
		release := make(chan struct{})
		h.pipeline.processFn = func(_ context.Context, key Key) error {
			entered <- key
			<-release
			return nil
		}

		stop := h.run(t)
		defer func() { close(release); stop() }()

		h.pipeline.Add(podKey("a"))
		h.pipeline.Add(podKey("b"))

		// Both keys must be *inside* Process at the same moment: neither is
		// released until both have been observed.
		seen := make(map[Key]struct{}, 2)
		for range 2 {
			select {
			case key := <-entered:
				seen[key] = struct{}{}
			case <-time.After(5 * time.Second):
				t.Fatalf("only %d of 2 distinct keys entered Process concurrently", len(seen))
			}
		}
		if len(seen) != 2 {
			t.Fatalf("expected two distinct keys in flight, saw %v", seen)
		}
	})
}

// TestPipelineOrderingThroughRealQueue drives Add → Update → Delete for one key
// through the actual workqueue and worker pool (no direct Process calls), and
// asserts the sink saw exactly the three events, in order.
func TestPipelineOrderingThroughRealQueue(t *testing.T) {
	h := newHarness(t)
	key := podKey("ordering")
	h.warm(key)
	stop := h.run(t)
	defer stop()

	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
	h.pipeline.Add(key)
	h.writer.awaitRecords(t, 1)

	h.lister.set(key, newPod(key.Name, "uid-1", "2", "busybox:2"))
	h.pipeline.Add(key)
	h.writer.awaitRecords(t, 2)

	h.lister.remove(key)
	h.pipeline.Add(key)
	records := h.writer.awaitRecords(t, 3)

	if got, want := h.writer.eventTypes(), []string{"Added", "Modified", "Deleted"}; !slices.Equal(got, want) {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}

	// The Modified row must carry a diff (not the full state), and the Deleted
	// row must carry the schema-v1 empty payload with the identity intact.
	if records[1].Diff == "" || records[1].Data != "" {
		t.Errorf("Modified row should carry a diff and no full state, got diff=%q data=%q", records[1].Diff, records[1].Data)
	}
	deleted := records[2]
	if deleted.Data != "" || deleted.Diff != "" || deleted.SHA256 != "" || len(deleted.Actors) != 0 {
		t.Errorf("Deleted row must carry empty data/diff/sha256/actors, got %+v", deleted)
	}
	if deleted.UID != "uid-1" || deleted.Name != key.Name || deleted.Kind != "Pod" {
		t.Errorf("Deleted row lost the object's identity: %+v", deleted)
	}
	// api_version is provenance-only and the queue key is version-agnostic, so
	// the delete path has to recover it from the last observed live state.
	if deleted.APIVersion != "v1" {
		t.Errorf("Deleted row api_version = %q, want v1 (carried forward from the cached entry)", deleted.APIVersion)
	}
}

// TestPipelineSnapshotTaggingUntilScopeWarm covers the ambiguity guard: before a
// scope has been warmed from the sink's own history, a cache-miss cannot be
// trusted to mean "genuinely new", so it is recorded as Snapshot. Once warm, the
// same situation is a real Added.
func TestPipelineSnapshotTaggingUntilScopeWarm(t *testing.T) {
	h := newHarness(t)

	cold := podKey("cold")
	h.lister.set(cold, newPod(cold.Name, "uid-cold", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, cold); err != nil {
		t.Fatalf("Process(cold): %v", err)
	}

	h.warm(cold)

	warmKey := podKey("warm")
	h.lister.set(warmKey, newPod(warmKey.Name, "uid-warm", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, warmKey); err != nil {
		t.Fatalf("Process(warm): %v", err)
	}

	if got, want := h.writer.eventTypes(), []string{"Snapshot", "Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v (cache-miss before warm must be Snapshot, after warm Added)", got, want)
	}

	// The gauge must be observable in both states, or "is this scope still
	// warming?" is unanswerable from metrics alone.
	gauge := h.pipeline.metrics.safeMode.WithLabelValues(testSink.String(), "", "Pod", "default")
	if got := testutil.ToFloat64(gauge); got != 0 {
		t.Errorf("safe_mode = %v after warming, want 0", got)
	}

	cold2 := podKey("cold-again")
	h.pipeline.EvictScope(testSink, cold2.Scope())
	h.lister.set(cold2, newPod(cold2.Name, "uid-cold-2", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, cold2); err != nil {
		t.Fatalf("Process(cold again): %v", err)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.safeMode.WithLabelValues(
		testSink.String(), "", "Pod", "default")); got != 1 {
		t.Errorf("safe_mode = %v while the scope is un-warmed, want 1", got)
	}
}

// TestPipelineWriteFailureRevertsCacheAndRequeues is the Invariant 3 guard: a
// write that is abandoned after retries must never be mistaken for a persisted
// one. The proof is behavioural rather than an internal cache peek — the key is
// re-processed and the *same* object content produces a second record. Had the
// optimistic cache entry survived the failure, the retry would have matched on
// hash and been deduplicated away, silently losing the row forever.
func TestPipelineWriteFailureRevertsCacheAndRequeues(t *testing.T) {
	h := newHarness(t)
	key := podKey("write-failure")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	// The first accepted job settles as a failed write; the retry succeeds.
	h.writer.failNextCommit()

	stop := h.run(t)
	defer stop()

	h.pipeline.Add(key)
	h.writer.awaitRecords(t, 2)

	if got, want := h.writer.eventTypes(), []string{"Added", "Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v — a reverted cache must re-emit the full Added, not a diff or a dedup skip", got, want)
	}
	if n := h.logs.countOf(errAsyncWriteFailed); n != 1 {
		t.Errorf("abandoned write logged %d times at Error level, want exactly 1 (Invariant 4)", n)
	}
}

// TestPipelineUnavailableSinkRetriesWithoutDropping covers the SinkRouter miss:
// a sink that is deleted or mid-recycle must cost the key nothing. It is
// re-queued through the rate limiter, logged once (not once per attempt), never
// dropped, and must never panic.
func TestPipelineUnavailableSinkRetriesWithoutDropping(t *testing.T) {
	h := newHarness(t)
	key := podKey("no-sink")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	// The sink vanishes before the item is ever processed.
	h.router.remove(testSink)

	stop := h.run(t)
	defer stop()

	h.pipeline.Add(key)

	// Every attempt reaches the lister before the router, so the lister's call
	// count is a faithful attempt counter: several attempts prove the key really
	// is being re-added rather than dropped or stuck.
	deadline := time.Now().Add(5 * time.Second)
	for h.lister.getCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := h.lister.getCount(); got < 3 {
		t.Fatalf("key was attempted %d times, want ≥3 rate-limited retries", got)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Fatalf("nothing may be written while the sink is unavailable, got %d records", len(got))
	}
	if n := h.logs.countOf(errSinkUnavailable); n != 1 {
		t.Errorf("unavailable sink logged %d times, want exactly 1 (rate-limited)", n)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.dropped.WithLabelValues(DropReasonScopeStopped)); got != 0 {
		t.Errorf("dropped_total{scope_stopped} = %v, want 0 — an unavailable sink is a retry, never a drop", got)
	}

	// Recovery: the sink comes back and the pending key settles on its own, with
	// no external nudge.
	h.router.set(testSink, h.writer)
	records := h.writer.awaitRecords(t, 1)
	if records[0].EventType != "Added" {
		t.Errorf("record after sink recovery = %q, want Added", records[0].EventType)
	}
}

// TestPipelineDropsItemWhenScopeStopped covers the other half of the scope
// contract: a queued item whose target was deactivated is dropped (counted, and
// logged at debug level), and must emit no record at all — least of all a
// Deleted row, which would turn "we stopped watching" into "it was deleted".
func TestPipelineDropsItemWhenScopeStopped(t *testing.T) {
	h := newHarness(t)
	key := podKey("stopped-scope")
	h.warm(key)
	// The object is still in the cache — only the scope stopped. Even so, and
	// even though a missing object would normally mean "deleted", nothing may be
	// written.
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
	h.lister.stopScope(testSink, key.Scope())

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("a stopped scope is not an error, got %v", err)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Fatalf("expected no records for a stopped scope, got %+v", got)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.dropped.WithLabelValues(DropReasonScopeStopped)); got != 1 {
		t.Errorf("dropped_total{reason=scope_stopped} = %v, want 1", got)
	}

	// The same is true when the object is gone as well: a stopped scope short-
	// circuits before the delete path is ever considered.
	h.lister.remove(key)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Fatalf("a stopped scope must never produce a Deleted row, got %+v", got)
	}
}

// TestPipelineDedupSkipsUnchangedObject asserts the hot-path short-circuit still
// works after the port: re-processing an unchanged object writes nothing and is
// counted as a dedup skip.
func TestPipelineDedupSkipsUnchangedObject(t *testing.T) {
	h := newHarness(t)
	key := podKey("dedup")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	for range 3 {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process: %v", err)
		}
	}

	if got := h.writer.eventTypes(); !slices.Equal(got, []string{"Added"}) {
		t.Fatalf("event types = %v, want a single Added", got)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.dedupSkips); got != 2 {
		t.Errorf("dedup_skips_total = %v, want 2", got)
	}
}

// TestPipelineListerErrorIsRetried asserts a genuine lookup failure is neither
// swallowed nor mistaken for "the object is gone" — the latter would emit a
// bogus Deleted row.
func TestPipelineListerErrorIsRetried(t *testing.T) {
	h := newHarness(t)
	key := podKey("lister-error")
	h.warm(key)
	boom := errors.New("indexer exploded")
	h.lister.setErr(boom)

	err := h.pipeline.Process(h.ctx, key)
	if !errors.Is(err, boom) {
		t.Fatalf("Process error = %v, want the lister's error so the item is retried", err)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Fatalf("a lister failure must not be recorded as a deletion, got %+v", got)
	}
	if n := h.logs.countOf(boom); n != 1 {
		t.Errorf("lister error logged %d times, want 1 (Invariant 4)", n)
	}
}

// TestPipelinePerSinkStateIsIndependent covers the reason hashCache became
// per-sink: the same object streaming to two sinks must produce a record on
// each, because a write confirmed on one says nothing about the other.
func TestPipelinePerSinkStateIsIndependent(t *testing.T) {
	other := clickHouseSink("audit")
	h := newHarness(t, testSink, other)

	keyA := podKey("shared")
	keyB := keyA
	keyB.Sink = other
	h.warm(keyA)
	h.warm(keyB)

	pod := newPod(keyA.Name, "uid-1", "1", "busybox:1")
	h.lister.set(keyA, pod)

	for _, key := range []Key{keyA, keyB} {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(%s): %v", key, err)
		}
	}

	if got := h.writer.eventTypes(); !slices.Equal(got, []string{"Added", "Added"}) {
		t.Fatalf("event types = %v, want an Added per sink (independent dedup state)", got)
	}

	// And the two caches really are separate objects, not one shared map.
	stA, _ := h.pipeline.sinks.lookup(testSink)
	stB, _ := h.pipeline.sinks.lookup(other)
	if stA == stB {
		t.Fatal("both sinks share one sinkState; dedup/version state must be per-sink")
	}
}

// TestPipelineSameNameDifferentKindsAreSeparateSinks is the regression guard for
// the precise corruption typed sink identity exists to prevent.
//
// A ClickHouseSink named "default" and an S3Sink named "default" are both legal at
// once in etcd — sink CRs are cluster-scoped and uniqueness is per *kind*. While
// the pipeline keyed its per-sink state by the bare name, the two shared one
// sinkState, so the *first* sink's confirmed write became the second's dedup
// baseline: the S3Sink would never receive an object the ClickHouseSink had
// already recorded, and there would be nothing in the logs to say so.
//
// The subject is therefore the hashCache entries themselves, not just the emitted
// rows: each sink must hold its own entry for the same object identity, and
// tearing one sink down must leave the other's baselines intact.
func TestPipelineSameNameDifferentKindsAreSeparateSinks(t *testing.T) {
	clickhouse := clickHouseSink("default")
	s3 := sink.ID{Kind: "S3Sink", Name: "default"}
	h := newHarness(t, clickhouse, s3)

	chKey := podKey("shared")
	chKey.Sink = clickhouse
	s3Key := chKey
	s3Key.Sink = s3
	h.warm(chKey)
	h.warm(s3Key)

	// One object identity, and only one — the cache key is deliberately
	// sink-agnostic (see TestCacheKeyIgnoresSink), so nothing but the registry's
	// keying keeps these two apart.
	if chKey.cacheKey() != s3Key.cacheKey() {
		t.Fatalf("fixture is wrong: %q and %q are not the same object identity",
			chKey.cacheKey(), s3Key.cacheKey())
	}
	h.lister.set(chKey, newPod(chKey.Name, "uid-1", "1", "busybox:1"))

	for _, key := range []Key{chKey, s3Key} {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(%s): %v", key, err)
		}
	}

	// Two rows, not one: the S3Sink's write was not suppressed by the
	// ClickHouseSink's.
	if got := h.writer.eventTypes(); !slices.Equal(got, []string{"Added", "Added"}) {
		t.Fatalf("event types = %v, want an Added per sink; a same-named sink of "+
			"another kind was deduplicated against the first one's history", got)
	}

	chState, ok := h.pipeline.sinks.lookup(clickhouse)
	if !ok {
		t.Fatalf("no state for %s", clickhouse)
	}
	s3State, ok := h.pipeline.sinks.lookup(s3)
	if !ok {
		t.Fatalf("no state for %s; it collided with %s", s3, clickhouse)
	}
	if chState == s3State {
		t.Fatalf("%s and %s share one sinkState; per-sink dedup state is keyed by name only",
			clickhouse, s3)
	}
	for _, tc := range []struct {
		id sink.ID
		st *sinkState
	}{{clickhouse, chState}, {s3, s3State}} {
		if _, exists := tc.st.cache.Load(chKey.cacheKey()); !exists {
			t.Errorf("%s holds no baseline for %s", tc.id, chKey.cacheKey())
		}
	}

	// The teardown half: removing one sink must not take the other's state with
	// it, which is the same collision seen from the other end.
	h.pipeline.RemoveSink(clickhouse)
	if _, ok := h.pipeline.sinks.lookup(clickhouse); ok {
		t.Error("RemoveSink left the ClickHouseSink's state behind")
	}
	survivor, ok := h.pipeline.sinks.lookup(s3)
	if !ok {
		t.Fatalf("removing %s also removed %s", clickhouse, s3)
	}
	if _, exists := survivor.cache.Load(s3Key.cacheKey()); !exists {
		t.Errorf("removing %s dropped %s's baseline for %s", clickhouse, s3, s3Key.cacheKey())
	}
	if !survivor.scopeWarm(s3Key) {
		t.Errorf("removing %s cleared %s's warm marker", clickhouse, s3)
	}
}

// TestPipelineEvictScope asserts the WatchManager's stop hook: the stopped
// scope's baselines and warm marker are dropped, other scopes are untouched, and
// no record of any kind is emitted for the eviction.
func TestPipelineEvictScope(t *testing.T) {
	h := newHarness(t)

	inDefault := podKey("evicted")
	inOther := Key{Sink: testSink, Group: "", Kind: "Pod", Namespace: "other", Name: "survivor"}
	h.warm(inDefault)
	h.warm(inOther)
	h.lister.set(inDefault, newPod(inDefault.Name, "uid-1", "1", "busybox:1"))
	h.lister.set(inOther, newPodIn(inOther.Namespace, inOther.Name, "uid-2", "1", "busybox:1"))

	for _, key := range []Key{inDefault, inOther} {
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(%s): %v", key, err)
		}
	}
	st, ok := h.pipeline.sinks.lookup(testSink)
	if !ok {
		t.Fatal("sink state was never created")
	}
	if st.cache.Len() != 2 {
		t.Fatalf("expected 2 cached baselines before eviction, got %d", st.cache.Len())
	}

	h.pipeline.EvictScope(testSink, inDefault.Scope())

	if _, exists := st.cache.Load(inDefault.cacheKey()); exists {
		t.Error("evicted scope's cache entry survived")
	}
	if _, exists := st.cache.Load(inOther.cacheKey()); !exists {
		t.Error("eviction removed an entry from a different namespace's scope")
	}
	if st.scopeWarm(inDefault) {
		t.Error("evicted scope is still marked warm; a re-activated scope must warm again before trusting cache-misses")
	}
	if !st.scopeWarm(inOther) {
		t.Error("eviction cleared an unrelated scope's warm marker")
	}
	if got := h.writer.eventTypes(); !slices.Equal(got, []string{"Added", "Added"}) {
		t.Fatalf("eviction must emit no records of its own, got %v", got)
	}
}

// TestPipelineRemoveSink asserts Task 1.8's teardown hook drops the whole sink's
// state: the next work item for a previously-known object is a genuine cache
// miss again rather than being deduplicated against a dead sink's history.
func TestPipelineRemoveSink(t *testing.T) {
	h := newHarness(t)
	key := podKey("removed-sink")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}

	h.pipeline.RemoveSink(testSink)
	if _, ok := h.pipeline.sinks.lookup(testSink); ok {
		t.Fatal("RemoveSink left the sink's state behind")
	}

	// Warm state went with it, so the rebuilt cache is untrusted again — exactly
	// what a freshly-created sink of the same name should see.
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process after RemoveSink: %v", err)
	}
	if got, want := h.writer.eventTypes(), []string{"Added", "Snapshot"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

// TestPipelineWorkersExitCleanlyOnCancel is the goleak shutdown guard: with the
// queue drained, cancelling the context must stop every worker and leave no
// goroutine behind.
func TestPipelineWorkersExitCleanlyOnCancel(t *testing.T) {
	// The shared zstd codec keeps a process-wide, lazily-created goroutine pool.
	// Warming it before the snapshot keeps a first-use inside the window from
	// looking like a leak.
	compressBaseline([]byte(`{"warm":true}`))
	snapshot := goleak.IgnoreCurrent()

	h := newHarness(t)
	key := podKey("shutdown")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	stop := h.run(t)
	h.pipeline.Add(key)
	h.writer.awaitRecords(t, 1)

	// Drain, then cancel: the AC's "clean exit with a drained queue".
	deadline := time.Now().Add(5 * time.Second)
	for h.pipeline.queue.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()
	h.pipeline.queue.ShutDown()

	goleak.VerifyNone(t, snapshot)
}
