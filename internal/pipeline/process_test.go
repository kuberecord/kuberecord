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
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The specs in this file are the re-hosted versions of the controller-era
// Reconcile tests: same scenarios, same guarantees, driven through Process
// instead. They call Process directly (rather than via the queue) wherever the
// property under test is about the decision logic, so each assertion observes
// exactly one settled work item.

// TestProcessReincarnationClosesOutOldHistory covers the anti-zombie path: an
// object that died and came back under a new UID while unobserved must produce a
// Deleted row for the old incarnation *and* an Added for the new one — never a
// Modified diffing one object's state against a different object's.
func TestProcessReincarnationClosesOutOldHistory(t *testing.T) {
	h := newHarness(t)
	key := podKey("reincarnated")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, "uid-old", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first incarnation): %v", err)
	}

	// Same name, different UID: the object was deleted and recreated while the
	// pipeline wasn't looking.
	h.lister.set(key, newPod(key.Name, "uid-new", "9", "busybox:2"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(reincarnation): %v", err)
	}

	records := h.writer.recorded()
	if got, want := h.writer.eventTypes(), []string{"Added", "Deleted", "Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
	if records[1].UID != "uid-old" {
		t.Errorf("close-out row must carry the OLD uid, got %q", records[1].UID)
	}
	if records[2].UID != "uid-new" {
		t.Errorf("post-reincarnation row must carry the NEW uid, got %q", records[2].UID)
	}
	// The new incarnation is a real, proven state transition — never downgraded
	// to Snapshot and never a diff against the dead object's baseline.
	if records[2].Diff != "" || records[2].Data == "" {
		t.Errorf("reincarnation must be recorded as full state, got diff=%q data-empty=%v", records[2].Diff, records[2].Data == "")
	}
}

// TestProcessReincarnationIsAddedEvenWhenScopeIsCold asserts the deliberate
// exception to Snapshot-tagging: a reincarnation is unambiguous (we hold the old
// UID and can see the new one), so it is recorded as Added even in a scope whose
// history has never been warmed.
func TestProcessReincarnationIsAddedEvenWhenScopeIsCold(t *testing.T) {
	h := newHarness(t)
	key := podKey("cold-reincarnation")
	// Deliberately NOT warmed.

	h.lister.set(key, newPod(key.Name, "uid-old", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first): %v", err)
	}
	h.lister.set(key, newPod(key.Name, "uid-new", "2", "busybox:2"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(reincarnation): %v", err)
	}

	if got, want := h.writer.eventTypes(), []string{"Snapshot", "Deleted", "Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v (the first miss is ambiguous, the reincarnation is not)", got, want)
	}
}

// TestProcessFailedCloseOutIsRetriedOnNextEvent asserts the close-out retry
// queue: unlike an ordinary write, a close-out has no cache entry left to gate
// its retry (Reserve has already overwritten the key with the new incarnation),
// so a failure must be remembered explicitly and re-attempted on the next event
// for that name — otherwise the old incarnation's history vanishes silently.
func TestProcessFailedCloseOutIsRetriedOnNextEvent(t *testing.T) {
	h := newHarness(t)
	key := podKey("failed-closeout")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, "uid-old", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first): %v", err)
	}

	// The reincarnation's close-out write is abandoned after retries.
	h.writer.failNextCommit()
	h.lister.set(key, newPod(key.Name, "uid-new", "2", "busybox:2"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(reincarnation): %v", err)
	}
	if got, want := h.writer.eventTypes(), []string{"Added", "Deleted", "Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
	if n := h.logs.countOf(errAsyncWriteFailed); n != 1 {
		t.Errorf("failed close-out logged %d times, want 1", n)
	}

	// Any later event for this name re-attempts the pending close-out.
	h.lister.set(key, newPod(key.Name, "uid-new", "3", "busybox:3"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(next event): %v", err)
	}

	records := h.writer.recorded()
	if got, want := h.writer.eventTypes(), []string{"Added", "Deleted", "Added", "Deleted", "Modified"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v (the pending close-out must be retried before the new event)", got, want)
	}
	if records[3].UID != "uid-old" {
		t.Errorf("retried close-out must still carry the old uid, got %q", records[3].UID)
	}
}

// TestProcessDeleteClaimedOnlyOnce is the duplicate-delete guard: two work items
// noticing the same disappearance (a redelivery, or the per-scope GC pass racing
// the live path) must yield exactly one Deleted row, because both claim through
// the same ReserveDelete primitive.
func TestProcessDeleteClaimedOnlyOnce(t *testing.T) {
	h := newHarness(t)
	key := podKey("claimed-once")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(add): %v", err)
	}

	// A failing delete write leaves the claim released but the entry present —
	// the state in which a duplicate could slip through if the claim were not
	// re-checked.
	h.lister.remove(key)
	h.writer.failNextCommit()
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(delete): %v", err)
	}
	// Retry, then a third attempt that must find nothing left to claim.
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(delete retry): %v", err)
	}
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(delete redelivery): %v", err)
	}

	if got, want := h.writer.eventTypes(), []string{"Added", "Deleted", "Deleted"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v — one failed delete, one successful retry, then nothing left to claim", got, want)
	}

	// After the successful delete committed, the entry is gone, so a further
	// redelivery has nothing to claim and stays silent.
	st, _ := h.pipeline.sinks.lookup(testSink)
	if _, exists := st.cache.Load(key.cacheKey()); exists {
		t.Error("cache entry survived a confirmed deletion")
	}
}

// TestProcessDeleteOfUnknownObjectIsSilent asserts the ordinary case behind that
// claim: a deletion event for an object the pipeline never recorded (e.g. it was
// created and deleted before this sink ever saw it) writes nothing rather than a
// Deleted row for an object with no history.
func TestProcessDeleteOfUnknownObjectIsSilent(t *testing.T) {
	h := newHarness(t)
	key := podKey("never-seen")
	h.warm(key)

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if got := h.writer.recorded(); len(got) != 0 {
		t.Fatalf("expected no records for an unknown object's deletion, got %+v", got)
	}
}

// TestProcessEnqueueFailureRevertsOptimisticEntry covers the synchronous
// hand-off failure: the job never entered the write pipeline, so no commit will
// ever fire for it and Process must undo its own optimistic cache entry. The
// proof is that the retry re-emits the same content instead of deduplicating it
// away.
func TestProcessEnqueueFailureRevertsOptimisticEntry(t *testing.T) {
	h := newHarness(t)
	key := podKey("enqueue-failure")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))

	queueFull := errors.New("write queue full")
	h.writer.failNextEnqueue(queueFull)

	err := h.pipeline.Process(h.ctx, key)
	if !errors.Is(err, queueFull) {
		t.Fatalf("Process error = %v, want the enqueue error so the item is retried", err)
	}
	st, _ := h.pipeline.sinks.lookup(testSink)
	if _, exists := st.cache.Load(key.cacheKey()); exists {
		t.Fatal("optimistic cache entry survived an enqueue failure; a lost write must never look persisted")
	}

	// The retry writes the full Added, proving nothing was deduplicated away.
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(retry): %v", err)
	}
	if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

// TestProcessEnqueueFailureOnDeleteReleasesClaim is the delete-path counterpart:
// a failed hand-off must release the ReserveDelete claim, or the deletion would
// be permanently unclaimable and the row lost forever.
func TestProcessEnqueueFailureOnDeleteReleasesClaim(t *testing.T) {
	h := newHarness(t)
	key := podKey("delete-enqueue-failure")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(add): %v", err)
	}

	h.lister.remove(key)
	queueFull := errors.New("write queue full")
	h.writer.failNextEnqueue(queueFull)
	if err := h.pipeline.Process(h.ctx, key); !errors.Is(err, queueFull) {
		t.Fatalf("Process error = %v, want the enqueue error", err)
	}

	st, _ := h.pipeline.sinks.lookup(testSink)
	entry, exists := st.cache.Load(key.cacheKey())
	if !exists {
		t.Fatal("the entry must survive a failed deletion write so the row can still be written later")
	}
	if entry.PendingDelete {
		t.Fatal("claim was not released after a failed hand-off; the deletion could never be retried")
	}

	// The retry lands the row.
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(retry): %v", err)
	}
	if got, want := h.writer.eventTypes(), []string{"Added", "Deleted"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

// TestProcessCorruptBaselineFallsBackToFullState ports the compressed-baseline
// corruption specs: a baseline that cannot be decompressed must degrade to a
// full-state write (Invariant 5) with the failure logged, never a dropped or
// mis-recorded event. The unchanged-object case must not even touch the
// baseline, since the dedup short-circuit compares hashes only.
func TestProcessCorruptBaselineFallsBackToFullState(t *testing.T) {
	t.Run("unchanged object short-circuits without decoding", func(t *testing.T) {
		h := newHarness(t)
		key := podKey("shortcircuit")
		h.warm(key)
		h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process: %v", err)
		}

		st, _ := h.pipeline.sinks.lookup(testSink)
		corruptBaseline(&st.cache, key.cacheKey())

		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(unchanged): %v", err)
		}
		if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
			t.Fatalf("event types = %v, want %v", got, want)
		}
		if errs := h.logs.loggedErrors(); len(errs) != 0 {
			t.Errorf("the corrupt baseline was decoded on the dedup hot path: %v", errs)
		}
	})

	t.Run("changed object writes full state and logs", func(t *testing.T) {
		h := newHarness(t)
		key := podKey("corrupt")
		h.warm(key)
		h.lister.set(key, newPod(key.Name, "uid-1", "1", "busybox:1"))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process: %v", err)
		}

		st, _ := h.pipeline.sinks.lookup(testSink)
		corruptBaseline(&st.cache, key.cacheKey())

		h.lister.set(key, newPod(key.Name, "uid-1", "2", "busybox:2"))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(changed): %v", err)
		}

		records := h.writer.recorded()
		if len(records) != 2 || records[1].EventType != "Modified" {
			t.Fatalf("expected a Modified record, got %v", h.writer.eventTypes())
		}
		if records[1].Data == "" || records[1].Diff != "" {
			t.Errorf("expected a full-state fallback, got data-empty=%v diff=%q", records[1].Data == "", records[1].Diff)
		}
		if errs := h.logs.loggedErrors(); len(errs) == 0 {
			t.Error("the decode failure must be logged at Error level, never swallowed")
		}
	})
}

// corruptBaseline truncates the compressed diff baseline stored for key so a
// later decodeBaseline fails, simulating an in-memory corruption. It leaves Hash
// untouched so callers can independently control whether the dedup
// short-circuit or the diff-decode path is exercised.
func corruptBaseline(hc *hashCache, key string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	entry := hc.data[key]
	if len(entry.JSON) > 1 {
		entry.JSON = entry.JSON[:len(entry.JSON)/2]
	}
	hc.data[key] = entry
}

// TestProcessDoesNotMutateTheListersObject is the watch-cache safety guard:
// ListerRegistry may hand back the informer's own shared instance, so Process
// must normalize a copy. Mutating the original would corrupt the cache for every
// other reader — including this pipeline's own next diff.
func TestProcessDoesNotMutateTheListersObject(t *testing.T) {
	h := newHarness(t)
	key := podKey("shared-instance")
	h.warm(key)

	pod := newPod(key.Name, "uid-1", "7", "busybox:1")
	meta := pod.Object["metadata"].(map[string]any)
	meta["managedFields"] = []any{map[string]any{"manager": "kubectl", "operation": "Update"}}
	meta["generation"] = int64(3)
	meta["annotations"] = map[string]any{ActorsAnnotation: "argocd-controller"}
	h.lister.set(key, pod)

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}

	for _, field := range []string{"managedFields", "resourceVersion", "generation", "annotations"} {
		if _, found, _ := unstructured.NestedFieldNoCopy(pod.Object, "metadata", field); !found {
			t.Errorf("Process stripped metadata.%s from the lister's own object", field)
		}
	}
}

// TestProcessRecordCarriesProvenance asserts the Record fields that come from the
// object rather than from the key: labels, actors, resourceVersion and the
// content hash all have to survive normalization even though most of what they
// are read from is stripped before hashing.
func TestProcessRecordCarriesProvenance(t *testing.T) {
	h := newHarness(t)
	key := podKey("provenance")
	h.warm(key)

	pod := newPod(key.Name, "uid-1", "42", "busybox:1")
	meta := pod.Object["metadata"].(map[string]any)
	meta["labels"] = map[string]any{"app": "demo"}
	meta["annotations"] = map[string]any{ActorsAnnotation: "argocd-controller,kubectl"}
	h.lister.set(key, pod)

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}

	record := h.writer.awaitRecords(t, 1)[0]
	if record.ClusterID != "test-cluster" {
		t.Errorf("cluster_id = %q, want the pipeline's configured cluster", record.ClusterID)
	}
	if record.ResourceVersion != "42" {
		t.Errorf("resource_version = %q, want 42", record.ResourceVersion)
	}
	if got := record.Labels["app"]; got != "demo" {
		t.Errorf("labels[app] = %q, want demo", got)
	}
	if want := []string{"argocd-controller", "kubectl"}; !slices.Equal(record.Actors, want) {
		t.Errorf("actors = %v, want %v (read from the transform's annotation)", record.Actors, want)
	}
	if record.SHA256 == "" || record.Data == "" {
		t.Errorf("an Added row must carry both the hash and the full state, got sha256=%q data-empty=%v",
			record.SHA256, record.Data == "")
	}
	// The operator-internal annotation is transport, not content: it must not
	// appear in the persisted state.
	if strings.Contains(record.Data, ActorsAnnotation) {
		t.Errorf("the actors annotation leaked into the stored object state: %s", record.Data)
	}
	// resourceVersion is stripped before hashing (it changes on every write), so
	// it must not appear in the stored state either.
	if strings.Contains(record.Data, `"resourceVersion"`) {
		t.Errorf("resourceVersion leaked into the stored object state: %s", record.Data)
	}
}

// TestObjectHashMatchesTheRecordedHash pins the contract ObjectHash exists for:
// the value an acceptance suite recomputes from the API server's copy of an
// object is the value the write path put in the sha256 column for it.
//
// Without this, Task 2.1's "no gaps" criterion — after an outage, the final
// sha256 in ClickHouse equals a live recompute — would be comparing two numbers
// that merely happened to agree, and would stop comparing anything at all the
// first time normalization changed on only one side.
func TestObjectHashMatchesTheRecordedHash(t *testing.T) {
	h := newHarness(t)
	key := podKey("hashed")
	h.warm(key)

	pod := newPod(key.Name, "uid-1", "17", "busybox:1")
	meta := pod.Object["metadata"].(map[string]any)
	meta["labels"] = map[string]any{"app": "demo"}
	h.lister.set(key, pod)

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}
	record := h.writer.awaitRecords(t, 1)[0]

	got, err := ObjectHash(pod, nil)
	if err != nil {
		t.Fatalf("ObjectHash: %v", err)
	}
	if got != record.SHA256 {
		t.Errorf("ObjectHash = %q, want the recorded %q", got, record.SHA256)
	}
}

// TestObjectHashIgnoresTransportFields asserts that the copy an acceptance suite
// reads from the API server hashes identically to the copy the operator observes
// through its informer.
//
// The two differ in exactly three ways — the informer's transform strips
// managedFields and adds the actors annotation, and every write bumps
// resourceVersion and generation — so a hash that were sensitive to any of them
// would make the recompute comparison fail on a perfectly converged object.
func TestObjectHashIgnoresTransportFields(t *testing.T) {
	fromAPIServer := newPod("shape", "uid-1", "17", "busybox:1")
	apiMeta := fromAPIServer.Object["metadata"].(map[string]any)
	apiMeta["managedFields"] = []any{map[string]any{"manager": "kubectl", "operation": "Apply"}}
	apiMeta["generation"] = int64(3)

	fromInformer := newPod("shape", "uid-1", "23", "busybox:1")
	informerMeta := fromInformer.Object["metadata"].(map[string]any)
	informerMeta["annotations"] = map[string]any{ActorsAnnotation: "kubectl"}
	informerMeta["generation"] = int64(4)

	apiHash, err := ObjectHash(fromAPIServer, nil)
	if err != nil {
		t.Fatalf("ObjectHash(api server copy): %v", err)
	}
	informerHash, err := ObjectHash(fromInformer, nil)
	if err != nil {
		t.Fatalf("ObjectHash(informer copy): %v", err)
	}
	if apiHash != informerHash {
		t.Errorf("the same object hashed differently through the two paths: %q vs %q", apiHash, informerHash)
	}

	// And it is still a content hash: a real change must change it.
	changed := newPod("shape", "uid-1", "17", "busybox:2")
	changedHash, err := ObjectHash(changed, nil)
	if err != nil {
		t.Fatalf("ObjectHash(changed): %v", err)
	}
	if changedHash == apiHash {
		t.Error("ObjectHash ignored a change to the object's spec")
	}
}

// --- Redaction (Task 3.3) ---
//
// The specs below are the pipeline half of redaction: not "does the redactor
// rewrite a path" (redact_test.go covers that in isolation) but "does scrubbing
// before hashing actually keep the value out of everything the sink stores" —
// the payload, the diff, and the hash that discriminates one state from another.

// podWithSecret builds a Pod whose container env carries value, plus a
// last-applied annotation embedding the same value — which is exactly what
// `kubectl apply` produces, and the reason the annotation is scrubbed
// unconditionally.
func podWithSecret(name, resourceVersion, value string) *unstructured.Unstructured {
	pod := newPod(name, testUID, resourceVersion, "busybox:1")
	container := pod.Object["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	container["env"] = []any{map[string]any{"name": "TOKEN", "value": value}}
	pod.Object["metadata"].(map[string]any)["annotations"] = map[string]any{
		LastAppliedConfigAnnotation: `{"spec":{"containers":[{"env":[{"name":"TOKEN","value":"` + value + `"}]}]}}`,
	}
	return pod
}

// envRedaction is the policy the specs below stream under.
const envRedactionPath = "spec.containers[*].env[*].value"

// TestRedactedDifferenceDedupsInsteadOfLeaking is *the* security property of
// Task 3.3, asserted by name: two states of an object that differ only in a
// redacted value must be indistinguishable to the pipeline.
//
// Indistinguishable has to mean all three of these at once, which is why one
// spec asserts all three: the same hash (so the value cannot be recovered by
// grinding candidates against the sha256 column), a dedup skip (so no second row
// is written at all), and therefore no diff — because a diff is where a
// value-changed-to-a-different-value would otherwise surface in full, even
// though the payload itself was scrubbed.
func TestRedactedDifferenceDedupsInsteadOfLeaking(t *testing.T) {
	h := newHarness(t)
	key := podKey("secretive")
	h.warm(key)
	h.redactions.set(key.Scope(), mustCompile(t, envRedactionPath))

	h.lister.set(key, podWithSecret(key.Name, "1", "hunter2"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first state): %v", err)
	}

	// The same object, with only the redacted value changed.
	h.lister.set(key, podWithSecret(key.Name, "2", "correct-horse-battery-staple"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(second state): %v", err)
	}

	records := h.writer.recorded()
	if len(records) != 1 {
		t.Fatalf("event types = %v, want exactly one row: the second state differs only in a redacted value",
			h.writer.eventTypes())
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.dedupSkips); got != 1 {
		t.Errorf("dedup_skips = %v, want 1 — the redacted difference must deduplicate, not write", got)
	}

	// The hashes of the two states, computed independently of the pipeline, are
	// equal: the sha256 column cannot distinguish them either.
	policy := mustCompile(t, envRedactionPath)
	firstHash, err := ObjectHash(podWithSecret(key.Name, "1", "hunter2"), policy)
	if err != nil {
		t.Fatalf("ObjectHash(first): %v", err)
	}
	secondHash, err := ObjectHash(podWithSecret(key.Name, "2", "correct-horse-battery-staple"), policy)
	if err != nil {
		t.Fatalf("ObjectHash(second): %v", err)
	}
	if firstHash != secondHash {
		t.Errorf("two states differing only in a redacted value hashed differently: %q vs %q",
			firstHash, secondHash)
	}
	if firstHash != records[0].SHA256 {
		t.Errorf("recorded sha256 = %q, want the redacted hash %q", records[0].SHA256, firstHash)
	}

	for _, secret := range []string{"hunter2", "correct-horse-battery-staple"} {
		if strings.Contains(records[0].Data, secret) {
			t.Errorf("the redacted value %q survived in the stored state: %s", secret, records[0].Data)
		}
	}
}

// TestRedactionHashIsStableAcrossRuns pins the other half of the dedup property:
// the same object under the same policy hashes the same every time. Without it
// the skip above would be luck — a hash that varied per call would re-write every
// object on every event and the dedup assertion would fail for the wrong reason.
func TestRedactionHashIsStableAcrossRuns(t *testing.T) {
	policy := mustCompile(t, envRedactionPath, "metadata.name")
	var first string
	for run := range 5 {
		hash, err := ObjectHash(podWithSecret("stable", "1", "hunter2"), policy)
		if err != nil {
			t.Fatalf("ObjectHash(run %d): %v", run, err)
		}
		if run == 0 {
			first = hash
			continue
		}
		if hash != first {
			t.Fatalf("hash changed between runs: %q then %q", first, hash)
		}
	}

	// A change *outside* the redacted paths still changes the hash — otherwise
	// "stable" would just mean "constant", and the pipeline would stop recording
	// real changes.
	changed := podWithSecret("stable", "1", "hunter2")
	changed.Object["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "busybox:2"
	changedHash, err := ObjectHash(changed, policy)
	if err != nil {
		t.Fatalf("ObjectHash(changed): %v", err)
	}
	if changedHash == first {
		t.Error("a change outside the redacted paths did not change the hash")
	}
}

// TestRedactedDiffCarriesNoUnredactedFragment covers the diff path, which is the
// leak a "redact the payload on the way out" design would miss entirely: the
// baseline and the update are both redacted before the diff is computed, so a
// change to a scrubbed value cannot appear as a patch operation carrying it.
func TestRedactedDiffCarriesNoUnredactedFragment(t *testing.T) {
	h := newHarness(t)
	key := podKey("diffed")
	h.warm(key)
	h.redactions.set(key.Scope(), mustCompile(t, envRedactionPath))

	const oldSecret = "hunter2"
	const newSecret = "correct-horse-battery-staple"

	h.lister.set(key, podWithSecret(key.Name, "1", oldSecret))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(baseline): %v", err)
	}

	// A real change (the image) *and* a change to the redacted value, so this
	// update genuinely produces a diff rather than deduplicating away.
	updated := podWithSecret(key.Name, "2", newSecret)
	updated.Object["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "busybox:2"
	h.lister.set(key, updated)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(update): %v", err)
	}

	records := h.writer.awaitRecords(t, 2)
	modified := records[1]
	if modified.EventType != eventModified {
		t.Fatalf("event types = %v, want the second row to be a Modified diff", h.writer.eventTypes())
	}
	if modified.Diff == "" {
		t.Fatal("the update produced no diff, so this spec would assert nothing")
	}
	if !strings.Contains(modified.Diff, "busybox:2") {
		t.Errorf("the diff does not carry the real change, so it is not the diff under test: %s", modified.Diff)
	}
	for _, fragment := range []string{oldSecret, newSecret} {
		for _, column := range []struct {
			name  string
			value string
		}{{"diff", modified.Diff}, {"data", modified.Data}} {
			if strings.Contains(column.value, fragment) {
				t.Errorf("the redacted value %q leaked into the %s column: %s",
					fragment, column.name, column.value)
			}
		}
	}
}

// TestLastAppliedIsScrubbedUnderAnEmptyPolicy is the built-in half of the AC,
// asserted through the pipeline rather than the redactor: an operator who
// configured no redaction at all still gets no `kubectl apply` payload in
// ClickHouse. It is the unit-level twin of the e2e assertion.
func TestLastAppliedIsScrubbedUnderAnEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	key := podKey("applied")
	h.warm(key)
	// Deliberately no h.redactions.set: this stream has no policy whatsoever.

	h.lister.set(key, podWithSecret(key.Name, "1", "hunter2"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process: %v", err)
	}

	record := h.writer.awaitRecords(t, 1)[0]
	if !strings.Contains(record.Data, RedactionSentinel) {
		t.Errorf("the last-applied annotation was not scrubbed: %s", record.Data)
	}
	// The env value itself is *not* redacted here — no policy asked for it — and
	// that is the point: only the annotation's embedded copy is gone, so the
	// assertion below proves the scrub rather than an absent fixture.
	if !strings.Contains(record.Data, `"value":"hunter2"`) {
		t.Errorf("the unredacted env value should still be recorded under an empty policy: %s", record.Data)
	}
	if strings.Contains(record.Data, `\"value\":\"hunter2\"`) {
		t.Errorf("the last-applied annotation's embedded copy survived: %s", record.Data)
	}
}

// TestProcessRetriesWhenNoRedactionPolicyIsInstalled covers the fail-closed
// direction of the RedactionRegistry contract. A scope whose last interest
// disappeared between the lister read and the policy lookup has no policy to
// write under, and the pipeline must retry rather than write object content with
// whatever redaction it can find — the retry then settles truthfully, because the
// next attempt's lister read reports the scope inactive and drops.
func TestProcessRetriesWhenNoRedactionPolicyIsInstalled(t *testing.T) {
	h := newHarness(t)
	key := podKey("policyless")
	h.warm(key)
	h.lister.set(key, newPod(key.Name, testUID, "1", "busybox:1"))
	h.redactions.drop(key.Scope())

	err := h.pipeline.Process(h.ctx, key)
	if !errors.Is(err, errRedactionUnavailable) {
		t.Fatalf("Process = %v, want errRedactionUnavailable", err)
	}
	if records := h.writer.recorded(); len(records) != 0 {
		t.Errorf("wrote %d record(s) with no redaction policy installed", len(records))
	}
	if got := h.logs.countOf(errRedactionUnavailable); got != 1 {
		t.Errorf("logged the missing policy %d times, want 1 (Invariant 4)", got)
	}

	// The scope really is gone: the retry drops rather than looping forever.
	h.lister.stopScope(key.Sink, key.Scope())
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(retry): %v", err)
	}
	if records := h.writer.recorded(); len(records) != 0 {
		t.Errorf("wrote %d record(s) for a stopped scope", len(records))
	}
}
