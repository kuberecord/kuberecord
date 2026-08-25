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
	"encoding/json"
	"slices"
	"strconv"
	"testing"

	"github.com/wI2L/jsondiff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// eventModified and eventCheckpoint spell the two event_type values these specs
// assert on. The pipeline writes them as string literals (there is no exported
// constant to share — see test/harness/clickhouse.go for the same note), so they
// are spelled once here rather than scattered through the assertions.
const (
	eventModified   = "Modified"
	eventCheckpoint = "Checkpoint"
)

// The specs in this file cover Checkpoint rows (Task 2.2): the two independent
// triggers that promote a diff-only Modified write to a Checkpoint carrying the
// full state as well as the diff, and the bounds of that promotion.

// TestCheckpointDue is the decision itself, in isolation: which trigger fires for
// a given cadence, run length, and pair of sizes. The scenario tests below prove
// the pipeline feeds it the right operands; this one proves the rule.
func TestCheckpointDue(t *testing.T) {
	tests := []struct {
		name        string
		every       int
		modifiedRun int
		diffBytes   int
		fullBytes   int
		wantReason  checkpointReason
		wantDue     bool
	}{
		{
			name: "mid-run-small-diff-is-a-plain-modified",
			// The run has not reached the cadence and the diff is a fraction of
			// the state: the ordinary steady-state case.
			every: 50, modifiedRun: 7, diffBytes: 120, fullBytes: 4000,
			wantDue: false,
		},
		{
			name: "run-reaching-the-cadence-fires-on-count",
			// Exactly the Nth: the trigger is >= so the Nth write is the
			// Checkpoint, never the (N+1)th.
			every: 3, modifiedRun: 3, diffBytes: 120, fullBytes: 4000,
			wantReason: checkpointReasonCount, wantDue: true,
		},
		{
			name:  "run-one-short-of-the-cadence-does-not-fire",
			every: 3, modifiedRun: 2, diffBytes: 120, fullBytes: 4000,
			wantDue: false,
		},
		{
			name: "diff-larger-than-the-state-fires-on-size",
			// Independent of the counter: the run is only just starting.
			every: 1000, modifiedRun: 1, diffBytes: 4001, fullBytes: 4000,
			wantReason: checkpointReasonSize, wantDue: true,
		},
		{
			name: "diff-equal-to-the-state-does-not-fire",
			// Strictly larger, not "as large as": a diff that merely matches the
			// state's size still costs the reader nothing extra to keep.
			every: 1000, modifiedRun: 1, diffBytes: 4000, fullBytes: 4000,
			wantDue: false,
		},
		{
			name:  "count-is-reported-when-both-triggers-would-fire",
			every: 3, modifiedRun: 3, diffBytes: 9000, fullBytes: 4000,
			wantReason: checkpointReasonCount, wantDue: true,
		},
		{
			name:  "zero-cadence-disables-the-count-trigger",
			every: 0, modifiedRun: 5000, diffBytes: 120, fullBytes: 4000,
			wantDue: false,
		},
		{
			name: "zero-cadence-disables-the-size-trigger-too",
			// checkpointEvery: 0 means "no Checkpoint rows for this sink",
			// however unflattering an individual diff's size is.
			every: 0, modifiedRun: 1, diffBytes: 40000, fullBytes: 4000,
			wantDue: false,
		},
		{
			name:  "negative-cadence-is-treated-as-disabled",
			every: -1, modifiedRun: 99, diffBytes: 40000, fullBytes: 4000,
			wantDue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, due := checkpointDue(tt.every, tt.modifiedRun,
				make([]byte, tt.diffBytes), make([]byte, tt.fullBytes))
			if due != tt.wantDue {
				t.Fatalf("checkpointDue(...) due = %v, want %v", due, tt.wantDue)
			}
			if reason != tt.wantReason {
				t.Errorf("checkpointDue(...) reason = %q, want %q", reason, tt.wantReason)
			}
		})
	}
}

// TestCheckpointEveryForWriterWithoutPolicy pins the fallback for a sink whose
// backend declares no cadence at all: no checkpoints. The pipeline must not invent
// one on a backend's behalf — a Writer-only sink (see sink.StateReader's note on
// the optional halves of the contract) has an owner who never asked for them.
func TestCheckpointEveryForWriterWithoutPolicy(t *testing.T) {
	if got := checkpointEveryFor(policyLessWriter{}); got != 0 {
		t.Errorf("checkpointEveryFor(writer without a policy) = %d, want 0 (disabled)", got)
	}
	w := newFakeWriter()
	w.setCheckpointEvery(17)
	if got := checkpointEveryFor(w); got != 17 {
		t.Errorf("checkpointEveryFor(writer declaring 17) = %d, want 17", got)
	}
}

// TestProcessCheckpointFiresOnExactlyTheNthModified drives real work items through
// Process and asserts the cadence lands on the Nth Modified — not the (N+1)th, and
// not once per Nth-and-after — and that the Checkpoint row is a Modified in every
// respect except its event_type and its populated data column.
func TestProcessCheckpointFiresOnExactlyTheNthModified(t *testing.T) {
	h := newHarness(t)
	h.writer.setCheckpointEvery(3)
	key := podKey("cadence")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, testUID, "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(create): %v", err)
	}
	for i := range 6 {
		h.lister.set(key, newPod(key.Name, testUID, rv(i+2), image(i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(update %d): %v", i, err)
		}
	}

	want := []string{
		"Added",
		eventModified, eventModified, eventCheckpoint,
		eventModified, eventModified, eventCheckpoint,
	}
	if got := h.writer.eventTypes(); !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}

	records := h.writer.recorded()
	checkpoint := records[3]
	if checkpoint.Data == "" {
		t.Error("a Checkpoint row must carry the full state in data")
	}
	if checkpoint.Diff == "" {
		t.Error("a Checkpoint row must carry the diff as well as the full state")
	}
	// Everything else is a Modified: same identity, same provenance, same hash
	// discipline — so warm queries (argMax over the whole history) and the dedup
	// baseline are unaffected by the promotion.
	if checkpoint.SHA256 == "" || checkpoint.UID != testUID || checkpoint.ResourceVersion == "" {
		t.Errorf("Checkpoint row lost Modified provenance: %+v", checkpoint)
	}
	// The full state in data must be the object's own normalized JSON as of the
	// checkpointed event (the third update), i.e. the exact bytes a
	// reconstruction is expected to land on.
	wantJSON, err := NormalizedJSON(newPod(key.Name, testUID, rv(4), image(2)), nil)
	if err != nil {
		t.Fatalf("NormalizedJSON: %v", err)
	}
	if checkpoint.Data != string(wantJSON) {
		t.Errorf("Checkpoint data =\n%s\nwant\n%s", checkpoint.Data, wantJSON)
	}
}

// TestProcessCheckpointSizeTriggerIsIndependentOfTheCounter covers the second
// trigger with the cadence set far out of reach, so only the size comparison can
// fire.
//
// The two cases together are what prove the comparison's operands are the ones
// Task 2.2 requires. The big-diff case fires while the counter is at 1; the
// small-diff case does not fire at all — which could not be true if the row's
// `data` *column* were an operand, since that column is empty on a Modified row
// and every non-empty diff would then look oversized. (The cached baseline is
// likewise not an operand: it is zstd-compressed, so comparing against it would
// over-trigger by the compression ratio.)
func TestProcessCheckpointSizeTriggerIsIndependentOfTheCounter(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(key Key) *unstructured.Unstructured
		wantEvent string
		wantData  bool
	}{
		{
			name: "diff-larger-than-the-new-full-state-is-checkpointed",
			// Every nodeSelector key is renamed, so the patch carries a remove
			// and an add (with full JSON pointers) per entry and outgrows the
			// small object it describes.
			mutate: func(key Key) *unstructured.Unstructured {
				return newPodWithSelector(key.Name, "2", "renamed-")
			},
			wantEvent: eventCheckpoint,
			wantData:  true,
		},
		{
			name: "ordinary-small-diff-stays-a-plain-modified",
			// One changed image: a handful of diff bytes against a much larger
			// object.
			mutate: func(key Key) *unstructured.Unstructured {
				pod := newPodWithSelector(key.Name, "2", "sel-")
				pod.Object["spec"].(map[string]any)["containers"] =
					[]any{map[string]any{"name": "c", "image": "busybox:2"}}
				return pod
			},
			wantEvent: eventModified,
			wantData:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			// Far out of reach: only the size trigger can fire below.
			h.writer.setCheckpointEvery(1000)
			key := podKey("size-trigger")
			h.warm(key)

			before := newPodWithSelector(key.Name, "1", "sel-")
			h.lister.set(key, before)
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(create): %v", err)
			}

			after := tt.mutate(key)
			h.lister.set(key, after)
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(update): %v", err)
			}

			// State the fixture's premise explicitly, so a future jsondiff
			// change that stops producing an oversized patch fails here — naming
			// the fixture — instead of failing the event-type assertion below
			// and reading like a regression in the trigger.
			assertDiffSize(t, before, after, tt.wantEvent == eventCheckpoint)

			records := h.writer.recorded()
			if len(records) != 2 {
				t.Fatalf("recorded %d rows, want 2: %v", len(records), h.writer.eventTypes())
			}
			got := records[1]
			if got.EventType != tt.wantEvent {
				t.Errorf("event type = %q, want %q", got.EventType, tt.wantEvent)
			}
			if (got.Data != "") != tt.wantData {
				t.Errorf("data populated = %v, want %v", got.Data != "", tt.wantData)
			}
			if got.Diff == "" {
				t.Error("both a Modified and a Checkpoint must carry the diff")
			}
		})
	}
}

// TestProcessCheckpointEveryZeroNeverCheckpoints asserts the off switch is a real
// off switch: neither trigger may fire for a sink whose owner set
// checkpointEvery: 0, no matter how long the diff run gets or how large a single
// diff turns out to be.
func TestProcessCheckpointEveryZeroNeverCheckpoints(t *testing.T) {
	h := newHarness(t)
	h.writer.setCheckpointEvery(0)
	key := podKey("disabled")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, testUID, "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(create): %v", err)
	}
	// A long diff run — well past any plausible cadence.
	for i := range 8 {
		h.lister.set(key, newPod(key.Name, testUID, rv(i+2), image(i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(update %d): %v", i, err)
		}
	}
	// Then a mutation whose diff dwarfs the object it describes, which is what
	// the size trigger exists for.
	h.lister.set(key, newPodWithSelector(key.Name, "20", "sel-"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(oversized diff): %v", err)
	}
	h.lister.set(key, newPodWithSelector(key.Name, "21", "renamed-"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(oversized diff): %v", err)
	}

	for i, event := range h.writer.eventTypes() {
		if i == 0 {
			continue // the creation is an Added and carries data by design
		}
		if event != eventModified {
			t.Fatalf("row %d is %q; checkpointEvery: 0 must never produce anything but Modified: %v",
				i, event, h.writer.eventTypes())
		}
	}
	for i, record := range h.writer.recorded()[1:] {
		if record.Data != "" {
			t.Errorf("row %d carries data with checkpointing disabled", i+1)
		}
	}
}

// TestProcessCheckpointCounterResetsAfterRestart documents and pins the accepted
// cost of keeping the per-key modified counter in memory (see
// CacheEntry.ModifiedSinceCheckpoint): an operator restart forgets how far each
// key's diff run had progressed.
//
// That is deliberate and free, because a restart re-baselines anyway — the fresh
// process's first row for a key is a data-bearing Added/Snapshot, which is exactly
// what a Checkpoint would have been. The run therefore restarts from that row, and
// the next Checkpoint is a full cadence later rather than "wherever the old process
// happened to have got to". This test drives two mutations, restarts, and proves
// the very next mutation is a plain Modified (a carried-over count of 2 would have
// made it the Checkpoint) while the third one after the restart is the Checkpoint.
func TestProcessCheckpointCounterResetsAfterRestart(t *testing.T) {
	h := newHarness(t)
	h.writer.setCheckpointEvery(3)
	key := podKey("restarted")
	h.warm(key)

	h.lister.set(key, newPod(key.Name, testUID, "1", "busybox:1"))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(create): %v", err)
	}
	for i := range 2 {
		h.lister.set(key, newPod(key.Name, testUID, rv(i+2), image(i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(update %d): %v", i, err)
		}
	}
	if got, want := h.writer.eventTypes(), []string{"Added", eventModified, eventModified}; !slices.Equal(got, want) {
		t.Fatalf("pre-restart event types = %v, want %v", got, want)
	}

	// The operator restarts: the watch cache and the sink's history survive, every
	// in-memory pipeline cache does not (Invariant 6).
	h.restart(t)
	h.warm(key)

	for i := range 4 {
		h.lister.set(key, newPod(key.Name, testUID, rv(i+10), image(i+10)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("Process(post-restart update %d): %v", i, err)
		}
	}

	want := []string{
		"Added", eventModified, eventModified, // before the restart
		"Added",                                       // the re-baselining first sighting in the new process
		eventModified, eventModified, eventCheckpoint, // the run starts over from there
	}
	if got := h.writer.eventTypes(); !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

// assertDiffSize states a size-trigger fixture's premise: whether the patch
// between two states really is larger than the newer state's own normalized JSON.
// It reproduces the comparison the pipeline makes, from the same two normalized
// documents, so a fixture that stops being oversized (or accidentally becomes so)
// is reported as a broken fixture rather than as a broken trigger.
func assertDiffSize(t *testing.T, before, after *unstructured.Unstructured, wantOversized bool) {
	t.Helper()
	beforeJSON, err := NormalizedJSON(before, nil)
	if err != nil {
		t.Fatalf("NormalizedJSON(before): %v", err)
	}
	afterJSON, err := NormalizedJSON(after, nil)
	if err != nil {
		t.Fatalf("NormalizedJSON(after): %v", err)
	}
	patch, err := jsondiff.CompareJSON(beforeJSON, afterJSON)
	if err != nil {
		t.Fatalf("CompareJSON: %v", err)
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		t.Fatalf("marshalling patch: %v", err)
	}
	if oversized := len(patchBytes) > len(afterJSON); oversized != wantOversized {
		t.Fatalf("fixture: diff is %d bytes and the new full state is %d (oversized = %v, want %v)",
			len(patchBytes), len(afterJSON), oversized, wantOversized)
	}
}

// rv renders a resourceVersion for the i-th update, and image an image tag, so the
// loops above read as "another genuine change" rather than as string plumbing.
func rv(i int) string { return "rv-" + strconv.Itoa(i) }

func image(i int) string { return "busybox:" + strconv.Itoa(i) }

// policyLessWriter is a sink.Writer that declares no Checkpoint cadence, standing
// in for a backend that never opted into the optional CheckpointPolicy half of the
// contract. It deliberately does not embed fakeWriter: embedding would promote
// that type's CheckpointEvery method and make this double satisfy the very
// interface it exists to *not* satisfy.
type policyLessWriter struct{}

func (policyLessWriter) Start(ctx context.Context) error { <-ctx.Done(); return nil }

func (policyLessWriter) Enqueue(_ context.Context, job sink.Job) error {
	if job.Commit != nil {
		job.Commit(true)
	}
	return nil
}
