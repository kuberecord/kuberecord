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

// The scope log's own mechanics. That each accepted transition is recorded once,
// with its fields and its order intact, is the contract's obligation and is
// asserted by the conformance suite; what is here is this backend's physical
// answer to it — the key layout, the line shape, the separation from the records
// tree, and the retry queue that carries an epoch across an outage.
package s3

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// testScopeEvent builds one transition over a fixed scope, dated from the corpus
// epoch so nothing about the object it lands in depends on when the test ran.
func testScopeEvent(action sink.ScopeAction) sink.ScopeEvent {
	return sink.ScopeEvent{
		Action: action,
		Scope: sink.ScopeFilter{
			ClusterID: testClusterID,
			APIGroup:  "apps",
			Kind:      "Deployment",
			Namespace: "kuberecord-system",
		},
		APIVersion: "v1",
		RuleRef:    "streamrule/kuberecord-system/platform-baseline",
		TS:         corpusEpoch,
	}
}

// TestScopeObjectKeyLayout is the golden for the scope log's half of the format
// contract (D15). The layout is spelled out in the task and in scopesPartition,
// and a reader's glob depends on every segment of it, so it is pinned literally
// rather than rebuilt from the constants that produced it.
func TestScopeObjectKeyLayout(t *testing.T) {
	const hash = "0f6a9c1e6f5a4a2e9a1d2c0f7b8e5d310f6a9c1e6f5a4a2e9a1d2c0f7b8e5d31"

	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{
			name:   "with a prefix",
			prefix: "audit/kuberecord",
			want:   "audit/kuberecord/format=jsonl-v1/scopes/date=2026-03-14/" + hash + ".jsonl.zst",
		},
		{
			// An empty prefix contributes no segment, and above all no leading
			// slash: every reader of this bucket would have to cope with it forever.
			name:   "without a prefix",
			prefix: "",
			want:   "format=jsonl-v1/scopes/date=2026-03-14/" + hash + ".jsonl.zst",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := scopeObjectKey(tc.prefix, corpusEpoch, hash); got != tc.want {
				t.Errorf("scope object key:\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestScopeObjectsAreOutsideTheRecordsTree is what keeps a reader's glob honest.
// The two kinds of object share a format= partition and have different line
// shapes, so a records query that matched a scope object would meet a line with no
// `uid` and no `data` and either fail or infer a merged schema. The separation is
// asserted through the same predicate the writer and the test fake route on, and
// against the records key layout itself.
func TestScopeObjectsAreOutsideTheRecordsTree(t *testing.T) {
	scopeKey := scopeObjectKey(testPrefix, corpusEpoch, "deadbeef")
	recordKey := objectKey(testPrefix, testClusterID, corpusEpoch, "deadbeef")

	if !isScopeObjectKey(scopeKey) {
		t.Errorf("%s is not recognised as a scope object", scopeKey)
	}
	if isScopeObjectKey(recordKey) {
		t.Errorf("%s is recognised as a scope object", recordKey)
	}
	if strings.Contains(scopeKey, "cluster_id=") {
		t.Errorf("the scope log is partitioned by date alone, but %s carries a cluster_id partition", scopeKey)
	}
	if !strings.Contains(scopeKey, "/"+formatPartition+"/") {
		t.Errorf("%s is outside the versioned format partition; a reader could not tell which contract wrote it", scopeKey)
	}
}

// TestScopeObjectRoundTrip: every field of a transition survives the object it is
// written into, the empty-namespace (all-namespaces) scope included.
//
// The empty namespace is the case that matters. It is a *value* — the
// all-namespaces scope — and a line that omitted it, or a decoder that read it
// back as absent, would make the broadest scope in the archive the one nobody can
// query for.
func TestScopeObjectRoundTrip(t *testing.T) {
	started := testScopeEvent(sink.ScopeActionStarted)
	wide := testScopeEvent(sink.ScopeActionStopped)
	wide.Scope.Namespace = ""
	wide.RuleRef = ""
	wide.TS = corpusEpoch.Add(2 * time.Minute)

	events := []sink.ScopeEvent{started, wide}
	obj, err := encodeScopeObject(testPrefix, events)
	if err != nil {
		t.Fatalf("encodeScopeObject: %v", err)
	}

	got, err := decodeScopeObject(obj.Payload)
	if err != nil {
		t.Fatalf("decodeScopeObject: %v", err)
	}
	if len(got) != len(events) {
		t.Fatalf("decoded %d transitions, want %d", len(got), len(events))
	}
	for i, want := range events {
		if got[i].Action != want.Action || got[i].Scope != want.Scope ||
			got[i].APIVersion != want.APIVersion || got[i].RuleRef != want.RuleRef || !got[i].TS.Equal(want.TS) {
			t.Errorf("transition %d round-tripped as %+v, want %+v", i, got[i], want)
		}
	}

	// The line shape is part of the contract: a query engine reads these names.
	lines := payloadLines(t, decodePayload(t, obj.Payload))
	for _, field := range []string{`"ts":`, `"cluster_id":`, `"group":`, `"version":`, `"kind":`, `"namespace":`, `"action":`, `"rule_ref":`} {
		if !strings.Contains(string(lines[0]), field) {
			t.Errorf("scope line does not carry %s:\n%s", field, lines[0])
		}
	}
	if !strings.Contains(string(lines[1]), `"namespace":""`) {
		t.Errorf("the all-namespaces scope's empty namespace was omitted rather than written as a value:\n%s", lines[1])
	}
}

// TestScopeObjectPartitionsByTheTransitionsOwnDate: the date= partition comes from
// the transition's instant, never from the wall clock at write time. An epoch
// written late — a retry after an outage, or a drain at shutdown — must still be
// filed under the day the watch actually started or stopped, or a reader asking
// "was this being watched on the 14th?" gets the wrong answer.
func TestScopeObjectPartitionsByTheTransitionsOwnDate(t *testing.T) {
	// A zoned instant on purpose: 2026-03-15T00:30:00+02:00 is 2026-03-14 in UTC,
	// so a partition taken without normalising would land a day out.
	zoned := time.Date(2026, 3, 15, 0, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	event := testScopeEvent(sink.ScopeActionStarted)
	event.TS = zoned

	obj, err := encodeScopeObject(testPrefix, []sink.ScopeEvent{event})
	if err != nil {
		t.Fatalf("encodeScopeObject: %v", err)
	}
	if !strings.Contains(obj.Key, "date=2026-03-14/") {
		t.Errorf("key %s is not filed under the transition's UTC date (2026-03-14)", obj.Key)
	}
}

// TestScopeObjectIsDeterministic: the same transitions produce the same key and
// the same bytes, so a retried scope object overwrites its own object instead of
// recording the epoch twice.
func TestScopeObjectIsDeterministic(t *testing.T) {
	events := []sink.ScopeEvent{testScopeEvent(sink.ScopeActionStarted), testScopeEvent(sink.ScopeActionStopped)}

	first, err := encodeScopeObject(testPrefix, events)
	if err != nil {
		t.Fatalf("encodeScopeObject: %v", err)
	}
	second, err := encodeScopeObject(testPrefix, events)
	if err != nil {
		t.Fatalf("encodeScopeObject: %v", err)
	}
	if first.Key != second.Key {
		t.Errorf("the same transitions produced two keys:\n %s\n %s", first.Key, second.Key)
	}
	if string(first.Payload) != string(second.Payload) {
		t.Error("the same transitions produced different bytes; a retry would write a second object")
	}
}

// TestScopeBatchBecomesOneObject: a burst of transitions — a GitOps apply of
// several rules at once — coalesces into a single object rather than one object
// per rule edge. It is the reason the scope path has a batcher at all.
func TestScopeBatchBecomesOneObject(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})

	handed := []sink.ScopeEvent{
		testScopeEvent(sink.ScopeActionStarted),
		func() sink.ScopeEvent {
			ev := testScopeEvent(sink.ScopeActionStarted)
			ev.Scope.Namespace = ""
			ev.RuleRef = "clusterstreamrule//platform-baseline"
			return ev
		}(),
		func() sink.ScopeEvent {
			ev := testScopeEvent(sink.ScopeActionStopped)
			ev.TS = corpusEpoch.Add(time.Minute)
			return ev
		}(),
	}
	for i, event := range handed {
		if err := r.w.EnqueueScopeEvent(r.ctx, event); err != nil {
			t.Fatalf("EnqueueScopeEvent(%d): %v", i, err)
		}
	}

	waitFor(t, "the scope batch to be written", func() bool { return len(r.store.scopeSnapshot()) == len(handed) })
	if got := len(r.store.scopeObjects()); got != 1 {
		t.Errorf("%d transitions handed over at once produced %d objects, want 1", len(handed), got)
	}
	r.stop(t)

	// The drain must not re-emit what it already wrote.
	if got := len(r.store.scopeSnapshot()); got != len(handed) {
		t.Errorf("after shutdown the store holds %d transitions, want %d", got, len(handed))
	}
}

// TestScopeTransitionsSurviveAnOutage is the retry queue's reason to exist. A
// record whose write fails will be observed again and re-recorded; a scope
// transition happens once, so a store that is down when the epoch is handed over
// must not cost the epoch.
//
// The store refuses every scope PUT, then starts accepting them, and the
// transition has to appear without anyone re-submitting it.
func TestScopeTransitionsSurviveAnOutage(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})
	// A short window so the first flush gives up quickly and the transition lands
	// on the retry queue rather than being retried in place for 30 seconds.
	r.w.scopeMaxRetryBackoff = 50 * time.Millisecond
	r.store.setScopeFault(errors.New("s3 test: the store is down"))

	if err := r.w.EnqueueScopeEvent(r.ctx, testScopeEvent(sink.ScopeActionStarted)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}
	waitFor(t, "the failing flush to put the transition on the retry queue", func() bool {
		return r.store.scopeAttemptCount() > 0 && len(r.store.scopeSnapshot()) == 0
	})

	r.store.setScopeFault(nil)
	waitFor(t, "the retry queue to carry the transition through", func() bool {
		return len(r.store.scopeSnapshot()) == 1
	})
	r.stop(t)

	if got := len(r.store.scopeSnapshot()); got != 1 {
		t.Errorf("the store holds %d transitions, want exactly 1: the retry must not record the epoch twice", got)
	}
}

// TestScopeDrainWritesWhatIsStillHeld: a transition handed over just before
// shutdown, with nothing to trigger its batcher, is written by the drain. A
// scope epoch lost at shutdown is an audit hole nothing can reconstruct — the
// operator is gone, and this backend cannot read its own history to notice.
func TestScopeDrainWritesWhatIsStillHeld(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})

	if err := r.w.EnqueueScopeEvent(r.ctx, testScopeEvent(sink.ScopeActionStopped)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}
	r.stop(t)

	if got := len(r.store.scopeSnapshot()); got != 1 {
		t.Errorf("the store holds %d transitions after the drain, want 1", got)
	}
}

// TestEncodeScopeObjectRejectsAnEmptyBatch: an object with no transitions in it
// would be permanently retained for nothing, exactly as an empty records object
// would, so it is refused rather than written.
func TestEncodeScopeObjectRejectsAnEmptyBatch(t *testing.T) {
	obj, err := encodeScopeObject(testPrefix, nil)
	if err == nil {
		t.Fatalf("expected an error, got object %s", obj.Key)
	}
	if !errors.Is(err, errEmptyBatch) {
		t.Errorf("error %q is not errEmptyBatch", err)
	}
}

// TestDecodeScopeObjectRejectsCorruptPayloads: a damaged scope object is reported
// as damaged. A decoder that returned the transitions it managed to read would
// make a truncated epoch log indistinguishable from a short one — and the epoch
// log is precisely what a reader consults to find out whether a gap is real.
func TestDecodeScopeObjectRejectsCorruptPayloads(t *testing.T) {
	obj, err := encodeScopeObject(testPrefix, []sink.ScopeEvent{testScopeEvent(sink.ScopeActionStarted)})
	if err != nil {
		t.Fatalf("encodeScopeObject: %v", err)
	}

	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "not a zstd frame", payload: []byte("plain JSONL\n"), want: "not a zstd frame"},
		{name: "truncated frame", payload: obj.Payload[:len(obj.Payload)/2], want: "decompress object payload"},
		{name: "malformed line", payload: zstdEncoder.EncodeAll([]byte("{\"ts\": not json}\n"), nil), want: "decode scope transition 0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			events, decErr := decodeScopeObject(tc.payload)
			if decErr == nil {
				t.Fatalf("expected an error, got %d transitions", len(events))
			}
			if events != nil {
				t.Errorf("a failed decode must return no transitions, got %d", len(events))
			}
			if !strings.Contains(decErr.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", decErr, tc.want)
			}
		})
	}
}

// TestEnqueueScopeEventHonoursTheCallersDeadline: the scope hand-off is bounded
// like the record one. The recorder calls it from its own goroutine precisely
// because it can block, and a caller that has given up must get its context's
// error back rather than waiting out the writer's own timeout.
func TestEnqueueScopeEventHonoursTheCallersDeadline(t *testing.T) {
	// Not started: nothing drains the queue, so filling it is deterministic.
	w := newTestWriter(newFakeStore(), Config{Bucket: "b", EnqueueTimeout: 5 * time.Second})
	for i := range scopeQueueSize {
		if err := w.EnqueueScopeEvent(context.Background(), testScopeEvent(sink.ScopeActionStarted)); err != nil {
			t.Fatalf("EnqueueScopeEvent(%d) was refused while the queue still had room: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.EnqueueScopeEvent(ctx, testScopeEvent(sink.ScopeActionStopped))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("EnqueueScopeEvent accepted a transition on a full queue")
	}
	if elapsed >= w.enqueueTimeout {
		t.Errorf("EnqueueScopeEvent took %s to honour a 50ms deadline; it waited out its own %s timeout instead",
			elapsed, w.enqueueTimeout)
	}
}
