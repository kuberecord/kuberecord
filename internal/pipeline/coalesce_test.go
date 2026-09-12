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
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/goleak"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// Event count-bump coalescing (Task 20.1). These specs are about the *volume*
// axis, which is a different thing from the relevance axis Phase 19 shipped and
// is not helped by it: a filter narrows which Event streams are kept, and this
// bounds how many rows each one produces (D50).
//
// The two halves are tested separately on purpose. The projection (what counts as
// "nothing but a bump") is pure and is tabled against both Event APIs and against
// a field no Kubernetes version has invented yet; the decision and the flush are
// driven through a real Process with an injected clock, because the property that
// matters most — the last bump of a burst is recorded with its true count — is
// about what happens *after* the writes stop.

// testWindow is the coalescing window every spec here declares. It is an
// unremarkable round value: nothing in the design depends on its size, and each
// spec advances an injected clock relative to it rather than waiting.
const testWindow = time.Minute

// bumpInstant is the timestamp a bump rewrites in place. Its value is immaterial —
// only that it differs from the baseline's absent one.
const bumpInstant = "2026-09-12T10:05:00Z"

// fakeClock is the Pipeline's injected time source. Every spec that measures a
// window uses one, so the assertions are about elapsed time in the model rather
// than about how long the test took to run.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	// An arbitrary fixed instant, in UTC because every stamped row is.
	return &fakeClock{now: time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// requeueCall is one delayed re-delivery the pipeline asked for.
type requeueCall struct {
	key   Key
	after time.Duration
}

// requeueRecorder stands in for the delaying queue's AddAfter, so a spec can
// assert *what was scheduled* without waiting for it — and can then drive the
// flush itself, at exactly the instant it wants to test.
//
// Substituting it also removes the only source of real-time behaviour from these
// specs: nothing here sleeps, and a slow CI machine changes no outcome.
type requeueRecorder struct {
	mu    sync.Mutex
	calls []requeueCall
}

func (r *requeueRecorder) add(key Key, after time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, requeueCall{key: key, after: after})
}

func (r *requeueRecorder) all() []requeueCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]requeueCall(nil), r.calls...)
}

func (r *requeueRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// coalescing puts the harness into the configuration every spec below shares: the
// sink declares window, the clock is injected and stopped, and delayed
// re-deliveries are captured rather than performed.
//
// It returns both seams because a spec that asserts the flush needs to advance
// the clock *and* to know what delay was asked for.
func (h *testHarness) coalescing(window time.Duration) (*fakeClock, *requeueRecorder) {
	clock := newFakeClock()
	requeues := &requeueRecorder{}
	h.writer.setCoalesceWindow(window)
	h.pipeline.now = clock.Now
	h.pipeline.requeueAfter = requeues.add
	return clock, requeues
}

// bump advances an Event to the next count, exactly as the API server does: the
// resourceVersion moves, the count moves, and nothing else does.
//
// The UID is testUID throughout, and deliberately so: a changed UID is a *new
// Event* under an old name, which the projection sees and writes (see the
// metadata.uid case in TestEventCoalesceHashExcludesOnlyTheBumpFields). Holding
// it fixed here is what makes every spec below a statement about bumps.
func bump(group, name, resourceVersion string, count int64) *unstructured.Unstructured {
	return newEvent(group, name, testUID, resourceVersion, count)
}

// counts reads the `count` field out of each recorded row's data column, which is
// where a full-state Event row carries it. A row that is not full state, or whose
// data will not parse, fails the spec rather than silently contributing a zero.
func counts(t *testing.T, records []sink.Record) []int64 {
	t.Helper()
	got := make([]int64, 0, len(records))
	for i, record := range records {
		if record.Data == "" {
			t.Fatalf("record %d (%s) carries no data; every Event row is a full-state row",
				i, record.EventType)
		}
		got = append(got, eventCountIn(t, record.Data))
	}
	return got
}

// eventCountIn pulls `count` out of one row's stored JSON.
func eventCountIn(t *testing.T, data string) int64 {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal([]byte(data), &decoded); err != nil {
		t.Fatalf("decoding a recorded Event row: %v", err)
	}
	raw, ok := decoded["count"]
	if !ok {
		t.Fatalf("a recorded Event row carries no count: %s", data)
	}
	switch typed := raw.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	default:
		t.Fatalf("a recorded Event row's count is %T, not a number: %s", raw, data)
		return 0
	}
}

//
// The projection: what counts as "nothing but a bump"
//

// TestEventCoalesceHashExcludesOnlyTheBumpFields is the by-exclusion rule's own
// table, and it is the spec the backlog singles out as the one an implementation
// gets subtly wrong.
//
// The two halves are not symmetrical in what they protect. The excluded half
// states which restatements may be suppressed; the compared half is what stops a
// real signal from being suppressed with them — and its last case, a field no
// Kubernetes version has invented yet, is the whole reason the comparison is by
// exclusion rather than by allow-list. An allow-list would pass every other case
// here and silently start dropping that one the day it appeared.
func TestEventCoalesceHashExcludesOnlyTheBumpFields(t *testing.T) {
	tests := []struct {
		name string
		// mutate edits one field of an Event that is otherwise identical to the
		// baseline.
		mutate func(object map[string]any)
		// sameHash says whether the edit must be invisible to the projection.
		sameHash bool
		why      string
	}{
		{
			name:     "count",
			mutate:   func(o map[string]any) { o["count"] = int64(47) },
			sameHash: true,
			why:      "the bump itself: count is cumulative, so the next row written carries it",
		},
		{
			name:     "lastTimestamp",
			mutate:   func(o map[string]any) { o["lastTimestamp"] = bumpInstant },
			sameHash: true,
			why:      "rewritten by the same update that bumps count",
		},
		{
			name:     "eventTime",
			mutate:   func(o map[string]any) { o["eventTime"] = bumpInstant },
			sameHash: true,
			why:      "the modern API's instant, rewritten in place for a series",
		},
		{
			name: "series",
			mutate: func(o map[string]any) {
				o["series"] = map[string]any{"count": int64(9), "lastObservedTime": bumpInstant}
			},
			sameHash: true,
			why:      "the aggregated bump, excluded as a whole subtree",
		},
		{
			name:     "deprecatedCount",
			mutate:   func(o map[string]any) { o["deprecatedCount"] = int64(47) },
			sameHash: true,
			why: "count as events.k8s.io/v1 spells it; left in, this window would be a " +
				"silent no-op for every rule naming the modern group (D31)",
		},
		{
			name:     "deprecatedLastTimestamp",
			mutate:   func(o map[string]any) { o["deprecatedLastTimestamp"] = bumpInstant },
			sameHash: true,
			why:      "lastTimestamp under the modern API's name",
		},
		{
			name:     "message",
			mutate:   func(o map[string]any) { o["message"] = "Back-off pulling image" },
			sameHash: false,
			why:      "a different fact about what happened, whatever the window says",
		},
		{
			name:     "reason",
			mutate:   func(o map[string]any) { o["reason"] = "Failed" },
			sameHash: false,
			why:      "a different fact",
		},
		{
			name:     "type",
			mutate:   func(o map[string]any) { o["type"] = "Normal" },
			sameHash: false,
			why:      "a Warning becoming Normal is the change an operator most wants to see",
		},
		{
			name: "involvedObject",
			mutate: func(o map[string]any) {
				o["involvedObject"] = map[string]any{
					"kind": "Pod", "namespace": "default", "name": "other", "uid": "other-uid",
				}
			},
			sameHash: false,
			why:      "a different subject is a different Event in every sense but its name",
		},
		{
			name:     "firstTimestamp",
			mutate:   func(o map[string]any) { o["firstTimestamp"] = "2026-09-12T09:00:00Z" },
			sameHash: false,
			why: "when it *started* is the property a coalesced archive has to agree with an " +
				"uncoalesced one about, so it is compared and not excluded",
		},
		{
			name: "metadata.uid",
			mutate: func(o map[string]any) {
				o["metadata"].(map[string]any)["uid"] = "a-different-event"
			},
			sameHash: false,
			why: "a new Event took the name over; the UID is inside the projection precisely " +
				"so a reincarnation can never be suppressed as a bump",
		},
		{
			name:     "a field Kubernetes has not invented yet",
			mutate:   func(o map[string]any) { o["someFutureSignal"] = "escalating" },
			sameHash: false,
			why: "the whole point of comparing by exclusion: an unrecognised field defaults " +
				"to write. An allow-list would drop this signal the day it shipped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseline := newEvent("", "crash", testUID, "100", 1).Object
			mutated := newEvent("", "crash", testUID, "100", 1).Object
			tt.mutate(mutated)

			baseHash, err := eventCoalesceHash(baseline)
			if err != nil {
				t.Fatalf("hashing the baseline: %v", err)
			}
			mutatedHash, err := eventCoalesceHash(mutated)
			if err != nil {
				t.Fatalf("hashing the mutation: %v", err)
			}

			if tt.sameHash && baseHash != mutatedHash {
				t.Errorf("changing %s changed the coalescing projection, so a bump would be "+
					"written as a change.\nIt must not: %s", tt.name, tt.why)
			}
			if !tt.sameHash && baseHash == mutatedHash {
				t.Errorf("changing %s left the coalescing projection identical, so the change "+
					"could be suppressed as if it were a bump.\nIt must not: %s", tt.name, tt.why)
			}
		})
	}
}

// TestEventCoalesceHashSeesNoResourceVersion pins the one field the exclusion set
// deliberately does *not* name.
//
// resourceVersion bumps on every write, so leaving it in the projection would
// make every bump look like a change and the window would never fire. It is
// absent from eventBumpFields because it never arrives: stripVolatileFields
// removes it before the object is hashed at all. That is a claim about code in
// another file, so it is asserted rather than trusted — if normalizeObject ever
// stopped stripping it, this fails here instead of silently turning coalescing
// off in production.
func TestEventCoalesceHashSeesNoResourceVersion(t *testing.T) {
	first, err := normalizeObject(newEvent("", "crash", testUID, "100", 1), nil)
	if err != nil {
		t.Fatalf("normalizing: %v", err)
	}
	second, err := normalizeObject(newEvent("", "crash", testUID, "37000", 1), nil)
	if err != nil {
		t.Fatalf("normalizing: %v", err)
	}

	metadata, ok := first.normalized["metadata"].(map[string]any)
	if !ok {
		t.Fatal("the normalized Event has no metadata map")
	}
	if _, present := metadata["resourceVersion"]; present {
		t.Error("resourceVersion survived normalization, so it reaches the coalescing " +
			"projection. It bumps on every write, so the window would never fire — either " +
			"restore the strip or add it to eventBumpFields")
	}

	firstHash, err := eventCoalesceHash(first.normalized)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	secondHash, err := eventCoalesceHash(second.normalized)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	if firstHash != secondHash {
		t.Error("two states of one Event differing only in resourceVersion projected " +
			"differently; every count bump would then be written as a change")
	}
}

//
// The decision
//

// TestCoalescerDecide tables every way a bump can fail to be suppressible. Four of
// the five rungs are "write anyway", which is the direction the whole design
// leans: suppression is the exception, and it needs all three of enabled, known
// and recent.
func TestCoalescerDecide(t *testing.T) {
	start := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	const key = "|Event|default/crash"

	tests := []struct {
		name      string
		seed      bool
		seedHash  string
		window    time.Duration
		hash      string
		at        time.Time
		wantSkip  bool
		wantFlush time.Duration
		why       string
	}{
		{
			name: "coalescing disabled", seed: true, seedHash: "h", window: 0,
			hash: "h", at: start, wantSkip: false,
			why: "0 must stay expressible: an operator who wants every bump recorded says so",
		},
		{
			name: "no row recorded yet", seed: false, window: testWindow,
			hash: "h", at: start, wantSkip: false,
			why: "there is nothing to be redundant with — a first sighting always writes",
		},
		{
			name: "a different fact", seed: true, seedHash: "h", window: testWindow,
			hash: "other", at: start.Add(time.Second), wantSkip: false,
			why: "a changed message or subject writes immediately, whatever the window says",
		},
		{
			name: "window already elapsed", seed: true, seedHash: "h", window: testWindow,
			hash: "h", at: start.Add(testWindow), wantSkip: false,
			why: "the boundary is inclusive: at exactly the window, the row is due",
		},
		{
			name: "a bump inside the window", seed: true, seedHash: "h", window: testWindow,
			hash: "h", at: start.Add(20 * time.Second), wantSkip: true,
			wantFlush: 40 * time.Second,
			why: "the only suppressible case, and the remaining window is what schedules " +
				"the flush that records the final count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &coalescer{}
			if tt.seed {
				c.remember(key, tt.seedHash, start, testWindow)
			}

			flushAfter, skip := c.decide(key, tt.hash, tt.window, tt.at)
			if skip != tt.wantSkip {
				t.Fatalf("decide skip = %v, want %v — %s", skip, tt.wantSkip, tt.why)
			}
			if skip && flushAfter != tt.wantFlush {
				t.Errorf("decide scheduled the flush %v out, want %v: the key must come back "+
					"exactly when the window expires, since that is what records the final count",
					flushAfter, tt.wantFlush)
			}
			if !skip && flushAfter != 0 {
				t.Errorf("decide scheduled a flush (%v) for a write it did not suppress", flushAfter)
			}
		})
	}
}

// TestCoalescerRemembersNothingWhenDisabled keeps the map of a sink that will
// never skip empty. It is not only tidiness: a sink set to 0s is one an operator
// chose to have record everything, and holding per-Event state for it would be a
// cost with no possible benefit, on the highest-churn kind in a cluster.
func TestCoalescerRemembersNothingWhenDisabled(t *testing.T) {
	c := &coalescer{}
	c.remember("|Event|default/a", "h", time.Now(), 0)
	if got := c.len(); got != 0 {
		t.Errorf("a disabled coalescer held %d entries, want 0", got)
	}
}

// TestCoalescerForgetsAndEvicts covers the three ways state leaves: one Event
// expiring, a whole scope stopping, and (implicitly) a sink being deleted, which
// drops the sinkState that owns the coalescer entirely.
//
// The prefix case is the one with a correctness consequence rather than only a
// memory one: an entry surviving a scope stop would be compared against the first
// row of the *next* epoch and could suppress it.
func TestCoalescerForgetsAndEvicts(t *testing.T) {
	now := time.Now()
	c := &coalescer{}
	c.remember("|Event|default/a", "h", now, testWindow)
	c.remember("|Event|default/b", "h", now, testWindow)
	c.remember("|Event|other/c", "h", now, testWindow)
	c.remember("|Pod|default/p", "h", now, testWindow)

	c.forget("|Event|default/a")
	if remembers(c, "|Event|default/a") {
		t.Error("forget left the entry behind; an expired Event's state must go or it leaks")
	}

	if removed := c.deletePrefix("|Event|default/"); removed != 1 {
		t.Errorf("deletePrefix removed %d entries, want 1 (b only — a was already forgotten)", removed)
	}
	if !remembers(c, "|Event|other/c") {
		t.Error("deletePrefix took an entry from another namespace; the trailing slash in a " +
			"scope prefix is what stops namespace foo matching foobar")
	}
	if !remembers(c, "|Pod|default/p") {
		t.Error("deletePrefix took an entry from another kind")
	}
}

// remembers reports whether the coalescer still holds state for one identity. It
// exists so no spec reaches into the map without the lock.
func remembers(c *coalescer, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, found := c.entries[key]
	return found
}

// TestCoalescerSweepsExpiredEntries is the boundedness proof: a namespace churning
// through Events cannot grow the coalescer without limit.
//
// It works because an entry older than the window can never produce a skip, so
// dropping it is indistinguishable from keeping it — which is what lets the sweep
// be opportunistic rather than exact, and why there is no cap or eviction policy
// to explain. The spec asserts both halves: the churn is bounded, and the Events
// that are still live survive it.
func TestCoalescerSweepsExpiredEntries(t *testing.T) {
	start := time.Now()
	c := &coalescer{}

	// A live Event, re-recorded throughout so it is never expired.
	const live = "|Event|default/live"

	// Ten windows' worth of churn: a thousand Events that fire once and are never
	// seen again, at a hundred per window.
	for i := range 1000 {
		at := start.Add(time.Duration(i) * testWindow / 100)
		c.remember(fmt.Sprintf("|Event|default/churn-%d", i), "h", at, testWindow)
		c.remember(live, "h", at, testWindow)
	}

	if got := c.len(); got > 4*coalesceSweepFloor {
		t.Errorf("the coalescer held %d entries after 1000 Events across ten windows.\n"+
			"Expired entries must be swept: they cannot produce a skip, so keeping them is "+
			"pure growth on the highest-churn kind in a cluster", got)
	}
	if !remembers(c, live) {
		t.Error("the sweep took an entry that was re-recorded inside the window; only entries " +
			"too old to suppress anything may go")
	}
}

//
// End to end through Process
//

// TestEventBurstWritesOneRowPerWindow is Phase 20's headline property: a
// crash-looping object writes rows proportional to elapsed time, not to `count`.
//
// It runs against both Event APIs deliberately. The events.k8s.io case is not
// redundant coverage — that API carries `deprecatedCount`, which bumps alongside
// `count`, and a projection that did not exclude it would pass the core case and
// leave this one writing every bump with nothing to say so.
func TestEventBurstWritesOneRowPerWindow(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			clock, requeues := h.coalescing(testWindow)
			key := eventKey(gvk.group, "crash")
			h.warm(key)

			// The Event appears, then bumps once a second for most of a window.
			for i := range 40 {
				h.lister.set(key, bump(gvk.group, "crash",
					fmt.Sprintf("%d", 100+i), int64(1+i)))
				if err := h.pipeline.Process(h.ctx, key); err != nil {
					t.Fatalf("bump %d: %v", i, err)
				}
				clock.advance(time.Second)
			}

			records := h.writer.recorded()
			if len(records) != 1 {
				t.Fatalf("40 bumps inside one %v window wrote %d rows, want 1.\n"+
					"counts: %v", testWindow, len(records), counts(t, records))
			}
			if got := counts(t, records)[0]; got != 1 {
				t.Errorf("the row written carries count=%d, want 1: the first sighting is "+
					"always written, and it is the bumps after it that coalesce", got)
			}
			if got := testutil.ToFloat64(h.pipeline.metrics.eventCoalesceSkips); got != 39 {
				t.Errorf("event_coalesce_skips_total = %v, want 39", got)
			}

			// Every suppressed bump scheduled its own flush, and each one is due at
			// the same instant — the end of the window the recorded row opened — so
			// the delay shrinks as the burst runs. That is what makes them collapse
			// onto one wake-up in the delaying queue rather than pushing the flush
			// outwards forever.
			calls := requeues.all()
			if len(calls) != 39 {
				t.Fatalf("39 suppressed bumps scheduled %d flushes, want one each", len(calls))
			}
			for i, call := range calls {
				want := testWindow - time.Duration(i+1)*time.Second
				if call.after != want {
					t.Errorf("flush %d scheduled %v out, want %v: every flush is due when the "+
						"*recorded* row's window expires, not a window after the bump",
						i, call.after, want)
				}
				if call.key != key {
					t.Errorf("flush %d re-delivers %v, want %v", i, call.key, key)
				}
			}
		})
	}
}

// TestEventCoalesceFlushRecordsTheTrueCount is the criterion the backlog names as
// the one most likely to be got subtly wrong: a window that ended the recording at
// the second-to-last bump would leave the archive claiming a lower count than the
// Event reached.
//
// The mechanism is stated in coalescer.decide and exercised here in full: the last
// bump of a burst schedules a flush, the flush re-delivers the key when the window
// expires, and Process then reads the Event's *current* state from the watch cache
// — so the row written carries the count the Event actually reached, not the one it
// held when the last row was written.
//
// The boundary is asserted on both sides. One tick before the window the bump is
// still suppressed; at exactly the window it is written. An off-by-one in either
// direction changes which of these two fails.
func TestEventCoalesceFlushRecordsTheTrueCount(t *testing.T) {
	h := newHarness(t)
	clock, requeues := h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	// First sighting: written.
	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}

	// A burst that ends well inside the window, at count=47.
	for i := range 46 {
		clock.advance(time.Second)
		h.lister.set(key, bump("", "crash", fmt.Sprintf("%d", 101+i), int64(2+i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("bump to %d: %v", 2+i, err)
		}
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Fatalf("the burst wrote %d rows, want 1", got)
	}
	if requeues.len() == 0 {
		t.Fatal("the burst scheduled no flush at all, so the final count would be lost the " +
			"moment the Event stopped changing")
	}

	// One tick short of the window, the last flush fires early (nothing else has
	// happened, so the key is simply re-delivered): still suppressed.
	clock.advance(testWindow - 46*time.Second - time.Nanosecond)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("flush a tick early: %v", err)
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Fatalf("a re-delivery one tick before the window wrote a row (%d total); the "+
			"boundary must be the window, not almost-the-window", got)
	}

	// At the window: the flush writes, and it writes the count the Event reached
	// rather than the one the previous row held.
	clock.advance(time.Nanosecond)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("flush at the window: %v", err)
	}
	got := counts(t, h.writer.recorded())
	want := []int64{1, 47}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("the archive holds counts %v, want %v.\n"+
			"The flush must record the Event as it *then* stands: an archive claiming a "+
			"lower count than the Event reached is the failure this criterion exists for", got, want)
	}
}

// TestEventCoalesceWritesImmediatelyOnAnyOtherChange is the other half of the
// contract. A changed message is a different fact and is written at once, however
// much of the window is left — and the row after it restarts the window rather
// than inheriting the suppressed one's.
func TestEventCoalesceWritesImmediatelyOnAnyOtherChange(t *testing.T) {
	h := newHarness(t)
	clock, _ := h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}

	// A pure bump a second later: suppressed.
	clock.advance(time.Second)
	h.lister.set(key, bump("", "crash", "101", 2))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("bump: %v", err)
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Fatalf("the bump was not coalesced (%d rows); the rest of this spec proves nothing", got)
	}

	// The message changes a second after that, still deep inside the window.
	clock.advance(time.Second)
	changed := bump("", "crash", "102", 3)
	changed.Object["message"] = "Back-off pulling image kuberecord:latest"
	h.lister.set(key, changed)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("changed message: %v", err)
	}

	records := h.writer.recorded()
	if len(records) != 2 {
		t.Fatalf("a changed message inside the window produced %d rows, want 2. A changed "+
			"message, reason, type or subject is a different fact whatever the window says",
			len(records))
	}
	if got := counts(t, records)[1]; got != 3 {
		t.Errorf("the row written for the changed message carries count=%d, want 3", got)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.eventCoalesceSkips); got != 1 {
		t.Errorf("event_coalesce_skips_total = %v, want 1 (the bump only)", got)
	}
}

// TestEventCoalesceWindowZeroWritesEveryBump keeps the opt-out working. An
// operator who wants every count bump in the archive must be able to say so, and
// `0s` is how they say it — so this is a spec about a value staying expressible,
// not about a default.
func TestEventCoalesceWindowZeroWritesEveryBump(t *testing.T) {
	h := newHarness(t)
	clock, requeues := h.coalescing(0)
	key := eventKey("", "crash")
	h.warm(key)

	for i := range 5 {
		h.lister.set(key, bump("", "crash", fmt.Sprintf("%d", 100+i), int64(1+i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("bump %d: %v", i, err)
		}
		clock.advance(time.Second)
	}

	if got := counts(t, h.writer.recorded()); len(got) != 5 {
		t.Errorf("coalesceWindow=0 wrote %d rows for 5 bumps, want 5 (counts %v)", len(got), got)
	}
	if requeues.len() != 0 {
		t.Errorf("a disabled window scheduled %d flushes; nothing was suppressed, so nothing "+
			"needs to come back", requeues.len())
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.eventCoalesceSkips); got != 0 {
		t.Errorf("event_coalesce_skips_total = %v, want 0", got)
	}
	if entries := h.pipeline.sinks.get(testSink).coalesce.len(); entries != 0 {
		t.Errorf("a disabled sink accumulated %d coalescer entries; a sink that will never "+
			"skip must hold no per-Event state at all", entries)
	}
}

// TestEventCoalesceSkipTakesNoVersion is the Invariant 3 assertion. Coalescing
// decides whether to *enqueue*; the commit contract, version gating and hashCache
// primitives are untouched by it.
//
// A skipped write must therefore leave the cache exactly as it found it: the same
// hash, the same UID, nothing marked pending — and, the load-bearing part, the
// same Version. Reserve is what assigns a version, so an unchanged one is direct
// proof that the skip never reached it.
func TestEventCoalesceSkipTakesNoVersion(t *testing.T) {
	h := newHarness(t)
	clock, _ := h.coalescing(testWindow)
	key := eventKey("", "crash")
	objectKey := key.cacheKey()
	h.warm(key)
	state := h.pipeline.sinks.get(testSink)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}
	before, ok := state.cache.Load(objectKey)
	if !ok {
		t.Fatal("the first sighting left no cache entry")
	}

	clock.advance(time.Second)
	h.lister.set(key, bump("", "crash", "101", 2))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("suppressed bump: %v", err)
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Fatalf("the bump was not suppressed (%d rows); this spec proves nothing", got)
	}

	after, ok := state.cache.Load(objectKey)
	if !ok {
		t.Fatal("the suppressed bump removed the cache entry")
	}
	if !reflect.DeepEqual(after, before) {
		t.Errorf("the suppressed bump changed the cache entry.\n before: %+v\n after:  %+v\n"+
			"A skip must take no version and leave no pending entry: nothing was written, "+
			"so nothing may be recorded as having been (Invariant 3)", before, after)
	}

	// And the next write for this key proceeds from that untouched version, which
	// is what proves the skip left no gap in the sequence the commits are gated on.
	clock.advance(testWindow)
	h.lister.set(key, bump("", "crash", "102", 3))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("the write after the window: %v", err)
	}
	committed, ok := state.cache.Load(objectKey)
	if !ok {
		t.Fatal("the write after the window left no cache entry")
	}
	if committed.Version != before.Version+1 {
		t.Errorf("the write after a suppressed bump committed at version %d, want %d: "+
			"a skip must consume nothing from the version sequence",
			committed.Version, before.Version+1)
	}
}

// TestEventCoalesceDoesNotSuppressARetriedWrite is why remember runs in the commit
// callback rather than at enqueue time.
//
// A write that fails is reverted and re-queued, and the change it carried is still
// unrecorded. An entry written optimistically would make that retry compare equal
// to a row the sink never took — suppressing it, and losing the change outright
// rather than merely deferring it.
func TestEventCoalesceDoesNotSuppressARetriedWrite(t *testing.T) {
	h := newHarness(t)
	clock, _ := h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}

	// A bump whose write fails after the window has passed, so it is not
	// suppressed — it is attempted and lost.
	clock.advance(testWindow)
	h.writer.failNextCommit()
	h.lister.set(key, bump("", "crash", "101", 2))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("failing bump: %v", err)
	}

	// The retry arrives immediately, well inside a fresh window. It must write:
	// the sink has no row carrying count=2.
	h.lister.set(key, bump("", "crash", "101", 2))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("retry: %v", err)
	}

	got := counts(t, h.writer.recorded())
	// The failed attempt is in recorded() too — the fake records what it accepted,
	// and it accepted that job before settling it false — so the retry is the
	// third row and the second count=2.
	if len(got) != 3 || got[2] != 2 {
		t.Errorf("the archive holds counts %v; the retry after a failed write must write "+
			"again rather than coalescing against a row the sink never took", got)
	}
}

// TestEventCoalesceStateEvictedWithTheScope covers the eviction the acceptance
// criteria name, and the one with a correctness consequence rather than only a
// memory one: an entry surviving a scope stop would be compared against the first
// row of the next epoch.
func TestEventCoalesceStateEvictedWithTheScope(t *testing.T) {
	h := newHarness(t)
	_, _ = h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}
	state := h.pipeline.sinks.get(testSink)
	if got := state.coalesce.len(); got != 1 {
		t.Fatalf("the recorded row left %d coalescer entries, want 1", got)
	}

	h.pipeline.EvictScope(testSink, key.Scope())
	if got := state.coalesce.len(); got != 0 {
		t.Errorf("EvictScope left %d coalescer entries behind. A surviving entry would be "+
			"compared against the first row of the next epoch and could suppress it", got)
	}
}

// TestEventCoalesceStateForgottenOnExpiry is the bound that matters in steady
// state. Events are the highest-churn kind in a cluster and each one is gone
// within the hour, so an entry left behind per expired Event is an unbounded leak
// on a scope nobody stopped.
func TestEventCoalesceStateForgottenOnExpiry(t *testing.T) {
	h := newHarness(t)
	_, _ = h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}
	state := h.pipeline.sinks.get(testSink)
	if got := state.coalesce.len(); got != 1 {
		t.Fatalf("the recorded row left %d coalescer entries, want 1", got)
	}

	// The Event's TTL comes round: it leaves the watch cache, which is an expiry
	// and not a deletion (see ephemeralKind), so nothing is written.
	h.lister.remove(key)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("expiry: %v", err)
	}
	if got := state.coalesce.len(); got != 0 {
		t.Errorf("an expired Event left %d coalescer entries behind, want 0", got)
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Errorf("the expiry wrote a row (%d total); an Event's disappearance is its TTL, "+
			"never a deletion", got)
	}
}

// TestEventCoalesceStateDoesNotSurviveARestart states the restart behaviour rather
// than leaving it to be discovered.
//
// In-memory coalescing state does not survive a restart, so the first Event
// observed afterwards writes. That is correct — the operator cannot know what it
// did not record — and it is also what recovers a burst interrupted by a restart:
// the hashCache is warm from the sink's own history, so the first post-restart
// sighting compares against the last *recorded* row and writes the count the Event
// has actually reached.
func TestEventCoalesceStateDoesNotSurviveARestart(t *testing.T) {
	h := newHarness(t)
	clock, _ := h.coalescing(testWindow)
	key := eventKey("", "crash")
	h.warm(key)

	h.lister.set(key, bump("", "crash", "100", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting: %v", err)
	}
	clock.advance(time.Second)
	h.lister.set(key, bump("", "crash", "101", 12))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("suppressed bump: %v", err)
	}
	if got := len(h.writer.recorded()); got != 1 {
		t.Fatalf("the bump was not suppressed (%d rows); this spec proves nothing", got)
	}

	// The operator restarts. The sink's history and the watch cache survive; every
	// in-memory pipeline cache does not.
	h.restart(t)
	restarted, _ := h.coalescing(testWindow)
	restarted.advance(0)
	h.warm(key)
	// Warm-up seeds the hash of the row the sink actually holds, which is the
	// count=1 row — not the count=12 state that was never written.
	seed, err := normalizeObject(bump("", "crash", "100", 1), nil)
	if err != nil {
		t.Fatalf("seeding the warm cache: %v", err)
	}
	h.pipeline.sinks.get(testSink).cache.StoreIfAbsent(key.cacheKey(),
		CacheEntry{Hash: seed.Hash, UID: testUID, APIVersion: "v1"})

	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("first sighting after the restart: %v", err)
	}
	got := counts(t, h.writer.recorded())
	if len(got) != 2 || got[1] != 12 {
		t.Errorf("the archive holds counts %v, want the count=1 row and then count=12.\n"+
			"Coalescing state is per-process, so the first Event after a restart writes — "+
			"and that write is what recovers a count the previous process had suppressed", got)
	}
}

// TestEventCoalescingIgnoresOrdinaryObjects keeps the feature to the kind it is
// about. A Deployment's hash changing means the Deployment changed, and there is
// no `count` to be cumulative about, so a window must not suppress a single row of
// it — nor hold any state for it.
func TestEventCoalescingIgnoresOrdinaryObjects(t *testing.T) {
	h := newHarness(t)
	clock, requeues := h.coalescing(testWindow)
	key := podKey("web")
	h.warm(key)

	for i := range 4 {
		h.lister.set(key, newPod("web", testUID, fmt.Sprintf("%d", 100+i),
			fmt.Sprintf("nginx:1.%d", i)))
		if err := h.pipeline.Process(h.ctx, key); err != nil {
			t.Fatalf("change %d: %v", i, err)
		}
		clock.advance(time.Second)
	}

	if got := len(h.writer.recorded()); got != 4 {
		t.Errorf("four changes to a Pod inside the window wrote %d rows, want 4: coalescing "+
			"is about a count bump, which only a Kubernetes Event has", got)
	}
	if requeues.len() != 0 {
		t.Errorf("a Pod scheduled %d coalescing flushes, want 0", requeues.len())
	}
	if got := h.pipeline.sinks.get(testSink).coalesce.len(); got != 0 {
		t.Errorf("a Pod left %d coalescer entries, want 0", got)
	}
}

// TestEventCoalesceConcurrentBursts is the -race spec: several Events bursting at
// once through the real worker pool, with the real delaying queue rather than the
// recorder.
//
// It asserts a bound rather than an exact count, because with concurrent workers
// and a real clock the number of rows depends on timing — which is the point. What
// must hold regardless is that the coalescer is race-free, that far fewer rows are
// written than bumps delivered, and that every Event ends up recorded at its true
// final count once the flushes have run.
func TestEventCoalesceConcurrentBursts(t *testing.T) {
	const (
		events         = 6
		bumpsPerEvent  = 40
		liveWindow     = 150 * time.Millisecond
		settleDeadline = 5 * time.Second
	)

	h := newHarness(t)
	// A real clock and the real queue: the seams exist for determinism elsewhere,
	// and using them here would defeat the point of this spec.
	h.writer.setCoalesceWindow(liveWindow)
	stop := h.run(t)

	for i := range events {
		key := eventKey("", fmt.Sprintf("crash-%d", i))
		h.warm(key)
	}

	var wg sync.WaitGroup
	for i := range events {
		wg.Go(func() {
			name := fmt.Sprintf("crash-%d", i)
			key := eventKey("", name)
			for n := range bumpsPerEvent {
				h.lister.set(key, bump("", name, fmt.Sprintf("%d", 100+n), int64(1+n)))
				h.pipeline.Add(key)
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Wait()

	// Wait for every Event to have been recorded at its true final count, which is
	// what the scheduled flushes deliver once the bumps stop.
	deadline := time.Now().Add(settleDeadline)
	var final map[string]int64
	for time.Now().Before(deadline) {
		final = finalCounts(t, h.writer.recorded())
		if len(final) == events && allAtLeast(final, bumpsPerEvent) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()

	for i := range events {
		name := fmt.Sprintf("crash-%d", i)
		if got := final[name]; got != bumpsPerEvent {
			t.Errorf("%s settled at count=%d, want %d: once an Event stops changing, the "+
				"flush must record it at the count it actually reached", name, got, bumpsPerEvent)
		}
	}

	written := len(h.writer.recorded())
	if written >= events*bumpsPerEvent {
		t.Errorf("%d rows for %d bumps: coalescing suppressed nothing at all",
			written, events*bumpsPerEvent)
	}
}

// finalCounts reduces a recorded stream to the last count seen per Event name.
func finalCounts(t *testing.T, records []sink.Record) map[string]int64 {
	t.Helper()
	final := make(map[string]int64)
	for _, record := range records {
		if record.Data == "" {
			continue
		}
		final[record.Name] = eventCountIn(t, record.Data)
	}
	return final
}

// allAtLeast reports whether every value reaches want.
func allAtLeast(counts map[string]int64, want int64) bool {
	for _, got := range counts {
		if got < want {
			return false
		}
	}
	return true
}

// TestEventCoalesceFlushSurvivesShutdown is the goleak guard for the one thing
// this task adds to the shutdown path: a scheduled flush that never fires.
//
// It needs no goroutine of its own — the delaying queue's existing waiting loop is
// what holds a pending AddAfter, and that loop stops with the queue — so the spec
// is that arranging a real pending flush and then cancelling leaves nothing
// behind. The flush itself is dropped, which is correct and is exactly the restart
// case TestEventCoalesceStateDoesNotSurviveARestart covers: the next process
// writes the Event at its true count.
func TestEventCoalesceFlushSurvivesShutdown(t *testing.T) {
	snapshot := goleak.IgnoreCurrent()

	h := newHarness(t)
	// A window far longer than the spec runs, so the flush is still pending at
	// shutdown rather than having fired.
	h.writer.setCoalesceWindow(time.Hour)
	stop := h.run(t)

	key := eventKey("", "crash")
	h.warm(key)
	h.lister.set(key, bump("", "crash", "100", 1))
	h.pipeline.Add(key)
	h.writer.awaitRecords(t, 1)

	h.lister.set(key, bump("", "crash", "101", 2))
	h.pipeline.Add(key)
	// Give the suppressed bump time to be processed and to schedule its flush.
	for range 100 {
		if testutil.ToFloat64(h.pipeline.metrics.eventCoalesceSkips) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := testutil.ToFloat64(h.pipeline.metrics.eventCoalesceSkips); got == 0 {
		t.Fatal("no bump was suppressed, so no flush is pending and this spec proves nothing")
	}

	stop()
	goleak.VerifyNone(t, snapshot)
}
