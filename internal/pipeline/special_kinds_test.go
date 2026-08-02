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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Events mode, end to end through Process. Every spec here runs against *both*
// Event GVKs rather than picking one, because "support whichever the rule names"
// is the actual contract: the two APIs are one storage, so an author who happens
// to have written events.k8s.io must get identical rows to one who wrote v1.

// eventGVKs are the two spellings a rule may use. The Kind is the same; only the
// group differs, which is exactly what ephemeralKind matches on.
var eventGVKs = []struct {
	name  string
	group string
}{
	{name: "core v1/Event", group: ""},
	{name: "events.k8s.io/v1/Event", group: "events.k8s.io"},
}

// eventKey builds a work key for an Event in the given group.
func eventKey(group, name string) Key {
	return Key{Sink: testSink, Group: group, Kind: "Event", Namespace: "default", Name: name}
}

// newEvent builds an Event as the watch cache holds one: the involved object it
// describes, the reason, and the count the API server bumps in place every time
// the same thing happens again.
//
// count is the field this whole mode exists for — it is what makes an Event a
// *mutable* ephemeral object rather than a write-once log line, and it is the
// case naive exporters drop.
func newEvent(group, name, uid, resourceVersion string, count int64) *unstructured.Unstructured {
	apiVersion := "v1"
	if group != "" {
		apiVersion = group + "/v1"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       "Event",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             uid,
			"resourceVersion": resourceVersion,
		},
		"involvedObject": map[string]any{
			"kind":      "Pod",
			"namespace": "default",
			"name":      "crasher",
			"uid":       "pod-uid",
		},
		"reason":  "BackOff",
		"message": "Back-off restarting failed container",
		"type":    "Warning",
		"count":   count,
	}}
}

// TestEphemeralKind is the predicate's own table: which (group, kind) pairs enter
// Events mode, and — just as load-bearing — which ones must not.
func TestEphemeralKind(t *testing.T) {
	tests := []struct {
		name  string
		group string
		kind  string
		want  bool
	}{
		{name: "core Event", group: "", kind: "Event", want: true},
		{name: "events.k8s.io Event", group: "events.k8s.io", kind: "Event", want: true},
		// Identity is version-agnostic (Invariant 7), so a future version of the
		// same resource must not be able to reach the durable-object path.
		{name: "a same-named kind in a foreign group is a different resource",
			group: "example.com", kind: "Event", want: false},
		{name: "core Pod", group: "", kind: "Pod", want: false},
		{name: "an events.k8s.io kind that is not Event", group: "events.k8s.io", kind: "EventSeries", want: false},
		{name: "kinds are case-sensitive", group: "", kind: "event", want: false},
		{name: "empty kind", group: "", kind: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ephemeralKind(tc.group, tc.kind); got != tc.want {
				t.Errorf("ephemeralKind(%q, %q) = %v, want %v", tc.group, tc.kind, got, tc.want)
			}
			key := Key{Group: tc.group, Kind: tc.kind}
			if got := key.ephemeral(); got != tc.want {
				t.Errorf("Key.ephemeral() = %v, want %v", got, tc.want)
			}
			scope := ScopeKey{Group: tc.group, Kind: tc.kind}
			if got := scope.ephemeral(); got != tc.want {
				t.Errorf("ScopeKey.ephemeral() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProcessEventCountBumpWritesFullState is the headline case: the API server
// updates an Event in place to bump `count`, and that update must land as a
// Modified row carrying the *whole* Event — no diff to replay — so the row is
// readable on its own.
func TestProcessEventCountBumpWritesFullState(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			key := eventKey(gvk.group, "crasher.17abc")
			h.warm(key)

			h.lister.set(key, newEvent(gvk.group, key.Name, "event-uid", "1", 1))
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(first sighting): %v", err)
			}

			// Three bumps of the same Event, exactly as the API server produces
			// them: same UID, same name, a higher count each time.
			for count := int64(2); count <= 4; count++ {
				h.lister.set(key, newEvent(gvk.group, key.Name, "event-uid",
					fmt.Sprintf("%d", count), count))
				if err := h.pipeline.Process(h.ctx, key); err != nil {
					t.Fatalf("Process(count=%d): %v", count, err)
				}
			}

			records := h.writer.recorded()
			if got, want := h.writer.eventTypes(),
				[]string{"Added", "Modified", "Modified", "Modified"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v (every count bump is an ordinary Modified)", got, want)
			}

			for i, record := range records {
				if record.Diff != "" {
					t.Errorf("row %d carries a diff %q; an Event is never diffed", i, record.Diff)
				}
				if record.Data == "" {
					t.Errorf("row %d carries no data; every Event row is full state", i)
				}
				if record.SHA256 == "" {
					t.Errorf("row %d carries no sha256", i)
				}
				// The count is the point: it has to be readable from the row
				// itself, which is only true because data is populated.
				if want := fmt.Sprintf(`"count":%d`, i+1); !strings.Contains(record.Data, want) {
					t.Errorf("row %d data does not contain %s: %s", i, want, record.Data)
				}
			}

			// The entry keeps the hash and the UID and nothing else. A diff
			// baseline would be compressed on every bump and read by nobody, on
			// the kind that produces more cache entries than any other.
			st := h.pipeline.sinks.get(testSink)
			entry, ok := st.cache.Load(key.cacheKey())
			if !ok {
				t.Fatal("no cache entry for the Event")
			}
			if entry.JSON != nil {
				t.Errorf("the Event's cache entry stores a %d-byte diff baseline; Events are never diffed",
					len(entry.JSON))
			}
			if entry.Hash == "" || entry.UID != "event-uid" {
				t.Errorf("cache entry = %+v, want the hash and uid populated", entry)
			}
		})
	}
}

// TestProcessEventResyncStillDeduplicates guards the other half of the count-bump
// contract: skipping the diff must not skip the *hash*. A resync that re-delivers
// an unchanged Event writes nothing at all.
func TestProcessEventResyncStillDeduplicates(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			key := eventKey(gvk.group, "crasher.17abc")
			h.warm(key)

			event := newEvent(gvk.group, key.Name, "event-uid", "1", 1)
			for range 3 {
				h.lister.set(key, event)
				if err := h.pipeline.Process(h.ctx, key); err != nil {
					t.Fatalf("Process: %v", err)
				}
			}

			if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v (an unchanged resync must deduplicate)", got, want)
			}
			if got := testutil.ToFloat64(h.pipeline.metrics.dedupSkips); got != 2 {
				t.Errorf("dedup_skips_total = %v, want 2", got)
			}
		})
	}
}

// TestProcessEventExpiryWritesNoRow is the TTL case: the watch delivers a Deleted
// for an Event that simply aged out, and kubestream must record *nothing* — not a
// Deleted row, not anything else. The cache entry goes, though: an entry left
// behind per expired Event is an unbounded leak on the highest-churn kind there
// is.
func TestProcessEventExpiryWritesNoRow(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			key := eventKey(gvk.group, "crasher.17abc")
			h.warm(key)

			h.lister.set(key, newEvent(gvk.group, key.Name, "event-uid", "1", 1))
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(first sighting): %v", err)
			}
			if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v", got, want)
			}

			// The TTL comes round: the informer delivers a Deleted and the object
			// leaves the watch cache.
			h.lister.remove(key)
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(expiry): %v", err)
			}
			// And again, because the workqueue guarantees at-least-once delivery:
			// a redelivered expiry must be just as silent as the first.
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(redelivered expiry): %v", err)
			}

			if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v (an Event's expiry is never recorded)", got, want)
			}

			st := h.pipeline.sinks.get(testSink)
			if _, exists := st.cache.Load(key.cacheKey()); exists {
				t.Error("the expired Event's cache entry survived; Events would leak one entry per expiry")
			}
			// The drop is metered rather than silent: on a cluster streaming Events
			// this counter's rate is the Event churn being absorbed.
			if got := testutil.ToFloat64(
				h.pipeline.metrics.dropped.WithLabelValues(DropReasonEphemeralDelete)); got != 2 {
				t.Errorf("dropped_total{reason=ephemeral_delete} = %v, want 2", got)
			}
			if got := testutil.ToFloat64(
				h.pipeline.metrics.dropped.WithLabelValues(DropReasonScopeStopped)); got != 0 {
				t.Errorf("dropped_total{reason=scope_stopped} = %v, want 0 — an expiry is not a stopped scope", got)
			}
		})
	}
}

// TestProcessEventExpiryNeedsNoSink asserts that suppression is unconditional: an
// Event's expiry writes nothing, so it must settle even when the sink has no live
// writer at all. Retrying it would be pointless work on the one drop reason that
// ticks continuously.
func TestProcessEventExpiryNeedsNoSink(t *testing.T) {
	h := newHarness(t)
	key := eventKey("", "crasher.17abc")
	h.warm(key)

	h.lister.set(key, newEvent("", key.Name, "event-uid", "1", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first sighting): %v", err)
	}

	h.lister.remove(key)
	h.router.remove(testSink)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(expiry with no sink) = %v, want nil — nothing is written, so nothing can fail", err)
	}
	if n := h.logs.countOf(errSinkUnavailable); n != 0 {
		t.Errorf("the missing sink was reported %d times; a suppressed expiry never resolves one", n)
	}
}

// TestProcessEventCacheMissIsAddedWhileCold asserts the third difference: an
// Events scope never Snapshot-tags. Snapshot hedges "new, or merely unseen by
// this process?", and for an Event there is nothing to hedge — a miss is a new
// Event.
func TestProcessEventCacheMissIsAddedWhileCold(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			key := eventKey(gvk.group, "crasher.17abc")
			// Deliberately NOT warmed — a Pod here would be tagged Snapshot.

			h.lister.set(key, newEvent(gvk.group, key.Name, "event-uid", "1", 1))
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process: %v", err)
			}

			if got, want := h.writer.eventTypes(), []string{"Added"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v (an Event cache miss is unambiguously new)", got, want)
			}
			// safe_mode describes Snapshot mode, so a scope that can never enter it
			// must never publish 1 for it.
			scope := key.Scope()
			if got := testutil.ToFloat64(h.pipeline.metrics.safeMode.WithLabelValues(
				key.Sink, scope.Group, scope.Kind, scope.Namespace)); got != 0 {
				t.Errorf("safe_mode = %v, want 0 for an Events scope", got)
			}
		})
	}
}

// TestProcessEventNameReuseWritesNoCloseOut covers the one path that could sneak
// a Deleted row past the TTL suppression: a *reincarnation*, where a new Event
// takes over a name the cache still holds under an older UID. The close-out is a
// Deleted row like any other, so for Events it must not be written — the
// successor is simply recorded as Added under its own UID.
func TestProcessEventNameReuseWritesNoCloseOut(t *testing.T) {
	for _, gvk := range eventGVKs {
		t.Run(gvk.name, func(t *testing.T) {
			h := newHarness(t)
			key := eventKey(gvk.group, "crasher.17abc")
			h.warm(key)

			h.lister.set(key, newEvent(gvk.group, key.Name, "uid-old", "1", 1))
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(first Event): %v", err)
			}

			h.lister.set(key, newEvent(gvk.group, key.Name, "uid-new", "9", 1))
			if err := h.pipeline.Process(h.ctx, key); err != nil {
				t.Fatalf("Process(successor): %v", err)
			}

			records := h.writer.recorded()
			if got, want := h.writer.eventTypes(), []string{"Added", "Added"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v (no close-out Deleted row for an Event)", got, want)
			}
			if records[1].UID != "uid-new" {
				t.Errorf("the successor's row must carry its own uid, got %q", records[1].UID)
			}
			if records[1].Data == "" || records[1].Diff != "" {
				t.Errorf("the successor is full state, got data-empty=%v diff=%q",
					records[1].Data == "", records[1].Diff)
			}
		})
	}
}

// TestProcessNonEventKindsAreUnaffected is the guard rail for everything above:
// the special-casing keys on the GVK, so a kind that merely *looks* adjacent must
// still diff, still Snapshot-tag, and still record its deletion.
func TestProcessNonEventKindsAreUnaffected(t *testing.T) {
	h := newHarness(t)
	// Same Kind, a foreign group — a CRD called Event is an ordinary object.
	key := Key{Sink: testSink, Group: "example.com", Kind: "Event", Namespace: "default", Name: "custom"}

	h.lister.set(key, newEvent("example.com", key.Name, "uid-1", "1", 1))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(first sighting): %v", err)
	}
	h.lister.set(key, newEvent("example.com", key.Name, "uid-1", "2", 2))
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(update): %v", err)
	}
	h.lister.remove(key)
	if err := h.pipeline.Process(h.ctx, key); err != nil {
		t.Fatalf("Process(delete): %v", err)
	}

	records := h.writer.recorded()
	if got, want := h.writer.eventTypes(),
		[]string{"Snapshot", "Modified", "Deleted"}; !slices.Equal(got, want) {
		t.Fatalf("event types = %v, want %v (a foreign-group Event is an ordinary object)", got, want)
	}
	if records[1].Diff == "" {
		t.Error("the update must be diffed; only Kubernetes Events skip diffing")
	}
}
