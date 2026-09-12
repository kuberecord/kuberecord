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

// This file bounds the Event volume amplifier.
//
// The amplifier is a consequence of the design special_kinds.go records: an
// Event is written as full state rather than as a diff, because the API server
// *updates* an Event in place to bump `count` and the interesting row is the
// Event as it stood at that moment. The content therefore genuinely changes on
// every bump, hash dedup cannot suppress it, and each bump costs one complete
// Event JSON in resource_states. A pod crash-looping for twenty minutes emits
// `BackOff` with `count` climbing, and the archive takes every increment.
//
// No Event *filter* touches this (D50). `types: [Warning]` keeps exactly the
// Events that bump and `subjectKinds: [Pod]` keeps exactly the subjects that
// produce them, so filtering narrows which streams are kept while leaving how
// deep each one goes untouched. The amplifier peaks when a cluster is unhealthy,
// which is when the operator is writing most and can least afford it.
//
// Coalescing attacks the depth. If the only thing that changed since the last
// recorded row for an Event is its `count` and its timestamps, and less than the
// sink's window has elapsed, the write is skipped. A hundred bumps become a
// handful of rows that still say what fired, when it started, and how many times
// — `count` is cumulative, so the rows that survive carry every occurrence the
// suppressed ones would have restated.
//
// Three properties hold the design together, and each is load-bearing:
//
//   - **The comparison is by exclusion.** Everything the Event carries is
//     compared except the fields a bump rewrites, so a field a future Kubernetes
//     version adds lands in the comparison and makes the write happen. An
//     allow-list would silently start dropping a new signal the day it appeared.
//   - **Coalescing decides whether to enqueue, and nothing else** (Invariant 3).
//     A skipped write takes no version, leaves no pending hashCache entry, and
//     fires no commit — there is no job for a commit to belong to.
//   - **The last bump of a burst is never lost.** A skip schedules the key to be
//     reconsidered when the window expires, and that reconsideration reads the
//     Event's *current* state from the watch cache. See coalescer.decide.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// CoalescePolicy is the optional half of a sink.Writer that declares how long
// this backend suppresses an Event whose only change is a `count` bump.
//
// It is declared here rather than in internal/sink for the reason CheckpointPolicy
// is (see process.go): the pipeline is the consumer, and internal/sink/clickhouse
// and internal/sink/s3 must stay free of any import of this package. A Writer that
// does not implement it gets no coalescing at all, which is the pre-Phase-20
// behaviour — suppressing a row is a declared per-sink policy, never a default the
// pipeline invents on a backend's behalf.
type CoalescePolicy interface {
	// CoalesceWindow returns how long after a recorded Event row a pure `count`
	// bump for the same Event is suppressed. Zero (or negative) disables
	// coalescing entirely for that sink, and that must stay expressible: an
	// operator who wants every bump recorded can say so.
	CoalesceWindow() time.Duration
}

// coalesceWindowFor resolves the key's sink's Event coalescing window. A backend
// that declares no policy reports 0, i.e. disabled (see CoalescePolicy).
func coalesceWindowFor(writer sink.Writer) time.Duration {
	policy, ok := writer.(CoalescePolicy)
	if !ok {
		return 0
	}
	return policy.CoalesceWindow()
}

// eventBumpFields are the fields the API server rewrites when it records that an
// Event happened again, and the *only* fields whose change may be suppressed.
// Every one of them is a restatement of the same fact rather than a new one.
//
// They are named individually, and the set is used by *exclusion* — see
// eventCoalesceHash — so anything not listed here is compared, including a field
// a future Kubernetes version adds. That direction is deliberate: an unrecognised
// field defaulting to "write" costs rows, while defaulting to "skip" would drop a
// signal nobody knew to look for.
//
// Both Event APIs are covered because both are backed by the same storage and a
// rule may name either (see ephemeralKind). `deprecatedCount` and
// `deprecatedLastTimestamp` are not extra fields to be careful about: they are
// `count` and `lastTimestamp` under the names events.k8s.io/v1 gives them, and
// leaving them in the comparison would make this window a silent no-op for every
// rule that names the modern group — a knob that is on, configured, and does
// nothing (D31).
//
// metadata.resourceVersion is absent because it never reaches here:
// stripVolatileFields removes it before the object is hashed at all, since it
// bumps on every write and would otherwise defeat dedup itself. That is asserted
// rather than assumed — see TestEventCoalesceHashSeesNoResourceVersion.
var eventBumpFields = map[string]struct{}{
	// count is the bump itself: how many times this Event has fired. It is
	// cumulative, so the next row that *is* written carries every occurrence the
	// suppressed rows would have restated.
	"count": {},
	// lastTimestamp is when it last fired, rewritten by the same update.
	"lastTimestamp": {},
	// eventTime is the modern API's single instant, rewritten in place for a
	// series.
	"eventTime": {},
	// series carries {count, lastObservedTime} for an aggregated Event — the
	// whole subtree is the bump, and it is excluded as a subtree so a field added
	// inside it cannot leak in as a difference.
	"series": {},
	// deprecatedCount and deprecatedLastTimestamp are count and lastTimestamp as
	// events.k8s.io/v1 renders them. See the type comment.
	"deprecatedCount":         {},
	"deprecatedLastTimestamp": {},
}

// eventCoalesceHash digests everything a normalized Event carries *except* the
// fields a `count` bump rewrites, so two states of one Event hash identically
// exactly when the only difference between them is that it happened again.
//
// normalized is the map normalizeObject marshalled and hashed — stripped of the
// volatile fields and already redacted — so the projection is a function of the
// same content the row would have carried, and a redacted difference cannot
// resurface here as a reason to write.
//
// Every excluded field sits at the object's top level in both Event APIs, so the
// projection is one shallow map copy: the nested values are shared with the
// caller's map by reference and only ever read by json.Marshal, exactly as
// stripVolatileFields shares them. The result must not be mutated or retained.
//
// json.Marshal sorts map keys, so the digest does not depend on Go's map
// iteration order.
func eventCoalesceHash(normalized map[string]any) (string, error) {
	projection := make(map[string]any, len(normalized))
	for field, value := range normalized {
		if _, bumped := eventBumpFields[field]; bumped {
			continue
		}
		projection[field] = value
	}

	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// coalesceSweepFloor is the entry count below which the coalescer never bothers
// sweeping. A map this small costs less to hold than to walk, and the walk would
// otherwise run on a cluster whose Event stream is a trickle.
const coalesceSweepFloor = 128

// coalesceEntry is what the coalescer remembers about one Event identity: the
// bump-free digest of the row that was last *recorded* for it, and when that row
// was stamped.
//
// "Recorded" is literal, and it is why remember is called from the commit
// callback rather than at enqueue time. A write that fails is reverted and
// re-queued (see processUpsert), and an entry written optimistically would make
// that retry compare equal to a row the sink never took — suppressing it, and
// losing the change outright.
type coalesceEntry struct {
	hash string
	at   time.Time
}

// coalescer holds the per-Event-identity state that decides whether a `count`
// bump is worth a row. One lives on each sinkState, so the state is per-(sink,
// Event identity) by construction: a bump recorded on one sink says nothing about
// whether another sink has it, exactly as the hashCache is per-sink.
//
// Its zero value is usable, and an empty map is the correct starting state after
// a restart: nothing is known to have been recorded, so the first Event observed
// writes. That is not a gap to be closed — the operator cannot know what it did
// not record, and the write is what recovers the true `count` after a restart
// that interrupted a burst.
//
// It is bounded without a cap or an eviction policy, because an entry older than
// the window can never produce a skip: decide would find it expired and write
// anyway. Dropping such an entry is therefore behaviour-neutral, which is what
// lets the sweep below be opportunistic rather than exact, and what bounds the
// map to the Events actually recorded within the last window. Entries also go
// when the Event's TTL expires (forget, from forgetEphemeral), when the watch
// scope stops (deletePrefix, from EvictScope) and when the sink is deleted (the
// whole sinkState).
type coalescer struct {
	mu      sync.Mutex
	entries map[string]coalesceEntry

	// sweepAt is the entry count that triggers the next sweep, doubled after each
	// one so the O(n) walk is amortized to O(1) per insertion rather than run on
	// every recorded Event.
	sweepAt int
}

// decide reports whether this state of an Event may be suppressed, and if so how
// long from now the key must be reconsidered.
//
// A skip needs all three of: coalescing enabled for the sink, a previously
// recorded row for this identity, and that row differing only in the bump fields
// (equal hashes) less than window ago. Any other outcome writes — a first
// sighting, a changed message or reason or subject, a new incarnation under the
// same name (the UID is inside the hash), or a window that has already elapsed.
//
// flushAfter is the remaining window, and it is the whole answer to "the last
// bump in a burst must not be lost". The caller re-delivers the key at that
// delay; Process then re-reads the Event from the watch cache at its *current*
// `count` and writes it, because by then the entry has expired. So the archive
// trails the live `count` by at most one window while a burst runs, and settles
// on the true one within one window of the burst ending. Repeated skips for one
// key collapse onto the earliest deadline rather than pushing it outwards — that
// is the client-go delaying queue's own contract (see Pipeline.requeueAfter), and
// it is what stops a continuously bumping Event from deferring its flush forever.
func (c *coalescer) decide(key, hash string, window time.Duration, now time.Time) (flushAfter time.Duration, skip bool) {
	if window <= 0 {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	last, recorded := c.entries[key]
	if !recorded || last.hash != hash {
		return 0, false
	}
	elapsed := now.Sub(last.at)
	if elapsed >= window {
		return 0, false
	}
	return window - elapsed, true
}

// remember records that a row carrying hash was durably written for key at
// stampedAt, making it the row every later bump is compared against. It is called
// from a job's commit callback on success only; see coalesceEntry.
//
// window is passed in rather than held because it belongs to the sink's live
// Writer, which is resolved per work item so a recycled sink's new configuration
// takes effect without the pipeline holding a stale copy. A non-positive window
// means coalescing is off for this sink, and then nothing is remembered at all:
// the map of a sink that will never skip stays empty rather than accumulating
// entries nothing reads.
func (c *coalescer) remember(key, hash string, stampedAt time.Time, window time.Duration) {
	if window <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]coalesceEntry)
	}
	c.entries[key] = coalesceEntry{hash: hash, at: stampedAt}
	c.sweepLocked(stampedAt, window)
}

// sweepLocked drops every entry too old to suppress anything, when the map has
// grown enough to be worth walking. It must be called with mu held.
//
// It changes no decision: decide already writes for an entry older than window,
// so removing one is indistinguishable from keeping it — which is exactly what
// makes an approximate, amortized sweep sound here where an LRU or a hard cap
// would each need a story about what they evict and why.
func (c *coalescer) sweepLocked(now time.Time, window time.Duration) {
	if len(c.entries) < c.sweepAt {
		return
	}
	for key, entry := range c.entries {
		if now.Sub(entry.at) >= window {
			delete(c.entries, key)
		}
	}
	// Double the survivors so the next walk is at least one full map's worth of
	// insertions away, with a floor so a handful of long-lived Events cannot make
	// the sweep run on every insertion.
	c.sweepAt = max(2*len(c.entries), coalesceSweepFloor)
}

// forget drops one identity's state, so the next row for that name is written.
// It is called when an Event leaves the watch cache — its TTL expired — for the
// same reason forgetEphemeral drops the hashCache entry: Events are the
// highest-churn kind in a cluster, and an entry left behind per expired Event is
// an unbounded leak.
func (c *coalescer) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// deletePrefix drops every identity under one watch scope, returning how many
// went. It mirrors hashCache.DeletePrefix and is called from the same place
// (EvictScope), so a scope's dedup baselines and its coalescing state are
// always dropped together — a surviving coalesce entry for a scope that is
// watched again later could suppress the first row of the new epoch.
func (c *coalescer) deletePrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
			removed++
		}
	}
	if removed > 0 {
		// A scope eviction can take most of the map, and the sweep threshold was
		// sized against the population before it. Re-derive it so the next sweep
		// is not deferred behind a count the map may never reach again.
		c.sweepAt = max(2*len(c.entries), coalesceSweepFloor)
	}
	return removed
}

// len reports how many Event identities are currently held. It exists for tests
// and for the entry-count gauge; nothing on the decision path reads it.
func (c *coalescer) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
