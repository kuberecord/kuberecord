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
	"sync"
	"testing"
)

// newUID is reused across several tests that need a distinct UID from
// "old-uid" to represent a reincarnated object.
const newUID = "new-uid"

func TestHashCacheReserveThenCommit(t *testing.T) {
	var c hashCache

	version := c.Reserve("k", CacheEntry{Hash: "h1"})
	if version != 1 {
		t.Fatalf("expected first Reserve to assign version 1, got %d", version)
	}

	if ok := c.CommitIfCurrent("k", version, CacheEntry{Hash: "h1-confirmed"}); !ok {
		t.Fatalf("CommitIfCurrent should apply when the version still matches")
	}

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "h1-confirmed" {
		t.Fatalf("expected confirmed entry, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheStaleCommitDoesNotClobberNewer reproduces the exact race
// behind confirmed findings #1/#2 of the code review: job A is issued
// first but its commit fires after job B's (a later write for the same
// key). Without version gating, A's commit would blindly overwrite B's
// already-confirmed result. With it, A's stale commit must be a no-op.
func TestHashCacheStaleCommitDoesNotClobberNewer(t *testing.T) {
	var c hashCache

	versionA := c.Reserve("k", CacheEntry{Hash: "hA"})
	versionB := c.Reserve("k", CacheEntry{Hash: "hB"}) // a second write supersedes the first

	if versionB != versionA+1 {
		t.Fatalf("expected monotonically increasing versions, got A=%d B=%d", versionA, versionB)
	}

	// B's commit fires first (its write finished faster).
	if ok := c.CommitIfCurrent("k", versionB, CacheEntry{Hash: "hB-confirmed"}); !ok {
		t.Fatalf("CommitIfCurrent for the current version should apply")
	}

	// A's commit fires later, even though it was issued first — it must be
	// rejected rather than clobbering B's confirmed state.
	if ok := c.CommitIfCurrent("k", versionA, CacheEntry{Hash: "hA-confirmed"}); ok {
		t.Fatalf("stale CommitIfCurrent must not apply")
	}

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "hB-confirmed" {
		t.Fatalf("expected B's confirmed entry to survive A's stale commit, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheStaleDeleteDoesNotClobberNewer reproduces confirmed finding
// #2: an object is deleted, then quickly recreated (new UID) before the
// delete job's commit fires. The stale DeleteIfCurrent must not remove the
// newer incarnation's entry.
func TestHashCacheStaleDeleteDoesNotClobberNewer(t *testing.T) {
	var c hashCache

	deleteVersion := c.Reserve("k", CacheEntry{UID: "old-uid"})
	// Object recreated under a new UID before the delete's commit fires.
	c.Reserve("k", CacheEntry{UID: newUID})

	if ok := c.DeleteIfCurrent("k", deleteVersion); ok {
		t.Fatalf("stale DeleteIfCurrent must not apply once a newer entry exists")
	}

	entry, exists := c.Load("k")
	if !exists || entry.UID != newUID {
		t.Fatalf("expected the newer incarnation's entry to survive, got %+v (exists=%v)", entry, exists)
	}
}

func TestHashCacheStoreIfAbsentDoesNotClobberLiveEntry(t *testing.T) {
	var c hashCache

	version := c.Reserve("k", CacheEntry{Hash: "live"})
	c.StoreIfAbsent("k", CacheEntry{Hash: "historical-baseline"})

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "live" || entry.Version != version {
		t.Fatalf("StoreIfAbsent must not overwrite an existing entry, got %+v (exists=%v)", entry, exists)
	}

	c.StoreIfAbsent("other-key", CacheEntry{Hash: "seeded"})
	entry, exists = c.Load("other-key")
	if !exists || entry.Hash != "seeded" {
		t.Fatalf("StoreIfAbsent should seed a genuinely absent key, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheReserveDeleteClaimsOnce reproduces the redelivery scenario
// behind the delete-path duplicate-write bug: two Reconciles both notice the
// same object is gone before the first one's write is confirmed. Without a
// claim, both would independently enqueue a "Deleted" row. With it, the
// second ReserveDelete call must see the claim already in place and refuse.
func TestHashCacheReserveDeleteClaimsOnce(t *testing.T) {
	var c hashCache
	c.Reserve("k", CacheEntry{UID: "uid-1"})

	entry, version, outcome := c.ReserveDelete("k", "")
	if outcome != deleteClaimed {
		t.Fatalf("expected the first ReserveDelete to claim the key, got %s", outcome)
	}
	if entry.UID != "uid-1" {
		t.Fatalf("expected the claimed entry to carry the pre-claim UID, got %+v", entry)
	}

	if _, _, again := c.ReserveDelete("k", ""); again == deleteClaimed {
		t.Fatalf("a second ReserveDelete must not claim a key that's already pending delete")
	}

	if ok := c.DeleteIfCurrent("k", version); !ok {
		t.Fatalf("DeleteIfCurrent should apply using the version ReserveDelete returned")
	}
	if _, exists := c.Load("k"); exists {
		t.Fatalf("expected the entry to be gone after the claimed delete committed")
	}
}

// TestHashCacheReserveDeleteNothingToDelete covers the ordinary case: the
// key was never known (or was already removed), so there's nothing to claim.
func TestHashCacheReserveDeleteNothingToDelete(t *testing.T) {
	var c hashCache
	if _, _, outcome := c.ReserveDelete("missing", ""); outcome == deleteClaimed {
		t.Fatalf("ReserveDelete must not claim a key with no entry")
	}
}

// TestHashCacheUnclaimDeleteAllowsRetry ensures a failed delete write
// releases its claim so a subsequent attempt (e.g. via requeue) can succeed.
func TestHashCacheUnclaimDeleteAllowsRetry(t *testing.T) {
	var c hashCache
	c.Reserve("k", CacheEntry{UID: "uid-1"})

	_, version, outcome := c.ReserveDelete("k", "")
	if outcome != deleteClaimed {
		t.Fatalf("expected the claim to succeed, got %s", outcome)
	}

	c.UnclaimDelete("k", version)

	entry, exists := c.Load("k")
	if !exists || entry.PendingDelete {
		t.Fatalf("expected the claim to be released and the entry to remain, got %+v (exists=%v)", entry, exists)
	}

	if _, _, again := c.ReserveDelete("k", ""); again != deleteClaimed {
		t.Fatalf("expected a fresh ReserveDelete to succeed once the prior claim was released, got %s", again)
	}
}

// TestHashCacheReserveDeleteRefusesUIDMismatch reproduces the confirmed
// GC-vs-reincarnation finding: the startup GC pass believes an object with a
// stale, point-in-time-snapshotted UID is gone, but a live Reconcile has
// since reincarnated it (deleted and recreated under a new UID) and already
// updated the cache via Reserve. Without the expectedUID check, ReserveDelete
// would claim and let the caller delete this live, correct entry by name
// alone. With it, a mismatched expectedUID must refuse the claim.
func TestHashCacheReserveDeleteRefusesUIDMismatch(t *testing.T) {
	var c hashCache
	c.Reserve("k", CacheEntry{UID: "old-uid"})

	// A live reincarnation happens before the GC pass gets to this key.
	c.Reserve("k", CacheEntry{UID: newUID})

	if _, _, outcome := c.ReserveDelete("k", "old-uid"); outcome == deleteClaimed {
		t.Fatalf("ReserveDelete must refuse a claim when expectedUID no longer matches the live entry")
	}

	entry, exists := c.Load("k")
	if !exists || entry.UID != newUID || entry.PendingDelete {
		t.Fatalf("the live entry must be untouched after a refused UID-mismatched claim, got %+v (exists=%v)", entry, exists)
	}

	// The GC pass's own UID does still match when no reincarnation occurred.
	if _, _, outcome := c.ReserveDelete("k", newUID); outcome != deleteClaimed {
		t.Fatalf("ReserveDelete should claim when expectedUID matches the current entry, got %s", outcome)
	}
}

// TestHashCacheReserveDeleteOutcomes pins the refusal *reason* each branch
// reports, not just the fact of a refusal. The three reasons are materially
// different to the per-scope GC pass — only deleteClaimUIDMismatch means "the
// successor owns this key, so nobody is left to record the old incarnation's
// death," while deleteClaimInFlight means the exact opposite (that row is already
// on its way) — and the reason must come from inside the cache's own lock,
// because reconstructing it afterwards races the very transitions it tries to
// distinguish.
func TestHashCacheReserveDeleteOutcomes(t *testing.T) {
	const liveUID = "uid-live"

	cases := []struct {
		name string
		// seed puts the cache into the state under test.
		seed        func(t *testing.T, c *hashCache)
		expectedUID string
		wantOutcome deleteClaimOutcome
		// wantVersion is the version the call must return; 0 for every refusal,
		// since a refusal has nothing to settle.
		wantVersion uint64
		wantUID     string
	}{
		{
			name:        "no entry at all",
			seed:        func(*testing.T, *hashCache) {},
			expectedUID: liveUID,
			wantOutcome: deleteClaimAbsent,
		},
		{
			name: "another claim already owns this deletion",
			seed: func(t *testing.T, c *hashCache) {
				c.Reserve("k", CacheEntry{UID: liveUID})
				if _, _, outcome := c.ReserveDelete("k", liveUID); outcome != deleteClaimed {
					t.Fatalf("seeding the in-flight claim: got %s, want claimed", outcome)
				}
			},
			expectedUID: liveUID,
			wantOutcome: deleteClaimInFlight,
		},
		{
			name: "the key now holds a different incarnation",
			seed: func(_ *testing.T, c *hashCache) {
				c.Reserve("k", CacheEntry{UID: newUID})
			},
			expectedUID: oldUID,
			wantOutcome: deleteClaimUIDMismatch,
		},
		{
			name: "present, unclaimed, expectedUID matches",
			seed: func(_ *testing.T, c *hashCache) {
				c.Reserve("k", CacheEntry{UID: liveUID}) // version 1
			},
			expectedUID: liveUID,
			wantOutcome: deleteClaimed,
			wantVersion: 2, // exactly one bump, as Reserve does
			wantUID:     liveUID,
		},
		{
			// The live delete path: it has no independent belief about which UID
			// it expects, so the mismatch check is skipped entirely and a UID
			// difference cannot produce deleteClaimUIDMismatch. This path's
			// behaviour is unchanged by reporting outcomes.
			name: "empty expectedUID ignores a UID difference",
			seed: func(_ *testing.T, c *hashCache) {
				c.Reserve("k", CacheEntry{UID: oldUID})
				c.Reserve("k", CacheEntry{UID: newUID}) // version 2
			},
			expectedUID: "",
			wantOutcome: deleteClaimed,
			wantVersion: 3,
			wantUID:     newUID,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c hashCache
			tc.seed(t, &c)

			entry, version, outcome := c.ReserveDelete("k", tc.expectedUID)
			if outcome != tc.wantOutcome {
				t.Errorf("outcome = %s, want %s", outcome, tc.wantOutcome)
			}
			if version != tc.wantVersion {
				t.Errorf("version = %d, want %d", version, tc.wantVersion)
			}
			if entry.UID != tc.wantUID {
				t.Errorf("returned entry uid = %q, want %q", entry.UID, tc.wantUID)
			}

			// A refusal must leave the cache exactly as it found it; a claim must
			// mark the entry pending at the version it returned.
			cur, present := c.Load("k")
			switch tc.wantOutcome {
			case deleteClaimed:
				if !present || !cur.PendingDelete || cur.Version != tc.wantVersion {
					t.Errorf("after a claim the entry = %+v (present %v), want pending at version %d",
						cur, present, tc.wantVersion)
				}
			case deleteClaimAbsent:
				if present {
					t.Errorf("a refused claim created an entry: %+v", cur)
				}
			case deleteClaimInFlight:
				if !present || !cur.PendingDelete {
					t.Errorf("the in-flight claim was disturbed by the refusal: %+v (present %v)", cur, present)
				}
			case deleteClaimUIDMismatch:
				if !present || cur.UID != newUID || cur.PendingDelete {
					t.Errorf("the live successor's entry was disturbed by the refusal: %+v (present %v)", cur, present)
				}
			}
		})
	}
}

// TestDeleteClaimOutcomeString covers the loggable rendering: a refusal names its
// own reason where it is acted on, so an operator reading the log can tell a
// protected live successor from a deletion that is already in flight.
func TestDeleteClaimOutcomeString(t *testing.T) {
	cases := map[deleteClaimOutcome]string{
		deleteClaimed:          "claimed",
		deleteClaimAbsent:      "absent",
		deleteClaimInFlight:    "in-flight",
		deleteClaimUIDMismatch: "uid-mismatch",
		deleteClaimOutcome(42): "deleteClaimOutcome(42)",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Errorf("deleteClaimOutcome(%d).String() = %q, want %q", int(outcome), got, want)
		}
	}
}

// TestHashCacheUnclaimDeleteStaleNoop reproduces the reincarnation race: a
// delete is claimed, then the object comes back under a new UID (Reserve)
// before the original delete's write settles. The stale UnclaimDelete must
// not touch the newer live entry.
func TestHashCacheUnclaimDeleteStaleNoop(t *testing.T) {
	var c hashCache
	c.Reserve("k", CacheEntry{UID: "old-uid"})
	_, deleteVersion, outcome := c.ReserveDelete("k", "")
	if outcome != deleteClaimed {
		t.Fatalf("expected the claim to succeed, got %s", outcome)
	}

	c.Reserve("k", CacheEntry{UID: newUID}) // reincarnation supersedes the claim

	c.UnclaimDelete("k", deleteVersion)

	entry, exists := c.Load("k")
	if !exists || entry.UID != newUID || entry.PendingDelete {
		t.Fatalf("stale UnclaimDelete must not disturb the newer live entry, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheConcurrentReserveCommit exercises Reserve/CommitIfCurrent
// under concurrent access (run with -race) to catch any lock-ordering bug
// in hashCache itself, independent of Reconcile's usage of it.
func TestHashCacheConcurrentReserveCommit(t *testing.T) {
	var c hashCache
	const n = 200

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			version := c.Reserve("k", CacheEntry{Hash: "h"})
			c.CommitIfCurrent("k", version, CacheEntry{Hash: "h-confirmed"})
		})
	}
	wg.Wait()

	if _, exists := c.Load("k"); !exists {
		t.Fatalf("expected an entry to exist after concurrent Reserve/CommitIfCurrent calls")
	}
}
