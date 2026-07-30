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
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// hashCache is a mutex-protected map from objectKey to CacheEntry, with a
// version-gated commit primitive so an async write's outcome can be applied
// to the cache only if nothing newer has landed for that key since the write
// was issued. Process calls for a given key are serialized by the workqueue
// (Invariant 2: an item is never handed to two workers at once), but the
// *async* sink write that a Process call kicks off is not — a later-issued
// write can finish before an earlier-issued one. Without version gating,
// whichever commit callback happens to run last would win regardless of which one was actually
// issued last, silently reverting the cache to stale data. A plain
// mutex-protected map (rather than sync.Map) is used because every mutation
// here is a read-decide-write sequence that must be atomic as a whole, which
// sync.Map's individual Load/Store/Delete operations cannot provide.
//
// The same reasoning applies to deletes, which is why they get their own
// claim primitive (ReserveDelete/UnclaimDelete) rather than reusing
// Reserve/CommitIfCurrent: a delete has no new content to reserve a version
// for, but still needs a synchronous, in-cache "claim" the moment it's
// noticed, so a redelivered Process call (or the per-scope GC pass) that
// notices the same disappearance before the first claim's write is confirmed
// does not enqueue a second "Deleted" row for it.
//
// One hashCache exists per sink (see sinkStateRegistry), keyed by the
// version-agnostic identity key across every GVK that sink receives: dedup and
// version state must be independent when the same object streams to two sinks,
// or a write confirmed by one would suppress the other's.
type hashCache struct {
	mu   sync.Mutex
	data map[string]CacheEntry
}

// Len returns the current number of entries. It exists so callers can feed the
// hashcache_entries metric without any metric call ever happening while the
// mutex is held: Len takes and releases the lock itself, and the caller does
// the gauge Set on the returned value afterwards (see recordHashCacheEntries).
func (c *hashCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.data)
}

// Load returns the current entry for key, if any.
func (c *hashCache) Load(key string) (CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	return entry, ok
}

// Reserve atomically assigns the next version for key and stores the entry
// built from it, returning that version. The caller threads the returned
// version into the write job it's about to issue, so the eventual commit can
// later prove (via CommitIfCurrent/DeleteIfCurrent) that it's still settling
// the latest write for this key before mutating the cache.
func (c *hashCache) Reserve(key string, entry CacheEntry) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	entry.Version = c.data[key].Version + 1
	c.data[key] = entry
	return entry.Version
}

// CommitIfCurrent stores entry for key only if the entry currently present
// (if any) still has exactly expectedVersion — i.e. no newer Reserve has
// happened for this key since the caller's write was issued. Returns
// whether it applied. A newer entry present means a later write has already
// superseded this one; leaving it alone (rather than overwriting) is what
// prevents a stale, out-of-order commit from clobbering fresher state.
func (c *hashCache) CommitIfCurrent(key string, expectedVersion uint64, entry CacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if present && cur.Version != expectedVersion {
		return false
	}
	if !present && expectedVersion != 0 {
		return false
	}
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	entry.Version = expectedVersion
	c.data[key] = entry
	return true
}

// DeleteIfCurrent removes key only if its entry still has exactly
// expectedVersion. Returns whether it applied.
func (c *hashCache) DeleteIfCurrent(key string, expectedVersion uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if !present || cur.Version != expectedVersion {
		return false
	}
	delete(c.data, key)
	return true
}

// deleteClaimOutcome is why a ReserveDelete succeeded or was refused. It is
// decided under the cache's own lock, so it is a fact about the moment of the
// decision — not something a caller can re-derive afterwards without racing.
//
// The distinction matters to exactly one caller today: the per-scope GC pass,
// which has to know whether a refusal left the old incarnation's death
// unrecorded (deleteClaimUIDMismatch — the successor owns the key) or already
// recorded by somebody else (deleteClaimInFlight — that very deletion's row is
// on its way to the sink). Collapsing the two makes the pass recover a close-out
// that is already in flight, producing a second, differently-timestamped Deleted
// row for one UID.
type deleteClaimOutcome int

const (
	deleteClaimed          deleteClaimOutcome = iota // the caller now owns this deletion
	deleteClaimAbsent                                // the key has no entry
	deleteClaimInFlight                              // another claim already owns this deletion
	deleteClaimUIDMismatch                           // the key now holds a different incarnation
)

// String makes the outcome loggable, so a refusal can name its own reason in the
// place it is acted on rather than being reported as an opaque integer.
func (o deleteClaimOutcome) String() string {
	switch o {
	case deleteClaimed:
		return "claimed"
	case deleteClaimAbsent:
		return "absent"
	case deleteClaimInFlight:
		return "in-flight"
	case deleteClaimUIDMismatch:
		return "uid-mismatch"
	default:
		return fmt.Sprintf("deleteClaimOutcome(%d)", int(o))
	}
}

// ReserveDelete claims key for a pending delete, the delete-path counterpart
// to Reserve: it lets a "Deleted" write be claimed synchronously, in-cache,
// before it's enqueued, so a redelivered Process call (or the per-scope GC
// pass noticing the same disappearance) sees the claim already in place instead
// of independently enqueuing a second "Deleted" row for the same object.
// Without this, nothing about entering the delete branch touched the cache
// until the write's commit fired, so any number of redeliveries for the same
// key in that window each enqueued their own duplicate write — the version
// check on commit kept the *cache* consistent, but by then every duplicate
// INSERT had already reached ClickHouse.
//
// It refuses, and says why (see deleteClaimOutcome), if key has no entry
// (deleteClaimAbsent — nothing to delete), the entry is already claimed
// (deleteClaimInFlight — someone else's delete is on its way to the sink), or
// expectedUID is non-empty and doesn't match the entry's current UID
// (deleteClaimUIDMismatch — the key now belongs to a different incarnation). A
// refusal returns a zero entry and version: there is nothing to settle. The
// reason is reported rather than left for the caller to reconstruct because the
// three are materially different — only a UID mismatch leaves the refused UID's
// death unrecorded — and reconstructing one after the fact races the very
// transitions it is trying to distinguish. Otherwise it bumps the version,
// exactly like Reserve, so any other write already in flight for this key is
// superseded and its eventual commit becomes a safe no-op; it returns
// deleteClaimed, the pre-claim entry (for its UID/content), and the new version
// to thread into the eventual DeleteIfCurrent/UnclaimDelete call.
//
// expectedUID matters for a caller (the per-scope GC pass) whose belief that
// "this object is gone" comes from a point-in-time snapshot rather than a live
// lister read: if a live Process call has since reincarnated this key (deleted and
// recreated under a new UID) and already updated the cache via Reserve, the
// entry is present and unclaimed, so without this check the claim would
// succeed against the *live* entry — claiming and deleting a currently-
// existing object by name alone. Passing "" skips the check entirely, for
// callers (the live absent-from-lister path) that have no independent, possibly-
// stale belief about which UID they expect and simply trust whatever the
// cache currently holds. On that path deleteClaimUIDMismatch is therefore
// unreachable — the check is skipped entirely — so the live delete path's
// behaviour is exactly what it was before outcomes were reported.
func (c *hashCache) ReserveDelete(key string, expectedUID string) (entry CacheEntry, version uint64, outcome deleteClaimOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if !present {
		return CacheEntry{}, 0, deleteClaimAbsent
	}
	if cur.PendingDelete {
		return CacheEntry{}, 0, deleteClaimInFlight
	}
	if expectedUID != "" && cur.UID != expectedUID {
		return CacheEntry{}, 0, deleteClaimUIDMismatch
	}
	claimedEntry := cur
	cur.Version++
	cur.PendingDelete = true
	c.data[key] = cur
	return claimedEntry, cur.Version, deleteClaimed
}

// UnclaimDelete releases a ReserveDelete claim after its write ultimately
// fails, so a later attempt (triggered by a requeue) can claim key again. A
// no-op if key has since moved on — superseded by a newer Reserve/
// ReserveDelete, or already removed by a successful commit — since in that
// case whatever is current is already correct and must not be disturbed.
func (c *hashCache) UnclaimDelete(key string, expectedVersion uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if !present || cur.Version != expectedVersion {
		return
	}
	cur.PendingDelete = false
	c.data[key] = cur
}

// StoreIfAbsent sets entry for key only if key has no entry yet. Used by the
// per-scope cache warm-up to seed historical baselines without clobbering a
// live entry that a concurrent Process call may have already reserved for this
// key while the restore was still in flight.
func (c *hashCache) StoreIfAbsent(key string, entry CacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	if _, exists := c.data[key]; exists {
		return
	}
	entry.Version = 1
	c.data[key] = entry
}

// DeletePrefix removes every entry whose key starts with prefix, returning how
// many it removed. It exists for scope eviction (see Pipeline.EvictScope): when
// a watch target stops, the objects it covered are no longer observable, so
// keeping their baselines would both leak memory and let a stale hash suppress
// a genuine change if the same scope is watched again later. Prefix matching is
// safe precisely because cacheKey's layout is "group|Kind|namespace/name" (see
// ScopeKey.scopeKeyPrefix).
//
// This is deliberately *not* a delete-path primitive: no "Deleted" row is ever
// emitted here. "We stopped watching" and "the object was deleted" are
// different truths (the scope-epoch design), and conflating them is exactly the
// audit lie Phase 1 exists to prevent.
func (c *hashCache) DeletePrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for key := range c.data {
		if strings.HasPrefix(key, prefix) {
			delete(c.data, key)
			removed++
		}
	}
	return removed
}

// entryEncoding marks how CacheEntry.JSON is stored so the diff path knows
// whether it must decompress before comparing. It exists because the diff
// baseline — a full normalized-JSON copy of every watched object, held in
// addition to the informer cache's own copy — is the dominant hashCache
// memory cost at scale (D2). Kubernetes JSON compresses extremely well, so
// baselines are stored zstd-compressed and decompressed only when a diff is
// actually computed. The marker (rather than always assuming compression)
// lets a compression failure degrade gracefully to storing raw bytes
// (Invariant 5) while a later diff still knows not to decompress them.
type entryEncoding uint8

const (
	// encodingRaw means CacheEntry.JSON holds uncompressed bytes. It is the
	// zero value, so a CacheEntry built with a nil JSON (e.g. a history-warmed
	// baseline) is correctly classified without any explicit assignment.
	encodingRaw entryEncoding = iota
	// encodingZstd means CacheEntry.JSON holds zstd-compressed bytes that
	// must be run through decodeBaseline before diffing.
	encodingZstd
)

// zstdMagic is the 4-byte magic number that opens every zstd frame
// (RFC 8878). decodeBaseline checks for it before attempting a decompress so
// a corrupted or mislabelled entry fails fast with a clear error instead of
// feeding garbage into the decoder — the corruption path then falls back to
// a full-state write exactly like a missing baseline.
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// zstdEncoder and zstdDecoder are process-wide singletons: zstd's EncodeAll
// and DecodeAll are explicitly safe for concurrent use (each call runs on its
// own goroutine from an internal pool), so every pipeline worker shares one
// pair rather than allocating a codec per write. SpeedDefault is the level
// the task pins — a good size/CPU trade-off for the hot write path. Both are
// created with valid options, so the errors are not expected; a nil codec
// (should construction ever fail) is handled defensively by compressBaseline
// and decodeBaseline rather than panicking.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// compressBaseline compresses a normalized-JSON diff baseline for storage in
// CacheEntry.JSON, returning the bytes to store, their encoding marker, and
// whether compression actually happened. It never fails the caller: if the
// shared encoder is unavailable (construction failed) it degrades to storing
// the raw bytes and reports compressed=false so the caller can log the
// anomaly at Error level (Invariant 5). An empty input is passed through
// untouched — there is nothing to compress and nothing to diff against.
func compressBaseline(raw []byte) (data []byte, enc entryEncoding, compressed bool) {
	if len(raw) == 0 || zstdEncoder == nil {
		return raw, encodingRaw, false
	}
	return zstdEncoder.EncodeAll(raw, nil), encodingZstd, true
}

// decodeBaseline returns the raw normalized JSON for this entry's stored diff
// baseline, decompressing if it was zstd-encoded. A nil JSON yields (nil, nil)
// — the "no prior baseline" case the caller already handles as a full-state
// write. A corrupt or truncated compressed entry returns an error so the
// caller falls back to a full-state write (and logs) rather than diffing
// against garbage; the magic-byte check catches a mislabelled entry before
// the decoder is even invoked.
func (e CacheEntry) decodeBaseline() ([]byte, error) {
	if e.JSON == nil {
		return nil, nil
	}
	switch e.Encoding {
	case encodingRaw:
		return e.JSON, nil
	case encodingZstd:
		if !bytes.HasPrefix(e.JSON, zstdMagic) {
			return nil, fmt.Errorf("compressed baseline missing zstd magic bytes (corrupt entry, %d bytes)", len(e.JSON))
		}
		if zstdDecoder == nil {
			return nil, fmt.Errorf("zstd decoder unavailable")
		}
		return zstdDecoder.DecodeAll(e.JSON, nil)
	default:
		return nil, fmt.Errorf("unknown cache entry encoding %d", e.Encoding)
	}
}

// CacheEntry holds the in-memory cached state for one object (see hashCache).
type CacheEntry struct {
	Hash string
	// JSON is the normalized-JSON diff baseline for this object, stored in the
	// form indicated by Encoding (zstd-compressed in the common case). It is
	// never read on the dedup short-circuit — only Hash is — so an unchanged
	// object is deduplicated without ever decompressing. Decode it via
	// decodeBaseline() before diffing. A nil JSON means "no confirmed
	// baseline yet" (e.g. a history-warmed entry) and diffs to full state.
	JSON []byte
	// Encoding records how JSON is stored (raw vs zstd) so decodeBaseline
	// knows whether to decompress. See entryEncoding.
	Encoding entryEncoding
	UID      string
	// APIVersion is the api_version last observed for this object. Identity is
	// version-agnostic (Invariant 7) and the queue Key therefore carries no
	// version, so the delete path — which has no live object left to read —
	// would otherwise write a Deleted row with an empty api_version. Carrying
	// the last observed value forward keeps that provenance column populated
	// (see docs/SCHEMA.md: api_version is provenance, never identity). It is
	// empty for a history-warmed entry, whose source row is not re-read.
	APIVersion string
	// ModifiedSinceCheckpoint counts the consecutive diff-only "Modified" rows
	// written for this key since the last row that carried full data (an
	// "Added"/"Snapshot", a full-state fallback, or a "Checkpoint"). It is what
	// bounds replay cost for a long-lived object: a reader reconstructing "state
	// at time T" replays at most this many diffs before it reaches a
	// data-bearing row (see docs/SCHEMA.md, "Reconstructing state at an
	// instant"), and the counter is what tells the write path when to interrupt
	// the diff run with a Checkpoint (see checkpointDue).
	//
	// It rides the same version gating as every other field: Reserve stores the
	// advanced count optimistically and a failed write reverts to the
	// pre-write entry, so a row that never reached the sink never advances the
	// run — the count always describes rows that are actually in the sink.
	//
	// It is deliberately in-memory only and **resets on operator restart**,
	// which costs nothing: a restart starts from an empty (or history-warmed,
	// JSON-less) cache, so the first row it writes for a key is data-bearing
	// anyway and re-baselines the replay window. Persisting the counter would
	// buy a slightly earlier checkpoint at the cost of durable state outside
	// the Kubernetes API and the sink (Invariant 6).
	ModifiedSinceCheckpoint int
	// Version is assigned by hashCache.Reserve/StoreIfAbsent and is the basis
	// for CommitIfCurrent/DeleteIfCurrent's staleness check: an async write's
	// outcome is only applied if the entry's Version hasn't moved on since
	// the write was issued, so an out-of-order (but stale) commit can never
	// clobber a newer entry. See hashcache.go.
	Version uint64
	// PendingDelete is set by hashCache.ReserveDelete while a "Deleted" write
	// for this key is in flight. It's what lets the live delete path and the
	// per-scope GC pass share one claim: whichever of them notices the object
	// is gone first claims it and flips this on; anyone else who notices the
	// same disappearance before the claim resolves sees it already set and
	// does not enqueue a second write. See hashcache.go.
	PendingDelete bool
}
