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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/wI2L/jsondiff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// normalizedObject is everything Process derives from one live object: the exact
// bytes that get hashed and diffed, plus the metadata the Record needs. It is
// produced by normalizeObject in a single pass so the "read this before
// stripping that" ordering constraints live in one place instead of being spread
// across the hot path.
type normalizedObject struct {
	// JSON is the normalized object: managedFields, resourceVersion, generation
	// and the actors annotation removed. It is both the hash input and the diff
	// baseline stored in the cache.
	JSON []byte
	// Hash is the hex SHA-256 of JSON — the dedup discriminator.
	Hash string
	// UID gates reincarnation detection (same name, new object).
	UID string
	// ResourceVersion is recorded for provenance even though it is stripped
	// before hashing (it changes on every write, so hashing it would defeat
	// dedup entirely).
	ResourceVersion string
	// APIVersion is recorded for provenance and cached, so the delete path — which
	// has no live object to read — can still populate the column.
	APIVersion string
	Labels     map[string]string
	// Actors are the field managers that touched the object, read from the
	// informer transform's annotation (or harvested directly from managedFields
	// when no transform ran). Never nil.
	Actors []string
}

// normalizeObject strips the volatile and operator-internal fields from obj,
// applies policy's redaction to what remains, and hashes the result.
//
// It never mutates obj. That is not defensive paranoia: the object comes from
// ListerRegistry and may be the informer's own cached instance, shared with every
// other reader in the process, so mutating it would corrupt the watch cache for
// everyone — including this pipeline's own next diff.
//
// It used to buy that guarantee with a full DeepCopy of the object, which
// measured (Task 2.3) as the single largest allocation source in the data plane —
// roughly half the bytes allocated per work item, paid on *every* event including
// the ones that deduplicate away and write nothing. The copy is now confined to
// the maps this function actually edits: a fresh top-level map, a fresh
// `metadata` map, and a fresh `annotations` map when one key has to come out of
// it (see stripVolatileFields). Everything else — spec, status, and every nested
// value under them, which is the overwhelming bulk of a Kubernetes object — is
// shared with the caller's object by reference and only ever read, by
// json.Marshal.
//
// What is stripped, and why:
//   - managedFields: enormous, and it changes on writes that change nothing
//     else. The informer transform (Task 1.4) normally removes it before the
//     object is ever cached; this strip is the defensive second line for any
//     object that reaches the pipeline untransformed.
//   - resourceVersion, generation: bump on every write, so hashing them would
//     make every object look changed on every event.
//   - the actors annotation: operator-internal transport, not object content.
//     It is read into the record first and removed before hashing, and the
//     annotations map itself is removed when that leaves it empty — otherwise an
//     object whose only annotation was ours would hash differently from the same
//     object before the transform ran, which is exactly the regression the hash
//     test guards.
//
// Redaction (Task 3.3) runs last, after stripping and *before* hashing, and that
// ordering is the whole security property. Hash, dedup baseline, diff and the
// stored payload are then all functions of the redacted content, so two objects
// differing only in a scrubbed value hash identically and deduplicate away
// instead of leaking the difference through a diff delta. A nil policy is not an
// opt-out: it applies the built-in scrubs (see RedactionPolicy.Apply).
func normalizeObject(obj *unstructured.Unstructured, policy *RedactionPolicy) (normalizedObject, error) {
	out := normalizedObject{
		UID:             string(obj.GetUID()),
		ResourceVersion: obj.GetResourceVersion(),
		APIVersion:      obj.GroupVersionKind().Version,
		// GetLabels returns a fresh map, which is what the Record needs: it
		// travels to the sink and outlives this call, so it must not alias the
		// informer's own map.
		Labels: obj.GetLabels(),
	}
	if out.Labels == nil {
		out.Labels = make(map[string]string)
	}

	// Read the actors before anything is removed. The annotation is the normal
	// source (the informer transform harvested it while managedFields was still
	// present); ExtractActors is the fallback for an object that still carries
	// managedFields, which keeps the actors column correct rather than silently
	// empty when a lister does not transform.
	annotations := obj.GetAnnotations()
	_, hasActorsAnnotation := annotations[ActorsAnnotation]
	if hasActorsAnnotation {
		out.Actors = decodeActors(annotations[ActorsAnnotation])
	} else {
		out.Actors = ExtractActors(obj)
	}

	objJSON, err := json.Marshal(policy.Apply(stripVolatileFields(obj.Object, hasActorsAnnotation)))
	if err != nil {
		return normalizedObject{}, err
	}
	hashBytes := sha256.Sum256(objJSON)

	out.JSON = objJSON
	out.Hash = hex.EncodeToString(hashBytes[:])
	return out, nil
}

// stripVolatileFields returns a view of object with the fields normalizeObject
// strips removed, copying only the maps it has to edit and sharing everything
// else with the argument by reference. The result is for marshalling only: it
// aliases the caller's object, so it must never be mutated or retained.
//
// stripActorsAnnotation says whether the caller found the operator's actors
// annotation via the object's typed accessor. It is threaded in rather than
// re-derived here so the decision is made once, off the same value the actors
// were read from — an object whose annotations map holds a non-string value makes
// that accessor report nothing, and this function must strip exactly what the
// caller believed it read (nothing, in that case) rather than form its own
// opinion from the raw map.
//
// An object with no `metadata` map, or a `metadata` that is not a map, is
// returned untouched: there is nothing to strip, and hashing it as-is is what the
// old copy-then-remove code did too (RemoveNestedField is a no-op on a path that
// does not exist).
func stripVolatileFields(object map[string]any, stripActorsAnnotation bool) map[string]any {
	metadata, ok := object["metadata"].(map[string]any)
	if !ok {
		return object
	}

	strippedMeta := make(map[string]any, len(metadata))
	for field, value := range metadata {
		switch field {
		case "managedFields", "resourceVersion", "generation":
			// Dropped: enormous (managedFields) or bumped on every write
			// (resourceVersion, generation), so hashing them would make every
			// object look changed on every event.
			continue
		case "annotations":
			if !stripActorsAnnotation {
				strippedMeta[field] = value
				continue
			}
			existing, isMap := value.(map[string]any)
			if !isMap {
				strippedMeta[field] = value
				continue
			}
			if len(existing) == 1 {
				// Ours was the only annotation, so the map goes too: an object
				// whose only annotation was the operator's must hash identically
				// to the same object before the transform ran.
				continue
			}
			withoutActors := make(map[string]any, len(existing)-1)
			for name, annotation := range existing {
				if name != ActorsAnnotation {
					withoutActors[name] = annotation
				}
			}
			strippedMeta[field] = withoutActors
		default:
			strippedMeta[field] = value
		}
	}

	stripped := make(map[string]any, len(object))
	maps.Copy(stripped, object)
	stripped["metadata"] = strippedMeta
	return stripped
}

// ObjectHash returns the canonical content hash of obj — the value that reaches
// the sink's sha256 column — without any of the pipeline's state.
//
// It exists for the acceptance suites, and specifically for Task 2.1's
// "no gaps" criterion: after an outage, the final sha256 recorded in ClickHouse
// must equal a hash recomputed from the object as it now lives in the API
// server. The alternative was for the suite to reimplement normalizeObject's
// strip rules, which would mean the assertion silently stopped comparing
// anything the first time those rules changed — the exact drift the criterion is
// meant to catch. Routing the suite through the same function the write path
// uses makes the comparison a real one.
//
// policy must be the redaction policy the stream in question runs under, since
// redaction happens before hashing and therefore changes the answer; nil means
// the built-in scrubs only, which is what an un-configured stream uses. Passing
// the wrong policy makes the comparison meaningless in the silent direction, so
// a suite that configures redaction must thread its own compiled policy in here.
//
// It does not mutate obj (see normalizeObject).
func ObjectHash(obj *unstructured.Unstructured, policy *RedactionPolicy) (string, error) {
	norm, err := normalizeObject(obj, policy)
	if err != nil {
		return "", err
	}
	return norm.Hash, nil
}

// NormalizedJSON returns the canonical normalized JSON of obj — the exact bytes
// the write path hashes, diffs against, and puts in a row's data column — without
// any of the pipeline's state.
//
// It exists for the same reason ObjectHash does, and for Task 2.2 specifically:
// the reconstruction recipe published in docs/SCHEMA.md is only meaningful if a
// test can prove that (last data-bearing row) + (subsequent diffs) reproduces the
// live object *byte for byte*. The alternative was for the suite to reimplement
// normalizeObject's strip rules, which would mean the assertion silently stopped
// comparing the real thing the first time those rules changed.
//
// policy carries the same meaning and the same caveat as it does for ObjectHash.
//
// It does not mutate obj (see normalizeObject).
func NormalizedJSON(obj *unstructured.Unstructured, policy *RedactionPolicy) ([]byte, error) {
	norm, err := normalizeObject(obj, policy)
	if err != nil {
		return nil, err
	}
	return norm.JSON, nil
}

// CheckpointPolicy is the optional half of a sink.Writer that declares how often
// its diff-only history should be interrupted by a full-state Checkpoint row.
//
// It is declared here, next to the code that consults it, rather than in
// internal/sink, for the same reason ListerRegistry and SinkRouter are: the
// pipeline is the consumer, and internal/sink/clickhouse must stay free of any
// import of this package (it already mirrors that rule for metrics). A Writer
// that does not implement it gets no checkpoints at all — checkpointing is a
// declared per-sink policy, not a default the pipeline can invent on a backend's
// behalf.
type CheckpointPolicy interface {
	// CheckpointEvery returns the number of consecutive diff-only Modified
	// writes after which the next one is promoted to a Checkpoint. Zero (or
	// negative) disables checkpointing entirely for that sink.
	CheckpointEvery() int
}

// checkpointEveryFor resolves the key's sink's checkpoint cadence. A backend that
// declares no policy reports 0, i.e. disabled (see CheckpointPolicy).
func checkpointEveryFor(writer sink.Writer) int {
	policy, ok := writer.(CheckpointPolicy)
	if !ok {
		return 0
	}
	return policy.CheckpointEvery()
}

// checkpointReason names why a Modified write was promoted to a Checkpoint, so
// the log line says which of the two independent triggers fired rather than
// leaving an operator to infer it from the row.
type checkpointReason string

const (
	// checkpointReasonCount is the cadence trigger: this is the every-Nth
	// Modified write for the key.
	checkpointReasonCount checkpointReason = "count"
	// checkpointReasonSize is the efficiency trigger: this single diff is larger
	// than the object it describes, so storing the diff alone saves nothing and
	// still costs a replay step.
	checkpointReasonSize checkpointReason = "size"
)

// checkpointDue decides whether the Modified write being assembled should be
// promoted to a Checkpoint (a row carrying the full data *and* the diff).
//
// Two independent triggers, either of which is enough:
//
//   - count: modifiedRun — this key's consecutive diff-only Modified rows,
//     including the one being written — has reached every. This is what bounds
//     replay cost for a long-lived object.
//   - size: this write's diff is larger than the full object it describes. A diff
//     that big is pure loss (more bytes than the state, and a replay step on top),
//     and it fires regardless of how far the run has progressed.
//
// Both operands of the size comparison must be *uncompressed* bytes of the same
// two things the row is actually made of, and two readings are explicitly wrong:
//
//   - the row's data *column* is not an operand. It is empty on a Modified row by
//     the schema-v1 design, so comparing against it would make every diff look
//     oversized and fire the trigger on every single update.
//   - the cached baseline is not an operand either. It is zstd-compressed (Task
//     0.7 mandates ≥60% compression), so comparing a raw diff against it would
//     over-trigger by roughly the compression ratio.
//
// fullJSON is therefore the freshly serialized normalized object the caller
// already holds at diff time, which makes the check free.
//
// every <= 0 disables *both* triggers: a sink that opted out of checkpointing
// must never get a Checkpoint row, however unflattering the diff's size.
func checkpointDue(every, modifiedRun int, diffBytes, fullJSON []byte) (checkpointReason, bool) {
	if every <= 0 {
		return "", false
	}
	if modifiedRun >= every {
		return checkpointReasonCount, true
	}
	if len(diffBytes) > len(fullJSON) {
		return checkpointReasonSize, true
	}
	return "", false
}

// Process settles one work item: it compares what the watch cache holds for the
// key's identity against what the key's sink has already been told, and enqueues
// at most one record describing the difference.
//
// It is the port of the old controller-runtime Reconcile body, and every
// correctness property is preserved verbatim — dedup by hash, reincarnation
// close-outs, UID-gated delete claims, version-gated commits. The only thing
// that changed is how work arrives: a workqueue item instead of a
// reconcile.Request, and a lister read instead of a client Get.
//
// A nil return means settled (written, deduplicated, or deliberately dropped)
// and clears the key's backoff. A non-nil return means "not recorded yet, try
// again": the caller re-adds the key through the rate limiter. Errors are
// logged where they happen, with full identity context, so returning one here
// never loses information (Invariant 4).
func (p *Pipeline) Process(ctx context.Context, key Key) error {
	log := logf.FromContext(ctx).WithName("pipeline").WithValues(key.logValues()...)

	// The lister is the only source of object state: no client Get, no API
	// round-trip on the hot path (Invariant 1).
	obj, found, scopeActive, err := p.lister.Get(key)
	if err != nil {
		log.Error(err, "Failed to look up object in the watch cache, retrying")
		return err
	}

	// The target was stopped while this item sat in the queue. Dropping is the
	// only truthful option: there is no live scope to observe the object through,
	// and recording a Deleted row for it would turn "we stopped watching" into
	// "it was deleted" — precisely the audit lie scope epochs exist to prevent.
	// The Stopped transition is recorded once, by the scope recorder (Task 1.6).
	if !scopeActive {
		p.metrics.dropped.WithLabelValues(DropReasonScopeStopped).Inc()
		log.V(1).Info("Dropping work item: watch scope is no longer active")
		return nil
	}

	// An Event that has left the watch cache has expired, not been deleted: its
	// ~1h TTL came round (see ephemeralKind). Suppressing the row here rather
	// than in emitDelete is what keeps the suppression free of every dependency a
	// real deletion needs — no sink to resolve, no claim to retry, no re-queue —
	// because there is nothing to write and never will be. The cache entry does
	// have to go, though: Events are the highest-churn kind in a cluster, so an
	// entry left behind per expired Event is an unbounded leak.
	if !found && key.ephemeral() {
		p.metrics.dropped.WithLabelValues(DropReasonEphemeralDelete).Inc()
		log.V(1).Info("Dropping work item: an Event left the watch cache, which is TTL expiry rather than a deletion")
		p.forgetEphemeral(key, p.sinks.get(key.Sink))
		return nil
	}

	// Resolve the sink per item rather than capturing a Writer at wiring time, so
	// a sink recycled after a credential rotation is picked up without holding a
	// stale instance. A missing sink is transient (deleted CR, mid-recycle), so
	// the item is retried rather than dropped — the change it describes is real
	// and still unrecorded.
	writer, ok := p.router.WriterFor(sinkIDFor(key.Sink))
	if !ok {
		if p.unavailableSinkLog.allow(key.Sink) {
			log.Error(errSinkUnavailable, "Sink has no live writer, re-queueing this and any further items for it")
		}
		return errSinkUnavailable
	}

	st := p.sinks.get(key.Sink)
	objectKey := key.cacheKey()

	// Retry any close-out write still pending for this identity before doing
	// anything else, so a previously-failed one gets another attempt on the very
	// next event for this name — including the re-add its own failure triggered.
	p.retryPendingCloseOuts(ctx, log, key, st, writer, objectKey)

	if !found {
		// Absent from the watch cache within an active scope: a genuine deletion.
		_, enqueueErr := p.emitDelete(ctx, log, key, st, writer, "")
		return enqueueErr
	}

	return p.processUpsert(ctx, log, key, st, writer, obj, objectKey)
}

// processUpsert handles the "object exists" half of Process: normalize, hash,
// deduplicate, diff, reserve a version, and hand the record to the sink.
//
// Two lint exemptions, both inherited from the code this ports:
// the branch structure is the battle-tested dedup / reincarnation /
// diff-fallback decision tree, and splitting it further would scatter one
// coherent state machine across helpers that each need most of the same locals
// (gocyclo); and it takes both a ctx and a logger because the logger is already
// decorated with this key's identity by Process — re-deriving it from the ctx in
// every helper would either drop that context or duplicate the decoration
// (logcheck).
//
//nolint:gocyclo,logcheck
func (p *Pipeline) processUpsert(ctx context.Context, log logr.Logger, key Key, st *sinkState,
	writer sink.Writer, obj *unstructured.Unstructured, objectKey string) error {
	// Resolved here rather than in Process because this is the only path that
	// writes object content: a Deleted row carries no data, diff or hash, so
	// there is nothing in it to redact.
	policy, ok := p.redactionFor(key)
	if !ok {
		// The scope stopped being watched between Process's scopeActive check and
		// now, so nothing currently declares what this stream must scrub. The item
		// is retried rather than written, because writing it would mean writing
		// object content under no policy at all — and the retry settles it
		// truthfully: the next attempt sees scopeActive=false and drops.
		log.Error(errRedactionUnavailable, "No redaction policy is installed for this key's scope, retrying")
		return errRedactionUnavailable
	}

	norm, err := normalizeObject(obj, policy)
	if err != nil {
		log.Error(err, "Failed to marshal object for hashing, retrying")
		return err
	}

	// ephemeral is Kubernetes-Event mode (see ephemeralKind): full state instead
	// of a diff, no close-out row for a superseded name, no Snapshot tagging. It
	// is resolved once because three separate branches below consult it.
	ephemeral := key.ephemeral()

	var eventType = "Added"
	var diffString = ""

	// carriesFullState says whether this row's data column holds the object's
	// whole normalized state. Every data-bearing row does (Added, Snapshot, a
	// full-state fallback, a Checkpoint); a plain diff-only Modified does not.
	//
	// It is a bool rather than the string itself because the conversion copies the
	// entire normalized object, and the two most frequent outcomes on a busy
	// cluster — a dedup skip and a diff-only Modified — would build that copy and
	// then throw it away. Measured (Task 2.3) at roughly a tenth of the bytes
	// allocated per work item, for a value most work items never use. The string
	// is materialized once, in the Record literal below.
	var carriesFullState = true

	// modifiedRun is the diff-only-Modified run length this write leaves behind
	// for the key (see CacheEntry.ModifiedSinceCheckpoint). It stays 0 for every
	// row that carries full data — Added, Snapshot, a full-state fallback, and a
	// Checkpoint all re-baseline the replay window — and is only advanced on the
	// plain-diff path below.
	var modifiedRun int

	// revertEntry is what the cache should fall back to if the write below
	// ultimately fails, so a lost write can never be mistaken for a
	// confirmed one on a subsequent Process call. nil means "no prior confirmed
	// state" (delete the key entirely on failure).
	var revertEntry *CacheEntry

	// cacheMiss records whether this call found no cache entry at all for
	// objectKey — the one case the Snapshot fallback below exists to guard,
	// since it's genuinely ambiguous whether "Added" means "truly new" or "not
	// yet warmed from the sink's history." A reincarnation (cache hit, UID
	// mismatch) is never ambiguous this way — we have direct proof (the stored
	// old UID vs. the live new UID) that this is a real, current state
	// transition, so it must always be recorded as "Added," never downgraded to
	// "Snapshot," regardless of warm state.
	var cacheMiss bool

	// --- DEDUPLICATION AND REINCARNATION BLOCK ---
	if cachedEntry, exists := st.cache.Load(objectKey); exists {
		// 🚨 ANTI-ZOMBIE MAGIC: check the UID!
		if cachedEntry.UID != "" && cachedEntry.UID != norm.UID {
			if ephemeral {
				// A different Event now answers to this name. Nothing is closed
				// out: an Event never gets a Deleted row (see ephemeralKind), and
				// the close-out is a Deleted row like any other, so emitting one
				// here would reintroduce through the back door precisely what the
				// TTL suppression keeps out. Nor is anything lost by declining —
				// the predecessor's own history stands untouched under its own UID,
				// and its absence of a Deleted row is the honest record of an Event
				// that expired rather than one that was deleted. The stale entry is
				// superseded by the Reserve below.
				log.Info("♻️ A new Event took this name over from an expired one, recording it as Added",
					"old_uid", cachedEntry.UID)
			} else if claimedEntry, _, outcome := st.cache.ReserveDelete(objectKey, cachedEntry.UID); outcome == deleteClaimed {
				log.Info("🧟 Reincarnation! Old object died while unobserved — closing its history and treating the current one as Added")
				// Deleted rows carry empty data/diff/sha256 in schema v1 —
				// event_type alone marks the deletion (see docs/SCHEMA.md).
				closeRecord := sink.Record{
					Timestamp:  time.Now().UTC(),
					ClusterID:  p.clusterID,
					EventType:  "Deleted",
					APIGroup:   key.Group,
					APIVersion: claimedEntry.APIVersion,
					Kind:       key.Kind,
					Namespace:  key.Namespace,
					Name:       key.Name,
					UID:        claimedEntry.UID,
				}

				// 1. Close out the old object's history — failures are
				// remembered and retried (see enqueueCloseOut), not just
				// logged, so this historical record can't be silently lost.
				// The claim above is immediately superseded by this call's own
				// Reserve for the new incarnation a few lines below, so
				// ReserveDelete's version-gated commit
				// (DeleteIfCurrent/UnclaimDelete) becomes a safe no-op once
				// that happens — it's enqueueCloseOut's own closeOuts-based
				// retry, not the claim, that carries this write forward on
				// failure.
				p.enqueueCloseOut(ctx, log, key, st, writer, objectKey, closeRecord)
			} else {
				log.Info("🧟 Old incarnation's deletion already claimed elsewhere, skipping close-out write",
					"old_uid", cachedEntry.UID, "reason", outcome.String())
			}

			// 2. The current object is treated as a plain Added (leave eventType = "Added").
			// There is no confirmed prior state for THIS UID, so revertEntry stays nil.
		} else {
			// Ordinary logic (same object)
			if cachedEntry.Hash == norm.Hash {
				p.metrics.dedupSkips.Inc()
				return nil // Duplicate
			}

			// switchToFullState is the shared fallback for every case where a
			// diff can't be produced (no prior JSON to diff against, or the
			// diff/marshal itself fails) — writing the full current state is
			// always correct on its own, just larger than a diff would be.
			switchToFullState := func() {
				carriesFullState = true
				diffString = ""
			}

			eventType = "Modified"
			baseline, decodeErr := cachedEntry.decodeBaseline()
			if ephemeral {
				// The count bump. An Event is updated in place to say "this
				// happened again", and a reader of that row wants the Event as it
				// stood at that moment — the whole thing, self-contained, so the
				// "events for object X around time T" recipe in docs/QUERIES.md
				// never has to replay a diff chain to read a `count` or a
				// `message`. A patch would save bytes on an object that is small to
				// begin with and gone within the hour, and would cost every reader
				// a replay step to recover it. Dedup has already run above, so a
				// no-op resync still writes nothing.
				//
				// This branch comes first deliberately: an Events entry stores no
				// baseline at all (see below), so without it every count bump would
				// take the "restored from sink history" path and log as if it were
				// recovering from a cold cache.
				switchToFullState()
			} else if cachedEntry.JSON == nil {
				log.Info("🔄 Restored from sink history (Full State)")
				switchToFullState()
			} else if decodeErr != nil {
				// A corrupt or truncated compressed baseline can't be diffed
				// against; fall back to writing full state exactly like the
				// missing-baseline path above, so the event is preserved
				// (never dropped) rather than silently mis-recorded.
				log.Error(decodeErr, "⚠️ Failed to decompress cached baseline, falling back to full state")
				switchToFullState()
			} else if patch, err := jsondiff.CompareJSON(baseline, norm.JSON); err != nil {
				// Not expected to be reachable today — cachedEntry.JSON and
				// norm.JSON are always the product of a prior successful
				// json.Marshal — but a silently-discarded error here would
				// otherwise write neither a diff nor the full state,
				// corrupting this row's audit value with no log signal.
				log.Error(err, "⚠️ Failed to compute JSON diff, falling back to full state")
				switchToFullState()
			} else if patchBytes, err := json.Marshal(patch); err != nil {
				log.Error(err, "⚠️ Failed to marshal JSON diff, falling back to full state")
				switchToFullState()
			} else if reason, due := checkpointDue(checkpointEveryFor(writer),
				cachedEntry.ModifiedSinceCheckpoint+1, patchBytes, norm.JSON); due {
				// A Checkpoint is a Modified in every semantic sense except its
				// event_type and its populated data column: it carries the diff
				// *and* the full state, so a reader replaying an object's history
				// never has to walk further back than the last one, while the warm
				// queries (argMax over the whole history) are unaffected.
				eventType = "Checkpoint"
				diffString = string(patchBytes)
				carriesFullState = true
				log.Info("📌 Change detected (Checkpoint: full state alongside the diff)",
					"reason", string(reason), "diff_bytes", len(patchBytes), "state_bytes", len(norm.JSON),
					"modified_run", cachedEntry.ModifiedSinceCheckpoint+1)
			} else {
				diffString = string(patchBytes)
				carriesFullState = false
				modifiedRun = cachedEntry.ModifiedSinceCheckpoint + 1
				log.Info("📝 Change detected (Diff)")
			}
			entryCopy := cachedEntry
			revertEntry = &entryCopy
		}
	} else {
		log.Info("🌟 New object observed")
		cacheMiss = true
	}

	// A genuine cache-miss in a scope that hasn't been warmed from the sink's
	// history can't be trusted to mean "genuinely new" — the cache may simply
	// not be warmed yet. Tag it Snapshot so a slow or unavailable sink at
	// startup (or when a brand-new rule appears) never masquerades as a mass
	// "Added" duplicate-write storm. This intentionally does not cover the
	// reincarnation branch above, which also reaches this point with eventType
	// == "Added" but is never ambiguous — see cacheMiss's doc comment.
	//
	// Nor does it cover Events. Snapshot exists to hedge the one question a cold
	// cache cannot answer — "is this object new, or merely unseen by *this*
	// process?" — and for an Event the hedge buys nothing: an Event is created
	// once, never updated except to bump `count`, and is gone within the hour, so
	// a miss is a new Event with overwhelming probability and the harm Snapshot
	// guards against (a duplicate-write storm over a large standing population)
	// has no standing population to storm over. Warm-up still primes the hashes,
	// which is what actually suppresses the re-emission (see WarmCoordinator.warm).
	if cacheMiss && !ephemeral && !st.scopeWarm(key) {
		eventType = "Snapshot"
		p.recordScopeUnwarmed(key)
		log.Info("🌱 Scope not yet warmed, tagging as Snapshot")
	}

	// Compress the diff baseline once, up front, and reuse the same bytes for
	// both the optimistic Reserve entry and the confirmed commit entry — the
	// compressed copy is what makes hashCache's per-object footprint a
	// fraction of the raw normalized JSON (Task 0.7). A compression failure
	// degrades to storing raw bytes and is logged at Error level (Invariant
	// 5); the diff path handles either encoding transparently via
	// decodeBaseline.
	//
	// Events store no baseline at all. They are never diffed, so the bytes would
	// be written and compressed on every count bump and read by nobody — and this
	// is the kind that produces the most cache entries per cluster, so the entry
	// is kept down to its hash and UID. Nothing downstream has to know: a nil
	// baseline is already the ordinary "no prior state to diff against" case (it
	// is exactly what warm-up seeds), and the Events branch above short-circuits
	// before the path that would act on it.
	var baselineData []byte
	var baselineEnc entryEncoding
	if !ephemeral {
		var compressed bool
		baselineData, baselineEnc, compressed = compressBaseline(norm.JSON)
		if !compressed {
			log.Error(errBaselineCompression, "⚠️ Storing uncompressed baseline")
		}
	}

	confirmedEntry := CacheEntry{
		Hash:                    norm.Hash,
		JSON:                    baselineData,
		Encoding:                baselineEnc,
		UID:                     norm.UID,
		APIVersion:              norm.APIVersion,
		ModifiedSinceCheckpoint: modifiedRun,
	}

	// Reserve atomically assigns the next version for this key and stores
	// the pending entry, so a duplicate work item firing before the write is
	// confirmed short-circuits as a no-op instead of enqueuing a second
	// write for identical content. The returned version is threaded into
	// the job below: the eventual commit (running in a sink worker,
	// possibly out of order relative to some other job for this same key)
	// only applies its result via CommitIfCurrent/revertVersion if this is
	// still the latest write issued for the key — otherwise a newer write
	// has already superseded it and is left alone.
	version := st.cache.Reserve(objectKey, confirmedEntry)
	// Reserve may have added a brand-new key; refresh the size gauge outside
	// any cache lock (recordCacheEntries takes/releases it internally).
	p.recordCacheEntries(key.Sink, st)

	revertVersion := func() {
		if revertEntry != nil {
			st.cache.CommitIfCurrent(objectKey, version, *revertEntry)
		} else {
			st.cache.DeleteIfCurrent(objectKey, version)
		}
	}

	// The one place the normalized state becomes a string, and only for the rows
	// that actually carry it (see carriesFullState).
	dataString := ""
	if carriesFullState {
		dataString = string(norm.JSON)
	}

	record := sink.Record{
		// Timestamp is stamped exactly once here, at processing time, and is never
		// re-stamped on retry: insertArgs renders it into the positional args
		// once, so a re-Exec by the sink's at-least-once isolation path re-sends
		// the identical ts. That immutability is precisely what makes a
		// re-inserted row byte-identical and lets resource_states
		// (ReplacingMergeTree) collapse it on merge — see docs/SCHEMA.md.
		Timestamp:       time.Now().UTC(),
		ClusterID:       p.clusterID,
		EventType:       eventType,
		APIGroup:        key.Group,
		APIVersion:      norm.APIVersion,
		Kind:            key.Kind,
		Namespace:       key.Namespace,
		Name:            key.Name,
		UID:             norm.UID,
		ResourceVersion: norm.ResourceVersion,
		Labels:          norm.Labels,
		Actors:          norm.Actors,
		Data:            dataString,
		Diff:            diffString,
		SHA256:          norm.Hash,
	}

	// --- EXPORT BLOCK ---
	enqueueErr := writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				// Only now is the write durably confirmed — settle the
				// pending marker into a confirmed cache entry, unless a
				// newer write has already superseded this one.
				st.cache.CommitIfCurrent(objectKey, version, confirmedEntry)
				return
			}
			log.Error(errAsyncWriteFailed, "Write failed after retries, reverting cache and re-queueing the key")
			revertVersion()
			// Returning an error from here is impossible — this callback runs
			// well after Process returned — so the key is re-queued explicitly
			// on the rate limiter. That is what replaced the old requeue
			// channel: the workqueue already owns per-key delivery and backoff,
			// so a second, hand-rolled trigger path (with its own bounded
			// channel to overflow and drop) is no longer needed.
			p.queue.AddRateLimited(key)
		},
	})
	if enqueueErr != nil {
		// The job never entered the write pipeline, so no commit will ever
		// fire for it — undo the optimistic marker ourselves.
		revertVersion()
		log.Error(enqueueErr, "Failed to queue write, retrying")
		return enqueueErr
	}

	return nil
}

// emitDelete is the single place a "Deleted" row is ever enqueued. Both the live
// delete path (an object absent from the watch cache inside an active scope) and
// the per-scope GC pass (Task 1.6) detect the same condition — this object is
// gone but the cache still holds its last-known state — and must claim through
// the same hashCache.ReserveDelete so they can never both emit a duplicate
// "Deleted" row for the same disappearance. It also protects against plain
// redelivery: the workqueue guarantees at least one more delivery for a key that
// was touched again while the current one was processing, and since Process
// returns as soon as the write is enqueued (long before the sink confirms it), a
// redelivered delete can easily run before the first one's commit fires. A
// second call for the same key returns deleteClaimInFlight and does nothing.
//
// Neither caller ever reaches it for a Kubernetes Event: the live path suppresses
// the write in Process and forgets the entry instead (see forgetEphemeral), and
// the GC pass never runs over an Events scope at all (see WarmCoordinator.warm).
// So "there is no Deleted row for an Event, ever" is a property of the two call
// sites rather than a check inside this function — which is deliberate, because
// this function's job is to make a deletion exactly-once, not to decide whether
// the deletion happened.
//
// expectedUID lets a caller whose belief that the object is gone comes from a
// stale, point-in-time snapshot (the GC pass) assert it still matches the cache's
// current UID before claiming — otherwise a live reincarnation that happened
// after the snapshot was taken would let this delete claim and remove a
// currently-existing object's entry by name alone. Pass "" for the live path,
// which has no independent belief to check and simply trusts whatever the cache
// currently holds.
//
// The returned outcome is the cache's own reason (see deleteClaimOutcome), passed
// through unchanged so a caller that has to act differently per reason — the GC
// pass, which recovers only a UID mismatch — reads it from where it was decided
// atomically instead of re-deriving it afterwards.
//
//nolint:logcheck
func (p *Pipeline) emitDelete(ctx context.Context, log logr.Logger, key Key, st *sinkState,
	writer sink.Writer, expectedUID string) (outcome deleteClaimOutcome, err error) {
	objectKey := key.cacheKey()

	entry, version, outcome := st.cache.ReserveDelete(objectKey, expectedUID)
	if outcome != deleteClaimed {
		return outcome, nil
	}

	log.Info("🗑️ Object gone, queuing Deleted event for the sink", "uid", entry.UID)

	// A Deleted row carries empty data, diff, and sha256: event_type alone
	// carries deletion semantics in schema v1 (see docs/SCHEMA.md), replacing
	// the pre-v1 data/sha256 deletion sentinels. api_version comes from the last
	// observed live state (see CacheEntry.APIVersion) because identity — and
	// therefore the queue key — is version-agnostic.
	record := sink.Record{
		Timestamp:  time.Now().UTC(),
		ClusterID:  p.clusterID,
		EventType:  "Deleted",
		APIGroup:   key.Group,
		APIVersion: entry.APIVersion,
		Kind:       key.Kind,
		Namespace:  key.Namespace,
		Name:       key.Name,
		UID:        entry.UID,
	}

	// The cache entry is only removed once the deletion record is durably
	// written (see commit) — never before — so a crash or write failure
	// can't silently drop this object from history. On failure, the claim is
	// released (not the whole entry) so a later attempt can retry; a stale
	// release from a superseded claim (e.g. the object was recreated under a
	// new UID while this write was in flight) is a safe no-op — see
	// UnclaimDelete.
	enqueueErr := writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				st.cache.DeleteIfCurrent(objectKey, version)
				p.recordCacheEntries(key.Sink, st)
				return
			}
			log.Error(errAsyncWriteFailed, "🗑️ Deletion write failed, releasing claim so it is retried")
			st.cache.UnclaimDelete(objectKey, version)
			p.queue.AddRateLimited(key)
		},
	})
	if enqueueErr != nil {
		log.Error(enqueueErr, "🗑️ Failed to queue deletion event, releasing claim")
		st.cache.UnclaimDelete(objectKey, version)
		return deleteClaimed, enqueueErr
	}
	return deleteClaimed, nil
}

// forgetEphemeral removes the dedup baseline of an Event that has left the watch
// cache, writing nothing at all.
//
// It is the counterpart of emitDelete for a kind that never gets a Deleted row,
// and it settles through the very same claim primitive rather than a bare map
// delete. That is not ceremony: ReserveDelete bumps the entry's version, so a
// write already in flight for this key (its commit runs in a sink worker, long
// after Process returned) finds itself superseded and its CommitIfCurrent becomes
// a safe no-op — exactly the version gating Invariant 3 requires. A plain delete
// would let that in-flight commit resurrect the entry it just removed.
//
// Unlike emitDelete there is no failure to recover from — nothing is enqueued —
// so the claim and its settlement happen back to back. A refusal means there is
// nothing to forget: deleteClaimAbsent (the Event was never seeded, or an earlier
// expiry already forgot it) is the only reachable one, since no ephemeral
// deletion is ever claimed for a write and no GC pass runs over an Events scope.
func (p *Pipeline) forgetEphemeral(key Key, st *sinkState) {
	objectKey := key.cacheKey()
	_, version, outcome := st.cache.ReserveDelete(objectKey, "")
	if outcome != deleteClaimed {
		return
	}
	if st.cache.DeleteIfCurrent(objectKey, version) {
		p.recordCacheEntries(key.Sink, st)
	}
}

// enqueueCloseOut submits a reincarnation close-out write (a "Deleted" row for a
// UID that's been superseded by a same-named recreate). Unlike emitDelete, it has
// no cache entry of its own to gate or settle — by the time this runs, the
// hashCache entry for objectKey is about to be (or already has been) overwritten
// with the new incarnation's live state. So instead of a version-gated commit, a
// failure (whether the enqueue itself or the write after the sink's own retries)
// is remembered in closeOuts and the key is re-queued;
// retryPendingCloseOuts re-attempts it on that (or any later) work item for this
// key, so a permanently-failed attempt keeps getting retried instead of the
// historical record silently vanishing.
//
//nolint:logcheck
func (p *Pipeline) enqueueCloseOut(ctx context.Context, log logr.Logger, key Key, st *sinkState,
	writer sink.Writer, objectKey string, record sink.Record) {
	enqueueErr := writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				return
			}
			log.Error(errAsyncWriteFailed,
				"🧟 Failed to close out reincarnated object's history, will retry on the next event for this name",
				"old_uid", record.UID)
			st.closeOuts.Add(objectKey, record)
			p.queue.AddRateLimited(key)
		},
	})
	if enqueueErr != nil {
		log.Error(enqueueErr,
			"🧟 Failed to queue reincarnation close-out event, will retry on the next event for this name",
			"old_uid", record.UID)
		st.closeOuts.Add(objectKey, record)
		p.queue.AddRateLimited(key)
	}
}

// retryPendingCloseOuts re-attempts any reincarnation close-out writes still
// pending for objectKey (see closeOuts). Called unconditionally near the top of
// Process so a previously-failed close-out gets retried on the very next work
// item for this name — including the rate-limited re-add that enqueueCloseOut's
// failure path explicitly triggers — rather than only ever being attempted once.
//
//nolint:logcheck // Takes the caller's already-decorated logger; see processUpsert.
func (p *Pipeline) retryPendingCloseOuts(ctx context.Context, log logr.Logger, key Key, st *sinkState,
	writer sink.Writer, objectKey string) {
	for _, record := range st.closeOuts.TakeAll(objectKey) {
		p.enqueueCloseOut(ctx, log, key, st, writer, objectKey, record)
	}
}

// closeOutRetryQueue is a mutex-protected map from cacheKey to the
// sink.Records still awaiting a successful close-out write for that key.
// A slice, not a single record, because a second reincarnation could in
// principle occur for the same name before the first close-out resolves;
// this way that (rare) case queues up rather than one write silently
// replacing tracking of the other.
//
// It is separate from hashCache because hashCache's entry for a key is
// immediately overwritten with the new incarnation's live state by Reserve —
// there is nowhere in hashCache to durably remember "the old UID's close-out
// still needs to happen" without a newer write clobbering it. One instance
// lives per sink (see sinkState), since a close-out is owed to one sink.
type closeOutRetryQueue struct {
	mu   sync.Mutex
	data map[string][]sink.Record
}

// Add appends record to key's pending list, to be retried on a later call to
// TakeAll for the same key.
func (q *closeOutRetryQueue) Add(key string, record sink.Record) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.data == nil {
		q.data = make(map[string][]sink.Record)
	}
	q.data[key] = append(q.data[key], record)
}

// TakeAll returns and clears key's pending records, if any.
func (q *closeOutRetryQueue) TakeAll(key string) []sink.Record {
	q.mu.Lock()
	defer q.mu.Unlock()
	records := q.data[key]
	delete(q.data, key)
	return records
}
