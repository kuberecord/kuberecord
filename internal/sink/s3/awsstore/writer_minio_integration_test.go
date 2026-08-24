//go:build integration

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

// Task 6.6: the S3 backend against a real object store rather than a fake.
//
// Everything here is asserted by reading MinIO back, never by inspecting the
// writer: the object key is listed from the bucket, the records are decoded from
// the bytes the bucket returns, the retention is whatever the store reports. The
// write path's own unit tests (internal/sink/s3) already prove these properties
// against a stand-in store, and the conformance suite proves the contract ones —
// what only a real store can prove is that the bytes, the key and the headers
// survive the SDK, the wire and the server.
//
// The fixture, the corpus and the read side are in
// minio_fixture_integration_test.go. Every test here runs only under
// `make test-integration` (build tag `integration`), which stands up MinIO
// alongside ClickHouse and points S3_TEST_ENDPOINT at it.
package awsstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
	kbs3 "github.com/yelzhy/kuberecord/internal/sink/s3"
)

// itTimeout bounds one test's dealings with the store. It is generous because a
// failure should read as "this never happened", not as "this machine was busy".
const itTimeout = 120 * time.Second

// TestObjectsLandAtTheDocumentedKeyLayoutIntegration is the first half of the
// task's key criterion: an object written through the shipped client lands at the
// key the published contract (D15) says it will.
//
// The expected key is derived two independent ways and both are asserted. The
// layout is spelled out segment by segment from the contract itself — which is
// what catches a change to the layout, since a check that only compared the
// writer against the encoder would agree with itself after any rename. And the
// content hash is taken from s3.Encode, the format's reference form, which is what
// makes "the accumulating write path and the one-shot encoder produce the same
// object" a claim about a real bucket rather than about two functions in one
// process.
func TestObjectsLandAtTheDocumentedKeyLayoutIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)
	records := itRecords(4)
	log := newCommitLog()

	// One worker and no size limit worth reaching: the whole batch is one object,
	// which is what lets the expected key be computed from the whole batch.
	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket:         bucket.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})

	log.assertSettledOnce(t, len(records))

	keys := bucket.keys(ctx, t)
	if len(keys) != 1 {
		t.Fatalf("bucket holds %d objects, want exactly 1: %v", len(keys), keys)
	}

	// The reference encoder's answer for the same batch. Same key, same content
	// hash — the payload bytes are explicitly not part of the contract (see
	// s3.Encode on why the hash covers the uncompressed payload).
	reference, err := kbs3.Encode(itPrefix, records)
	if err != nil {
		t.Fatalf("encode the batch through the reference encoder: %v", err)
	}
	if keys[0] != reference.Key {
		t.Errorf("object landed at %q, want %q (the reference encoder's key for the same batch)",
			keys[0], reference.Key)
	}

	// And the layout, stated from the contract rather than from the encoder. The
	// first record's timestamp fixes the date and hour partitions; itEpoch is
	// 2026-03-14T09:26:53Z, so the object belongs to date=2026-03-14 hour=09.
	wantPrefix := itPrefix + "/format=jsonl-v1/cluster_id=" + itClusterID + "/date=2026-03-14/hour=09/"
	if !strings.HasPrefix(keys[0], wantPrefix) {
		t.Errorf("object key %q does not sit in the documented partition %q", keys[0], wantPrefix)
	}
	hash := strings.TrimSuffix(strings.TrimPrefix(keys[0], wantPrefix), ".jsonl.zst")
	if len(hash) != 64 {
		t.Errorf("object key %q does not end in a 64-character content hash and %q", keys[0], ".jsonl.zst")
	}
	if hash != reference.ContentHash {
		t.Errorf("object key carries content hash %q, want %q", hash, reference.ContentHash)
	}
}

// TestDecompressedObjectsDecodeToTheRecordsEnqueuedIntegration is the fidelity
// criterion: what comes out of the bucket is what went in, byte for byte of
// meaning.
//
// The corpus deliberately includes the shapes a naive encoder loses — an empty
// diff, an empty data, an empty label map, multi-byte UTF-8 in a name — because
// the format's promise to readers is that every line has the same shape and needs
// no schema inference. Decoding through the shipped s3.Decode (rather than a
// hand-rolled JSON reader) is what makes this the same read path the documented
// recipes describe.
func TestDecompressedObjectsDecodeToTheRecordsEnqueuedIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)

	records := itRecords(3)
	// A deleted object: no data, no diff, no hash — event_type alone carries the
	// fact. It is the row shape most easily lost by an encoder that omits empties.
	records[1].EventType = "Deleted"
	records[1].Data = ""
	records[1].SHA256 = ""
	records[1].Labels = nil
	// A modification: a diff and no full state.
	records[2].EventType = "Modified"
	records[2].Diff = `[{"op":"replace","path":"/spec/replicas","value":2}]`
	records[2].Data = ""
	// Multi-byte UTF-8 in the fields a reader searches on.
	records[2].Name = "wysoką-dostępność-λ"
	records[2].Labels = map[string]string{"tenant": "München", "team": "команда"}

	log := newCommitLog()
	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket:         bucket.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})
	log.assertSettledOnce(t, len(records))

	got := bucket.records(ctx, t)
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("the archive did not return the records that were enqueued:\n got: %#v\nwant: %#v", got, records)
	}
}

// TestARetriedObjectLeavesExactlyOneObjectIntegration is the idempotency
// criterion, and the reason the object key is content-addressed at all.
//
// The failure it forces is the honest one: the first PUT reaches MinIO and
// succeeds, and its acknowledgement is then lost, so the writer retries an object
// the bucket already holds (see faultOnce). That is the case a duplicate would
// come from — a store that had never seen the object could not duplicate anything
// — and the assertion is that the bucket ends up with one object, at one key,
// whose bytes still decode to the batch.
//
// Two PUTs reaching the store is asserted as well, because "exactly one object"
// is only evidence of idempotency if the write really was attempted twice.
//
// The bucket here is unversioned, which is why the count is exact: the second PUT
// replaces the first and there is nothing kept underneath. On a versioned bucket —
// which every Object Lock bucket is — the answer differs, and that is
// TestARetriedObjectOnALockedBucketLeavesOneCurrentVersionIntegration below.
func TestARetriedObjectLeavesExactlyOneObjectIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)
	records := itRecords(5)
	log := newCommitLog()

	store := newFaultOnce(newITStore(t))
	w := kbs3.NewWriter(store, kbs3.Config{
		Bucket:         bucket.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})

	// The retry succeeded, so every job settled true exactly once: a lost
	// acknowledgement must not reach the pipeline as a failed write.
	log.assertSettledOnce(t, len(records))

	if got := store.attempts(); got != 2 {
		t.Errorf("the store saw %d PUTs, want 2 (the lost-ack attempt and its retry)", got)
	}

	keys := bucket.keys(ctx, t)
	if len(keys) != 1 {
		t.Fatalf("a retried object left %d objects in the bucket, want exactly 1: %v", len(keys), keys)
	}
	got := bucket.records(ctx, t)
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("the surviving object is not the batch that was written:\n got: %#v\nwant: %#v", got, records)
	}
}

// TestARetriedObjectOnALockedBucketLeavesOneCurrentVersionIntegration is the same
// idempotency property on the bucket a compliance archive actually runs on, and it
// exists because the answer there is different from the one above — different
// enough that this project documented the wrong one until this test was written.
//
// The retired claim was that S3 refuses the overwrite of a retained object, so a
// retried PUT would fail harmlessly and say so in the sink's logs. It does not.
// Object Lock requires versioning, and on a versioned bucket S3 has no idempotent
// PUT: the retry is *accepted*, creates a second version, and the retained one
// keeps its own retention. Nothing is refused and nothing is logged. Had this test
// existed, it would have failed under that belief in the most direct way possible
// — the second PUT erroring means the writer retries to maxRetryBackoff and
// settles the whole batch as a failed write, so log.assertSettledOnce below would
// have reported five jobs settled false for objects that were sitting in the
// bucket.
//
// So the property is asserted at both levels, because they disagree and both
// matter:
//
//   - What a reader sees: exactly one *current* version at the key, holding
//     exactly the batch. Every consumer of this archive reads current versions —
//     ListObjectsV2, an unversioned GET, the documented DuckDB and Athena recipes
//     — so this is the level at which "a retried write leaves one object" is true.
//   - What the bucket stores: one or two versions of that key. Two is the expected
//     answer on a versioned store and is what MinIO and S3 both do; one is what an
//     implementation that really did collapse the write would leave, and it is not
//     a failure of the writer, so it passes and is reported. Anything else is a
//     duplicate the content-addressed key was supposed to make impossible.
//
// Every version is decoded, not just the current one, which is what makes the
// duplicate provably harmless rather than merely invisible: both hold the same
// batch and carry the same retention. docs/RETENTION.md is where the consequence
// of that duplicate under COMPLIANCE is spelled out.
func TestARetriedObjectOnALockedBucketLeavesOneCurrentVersionIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	locked := newBucket(ctx, t, true)
	records := itRecords(5)
	log := newCommitLog()

	store := newFaultOnce(newITStore(t))
	w := kbs3.NewWriter(store, kbs3.Config{
		Bucket:         locked.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
		// One day is the CRD's floor and the shortest retention this can ask for.
		// The suite's cleanup bypasses governance retention, so nothing is left
		// behind either way — but asking for the minimum keeps that true even if a
		// future run's cleanup cannot bypass.
		ObjectLock: &kbs3.ObjectLock{Mode: itLockMode, RetainDays: 1},
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})

	// The retry was accepted by a bucket that already held a retained version of
	// that exact key. This is the assertion the retired claim would have failed.
	log.assertSettledOnce(t, len(records))
	if got := store.attempts(); got != 2 {
		t.Errorf("the store saw %d PUTs, want 2 (the lost-ack attempt and its retry)", got)
	}

	current := locked.recordObjects(ctx, t)
	if len(current) != 1 {
		t.Fatalf("a retried object left %d current record objects, want exactly 1: %v",
			len(current), locked.keys(ctx, t))
	}
	decoded, err := kbs3.Decode(current[0].body)
	if err != nil {
		t.Fatalf("decode the current version of %q: %v", current[0].key, err)
	}
	if !reflect.DeepEqual(decoded, records) {
		t.Fatalf("the current version of %q is not the batch that was written:\n got: %#v\nwant: %#v",
			current[0].key, decoded, records)
	}

	versions, deleteMarkers := locked.versionIDsOf(ctx, t, current[0].key)
	if deleteMarkers != 0 {
		t.Errorf("key %q carries %d delete markers; nothing in this test deletes anything",
			current[0].key, deleteMarkers)
	}
	switch len(versions) {
	case 1:
		t.Logf("the store collapsed the retried PUT into one version of %q", current[0].key)
	case 2:
		t.Logf("the store kept %d versions of %q, which is what a versioned bucket does with a retried PUT",
			len(versions), current[0].key)
	default:
		t.Fatalf("key %q has %d versions, want 1 or 2 (the accepted PUT and its retry)",
			current[0].key, len(versions))
	}

	// Both versions, if there are two, are the same object: same records, same
	// retention. A retry that had rebuilt or re-dated anything would show up here
	// and nowhere else.
	for _, versionID := range versions {
		obj := locked.objectVersion(ctx, t, current[0].key, versionID)
		versionRecords, err := kbs3.Decode(obj.body)
		if err != nil {
			t.Fatalf("decode version %q of %q: %v", versionID, current[0].key, err)
		}
		if !reflect.DeepEqual(versionRecords, records) {
			t.Errorf("version %q of %q holds %d records, want the %d that were enqueued",
				versionID, current[0].key, len(versionRecords), len(records))
		}
		if obj.lockMode != itLockMode {
			t.Errorf("version %q of %q reports lock mode %q, want %q: every accepted PUT carries the retention",
				versionID, current[0].key, obj.lockMode, itLockMode)
		}
	}
}

// TestRotationBySizeProducesOneObjectPerFullBatchIntegration is the size half of
// the rotation criterion.
//
// The limit is set below one record's compressed size, so every record fills the
// object it opens and the expected object count is exactly the record count —
// which is what makes this an assertion about the trigger rather than about
// zstd's ratio. The records are distinct in every identifying field, so their
// content hashes (and therefore their keys) are distinct too: a rotation bug that
// wrote the same object twice would show up as a missing key, not as a passing
// count.
func TestRotationBySizeProducesOneObjectPerFullBatchIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)
	const wantObjects = 6
	records := itRecords(wantObjects)
	log := newCommitLog()

	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket: bucket.name,
		Prefix: itPrefix,
		// One byte: the object is full as soon as its first record has been
		// compressed into it. Far below the CRD's 1Mi floor on purpose — that floor
		// is about not littering a real archive with small objects, this is about
		// exercising the trigger.
		MaxObjectBytes: 1,
		// The age trigger must not fire here, or it, and not the size, would be
		// what closed the objects.
		MaxObjectAge: time.Hour,
		Workers:      1,
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})
	log.assertSettledOnce(t, len(records))

	keys := bucket.keys(ctx, t)
	if len(keys) != wantObjects {
		t.Fatalf("size rotation produced %d objects, want %d: %v", len(keys), wantObjects, keys)
	}
	for _, obj := range bucket.recordObjects(ctx, t) {
		decoded, err := kbs3.Decode(obj.body)
		if err != nil {
			t.Fatalf("decode %q: %v", obj.key, err)
		}
		if len(decoded) != 1 {
			t.Errorf("object %q holds %d records, want 1 (the size limit closes it on its first)",
				obj.key, len(decoded))
		}
	}
	if got := bucket.records(ctx, t); len(got) != wantObjects {
		t.Errorf("the archive holds %d records across those objects, want %d", len(got), wantObjects)
	}
}

// TestRotationByAgeClosesAnObjectPerBurstIntegration is the age half of the
// rotation criterion, and the only test here that asserts on an object nothing
// closed deliberately: no size limit is reached and no drain happens while the
// assertion is made, so the object in the bucket got there because maxObjectAge
// elapsed.
//
// Two bursts, one object each. A burst is a handful of hand-offs microseconds
// apart, and the age is seconds, so the two cannot merge and neither can split —
// the margin is what keeps this from asserting on the scheduler.
func TestRotationByAgeClosesAnObjectPerBurstIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)
	const rotationAge = 3 * time.Second
	records := itRecords(5)
	log := newCommitLog()

	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket: bucket.name,
		Prefix: itPrefix,
		// Large enough that nothing here can reach it: only the age trigger closes
		// these objects.
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   rotationAge,
		Workers:        1,
	}, itMetrics())

	runWriter(t, w, func() {
		firstBurst, secondBurst := records[:3], records[3:]

		for i, record := range firstBurst {
			if err := w.Enqueue(ctx, sink.Job{Record: record, Commit: log.commitFor(i)}); err != nil {
				t.Fatalf("enqueue record %d: %v", i, err)
			}
		}
		eventually(t, itTimeout, "the age trigger to close the first burst's object", func() bool {
			return len(bucket.keys(ctx, t)) == 1
		})

		first := bucket.recordObjects(ctx, t)
		decoded, err := kbs3.Decode(first[0].body)
		if err != nil {
			t.Fatalf("decode the first burst's object %q: %v", first[0].key, err)
		}
		if !reflect.DeepEqual(decoded, firstBurst) {
			t.Errorf("the first burst's object holds %d records, want the 3 enqueued before the age elapsed",
				len(decoded))
		}

		for i, record := range secondBurst {
			job := sink.Job{Record: record, Commit: log.commitFor(len(firstBurst) + i)}
			if err := w.Enqueue(ctx, job); err != nil {
				t.Fatalf("enqueue record %d: %v", len(firstBurst)+i, err)
			}
		}
		eventually(t, itTimeout, "the age trigger to close the second burst's object", func() bool {
			return len(bucket.keys(ctx, t)) == 2
		})
	})

	log.assertSettledOnce(t, len(records))

	// Compared by name rather than positionally: two objects in one date/hour
	// partition are listed in content-hash order, which is not burst order, and
	// pinning the listing order would be asserting on SHA-256 rather than on
	// rotation. Which records shared an object is asserted above, per object.
	got := bucket.records(ctx, t)
	if !slices.Equal(recordNames(got), recordNames(records)) {
		t.Errorf("the two age-closed objects hold %v, want %v", recordNames(got), recordNames(records))
	}
}

// recordNames renders a slice of records as their sorted names, which is how this
// suite compares two sets of records whose listing order is content-addressed
// rather than chronological.
func recordNames(records []sink.Record) []string {
	names := make([]string, 0, len(records))
	for _, record := range records {
		names = append(names, record.Name)
	}
	slices.Sort(names)
	return names
}

// TestObjectLockRetentionTravelsWithTheObjectIntegration is the Object Lock
// criterion: when spec.objectLock is set, the retention headers really are on the
// object in the bucket, and when it is not, the object carries none of its own.
//
// It runs against a bucket created with Object Lock enabled, because that is the
// only kind that can hold a retained object — a bucket-level setting kuberecord
// documents as a prerequisite it cannot satisfy for an operator (see
// v1alpha1.S3ObjectLockSpec and docs/RETENTION.md).
//
// GOVERNANCE, never COMPLIANCE: a COMPLIANCE-retained object cannot be deleted by
// anyone until its date passes, including the suite that wrote it, so a run would
// leave a bucket behind for as long as the retention it asked for. The difference
// between the two modes is the store's to enforce and the CRD's to validate, and
// neither needs an undeletable object written on every CI run to be true.
func TestObjectLockRetentionTravelsWithTheObjectIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	locked := newBucket(ctx, t, true)
	const retainDays = 2
	records := itRecords(2)
	log := newCommitLog()

	before := time.Now().UTC()
	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket:         locked.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
		ObjectLock:     &kbs3.ObjectLock{Mode: itLockMode, RetainDays: retainDays},
	}, itMetrics())

	runWriter(t, w, func() {
		enqueueAll(ctx, t, w, records, log)
	})
	log.assertSettledOnce(t, len(records))

	objects := locked.recordObjects(ctx, t)
	if len(objects) != 1 {
		t.Fatalf("bucket holds %d record objects, want exactly 1: %v", len(objects), locked.keys(ctx, t))
	}
	obj := objects[0]
	if obj.lockMode != itLockMode {
		t.Errorf("object %q reports lock mode %q, want %q", obj.key, obj.lockMode, itLockMode)
	}
	// The retention is resolved when the object is built, from the wall clock, so
	// it lands between "retainDays from just before the write" and "retainDays from
	// just after it". Asserting the window rather than an instant is what keeps
	// this from being a clock-skew test.
	after := time.Now().UTC()
	earliest := before.Add(retainDays * 24 * time.Hour).Add(-time.Minute)
	latest := after.Add(retainDays * 24 * time.Hour).Add(time.Minute)
	if obj.retainUntil.Before(earliest) || obj.retainUntil.After(latest) {
		t.Errorf("object %q is retained until %s, want an instant between %s and %s (%d days from the PUT)",
			obj.key, obj.retainUntil, earliest, latest, retainDays)
	}

	// The complement, on a bucket of its own: with no spec.objectLock the object
	// carries no retention of its own, so the bucket's default retention (if it
	// has one) is what applies rather than something kuberecord invented.
	plain := newBucket(ctx, t, false)
	plainLog := newCommitLog()
	plainWriter := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket:         plain.name,
		Prefix:         itPrefix,
		MaxObjectBytes: 64 << 20,
		MaxObjectAge:   time.Hour,
		Workers:        1,
	}, itMetrics())
	runWriter(t, plainWriter, func() {
		enqueueAll(ctx, t, plainWriter, records, plainLog)
	})
	plainLog.assertSettledOnce(t, len(records))

	plainObjects := plain.recordObjects(ctx, t)
	if len(plainObjects) != 1 {
		t.Fatalf("the unlocked bucket holds %d record objects, want exactly 1", len(plainObjects))
	}
	if plainObjects[0].lockMode != "" {
		t.Errorf("an object written by a sink with no objectLock reports lock mode %q, want none",
			plainObjects[0].lockMode)
	}
}

// TestProbeWritesAndClassifiesAgainstARealBucketIntegration covers the health
// probe against a real store, which is the one part of this backend whose verdict
// an operator reads directly off the CR.
//
// Two claims. First, that the probe writes: a reachable, writable bucket passes
// and the probe object is really there, outside the format=jsonl-v1 partition so
// no reader's glob over the archive ever meets it. Second, that a bucket which
// cannot accept this sink's objects is reported as sink.ErrSchemaInvalid rather
// than as an outage — the difference between telling whoever is on call "wait" and
// "change something".
//
// The second claim is the one that needs a real server. awsstore recognises that
// refusal by matching the S3 error code *and* the error message (see
// refusesObjectLock), because the codes are shared with ordinary malformed
// requests and the wording differs between implementations. A match against a
// hand-written error proves only that the matcher matches itself; this asserts it
// against what MinIO actually says, and reports the code and message it saw when
// it does not.
func TestProbeWritesAndClassifiesAgainstARealBucketIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	t.Run("a writable bucket passes and the probe object is outside the archive", func(t *testing.T) {
		bucket := newBucket(ctx, t, false)
		w := kbs3.NewWriter(newITStore(t), kbs3.Config{
			Bucket: bucket.name,
			Prefix: itPrefix,
		}, itMetrics())

		if err := w.Probe(ctx); err != nil {
			t.Fatalf("probing a writable bucket failed: %v", err)
		}

		keys := bucket.keys(ctx, t)
		if len(keys) != 1 {
			t.Fatalf("the probe left %d objects in the bucket, want exactly 1: %v", len(keys), keys)
		}
		if want := itPrefix + "/.kuberecord-probe"; keys[0] != want {
			t.Errorf("the probe object is at %q, want %q", keys[0], want)
		}
		if isRecordKey(keys[0]) || keyHasSegment(keys[0], "format=jsonl-v1") {
			t.Errorf("the probe object %q sits inside the archive's own partition, where a reader's "+
				"glob would meet it", keys[0])
		}

		// Probing twice overwrites the one object rather than littering the bucket:
		// a probe runs every minute for the life of the sink.
		if err := w.Probe(ctx); err != nil {
			t.Fatalf("re-probing a writable bucket failed: %v", err)
		}
		if keys := bucket.keys(ctx, t); len(keys) != 1 {
			t.Errorf("two probes left %d objects, want 1: %v", len(keys), keys)
		}
	})

	t.Run("a bucket with no Object Lock configuration is a schema fault, not an outage", func(t *testing.T) {
		// A bucket created *without* Object Lock, and a sink configured to write
		// retained objects into it. No amount of waiting fixes this — Object Lock
		// cannot be enabled on an existing bucket — so it must not be reported as
		// reachability.
		bucket := newBucket(ctx, t, false)
		w := kbs3.NewWriter(newITStore(t), kbs3.Config{
			Bucket:     bucket.name,
			Prefix:     itPrefix,
			ObjectLock: &kbs3.ObjectLock{Mode: itLockMode, RetainDays: 1},
		}, itMetrics())

		err := w.Probe(ctx)
		if err == nil {
			t.Fatal("probing a lock-less bucket with an objectLock sink succeeded; " +
				"the refusal this test exists for did not happen")
		}
		if !errors.Is(err, sink.ErrSchemaInvalid) {
			t.Errorf("the refusal was not classified as sink.ErrSchemaInvalid, so the sink would report "+
				"BucketReachable=False and retry forever.\n error: %v\n S3 code: %q\n S3 message: %q",
				err, awsErrorCode(err), awsErrorMessage(err))
		}
		if !errors.Is(err, kbs3.ErrBucketIncompatible) {
			t.Errorf("the refusal does not wrap s3.ErrBucketIncompatible: %v", err)
		}
	})
}

// TestScopeTransitionsLandInTheScopeLogIntegration covers the other half of what
// this backend writes: the scope log that makes a gap in the records explicable
// rather than merely empty.
//
// It asserts the published layout — format=jsonl-v1/scopes/date=<YYYY-MM-DD>/ —
// and the line shape, read as a reader would read it. s3.Decode is deliberately
// not used: it is typed to sink.Record, so a scope line decoded through it would
// succeed while filling in almost nothing, and the test would pass having asserted
// the wrong thing. The documented DuckDB recipes glob the two trees separately for
// the same reason.
func TestScopeTransitionsLandInTheScopeLogIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), itTimeout)
	defer cancel()

	bucket := newBucket(ctx, t, false)
	w := kbs3.NewWriter(newITStore(t), kbs3.Config{
		Bucket:  bucket.name,
		Prefix:  itPrefix,
		Workers: 1,
	}, itMetrics())

	event := sink.ScopeEvent{
		Action: sink.ScopeActionStarted,
		Scope: sink.ScopeFilter{
			ClusterID: itClusterID,
			APIGroup:  "apps",
			Kind:      "Deployment",
			Namespace: "minio-it",
		},
		APIVersion: "v1",
		RuleRef:    "streamrule/minio-it/archive",
		TS:         itEpoch,
	}

	runWriter(t, w, func() {
		if err := w.EnqueueScopeEvent(ctx, event); err != nil {
			t.Fatalf("enqueue a scope transition: %v", err)
		}
		eventually(t, itTimeout, "the scope object to reach the bucket", func() bool {
			return len(bucket.keys(ctx, t)) == 1
		})
	})

	objects := bucket.objects(ctx, t)
	if len(objects) != 1 {
		t.Fatalf("bucket holds %d objects, want exactly 1: %v", len(objects), bucket.keys(ctx, t))
	}
	obj := objects[0]

	wantPrefix := itPrefix + "/format=jsonl-v1/scopes/date=2026-03-14/"
	if !strings.HasPrefix(obj.key, wantPrefix) || !strings.HasSuffix(obj.key, ".jsonl.zst") {
		t.Errorf("the scope object landed at %q, want a key under %q ending in %q",
			obj.key, wantPrefix, ".jsonl.zst")
	}
	// A scope object must not be reachable from a records glob, and vice versa:
	// the two have different line shapes, so a reader that mixed them would have
	// to infer a schema per line.
	if isRecordKey(obj.key) {
		t.Errorf("the scope object %q also matches the records glob", obj.key)
	}

	lines := jsonlLines(t, obj.body)
	if len(lines) != 1 {
		t.Fatalf("the scope object holds %d lines, want 1", len(lines))
	}
	var got struct {
		TS         time.Time `json:"ts"`
		ClusterID  string    `json:"cluster_id"`
		APIGroup   string    `json:"group"`
		APIVersion string    `json:"version"`
		Kind       string    `json:"kind"`
		Namespace  string    `json:"namespace"`
		Action     string    `json:"action"`
		RuleRef    string    `json:"rule_ref"`
	}
	if err := json.Unmarshal(lines[0], &got); err != nil {
		t.Fatalf("decode the scope line %q: %v", lines[0], err)
	}
	want := fmt.Sprintf("%s %s/%s %s %s %s", event.Action, event.Scope.APIGroup, event.Scope.Kind,
		event.Scope.Namespace, event.RuleRef, event.TS.UTC())
	have := fmt.Sprintf("%s %s/%s %s %s %s", got.Action, got.APIGroup, got.Kind,
		got.Namespace, got.RuleRef, got.TS.UTC())
	if have != want {
		t.Errorf("the scope line reads %q, want %q", have, want)
	}
	if got.ClusterID != itClusterID || got.APIVersion != event.APIVersion {
		t.Errorf("the scope line carries cluster_id=%q version=%q, want %q and %q",
			got.ClusterID, got.APIVersion, itClusterID, event.APIVersion)
	}
}
