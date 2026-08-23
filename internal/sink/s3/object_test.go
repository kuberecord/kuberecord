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

// The accumulating encoder. Its one obligation towards the rest of the world is
// that it produces the same object the one-shot Encode does, since the format is
// a public contract (D15) and two encoders that disagreed would mean the archive
// had two shapes depending on which path wrote it.
package s3

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// buildObject runs a batch of records through the accumulating builder exactly as
// a worker would: one line at a time, then built, under the corpus prefix and a
// size limit no test batch comes near — the size trigger has tests of its own,
// and here it must not cut a batch short.
func buildObject(t *testing.T, records []sink.Record) Object {
	t.Helper()
	b, err := newObjectBuilder(defaultMaxObjectBytes)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}
	for i, rec := range records {
		line, lineErr := marshalRecordLine(rec)
		if lineErr != nil {
			t.Fatalf("marshalRecordLine(%d): %v", i, lineErr)
		}
		if appendErr := b.append(line, rec.ClusterID, rec.Timestamp); appendErr != nil {
			t.Fatalf("append(%d): %v", i, appendErr)
		}
	}
	obj, err := b.build(testPrefix)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return obj
}

// TestBuilderAndEncodeAgree is the anti-drift guarantee between the two encoders.
//
// They agree on everything that is part of the contract — the key, the content
// hash, and the records a reader gets back — because both write the same lines
// through the same line encoder and both hash the *uncompressed* payload. They do
// not agree on the compressed bytes, and must not be made to: Encode compresses
// in one pass and the builder streams, and the format explicitly puts the
// compressed representation outside the contract precisely so that the two can
// differ (and so a compressor upgrade cannot re-key a single object).
//
// If this test ever fails on Key or ContentHash, the two paths have drifted and
// the archive has two shapes. If it fails because the payloads became equal,
// nothing is wrong.
func TestBuilderAndEncodeAgree(t *testing.T) {
	records := corpus()

	oneShot, err := Encode(testPrefix, records)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	streamed := buildObject(t, records)

	if streamed.Key != oneShot.Key {
		t.Errorf("the two encoders disagree on the object key:\n builder: %s\n Encode:  %s", streamed.Key, oneShot.Key)
	}
	if streamed.ContentHash != oneShot.ContentHash {
		t.Errorf("the two encoders disagree on the content hash: %s vs %s", streamed.ContentHash, oneShot.ContentHash)
	}

	fromStreamed, err := Decode(streamed.Payload)
	if err != nil {
		t.Fatalf("Decode(builder payload): %v", err)
	}
	fromOneShot, err := Decode(oneShot.Payload)
	if err != nil {
		t.Fatalf("Decode(Encode payload): %v", err)
	}
	if !reflect.DeepEqual(fromStreamed, fromOneShot) {
		t.Error("the two encoders' objects decode to different records")
	}
	if !reflect.DeepEqual(fromStreamed, records) {
		t.Error("the builder's object does not decode back to the records it was built from")
	}
}

// TestBuilderIsDeterministic: the same records, built again, produce the same key
// *and* the same bytes. The key is what makes a retried PUT an overwrite; the
// bytes are what make that overwrite harmless.
func TestBuilderIsDeterministic(t *testing.T) {
	records := corpus()

	first := buildObject(t, records)
	second := buildObject(t, records)

	if first.Key != second.Key {
		t.Errorf("the same records produced two keys:\n %s\n %s", first.Key, second.Key)
	}
	if string(first.Payload) != string(second.Payload) {
		t.Errorf("the same records produced different bytes (%d and %d): an object rebuilt from the same "+
			"records must be the same object, or a replay writes a second copy",
			len(first.Payload), len(second.Payload))
	}
}

// TestBuilderReportsFullOnTheEncodedSize is the rotation trigger in isolation:
// full() must become true against the *compressed* length, and only once it has
// really reached the limit.
//
// The reading it is taken from lags by design (measuring it costs a flush, so the
// builder only refreshes when the limit could have been crossed — see
// objectFlushSlack), which is why this asserts the finished object is at or above
// the limit rather than exactly at it. Undershooting is the bug that matters: it
// would mean objects a fraction of the configured size, which is the small-file
// pattern the CRD's floor exists to prevent.
func TestBuilderReportsFullOnTheEncodedSize(t *testing.T) {
	const limit = 2048

	b, err := newObjectBuilder(limit)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}
	uncompressed := 0
	appended := 0
	for i := range 10000 {
		rec := writerRecord(i)
		line, lineErr := marshalRecordLine(rec)
		if lineErr != nil {
			t.Fatalf("marshalRecordLine: %v", lineErr)
		}
		if appendErr := b.append(line, rec.ClusterID, rec.Timestamp); appendErr != nil {
			t.Fatalf("append: %v", appendErr)
		}
		uncompressed += len(line)
		appended++
		if b.full() {
			break
		}
	}
	if !b.full() {
		t.Fatalf("the builder never reported full after %d records (%d bytes of JSONL) at a %d-byte limit",
			appended, uncompressed, limit)
	}

	obj, err := b.build(testPrefix)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(obj.Payload) < limit {
		t.Errorf("the object is %d encoded bytes at a %d-byte limit: full() fired before the limit was reached",
			len(obj.Payload), limit)
	}
	if uncompressed <= limit {
		t.Fatalf("only %d bytes of JSONL were needed to reach a %d-byte encoded limit, so this test cannot "+
			"tell the compressed reading from the uncompressed one", uncompressed, limit)
	}
	t.Logf("full at %d records: %d encoded bytes from %d bytes of JSONL", appended, len(obj.Payload), uncompressed)
}

// TestBuilderWithNoLimitIsNeverFull: a non-positive maxObjectBytes means "no size
// trigger", which is what a rotation configured by age alone amounts to. A builder
// that reported itself full at zero bytes would close an object per record.
func TestBuilderWithNoLimitIsNeverFull(t *testing.T) {
	b, err := newObjectBuilder(0)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}
	for i := range 200 {
		rec := writerRecord(i)
		line, lineErr := marshalRecordLine(rec)
		if lineErr != nil {
			t.Fatalf("marshalRecordLine: %v", lineErr)
		}
		if appendErr := b.append(line, rec.ClusterID, rec.Timestamp); appendErr != nil {
			t.Fatalf("append: %v", appendErr)
		}
		if b.full() {
			t.Fatalf("a builder with no size limit reported full after %d records", i+1)
		}
	}
}

// TestBuilderRefusesToStrayAcrossClusters: the first record fixes the object's
// cluster_id partition, and a record from another cluster is reported rather than
// filed under it. One operator process serves one cluster (Invariant 7), so this
// cannot happen today — but the failure it would cause is the silent kind, an
// object that looks correct and answers every query wrongly, so the builder says
// no and the worker rotates instead.
func TestBuilderRefusesToStrayAcrossClusters(t *testing.T) {
	b, err := newObjectBuilder(defaultMaxObjectBytes)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}

	first := writerRecord(0)
	line, err := marshalRecordLine(first)
	if err != nil {
		t.Fatalf("marshalRecordLine: %v", err)
	}
	if appendErr := b.append(line, first.ClusterID, first.Timestamp); appendErr != nil {
		t.Fatalf("append: %v", appendErr)
	}

	other := writerRecord(1)
	other.ClusterID = "staging-us-2"
	otherLine, err := marshalRecordLine(other)
	if err != nil {
		t.Fatalf("marshalRecordLine: %v", err)
	}
	appendErr := b.append(otherLine, other.ClusterID, other.Timestamp)
	if !errors.Is(appendErr, errClusterMismatch) {
		t.Fatalf("append of a foreign cluster's record returned %v, want errClusterMismatch", appendErr)
	}
	if b.records != 1 {
		t.Errorf("the refused record was counted anyway: the object holds %d records, want 1", b.records)
	}

	// The refusal must leave the object usable: the worker's response is to close
	// it and open another, so what it holds has to still be writable.
	obj, err := b.build(testPrefix)
	if err != nil {
		t.Fatalf("build after a refused append: %v", err)
	}
	decoded, err := Decode(obj.Payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Name != first.Name {
		t.Errorf("the object holds %d records (%v), want just the first", len(decoded), decoded)
	}
}

// TestBuilderRefusesAnEmptyClusterID: a record with no cluster_id would file the
// whole object under `cluster_id=`, which is a partition no query will ever look
// in and no reader can interpret. The writer refuses such a record at enqueue
// time; the builder refuses it too, so neither path can produce that object.
func TestBuilderRefusesAnEmptyClusterID(t *testing.T) {
	b, err := newObjectBuilder(defaultMaxObjectBytes)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}
	if appendErr := b.append([]byte("{}\n"), "", corpusEpoch); appendErr == nil {
		t.Fatal("the builder accepted a record with no cluster_id")
	}
}

// TestBuildRefusesAnEmptyObject: an object with no records in it would be
// permanently retained in an archive bucket for nothing, so building one is
// refused exactly as encoding one is.
func TestBuildRefusesAnEmptyObject(t *testing.T) {
	b, err := newObjectBuilder(defaultMaxObjectBytes)
	if err != nil {
		t.Fatalf("newObjectBuilder: %v", err)
	}
	if _, buildErr := b.build(testPrefix); !errors.Is(buildErr, errEmptyBatch) {
		t.Errorf("build of an empty object returned %v, want errEmptyBatch", buildErr)
	}
}

// TestBuilderPartitionsByTheFirstRecord: the object's date/hour partition comes
// from the first record it took, not from whichever record happens to be latest
// and not from the wall clock. It is the same rule Encode follows, and it is what
// makes an object's partition agree with the rotation deadline measured from that
// same first record.
func TestBuilderPartitionsByTheFirstRecord(t *testing.T) {
	first := writerRecord(0)
	later := writerRecord(1)
	later.Timestamp = corpusEpoch.Add(48 * time.Hour)

	obj := buildObject(t, []sink.Record{first, later})
	if want := "date=2026-03-14/hour=15/"; !strings.Contains(obj.Key, want) {
		t.Errorf("key %s is not partitioned by the first record's instant (%s)", obj.Key, want)
	}
}
