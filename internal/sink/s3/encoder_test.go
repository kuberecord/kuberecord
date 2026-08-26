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

package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/sink"
)

const (
	// testClusterID and testPrefix stand in for an S3Sink's resolved cluster
	// identity and spec.prefix. The prefix is a two-segment one on purpose: a
	// single segment would not catch a builder that joined only the last one.
	testClusterID = "prod-eu-1"
	testPrefix    = "audit/kuberecord"
)

// corpusEpoch is the instant every corpus record is dated from. It is a fixed,
// long-past UTC instant rather than time.Now() for two independent reasons: the
// partition assertions below must pin a literal date= and hour=, and a wall-clock
// timestamp would make a partition derived from *now* indistinguishable from one
// derived from the record — which is exactly the bug the layout must not have.
//
// It is constructed in time.UTC because the round-trip assertions compare with
// reflect.DeepEqual: encoding/json renders a time.Time as RFC 3339 and parses it
// back into UTC when the offset is "Z", so a UTC corpus round-trips to a
// structurally identical value. A record carrying a zoned timestamp is covered
// separately by TestPartitionUsesFirstRecordTimestampInUTC, which asserts on the
// key rather than on struct equality.
var corpusEpoch = time.Date(2026, 3, 14, 15, 9, 26, 535897932, time.UTC)

// baseRecord is a fully populated Added record: every field non-empty, so each
// corpus variant below can empty exactly the fields its case is about and
// nothing else.
func baseRecord() sink.Record {
	return sink.Record{
		Timestamp:       corpusEpoch,
		ClusterID:       testClusterID,
		EventType:       "Added",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "kuberecord-system",
		Name:            "controller-manager",
		UID:             "0f6a9c1e-6f5a-4a2e-9a1d-2c0f7b8e5d31",
		ResourceVersion: "104211",
		Labels:          map[string]string{"app": "kuberecord", "tier": "control-plane"},
		Actors:          []string{"kube-controller-manager", "kubectl-client-side-apply"},
		Data:            `{"apiVersion":"apps/v1","kind":"Deployment","spec":{"replicas":2}}`,
		Diff:            "",
		SHA256:          "9f2c1b6d0a4e8f37c5d21e9b7a3f4c8d6e0b2a19c7d5f3e18b4a6c2d0f9e7b53",
	}
}

// corpus is the fidelity corpus the AC calls for: an empty diff (every record but
// the modified one), an empty data (the modified and deleted ones), multi-byte
// UTF-8 in names and label values, and a record carrying the redaction sentinel.
// The markup record is here for a narrower reason — it is the one that fails if
// the encoder's HTML escaping is ever turned back on.
func corpus() []sink.Record {
	added := baseRecord()

	// A Modified record carries a diff and no full state: the pair of empty-data
	// and non-empty-diff is the shape the encoder must not "tidy up".
	modified := baseRecord()
	modified.Timestamp = corpusEpoch.Add(90 * time.Second)
	modified.EventType = "Modified"
	modified.ResourceVersion = "104884"
	modified.Data = ""
	modified.Diff = `[{"op":"replace","path":"/spec/replicas","value":3}]`
	modified.SHA256 = "1a7b3c5d9e2f48061b8d4a6c0e3f7295d1c8b0a462e9f37d5c1b8a604f2e9d73"

	// A Deleted record is the emptiest legal shape: no data, no diff, no sha256,
	// no actors (there is no live object left to attribute the change to) and, for
	// this variant, no labels either. It is the case that proves an absent
	// collection is still a present field.
	deleted := baseRecord()
	deleted.Timestamp = corpusEpoch.Add(4 * time.Minute)
	deleted.EventType = "Deleted"
	deleted.ResourceVersion = "105330"
	deleted.Labels = nil
	deleted.Actors = nil
	deleted.Data = ""
	deleted.Diff = ""
	deleted.SHA256 = ""

	// Multi-byte UTF-8 throughout the identity and the labels. Kubernetes would
	// not admit most of these names, but the encoder's job is to be byte-faithful
	// to whatever the pipeline hands it, not to re-validate the API server.
	unicodeRecord := baseRecord()
	unicodeRecord.Timestamp = corpusEpoch.Add(11 * time.Second)
	unicodeRecord.Namespace = "名前空間-производство"
	unicodeRecord.Name = "приложение-日本語-🚀-ünïcödé"
	unicodeRecord.Labels = map[string]string{"команда": "плат-форма", "所有者": "監査チーム"}
	unicodeRecord.Actors = []string{"ünïcödé-manager", "監査-controller"}
	unicodeRecord.Data = `{"metadata":{"annotations":{"note":"héllo — wörld ✅"}}}`

	// A record whose data has been through the redaction pass. The archive is the
	// longest-lived copy of any record, so "the sentinel survives verbatim" is the
	// property that says a redacted value stayed redacted on its way to disk.
	redacted := baseRecord()
	redacted.Timestamp = corpusEpoch.Add(21 * time.Second)
	redacted.Name = "redacted-deployment"
	redacted.Data = `{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"TOKEN","value":"` + pipeline.RedactionSentinel + `"}]}]}}}}`

	// Characters encoding/json escapes by default. With SetEscapeHTML(true) these
	// would appear as \u003c, \u003e and \u0026 on the wire; the assertion that
	// they do not is in TestEncodedLinesAreNotHTMLEscaped.
	markup := baseRecord()
	markup.Timestamp = corpusEpoch.Add(33 * time.Second)
	markup.Name = "markup-deployment"
	markup.Data = `{"note":"a < b && c > d"}`

	return []sink.Record{added, modified, deleted, unicodeRecord, redacted, markup}
}

// decodePayload decompresses an encoded object back to its raw JSONL. Tests that
// assert on the wire format need the uncompressed bytes, and going through the
// package's own shared decoder keeps them honest about what a reader sees.
func decodePayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	jsonl, err := zstdDecoder.DecodeAll(payload, nil)
	if err != nil {
		t.Fatalf("decompress payload: %v", err)
	}
	return jsonl
}

// payloadLines splits a JSONL payload into its lines, asserting the trailing
// newline the format promises. Every line is terminated, including the last, so
// two objects can be concatenated without gluing two records together.
func payloadLines(t *testing.T, jsonl []byte) [][]byte {
	t.Helper()
	if len(jsonl) == 0 || jsonl[len(jsonl)-1] != '\n' {
		t.Fatalf("payload is not newline-terminated (%d bytes)", len(jsonl))
	}
	var lines [][]byte
	for line := range bytes.SplitSeq(bytes.TrimSuffix(jsonl, []byte("\n")), []byte("\n")) {
		lines = append(lines, line)
	}
	return lines
}

// TestEncodeDecodeRoundTrip is the fidelity proof: every record in the corpus
// survives encode → decode structurally unchanged, both as one mixed batch and
// on its own.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	records := corpus()

	obj, err := Encode(testPrefix, records)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(obj.Payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("batch round-trip lost fidelity:\n got: %#v\nwant: %#v", got, records)
	}

	for i, record := range records {
		t.Run(record.EventType+"/"+record.Name, func(t *testing.T) {
			single, encErr := Encode(testPrefix, []sink.Record{record})
			if encErr != nil {
				t.Fatalf("Encode record %d: %v", i, encErr)
			}
			back, decErr := Decode(single.Payload)
			if decErr != nil {
				t.Fatalf("Decode record %d: %v", i, decErr)
			}
			if len(back) != 1 {
				t.Fatalf("expected 1 record back, got %d", len(back))
			}
			if !reflect.DeepEqual(back[0], record) {
				t.Fatalf("record %d lost fidelity:\n got: %#v\nwant: %#v", i, back[0], record)
			}
		})
	}
}

// TestEncodeIsDeterministic pins the property the whole idempotency design rests
// on: the same batch encodes to the same bytes and the same key, every time. A
// separately built copy of the batch is used for the second encode so the result
// cannot come from the encoder happening to see the identical slice header.
func TestEncodeIsDeterministic(t *testing.T) {
	first, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("first Encode: %v", err)
	}
	second, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("second Encode: %v", err)
	}

	if first.Key != second.Key {
		t.Errorf("key is not deterministic:\n first: %s\nsecond: %s", first.Key, second.Key)
	}
	if first.ContentHash != second.ContentHash {
		t.Errorf("content hash is not deterministic: %s vs %s", first.ContentHash, second.ContentHash)
	}
	if !bytes.Equal(first.Payload, second.Payload) {
		t.Errorf("payload is not deterministic: %d vs %d bytes", len(first.Payload), len(second.Payload))
	}
}

// TestContentHashCoversUncompressedPayload proves the hash is taken over the
// JSONL and not over the compressed bytes. The distinction is invisible until a
// compressor version or level changes, at which point hashing the compressed form
// would re-key every object and turn every retry into a duplicate — so it is
// asserted rather than trusted to the comment on Encode.
func TestContentHashCoversUncompressedPayload(t *testing.T) {
	obj, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	jsonl := decodePayload(t, obj.Payload)
	wantUncompressed := sha256.Sum256(jsonl)
	if got, want := obj.ContentHash, hex.EncodeToString(wantUncompressed[:]); got != want {
		t.Errorf("content hash does not cover the uncompressed payload:\n got: %s\nwant: %s", got, want)
	}

	compressed := sha256.Sum256(obj.Payload)
	if obj.ContentHash == hex.EncodeToString(compressed[:]) {
		t.Error("content hash covers the compressed bytes; it must cover the uncompressed JSONL (see Encode)")
	}
	if !strings.HasSuffix(obj.Key, obj.ContentHash+objectSuffix) {
		t.Errorf("key %q does not end in the content hash", obj.Key)
	}
}

// TestDistinctBatchesYieldDistinctKeys checks the other half of idempotency: a
// key collides only when the bytes are genuinely identical. The one-character
// label edit is the case that matters — a hash over a subset of the record, or a
// key derived from identity plus timestamp alone, would pass every other test
// here and silently overwrite one batch with another.
func TestDistinctBatchesYieldDistinctKeys(t *testing.T) {
	full := corpus()

	oneLabelChanged := corpus()
	oneLabelChanged[0].Labels = map[string]string{"app": "kuberecordX", "tier": "control-plane"}

	reordered := corpus()
	slices.Reverse(reordered)

	batches := map[string][]sink.Record{
		"full":              full,
		"one record fewer":  full[:len(full)-1],
		"one label changed": oneLabelChanged,
		"reordered":         reordered,
		"single record":     {baseRecord()},
	}

	seen := make(map[string]string, len(batches))
	for name, records := range batches {
		obj, err := Encode(testPrefix, records)
		if err != nil {
			t.Fatalf("Encode %s: %v", name, err)
		}
		if other, clash := seen[obj.Key]; clash {
			t.Errorf("batches %q and %q produced the same key %s", name, other, obj.Key)
		}
		seen[obj.Key] = name
	}
}

// TestPartitionUsesFirstRecordTimestampInUTC covers the three ways the partition
// could be derived wrongly: from the wall clock, from the timestamp's own
// location, and from a record other than the first.
func TestPartitionUsesFirstRecordTimestampInUTC(t *testing.T) {
	// Kathmandu is +05:45, so this instant is a *different date and hour* locally
	// than in UTC — and the offset is not a whole number of hours, which catches a
	// truncation that happens to work for +01:00.
	kathmandu := time.FixedZone("NPT", 5*3600+45*60)
	zoned := baseRecord()
	zoned.Timestamp = time.Date(2026, 1, 1, 0, 30, 0, 0, kathmandu) // 2025-12-31T18:45:00Z

	later := baseRecord()
	later.Timestamp = time.Date(2025, 12, 31, 23, 5, 0, 0, time.UTC)

	tests := []struct {
		name    string
		records []sink.Record
		want    string
	}{
		{
			name:    "zoned timestamp partitions by its UTC date and hour",
			records: []sink.Record{zoned},
			want:    "date=2025-12-31/hour=18",
		},
		{
			name:    "the first record decides, not the last",
			records: []sink.Record{zoned, later},
			want:    "date=2025-12-31/hour=18",
		},
		{
			name:    "reversing the batch moves the partition to the new first record",
			records: []sink.Record{later, zoned},
			want:    "date=2025-12-31/hour=23",
		},
		{
			name:    "a long-past record stays in its own partition, not today's",
			records: []sink.Record{baseRecord()},
			want:    "date=2026-03-14/hour=15",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := Encode(testPrefix, tc.records)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !strings.Contains(obj.Key, "/"+tc.want+"/") {
				t.Errorf("key %q does not carry partition %q", obj.Key, tc.want)
			}
			if now := time.Now().UTC().Format("date=2006-01-02"); strings.Contains(obj.Key, now) {
				t.Errorf("key %q carries the wall-clock date %q; the partition must come from the record", obj.Key, now)
			}
		})
	}
}

// TestObjectKeyLayout is the golden for the documented contract (D15). The
// expectations are written out in full rather than assembled from the same
// constants the builder uses, so a change to a segment name has to be made twice
// — once in the code and once here, deliberately.
func TestObjectKeyLayout(t *testing.T) {
	hash := strings.Repeat("ab", 32)

	tests := []struct {
		name   string
		prefix string
		ts     time.Time
		want   string
	}{
		{
			name:   "with a multi-segment prefix",
			prefix: testPrefix,
			ts:     time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC),
			want:   "audit/kuberecord/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=15/" + hash + ".jsonl.zst",
		},
		{
			name:   "without a prefix",
			prefix: "",
			ts:     time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC),
			want:   "format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=15/" + hash + ".jsonl.zst",
		},
		{
			name:   "single-digit hour is zero padded",
			prefix: testPrefix,
			ts:     time.Date(2026, 3, 14, 5, 0, 0, 0, time.UTC),
			want:   "audit/kuberecord/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=05/" + hash + ".jsonl.zst",
		},
		{
			name:   "midnight is hour 00, not 24",
			prefix: testPrefix,
			ts:     time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC),
			want:   "audit/kuberecord/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=00/" + hash + ".jsonl.zst",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := objectKey(tc.prefix, testClusterID, tc.ts, hash)
			if got != tc.want {
				t.Errorf("objectKey:\n got: %s\nwant: %s", got, tc.want)
			}
			if strings.HasPrefix(got, "/") {
				t.Errorf("key %q starts with a slash", got)
			}
			if strings.Contains(got, "//") {
				t.Errorf("key %q contains an empty segment", got)
			}
		})
	}
}

// TestEncodedLinesCarryEveryRecordFieldInOrder is the executable form of "fields
// named exactly after Record's JSON tags, no reordering, no omission". The
// expectation is reflected off sink.Record itself, so adding a field to the
// logical contract without deciding what the archive does with it fails here
// rather than shipping an object whose lines have two different shapes.
func TestEncodedLinesCarryEveryRecordFieldInOrder(t *testing.T) {
	recordType := reflect.TypeFor[sink.Record]()
	want := make([]string, 0, recordType.NumField())
	for i := range recordType.NumField() {
		name, _, _ := strings.Cut(recordType.Field(i).Tag.Get("json"), ",")
		if name == "" {
			t.Fatalf("sink.Record field %s has no json tag name", recordType.Field(i).Name)
		}
		want = append(want, name)
	}

	obj, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	lines := payloadLines(t, decodePayload(t, obj.Payload))
	if len(lines) != len(corpus()) {
		t.Fatalf("expected one line per record, got %d lines for %d records", len(lines), len(corpus()))
	}

	for i, line := range lines {
		got := jsonKeysInOrder(t, line)
		if !slices.Equal(got, want) {
			t.Errorf("line %d fields:\n got: %v\nwant: %v", i, got, want)
		}
	}
}

// jsonKeysInOrder returns one JSON object's member names in the order they appear
// on the wire. It walks the token stream rather than unmarshalling into a map,
// because a map would answer "which fields are present" while discarding the
// ordering half of the contract.
func jsonKeysInOrder(t *testing.T, line []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	open, err := dec.Token()
	if err != nil {
		t.Fatalf("read opening token: %v", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		t.Fatalf("line does not open with '{': %v", open)
	}

	var keys []string
	for dec.More() {
		key, keyErr := dec.Token()
		if keyErr != nil {
			t.Fatalf("read member name: %v", keyErr)
		}
		name, ok := key.(string)
		if !ok {
			t.Fatalf("member name is not a string: %v", key)
		}
		keys = append(keys, name)

		var value json.RawMessage
		if valErr := dec.Decode(&value); valErr != nil {
			t.Fatalf("skip value of %q: %v", name, valErr)
		}
	}
	return keys
}

// TestEmptyFieldsArePresentNotOmitted checks the reader-facing half of the same
// promise from the other direction: the emptiest record in the corpus still
// carries every field, with the empty strings spelled "" and the absent
// collections spelled null rather than dropped. A reader can therefore project
// any field of any line without inferring a schema first.
func TestEmptyFieldsArePresentNotOmitted(t *testing.T) {
	deleted := baseRecord()
	deleted.Labels = nil
	deleted.Actors = nil
	deleted.Data = ""
	deleted.Diff = ""
	deleted.SHA256 = ""

	obj, err := Encode(testPrefix, []sink.Record{deleted})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	line := payloadLines(t, decodePayload(t, obj.Payload))[0]

	for _, want := range []string{`"data":""`, `"diff":""`, `"sha256":""`, `"labels":null`, `"actors":null`} {
		if !bytes.Contains(line, []byte(want)) {
			t.Errorf("line does not contain %s:\n%s", want, line)
		}
	}
}

// TestEncodedLinesAreNotHTMLEscaped pins SetEscapeHTML(false). The bytes are read
// by query engines and by people, never embedded in a page, and \u003c noise
// inside a data payload makes both jobs harder.
func TestEncodedLinesAreNotHTMLEscaped(t *testing.T) {
	markup := baseRecord()
	markup.Data = `{"note":"a < b && c > d"}`

	obj, err := Encode(testPrefix, []sink.Record{markup})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	line := payloadLines(t, decodePayload(t, obj.Payload))[0]

	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if bytes.Contains(line, []byte(escaped)) {
			t.Errorf("line carries HTML-escaped %s:\n%s", escaped, line)
		}
	}
	if !bytes.Contains(line, []byte("a < b && c > d")) {
		t.Errorf("line does not carry the markup verbatim:\n%s", line)
	}
}

// TestEncodeRejectsUnusableBatches covers the inputs that would produce a
// correct-looking object filed under the wrong cluster, or an object holding
// nothing at all. None can happen while one operator serves one cluster
// (Invariant 7); all of them fail loudly rather than silently (Invariant 4).
func TestEncodeRejectsUnusableBatches(t *testing.T) {
	emptyCluster := baseRecord()
	emptyCluster.ClusterID = ""

	otherCluster := baseRecord()
	otherCluster.ClusterID = "staging-us-2"

	tests := []struct {
		name    string
		records []sink.Record
		want    string
	}{
		{name: "no records", records: nil, want: "empty batch"},
		{name: "empty slice", records: []sink.Record{}, want: "empty batch"},
		{name: "empty cluster_id", records: []sink.Record{emptyCluster}, want: "empty cluster_id"},
		{name: "mixed cluster_ids", records: []sink.Record{baseRecord(), otherCluster}, want: "mixes cluster_ids"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := Encode(testPrefix, tc.records)
			if err == nil {
				t.Fatalf("expected an error, got object %s", obj.Key)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if obj.Key != "" || obj.Payload != nil {
				t.Errorf("a rejected batch must yield a zero Object, got %+v", obj)
			}
		})
	}
}

// TestDecodeRejectsCorruptPayloads asserts a damaged object is reported as
// damaged. A decoder that returned the records it managed to read would make a
// truncated archive indistinguishable from a short one.
func TestDecodeRejectsCorruptPayloads(t *testing.T) {
	obj, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	badJSON, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	badJSON.Payload = zstdEncoder.EncodeAll([]byte("{\"timestamp\": not json}\n"), nil)

	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "not a zstd frame", payload: []byte("plain JSONL, uncompressed\n"), want: "not a zstd frame"},
		{name: "empty payload", payload: nil, want: "not a zstd frame"},
		{name: "truncated frame", payload: obj.Payload[:len(obj.Payload)/2], want: "decompress object payload"},
		{name: "malformed line", payload: badJSON.Payload, want: "decode record 0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			records, decErr := Decode(tc.payload)
			if decErr == nil {
				t.Fatalf("expected an error, got %d records", len(records))
			}
			if records != nil {
				t.Errorf("a failed decode must return no records, got %d", len(records))
			}
			if !strings.Contains(decErr.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", decErr, tc.want)
			}
		})
	}
}

// TestEncodeIsSafeForConcurrentUse exercises the package-level codec from many
// goroutines at once, which is the whole justification for it being shared: under
// -race this fails if the singleton is not in fact concurrency-safe, and the
// equality assertion fails if two concurrent encodes could ever interleave into
// each other's output.
func TestEncodeIsSafeForConcurrentUse(t *testing.T) {
	const goroutines = 32

	want, err := Encode(testPrefix, corpus())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	results := make([]Object, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			results[i], errs[i] = Encode(testPrefix, corpus())
		})
	}
	wg.Wait()

	for i := range goroutines {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if results[i].Key != want.Key {
			t.Fatalf("goroutine %d produced key %s, want %s", i, results[i].Key, want.Key)
		}
		if !bytes.Equal(results[i].Payload, want.Payload) {
			t.Fatalf("goroutine %d produced a different payload (%d vs %d bytes)", i, len(results[i].Payload), len(want.Payload))
		}
	}
}
