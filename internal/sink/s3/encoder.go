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

// Package s3 implements kuberecord's S3 / MinIO archive backend: a Writer-only
// sink (D12) that streams sink.Records to an object store as rotated,
// zstd-compressed JSON Lines objects.
//
// This file defines the backend's *physical format* — how a batch of Records
// becomes one object's bytes and one object's key — in its one-shot, reference
// form; object.go carries the accumulating form the write path uses, and the two
// agree on every part of the format that is a contract. Per D15 that format is its
// own versioned public contract, on its own timeline and entirely separate from
// the frozen ClickHouse schema, which is why the version is stamped into the
// object key itself (format=jsonl-v1) rather than living only in the S3Sink CR:
// a reader handed nothing but a bucket must be able to tell which contract
// produced the bytes it is looking at.
package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The object key layout, spelled once. Every segment below is part of the
// public contract (D15) and none of them may change meaning under jsonl-v1.
//
//	<prefix>/format=jsonl-v1/cluster_id=<id>/date=<YYYY-MM-DD>/hour=<HH>/<content-hash>.jsonl.zst
//
// The Hive-style `key=value` segments are what let DuckDB's hive_partitioning
// and Athena's partition projection read the path as columns rather than as an
// opaque string, so a time-window query prunes objects it never has to fetch.
//
// Partitioning is by cluster, date and hour *only*. Kind is deliberately absent:
// a batch mixes kinds by construction (one workqueue drains every watched GVK),
// so a kind= segment would force one batch to be split across as many objects as
// it has kinds — fighting rotation, multiplying small files, and making the
// per-object overhead that rotation exists to amortise worse the busier the
// cluster gets.
const (
	// formatPartition names the version of this contract. It is the "jsonl-v1"
	// half of the S3Sink CR's `format: jsonl-v1-zstd`; the compression half is
	// carried by objectSuffix instead, because compression is a property of the
	// bytes on disk rather than of the record layout.
	formatPartition = "format=jsonl-v1"

	// objectSuffix is the extension every object carries. Both halves are load
	// bearing for readers: `.jsonl` tells a query engine to expect one JSON value
	// per line, `.zst` tells it (and the AWS CLI, and DuckDB's glob) to
	// decompress first.
	objectSuffix = ".jsonl.zst"

	// partitionDateLayout and partitionHourLayout render the two time partitions.
	// The hour is zero-padded ("05", not "5") so the partitions sort
	// lexicographically in the same order they sort chronologically — which is
	// what makes a prefix listing of a bucket usable as a time-ordered scan.
	partitionDateLayout = "2006-01-02"
	partitionHourLayout = "15"
)

// zstdMagic is the 4-byte magic number that opens every zstd frame (RFC 8878).
// Decode checks for it before invoking the decoder so a truncated, plaintext or
// mislabelled object fails with a message naming the actual problem instead of
// whatever the decompressor makes of the bytes.
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// zstdEncoder and zstdDecoder are process-wide singletons, the same discipline
// internal/pipeline's hash-cache codecs follow: zstd's EncodeAll and DecodeAll
// are explicitly safe for concurrent use, so every writer worker shares one pair
// rather than allocating a codec per object — which at the ceiling of 64 workers
// rotating 64Mi objects is the difference between a fixed cost and a per-object
// one.
//
// SpeedDefault matches the pipeline's level, and the choice is deliberately not
// part of the public contract: because the content hash is taken over the
// *uncompressed* payload (see Encode), the level can be raised or lowered in a
// later release without invalidating a single key or breaking a single reader.
//
// Both are constructed with valid options, so the errors are not expected; a nil
// codec (should construction ever fail) is reported as an error by Encode and
// Decode rather than panicking on the write path.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// errEmptyBatch rejects an encode of no records. It is a caller bug rather than
// a runtime condition — rotation never closes an empty object — but a zero-record
// PUT would still create a real, permanently retained object in an archive
// bucket, so it is refused rather than written.
var errEmptyBatch = errors.New("s3: refusing to encode an empty batch: an object with no records would be permanently retained for nothing")

// errNoEncoder and errNoDecoder report an unavailable shared codec. See the
// zstdEncoder/zstdDecoder comment: unreachable in practice, surfaced rather than
// ignored so it can never degrade into a silently unwritten archive (Invariant 4).
var (
	errNoEncoder = errors.New("s3: zstd encoder unavailable")
	errNoDecoder = errors.New("s3: zstd decoder unavailable")
)

// Object is one encoded S3 object: the bytes to PUT and the key to PUT them
// under.
//
// It carries the content hash separately even though the key already ends in it,
// because the hash is the idempotency token the writer logs and reasons about —
// re-deriving it by string-surgery on the key at every call site is exactly the
// kind of parsing that drifts from the layout it parses.
type Object struct {
	// Key is the full object key, prefix included and with no leading slash. See
	// the layout constants above for the contract it follows.
	Key string

	// ContentHash is the hex SHA-256 of the *uncompressed* JSONL payload, and is
	// the final path segment of Key.
	ContentHash string

	// Payload is the zstd-compressed JSONL, ready to PUT unchanged.
	Payload []byte
}

// Encode serializes a batch of records into one archive object, in one pass over
// a finished batch.
//
// It is the format's reference form: given the records, this is the object. The
// write path does not use it, and that is deliberate rather than an oversight —
// rotation is specified on the encoded size, so a worker has to know how large
// the object has become while it is still filling it, which a one-shot encoder
// cannot answer. It therefore builds the same object incrementally (see
// object.go), and the two agree on the key and the content hash because both hash
// the uncompressed payload. Do not "unify" them by making the writer buffer a
// whole batch and call this: that is what would put an object's worth of
// uncompressed JSON in each worker's memory.
//
// The record layout is a direct serialization of sink.Record (D9): one JSON
// object per line, terminated by "\n" including the final line, with fields
// named by Record's own JSON tags, in Record's own field order, and with every
// field always present. Nothing is dropped when empty — an unmodified object's
// `diff` is present-and-empty rather than absent — so a reader can treat every
// line as having the same shape and needs no schema inference across a glob of
// objects. Note that Record's tags are the logical contract's names, so `group`
// and `version` here are what ClickHouse's physical mapping spells `api_group`
// and `api_version`; the two backends map the same Record, they do not share a
// column vocabulary.
//
// Determinism is a property callers depend on, not a coincidence: the same batch
// encodes to byte-identical output, because Record's field order is fixed by its
// declaration, encoding/json emits map keys (Labels) in sorted order, and
// EncodeAll runs a single deterministic pass. That is what makes a retried PUT
// land on the same key with the same bytes, so no reader of the archive can see
// a duplicate: on an unversioned bucket the second PUT replaces the first, and
// on a versioned one it becomes the current version of the same key. It is this
// backend's answer to ClickHouse's ReplacingMergeTree, but synchronous and exact
// instead of eventual and best-effort. What a versioned bucket keeps underneath
// is docs/RETENTION.md's subject, not the key's.
//
// The content hash is taken over the **uncompressed** payload, deliberately. A
// compressor's output is not required to be bit-stable across library versions
// or option changes, so hashing the compressed bytes would silently re-key every
// object the first time klauspost/compress changed its internals or someone
// tuned the level — turning a retry into a duplicate, which is precisely the
// failure this hash exists to prevent. A future optimisation that "saves a pass"
// by hashing Payload instead would break that; it must not be made.
//
// The object's date/hour partition comes from the **first** record's timestamp,
// converted to UTC, never from the wall clock at encode time — an object
// assembled at 09:00 out of records stamped last Tuesday belongs in last
// Tuesday's partition. The first record is used rather than the batch minimum
// for two reasons: it is the same reference point rotation measures maxObjectAge
// from, so the object's partition and its rotation deadline agree; and it is
// stable against out-of-order arrivals, where a lone warm-up close-out dated
// from history would otherwise drag a whole object of fresh records into an
// ancient partition. One consequence belongs in every reader's mind and in the
// query recipes: an object may contain records from neighbouring hours, so a
// time-window scan must widen its partition range by the sink's maxObjectAge.
//
// A batch that mixes cluster IDs, or whose records carry none, is refused. One
// operator process serves exactly one cluster (Invariant 7) so neither can
// happen today, but the failure they would cause is the silent kind: records
// filed under another cluster's partition, correct-looking in every object we
// write and wrong in every query anyone runs against them.
func Encode(prefix string, records []sink.Record) (Object, error) {
	if len(records) == 0 {
		return Object{}, errEmptyBatch
	}
	if zstdEncoder == nil {
		return Object{}, errNoEncoder
	}

	clusterID := records[0].ClusterID
	if clusterID == "" {
		return Object{}, fmt.Errorf("s3: record 0 (%s) carries an empty cluster_id, which would file the whole object under cluster_id=", recordRef(records[0]))
	}

	// One pass validates and marshals: the encoder appends "\n" after each value,
	// so a stream of Encode calls over a shared buffer *is* the JSONL payload. It
	// is the shared lineEncoder, so this one-shot path and the writer's
	// accumulating one (object.go) cannot drift on how a line is rendered.
	var jsonl bytes.Buffer
	enc := lineEncoder(&jsonl)
	for i := range records {
		if records[i].ClusterID != clusterID {
			return Object{}, fmt.Errorf("s3: batch mixes cluster_ids: record 0 is %q, record %d (%s) is %q", clusterID, i, recordRef(records[i]), records[i].ClusterID)
		}
		if err := enc.Encode(records[i]); err != nil {
			return Object{}, fmt.Errorf("s3: marshal record %d (%s): %w", i, recordRef(records[i]), err)
		}
	}

	sum := sha256.Sum256(jsonl.Bytes())
	contentHash := hex.EncodeToString(sum[:])

	return Object{
		Key:         objectKey(prefix, clusterID, records[0].Timestamp, contentHash),
		ContentHash: contentHash,
		Payload:     zstdEncoder.EncodeAll(jsonl.Bytes(), nil),
	}, nil
}

// Decode reads an object's compressed payload back into the records it was
// encoded from. It is the executable statement of the read half of the contract:
// anything Encode writes, this reads back unchanged.
//
// It tolerates fields it does not know, because the format's change policy is
// additive — an object written by a newer operator must still be readable by an
// older one, minus the fields that did not exist when it was built. It does not
// tolerate a truncated frame or a malformed line: a partially decoded object is
// indistinguishable from a short one, and reporting it as success would turn
// corruption into silent data loss.
func Decode(payload []byte) ([]sink.Record, error) {
	jsonl, err := decompress(payload)
	if err != nil {
		return nil, err
	}

	var records []sink.Record
	dec := json.NewDecoder(bytes.NewReader(jsonl))
	for {
		var record sink.Record
		if decErr := dec.Decode(&record); decErr != nil {
			if errors.Is(decErr, io.EOF) {
				return records, nil
			}
			return nil, fmt.Errorf("s3: decode record %d of object: %w", len(records), decErr)
		}
		records = append(records, record)
	}
}

// objectKey builds the full key for an object. It is the only place the layout
// documented on the format constants above is constructed, so a reader wanting
// to know what kuberecord writes has exactly one function to read.
//
// ts is converted to UTC here rather than trusted to arrive that way: the
// pipeline stamps records with time.Now().UTC(), but a record reconstructed from
// a backend's history carries whatever location that driver returned, and a
// partition that depended on the operator pod's timezone would scatter one
// cluster's archive across two date= prefixes at every midnight.
//
// An empty prefix is a supported, ordinary configuration (a bucket dedicated to
// one cluster's archive) and simply contributes no segment — joining it blindly
// would open every key with a "/", which S3 accepts and every reader then has to
// deal with forever.
func objectKey(prefix, clusterID string, ts time.Time, contentHash string) string {
	utc := ts.UTC()
	segments := make([]string, 0, 6)
	if prefix != "" {
		segments = append(segments, prefix)
	}
	segments = append(segments,
		formatPartition,
		"cluster_id="+clusterID,
		"date="+utc.Format(partitionDateLayout),
		"hour="+utc.Format(partitionHourLayout),
		contentHash+objectSuffix,
	)
	return strings.Join(segments, "/")
}

// recordRef renders a record's identity for an error message, in the
// kind/namespace/name form Invariant 4 requires anomalies to carry. It is only
// ever used on a failure path, so it favours being readable in a log line over
// being cheap.
func recordRef(record sink.Record) string {
	return fmt.Sprintf("%s %s/%s", record.Kind, record.Namespace, record.Name)
}
