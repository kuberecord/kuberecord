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

// This file is the *accumulating* half of the object format encoder.go defines
// one-shot. Encode takes a finished batch and produces its object; objectBuilder
// takes one record at a time and reports how large the object has become, which
// is what rotation needs and what a one-shot encoder cannot answer.
//
// The two produce the same object. Both write the same JSONL lines (through the
// one lineEncoder below), both hash the uncompressed payload, and both derive the
// key from that hash through objectKey — so for the same records they agree on
// Key and ContentHash exactly. Only the compressed representation differs
// (EncodeAll versus a streamed frame), and that is explicitly not part of the
// contract: see Encode's note on why the content hash covers the uncompressed
// payload precisely so the compressor's output need not be bit-stable.
// TestBuilderAndEncodeAgree is the executable form of that claim.
//
// Why a streamed frame at all, rather than accumulating records and encoding
// once: rotation is specified on the *encoded* size (spec.rotation.maxObjectBytes
// is documented as measured on the compressed payload), and S3WriterSpec.Workers
// promises a steady-state memory ceiling of workers × maxObjectBytes. Both are
// only true if what a worker holds is compressed bytes. Accumulating records and
// compressing at the end would hold the batch inflated — at a realistic 10:1 on
// Kubernetes JSON, ten times the ceiling the API documents — or, if it rotated on
// the uncompressed size instead, would emit objects a tenth of the size the
// operator asked for and reintroduce the small-file problem the 1Mi floor exists
// to prevent.

package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// objectFlushSlack is how much uncompressed input the builder writes between
// refreshes of its compressed-length reading.
//
// A refresh costs a zstd Flush, which is the only way to know how many bytes the
// encoder has actually emitted: the encoder writes to its output from its own
// goroutines and synchronises them on Flush and Close, so reading the output
// buffer's length at any other moment is both racy and short. A Flush also ends
// the current block, so refreshing after every record would cut the frame into
// record-sized blocks and cost compression ratio.
//
// 256Ki is a few natural zstd blocks: frequent enough that the reading is never
// far behind (rotation overshoots maxObjectBytes by at most the bytes written
// since the last refresh, and the refresh below is forced as soon as those could
// possibly carry the object over the limit), rare enough that the block
// structure of a 64Mi object is unchanged.
const objectFlushSlack = 256 << 10

// errClusterMismatch reports that a record does not belong in the object
// currently open: its cluster_id is not the one the object was opened with.
//
// It is a sentinel rather than a plain error because the caller's response is to
// *rotate*, not to fail — the record is perfectly writable, just not here (see
// Writer.appendToObject). One operator process serves one cluster (Invariant 7)
// so it cannot happen today; handling it by closing the object and opening
// another is what keeps that from becoming a silently mixed partition if it ever
// does.
var errClusterMismatch = errors.New("s3: record belongs to a different cluster_id than the object currently open")

// lineEncoder returns the JSON encoder every JSONL line in this format is
// written with, wherever it is written from.
//
// It exists so the line format has exactly one definition: Encode streams a
// whole batch through one of these, the writer's enqueue path renders a single
// line through another, and neither can drift into escaping HTML differently or
// forgetting the terminating newline (json.Encoder.Encode appends it, which is
// what makes a sequence of Encode calls a JSONL payload).
//
// HTML escaping is off because these bytes are read by humans and by query
// engines rather than embedded in a page, and `<` noise inside a `data`
// payload helps neither; the two forms decode identically.
func lineEncoder(w io.Writer) *json.Encoder {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc
}

// marshalRecordLine renders one record as its JSONL line, newline included.
//
// The writer calls this on the *enqueue* path rather than on a worker, mirroring
// how the ClickHouse writer renders its insert arguments at enqueue time: the
// worker then holds bytes rather than records, which is what makes a worker's
// memory footprint the compressed object it is building rather than the inflated
// batch behind it, and what lets an unencodable record be refused to its caller
// synchronously instead of settling as a failed write nobody can act on.
func marshalRecordLine(record sink.Record) ([]byte, error) {
	var line bytes.Buffer
	if err := lineEncoder(&line).Encode(record); err != nil {
		return nil, fmt.Errorf("s3: marshal record (%s): %w", recordRef(record), err)
	}
	return line.Bytes(), nil
}

// decompress unwraps an object's payload back to the JSONL bytes behind it.
//
// It is shared by both readers — Decode for record objects, decodeScopeObject for
// scope objects — because the framing is the format's, not either line shape's,
// and a second copy of these three checks is a second place for the magic-bytes
// guard to be forgotten.
func decompress(payload []byte) ([]byte, error) {
	if !bytes.HasPrefix(payload, zstdMagic) {
		return nil, fmt.Errorf("s3: payload is not a zstd frame (missing magic bytes, %d bytes)", len(payload))
	}
	if zstdDecoder == nil {
		return nil, errNoDecoder
	}
	jsonl, err := zstdDecoder.DecodeAll(payload, nil)
	if err != nil {
		return nil, fmt.Errorf("s3: decompress object payload (%d bytes): %w", len(payload), err)
	}
	return jsonl, nil
}

// objectBuilder accumulates JSONL lines into one object's compressed payload,
// tracking how large that payload has become so rotation can act on the encoded
// size.
//
// One builder is one object: it owns the frame, the encoder that produces it and
// the running digest of the uncompressed bytes behind it, and it is discarded
// once built. The encoder is per object rather than per worker deliberately — a
// rotation is seconds to minutes apart, so the allocation is immaterial, the live
// footprint is identical (a worker has one open object either way), and the
// frame's lifetime and its encoder's lifetime being the same object means no
// reset discipline exists to get wrong. The one-shot path keeps using the
// package-level encoder (see encoder.go).
type objectBuilder struct {
	// maxBytes is the encoded size at which this object is full. Zero or negative
	// means "never full by size", which only the age trigger then closes.
	maxBytes int64

	enc *zstd.Encoder
	// buf holds the compressed bytes the encoder has emitted. Its length is only
	// read straight after a Flush or Close — see objectFlushSlack.
	buf bytes.Buffer
	// digest is SHA-256 over the *uncompressed* JSONL, which is what the content
	// hash and therefore the object key are taken from (see Encode).
	digest hash.Hash

	// records is how many lines this object holds; emitted is the compressed
	// length as of the last refresh; pending is the uncompressed input written
	// since that refresh.
	records int
	emitted int64
	pending int64

	// clusterID and firstTS are captured from the first record and fix the
	// object's partition (see objectKey). firstTS is the *record's* timestamp,
	// never the wall clock: an object assembled now out of records stamped last
	// Tuesday belongs in last Tuesday's partition.
	clusterID string
	firstTS   time.Time
}

// newObjectBuilder opens an object that is full at maxBytes encoded bytes.
func newObjectBuilder(maxBytes int64) (*objectBuilder, error) {
	b := &objectBuilder{maxBytes: maxBytes, digest: sha256.New()}
	// Concurrency 1 for two independent reasons: it keeps the encoder's output
	// deterministic for a given sequence of writes (so rebuilding an object from
	// the same records reproduces its bytes, not merely its key), and it bounds
	// how far the emitted length can lag the input, which is what makes the
	// rotation reading above trustworthy. The level matches the one-shot path's.
	enc, err := zstd.NewWriter(&b.buf,
		zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("s3: open a zstd frame for a new object: %w", err)
	}
	b.enc = enc
	return b, nil
}

// append adds one already-rendered line to the object.
//
// It returns errClusterMismatch — and adds nothing — when the line belongs to a
// different cluster than the object was opened with, so the caller can rotate and
// place it in an object of its own. Any other error means this object can no
// longer be finished (see Writer.appendToObject, which then settles every job in
// it rather than writing a frame it could not complete).
func (b *objectBuilder) append(line []byte, clusterID string, ts time.Time) error {
	if clusterID == "" {
		return fmt.Errorf("s3: a record carries an empty cluster_id, which would file the whole object under cluster_id=")
	}
	if b.records == 0 {
		b.clusterID, b.firstTS = clusterID, ts
	} else if clusterID != b.clusterID {
		return fmt.Errorf("%w: object holds %q, record carries %q", errClusterMismatch, b.clusterID, clusterID)
	}

	if _, err := b.enc.Write(line); err != nil {
		return fmt.Errorf("s3: compress a record into the open object: %w", err)
	}
	// A hash writer never fails by contract; the result is still read rather than
	// discarded (Invariant 4), because a digest that silently stopped covering
	// part of the payload would re-key every object built after it.
	if _, err := b.digest.Write(line); err != nil {
		return fmt.Errorf("s3: hash a record into the open object: %w", err)
	}

	b.records++
	b.pending += int64(len(line))
	if b.refreshDue() {
		if err := b.enc.Flush(); err != nil {
			return fmt.Errorf("s3: flush the open object's frame: %w", err)
		}
		b.emitted = int64(b.buf.Len())
		b.pending = 0
	}
	return nil
}

// refreshDue reports whether the emitted length must be re-read before full can
// be trusted: either enough input has gone by to be worth a refresh, or — and
// this is the load-bearing half — the input written since the last refresh could
// on its own have carried the object over the limit. Compressed output is never
// materially larger than its input, so emitted+pending is an upper bound on the
// true compressed length, and refreshing whenever that bound reaches the limit
// means the object is never closed *late*.
func (b *objectBuilder) refreshDue() bool {
	return b.pending >= objectFlushSlack || (b.maxBytes > 0 && b.emitted+b.pending >= b.maxBytes)
}

// full reports whether the object has reached its encoded size limit. It is a
// plain comparison: append has already refreshed the reading whenever the limit
// could have been crossed.
func (b *objectBuilder) full() bool {
	return b.maxBytes > 0 && b.emitted >= b.maxBytes
}

// build closes the frame and returns the finished object.
//
// The builder is spent afterwards and must be discarded: the returned Payload
// aliases its buffer, which is what keeps a 64Mi object from being copied on its
// way to a PUT.
func (b *objectBuilder) build(prefix string) (Object, error) {
	if b.records == 0 {
		return Object{}, errEmptyBatch
	}
	if err := b.enc.Close(); err != nil {
		return Object{}, fmt.Errorf("s3: close the object's zstd frame: %w", err)
	}
	contentHash := hex.EncodeToString(b.digest.Sum(nil))
	return Object{
		Key:         objectKey(prefix, b.clusterID, b.firstTS, contentHash),
		ContentHash: contentHash,
		Payload:     b.buf.Bytes(),
	}, nil
}

// discard releases the frame of an object that will never be written, so an
// abandoned builder does not leave its encoder's goroutines and buffers waiting
// on a Close that never comes.
//
// It returns Close's error rather than swallowing it: the object is already lost,
// but a frame that cannot even be closed is the only evidence of a misbehaving
// encoder, and the caller logs it (Invariant 4).
func (b *objectBuilder) discard() error {
	if err := b.enc.Close(); err != nil {
		return fmt.Errorf("s3: close the frame of a discarded object: %w", err)
	}
	return nil
}
