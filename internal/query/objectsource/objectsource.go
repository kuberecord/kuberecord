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

// Package objectsource is the read side of the archive: the seam through which a
// query engine discovers what objects exist and gets at their bytes.
//
// It is the counterpart of the write path's tiny object-store interface
// (internal/sink/s3.ObjectStore), and it is deliberately a *different* interface
// rather than an extension of it. The write path needs one method, PutObject, and
// says so in order to state the permission set an operator has to grant a sink: a
// single PutObject, no LIST, no GET, no DELETE. Reading the archive back needs
// exactly the two the sink is documented not to have, and it is done by a
// different consumer — the CLI, run by an engineer with their own credential —
// rather than by the operator. Two interfaces is what keeps that asymmetry a
// property of the code instead of a paragraph somebody has to remember.
//
// The interface exists at all so that a bucket and a directory are the same thing
// to the engine above it. That is the whole of the zero-infrastructure story: an
// evaluator syncs an archive to a laptop, or points the CLI at a mounted volume,
// and the same query code runs with no cloud credential anywhere. It is also what
// makes the local source the primary test vehicle — every property the engine
// depends on is asserted against a directory, and the object store is then one
// implementation of a contract rather than the thing the tests are written around.
//
// No implementation in this package links a cloud SDK, and nothing here may: the
// SDK is confined by the object-store-client-is-confined depguard rule to the two
// packages that exist to hold it, and the object-store implementation of this
// interface is one of them (internal/query/objectsource/awssource).
package objectsource

import (
	"context"
	"errors"
	"io"
)

// Object is one listed object: where it is and how large it is.
//
// Size is the *stored* size — the compressed bytes an implementation would have to
// transfer — because that is what the two things it is for both need. A scan
// estimate presented to a human before a 90-day query is an estimate of bytes off
// the wire, and a bounded-cost decision about how many objects to fetch in
// parallel is a decision about the same bytes. An uncompressed size would be a
// better predictor of decode time and is not available from a listing at all.
//
// There is deliberately no modification time. The instant an object belongs to is
// carried by its key, in the date= and hour= partitions the writer derives from
// the records themselves, and that is the only timestamp with a defined meaning
// here: an archive that has been synced, restored from a snapshot or copied
// between buckets has file times describing the copy rather than the capture. A
// field that is authoritative in one source and misleading in another is worse
// than an absent one, because pruning built on it would be quietly wrong exactly
// where the archive is oldest.
type Object struct {
	// Key is the object's full key, relative to the source's root — the bucket for
	// an object store, the directory for a local source — with no leading slash. It
	// is what Open takes.
	Key string

	// Size is the object's size in bytes as stored.
	Size int64
}

// ObjectIterator is a streaming cursor over a listing.
//
// It is a cursor and not a slice because a listing is genuinely large: a 90-day
// prefix of a busy cluster holds hundreds of thousands of keys, and a contract
// returning a slice would require every caller to hold all of them before it could
// discard the ones its time bound excludes. Streaming is also what lets a scan be
// interrupted — a caller that has seen enough closes the iterator and the
// implementation stops paging.
//
// The usage shape, and the error check after the loop, are the ones the read-plane
// contract already establishes for query.ChangeIterator:
//
//	it := src.List(ctx, prefix)
//	defer func() { _ = it.Close() }()
//	for it.Next() {
//	        consider(it.Object())
//	}
//	return it.Err()
//
// Skipping Err turns a listing that failed on its third page into a listing that
// looks complete and merely short — and a scan built on it reports "nothing
// changed" for a window it never actually read (Invariant 4).
//
// An iterator is not safe for concurrent use. The source that produced it is; see
// ObjectSource.
type ObjectIterator interface {
	// Next advances to the next object and reports whether one is available. It
	// returns false at the end of the listing and also on failure; Err
	// distinguishes them.
	Next() bool

	// Object returns the object Next just advanced to. It is only valid after Next
	// has returned true, and the value is the caller's to keep.
	Object() Object

	// Err returns the error that ended the listing, or nil if it was exhausted
	// normally. It is valid once Next has returned false.
	//
	// A cancelled or expired context surfaces here, as ctx.Err(), rather than as a
	// silently short listing.
	Err() error

	// Close releases whatever the iterator holds — an open directory, a page of
	// results, the paging state. It is safe to call at any point, including before
	// the listing is exhausted, which is the normal path whenever a caller has seen
	// enough. Calling it more than once is safe.
	Close() error
}

// ObjectSource is a listing-and-fetching view of one archive.
//
// A source is bound to its root when it is constructed — a bucket, or a directory
// — and neither method takes one, so a misdirected read is a property of the
// source somebody built rather than an invisible argument at the call site. This
// is the opposite of the write path's PutObjectInput, which carries its bucket
// precisely because one writer serves one sink and a misrouted object had to be
// visible in the request; a reader is pointed at one archive for its whole life.
//
// A source is safe for concurrent use by multiple goroutines. That is not
// incidental: a query over a wide window fetches objects in parallel under a
// concurrency cap, and it lists while it fetches, so every method here is reached
// from several goroutines at once. Iterators are per-goroutine.
type ObjectSource interface {
	// List streams the objects whose keys begin with prefix, in ascending byte
	// order of the whole key.
	//
	// prefix is a *byte prefix of the key*, not a path: "date=2026-03-1" matches
	// "date=2026-03-14/hour=07/….jsonl.zst", and an empty prefix lists everything
	// under the root. Object-store listings work this way, and a local source that
	// treated the prefix as a directory would answer a different question from the
	// one the engine's partition pruning asks — which is the divergence that would
	// make every test against the local source prove nothing about the bucket.
	//
	// The ordering is a guarantee rather than an accident. The writer zero-pads the
	// hour partition so that keys sort chronologically when they sort
	// lexicographically, and the engine relies on that to walk a window in time
	// order without first collecting it.
	//
	// A prefix that matches nothing yields an empty listing and a nil Err. It is
	// not an error: "there are no objects here" and "the archive could not be read"
	// are different answers and the caller has to be able to tell them apart
	// (Invariant 4).
	//
	// It returns an iterator rather than (iterator, error) because a listing's
	// first failure is not privileged: an implementation that pages cannot know its
	// prefix is unreadable until it asks, and a signature promising otherwise would
	// have every caller handle the same failure in two places. ctx bounds the whole
	// listing and is retained by the iterator.
	List(ctx context.Context, prefix string) ObjectIterator

	// Open returns the object's bytes as a stream. The caller closes it.
	//
	// It is a stream and not a []byte because objects are rotated at up to 64Mi and
	// are read by decoding them a line at a time; an implementation that buffered
	// the body to satisfy this signature would make peak memory scale with object
	// size at exactly the concurrency the caller chose for the opposite reason.
	//
	// A key that does not exist is ErrKeyNotFound, and a credential that may not
	// read it is ErrAccessDenied. Both are ordinary and both must be
	// distinguishable: the first happens whenever an object is aged out between a
	// listing and the fetch it fed, and the second is what an engineer gets for
	// reusing the operator's own write-only credential.
	Open(ctx context.Context, key string) (io.ReadCloser, error)

	// Close releases what the source holds open — a transport's pooled connections,
	// a directory handle. Readers handed out by Open are the caller's and are not
	// closed by it; a source must be closed after them.
	Close() error
}

// ErrKeyNotFound reports that the archive holds no object at that key.
//
// It is a sentinel because the interesting case is not a typo. An object aged out
// by a lifecycle rule, or deleted by a retention policy, between the listing that
// named it and the fetch that wanted it is an ordinary event in an archive that is
// doing its job — and the caller's correct response is to carry on with a recorded
// gap, not to abandon a timeline. Without a sentinel that decision would rest on
// matching an object store's error text, which is the sort of thing that works
// until the day the archive is a directory instead.
var ErrKeyNotFound = errors.New("objectsource: no object at that key")

// ErrAccessDenied reports that the credential this source was built with is not
// allowed to read what was asked for.
//
// It is called out separately from any other failure because it is the single most
// likely way a first CLI invocation fails, and because the reason is a good one:
// the sink's own credential is documented as needing PutObject and nothing else —
// no LIST, no GET — so an engineer who reuses it, which is the obvious thing to
// try, is refused on the very first listing. That deserves "this credential can
// write to the archive but not read it", and the CLI can only say so if the
// distinction survives the seam.
var ErrAccessDenied = errors.New("objectsource: not permitted to read the archive with this credential")
