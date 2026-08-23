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
	"context"
	"errors"
	"time"
)

// ObjectStore is the whole of the object-store API this backend uses: put one
// object, and release the transport.
//
// It is an interface, and a deliberately tiny one, for three reasons. It keeps
// the AWS SDK out of the write path's own package, so the shipped rotation,
// retry and commit logic is exercised in tests against a stand-in store rather
// than against a mocked HTTP client (the same discipline the ClickHouse writer
// follows with driver.Conn). It states, in one place, exactly what credentials a
// kuberecord S3Sink needs — a single PutObject and nothing else, no LIST, no
// GET, no DELETE — which is the permission set an operator has to grant. And it
// is what makes MinIO and S3 interchangeable here: everything that differs
// between them (endpoint, path style, region, credential chain) is resolved when
// the client is constructed, not when an object is written.
//
// The concrete AWS SDK v2 implementation lives beside this package rather than in
// it, in internal/sink/s3/awsstore, which is what keeps the claim above literally
// true: the write path's own package links no SDK, and the composition root
// assembles the two. Nothing in this file, or in the writer, may grow a second
// method without a matching widening of that documented permission set.
type ObjectStore interface {
	// PutObject writes one object, atomically: it is visible with all of its
	// bytes or not at all. There is no partial-success outcome to report, which
	// is why this backend needs no per-record isolation phase on the way out (see
	// Writer.flush).
	//
	// An implementation must be safe for concurrent use — every worker in the
	// pool shares one store — and must honour ctx's deadline, since that deadline
	// is how a shutdown drain is bounded.
	PutObject(ctx context.Context, in PutObjectInput) error

	// Close releases whatever the implementation holds open (an HTTP transport's
	// idle connections, typically). It is called exactly once, at the very end of
	// the writer's shutdown sequence, after every worker has stopped and every
	// other user of the store has finished — so an implementation may assume no
	// PutObject is in flight.
	//
	// It exists even though the AWS SDK's S3 client needs no teardown: a Writer
	// that owns a resource and never releases it, and a Writer whose resource
	// happens to need no releasing, must be the same shape from the outside, or
	// the shutdown ordering that the sink contract turns on ("no write attempt
	// after the close") has nothing to be asserted against.
	Close() error
}

// PutObjectInput is one object to write.
//
// It carries the bucket rather than binding the store to one, because the bucket
// is a property of the S3Sink CR (spec.bucket) while the client is built from
// the credential and endpoint settings around it. Keeping them separate means a
// misrouted object is a visible field on this struct rather than an invisible
// property of a client someone constructed elsewhere.
type PutObjectInput struct {
	// Bucket is the destination bucket, exactly as spec.bucket spells it.
	Bucket string

	// Key is the full object key, prefix included, with no leading slash. See
	// objectKey for the layout it follows (D15).
	Key string

	// Body is the object's bytes, ready to write unchanged. The writer never
	// hands over a body it has not finished building, so an implementation may
	// treat this as immutable and may retain it for the duration of the call.
	Body []byte

	// Retention is the per-object S3 Object Lock retention to apply, or nil to
	// write the object with none of its own (the bucket's default retention, if
	// it has one, then applies). It is set from the sink's spec.objectLock.
	Retention *Retention
}

// Retention is one object's S3 Object Lock retention, in the terms the S3 API
// itself uses.
//
// It carries an absolute instant rather than a number of days because the sink
// spec's retainDays has to be resolved against *some* clock, and doing it once
// when the object is built — rather than once per PUT attempt — is what keeps a
// retried PUT byte-for-byte and header-for-header identical to the attempt it
// repeats. A retry that moved the retention date would make the second attempt a
// different request, which is precisely what the deterministic object key exists
// to avoid.
type Retention struct {
	// Mode is "GOVERNANCE" or "COMPLIANCE", spelled as S3 spells it (the
	// v1alpha1.ObjectLockMode constants are the same two strings). It is a plain
	// string so this package does not import the API types: the physical form of
	// a retention header is the store implementation's business, and the CR's
	// enum has already validated the value.
	Mode string

	// RetainUntil is when the retention expires.
	RetainUntil time.Time
}

// ErrBucketIncompatible is what an ObjectStore implementation wraps around a
// refusal that is about the *shape* of what this sink writes rather than about
// reachability: the bucket cannot accept these objects and will not start being
// able to on its own.
//
// The case that exists today is an S3Sink configured with spec.objectLock
// against a bucket that has no Object Lock configuration — which on S3 can only
// be enabled at bucket creation, so no amount of waiting or retrying fixes it,
// and every write this sink ever attempts will fail the same way.
//
// It exists as a sentinel here, rather than as SDK error-code matching in the
// prober, because the classification is the only thing the sink contract cares
// about and the codes are the SDK's private vocabulary. Probe turns this into
// sink.ErrSchemaInvalid, which is the difference between telling an operator "the
// backend is down, wait" and "the backend is not the shape this sink writes,
// fix it" (see Writer.Probe). An implementation that failed to wrap such a
// refusal would have it reported as a transient outage forever.
var ErrBucketIncompatible = errors.New("s3: the bucket cannot accept the objects this sink is configured to write")
