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

// The read-plane conformance suite, run against a real object store.
//
// # Why it exists when the same suite already runs over a directory
//
// The engine's own package runs every property against a local archive, and that run
// proves the format and the semantics: the layout, the lines, the frames, the ordering,
// the reconstruction. What it cannot prove is that a bucket answers the listing
// questions the way a directory does. Task 10.1's suite pins that seam directly — same
// keys, both sources, listings compared — and this pins the other end of the same
// claim: the engine built on that seam passes the whole contract when the seam is a
// real store, over an archive seeded from the same history fixtures.
//
// Between them, "this backend passes conformance" means something for a deployment
// rather than only for a laptop. Neither run is sufficient alone, and the local one is
// the one that runs on every commit.
//
// # One bucket, one prefix per property
//
// The suite builds a fresh harness per property and seeds it once. A bucket per property
// would be two dozen CreateBucket calls; a prefix per property is the same isolation for
// the price of a string, and it exercises the prefix handling that an archive sharing a
// bucket with anything else depends on.

package awssource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// TestIntegrationQueryConformanceAgainstMinIO runs the read-plane contract against an
// archive in a real bucket.
func TestIntegrationQueryConformanceAgainstMinIO(t *testing.T) {
	ctx := t.Context()
	bucket := newITBucket(ctx, t)
	client := itClient(ctx, t, itSecretKey())

	// One prefix per property, so no property inherits another's objects.
	var prefixes atomic.Int64

	conformance.RunQuerySuite(t, func(t *testing.T) conformance.Harness {
		t.Helper()
		prefix := fmt.Sprintf("audit-%02d", prefixes.Add(1))

		source, err := New(ctx, itConfig(bucket, itSecretKey()))
		if err != nil {
			t.Fatalf("building a source for bucket %q: %v", bucket, err)
		}
		faulting := &faultingSource{inner: source}
		t.Cleanup(func() {
			if err := faulting.Close(); err != nil {
				t.Errorf("closing the source: %v", err)
			}
		})

		engine, err := objectsource.NewEngine(faulting, objectsource.Options{Prefix: prefix})
		if err != nil {
			t.Fatalf("building an engine over bucket %q: %v", bucket, err)
		}

		h := &bucketHarness{ctx: ctx, client: client, bucket: bucket, prefix: prefix, source: faulting}
		return conformance.Harness{
			Engine:         engine,
			Seed:           h.seed,
			SeedCorpus:     h.seedCorpus,
			SetStreamFault: h.setFault,
			Capabilities: conformance.DeclareCapabilities(
				// The same declaration the local run makes, written out again rather
				// than shared: two runs agreeing because they read one literal would
				// not notice the day the engine's own report changed.
				conformance.CapTimeBoundRequired,
			),
		}
	})
}

// bucketHarness seeds a history into a bucket and installs the suite's stream fault.
type bucketHarness struct {
	ctx    context.Context
	client *awss3.Client
	bucket string
	prefix string
	source *faultingSource
	layout *archivetest.Layout
}

// seed writes the history the property asserts against, as real objects.
//
// It returns only once every object is readable, which is what the suite's contract asks
// of a Seed: an object store's PUT is acknowledged when the object is durable, so there
// is nothing to wait for beyond the calls themselves.
func (h *bucketHarness) seed(history conformance.History) error {
	layout, err := archivetest.Write(h.put, h.prefix, history)
	if err != nil {
		return err
	}
	h.layout = layout
	return nil
}

// seedCorpus writes the shared agreement corpus into the bucket as real objects, one
// per flush, so that the cross-backend comparison is made against the objects a
// writer would actually have produced rather than against a shape assembled for the
// test.
func (h *bucketHarness) seedCorpus(corpus conformance.Corpus) error {
	layout, err := archivetest.WriteCorpus(h.put, h.prefix, corpus)
	if err != nil {
		return err
	}
	h.layout = layout
	return nil
}

// put is the fixture's own writer. The source under test lists and fetches, which is
// the whole of what reading an archive needs; widening it so a test could seed one
// would make that claim untrue.
func (h *bucketHarness) put(key string, body []byte) error {
	_, err := h.client.PutObject(h.ctx, &awss3.PutObjectInput{
		Bucket: aws.String(h.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}

// setFault refuses the objects holding every change after the nth, which is how a
// mid-stream failure is injected into a backend whose stream is a set of objects.
//
// It refuses them in the source rather than deleting them from the bucket, and
// deliberately so: a deleted object is a *different* failure — the one the seam reports
// as ErrKeyNotFound and a scan carries on past with a recorded gap — and the property
// here is about an arbitrary backend failure reaching Err rather than about how an
// archive under a lifecycle rule behaves.
func (h *bucketHarness) setFault(fault *conformance.StreamFault) {
	if fault == nil {
		h.source.refuse(nil, nil)
		return
	}
	keys := h.layout.RecordKeys
	if fault.AfterChanges < len(keys) {
		keys = keys[fault.AfterChanges:]
	} else {
		keys = nil
	}
	h.source.refuse(keys, fault.Err)
}

// faultingSource is a source that refuses named objects, wrapping the shipped one so
// that everything but the injected failure is the code that ships.
type faultingSource struct {
	inner objectsource.ObjectSource

	mu      sync.Mutex
	refused map[string]bool
	err     error
}

func (s *faultingSource) refuse(keys []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refused = make(map[string]bool, len(keys))
	for _, key := range keys {
		s.refused[key] = true
	}
	s.err = err
}

func (s *faultingSource) fault(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refused[key] {
		return s.err
	}
	return nil
}

func (s *faultingSource) List(ctx context.Context, prefix string) objectsource.ObjectIterator {
	return s.inner.List(ctx, prefix)
}

func (s *faultingSource) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.fault(key); err != nil {
		return nil, err
	}
	return s.inner.Open(ctx, key)
}

func (s *faultingSource) Close() error { return s.inner.Close() }

// Compile-time proof that the wrapper is a source like any other.
var _ objectsource.ObjectSource = (*faultingSource)(nil)
