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

// This file is where the S3 backend is measured against the sink contract rather
// than against its own behaviour. It asserts nothing itself: every assertion
// lives in internal/sink/conformance (D11), so the properties this backend passes
// are word-for-word the ones ClickHouse passes.
//
// What "S3 passes conformance" does and does not mean — the same two-layer caveat
// the ClickHouse harness carries, and for the same reason:
//
//   - What is proven here is the *Go* logic against a stand-in store: rotation,
//     the retry path, the drain, which records end up in which object, how a
//     commit is settled, how an overwrite behaves. That is the shipped writer's
//     own behaviour, and a bug in it fails these tests.
//   - The stand-in is not S3. It models atomic PUTs, one object per key, and
//     overwrite-by-key (see fakeStore), which is what the writer's correctness
//     rests on — but it does not speak the S3 API, enforce a credential, or apply
//     Object Lock. Those are discharged in Task 6.6 against a real MinIO under
//     `make test-integration`. Neither layer is sufficient alone.
//
// Which assertion belongs where — read this before adding one.
//
// From the suite (contract obligations; never re-assert them in this package):
// ExactlyOnceCommit/{Success,PermanentFailure,ContextCancelledMidFlight,Drain},
// NoLostJobs, DrainOrdering, EnqueueBounded, AtLeastOnceIdempotency,
// ConcurrentEnqueueStorm — and, for the two optional halves this backend does
// implement, ScopeEventWriter/{EpochTransitionsRecordedExactlyOnce,
// RejectionIsSurfaced} and Prober/{HealthyBackendPasses,
// SchemaMismatchIsClassified,OtherFailuresReadAsUnreachable}.
//
// The suite also *skips* the StateReader halves, loudly, because this backend
// declares it implements none of them — which is D12's whole point and is checked
// here rather than assumed: the harness's declaration below is compared against
// the same type assertion SinkManager.newLiveSink makes, so an S3 writer that
// grew a StateReader without anyone reviewing what that turns back on would fail
// this file.
//
// Backend-specific, and therefore in writer_test.go / scopewriter_test.go /
// instance_test.go (the suite is deliberately silent about all of it):
// rotation by size and by age, the lone-record age flush, the drain flushing a
// partial object before the client closes, the retried object being one key
// written twice with identical bytes, the Object Lock headers, the probe's
// write-not-read discipline, and the scope log's own key layout.
package s3

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/conformance"
)

// testSinkID labels the metrics a test writer reports, exactly as the sink
// runtime would.
var testSinkID = sink.ID{Kind: "S3Sink", Name: "conformance"}

// The tuning every conformance harness builds its Writer with.
//
// conformanceMaxObjectBytes is 1, which makes every record its own object, and
// that is a deliberate and load-bearing choice rather than a shortcut. The suite
// asks two things at once of a backend that deduplicates by overwriting
// (DedupObjectOverwrite): that each record has a key, and that writing a record
// twice leaves exactly one physical copy under that key. For an object store both
// are true only when the key is the object's — so the record's key and the
// object's key have to be the same thing, which means one record per object. With
// several records to an object, a replay whose rotation boundaries fell
// differently would genuinely leave that record in two surviving objects, and no
// declaration could make that one physical copy.
//
// So the suite certifies the commit, drain, ordering and idempotency properties,
// and rotation is certified next door in writer_test.go, where an object holding
// many records is exactly what is under test. Neither file covers for the other.
//
// conformanceMaxRetryBackoff is chosen for one property: backoff/v4 stops once
// elapsed+next would exceed MaxElapsedTime and its first interval is 250–750ms,
// so one second buys exactly one whole-object retry — which is what makes
// AtLeastOnceIdempotency exercise the re-PUT a lost acknowledgement really
// produces, rather than a single abandoned attempt.
const (
	conformanceBucket         = "conformance-bucket"
	conformancePrefix         = "conformance/audit"
	conformanceQueueCapacity  = 256
	conformanceWorkers        = 4
	conformanceMaxObjectBytes = 1
	conformanceMaxObjectAge   = 1 * time.Second
	conformancePutTimeout     = 2 * time.Second
	conformanceRetryBackoff   = 1 * time.Second
	conformanceDrainTimeout   = 10 * time.Second
	conformanceEnqueueTimeout = 2 * time.Second
	// conformanceSettleWithin must exceed the writer's whole retry budget, not one
	// attempt: the permanently-failing property waits for every job to settle
	// against a store that refuses each of them.
	conformanceSettleWithin = 20 * time.Second
	// conformanceScopeRetryBackoff shortens the scope path's own retry window from
	// its production 30s. Nothing faults a scope PUT here, so this only bounds how
	// long a property would wait before failing rather than hanging.
	conformanceScopeRetryBackoff = 500 * time.Millisecond
)

// TestWriterConformance runs the backend-agnostic Writer suite against the S3
// Writer.
//
// The harness constructor is called once per property (the suite shuts the Writer
// down in several of them, and a Writer is not restartable), so each property
// gets a fresh store, a fresh log and a fresh metrics registry and cannot inherit
// another's state.
func TestWriterConformance(t *testing.T) {
	conformance.RunWriterSuite(t, newConformanceHarness)
}

// newConformanceHarness wires a Writer over this package's fakeStore up as a
// conformance.Harness. No production code is involved beyond NewWriter, which is
// the point: the suite passes or fails on the shipped writer's own behaviour.
func newConformanceHarness(t *testing.T) conformance.Harness {
	t.Helper()

	store := newFakeStore()
	w := newTestWriter(store, Config{
		Bucket:         conformanceBucket,
		Prefix:         conformancePrefix,
		MaxObjectBytes: conformanceMaxObjectBytes,
		MaxObjectAge:   conformanceMaxObjectAge,
		QueueSize:      conformanceQueueCapacity,
		Workers:        conformanceWorkers,
		EnqueueTimeout: conformanceEnqueueTimeout,
		DrainTimeout:   conformanceDrainTimeout,
	})

	// A modelling violation in the store is not a contract violation, so it is
	// reported here rather than through the suite: it would otherwise surface as a
	// property comparing records this backend never really stored, and the reader
	// would go looking for the bug in the writer.
	t.Cleanup(func() {
		if err := store.firstHarnessErr(); err != nil {
			t.Errorf("the object store stand-in observed something it cannot model: %v", err)
		}
	})

	return conformance.Harness{
		Writer:         w,
		Events:         store.snapshot,
		SetFault:       store.setFault,
		LogicalKey:     conformanceLogicalKey,
		Dedup:          conformance.DedupObjectOverwrite,
		QueueCapacity:  conformanceQueueCapacity,
		EnqueueTimeout: conformanceEnqueueTimeout,
		SettleWithin:   conformanceSettleWithin,

		// The optional halves. This backend implements two of the three and says
		// so; the suite compares the claim against the type assertion
		// SinkManager.newLiveSink makes and fails on either disagreement. The
		// absent one is sink.StateReader, and its absence is D12 — see
		// instance.go, which names what it costs. Adding CapStateReader here to
		// make a skipped suite run would be claiming a capability this backend
		// cannot have.
		Capabilities: conformance.DeclareCapabilities(
			conformance.CapScopeEventWriter, conformance.CapProber),
		ScopeWrites:     store.scopeSnapshot,
		SetProbeOutcome: store.setProbeOutcome,
	}
}

// newTestWriter builds a Writer with the conformance tuning for the three fields
// NewWriter does not take — the PUT and retry budgets, which are production-sized
// constants, and the scope path's retry window — and points the store's probe
// routing at the key this writer will actually probe.
//
// It takes the whole Config so the backend-specific tests can vary rotation while
// still getting the same test-sized timeouts.
func newTestWriter(store *fakeStore, cfg Config) *Writer {
	w := NewWriter(store, cfg, newTestMetrics())
	w.putTimeout = conformancePutTimeout
	w.maxRetryBackoff = conformanceRetryBackoff
	w.scopeMaxRetryBackoff = conformanceScopeRetryBackoff
	store.probeKey = w.probeKey()
	return w
}

// newTestMetrics is a fresh, isolated metrics instance: an own registry per
// writer, so one test's counters can never be read as another's — and so the real
// per-sink collectors are exercised rather than a stub that would hide a metric
// this writer forgot to record.
func newTestMetrics() Metrics {
	return pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID)
}

// conformanceLogicalKey implements conformance.Harness.LogicalKey: the key this
// backend deduplicates on, which for an object store is the object's own key.
//
// It is computed through the shipped one-shot encoder rather than assembled here,
// so the suite is asking the same question the writer answers: with one record to
// an object, Encode over that single record produces exactly the key the writer's
// accumulating builder produces for it (both hash the uncompressed payload — see
// object.go, and TestBuilderAndEncodeAgree, which pins it).
//
// It is emphatically not the object identity key of Invariant 7: two events for
// one object are two different records, land in two different objects, and are two
// logical records here.
func conformanceLogicalKey(rec sink.Record) string {
	obj, err := Encode(conformancePrefix, []sink.Record{rec})
	if err != nil {
		// Unreachable for records the suite builds, and a distinct key rather than
		// a shared empty one so a failure names the record instead of collapsing
		// every unencodable record into one bucket.
		return "unencodable/" + rec.Namespace + "/" + rec.Name
	}
	return obj.Key
}
