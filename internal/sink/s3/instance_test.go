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

// The health probe's own mechanics, and the capability declaration.
//
// How a probe failure must be *classified* is the contract's obligation and is
// asserted by the conformance suite (Prober/{HealthyBackendPasses,
// SchemaMismatchIsClassified,OtherFailuresReadAsUnreachable}). What is here is
// what this backend does to answer it: that it writes rather than reads, where it
// writes, that it exercises the Object Lock configuration once and then stops
// paying for it, and that a drained writer refuses to probe at all.
package s3

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/sink"
	"github.com/kuberecord/kuberecord/internal/sink/conformance"
)

// TestProbeWritesRatherThanReads is the reason this probe exists in the shape it
// does. A HEAD or a GET would be cheaper and would pass for a credential that
// cannot write, producing a sink that reports itself Ready and then fails every
// single write with nothing on the CR to explain it. So the probe puts an object,
// and this asserts it really does.
func TestProbeWritesRatherThanReads(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})

	if err := r.w.Probe(r.ctx); err != nil {
		t.Fatalf("Probe against a healthy store: %v", err)
	}
	if got := r.store.probeCount(); got != 1 {
		t.Errorf("the probe made %d writes, want exactly 1", got)
	}
	r.stop(t)
}

// TestProbeObjectIsOutsideTheArchive: the probe's object is operational exhaust,
// not audit data, and it must not turn up in anyone's query.
//
// A key inside format=jsonl-v1 would be matched by every reader's glob and would
// carry a line shape that belongs to neither records nor scopes, which breaks
// schema inference for the whole prefix. It also has to live under the sink's own
// prefix, so two sinks sharing a bucket do not probe through each other's key.
func TestProbeObjectIsOutsideTheArchive(t *testing.T) {
	withPrefix := newTestWriter(newFakeStore(), Config{Bucket: "b", Prefix: "audit/kuberecord"})
	if got, want := withPrefix.probeKey(), "audit/kuberecord/"+probeObjectKey; got != want {
		t.Errorf("probe key is %q, want %q", got, want)
	}

	noPrefix := newTestWriter(newFakeStore(), Config{Bucket: "b"})
	if got := noPrefix.probeKey(); got != probeObjectKey {
		t.Errorf("probe key with no prefix is %q, want %q (no leading slash)", got, probeObjectKey)
	}

	for _, key := range []string{withPrefix.probeKey(), noPrefix.probeKey()} {
		if strings.Contains(key, formatPartition) {
			t.Errorf("probe key %q is inside the archive's format partition; every reader's glob would match it", key)
		}
		if strings.HasSuffix(key, objectSuffix) {
			t.Errorf("probe key %q ends in the archive's object suffix; a glob on it would match the probe", key)
		}
	}
}

// TestProbeCarriesObjectLockOnceThenStops: the retention headers have to be
// exercised at least once, because a bucket that will not accept them is exactly
// what the schema classification is about — and they must not be carried on every
// probe, because a bucket with Object Lock enabled is versioned and a retained
// version per probe cycle is an ever-growing set of tiny objects that COMPLIANCE
// mode makes undeletable.
//
// Once is enough: Object Lock cannot be disabled on a bucket once enabled, so the
// answer cannot change under a running instance, and a change to spec.objectLock
// arrives as a new instance which probes afresh.
func TestProbeCarriesObjectLockOnceThenStops(t *testing.T) {
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testLongAge,
		Workers:        1,
		ObjectLock:     &ObjectLock{Mode: "GOVERNANCE", RetainDays: 30},
	})

	if err := r.w.Probe(r.ctx); err != nil {
		t.Fatalf("first Probe: %v", err)
	}
	first := r.store.lastProbeRetention()
	if first == nil {
		t.Fatal("the first probe carried no retention, so a bucket without Object Lock would never be diagnosed")
	}
	if first.Mode != "GOVERNANCE" {
		t.Errorf("the first probe carried retention mode %q, want GOVERNANCE", first.Mode)
	}

	if err := r.w.Probe(r.ctx); err != nil {
		t.Fatalf("second Probe: %v", err)
	}
	if got := r.store.lastProbeRetention(); got != nil {
		t.Errorf("the second probe carried retention %+v; once the bucket has accepted it, re-asking only "+
			"leaves another retained version behind", *got)
	}
	r.stop(t)
}

// TestProbeKeepsRetryingAfterAnIncompatibleBucket: a failed probe must not consume
// the one retention-carrying attempt. Otherwise a sink that came up while its
// bucket was unreachable would never re-ask the question, and a genuinely
// misconfigured bucket would be reported as healthy from the second probe onwards.
func TestProbeKeepsRetryingAfterAnIncompatibleBucket(t *testing.T) {
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testLongAge,
		Workers:        1,
		ObjectLock:     &ObjectLock{Mode: "COMPLIANCE", RetainDays: 3650},
	})
	r.store.setProbeOutcome(conformance.ProbeSchemaMismatch)

	for attempt := range 3 {
		err := r.w.Probe(r.ctx)
		if !errors.Is(err, sink.ErrSchemaInvalid) {
			t.Fatalf("probe %d returned %v, want an error satisfying errors.Is(err, sink.ErrSchemaInvalid)", attempt, err)
		}
		if got := r.store.lastProbeRetention(); got == nil {
			t.Fatalf("probe %d carried no retention; a probe that never succeeded must keep asking", attempt)
		}
	}
	r.stop(t)
}

// TestProbeIsRefusedAfterShutdown: the probe shares the client with the write
// path, and the client is closed at the end of the drain. A probe accepted
// afterwards would be a use of a closed client — which is precisely what the
// otherUsers wait exists to prevent, and the refusal is the other half of it.
func TestProbeIsRefusedAfterShutdown(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})
	r.stop(t)

	if err := r.w.Probe(context.Background()); err == nil {
		t.Error("Probe was accepted after shutdown; it would reach a closed client")
	}
}

// TestProbeHonoursTheCallersDeadline: the manager bounds each probe attempt with
// the context it passes, so a store that never answers must not pin the probe
// goroutine — and, through otherUsers, the whole shutdown behind it.
func TestProbeHonoursTheCallersDeadline(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	r.store.setProbeBlock(blocked)

	ctx, cancel := context.WithTimeout(r.ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := r.w.Probe(ctx); err == nil {
		t.Error("Probe succeeded against a store that never answered")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Probe took %s to honour a 100ms deadline", elapsed)
	}
	r.stop(t)
}

// TestWriterImplementsExactlyTheDeclaredHalves is the capability declaration as a
// test rather than only as a compile-time assertion.
//
// The two directions fail for different reasons and both matter. A missing half
// that should be there is a sink the runtime builds degraded while every test
// stays green (SinkManager.newLiveSink makes this same assertion). A present half
// that should not be there — a StateReader on this backend — is D12 quietly
// reversed: warm-up, zombie GC and boot reconciliation would switch back on for an
// archive that cannot answer them, and HistoryUnavailable would stop being
// reported.
func TestWriterImplementsExactlyTheDeclaredHalves(t *testing.T) {
	w := newTestWriter(newFakeStore(), Config{Bucket: "b"})

	if _, ok := any(w).(sink.ScopeEventWriter); !ok {
		t.Error("the S3 writer does not implement sink.ScopeEventWriter; the archive would lose its epoch log")
	}
	if _, ok := any(w).(sink.Prober); !ok {
		t.Error("the S3 writer does not implement sink.Prober; the sink's CR would carry no health verdict")
	}
	if _, ok := any(w).(sink.StateReader); ok {
		t.Error("the S3 writer implements sink.StateReader, which D12 says it must not: an object store cannot " +
			"answer a warm-up read without scanning its whole prefix, and implementing it would silently " +
			"re-enable cache warm-up, zombie GC and boot reconciliation for an archive tier")
	}
}
