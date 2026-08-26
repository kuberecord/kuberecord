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

// Credential rotation, end to end through the real sink runtime.
//
// Everything else in this package tests the writer in isolation, with the
// lifecycle driven by the test. This file is the one place the shipped
// SinkManager drives it, because the property under test is not the writer's and
// not the manager's but the *seam* between them: a rotated access key produces a
// new fingerprint, the manager builds a second instance and swaps routing to it,
// and no job is lost or double-settled across that swap.
//
// The manager's swap semantics are already asserted against fake writers in
// internal/sink. What could not be asserted there is that this backend's drain —
// which has to close and PUT a partial object, not merely flush a queue — actually
// completes inside it. A writer that dropped its open object on cancellation would
// pass every test in internal/sink and lose an object's worth of audit trail on
// every credential rotation.
package s3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// rotationSinkID is the identity the sink under test runs as. The kind is spelled
// out because it is part of what this asserts: the manager routes, drains and
// evicts per (kind, name), and the wiring declares this backend as "S3Sink".
var rotationSinkID = sink.ID{Kind: "S3Sink", Name: "audit"}

// nopSinkPipeline is the data-plane state eviction the manager requires and this
// test has nothing to evict for. It records the calls anyway: a recycle must *not*
// evict, and a silent no-op could not tell the difference.
type nopSinkPipeline struct {
	mu      sync.Mutex
	removed []sink.ID
}

func (p *nopSinkPipeline) RemoveSink(id sink.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removed = append(p.removed, id)
}

func (p *nopSinkPipeline) removals() []sink.ID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sink.ID(nil), p.removed...)
}

// rotationConfig is the sink's configuration with one credential in it. Rotation is
// modelled the way it actually happens: the same spec, a different secret access
// key, hence a different fingerprint.
//
// Rotation triggers are set so that *nothing* closes an object on its own — a large
// size limit and an hour's age — because the point is that the drain closes it. An
// object that rotated by itself mid-test would settle its jobs for the wrong
// reason and the assertion would pass without the property holding.
func rotationConfig(secretAccessKey string) SinkConfig {
	return SinkConfig{
		Client: ClientConfig{
			Region:         "us-east-1",
			Endpoint:       "http://minio.kuberecord-system.svc:9000",
			ForcePathStyle: true,
			Credentials: Credentials{
				AccessKeyID:     "AKIAKUBERECORD",
				SecretAccessKey: secretAccessKey,
			},
		},
		Writer: Config{
			Bucket:       conformanceBucket,
			Prefix:       conformancePrefix,
			MaxObjectAge: testLongAge,
			Workers:      1,
			DrainTimeout: 2 * time.Second,
		},
	}
}

// TestSecretRotationRecyclesWithoutLosingJobs is Task 6.4's zero-job-loss
// criterion, asserted with per-job counters: every job enqueued before the swap
// settles exactly once on the old instance and lands in the old instance's bucket
// client, every job enqueued after it settles exactly once on the new one, and no
// job crosses.
//
// The two stores are what make "settles on the old instance" observable rather
// than merely counted. A rotation that dropped the old instance's open object
// would show up as unsettled jobs; one that migrated queued work to the new
// instance would show up as records in the wrong store — and that second failure
// mode is invisible to a test that only counts commits.
func TestSecretRotationRecyclesWithoutLosingJobs(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	var (
		mu     sync.Mutex
		stores []*fakeStore
	)
	pipe := &nopSinkPipeline{}
	mgr, err := sink.NewSinkManager(sink.ManagerOptions{
		Pipeline: pipe,
		// No probing: this test is about the write path, and a probe would put a
		// second writer of the store in play for no assertion's benefit.
		ProbeInterval: time.Hour,
		// The manager's drain bound is the outer guard, deliberately looser than the
		// writer's own 2s: reaching it would mean the writer failed to finish, which
		// is the failure this test must report rather than absorb.
		DrainTimeout: testSettleWithin,
		Factory: func(id sink.ID, cfg sink.InstanceConfig) (sink.Writer, error) {
			if id != rotationSinkID {
				return nil, fmt.Errorf("unexpected sink %s", id)
			}
			typed, ok := cfg.(SinkConfig)
			if !ok {
				return nil, fmt.Errorf("sink %s: %T is not an S3 configuration", id, cfg)
			}
			store := newFakeStore()
			mu.Lock()
			stores = append(stores, store)
			mu.Unlock()
			return newTestWriter(store, typed.Writer), nil
		},
	})
	if err != nil {
		t.Fatalf("NewSinkManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-mgrDone:
			if err != nil {
				t.Errorf("SinkManager.Start returned %v, want nil", err)
			}
		case <-time.After(testSettleWithin):
			t.Fatalf("the sink runtime did not shut down within %s", testSettleWithin)
		}
	}
	t.Cleanup(stop)

	// The sink as it runs before the rotation.
	if err := mgr.Ensure(rotationSinkID, rotationConfig("old-secret")); err != nil {
		t.Fatalf("Ensure(old credential): %v", err)
	}
	waitFor(t, "the sink to be routed", func() bool {
		_, ok := mgr.WriterFor(rotationSinkID)
		return ok
	})
	before, _ := mgr.WriterFor(rotationSinkID)

	const preSwap = 6
	commits := newCommitLog()
	records := writerRecords(preSwap * 2)
	for i := range preSwap {
		if err := before.Enqueue(ctx, sink.Job{Record: records[i], Commit: commits.commitFor(i)}); err != nil {
			t.Fatalf("Enqueue(pre-swap job %d): %v", i, err)
		}
	}
	// Nothing has closed the object yet, so nothing has settled and nothing has been
	// written. It is the baseline that makes the post-swap assertion mean something:
	// jobs that had already settled here would prove nothing about the drain.
	//
	// It is checked rather than waited for, deliberately — waiting for a condition
	// that is already true proves nothing. What proves the jobs really reached the
	// worker is the assertion further down that the *drain* settles exactly these
	// six: had the hand-off dropped them, none would settle and that assertion would
	// time out.
	if got := commits.settled(); got != 0 {
		t.Fatalf("%d pre-swap jobs settled before anything closed their object", got)
	}
	if got := stores[0].objects(); len(got) != 0 {
		t.Fatalf("the store holds %d objects before any rotation trigger fired", len(got))
	}

	// The rotation: same sink, new credential, hence a new fingerprint.
	if err := mgr.Ensure(rotationSinkID, rotationConfig("new-secret")); err != nil {
		t.Fatalf("Ensure(rotated credential): %v", err)
	}
	waitFor(t, "the second instance to be built and routed", func() bool {
		mu.Lock()
		built := len(stores)
		mu.Unlock()
		after, ok := mgr.WriterFor(rotationSinkID)
		return built == 2 && ok && after != before
	})
	after, _ := mgr.WriterFor(rotationSinkID)

	// The swap drains the old instance, which closes and PUTs its partial object —
	// so every pre-swap job settles there, exactly once, as a success.
	waitFor(t, "the pre-rotation instance to settle its jobs", func() bool {
		return commits.settled() == preSwap
	})
	commits.assertExactlyOnce(t, preSwap, true)
	assertStoreHolds(t, "the pre-rotation store", stores[0], records[:preSwap])

	// Post-swap work goes to the new instance and stays open there: the rotation
	// must not settle it early either.
	for i := preSwap; i < len(records); i++ {
		if err := after.Enqueue(ctx, sink.Job{Record: records[i], Commit: commits.commitFor(i)}); err != nil {
			t.Fatalf("Enqueue(post-swap job %d): %v", i, err)
		}
	}
	if got := commits.settled(); got != preSwap {
		t.Errorf("%d jobs have settled, want the %d pre-swap ones only", got, preSwap)
	}

	// Shutting the runtime down drains the new instance in turn.
	stop()
	commits.assertExactlyOnce(t, len(records), true)
	assertStoreHolds(t, "the post-rotation store", stores[1], records[preSwap:])
	// The pre-rotation store never saw the post-swap records: the swap routed work,
	// it did not migrate it.
	assertStoreHolds(t, "the pre-rotation store after shutdown", stores[0], records[:preSwap])

	// A recycle keeps the sink's pipeline state: it is the same sink with the same
	// durable history, so discarding its dedup baselines would re-emit every object
	// in every scope it serves.
	if got := pipe.removals(); len(got) != 0 {
		t.Errorf("a credential rotation evicted pipeline state for %v, want no eviction", got)
	}
}

// assertStoreHolds fails unless store's objects carry exactly want, in order. The
// records are compared by name, which is what identifies them in this corpus, so a
// failure names the records that went astray rather than printing two structs.
func assertStoreHolds(t *testing.T, what string, store *fakeStore, want []sink.Record) {
	t.Helper()
	got := objectRecords(store.objects())
	gotNames := make([]string, 0, len(got))
	for _, rec := range got {
		gotNames = append(gotNames, rec.Name)
	}
	wantNames := make([]string, 0, len(want))
	for _, rec := range want {
		wantNames = append(wantNames, rec.Name)
	}
	if fmt.Sprint(gotNames) != fmt.Sprint(wantNames) {
		t.Errorf("%s holds %v, want %v", what, gotNames, wantNames)
	}
}

// TestRotationConfigFingerprintCoversTheCredential is the precondition the whole
// rotation depends on, asserted directly rather than inferred from the recycle
// above: the fingerprint must change when the secret access key changes, and must
// not change when nothing does.
//
// A fingerprint that ignored the credential would make Ensure a no-op on rotation —
// the operator would report a rotated key while writing with the old one until
// something else happened to change the spec.
func TestRotationConfigFingerprintCoversTheCredential(t *testing.T) {
	base := rotationConfig("old-secret")
	if base.Fingerprint() != rotationConfig("old-secret").Fingerprint() {
		t.Error("two identical configurations fingerprint differently; every reconcile would recycle the sink")
	}
	if base.Fingerprint() == rotationConfig("new-secret").Fingerprint() {
		t.Error("a rotated secret access key produced the same fingerprint; the rotation would never take effect")
	}
	// And the digest is a digest: the credential must not be recoverable from the
	// value the manager logs.
	if fingerprint := base.Fingerprint(); len(fingerprint) != 64 {
		t.Errorf("fingerprint %q is not a hex SHA-256 digest", fingerprint)
	}
}
