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

// The S3 write path's own behaviour: everything the sink contract is silent about
// and this backend therefore has to prove for itself.
//
// The contract obligations — exactly-once commits, the drain, bounded enqueue,
// idempotency — are asserted by the conformance suite and are deliberately not
// re-asserted here (see writer_conformance_test.go). What is here is rotation:
// when an object closes, what it holds, that its size is measured on the
// compressed payload, and that a retry of one leaves one object. Those are
// choices this backend makes, not promises the contract extracts, and the
// conformance harness runs with one record per object precisely so that this file
// is where a multi-record object is put under test.
package s3

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/kuberecord/kuberecord/internal/sink"
	"github.com/kuberecord/kuberecord/internal/sink/conformance"
)

// The rotation tests' shared tuning. A single worker is what makes them
// deterministic: with several, which records share an object depends on which
// worker happened to receive them, so an assertion on object boundaries would be
// asserting the scheduler. Concurrency is covered separately, by
// TestConcurrentEnqueueStormRotatesWithoutLosingRecords, which asserts what is
// true regardless of the split.
const (
	// testRotationBytes is a small encoded-size limit, chosen so that a few hundred
	// test records fill a dozen objects in well under a second: these records
	// compress about 13:1, so roughly 28 of them close one object. It is far below
	// the CRD's 1Mi floor on purpose — the floor is about not littering a real
	// bucket, and this is about exercising the trigger.
	testRotationBytes = 1024
	// testLongAge stands for "the age trigger must not fire during this test".
	testLongAge = 1 * time.Hour
	// testShortAge is the age trigger under test. It is short enough to keep these
	// tests quick and long enough that a handful of enqueues cannot straddle it on
	// a loaded machine — the age assertions are about one object holding a whole
	// burst, so a stall between two hand-offs must not split it.
	testShortAge = 300 * time.Millisecond
	// testSettleWithin bounds every poll in this file. It is generous because a
	// failure here should read as "this never happened", not as "this machine was
	// busy".
	testSettleWithin = 15 * time.Second
)

// writerRecord builds record i: distinct in every field that identifies it,
// stable across calls (so a replay is genuinely identical), and dated from the
// corpus epoch so nothing in an object's key depends on when the test ran.
func writerRecord(i int) sink.Record {
	rec := baseRecord()
	rec.Timestamp = corpusEpoch.Add(time.Duration(i) * time.Millisecond)
	rec.Name = fmt.Sprintf("obj-%05d", i)
	rec.UID = "uid-" + rec.Name
	rec.ResourceVersion = strconv.Itoa(1000 + i)
	rec.Labels = map[string]string{"app": rec.Name}
	return rec
}

// writerRecords builds n records, indexed by job number.
func writerRecords(n int) []sink.Record {
	out := make([]sink.Record, n)
	for i := range n {
		out[i] = writerRecord(i)
	}
	return out
}

// commitLog is the instrument every test here settles jobs through: one counter
// per job, so a stranded job (never settled) and a double-settled one are both
// visible, and the outcome each job saw.
type commitLog struct {
	mu     sync.Mutex
	counts map[int]int
	oks    map[int]bool
}

func newCommitLog() *commitLog {
	return &commitLog{counts: map[int]int{}, oks: map[int]bool{}}
}

// commitFor returns job i's callback.
func (c *commitLog) commitFor(i int) func(bool) {
	return func(ok bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.counts[i]++
		c.oks[i] = ok
	}
}

// settled is how many jobs have settled at least once.
func (c *commitLog) settled() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.counts)
}

// assertExactlyOnce fails unless every one of the first n jobs settled exactly
// once, with the outcome wantOK.
func (c *commitLog) assertExactlyOnce(t *testing.T, n int, wantOK bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range n {
		switch got := c.counts[i]; {
		case got == 0:
			t.Errorf("job %d never settled: the writer stranded it", i)
		case got > 1:
			t.Errorf("job %d settled %d times, want exactly 1: a double settle corrupts the caller's dedup cache", i, got)
		}
		if c.oks[i] != wantOK {
			t.Errorf("job %d settled ok=%t, want ok=%t", i, c.oks[i], wantOK)
		}
	}
}

// running is one started Writer under test, with the store it writes to.
type running struct {
	w     *Writer
	store *fakeStore
	ctx   context.Context

	cancel context.CancelFunc
	done   chan error
	waited bool
}

// startWriter builds a Writer with the given rotation and starts it. The store's
// probe routing is wired up too, so a test may probe without further setup.
func startWriter(t *testing.T, cfg Config) *running {
	t.Helper()
	if cfg.Bucket == "" {
		cfg.Bucket = conformanceBucket
	}
	if cfg.Prefix == "" {
		cfg.Prefix = conformancePrefix
	}
	store := newFakeStore()
	w := newTestWriter(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	r := &running{w: w, store: store, ctx: ctx, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- w.Start(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

// stop cancels the writer and waits for Start to return, failing if the shutdown
// is not bounded. It is idempotent so a test can stop explicitly (most do — the
// drain is what they assert) and still have the cleanup as a safety net.
func (r *running) stop(t *testing.T) {
	t.Helper()
	if r.waited {
		return
	}
	r.waited = true
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("Start returned %v; a clean shutdown must return nil", err)
		}
	case <-time.After(testSettleWithin):
		t.Fatalf("Start did not return within %s of cancellation: shutdown is not bounded", testSettleWithin)
	}
	if err := r.store.firstHarnessErr(); err != nil {
		t.Errorf("the object store stand-in observed something it cannot model: %v", err)
	}
}

// enqueue submits records[i] for every i, failing on a refusal — every test here
// sizes its queue to accept the whole run.
func (r *running) enqueue(t *testing.T, c *commitLog, records []sink.Record) {
	t.Helper()
	for i, rec := range records {
		if err := r.w.Enqueue(r.ctx, sink.Job{Record: rec, Commit: c.commitFor(i)}); err != nil {
			t.Fatalf("Enqueue(job %d): %v", i, err)
		}
	}
}

// waitFor polls cond until it holds, failing with msg when it never does.
func waitFor(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testSettleWithin)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", testSettleWithin, msg)
		}
		time.Sleep(time.Millisecond)
	}
}

// objectRecords is every record the store holds, in object order and then in line
// order within each object — which for a single-worker writer is exactly the
// order the records were enqueued in.
func objectRecords(objects []storeEntry) []sink.Record {
	var out []sink.Record
	for _, obj := range objects {
		out = append(out, obj.records()...)
	}
	return out
}

// assertSameRecords fails unless got and want are the same records in the same
// order, comparing the fields that identify one.
func assertSameRecords(t *testing.T, got, want []sink.Record, when string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: the store holds %d records, want %d", when, len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i].Name || !got[i].Timestamp.Equal(want[i].Timestamp) ||
			got[i].UID != want[i].UID || got[i].ResourceVersion != want[i].ResourceVersion {
			t.Fatalf("%s: record %d is %s@%s, want %s@%s", when, i,
				got[i].Name, got[i].Timestamp, want[i].Name, want[i].Timestamp)
		}
	}
}

// TestRotationBySizeClosesFullObjects is the size trigger on its own: with the
// age trigger set an hour out, nothing but the encoded size can close an object.
//
// It asserts the trigger both ways. Every object but the last really did reach
// maxObjectBytes (so rotation is not firing early), and several records share an
// object (so it is not firing per record) — and the records come back in order,
// complete, which is what says rotation splits the stream rather than losing part
// of it.
func TestRotationBySizeClosesFullObjects(t *testing.T) {
	const jobs = 400
	r := startWriter(t, Config{
		MaxObjectBytes: testRotationBytes,
		MaxObjectAge:   testLongAge,
		Workers:        1,
		QueueSize:      jobs,
	})

	records := writerRecords(jobs)
	c := newCommitLog()
	r.enqueue(t, c, records)
	// Wait for the size trigger to fire while the writer is running — the records
	// left over after the last full object are still open, and nothing but the
	// drain will close them, which is why the shutdown comes next rather than a
	// wait for every job.
	waitFor(t, "the size trigger to close objects", func() bool { return len(r.store.objects()) >= 2 })
	r.stop(t)

	c.assertExactlyOnce(t, jobs, true)
	objects := r.store.objects()
	if len(objects) < 2 {
		t.Fatalf("%d records at a %d-byte rotation produced %d object(s); the size trigger never fired",
			jobs, testRotationBytes, len(objects))
	}
	assertSameRecords(t, objectRecords(objects), records, "size rotation")

	for i, obj := range objects[:len(objects)-1] {
		if len(obj.body) < testRotationBytes {
			t.Errorf("object %d (%s) is %d encoded bytes, below the %d-byte rotation limit: "+
				"a closed object must have reached the limit, not merely approached it",
				i, obj.key, len(obj.body), testRotationBytes)
		}
		if len(obj.records()) < 2 {
			t.Errorf("object %d (%s) holds %d record(s): rotation is closing an object per record, "+
				"which is the small-file pattern the limit exists to prevent", i, obj.key, len(obj.records()))
		}
	}
}

// TestRotationMeasuresTheEncodedSize is what makes "maxObjectBytes is measured on
// the encoded payload" checkable rather than merely documented.
//
// The records are highly compressible, so the two readings are far apart: an
// object closed on its *uncompressed* size would come out at a fraction of the
// limit. Asserting that each full object is at or above the limit while the JSONL
// behind it is several times larger pins the trigger to the compressed reading —
// and with it the memory ceiling the CRD documents, which is only true if what a
// worker accumulates is compressed bytes.
func TestRotationMeasuresTheEncodedSize(t *testing.T) {
	const jobs = 400
	r := startWriter(t, Config{
		MaxObjectBytes: testRotationBytes,
		MaxObjectAge:   testLongAge,
		Workers:        1,
		QueueSize:      jobs,
	})

	c := newCommitLog()
	r.enqueue(t, c, writerRecords(jobs))
	waitFor(t, "the size trigger to close an object", func() bool { return len(r.store.objects()) >= 2 })
	r.stop(t)

	c.assertExactlyOnce(t, jobs, true)
	objects := r.store.objects()
	if len(objects) < 2 {
		t.Fatalf("expected several objects, got %d", len(objects))
	}
	full := objects[0]
	uncompressed := len(decodePayload(t, full.body))
	if len(full.body) < testRotationBytes {
		t.Errorf("the first full object is %d encoded bytes, below the %d-byte limit", len(full.body), testRotationBytes)
	}
	if uncompressed <= testRotationBytes {
		t.Fatalf("the first object's JSONL is only %d bytes against a %d-byte limit, so this test cannot tell "+
			"the two readings apart; make the records more compressible or the limit smaller",
			uncompressed, testRotationBytes)
	}
	t.Logf("first object: %d encoded bytes from %d bytes of JSONL (%.1fx)",
		len(full.body), uncompressed, float64(uncompressed)/float64(len(full.body)))
}

// TestRotationByAgeClosesAnUnfilledObject is the age trigger on its own: with the
// size limit far out of reach, nothing but maxObjectAge can close the object — and
// it must, while the writer is still running, because that is the whole point of
// the trigger. An operator's exposure to a crash is bounded by this and by nothing
// else.
func TestRotationByAgeClosesAnUnfilledObject(t *testing.T) {
	const jobs = 3
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
	})

	records := writerRecords(jobs)
	c := newCommitLog()
	r.enqueue(t, c, records)

	// No shutdown: the object has to be written by the age timer alone.
	waitFor(t, "the age trigger to close an object", func() bool { return len(r.store.objects()) == 1 })
	waitFor(t, "every job to settle", func() bool { return c.settled() == jobs })

	c.assertExactlyOnce(t, jobs, true)
	objects := r.store.objects()
	assertSameRecords(t, objectRecords(objects), records, "age rotation")
	if got := len(objects[0].records()); got != jobs {
		t.Errorf("the object holds %d records, want all %d: the age trigger must close one object, not one per record", got, jobs)
	}
	r.stop(t)
}

// TestLoneRecordFlushesAfterMaxObjectAge is the quiet-cluster case, and the one
// that would be silently broken by an age timer armed only when a batch fills:
// one record arrives, nothing follows it, and it must still reach the archive
// within maxObjectAge rather than waiting for a neighbour that never comes.
func TestLoneRecordFlushesAfterMaxObjectAge(t *testing.T) {
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
	})

	c := newCommitLog()
	r.enqueue(t, c, writerRecords(1))

	waitFor(t, "the lone record's object to be written", func() bool { return len(r.store.objects()) == 1 })
	// The store observes the PUT before the worker settles the jobs it carried, so
	// the commit has to be waited for rather than read off the back of the write.
	waitFor(t, "the lone record's job to settle", func() bool { return c.settled() == 1 })
	c.assertExactlyOnce(t, 1, true)
	if got := len(r.store.objects()[0].records()); got != 1 {
		t.Errorf("the object holds %d records, want 1", got)
	}
	r.stop(t)
}

// TestIdleWorkerWritesNothing is the other half of the timer discipline: the age
// timer is armed only while an object is open, so an idle writer must produce no
// objects at all — not an empty one per maxObjectAge.
//
// An empty object would be permanently retained in an archive bucket for nothing,
// which is why the encoder refuses one outright; this asserts the writer never
// asks it to.
func TestIdleWorkerWritesNothing(t *testing.T) {
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        2,
	})

	time.Sleep(6 * testShortAge)
	if got := len(r.store.objects()); got != 0 {
		t.Errorf("an idle writer produced %d object(s) over %s; the age timer must be armed only while an "+
			"object is open", got, 6*testShortAge)
	}
	r.stop(t)
	if got := len(r.store.objects()); got != 0 {
		t.Errorf("shutting an idle writer down produced %d object(s), want none", got)
	}
}

// TestDrainFlushesPartialObjectBeforeClientClose is the shutdown case the AC
// singles out: a half-full object, no trigger anywhere near firing, and the drain
// has to close it — and close it *before* the client goes away.
//
// The ordering is the half that would be silently wrong: a writer that released
// its client first would lose exactly the records that were only in memory, and
// every commit would still fire. So the store's own log is what is asserted, not
// just its contents.
func TestDrainFlushesPartialObjectBeforeClientClose(t *testing.T) {
	const jobs = 5
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testLongAge,
		Workers:        1,
	})

	records := writerRecords(jobs)
	c := newCommitLog()
	r.enqueue(t, c, records)

	// Nothing can close this object but the drain: neither trigger is reachable.
	waitFor(t, "the records to reach the worker", func() bool { return len(r.w.jobs) == 0 })
	if got := len(r.store.objects()); got != 0 {
		t.Fatalf("%d object(s) were written before shutdown, so this test is not measuring the drain", got)
	}

	r.stop(t)

	c.assertExactlyOnce(t, jobs, true)
	objects := r.store.objects()
	if len(objects) != 1 {
		t.Fatalf("the drain produced %d objects, want exactly 1", len(objects))
	}
	assertSameRecords(t, objectRecords(objects), records, "drain flush")

	events := r.store.snapshot()
	closeAt := slices.IndexFunc(events, func(ev conformance.Event) bool { return ev.Kind == conformance.EventClose })
	switch {
	case closeAt < 0:
		t.Fatal("the client was never closed: shutdown must release what it owns")
	case closeAt == 0:
		t.Error("the client was closed before the drain wrote anything: the partial object would have been lost")
	}
	if got := r.store.closeCount(); got != 1 {
		t.Errorf("the client was closed %d times, want exactly 1", got)
	}
}

// TestForcedPutFailureSettlesEveryRecordOnceAsFailed: one object, one PUT, one
// outcome. When that PUT cannot be made to work, every record in the object
// settles false — exactly once each — and nothing is left in the store.
//
// It is the property that makes a failed archive write *recoverable*: commit(false)
// is what reverts the pipeline's version-gated cache entry, so the record is
// re-offered later. A record that settled true, or never settled at all, would be
// gone from the archive with nothing to say so.
func TestForcedPutFailureSettlesEveryRecordOnceAsFailed(t *testing.T) {
	const jobs = 6
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
	})
	refused := errors.New("s3 test: the store refuses every object")
	r.store.setFault(func(context.Context, []sink.Record) error { return refused })

	c := newCommitLog()
	r.enqueue(t, c, writerRecords(jobs))
	waitFor(t, "every job to settle as failed", func() bool { return c.settled() == jobs })
	r.stop(t)

	c.assertExactlyOnce(t, jobs, false)
	if got := len(r.store.objects()); got != 0 {
		t.Errorf("the store holds %d object(s) after refusing every PUT", got)
	}
	// Every attempt is in the log, and every one of them failed: a writer that
	// gave up without trying would leave a single entry, and one that reported a
	// success would leave a durable one.
	events := r.store.snapshot()
	attempts := 0
	for _, ev := range events {
		if ev.Kind != conformance.EventWrite {
			continue
		}
		attempts++
		if ev.Durable() {
			t.Errorf("an attempt is recorded durable at a store that refused every PUT")
		}
	}
	if attempts < 2 {
		t.Errorf("only %d PUT attempt(s) were made for a failing object; the retry path never ran", attempts)
	}
}

// TestRetriedObjectIsOneKeyWrittenTwiceIdentically is the Phase 6 acceptance
// criterion "a retried write leaves exactly one object, byte-identical", made
// executable.
//
// The lost acknowledgement is the case that matters and the one that cannot be
// avoided: the object reached the bucket and the answer did not, so the writer
// retries a write that has already happened. Because the key is the hash of the
// object's own uncompressed payload, the retry lands on the same key with the same
// bytes — an overwrite, not a duplicate. The store checks the byte-identity itself
// (see fakeStore.checkOverwriteLocked), so a re-encode that differed would fail
// here even if the count came out right.
func TestRetriedObjectIsOneKeyWrittenTwiceIdentically(t *testing.T) {
	const jobs = 4
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
	})

	var attempts int
	var mu sync.Mutex
	r.store.setFault(func(context.Context, []sink.Record) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			// Durable, and reported as a failure: a timeout after the object
			// landed looks exactly like this from the client.
			return conformance.ErrLostAck
		}
		return nil
	})

	records := writerRecords(jobs)
	c := newCommitLog()
	r.enqueue(t, c, records)
	waitFor(t, "every job to settle", func() bool { return c.settled() == jobs })
	r.stop(t)

	c.assertExactlyOnce(t, jobs, true)
	objects := r.store.objects()
	if len(objects) != 1 {
		t.Fatalf("a retried object left %d objects in the store, want exactly 1: %v", len(objects), objectKeys(objects))
	}
	if got := objects[0].writes; got != 2 {
		t.Errorf("the object's key was written %d time(s), want 2 (the lost-ack attempt and its retry)", got)
	}
	assertSameRecords(t, objectRecords(objects), records, "retried object")
}

// TestConcurrentEnqueueStormRotatesWithoutLosingRecords is the multi-worker,
// production-shaped counterpart to the conformance storm (which runs with one
// record per object): here every worker is filling its own object while producers
// hand off concurrently, which is where a shared builder or a mis-scoped timer
// would show up.
//
// What it asserts is what remains true whatever the split: every job settles once,
// and every record is in the archive exactly once. Which object a record landed in
// is deliberately not asserted — that depends on which worker received it, and a
// test that pinned it would be testing the scheduler.
//
// Its real value is under -race.
func TestConcurrentEnqueueStormRotatesWithoutLosingRecords(t *testing.T) {
	const (
		producers   = 10
		perProducer = 50
		jobs        = producers * perProducer
	)
	r := startWriter(t, Config{
		MaxObjectBytes: testRotationBytes,
		MaxObjectAge:   testShortAge,
		Workers:        4,
		QueueSize:      jobs,
	})

	records := writerRecords(jobs)
	c := newCommitLog()
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range perProducer {
				n := p*perProducer + i
				if err := r.w.Enqueue(r.ctx, sink.Job{Record: records[n], Commit: c.commitFor(n)}); err != nil {
					t.Errorf("Enqueue(job %d): %v", n, err)
					return
				}
			}
		})
	}
	wg.Wait()
	waitFor(t, "every job to settle", func() bool { return c.settled() == jobs })
	r.stop(t)

	c.assertExactlyOnce(t, jobs, true)
	seen := map[string]int{}
	for _, rec := range objectRecords(r.store.objects()) {
		seen[rec.Name]++
	}
	for _, rec := range records {
		switch got := seen[rec.Name]; {
		case got == 0:
			t.Fatalf("%s settled true but is in no object: the archive is missing a record it acknowledged", rec.Name)
		case got > 1:
			t.Fatalf("%s appears in %d objects: a record must be archived once", rec.Name, got)
		}
	}
}

// TestObjectLockRetentionTravelsWithEveryPut: when spec.objectLock is set, the
// retention goes on the PUT — on the records and on the scope log alike, since
// both are the archive.
//
// A sink whose objects are written without their retention is the failure this
// asserts against, and it is invisible without a check like this one: every write
// succeeds, every record is present, and the compliance property the operator
// configured — that nobody can delete or alter the archive before its date — is
// simply absent.
func TestObjectLockRetentionTravelsWithEveryPut(t *testing.T) {
	const retainDays = 7
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
		ObjectLock:     &ObjectLock{Mode: "COMPLIANCE", RetainDays: retainDays},
	})

	before := time.Now().UTC()
	c := newCommitLog()
	r.enqueue(t, c, writerRecords(2))
	if err := r.w.EnqueueScopeEvent(r.ctx, testScopeEvent(sink.ScopeActionStarted)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}
	waitFor(t, "the object to be written", func() bool { return len(r.store.objects()) == 1 })
	waitFor(t, "the scope object to be written", func() bool { return len(r.store.scopeObjects()) == 1 })
	r.stop(t)

	after := time.Now().UTC()
	for _, obj := range slices.Concat(r.store.objects(), r.store.scopeObjects()) {
		if obj.retention == nil {
			t.Fatalf("object %s was written with no retention, but the sink configures Object Lock", obj.key)
		}
		if obj.retention.Mode != "COMPLIANCE" {
			t.Errorf("object %s carries retention mode %q, want COMPLIANCE", obj.key, obj.retention.Mode)
		}
		window := retainDays * 24 * time.Hour
		if obj.retention.RetainUntil.Before(before.Add(window)) || obj.retention.RetainUntil.After(after.Add(window)) {
			t.Errorf("object %s is retained until %s, want %s ± the test's own duration",
				obj.key, obj.retention.RetainUntil, before.Add(window))
		}
	}
}

// TestNoObjectLockMeansNoRetentionHeader: the absence of spec.objectLock is a
// supported configuration and must reach the store as an absence, not as a
// zero-valued retention — an object retained until the zero instant is a request
// a real store rejects.
func TestNoObjectLockMeansNoRetentionHeader(t *testing.T) {
	r := startWriter(t, Config{
		MaxObjectBytes: defaultMaxObjectBytes,
		MaxObjectAge:   testShortAge,
		Workers:        1,
	})

	c := newCommitLog()
	r.enqueue(t, c, writerRecords(1))
	waitFor(t, "the object to be written", func() bool { return len(r.store.objects()) == 1 })
	r.stop(t)

	if got := r.store.objects()[0].retention; got != nil {
		t.Errorf("an object was written with retention %+v, but the sink configures no Object Lock", *got)
	}
}

// TestEnqueueRefusesAnUnencodableRecord: a record that cannot be part of any
// object is refused to its caller, synchronously, and never settled.
//
// The distinction matters. A refusal is an error the caller propagates, so the
// pipeline's own requeue and backoff take over and nothing has been counted as
// written. An accepted-then-failed job has already been through a commit
// callback, which the caller reads as a settled write it must revert. Refusing
// is both cheaper and more truthful for a record no retry can fix.
func TestEnqueueRefusesAnUnencodableRecord(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})

	noCluster := writerRecord(0)
	noCluster.ClusterID = ""
	c := newCommitLog()
	err := r.w.Enqueue(r.ctx, sink.Job{Record: noCluster, Commit: c.commitFor(0)})
	if err == nil {
		t.Fatal("Enqueue accepted a record with no cluster_id; every object is partitioned by it")
	}
	if got := c.settled(); got != 0 {
		t.Errorf("a refused job settled %d time(s): the caller already has the error as its outcome", got)
	}
	r.stop(t)
	if got := len(r.store.objects()); got != 0 {
		t.Errorf("the store holds %d object(s) after the only record was refused", got)
	}
}

// TestEnqueueIsRefusedAfterShutdown: once Start has returned, the queues are
// closed and a send would panic. Enqueue must therefore refuse — the manager can
// still hold a reference to a drained instance while a rule's last work item is
// in flight.
func TestEnqueueIsRefusedAfterShutdown(t *testing.T) {
	r := startWriter(t, Config{MaxObjectBytes: defaultMaxObjectBytes, MaxObjectAge: testLongAge, Workers: 1})
	r.stop(t)

	c := newCommitLog()
	if err := r.w.Enqueue(context.Background(), sink.Job{Record: writerRecord(0), Commit: c.commitFor(0)}); err == nil {
		t.Error("Enqueue accepted a record after shutdown; the job would be stranded on a closed queue")
	}
	if got := c.settled(); got != 0 {
		t.Errorf("a refused job settled %d time(s), want 0", got)
	}
}

// TestWriterGoroutinesExitOnShutdown is the goleak guard: once Start returns,
// every worker, the scope worker and every timer-driven goroutine the writer
// created is gone.
//
// A leaked worker is not a leaked goroutine only. It holds the object it was
// filling and the commit callbacks in it, which is the shape of a slow leak that
// also quietly drops records.
func TestWriterGoroutinesExitOnShutdown(t *testing.T) {
	leaked := goleak.IgnoreCurrent()

	r := startWriter(t, Config{
		MaxObjectBytes: testRotationBytes,
		MaxObjectAge:   testShortAge,
		Workers:        4,
		QueueSize:      64,
	})
	c := newCommitLog()
	r.enqueue(t, c, writerRecords(32))
	if err := r.w.EnqueueScopeEvent(r.ctx, testScopeEvent(sink.ScopeActionStarted)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}
	waitFor(t, "every job to settle", func() bool { return c.settled() == 32 })
	r.stop(t)

	goleak.VerifyNone(t, leaked)
}

// TestZeroConfigIsADefaultedConfig: a Config left empty behaves like a defaulted
// CR rather than like a broken sink — a queue that cannot hold anything, a
// worker pool of none, or an object that is full at zero bytes would each be a
// writer that silently does nothing.
func TestZeroConfigIsADefaultedConfig(t *testing.T) {
	w := NewWriter(newFakeStore(), Config{Bucket: "b"}, newTestMetrics())

	if got := cap(w.jobs); got != defaultQueueSize {
		t.Errorf("queue capacity is %d, want the default %d", got, defaultQueueSize)
	}
	if w.workers != defaultWorkers {
		t.Errorf("workers is %d, want the default %d", w.workers, defaultWorkers)
	}
	if w.maxObjectBytes != defaultMaxObjectBytes {
		t.Errorf("maxObjectBytes is %d, want the default %d", w.maxObjectBytes, defaultMaxObjectBytes)
	}
	if w.maxObjectAge != defaultMaxObjectAge {
		t.Errorf("maxObjectAge is %s, want the default %s", w.maxObjectAge, defaultMaxObjectAge)
	}
	if w.enqueueTimeout != defaultEnqueueTimeout {
		t.Errorf("enqueueTimeout is %s, want the default %s", w.enqueueTimeout, defaultEnqueueTimeout)
	}
	if w.drainTimeout != defaultDrainTimeout {
		t.Errorf("drainTimeout is %s, want the default %s", w.drainTimeout, defaultDrainTimeout)
	}
}

// objectKeys renders a store's object keys for a failure message.
func objectKeys(objects []storeEntry) []string {
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, obj.key)
	}
	return keys
}
