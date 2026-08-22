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

package conformance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// fakeWriter is a minimal but genuinely compliant sink.Writer over an in-memory
// backend, with switches that break one contract obligation each.
//
// It carries the whole weight of this package's own credibility. Run against the
// compliant configuration, it proves the suite can be passed at all — without
// that, a suite that failed everything would look identical to a rigorous one.
// Run against each broken configuration, it proves the corresponding property
// actually objects. It is also the shortest worked example of what a Writer has
// to do, which is what a new backend will read first.
//
// Its shape mirrors the one implementation that exists: a bounded hand-off queue,
// a small pool of batching workers, and a shutdown that stops accepting, waits for
// in-flight senders, drains, and only then closes the backend.
type fakeWriter struct {
	opts fakeOpts

	jobs           chan sink.Job
	workers        int
	batchMax       int
	batchWait      time.Duration
	enqueueTimeout time.Duration
	retries        int
	retryPause     time.Duration

	// mu guards closing; inflight tracks Enqueue calls that passed the closing
	// check and may therefore still send, so close(jobs) can never race a send.
	mu       sync.Mutex
	closing  bool
	inflight sync.WaitGroup
	// hardStop is closed at the very start of shutdown, before anything waits on
	// anything: it releases a blocked Enqueue (so the drain cannot deadlock behind
	// one) and, for the drop-on-drain fixture, tells the workers to abandon
	// whatever they are holding.
	hardStop chan struct{}

	faultMu sync.Mutex
	fault   FaultFunc

	evMu   sync.Mutex
	events []Event

	// stamp makes each stored record unique for the non-idempotent fixture.
	stamp atomic.Int64
}

// fakeOpts selects which obligation this writer violates. The zero value is the
// compliant writer.
type fakeOpts struct {
	// doubleCommit settles every job twice — the corruption D11 exists to catch.
	doubleCommit bool
	// dropOnDrain abandons queued and buffered work when shutdown begins, closing
	// the backend without flushing and without settling those jobs.
	dropOnDrain bool
	// lyingCommit reports every job as durably written, whatever the backend said.
	lyingCommit bool
	// unboundedEnqueue ignores the enqueue timeout and blocks on a full queue
	// until shutdown.
	unboundedEnqueue bool
	// nonIdempotent re-stamps each record as it is stored, so a re-written record
	// is a second logical record rather than a collapsible duplicate.
	nonIdempotent bool
}

// The tuning the fixtures run with: small enough that several batches and both
// workers are exercised by suiteJobs, fast enough that a whole property settles
// in well under a second against a healthy backend.
const (
	fakeQueueCapacity  = 64
	fakeWorkers        = 2
	fakeBatchMax       = 8
	fakeBatchWait      = 20 * time.Millisecond
	fakeEnqueueTimeout = time.Second
	fakeRetries        = 2
	fakeRetryPause     = 5 * time.Millisecond
	fakeSettleWithin   = 3 * time.Second
)

var errFakeQueueFull = errors.New("fake: write queue still full")
var errFakeShuttingDown = errors.New("fake: shutting down, refusing new write")

func newFakeWriter(opts fakeOpts) *fakeWriter {
	return &fakeWriter{
		opts:           opts,
		jobs:           make(chan sink.Job, fakeQueueCapacity),
		workers:        fakeWorkers,
		batchMax:       fakeBatchMax,
		batchWait:      fakeBatchWait,
		enqueueTimeout: fakeEnqueueTimeout,
		retries:        fakeRetries,
		retryPause:     fakeRetryPause,
		hardStop:       make(chan struct{}),
	}
}

// newFakeHarness wires a fakeWriter up as a Harness.
func newFakeHarness(opts fakeOpts) Harness {
	w := newFakeWriter(opts)
	return Harness{
		Writer:         w,
		Events:         w.snapshot,
		SetFault:       w.setFault,
		LogicalKey:     fakeLogicalKey,
		Dedup:          DedupMergeCollapse,
		QueueCapacity:  fakeQueueCapacity,
		EnqueueTimeout: fakeEnqueueTimeout,
		SettleWithin:   fakeSettleWithin,
	}
}

// fakeLogicalKey is this backend's dedup key, modelled on the one real backend's:
// the identity plus the instant, so two events for the same object are two
// records and a re-write of one event is the same record.
func fakeLogicalKey(rec sink.Record) string {
	return strings.Join([]string{
		rec.ClusterID, rec.APIGroup, rec.Kind, rec.Namespace, rec.Name,
		rec.Timestamp.UTC().Format(time.RFC3339Nano),
	}, "|")
}

func (w *fakeWriter) setFault(f FaultFunc) {
	w.faultMu.Lock()
	defer w.faultMu.Unlock()
	w.fault = f
}

// snapshot returns a copy of the observation log; the suite reads it while the
// workers are still appending to it.
func (w *fakeWriter) snapshot() []Event {
	w.evMu.Lock()
	defer w.evMu.Unlock()
	out := make([]Event, len(w.events))
	copy(out, w.events)
	return out
}

func (w *fakeWriter) record(ev Event) {
	w.evMu.Lock()
	defer w.evMu.Unlock()
	w.events = append(w.events, ev)
}

// Enqueue is the bounded hand-off: it never blocks past its timeout, the caller's
// deadline, or shutdown — except for the fixture built to do exactly that.
func (w *fakeWriter) Enqueue(ctx context.Context, job sink.Job) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return errFakeShuttingDown
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	// A nil channel never fires, which is how the unbounded fixture ignores its
	// own timeout without also ignoring shutdown (that would deadlock the drain
	// rather than fail the property).
	var full <-chan time.Time
	if !w.opts.unboundedEnqueue {
		timer := time.NewTimer(w.enqueueTimeout)
		defer timer.Stop()
		full = timer.C
	}

	select {
	case w.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-full:
		return errFakeQueueFull
	case <-w.hardStop:
		return errFakeShuttingDown
	}
}

// Start runs the workers until ctx is cancelled, then shuts down in the order the
// contract requires: stop accepting, wait for senders, close the queue, let the
// workers drain it, and only then close the backend.
func (w *fakeWriter) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for range w.workers {
		wg.Go(func() { w.worker(ctx) })
	}

	<-ctx.Done()

	w.mu.Lock()
	w.closing = true
	w.mu.Unlock()
	close(w.hardStop)
	w.inflight.Wait()
	close(w.jobs)
	wg.Wait()

	w.record(Event{Kind: EventClose})
	return nil
}

// worker accumulates jobs into a batch and flushes it on size, on the batch timer,
// or on the final (closed) receive — the drain flush the contract turns on.
func (w *fakeWriter) worker(ctx context.Context) {
	batch := make([]sink.Job, 0, w.batchMax)
	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}
	defer disarm()

	// Non-nil only for the fixture that abandons work at shutdown; nil otherwise,
	// so the compliant writer's select cannot take this branch at all.
	var abandon <-chan struct{}
	if w.opts.dropOnDrain {
		abandon = w.hardStop
	}

	for {
		select {
		case <-abandon:
			return
		case job, ok := <-w.jobs:
			if !ok {
				if len(batch) > 0 {
					w.flush(ctx, batch)
				}
				return
			}
			batch = append(batch, job)
			if len(batch) == 1 {
				timer = time.NewTimer(w.batchWait)
				timerC = timer.C
			}
			if len(batch) >= w.batchMax {
				w.flush(ctx, batch)
				batch = batch[:0]
				disarm()
			}
		case <-timerC:
			timer = nil
			timerC = nil
			w.flush(ctx, batch)
			batch = batch[:0]
		}
	}
}

// flush attempts one batch, retries it a bounded number of times, and settles
// every job in it exactly once — the whole contract, in one place, which is what
// makes settling it twice a one-line fixture rather than a redesign.
func (w *fakeWriter) flush(ctx context.Context, batch []sink.Job) {
	records := make([]sink.Record, len(batch))
	for i, job := range batch {
		records[i] = w.stored(job.Record)
	}

	var err error
	for attempt := range w.retries + 1 {
		if attempt > 0 {
			time.Sleep(w.retryPause)
		}
		if err = w.attempt(ctx, records); err == nil {
			break
		}
	}

	ok := err == nil
	if w.opts.lyingCommit {
		ok = true
	}
	for _, job := range batch {
		job.Commit(ok)
		if w.opts.doubleCommit {
			job.Commit(ok)
		}
	}
}

// attempt consults the fault and logs what happened. ErrLostAck is recorded as
// the error it is; Event.Durable is what decides that its records still landed.
func (w *fakeWriter) attempt(ctx context.Context, records []sink.Record) error {
	w.faultMu.Lock()
	f := w.fault
	w.faultMu.Unlock()

	var err error
	if f != nil {
		err = f(ctx, records)
	}
	stored := make([]sink.Record, len(records))
	copy(stored, records)
	w.record(Event{Kind: EventWrite, Records: stored, Err: err})
	return err
}

// stored renders a record as this backend keeps it: unchanged, unless the
// non-idempotent fixture is re-stamping it so no two writes of the same record
// can ever collapse.
func (w *fakeWriter) stored(rec sink.Record) sink.Record {
	if w.opts.nonIdempotent {
		rec.Timestamp = rec.Timestamp.Add(time.Duration(w.stamp.Add(1)) * time.Nanosecond)
	}
	return rec
}
