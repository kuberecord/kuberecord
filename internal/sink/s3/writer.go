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
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The write path's defaults. Unlike the ClickHouse writer's, they are not
// exported: every value reaching Config comes from an S3Sink CR whose CRD
// already carries the defaults an author sees (see api/v1alpha1.S3WriterSpec and
// S3RotationSpec), so there is no flag help text for a Go constant to be the
// single source of truth for. They exist to make a zero-valued Config behave like
// a defaulted CR rather than like a broken one, which is what keeps a
// test-constructed writer honest.
const (
	defaultQueueSize      = 5000
	defaultWorkers        = 4
	defaultMaxObjectBytes = 64 << 20
	defaultMaxObjectAge   = 5 * time.Minute
	defaultEnqueueTimeout = 2 * time.Second
	defaultDrainTimeout   = 15 * time.Second
)

const (
	// defaultPutTimeout bounds one PUT attempt. It is generous next to
	// ClickHouse's five-second insert timeout because the unit of work is not
	// comparable: a full object at the default rotation is 64Mi of body on the
	// wire, and an object store's tail latency for one is seconds, not
	// milliseconds. It is not a CR knob — an operator sizing rotation is already
	// choosing how big a request this is, and a second control over the same
	// trade-off would only make the two disagree.
	defaultPutTimeout = 60 * time.Second

	// defaultMaxRetryBackoff bounds how long one object is retried before its
	// records are settled as failed and handed back to the caller's own
	// requeue/backoff. It matches the ClickHouse writer's for the same reason it
	// exists there: an object that cannot be written for a minute is not going to
	// be written by this worker holding on to it, and the pipeline's version-gated
	// revert is a better place to wait than a worker that is not draining its
	// queue meanwhile.
	defaultMaxRetryBackoff = 60 * time.Second
)

// ObjectLock is an S3Sink's spec.objectLock, resolved for the write path: the
// retention mode and how long from the PUT each object is retained.
//
// It is separate from Retention (which carries an absolute instant) because these
// two are different things: this is the *policy*, stable for the sink's lifetime,
// and Retention is one object's resolution of it, fixed when that object is built
// so a retried PUT repeats the request rather than re-dating it.
type ObjectLock struct {
	// Mode is "GOVERNANCE" or "COMPLIANCE", exactly as spec.objectLock.mode
	// spells it.
	Mode string
	// RetainDays is how long, in days from the PUT, each object is retained.
	RetainDays int32
}

// Config is everything the S3 write path is built from, with the S3Sink CR's
// spec already resolved into it.
//
// It deliberately carries no connection or credential settings: those belong to
// whatever constructs the ObjectStore, which is Task 6.4's credential resolution
// and client factory. Keeping them out means this package has no opinion on how a
// bucket is reached, which is what lets MinIO and S3 be the same code path here
// and what keeps the writer testable without a network client.
//
// Every non-positive numeric or duration field falls back to the package default,
// so a zero Config is a working one.
type Config struct {
	// Bucket and Prefix come from spec.bucket and spec.prefix. Prefix is a path
	// fragment with no leading or trailing slash (the CRD's Pattern enforces it);
	// empty is an ordinary configuration and simply contributes no key segment.
	Bucket string
	Prefix string

	// MaxObjectBytes and MaxObjectAge are spec.rotation. The first is measured on
	// the *encoded* payload — see objectBuilder for how that is tracked, and why
	// it is the only reading under which this sink's documented memory ceiling is
	// true. The second is measured from the arrival of the object's first record,
	// not from that record's own timestamp: it bounds how long data sits only in
	// memory, which is a property of this process, not of the audit trail.
	MaxObjectBytes int64
	MaxObjectAge   time.Duration

	// ObjectLock is spec.objectLock, or nil when the sink applies no per-object
	// retention of its own.
	ObjectLock *ObjectLock

	// QueueSize, Workers, EnqueueTimeout and DrainTimeout are spec.writer. Each
	// worker accumulates its own object, so Workers multiplies both the memory
	// ceiling and the object count — see api/v1alpha1.S3WriterSpec.Workers.
	QueueSize      int
	Workers        int
	EnqueueTimeout time.Duration
	DrainTimeout   time.Duration
}

// Metrics is the narrow slice of pipeline metrics this writer records. It is an
// interface, and structurally the same one the ClickHouse writer declares, so
// this package never imports internal/pipeline and an S3 sink lights up exactly
// the same per-sink series as a ClickHouse one — the caller passes
// pipeline.PipelineMetrics.ForSink(id) to either.
//
// The one series whose meaning shifts is write_batch_rows: for this backend a
// "batch" is an object, so it reports how many records each object carries. That
// is the same quantity read the same way (how much work one write settles), and
// inventing a parallel object-size series would split one dashboard panel into
// two that must never be summed.
type Metrics interface {
	// SetWriteQueueDepth publishes the current hand-off queue depth.
	SetWriteQueueDepth(n float64)
	// SetWriteQueueCapacity publishes the fixed hand-off queue capacity.
	SetWriteQueueCapacity(n float64)
	// ObserveEnqueueBlock records how long an Enqueue blocked waiting for room.
	ObserveEnqueueBlock(seconds float64)
	// IncEnqueueTimeout counts an Enqueue that gave up because the queue stayed full.
	IncEnqueueTimeout()
	// ObserveWriteLatency records a job's settle latency (the PUT that carried it).
	ObserveWriteLatency(seconds float64)
	// IncWriteRetryAttempt counts one PUT attempt beyond the first.
	IncWriteRetryAttempt()
	// IncWrite counts one settled write by outcome ("success" | "failed").
	IncWrite(outcome string)
	// ObserveWriteBatchRows records how many records one written object carried.
	ObserveWriteBatchRows(rows float64)
}

// writeJob is one record on its way into an object, as a worker sees it: the
// JSONL line it contributes, the two fields that fix the object's partition, and
// the callback that settles it.
//
// The record itself is deliberately not here. It is rendered to its line once, on
// the enqueue path, and then released — so what a worker holds while it fills a
// 64Mi object is that object's compressed bytes plus one line per queued job,
// rather than a queue of inflated Records. It is the same reason the ClickHouse
// writer renders its insert arguments at enqueue time.
type writeJob struct {
	line      []byte
	clusterID string
	ts        time.Time
	commit    func(ok bool)
}

// openObject is the object a worker is currently filling: the frame being built
// and the commit callbacks it has taken responsibility for.
//
// The two are one struct because they must be settled together and exactly once:
// every callback in commits belongs to a line already inside builder's frame, so
// whatever happens to the object happens to all of them — one PUT, one outcome,
// one commit each.
type openObject struct {
	builder *objectBuilder
	commits []func(bool)
}

// append puts one job's line into the object, taking on its commit callback only
// once the line is really in the frame. On any error the job is untouched and
// still unsettled, which is what lets the caller decide between rotating (a
// cluster mismatch) and abandoning (a frame that can no longer be finished).
func (o *openObject) append(job writeJob) error {
	if err := o.builder.append(job.line, job.clusterID, job.ts); err != nil {
		return err
	}
	o.commits = append(o.commits, job.commit)
	return nil
}

// records is how many records the object holds.
func (o *openObject) records() int { return len(o.commits) }

// Writer streams records to an object store as rotated, zstd-compressed JSONL
// objects. It implements sink.Writer, sink.ScopeEventWriter (see scopewriter.go)
// and sink.Prober (see instance.go), and shares one ObjectStore across all
// three.
//
// It is structurally simpler than the ClickHouse writer in the one way that
// matters: an object is written by a single atomic PUT, which is visible with all
// of its records or with none of them. There is no partial-success outcome, so
// there is no per-record isolation phase to run when a write fails — a failed
// object failed for every record in it equally, and blaming one of them
// individually would be inventing information the backend never gave us.
//
// It does not implement sink.StateReader. That is a declared capability limit,
// not an omission — see instance.go, which names the consequences.
type Writer struct {
	store  ObjectStore
	bucket string
	prefix string

	maxObjectBytes int64
	maxObjectAge   time.Duration
	objectLock     *ObjectLock

	jobs    chan writeJob
	workers int

	// scopeEvents is the dedicated hand-off queue for watch-scope epoch
	// transitions, drained by a single scopeWorker (see scopewriter.go). It is
	// deliberately not the jobs channel: a scope epoch must not queue behind a
	// backlog of records, and it has no commit contract to settle.
	scopeEvents chan sink.ScopeEvent
	// scopeRetryMu guards scopeRetries, the events whose object could not be
	// written and that the scope worker re-attempts on its next tick. A scope
	// transition happens once and cannot be re-derived, so it is retried rather
	// than dropped.
	scopeRetryMu sync.Mutex
	scopeRetries []sink.ScopeEvent
	// scopeMaxRetryBackoff bounds one scope flush's retry window before its events
	// go back on scopeRetries. A field rather than the constant so tests can
	// shorten it without waiting out production backoff.
	scopeMaxRetryBackoff time.Duration

	enqueueTimeout time.Duration
	// putTimeout bounds one PUT attempt and maxRetryBackoff bounds the retries of
	// one object. Both are fields rather than the constants directly so tests can
	// drive the retry and give-up paths without waiting out production values.
	putTimeout      time.Duration
	maxRetryBackoff time.Duration
	drainTimeout    time.Duration

	// mu guards closing and drainCtx; see Enqueue/attemptContext.
	mu      sync.Mutex
	closing bool
	// inflight tracks Enqueue / EnqueueScopeEvent calls that observed
	// closing==false and are therefore permitted to send; Start waits for it to
	// drain to zero before closing either channel, so a send can never race a
	// close.
	inflight sync.WaitGroup
	// drainCtx is swapped from a plain context.Background() to a
	// drainTimeout-bounded context the moment Start detects shutdown — see
	// attemptContext. Starts non-nil so a worker that reads it before Start ever
	// swaps it still gets a safe, non-nil context.
	drainCtx context.Context
	// otherUsers tracks in-flight uses of store that are NOT the worker pool —
	// today the health probe, which writes through the same client. Start waits
	// for it after its own workers finish and before closing store, so shutdown
	// can never race a use of the shared client against its closure.
	otherUsers sync.WaitGroup
	// lockVerified records that a probe has already had this bucket accept an
	// object carrying this sink's Object Lock retention, so later probes need not
	// keep writing retained objects to re-ask a question whose answer cannot
	// change. Guarded by mu, like closing; see Probe for why once is enough.
	lockVerified bool

	// metrics records queue depth/capacity, enqueue blocking and timeouts, and
	// per-object write latency, retries and outcomes. Never nil once built via
	// NewWriter.
	metrics Metrics
}

// NewWriter builds a Writer over an existing object store. It does not start it:
// the sink runtime owns the lifecycle (internal/sink.SinkManager), and the Writer
// it returns is a manager.Runnable via Start.
//
// The store is owned by the returned Writer and closed on shutdown, so a caller
// must not close it or share it with another Writer.
func NewWriter(store ObjectStore, cfg Config, metrics Metrics) *Writer {
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	maxObjectBytes := cfg.MaxObjectBytes
	if maxObjectBytes <= 0 {
		maxObjectBytes = defaultMaxObjectBytes
	}
	maxObjectAge := cfg.MaxObjectAge
	if maxObjectAge <= 0 {
		maxObjectAge = defaultMaxObjectAge
	}
	enqueueTimeout := cfg.EnqueueTimeout
	if enqueueTimeout <= 0 {
		enqueueTimeout = defaultEnqueueTimeout
	}
	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}
	return &Writer{
		store:                store,
		bucket:               cfg.Bucket,
		prefix:               cfg.Prefix,
		maxObjectBytes:       maxObjectBytes,
		maxObjectAge:         maxObjectAge,
		objectLock:           cfg.ObjectLock,
		jobs:                 make(chan writeJob, queueSize),
		workers:              workers,
		scopeEvents:          make(chan sink.ScopeEvent, scopeQueueSize),
		scopeMaxRetryBackoff: defaultScopeMaxRetryBackoff,
		enqueueTimeout:       enqueueTimeout,
		putTimeout:           defaultPutTimeout,
		maxRetryBackoff:      defaultMaxRetryBackoff,
		drainTimeout:         drainTimeout,
		drainCtx:             context.Background(),
		metrics:              metrics,
	}
}

// Enqueue implements sink.Writer. It renders the record to its JSONL line here,
// once, and hands the line to the bounded queue — so no worker ever touches a
// sink.Record and the caller learns immediately about a record that cannot be
// encoded at all.
//
// A record that cannot be rendered, or that carries no cluster_id, is refused
// rather than accepted-and-failed. The distinction matters to the caller: a
// refusal is an error it propagates (and the pipeline's requeue/backoff takes
// over), whereas an accepted job that later settles false has already been
// counted as a write attempt and reverted through the cache. Neither loses data,
// but only one of them says what actually happened.
//
// If the queue is full it waits up to the configured enqueue timeout for room
// before giving up — a job is never dropped silently (Invariant 1).
func (w *Writer) Enqueue(ctx context.Context, job sink.Job) error {
	if job.Record.ClusterID == "" {
		return fmt.Errorf("s3writer: refusing a record with an empty cluster_id (%s): "+
			"every object is partitioned by cluster_id and this one has none", recordRef(job.Record))
	}
	line, err := marshalRecordLine(job.Record)
	if err != nil {
		return fmt.Errorf("s3writer: %w", err)
	}
	internal := writeJob{
		line:      line,
		clusterID: job.Record.ClusterID,
		ts:        job.Record.Timestamp,
		commit:    job.Commit,
	}

	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("s3writer: shutting down, refusing new write")
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	timer := time.NewTimer(w.enqueueTimeout)
	defer timer.Stop()

	// enqueue_block_seconds measures how long the hot path actually waited for
	// room, whether the send eventually succeeded or timed out.
	start := time.Now()
	select {
	case w.jobs <- internal:
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		w.metrics.SetWriteQueueDepth(float64(len(w.jobs)))
		return nil
	case <-ctx.Done():
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		return ctx.Err()
	case <-timer.C:
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		w.metrics.IncEnqueueTimeout()
		return fmt.Errorf("s3writer: write queue still full after waiting %s", w.enqueueTimeout)
	}
}

// Start implements manager.Runnable and sink.Writer. It runs the worker pool
// until ctx is cancelled, then shuts down in the same strict order the ClickHouse
// writer does, for the same reason — so no write is ever stranded or raced
// against the client's closure:
//  1. Stop accepting new Enqueue calls (under mu, so this can't race a send).
//  2. Swap in a fresh, drainTimeout-bounded drainCtx for any job processed from
//     here on — see attemptContext for why the original ctx (already cancelled by
//     this point) can't be reused for these attempts.
//  3. Wait for any Enqueue / EnqueueScopeEvent call already past the closing
//     check to finish sending (or bail via its own ctx/timeout) — after this,
//     neither queue can receive further sends from anyone.
//  4. Close jobs and scopeEvents. Each worker receives until its channel is
//     drained and closed, then closes and PUTs the partial object it still holds
//     before returning — which is what makes the drain flush a half-full object
//     rather than lose it.
//  5. Wait for otherUsers — the health probe shares the client.
//  6. Close store — guaranteed safe now, since nothing can still be using it.
func (w *Writer) Start(ctx context.Context) error {
	log := logf.Log.WithName("s3writer")

	// Capacity is fixed for this Writer's lifetime; publishing it here lets
	// dashboards express depth as a fraction of it.
	w.metrics.SetWriteQueueCapacity(float64(cap(w.jobs)))

	var wg sync.WaitGroup
	for range w.workers {
		wg.Go(func() {
			w.worker(ctx, log)
		})
	}
	// One scope worker, always: per-scope epoch ordering depends on a single
	// drainer (see scopeWorker). It joins the same WaitGroup so the drain phase
	// covers scope events too.
	wg.Go(func() {
		w.scopeWorker(ctx, log)
	})

	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), w.drainTimeout)
	defer cancel()

	w.mu.Lock()
	w.closing = true
	w.drainCtx = drainCtx
	w.mu.Unlock()
	w.inflight.Wait()
	close(w.jobs)
	close(w.scopeEvents)
	wg.Wait()

	w.otherUsers.Wait()

	if err := w.store.Close(); err != nil {
		log.Error(err, "s3writer: failed to close the object store client")
		return err
	}
	log.Info("s3writer: object store client closed")
	return nil
}

// attemptContext returns ctx unchanged while it's still live, so a PUT that is
// genuinely in flight when shutdown begins is cancelled promptly rather than
// allowed to run past the manager's own shutdown deadline. Once ctx has already
// fired — meaning this object is being closed during Start's post-shutdown drain,
// not interrupted mid-attempt — deriving its PUT's timeout from ctx would
// guarantee an instant, no-chance failure regardless of the store's actual
// health, defeating the drain phase's whole purpose. Such objects get drainCtx
// instead: one context.WithTimeout(drainTimeout) shared by every object written
// during shutdown, so the whole drain phase (not each object individually) is
// what's bounded.
func (w *Writer) attemptContext(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.drainCtx
}

// worker drains w.jobs into objects, closing the one it is filling when the
// object's encoded size reaches maxObjectBytes or maxObjectAge has elapsed since
// its first record arrived, whichever comes first.
//
// The age timer is armed only while an object is open (an empty worker selects on
// a nil channel), so an idle worker never busy-waits and never fires a flush with
// nothing to write — the same discipline the ClickHouse writer's batchMaxWait
// timer follows.
//
// Trickle traffic accepts maxObjectAge as its write-latency ceiling: a lone
// record waits up to maxObjectAge for object-mates that never arrive before its
// object is closed and written. That is also the bound on how much of the audit
// trail exists only in this process's memory, which is why the CRD caps it at an
// hour.
//
// On the final receive (w.jobs closed and drained) the worker closes and writes
// the partial object it still holds, within Start's drain window, so a shutdown
// never strands a half-full object.
//
//nolint:logcheck
func (w *Writer) worker(ctx context.Context, log logr.Logger) {
	var open *openObject

	// timerC is nil while no object is open, so the select below cannot fire an
	// age-driven flush on nothing and does not busy-wait when idle.
	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}
	// closeOpen writes whatever is open (settling every job in it) and disarms the
	// age timer. It is the only way an object leaves this worker.
	closeOpen := func(attemptCtx context.Context) {
		if open != nil {
			w.flush(attemptCtx, log, open)
			open = nil
		}
		disarm()
	}

	for {
		select {
		case job, ok := <-w.jobs:
			if !ok {
				// Channel closed and drained: write what we still hold (within
				// Start's drain window) and exit.
				closeOpen(w.attemptContext(ctx))
				return
			}
			// Two attempts at most: the second is against a freshly opened object,
			// which accepts any record the first could not.
			for attempt := range 2 {
				if open == nil {
					builder, err := newObjectBuilder(w.maxObjectBytes)
					if err != nil {
						log.Error(err, "s3writer: cannot open an object, abandoning the record")
						w.settle(job.commit, false)
						break
					}
					open = &openObject{builder: builder}
					// First record of a new object: arm the age ceiling now, not
					// before — an idle worker has no deadline to keep.
					timer = time.NewTimer(w.maxObjectAge)
					timerC = timer.C
				}

				err := open.append(job)
				switch {
				case err == nil:
					if open.builder.full() {
						closeOpen(w.attemptContext(ctx))
					}
				case errors.Is(err, errClusterMismatch) && attempt == 0:
					// Unreachable while one process serves one cluster (Invariant
					// 7). Handled by rotating rather than by refusing the record,
					// so the worst case is an extra object instead of a partition
					// filed under another cluster's id.
					log.Error(err, "s3writer: rotating early, the record belongs to another cluster")
					closeOpen(w.attemptContext(ctx))
					continue
				default:
					// The frame can no longer be finished, so nothing in it will
					// ever be written: settle every job it holds — and this one —
					// exactly once, and start over. Writing a frame we could not
					// complete would be worse than losing it, because it would
					// look complete to every reader.
					log.Error(err, "s3writer: abandoning the object being built", "records", open.records())
					w.abandon(log, open)
					w.settle(job.commit, false)
					open = nil
					disarm()
				}
				break
			}
		case <-timerC:
			// maxObjectAge elapsed on an open object: close it. It is non-empty by
			// construction (the timer is armed when the first record lands and
			// disarmed on every close).
			timer = nil
			timerC = nil
			closeOpen(w.attemptContext(ctx))
		}
	}
}

// flush closes the open object, writes it, and settles every job in it. It is
// the sole place a record's outcome is decided on the success path, and — with
// abandon — one of exactly two places any outcome is decided at all.
//
// Exactly-once is a property of the commit callbacks, not of the PUT: the PUT
// itself is at-least-once, because a timeout or a reset connection after the
// object landed is indistinguishable from one before it. What makes that harmless
// is the object key: a retry rebuilds nothing, it re-sends the same bytes under
// the same content-addressed key, so every reader of the archive sees one object
// either way (D15) — on an unversioned bucket because the second PUT replaces the
// first, on a versioned one because the second version becomes the current one
// and holds identical bytes (see docs/RETENTION.md for what the bucket stores in
// that case). Every commit still fires exactly once regardless.
//
// There is deliberately no per-record isolation phase. A PUT has no partial
// outcome to isolate: the object is visible with all its records or with none, so
// re-attempting records individually would multiply objects without learning
// anything the store did not already tell us.
//
//nolint:logcheck
func (w *Writer) flush(ctx context.Context, log logr.Logger, open *openObject) {
	// An object leaves the queue as it is assembled, so refresh depth here too —
	// not only on enqueue — so a draining queue is reflected, not just a filling
	// one. write_batch_rows is emitted once per object.
	w.metrics.SetWriteQueueDepth(float64(len(w.jobs)))
	records := open.records()
	if records == 0 {
		// Unreachable: an object is opened by its first record and closed on the
		// way out. Returning rather than building keeps errEmptyBatch a statement
		// about callers of Encode, not a log line nobody can act on.
		return
	}
	w.metrics.ObserveWriteBatchRows(float64(records))

	obj, err := open.builder.build(w.prefix)
	if err != nil {
		log.Error(err, "s3writer: could not finish an object, abandoning its records", "records", records)
		w.settleAll(open.commits, false)
		return
	}

	start := time.Now()
	putErr := w.put(ctx, obj)
	elapsed := time.Since(start).Seconds()
	if putErr != nil {
		log.Error(putErr, "s3writer: giving up on an object after retries",
			"bucket", w.bucket, "key", obj.Key, "records", records, "bytes", len(obj.Payload))
	} else {
		log.V(1).Info("s3writer: wrote object",
			"bucket", w.bucket, "key", obj.Key, "records", records, "bytes", len(obj.Payload))
	}
	for _, commit := range open.commits {
		w.metrics.ObserveWriteLatency(elapsed)
		w.settle(commit, putErr == nil)
	}
}

// abandon settles every job in an object that will never be written and releases
// its frame. It is the other of the two places an outcome is decided, and it
// exists so that "the object could not be built" reaches the caller as a failed
// write it can requeue rather than as a job that silently never settles.
//
//nolint:logcheck
func (w *Writer) abandon(log logr.Logger, open *openObject) {
	w.settleAll(open.commits, false)
	if err := open.builder.discard(); err != nil {
		log.Error(err, "s3writer: failed to release an abandoned object's frame")
	}
}

// settle fires one commit callback, once, and counts the write it settled.
//
// It is the only place this package calls a commit callback or the writes_total
// counter, which is what makes "exactly one commit per job" — and "every settled
// job is counted exactly once" — a property of a handful of call sites rather
// than of the whole file. A nil callback is tolerated (sink.Job's is optional)
// and still counted: the write happened either way.
func (w *Writer) settle(commit func(bool), ok bool) {
	if ok {
		w.metrics.IncWrite("success")
	} else {
		w.metrics.IncWrite("failed")
	}
	if commit != nil {
		commit(ok)
	}
}

// settleAll fires every callback in a closed object, once each.
func (w *Writer) settleAll(commits []func(bool), ok bool) {
	for _, commit := range commits {
		w.settle(commit, ok)
	}
}

// put writes one object, retrying the whole object with the shared exponential
// backoff. Each attempt's deadline is derived from ctx (see attemptContext), so a
// manager shutdown cancels an in-flight PUT immediately instead of letting it run
// for a full putTimeout. It never touches commit callbacks — flush owns settling.
//
// The request is built once, before the first attempt, so every retry re-sends a
// byte-identical body under an identical key with an identical retention header.
// That is what makes a retried write leave exactly one *current* object rather
// than one per attempt. A versioned bucket still records a version per accepted
// PUT — S3 has no idempotent PUT, and a retained version does not refuse one —
// so the duplicate is a second version of the same key holding the same bytes,
// invisible to a reader and billed for until it can be expired (D15,
// docs/RETENTION.md).
func (w *Writer) put(ctx context.Context, obj Object) error {
	in := PutObjectInput{
		Bucket:    w.bucket,
		Key:       obj.Key,
		Body:      obj.Payload,
		Retention: w.retention(),
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.maxRetryBackoff

	attempts := 0
	return backoff.Retry(func() error {
		// Every attempt after the first is a retry; counting them here (rather
		// than the successes) is what makes a retry storm visible even when the
		// object ultimately lands.
		if attempts > 0 {
			w.metrics.IncWriteRetryAttempt()
		}
		attempts++
		return w.putOnce(ctx, in)
	}, backoff.WithContext(eb, ctx))
}

// putOnce performs a single PUT attempt under its own putTimeout.
func (w *Writer) putOnce(ctx context.Context, in PutObjectInput) error {
	attemptCtx, cancel := context.WithTimeout(ctx, w.putTimeout)
	defer cancel()
	return w.store.PutObject(attemptCtx, in)
}

// retention resolves this sink's Object Lock policy for one object, or nil when
// it has none.
//
// The instant is taken now, when the object is built, and then travels with the
// request through every retry — see Retention. It is stamped from the wall clock
// rather than from the records' timestamps because retention is a property of
// when the archive received the object, not of when the events in it happened: a
// warm-up close-out dated from history must not arrive already expired.
func (w *Writer) retention() *Retention {
	if w.objectLock == nil {
		return nil
	}
	return &Retention{
		Mode:        w.objectLock.Mode,
		RetainUntil: time.Now().UTC().Add(time.Duration(w.objectLock.RetainDays) * 24 * time.Hour),
	}
}
