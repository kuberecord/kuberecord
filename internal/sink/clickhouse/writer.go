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

// Package clickhouse is the ClickHouse implementation of the sink contract
// (internal/sink). CHWriter implements sink.Writer (batched, asynchronous
// resource_states inserts off the caller's hot path), sink.ScopeEventWriter (a
// separate small batcher for watch_scopes epoch rows) and sink.StateReader (cache
// warm-up history plus the scope-epoch reads), and owns the single shared
// ClickHouse connection for all of them, plus the schema introspection its
// sink.Prober half answers health probes with.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// These are the tunable-knob defaults for the async write path. They are
// exported so cmd/main.go can advertise the exact same values as its
// flag/env defaults (--writer-* and their WRITER_* twins, Task 0.8) — a single
// source of truth, so the documented default, the --help text, and the
// behavior when a knob is left at zero can never drift apart. NewCHWriter still
// clamps any non-positive value back to these, so passing 0 anywhere is
// equivalent to passing the default.
const (
	// DefaultWriteQueueSize is the hand-off queue capacity (jobs).
	DefaultWriteQueueSize = 5000
	// DefaultWriteWorkers is the number of queue-draining workers.
	DefaultWriteWorkers = 4
	// DefaultBatchMaxRows is the row count that flushes an accumulating batch.
	DefaultBatchMaxRows = 1000
	// DefaultBatchMaxWait is the maximum time a batch's first job waits for the
	// batch to fill before it is flushed regardless.
	DefaultBatchMaxWait = 1 * time.Second
	// DefaultEnqueueTimeout is how long Enqueue waits for queue room before
	// giving up (never dropping the job silently).
	DefaultEnqueueTimeout = 2 * time.Second
	// DefaultShutdownDrainTimeout bounds the post-shutdown drain phase.
	DefaultShutdownDrainTimeout = 15 * time.Second
	// DefaultCheckpointEvery is how many consecutive diff-only Modified rows one
	// object gets before the next one is promoted to a Checkpoint (a row carrying
	// the full state as well as the diff), bounding a reader's replay walk to
	// this many diffs. 50 keeps the storage cost of the extra full states in the
	// noise (one full state per 50 diffs, ZSTD(3)-compressed) while making
	// "reconstruct state at time T" a bounded query rather than a walk back to
	// the object's creation. Zero disables checkpointing; unlike the knobs above,
	// zero is therefore *meaningful* here and is never clamped to this default —
	// only a negative value is.
	DefaultCheckpointEvery = 50
)

const (
	defaultInsertTimeout   = 5 * time.Second
	defaultMaxRetryBackoff = 60 * time.Second
	// defaultMaxIsolationPhase is the ceiling on how long the per-row poison-
	// isolation phase of a single flushBatch may run before a hung backend is
	// declared and the remaining rows are failed (and requeued by the caller).
	// It bounds the worst case that Fix 3 (Task 0.9) closes: with the shipped
	// defaults the isolation loop could otherwise spend insertTimeout on each of
	// up to batchMaxRows rows (5s × 1000 ≈ 83 minutes) with one worker pinned on
	// a single doomed batch during a sustained outage. The effective per-batch
	// budget is min(len(batch)×insertTimeout, this) — small and typical batches
	// get the full sum-of-per-row-timeouts (so a slow-but-responsive backend is
	// never truncated), while only an oversized batch against an unresponsive
	// backend hits this ceiling. Two minutes leaves generous headroom for any
	// healthy backend (a single-row insert settles in milliseconds) while
	// converting an indefinite hang into a bounded, retried failure.
	defaultMaxIsolationPhase = 2 * time.Minute

	// chTimeFormat renders an instant for a *query literal* only — see
	// scopeWasActiveQuery. Quoted into SQL, it is parsed server-side against a
	// DateTime64(9, 'UTC') column, so it is exact there. It must never be used to
	// bind an insert argument: the driver parses such a string client-side and
	// reinterprets it in time.Local (see insertArgs).
	chTimeFormat = "2006-01-02 15:04:05.999999999"

	insertResourceStateQuery = `
        INSERT INTO resource_states (
            ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, actors, data, diff, sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// Config holds the externally configurable ClickHouse connection settings. It
// carries no defaults of its own — cmd/main.go is responsible for sourcing
// every field from flags/environment variables, so no ClickHouse host,
// credential, or timeout is ever hardcoded in this package.
type Config struct {
	Addr        string
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
	// AutoCreateSchema, when true, makes this instance execute the shipped DDL
	// (deploy/clickhouse/schema) idempotently in the background when it starts,
	// before its first write lands. Sourced from --ch-auto-create-schema /
	// CH_AUTO_CREATE_SCHEMA — an operator-level setting, not a per-sink CR field,
	// because whether the operator may run DDL at all is a deployment-time
	// decision. Defaults to false so it never mutates ClickHouse DDL unless asked.
	AutoCreateSchema bool
	// BatchMaxRows is the row count at which a worker flushes its accumulated
	// batch to ClickHouse; BatchMaxWait is the maximum time a batch's first job
	// waits for the batch to fill before it is flushed regardless. These are the
	// client-side batching knobs (row-per-INSERT is ClickHouse's pathological
	// write pattern). Zero values fall back to the package defaults in
	// NewCHWriter.
	BatchMaxRows int
	BatchMaxWait time.Duration
	// WriteQueueSize, WriteWorkers, EnqueueTimeout, and ShutdownDrainTimeout are
	// the remaining async-write-path knobs D2 requires operators to size per
	// environment. Sourced from the --writer-* flags / WRITER_* env twins in
	// cmd/main.go (Task 0.8); each zero value falls back to its Default* above
	// via NewCHWriter, so an unset knob keeps the shipped behavior.
	WriteQueueSize       int
	WriteWorkers         int
	EnqueueTimeout       time.Duration
	ShutdownDrainTimeout time.Duration
	// CheckpointEvery is how many consecutive diff-only Modified rows this sink
	// accepts for one object before the next one is written as a Checkpoint (full
	// data *and* diff), bounding a reader's replay cost. Sourced from the sink's
	// own spec.writer.checkpointEvery (Task 2.2), which the CRD defaults to
	// DefaultCheckpointEvery.
	//
	// Zero is meaningful — it disables checkpointing for this sink — so it is
	// deliberately *not* treated as "unset" the way the knobs above are; only a
	// negative value falls back to the default. Every value reaching here is
	// explicit, since cmd/main.go resolves the optional CR field before building
	// this struct.
	CheckpointEvery int
}

// Metrics is the narrow slice of pipeline metrics CHWriter records. It is an
// interface (rather than a concrete dependency on the pipeline metrics struct)
// so this package never imports internal/pipeline: the caller injects an
// implementation — see internal/pipeline.PipelineMetrics.
type Metrics interface {
	// SetWriteQueueDepth publishes the current hand-off queue depth.
	SetWriteQueueDepth(n float64)
	// SetWriteQueueCapacity publishes the fixed hand-off queue capacity.
	SetWriteQueueCapacity(n float64)
	// ObserveEnqueueBlock records how long an Enqueue blocked waiting for room.
	ObserveEnqueueBlock(seconds float64)
	// IncEnqueueTimeout counts an Enqueue that gave up because the queue stayed full.
	IncEnqueueTimeout()
	// ObserveWriteLatency records a job's first-attempt-to-final-settle latency.
	ObserveWriteLatency(seconds float64)
	// IncWriteRetryAttempt counts one write attempt beyond the first.
	IncWriteRetryAttempt()
	// IncWrite counts one settled write by outcome ("success" | "failed").
	IncWrite(outcome string)
	// ObserveWriteBatchRows records the row count of one flushed insert batch.
	ObserveWriteBatchRows(rows float64)
}

// writeJob is a single ClickHouse insert drained by a CHWriter worker. It is the
// backend-specific rendering of a sink.Job: the column arguments are computed once,
// at Enqueue time, from the job's Record, so a worker never touches sink types. The
// commit callback carries the exactly-once contract documented on sink.Job.
//
// It carries no query of its own: every row goes through insertResourceStateQuery,
// and both the batch attempt and the per-row isolation attempt prepare that one
// statement (see flushBatch), so there is exactly one encoding of a row in this
// package.
type writeJob struct {
	args   []any
	commit func(ok bool)
}

// CHWriter decouples ClickHouse inserts from the caller's hot path. Writes
// are handed off to a bounded queue and drained by a small worker pool, so a
// slow or unavailable ClickHouse connection never blocks a reconcile. A single
// CHWriter is shared across all watched GVKs. It implements sink.Writer,
// sink.ScopeEventWriter (see scopewriter.go) and sink.StateReader (see
// statereader.go), sharing one connection for all three.
type CHWriter struct {
	conn driver.Conn
	// database is the schema-introspection target Probe validates against; set
	// by Open, empty for test-constructed writers that never validate.
	database string
	// autoCreate mirrors Config.AutoCreateSchema; consulted by Start, which
	// applies the shipped DDL in the background before the first insert lands.
	autoCreate bool

	jobs    chan writeJob
	workers int

	// scopeEvents is the dedicated hand-off queue for watch-scope epoch
	// transitions, drained by a single scopeWorker with its own batching and
	// retry queue (see scopewriter.go). It is deliberately not the jobs channel:
	// a scope epoch must not queue behind a resource_states backlog, and it has
	// no commit contract to settle.
	scopeEvents chan sink.ScopeEvent
	// scopeRetryMu guards scopeRetries, the events whose insert failed after
	// backoff and that the scope worker re-attempts on its next tick. A scope
	// transition happens once and cannot be re-derived, so it is retried rather
	// than dropped.
	scopeRetryMu sync.Mutex
	scopeRetries []sink.ScopeEvent
	// scopeMaxRetryBackoff bounds one flush's retry window before its events go
	// back on scopeRetries. A field (not the constant directly) so tests can
	// shorten it without waiting out production backoff.
	scopeMaxRetryBackoff time.Duration

	// batchMaxRows / batchMaxWait govern client-side batching: a worker flushes
	// its accumulated batch once it holds batchMaxRows jobs or batchMaxWait has
	// elapsed since the batch's first job, whichever comes first. See worker.
	batchMaxRows int
	batchMaxWait time.Duration

	// enqueueTimeout bounds how long Enqueue blocks waiting for queue room
	// before returning an error (the job is never dropped silently). Sourced
	// from --writer-enqueue-timeout / WRITER_ENQUEUE_TIMEOUT via Config; falls
	// back to DefaultEnqueueTimeout when non-positive.
	enqueueTimeout       time.Duration
	insertTimeout        time.Duration
	maxRetryBackoff      time.Duration
	shutdownDrainTimeout time.Duration
	// checkpointEvery is this sink's Checkpoint cadence, read by the pipeline
	// through the CheckpointEvery method below (it implements
	// pipeline.CheckpointPolicy). Like maxIsolationPhase it is a field rather
	// than a NewCHWriter parameter — the constructor's positional signature is a
	// stable contract the existing tests depend on — so NewCHWriter installs
	// DefaultCheckpointEvery and Open overwrites it from Config, exactly as it
	// does for database and autoCreate.
	//
	// It is written once, before the writer is handed to the SinkManager, and
	// only read afterwards; a re-tuned cadence arrives as a new fingerprint and
	// therefore as a new instance (see Config.Fingerprint), never as a mutation
	// of a running one, which is what keeps this lock-free on the hot path.
	checkpointEvery int

	// maxIsolationPhase caps the whole per-row poison-isolation phase of one
	// flushBatch (all rows, not each row), so a hung backend cannot pin a worker
	// for insertTimeout × batchMaxRows. It is not a NewCHWriter parameter (the
	// constructor signature is a stable contract the existing tests depend on);
	// NewCHWriter always sets it to defaultMaxIsolationPhase, and same-package
	// tests set it directly to exercise the bound. See flushBatch.
	maxIsolationPhase time.Duration

	// mu guards closing and drainCtx; see Enqueue/attemptContext.
	mu      sync.Mutex
	closing bool
	// inflight tracks Enqueue calls that observed closing==false and are
	// therefore permitted to send on jobs; Start waits for it to drain to
	// zero before closing jobs, so a send can never race a close.
	inflight sync.WaitGroup
	// drainCtx is swapped from a plain context.Background() to a
	// shutdownDrainTimeout-bounded context the moment Start detects shutdown
	// (ctx.Done() fires) — see attemptContext. Starts non-nil so a worker
	// that reads it before Start ever swaps it (a narrow, harmless race) still
	// gets a safe, non-nil context rather than risking a nil-context panic.
	drainCtx context.Context

	// otherUsers tracks in-flight uses of conn that are NOT the worker pool —
	// namely LastKnownStates calls (cache warm-up) and the schema-validation
	// runnable, both of which share this CHWriter's connection. Start waits for
	// it to drain to zero, after its own workers finish, before closing conn, so
	// shutdown can never race a use of the shared connection against its
	// closure. Registration mirrors inflight: a user Adds only after observing
	// closing==false under mu, so an Add can never race Start's Wait.
	otherUsers sync.WaitGroup

	// metrics records queue depth/capacity, enqueue blocking and timeouts, and
	// per-job write latency, retries, and outcomes. Never nil once built via
	// NewCHWriter.
	metrics Metrics
}

// NewCHWriter builds a CHWriter around an existing ClickHouse connection and
// the metrics sink it reports to. Zero-valued queueSize/workers/timeouts fall
// back to sane defaults. Exposed (rather than only Open) so tests can drive the
// writer with a fake driver.Conn and an isolated metrics instance.
func NewCHWriter(conn driver.Conn, queueSize, workers, batchMaxRows int, insertTimeout, maxRetryBackoff, shutdownDrainTimeout, batchMaxWait, enqueueTimeout time.Duration, metrics Metrics) *CHWriter {
	if queueSize <= 0 {
		queueSize = DefaultWriteQueueSize
	}
	if workers <= 0 {
		workers = DefaultWriteWorkers
	}
	if batchMaxRows <= 0 {
		batchMaxRows = DefaultBatchMaxRows
	}
	if batchMaxWait <= 0 {
		batchMaxWait = DefaultBatchMaxWait
	}
	if enqueueTimeout <= 0 {
		enqueueTimeout = DefaultEnqueueTimeout
	}
	if insertTimeout <= 0 {
		insertTimeout = defaultInsertTimeout
	}
	if maxRetryBackoff <= 0 {
		maxRetryBackoff = defaultMaxRetryBackoff
	}
	if shutdownDrainTimeout <= 0 {
		shutdownDrainTimeout = DefaultShutdownDrainTimeout
	}
	return &CHWriter{
		conn:                 conn,
		jobs:                 make(chan writeJob, queueSize),
		workers:              workers,
		scopeEvents:          make(chan sink.ScopeEvent, scopeQueueSize),
		scopeMaxRetryBackoff: defaultScopeMaxRetryBackoff,
		batchMaxRows:         batchMaxRows,
		batchMaxWait:         batchMaxWait,
		enqueueTimeout:       enqueueTimeout,
		insertTimeout:        insertTimeout,
		maxRetryBackoff:      maxRetryBackoff,
		shutdownDrainTimeout: shutdownDrainTimeout,
		checkpointEvery:      DefaultCheckpointEvery,
		maxIsolationPhase:    defaultMaxIsolationPhase,
		drainCtx:             context.Background(),
		metrics:              metrics,
	}
}

// Open dials ClickHouse per cfg and returns a CHWriter over that connection,
// reporting to metrics. insertTimeout is cfg.ReadTimeout, so the
// operator-configured value governs the async write path (not just the
// connection's own driver-level timeout). The returned CHWriter owns conn and
// closes it on shutdown (see Start); it is a manager.Runnable, and the sink
// runtime is what starts and drains it (internal/sink.SinkManager).
func Open(cfg Config, metrics Metrics) (*CHWriter, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol:    clickhouse.Native,
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
	})
	if err != nil {
		return nil, err
	}
	w := NewCHWriter(conn, cfg.WriteQueueSize, cfg.WriteWorkers, cfg.BatchMaxRows, cfg.ReadTimeout, 0,
		cfg.ShutdownDrainTimeout, cfg.BatchMaxWait, cfg.EnqueueTimeout, metrics)
	w.database = cfg.Database
	w.autoCreate = cfg.AutoCreateSchema
	if cfg.CheckpointEvery >= 0 {
		// Zero is honoured as "checkpointing off for this sink"; only a negative
		// value (which no CRD-validated spec can produce) keeps the default.
		w.checkpointEvery = cfg.CheckpointEvery
	}
	return w, nil
}

// CheckpointEvery implements pipeline.CheckpointPolicy: how many consecutive
// diff-only Modified rows this sink accepts for one object before the next one is
// written as a Checkpoint, or 0 when the sink's owner disabled checkpointing.
//
// It is a policy read, not a write-path decision: the pipeline consults it while
// assembling a record (see internal/pipeline.checkpointDue), because whether a row
// carries full state is a property of the record, and this writer only ever
// inserts the row it is handed.
func (w *CHWriter) CheckpointEvery() int { return w.checkpointEvery }

// startAutoCreate applies the shipped DDL in the background, when this writer
// was opened with AutoCreateSchema.
//
// It shares conn, so it registers in otherUsers exactly like a LastKnownStates
// call — under the closing check, so the Add can never race Start's
// otherUsers.Wait, and released when the DDL work returns. That is what lets
// Start's step 5 guarantee nothing is still using conn when it closes it.
//
// It is a goroutine rather than an inline call because the DDL retries an
// unreachable ClickHouse indefinitely: blocking Start on it would mean a backend
// that is down at boot never starts draining its queue, so the enqueue path would
// report backpressure instead of buffering (Invariant 1).
//
// The logger is taken from the package's own delegating sink rather than from
// ctx: the DDL work outlives this call, and the rest of this file logs the same
// way (see Start).
func (w *CHWriter) startAutoCreate(ctx context.Context) {
	if !w.autoCreate {
		return
	}
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()

	go func() {
		defer w.otherUsers.Done()
		autoCreateSchemaWithRetry(ctx, w.conn, logf.Log.WithName("chwriter").WithName("schema"))
	}()
}

// Enqueue submits a write job without blocking the caller on the actual
// ClickHouse round-trip. The job's Record is rendered to the insert query and
// positional args here, once, so workers never touch sink types. If the queue
// is full, it waits up to the configured enqueue timeout for room before giving
// up — a job is never dropped silently. A returned error should be propagated by
// the caller (e.g. as Reconcile's own error) so controller-runtime's requeue
// and backoff take over as backpressure; nothing is lost in the meantime
// because the informer cache still holds the object's latest state for the
// retry.
func (w *CHWriter) Enqueue(ctx context.Context, job sink.Job) error {
	internal := writeJob{
		args:   insertArgs(job.Record),
		commit: job.Commit,
	}

	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("chwriter: shutting down, refusing new write")
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	timer := time.NewTimer(w.enqueueTimeout)
	defer timer.Stop()

	// enqueue_block_seconds measures how long the hot path actually waited for
	// room, whether the send eventually succeeded or timed out — the direct
	// backpressure signal a reconciler feels.
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
		return fmt.Errorf("chwriter: write queue still full after waiting %s", w.enqueueTimeout)
	}
}

// Start implements manager.Runnable and sink.Writer. It runs the worker pool
// until ctx is cancelled, then shuts down in a strict order so no write is ever
// stranded or raced against connection closure:
//  1. Stop accepting new Enqueue calls (under mu, so this can't race a send).
//  2. Swap in a fresh, shutdownDrainTimeout-bounded drainCtx for any job
//     processed from here on — see attemptContext for why the original ctx
//     (already cancelled by this point) can't be reused for these attempts.
//  3. Wait for any Enqueue / EnqueueScopeEvent call already past the closing
//     check to finish sending (or bail via its own ctx/timeout) — after this,
//     neither queue can receive further sends from anyone.
//  4. Close jobs and scopeEvents. Each worker receives from its channel until it
//     is drained and closed, flushing its partial in-flight batch (and, for the
//     scope worker, its retry queue) on the final (closed) receive, then exits
//     cleanly — no worker can exit "too early" and leave a job (or a buffered
//     batch) stranded.
//  5. Wait for otherUsers — the state-read / schema-validation goroutines that
//     share conn.
//  6. Close conn — guaranteed safe now, since nothing can still be using it.
func (w *CHWriter) Start(ctx context.Context) error {
	log := logf.Log.WithName("chwriter")

	// Capacity is fixed for this CHWriter's lifetime; publishing it here (once
	// the queue exists) lets dashboards express depth as a fraction of it.
	w.metrics.SetWriteQueueCapacity(float64(cap(w.jobs)))

	// Off the critical path: the workers below start draining immediately, and
	// their inserts retry until the tables exist.
	w.startAutoCreate(ctx)

	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
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

	drainCtx, cancel := context.WithTimeout(context.Background(), w.shutdownDrainTimeout)
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

	if err := w.conn.Close(); err != nil {
		log.Error(err, "chwriter: failed to close ClickHouse connection")
		return err
	}
	log.Info("chwriter: ClickHouse connection closed")
	return nil
}

// attemptContext returns ctx unchanged while it's still live, so a write that
// is genuinely in flight when shutdown begins is still cancelled promptly
// rather than allowed to run past the manager's own shutdown deadline. Once
// ctx has already fired — meaning this job is being pulled from the queue
// during Start's post-shutdown drain, not interrupted mid-attempt — deriving
// its write's timeout from ctx would guarantee an instant, no-chance failure
// on the very first attempt regardless of ClickHouse's actual health,
// defeating the drain phase's whole purpose. Such jobs get drainCtx instead:
// one context.WithTimeout(shutdownDrainTimeout) shared by every job drained
// during shutdown, so the whole drain phase (not each job individually) is
// what's bounded, giving queued writes a genuine chance to flush to a
// healthy ClickHouse before the process exits.
func (w *CHWriter) attemptContext(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.drainCtx
}

// worker drains w.jobs, accumulating jobs into a batch and flushing it once it
// reaches batchMaxRows or batchMaxWait has elapsed since the batch's first job,
// whichever comes first. The batchMaxWait timer is armed only while a batch is
// non-empty (an empty batch selects on a nil channel), so an idle worker never
// busy-waits or fires a spurious empty flush.
//
// Trickle traffic accepts batchMaxWait as its write-latency ceiling: a lone job
// waits up to batchMaxWait for batch-mates that never arrive before it flushes.
// Tune batchMaxWait down to trade batch efficiency for lower trickle latency.
//
// On the final receive (w.jobs closed and drained) the worker flushes its
// partial in-flight batch before returning, so Start's drain phase never
// strands a buffered batch.
//
//nolint:logcheck
func (w *CHWriter) worker(ctx context.Context, log logr.Logger) {
	batch := make([]writeJob, 0, w.batchMaxRows)

	// timerC is nil while the batch is empty, so the select below cannot fire a
	// wait-driven flush on an empty batch and does not busy-wait when idle.
	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	for {
		select {
		case job, ok := <-w.jobs:
			if !ok {
				// Channel closed and drained: flush whatever we still hold
				// (within Start's drain window) and exit.
				if len(batch) > 0 {
					w.flushBatch(w.attemptContext(ctx), log, batch)
				}
				disarm()
				return
			}
			batch = append(batch, job)
			if len(batch) == 1 {
				// First job of a new batch: arm the wait ceiling now, not before.
				timer = time.NewTimer(w.batchMaxWait)
				timerC = timer.C
			}
			if len(batch) >= w.batchMaxRows {
				w.flushBatch(w.attemptContext(ctx), log, batch)
				batch = batch[:0]
				disarm()
			}
		case <-timerC:
			// batchMaxWait elapsed with a partially-filled batch: flush it. The
			// batch is non-empty by construction (the timer is armed only when
			// the first job lands and disarmed on every flush).
			timer = nil
			timerC = nil
			w.flushBatch(w.attemptContext(ctx), log, batch)
			batch = batch[:0]
		}
	}
}

// flushBatch persists a full batch of jobs to ClickHouse and settles each job's
// commit callback. It is the sole place a batch's outcomes are settled.
//
// Exactly-once is a property of the commit callback, not of row insertion:
//
//	Every commit callback fires exactly once per job across all paths.
//
// Row insertion itself is at-least-once. driver.Batch.Send() is a network
// operation with three outcomes: nothing inserted; everything inserted but the
// acknowledgement lost (timeout / connection reset mid-response); or a partial
// insert. In the lost-ack and partial cases the call returns an error while the
// rows are already durable in ClickHouse, so the isolation path below re-inserts
// them. Each re-insert is byte-identical to the original — Record.Timestamp is
// frozen once when the event was processed, bound as an instant by insertArgs, and
// sent through the same encoder both here and in the isolation phase — so
// resource_states (ReplacingMergeTree) collapses it to a single row on merge (see
// docs/SCHEMA.md, "Delivery semantics"). The commit callback still fires exactly
// once regardless.
//
// The happy path prepares one client-side batch and Sends it (row-per-INSERT is
// ClickHouse's pathological write pattern), retrying the whole batch with the
// shared exponential backoff. If the batch still fails after retries, poison
// isolation kicks in: each row is re-attempted once on its own, so a single bad
// row cannot doom its blameless batch-mates — only the individually-failing
// rows get commit(false) (and the caller's revert/requeue path), the rest get
// commit(true).
//
// The isolation phase runs under its own bounded sub-context (isolationCtx),
// derived from the attemptContext-selected ctx so a shutdown drain stays capped
// by shutdownDrainTimeout as before. The phase budget is
// min(len(batch)×insertTimeout, maxIsolationPhase): a hung backend (rows neither
// succeeding nor erroring) can no longer pin a worker for insertTimeout ×
// batchMaxRows. The budget is sized never to truncate a responsive backend — it
// is ≥ the sum of the per-row insertTimeouts for any batch up to the ceiling, so
// a merely slow-but-alive backend always finishes and no row that would have
// succeeded is cancelled into a manufactured failure. When the sub-context does
// expire (only a genuinely unresponsive backend reaches it), the in-flight and
// all remaining rows settle via the normal path — their Exec returns a context
// error, they are counted writes_total{outcome="failed"} and committed false
// exactly once, and the caller's revert/requeue re-drives them. It converts an
// unbounded hang into a bounded, retried failure.
//
//nolint:logcheck
func (w *CHWriter) flushBatch(ctx context.Context, log logr.Logger, batch []writeJob) {
	// A batch leaves the queue as it is assembled, so refresh depth here too —
	// not only on enqueue — so a draining queue is reflected, not just a
	// filling one. write_batch_rows is emitted once per flush.
	w.metrics.SetWriteQueueDepth(float64(len(w.jobs)))
	w.metrics.ObserveWriteBatchRows(float64(len(batch)))

	start := time.Now()
	err := w.sendBatch(ctx, batch)
	if err == nil {
		elapsed := time.Since(start).Seconds()
		for _, job := range batch {
			w.metrics.ObserveWriteLatency(elapsed)
			w.metrics.IncWrite("success")
			if job.commit != nil {
				job.commit(true)
			}
		}
		return
	}

	// Batch exhausted retries. Re-attempt each row individually, exactly once,
	// so one poison row is blamed in isolation rather than failing the batch.
	// The whole phase is bounded by isolationCtx so a hung backend can't pin the
	// worker; each row still gets its own insertTimeout inside that cap.
	log.Error(err, "chwriter: batch write failed after retries; isolating rows individually", "rows", len(batch))
	isolationCtx, cancelPhase := context.WithTimeout(ctx, min(time.Duration(len(batch))*w.insertTimeout, w.maxIsolationPhase))
	defer cancelPhase()
	for _, job := range batch {
		rowStart := time.Now()
		rowCtx, cancel := context.WithTimeout(isolationCtx, w.insertTimeout)
		// A one-row batch, not conn.Exec: the isolation attempt must go through the
		// very same encoder as the batch attempt it follows, or the two disagree
		// about how a value is rendered. Exec interpolates positional arguments
		// into SQL text, which silently reduces a DateTime64(9) to second
		// precision; the batch path encodes typed columns directly. A re-insert
		// that differs from the original in any ORDER BY column — ts included — is
		// a second row ReplacingMergeTree can never collapse, which is exactly the
		// at-least-once idempotency property this path exists to preserve.
		rowErr := w.sendBatchOnce(rowCtx, []writeJob{job})
		cancel()

		w.metrics.ObserveWriteLatency(time.Since(rowStart).Seconds())
		if rowErr != nil {
			w.metrics.IncWrite("failed")
			log.Error(rowErr, "chwriter: giving up on row after individual retry")
		} else {
			w.metrics.IncWrite("success")
		}
		if job.commit != nil {
			job.commit(rowErr == nil)
		}
	}
}

// sendBatch prepares and sends the whole batch as one client-side INSERT,
// retrying the entire batch with the shared exponential backoff. Each attempt's
// deadline is derived from ctx (see attemptContext), so a manager shutdown
// cancels an in-flight attempt immediately instead of letting it run for a full
// insertTimeout. It never touches commit callbacks — flushBatch owns settling.
func (w *CHWriter) sendBatch(ctx context.Context, batch []writeJob) error {
	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.maxRetryBackoff

	attempts := 0
	return backoff.Retry(func() error {
		// Every attempt after the first is a retry; counting them here (rather
		// than the successes) is what makes a retry storm visible even when the
		// batch ultimately succeeds.
		if attempts > 0 {
			w.metrics.IncWriteRetryAttempt()
		}
		attempts++
		return w.sendBatchOnce(ctx, batch)
	}, backoff.WithContext(eb, ctx))
}

// sendBatchOnce performs a single insert attempt: prepare a batch, append every
// row, and send. All rows share insertResourceStateQuery (the driver normalizes
// away the VALUES clause for PrepareBatch), so no per-job query is needed here.
// On an Append failure the half-built batch is aborted so it is not leaked, and
// both errors are surfaced (no silent error) so the backoff sees a real failure.
//
// It is the *only* way this package inserts a resource_states row: flushBatch's
// per-row isolation phase calls it with a one-row slice rather than reaching for
// conn.Exec, so a re-inserted row is byte-identical to the batch attempt that may
// already have landed.
func (w *CHWriter) sendBatchOnce(ctx context.Context, batch []writeJob) error {
	attemptCtx, cancel := context.WithTimeout(ctx, w.insertTimeout)
	defer cancel()

	b, err := w.conn.PrepareBatch(attemptCtx, insertResourceStateQuery)
	if err != nil {
		return err
	}
	for _, job := range batch {
		if appendErr := b.Append(job.args...); appendErr != nil {
			return errors.Join(appendErr, b.Abort())
		}
	}
	return b.Send()
}

// insertArgs returns the arguments for the resource_states INSERT, in exactly the
// column order expected by insertResourceStateQuery. nil Labels/Actors are coerced
// to empty containers so the Map/Array columns always bind a concrete value rather
// than a NULL the non-nullable schema would reject.
//
// The timestamp is bound as a time.Time — an instant — and never as a formatted
// datetime string. The pinned driver (clickhouse-go v2.46.0,
// lib/column/datetime64.go) parses a bare "2006-01-02 15:04:05.999999999" string
// and then *reinterprets those wall-clock digits in time.Local*, so an operator
// process running outside UTC wrote every ts into the DateTime64(9, 'UTC') column
// shifted by its own offset: a 12:00Z event landed as 10:00Z from a CEST pod. An
// offset-bearing string is not a fix either — the driver accepts it, but ClickHouse
// rejects "… +00:00" when it parses the same literal server-side. A time.Time
// carries the instant itself, so no zone is inferred anywhere and the value is
// identical whatever TZ the operator runs under.
func insertArgs(rec sink.Record) []any {
	labels := rec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	actors := rec.Actors
	if actors == nil {
		actors = []string{}
	}
	return []any{
		rec.Timestamp.UTC(), rec.ClusterID, rec.EventType, rec.APIGroup, rec.APIVersion,
		rec.Kind, rec.Namespace, rec.Name, rec.UID, rec.ResourceVersion, labels, actors, rec.Data, rec.Diff, rec.SHA256,
	}
}
