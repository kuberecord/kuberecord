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

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The scope-event path's own knobs. They are deliberately not operator-tunable:
// scope transitions arrive at rule-lifecycle rate (a handful per apply, not
// thousands per second), so there is nothing to size per environment, and every
// extra flag is one more thing that can be misconfigured.
const (
	// scopeQueueSize is the hand-off capacity for scope events. A few thousand
	// rule edges of headroom is far beyond any realistic burst — a GitOps apply
	// of every rule in a large cluster is a few hundred transitions — and the
	// queue only ever backs up while ClickHouse is unreachable, a condition the
	// recorder's own retry queue is designed to ride out.
	scopeQueueSize = 2048

	// scopeBatchMaxRows and scopeBatchMaxWait govern the dedicated batcher. Both
	// are much smaller than the record path's: a scope epoch is an audit fact an
	// operator expects to see in the table promptly after applying a rule, and
	// there is never enough volume for large batches to matter. The wait is what
	// coalesces the one burst that does happen (many rules applied at once).
	scopeBatchMaxRows = 64
	scopeBatchMaxWait = 500 * time.Millisecond

	// scopeInsertTimeout bounds one INSERT attempt, and
	// defaultScopeMaxRetryBackoff bounds how long a batch is retried before its
	// rows are put back on the internal retry queue (the field it initializes is
	// shortened by tests). They are separate from the record path's so a
	// tuned-down insert timeout for high-volume rows cannot make the (rare, tiny)
	// scope insert flaky.
	scopeInsertTimeout          = 10 * time.Second
	defaultScopeMaxRetryBackoff = 30 * time.Second
	scopeRetryQueueMaxLen       = 4096

	insertScopeEventQuery = `
        INSERT INTO watch_scopes (
            ts, cluster_id, api_group, api_version, kind, namespace, action, rule_ref
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// errScopeEventsDropped gives the drop log line a non-nil error value. Dropping
// a scope epoch is an audit hole, so it is reported at Error level (Invariant 4)
// even though it is the deliberate, bounded-memory response to a sink that has
// been unreachable for a very long time.
var errScopeEventsDropped = errors.New("watch_scopes retry queue overflowed, oldest scope events dropped")

// EnqueueScopeEvent implements sink.ScopeEventWriter. It hands one watch-scope
// transition to the dedicated scope-event queue, which is drained by a single
// worker with its own batching and retry (see scopeWorker) rather than sharing
// the record path's queue and worker pool.
//
// The separation is the point: a scope epoch must not queue behind a backlog of
// object rows (an operator applying a rule wants to see the epoch land, not wait
// out a resource_states flush), and the record path's poison-isolation and
// commit-settling machinery has nothing to settle here — a scope event carries no
// cache state.
//
// Like the record path's Enqueue it may wait a bounded time for queue room, so
// it must not be called from a watch-lifecycle path (Invariant 1); the scope
// recorder calls it from its own goroutine.
func (w *CHWriter) EnqueueScopeEvent(ctx context.Context, event sink.ScopeEvent) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("chwriter: shutting down, refusing new scope event")
	}
	// Registered on the same WaitGroup as record enqueues: Start waits for it to
	// drain to zero before closing either channel, so a send can never race a
	// close.
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	timer := time.NewTimer(w.enqueueTimeout)
	defer timer.Stop()

	select {
	case w.scopeEvents <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("chwriter: scope event queue still full after waiting %s", w.enqueueTimeout)
	}
}

// scopeWorker drains w.scopeEvents, accumulating events into a batch that is
// flushed once it holds scopeBatchMaxRows or scopeBatchMaxWait has elapsed since
// the batch's first event — the same arming discipline as the record worker (the
// timer channel is nil while the batch is empty, so an idle worker neither
// busy-waits nor fires an empty flush).
//
// There is exactly one such worker. Scope events for one scope must land in the
// order they happened (a Started that overtook its own Stopped would invert the
// epoch), and a single worker draining a single FIFO queue is the cheapest way to
// guarantee that: with two workers, two batches could reach ClickHouse in either
// order.
//
// On the final receive (the channel closed and drained) it flushes what it still
// holds — including anything left on the retry queue — inside Start's drain
// window, so a shutdown does not strand an epoch that was only waiting on a
// retry.
//
//nolint:logcheck
func (w *CHWriter) scopeWorker(ctx context.Context, log logr.Logger) {
	batch := make([]sink.ScopeEvent, 0, scopeBatchMaxRows)

	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	// retryTick re-attempts events whose flush failed. It runs regardless of
	// whether new events arrive, which is what carries an epoch forward while
	// ClickHouse is down and nothing else is changing.
	retryTick := time.NewTicker(scopeBatchMaxWait)
	defer retryTick.Stop()

	for {
		select {
		case event, ok := <-w.scopeEvents:
			if !ok {
				batch = append(batch, w.takeScopeRetries()...)
				if len(batch) > 0 {
					w.flushScopeBatch(w.attemptContext(ctx), log, batch)
				}
				disarm()
				return
			}
			batch = append(batch, event)
			if len(batch) == 1 {
				timer = time.NewTimer(scopeBatchMaxWait)
				timerC = timer.C
			}
			if len(batch) >= scopeBatchMaxRows {
				w.flushScopeBatch(w.attemptContext(ctx), log, batch)
				batch = batch[:0]
				disarm()
			}
		case <-timerC:
			timer = nil
			timerC = nil
			w.flushScopeBatch(w.attemptContext(ctx), log, batch)
			batch = batch[:0]
		case <-retryTick.C:
			// Retries are prepended, not appended: the queue holds events that
			// happened before anything currently batched, and per-scope ordering
			// is the one property this path must not lose.
			if pending := w.takeScopeRetries(); len(pending) > 0 {
				w.flushScopeBatch(w.attemptContext(ctx), log, append(pending, batch...))
				batch = batch[:0]
				disarm()
			}
		}
	}
}

// flushScopeBatch inserts a batch of scope events, retrying the whole batch with
// bounded exponential backoff. Events that still cannot be written are put on the
// internal retry queue rather than dropped: unlike a resource_states row, whose
// object will be re-observed and re-recorded on the next event for it, a scope
// transition happens once and is unrecoverable if lost.
//
// There is no per-row poison isolation here. A scope event is eight scalar
// columns with no user-controlled payload, so "one bad row poisons the batch" is
// not a failure mode this path has; a failure means the backend is unreachable,
// which affects every row equally.
//
//nolint:logcheck
func (w *CHWriter) flushScopeBatch(ctx context.Context, log logr.Logger, batch []sink.ScopeEvent) {
	if len(batch) == 0 {
		return
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.scopeMaxRetryBackoff
	err := backoff.Retry(func() error {
		return w.sendScopeBatchOnce(ctx, batch)
	}, backoff.WithContext(eb, ctx))
	if err == nil {
		log.V(1).Info("chwriter: wrote watch_scopes rows", "rows", len(batch))
		return
	}

	log.Error(err, "chwriter: watch_scopes insert failed after retries, keeping the events for a later attempt",
		"rows", len(batch))
	w.queueScopeRetries(log, batch)
}

// sendScopeBatchOnce performs a single batch attempt. The half-built batch is
// aborted on an Append failure so it is not leaked, and both errors are surfaced
// (no silent error) so the backoff sees a real failure.
func (w *CHWriter) sendScopeBatchOnce(ctx context.Context, batch []sink.ScopeEvent) error {
	attemptCtx, cancel := context.WithTimeout(ctx, scopeInsertTimeout)
	defer cancel()

	b, err := w.conn.PrepareBatch(attemptCtx, insertScopeEventQuery)
	if err != nil {
		return err
	}
	for _, event := range batch {
		if appendErr := b.Append(scopeInsertArgs(event)...); appendErr != nil {
			return errors.Join(appendErr, b.Abort())
		}
	}
	return b.Send()
}

// queueScopeRetries stores events for a later attempt, oldest first.
//
// The queue is bounded because it is the one unbounded-growth risk on this path:
// a sink that has been unreachable for days while rules churn would otherwise
// accumulate events until the process died, which trades a partial audit gap for
// a total outage. On overflow the *oldest* events are dropped and reported at
// Error level — the newest transitions describe the scope's current epoch, which
// is the more useful half to keep.
func (w *CHWriter) queueScopeRetries(log logr.Logger, batch []sink.ScopeEvent) {
	w.scopeRetryMu.Lock()
	defer w.scopeRetryMu.Unlock()

	w.scopeRetries = append(w.scopeRetries, batch...)
	if overflow := len(w.scopeRetries) - scopeRetryQueueMaxLen; overflow > 0 {
		w.scopeRetries = w.scopeRetries[overflow:]
		log.Error(errScopeEventsDropped, "chwriter: dropped the oldest watch_scopes events",
			"dropped", overflow, "queue_limit", scopeRetryQueueMaxLen)
	}
}

// takeScopeRetries removes and returns everything on the retry queue, oldest
// first. Taking (rather than peeking) keeps the queue and the in-flight batch
// from ever holding the same event twice: a failed flush puts its events back.
func (w *CHWriter) takeScopeRetries() []sink.ScopeEvent {
	w.scopeRetryMu.Lock()
	defer w.scopeRetryMu.Unlock()
	pending := w.scopeRetries
	w.scopeRetries = nil
	return pending
}

// scopeInsertArgs returns the positional arguments for the watch_scopes INSERT,
// in exactly the column order of insertScopeEventQuery.
//
// The timestamp is the event's own TS — stamped when the transition was observed,
// never when the write happens — so a retried epoch still records when the watch
// actually started or stopped (see sink.ScopeEvent.TS).
//
// It is bound as a time.Time rather than as a formatted string, and that is
// load-bearing: the driver parses a bare "2006-01-02 15:04:05" string in the
// *process's local* zone, so a formatted timestamp from an operator running outside
// UTC lands in the DateTime64(9, 'UTC') column shifted by its offset. A time.Time
// carries an unambiguous instant, which is what the epoch check's strict `ts <`
// cutoff needs to be exact.
func scopeInsertArgs(event sink.ScopeEvent) []any {
	return []any{
		event.TS.UTC(), event.Scope.ClusterID, event.Scope.APIGroup,
		event.APIVersion, event.Scope.Kind, event.Scope.Namespace, string(event.Action), event.RuleRef,
	}
}

// Compile-time proof that the ClickHouse sink implements the scope-log contract;
// asserted here rather than at wiring time so a signature drift surfaces in this
// file.
var _ sink.ScopeEventWriter = (*CHWriter)(nil)
