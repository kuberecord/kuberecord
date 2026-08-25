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

// This file is the S3 backend's scope log: the half of the archive that says
// when kuberecord started and stopped watching something, so a gap in the
// records is explicable rather than merely empty.
//
// It is what keeps this backend's archive honest about its own coverage. An S3
// archive holds no deletions for the periods the operator was down (D12), so
// "there are no records for this Deployment after Tuesday" is ambiguous on its
// own: it could mean nothing changed, or that nobody was watching. The scope
// objects are what disambiguate it, from inside the bucket, with no operator and
// no ClickHouse to ask.
//
// **These transitions are recorded but never reconciled.** Boot reconciliation —
// finding the scopes a previous process left open and closing them out — is a
// sink.StateReader operation, and this backend deliberately implements none (see
// instance.go). So a process that dies with scopes open leaves those scopes'
// Started objects with no matching Stopped object, forever: the next process
// writes its own Started and never learns there was an earlier one. A reader must
// therefore treat an unmatched Started as "watching began here, and this epoch's
// end is unknown", not as "still watching". Nothing in this package can close
// that gap; the tee pattern (D14) is what closes it, by keeping a ClickHouseSink
// alongside that can read its own history.

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The scope-event path's own knobs, deliberately not operator-tunable for the
// same reason the ClickHouse backend's are not: scope transitions arrive at
// rule-lifecycle rate (a handful per apply, not thousands per second), so there
// is nothing to size per environment and every extra CR field is one more thing
// that can be misconfigured.
const (
	// scopeQueueSize is the hand-off capacity for scope events. It only ever backs
	// up while the store is unreachable, a condition the recorder's own retry
	// queue is designed to ride out.
	scopeQueueSize = 2048

	// scopeBatchMaxRows and scopeBatchMaxWait govern the dedicated batcher. Both
	// are far smaller than the record path's rotation: a scope epoch is an audit
	// fact an operator expects to see in the bucket promptly after applying a
	// rule, and there is never enough volume for large objects to matter. The wait
	// is what coalesces the one burst that does happen (many rules applied at
	// once) into a single object instead of one object per rule.
	scopeBatchMaxRows = 64
	scopeBatchMaxWait = 500 * time.Millisecond

	// scopePutTimeout bounds one scope-object PUT and defaultScopeMaxRetryBackoff
	// bounds how long a batch is retried before its events go back on the internal
	// retry queue. They are separate from the record path's so a rotation sized
	// for 64Mi objects cannot make the (rare, tiny) scope write flaky, and so a
	// scope object never waits out a record object's much longer budget.
	scopePutTimeout             = 10 * time.Second
	defaultScopeMaxRetryBackoff = 30 * time.Second
	scopeRetryQueueMaxLen       = 4096

	// scopesPartition is the segment that separates the scope log from the records
	// under one prefix:
	//
	//	<prefix>/format=jsonl-v1/scopes/date=<YYYY-MM-DD>/<content-hash>.jsonl.zst
	//
	// It sits inside format=jsonl-v1 because it is part of the same versioned
	// contract (D15), and outside cluster_id= because it is partitioned by date
	// alone — a scope log is small enough that hour= partitions would only produce
	// near-empty prefixes, and coarse enough that a reader asking "was this being
	// watched?" wants a day, not an hour.
	//
	// One consequence belongs in every reader's mind: a glob over
	// `format=jsonl-v1/**/*.jsonl.zst` matches both kinds of object, and the two
	// have different line shapes. A records query must glob
	// `format=jsonl-v1/cluster_id=*/**/*.jsonl.zst`, and a scope query
	// `format=jsonl-v1/scopes/**/*.jsonl.zst`.
	scopesPartition = "scopes"
)

// errScopeEventsDropped gives the drop log line a non-nil error value. Dropping
// a scope epoch is an audit hole, so it is reported at Error level (Invariant 4)
// even though it is the deliberate, bounded-memory response to a store that has
// been unreachable for a very long time.
var errScopeEventsDropped = errors.New("s3: scope-event retry queue overflowed, oldest transitions dropped")

// scopeEventLine is the physical form of one watch-scope transition: one JSON
// object per line, exactly as a record line is.
//
// It is a type of this package's own rather than JSON tags on sink.ScopeEvent,
// because how a logical value is spelled on disk is the backend's business and
// not the contract's (D9) — the ClickHouse backend spells the same fields as
// eight columns named its own way, and neither mapping is entitled to dictate the
// other. The scope's identity is flattened into the line rather than nested,
// because these lines are read by query engines that infer columns from the top
// level, and a nested `scope` object would make every query dereference it.
//
// The field names follow the record lines' vocabulary, so `group` and `version`
// mean here what they mean there. Nothing is omitted when empty: an empty
// `namespace` is the all-namespaces scope, which is a *value*, and an absent
// field would make the most important scope in the archive the one hardest to
// query for.
type scopeEventLine struct {
	TS         time.Time `json:"ts"`
	ClusterID  string    `json:"cluster_id"`
	APIGroup   string    `json:"group"`
	APIVersion string    `json:"version"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace"`
	Action     string    `json:"action"`
	RuleRef    string    `json:"rule_ref"`
}

// EnqueueScopeEvent implements sink.ScopeEventWriter. It hands one watch-scope
// transition to the dedicated scope queue, drained by a single worker with its
// own batching and retry (see scopeWorker) rather than sharing the record path's
// queue and workers.
//
// The separation is the point: a scope epoch must not queue behind a rotating
// 64Mi object (an operator applying a rule wants to see the epoch land, not wait
// out a rotation), and the record path's commit-settling machinery has nothing to
// settle here — a scope event carries no cache state.
//
// Like Enqueue it may wait a bounded time for queue room, so it must not be
// called from a watch-lifecycle path (Invariant 1); the scope recorder calls it
// from its own goroutine.
func (w *Writer) EnqueueScopeEvent(ctx context.Context, event sink.ScopeEvent) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("s3writer: shutting down, refusing new scope event")
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
		return fmt.Errorf("s3writer: scope event queue still full after waiting %s", w.enqueueTimeout)
	}
}

// scopeWorker drains w.scopeEvents, accumulating events into a batch that is
// written as one object once it holds scopeBatchMaxRows or scopeBatchMaxWait has
// elapsed since the batch's first event — the same arming discipline as the
// record worker (the timer channel is nil while the batch is empty, so an idle
// worker neither busy-waits nor writes an empty object).
//
// There is exactly one such worker. Events for one scope must land in the order
// they happened (a Started that overtook its own Stopped would invert the epoch),
// and a single worker draining a single FIFO queue is the cheapest way to
// guarantee it: with two workers, two objects could reach the store in either
// order.
//
// On the final receive (the channel closed and drained) it writes what it still
// holds — including anything left on the retry queue — inside Start's drain
// window, so a shutdown does not strand an epoch that was only waiting on a
// retry.
//
//nolint:logcheck
func (w *Writer) scopeWorker(ctx context.Context, log logr.Logger) {
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

	// retryTick re-attempts events whose object could not be written. It runs
	// regardless of whether new events arrive, which is what carries an epoch
	// forward while the store is down and nothing else is changing.
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
			// happened before anything currently batched, and per-scope ordering is
			// the one property this path must not lose.
			if pending := w.takeScopeRetries(); len(pending) > 0 {
				w.flushScopeBatch(w.attemptContext(ctx), log, append(pending, batch...))
				batch = batch[:0]
				disarm()
			}
		}
	}
}

// flushScopeBatch writes one batch of transitions as a single object, retrying
// the whole object with bounded exponential backoff. Events that still cannot be
// written go on the internal retry queue rather than being dropped: unlike a
// record, whose object will be observed again and re-recorded, a scope transition
// happens once and is unrecoverable if lost.
//
// A retried batch is content-addressed exactly as a record object is, so the
// retry of an unchanged batch lands on its own key rather than adding a second
// copy of the epoch to the log a reader sees (on a versioned bucket that is one
// current version out of two identical ones — see docs/RETENTION.md). A batch
// that is *regrouped* by the retry queue (its events flushed together with later
// ones) does produce a distinct object — which is why a reader of the scope log
// must treat a transition as identified by its fields, not by the object it
// arrived in.
//
//nolint:logcheck
func (w *Writer) flushScopeBatch(ctx context.Context, log logr.Logger, batch []sink.ScopeEvent) {
	if len(batch) == 0 {
		return
	}

	obj, err := encodeScopeObject(w.prefix, batch)
	if err != nil {
		// Unencodable transitions cannot be fixed by retrying them, and keeping
		// them would wedge the retry queue behind an event that can never be
		// written, so they are dropped — loudly (Invariant 4).
		log.Error(err, "s3writer: dropping scope transitions that cannot be encoded", "events", len(batch))
		return
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.scopeMaxRetryBackoff
	in := PutObjectInput{Bucket: w.bucket, Key: obj.Key, Body: obj.Payload, Retention: w.retention()}
	err = backoff.Retry(func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, scopePutTimeout)
		defer cancel()
		return w.store.PutObject(attemptCtx, in)
	}, backoff.WithContext(eb, ctx))
	if err == nil {
		log.V(1).Info("s3writer: wrote scope object", "bucket", w.bucket, "key", obj.Key, "events", len(batch))
		return
	}

	log.Error(err, "s3writer: scope object PUT failed after retries, keeping the transitions for a later attempt",
		"bucket", w.bucket, "key", obj.Key, "events", len(batch))
	w.queueScopeRetries(log, batch)
}

// queueScopeRetries stores events for a later attempt, oldest first.
//
// The queue is bounded because it is the one unbounded-growth risk on this path:
// a store that has been unreachable for days while rules churn would otherwise
// accumulate events until the process died, which trades a partial audit gap for
// a total outage. On overflow the *oldest* events are dropped and reported at
// Error level — the newest transitions describe each scope's current epoch, which
// is the more useful half to keep.
func (w *Writer) queueScopeRetries(log logr.Logger, batch []sink.ScopeEvent) {
	w.scopeRetryMu.Lock()
	defer w.scopeRetryMu.Unlock()

	w.scopeRetries = append(w.scopeRetries, batch...)
	if overflow := len(w.scopeRetries) - scopeRetryQueueMaxLen; overflow > 0 {
		w.scopeRetries = w.scopeRetries[overflow:]
		log.Error(errScopeEventsDropped, "s3writer: dropped the oldest scope transitions",
			"dropped", overflow, "queue_limit", scopeRetryQueueMaxLen)
	}
}

// takeScopeRetries removes and returns everything on the retry queue, oldest
// first. Taking (rather than peeking) keeps the queue and the in-flight batch
// from ever holding the same event twice: a failed flush puts its events back.
func (w *Writer) takeScopeRetries() []sink.ScopeEvent {
	w.scopeRetryMu.Lock()
	defer w.scopeRetryMu.Unlock()
	pending := w.scopeRetries
	w.scopeRetries = nil
	return pending
}

// encodeScopeObject serializes a batch of transitions into one scope object.
//
// It mirrors Encode in every respect that is part of the contract — one JSON
// object per line, the content hash taken over the *uncompressed* payload, the
// key derived from that hash — and differs only in the two ways the scope log
// differs: the line shape (scopeEventLine) and the partition (date only, see
// scopesPartition). The date comes from the batch's first event's own instant,
// never from the wall clock, so a transition written late is still filed under
// the day it happened.
//
// It compresses with the package-level encoder rather than the accumulating
// builder the record path uses: a scope batch is at most scopeBatchMaxRows tiny
// lines, so there is nothing to stream and no size to rotate on.
func encodeScopeObject(prefix string, events []sink.ScopeEvent) (Object, error) {
	if len(events) == 0 {
		return Object{}, errEmptyBatch
	}
	if zstdEncoder == nil {
		return Object{}, errNoEncoder
	}

	var jsonl bytes.Buffer
	enc := lineEncoder(&jsonl)
	for i, event := range events {
		if err := enc.Encode(scopeLine(event)); err != nil {
			return Object{}, fmt.Errorf("s3: marshal scope transition %d (%s %s/%s): %w",
				i, event.Action, event.Scope.Kind, event.Scope.Namespace, err)
		}
	}

	sum := sha256.Sum256(jsonl.Bytes())
	contentHash := hex.EncodeToString(sum[:])

	return Object{
		Key:         scopeObjectKey(prefix, events[0].TS, contentHash),
		ContentHash: contentHash,
		Payload:     zstdEncoder.EncodeAll(jsonl.Bytes(), nil),
	}, nil
}

// decodeScopeObject reads a scope object's payload back into the transitions it
// was encoded from. It is the read half of the same contract, and it exists for
// the same reason Decode does: a format nothing can read back is a format nothing
// can check.
//
// It is unexported where Decode is exported because the record lines are what a
// consumer of this archive queries; the scope log is read by this package's own
// tests and, from outside, by the documented DuckDB/Athena recipes rather than by
// Go code. Export it the moment something outside needs it, not before.
func decodeScopeObject(payload []byte) ([]sink.ScopeEvent, error) {
	jsonl, err := decompress(payload)
	if err != nil {
		return nil, err
	}

	var events []sink.ScopeEvent
	dec := json.NewDecoder(bytes.NewReader(jsonl))
	for {
		var line scopeEventLine
		if decErr := dec.Decode(&line); decErr != nil {
			if errors.Is(decErr, io.EOF) {
				return events, nil
			}
			return nil, fmt.Errorf("s3: decode scope transition %d of object: %w", len(events), decErr)
		}
		events = append(events, sink.ScopeEvent{
			Action: sink.ScopeAction(line.Action),
			Scope: sink.ScopeFilter{
				ClusterID: line.ClusterID,
				APIGroup:  line.APIGroup,
				Kind:      line.Kind,
				Namespace: line.Namespace,
			},
			APIVersion: line.APIVersion,
			RuleRef:    line.RuleRef,
			TS:         line.TS,
		})
	}
}

// scopeLine renders one transition as its line. The instant is the event's own TS
// — stamped when the transition was observed, never when the object was written —
// so a delayed or retried write still tells the truth about when watching started
// or stopped (see sink.ScopeEvent.TS). It is normalised to UTC for the same reason
// the record partitions are: a line whose zone depended on the operator pod's
// timezone would sort differently from the same event written by a pod elsewhere.
func scopeLine(event sink.ScopeEvent) scopeEventLine {
	return scopeEventLine{
		TS:         event.TS.UTC(),
		ClusterID:  event.Scope.ClusterID,
		APIGroup:   event.Scope.APIGroup,
		APIVersion: event.APIVersion,
		Kind:       event.Scope.Kind,
		Namespace:  event.Scope.Namespace,
		Action:     string(event.Action),
		RuleRef:    event.RuleRef,
	}
}

// scopeObjectKey builds the full key for a scope object. Like objectKey it is the
// only place its layout is constructed, and it shares that function's treatment
// of an empty prefix (no segment, never a leading slash).
func scopeObjectKey(prefix string, ts time.Time, contentHash string) string {
	segments := make([]string, 0, 5)
	if prefix != "" {
		segments = append(segments, prefix)
	}
	segments = append(segments,
		formatPartition,
		scopesPartition,
		"date="+ts.UTC().Format(partitionDateLayout),
		contentHash+objectSuffix,
	)
	return strings.Join(segments, "/")
}

// isScopeObjectKey reports whether a key names a scope object rather than a
// record object. It exists so nothing has to match the layout with a string
// literal of its own: the segment is spelled once, here, and a test fake or a
// reader can ask this instead of guessing.
func isScopeObjectKey(key string) bool {
	return strings.Contains(key, "/"+formatPartition+"/"+scopesPartition+"/") ||
		strings.HasPrefix(key, formatPartition+"/"+scopesPartition+"/")
}

// Compile-time proof that this backend implements the scope-log contract;
// asserted here rather than at wiring time so a signature drift surfaces in this
// file.
var _ sink.ScopeEventWriter = (*Writer)(nil)
