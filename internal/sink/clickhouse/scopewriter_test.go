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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// scopeRowRecorder captures the rows each watch_scopes batch was sent with, and can
// be scripted to fail the first n sends — the "ClickHouse is down while a rule is
// applied" scenario, which must not lose the epoch.
type scopeRowRecorder struct {
	mu       sync.Mutex
	batches  [][][]any
	failures int
	sends    int
}

// hook returns the fakeConn sendErr hook.
func (r *scopeRowRecorder) hook() func(ctx context.Context, rows [][]any) error {
	return func(_ context.Context, rows [][]any) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sends++
		if r.failures > 0 {
			r.failures--
			return errors.New("clickhouse unavailable")
		}
		r.batches = append(r.batches, slices.Clone(rows))
		return nil
	}
}

// rows returns every accepted row across all batches, in order.
func (r *scopeRowRecorder) rows() [][]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]any
	for _, batch := range r.batches {
		out = append(out, batch...)
	}
	return out
}

func (r *scopeRowRecorder) batchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.batches)
}

// newScopeTestWriter builds a CHWriter over conn with a scope-retry window short
// enough that a failed flush is re-attempted inside the test.
func newScopeTestWriter(conn *fakeConn) *CHWriter {
	m := pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID)
	w := NewCHWriter(conn, 16, 1, 8, 50*time.Millisecond, time.Second, time.Second,
		10*time.Millisecond, time.Second, m)
	w.scopeMaxRetryBackoff = 20 * time.Millisecond
	return w
}

// scopeEvent builds one transition for a namespaced Pod scope.
func scopeEvent(action sink.ScopeAction, namespace, ruleRef string, ts time.Time) sink.ScopeEvent {
	return sink.ScopeEvent{
		Action:     action,
		Scope:      sink.ScopeFilter{ClusterID: "c1", APIGroup: "", Kind: "Pod", Namespace: namespace},
		APIVersion: "v1",
		RuleRef:    ruleRef,
		TS:         ts,
	}
}

// awaitRows waits until the recorder has at least n rows.
func awaitRows(t *testing.T, rec *scopeRowRecorder, n int) [][]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rows := rec.rows(); len(rows) >= n {
			return rows
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d watch_scopes rows, have %d", n, len(rec.rows()))
	return nil
}

// TestScopeEventsAreWrittenToWatchScopes pins the dedicated scope path: events go to
// watch_scopes (never to the record path's table), in the column order the frozen
// schema declares, batched by the scope worker rather than by the record workers.
func TestScopeEventsAreWrittenToWatchScopes(t *testing.T) {
	rec := &scopeRowRecorder{}
	conn := &fakeConn{sendErr: rec.hook()}
	w := newScopeTestWriter(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	ts := time.Date(2026, 7, 26, 12, 0, 0, 123456789, time.UTC)
	started := scopeEvent(sink.ScopeActionStarted, "team-a", "team-a/stream-pods", ts)
	stopped := scopeEvent(sink.ScopeActionStopped, "team-a", "team-a/stream-pods", ts.Add(time.Minute))
	for _, event := range []sink.ScopeEvent{started, stopped} {
		if err := w.EnqueueScopeEvent(ctx, event); err != nil {
			t.Fatalf("EnqueueScopeEvent: %v", err)
		}
	}

	rows := awaitRows(t, rec, 2)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v", err)
	}

	// Every prepared batch on this writer is a watch_scopes insert: the record path
	// was never involved.
	queries := conn.preparedQueries()
	if len(queries) == 0 {
		t.Fatal("no batch was prepared")
	}
	for _, q := range queries {
		if !strings.Contains(q, "INSERT INTO watch_scopes") {
			t.Errorf("scope events were written with %q, want an INSERT INTO watch_scopes", q)
		}
	}

	// The timestamp is bound as an instant, not as a formatted string: a bare
	// datetime string would be parsed in the process's local zone and land in the
	// UTC column shifted (see scopeInsertArgs).
	want := []any{
		ts, "c1", "", "v1", "Pod", "team-a", "Started", "team-a/stream-pods",
	}
	if !slices.Equal(rows[0], want) {
		t.Errorf("Started row = %v, want %v", rows[0], want)
	}
	if got := rows[1][6]; got != "Stopped" {
		t.Errorf("second row action = %v, want Stopped", got)
	}
	// Order is preserved: an inverted epoch is unreadable, and this path has exactly
	// one worker so that it cannot happen.
	first, second := rows[0][0].(time.Time), rows[1][0].(time.Time)
	if !first.Before(second) {
		t.Errorf("row timestamps out of order: %v then %v", first, second)
	}
}

// TestScopeEventsRetriedAfterAFailedFlush is the durability property that makes the
// recorder's best-effort hand-off safe: unlike a resource_states row, whose object
// will be observed again, a scope transition happens once. A flush that fails must
// keep its events and re-attempt them, with their original timestamps.
func TestScopeEventsRetriedAfterAFailedFlush(t *testing.T) {
	rec := &scopeRowRecorder{failures: 3}
	conn := &fakeConn{sendErr: rec.hook()}
	w := newScopeTestWriter(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	ts := time.Date(2026, 7, 26, 9, 30, 0, 0, time.UTC)
	if err := w.EnqueueScopeEvent(ctx, scopeEvent(sink.ScopeActionStarted, "team-b", "cluster-rule", ts)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}

	rows := awaitRows(t, rec, 1)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v", err)
	}

	if got := rows[0][0].(time.Time); !got.Equal(ts) {
		t.Errorf("retried row ts = %v, want the transition's own %v", got, ts)
	}
	if got := rows[0][5]; got != "team-b" {
		t.Errorf("retried row namespace = %v, want team-b", got)
	}
	// Exactly one row survived the retries: the retry queue re-attempts events, it
	// does not duplicate them.
	if len(rows) != 1 {
		t.Errorf("recorded %d rows, want exactly 1: %v", len(rows), rows)
	}
}

// TestScopeEventsDrainedBeforeTheConnectionCloses covers the shutdown ordering: the
// scope worker joins the same drain phase as the record workers, so a transition
// enqueued moments before the process exits still reaches ClickHouse — and does so
// strictly before the shared connection is closed.
func TestScopeEventsDrainedBeforeTheConnectionCloses(t *testing.T) {
	rec := &scopeRowRecorder{}
	conn := &fakeConn{sendErr: rec.hook()}
	// A long batch wait, so only the drain can flush this event.
	m := pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID)
	w := NewCHWriter(conn, 16, 1, 8, 50*time.Millisecond, time.Second, time.Second, time.Hour, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	ts := time.Now().UTC()
	if err := w.EnqueueScopeEvent(ctx, scopeEvent(sink.ScopeActionStopped, "team-c", "rule-c", ts)); err != nil {
		t.Fatalf("EnqueueScopeEvent: %v", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v", err)
	}

	if rec.batchCount() != 1 {
		t.Fatalf("flushed %d batches, want the drain to flush exactly 1", rec.batchCount())
	}
	if last, closed := conn.lastSend.Load(), conn.closeSeq.Load(); last == 0 || last > closed {
		t.Errorf("the drain's Send (seq %d) did not happen before Close (seq %d)", last, closed)
	}

	// After shutdown the queue refuses new events rather than blocking or dropping
	// them silently: the caller keeps them and reports.
	if err := w.EnqueueScopeEvent(context.Background(),
		scopeEvent(sink.ScopeActionStarted, "team-c", "rule-c", ts)); err == nil {
		t.Error("EnqueueScopeEvent accepted an event after shutdown, want an error")
	}
}

// TestScopeWasActiveQueryScoping pins the epoch probe's two load-bearing details: the
// namespace is matched exactly (an empty namespace is the all-namespaces *scope*, not
// a wildcard over namespaces), and the cutoff is strict, so the current epoch's own
// Started row can never answer for a previous one.
func TestScopeWasActiveQueryScoping(t *testing.T) {
	asOf := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)

	t.Run("an empty namespace is the cluster-wide scope, matched exactly", func(t *testing.T) {
		q, args := scopeWasActiveQuery(sink.ScopeFilter{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment"}, asOf)
		if !strings.Contains(q, "namespace = ?") {
			t.Errorf("expected an exact namespace predicate, got query:\n%s", q)
		}
		if !strings.Contains(q, "ts < ?") {
			t.Errorf("expected a strict ts cutoff, got query:\n%s", q)
		}
		if len(args) != 5 {
			t.Fatalf("expected 5 args, got %d: %v", len(args), args)
		}
		if args[0] != "c1" || args[1] != "apps" || args[2] != "Deployment" || args[3] != "" {
			t.Errorf("args = %v, want [c1 apps Deployment \"\" <ts>]", args)
		}
		if args[4] != asOf.Format(chTimeFormat) {
			t.Errorf("cutoff arg = %v, want %v", args[4], asOf.Format(chTimeFormat))
		}
	})

	t.Run("a namespaced scope is a different scope", func(t *testing.T) {
		_, args := scopeWasActiveQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "team-a",
		}, asOf)
		if args[3] != "team-a" {
			t.Errorf("namespace arg = %v, want team-a", args[3])
		}
	})
}

// TestActiveScopesQueryGroupsByScopeIdentity pins the boot-reconciliation
// enumeration: it groups by the scope's identity columns only. Including api_version
// would split one scope into two rows the moment two versions of a resource served
// it, and boot reconciliation would then close a scope that is still open.
func TestActiveScopesQueryGroupsByScopeIdentity(t *testing.T) {
	q, args := activeScopesQuery("c1")

	if strings.Contains(q, "api_version") {
		t.Errorf("the enumeration must not involve api_version (provenance, not identity):\n%s", q)
	}
	if !strings.Contains(q, "GROUP BY api_group, kind, namespace") {
		t.Errorf("expected a group-by over the scope identity columns, got:\n%s", q)
	}
	if !strings.Contains(q, "argMax(action, ts) = ?") {
		t.Errorf("expected the most-recent-action predicate, got:\n%s", q)
	}
	if len(args) != 2 || args[0] != "c1" || args[1] != string(sink.ScopeActionStarted) {
		t.Errorf("args = %v, want [c1 Started]", args)
	}
}
