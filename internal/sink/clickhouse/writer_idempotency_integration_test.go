//go:build integration

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
	"os"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/sink"
)

// TestLostAckReInsertConvergesIntegration guards the H2 idempotency fix (Task
// 0.9, Fix 2) and the "zero duplicate rows per identity" property. The write
// path is at-least-once: a lost acknowledgement after a successful server-side
// insert makes the poison-isolation path re-insert a byte-identical row. Because
// Record.Timestamp is frozen at reconcile time, that re-insert shares the full
// ORDER BY key, so resource_states (ReplacingMergeTree) must collapse it to a
// single logical row on merge. The test inserts one row via the writer's normal
// path, then re-inserts the byte-identical row directly (simulating the isolation
// path after a lost ack), and asserts that both a FINAL query and
// LastKnownStates report exactly one row for the identity.
//
// It runs as if on a **non-UTC host** (see pinLocalZone): the convergence property
// used to hold only under TZ=UTC, because a re-insert rendered the timestamp through
// a different encoder than the batch attempt and the two disagreed about the zone.
// Pinning a half-hour offset keeps that regression caught wherever the suite runs.
//
// Runs only under `make test-integration` (build tag `integration`), which
// stands up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestLostAckReInsertConvergesIntegration(t *testing.T) {
	pinLocalZone(t, 5*time.Hour+30*time.Minute)

	addr := envOrDefault("CH_TEST_ADDR", "127.0.0.1:9000")
	username := envOrDefault("CH_TEST_USER", "default")
	password := os.Getenv("CH_TEST_PASSWORD")
	database := envOrDefault("CH_TEST_DB", "default")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := chdriver.Open(&chdriver.Options{
		Addr:        []string{addr},
		Auth:        chdriver.Auth{Database: database, Username: username, Password: password},
		Protocol:    chdriver.Native,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("open connection: %v", err)
	}
	// Start from empty tables. The teardown below cannot be relied on for this: the
	// CHWriter these tests hand `conn` to closes it during its own shutdown, so a
	// deferred DROP runs against a closed connection and silently does nothing. Both
	// tests count rows, so a leftover row from an earlier run would make them lie —
	// against a throwaway container as well as a developer's persistent ClickHouse.
	dropOperatorTables(ctx, t, conn)
	defer func() {
		_ = conn.Close()
	}()

	// 1. Apply the shipped schema so resource_states is ReplacingMergeTree.
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}

	rec := sink.Record{
		Timestamp:       time.Date(2026, 7, 23, 12, 0, 0, 987654321, time.UTC),
		ClusterID:       "idem-cluster",
		EventType:       "Added",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "lost-ack",
		UID:             "uid-lost-ack",
		ResourceVersion: "7",
		Labels:          map[string]string{"app": "demo"},
		Actors:          []string{"kubectl"},
		Data:            `{"kind":"Deployment"}`,
		SHA256:          "sha-lost-ack",
	}

	// 2. Insert the row via the writer's normal path (batched async Enqueue).
	// The writer is kept running for the rest of the test: its Start closes the
	// shared connection on shutdown, so LastKnownStates (step 4b) and the direct
	// re-insert (step 3) run while it is still alive, and shutdown happens last.
	reg := prometheus.NewRegistry()
	metrics := pipeline.NewPipelineMetrics(reg)
	w := NewCHWriter(conn, 10, 1, 10, 10*time.Second, 0, 5*time.Second, 50*time.Millisecond, time.Second, metrics)

	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(wctx) }()
	defer func() {
		wcancel()
		if err := <-done; err != nil {
			t.Errorf("writer Start returned error: %v", err)
		}
	}()

	committed := make(chan bool, 1)
	if err := w.Enqueue(wctx, sink.Job{Record: rec, Commit: func(ok bool) { committed <- ok }}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case ok := <-committed:
		if !ok {
			t.Fatalf("writer path reported the insert as failed")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for the writer to settle the insert")
	}

	// 3. Re-insert the byte-identical row, simulating the isolation path re-sending
	// after a lost acknowledgement (same ts, sha256, uid, event_type, and payload —
	// every ORDER BY column identical). It goes through a one-row prepared batch
	// because that is what the isolation path itself now does: sharing the batch
	// encoder is what makes the re-insert byte-identical rather than merely intended
	// to be (see flushBatch).
	if err := w.sendBatchOnce(ctx, []writeJob{{args: insertArgs(rec)}}); err != nil {
		t.Fatalf("isolation-path re-insert: %v", err)
	}

	// 3b. The stored timestamp is the instant the record carries — not that
	// wall-clock reading shifted into the process's local zone, which is what a
	// string-rendered argument produced.
	var storedTS time.Time
	tsRow := conn.QueryRow(ctx, `
        SELECT max(ts) FROM resource_states
        WHERE cluster_id = ? AND kind = ? AND name = ?`, rec.ClusterID, rec.Kind, rec.Name)
	if err := tsRow.Scan(&storedTS); err != nil {
		t.Fatalf("stored ts scan: %v", err)
	}
	if !storedTS.Equal(rec.Timestamp) {
		t.Fatalf("stored ts = %s, want the record's instant %s (local zone leaked into the binding)",
			storedTS.UTC(), rec.Timestamp)
	}

	// 4a. A FINAL query must report exactly one logical row for the identity,
	// regardless of whether the background merge has run yet.
	var finalCount uint64
	row := conn.QueryRow(ctx, `
        SELECT count()
        FROM (
            SELECT ts FROM resource_states FINAL
            WHERE cluster_id = ? AND api_group = ? AND kind = ? AND namespace = ? AND name = ?
        )`, rec.ClusterID, rec.APIGroup, rec.Kind, rec.Namespace, rec.Name)
	if err := row.Scan(&finalCount); err != nil {
		t.Fatalf("FINAL count scan: %v", err)
	}
	if finalCount != 1 {
		t.Fatalf("FINAL query returned %d rows for the identity, want exactly 1", finalCount)
	}

	// 4b. LastKnownStates for the scope must return exactly one KnownState.
	states, err := w.LastKnownStates(ctx, sink.ScopeFilter{
		ClusterID: rec.ClusterID,
		APIGroup:  rec.APIGroup,
		Kind:      rec.Kind,
	})
	if err != nil {
		t.Fatalf("LastKnownStates: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("LastKnownStates returned %d states, want exactly 1: %+v", len(states), states)
	}
	if states[0].Namespace != rec.Namespace || states[0].Name != rec.Name ||
		states[0].UID != rec.UID || states[0].SHA256 != rec.SHA256 {
		t.Fatalf("KnownState mismatch: got %+v, want ns=%s name=%s uid=%s sha=%s",
			states[0], rec.Namespace, rec.Name, rec.UID, rec.SHA256)
	}
}
