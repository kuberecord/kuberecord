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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// checkpointCadence is the Checkpoint cadence this test's sink declares. It is far
// below the shipped default so a handful of mutations exercises several complete
// diff runs, which is what makes "the walk back is bounded" an observable property
// rather than an intention.
const checkpointCadence = 4

// checkpointMutations is how many times the object is mutated after its creation.
// With the cadence above it produces two Checkpoints with diff runs on both sides
// of them, so the reconstruction below has to pick the *last* data-bearing row
// rather than merely finding one.
const checkpointMutations = 11

// TestCheckpointStateReconstructionIntegration is Task 2.2's end-to-end claim: an
// object's state at an instant can be rebuilt from ClickHouse alone, in bounded
// work, and the result is byte-identical to the object as the operator normalizes
// it.
//
// It drives the real pipeline (normalize → hash → dedup → diff → version-gated
// commit) against a real ClickHouse through a real CHWriter, applies K sequential
// mutations to one object, and then executes exactly the recipe published in
// docs/SCHEMA.md ("Reconstructing state at an instant"): take the newest
// data-bearing row at or before the target instant, then apply the RFC 6902
// patches of every row after it, in ts order. The reconstructed document is
// compared with pipeline.NormalizedJSON of the live object — the same function the
// write path uses, so the assertion cannot drift away from the normalization rules
// the way a reimplementation of them would.
//
// Runs only under `make test-integration` (build tag `integration`), which stands
// up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestCheckpointStateReconstructionIntegration(t *testing.T) {
	const clusterID = "checkpoint-cluster"

	addr := envOrDefault("CH_TEST_ADDR", "127.0.0.1:9000")
	username := envOrDefault("CH_TEST_USER", "default")
	password := os.Getenv("CH_TEST_PASSWORD")
	database := envOrDefault("CH_TEST_DB", "default")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	// Start from empty tables: this test reads a whole object history back and
	// replays it, so a leftover row from an earlier run would be replayed too.
	dropOperatorTables(ctx, t, conn)
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}

	// A short batchMaxWait keeps the poll below quick; nothing about the property
	// under test depends on batching, and one worker keeps the insert order for a
	// single key identical to the order the pipeline issued the writes in.
	metrics := pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID)
	writer := NewCHWriter(conn, 64, 1, 8, 10*time.Second, 0, 5*time.Second, 100*time.Millisecond, time.Second, metrics)
	writer.checkpointEvery = checkpointCadence

	wctx, wcancel := context.WithCancel(context.Background())
	writerDone := make(chan error, 1)
	go func() { writerDone <- writer.Start(wctx) }()
	defer func() {
		wcancel()
		if err := <-writerDone; err != nil {
			t.Errorf("writer Start returned error: %v", err)
		}
	}()

	lister := &checkpointLister{}
	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: clusterID,
		Workers:   1,
		Lister:    lister,
		Router:    singleWriterRouter{writer: writer},
		Metrics:   pipeline.NewPipelineMetrics(prometheus.NewRegistry()),
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}

	key := pipeline.Key{Sink: testSinkID, Group: "apps", Kind: "Deployment",
		Namespace: "default", Name: "replayed"}
	// Warmed, so the object's first sighting is an Added rather than a Snapshot.
	// Both carry full data and either would do for the replay; Added simply makes
	// the recorded history the ordinary steady-state one.
	pipe.MarkScopeWarm(key.Sink, key.Scope())

	// Creation, then K sequential mutations. Each Process call settles one work
	// item, exactly as a worker draining the queue would.
	lister.set(checkpointDeployment(key.Name, 1))
	if err := pipe.Process(ctx, key); err != nil {
		t.Fatalf("Process(create): %v", err)
	}
	for i := range checkpointMutations {
		lister.set(checkpointDeployment(key.Name, i+2))
		if err := pipe.Process(ctx, key); err != nil {
			t.Fatalf("Process(mutation %d): %v", i+1, err)
		}
	}

	wantRows := checkpointMutations + 1
	waitForResourceRows(t, ctx, conn, clusterID, wantRows)

	// The history must actually contain Checkpoints — otherwise the replay below
	// would be proving nothing about bounded reconstruction.
	history := readHistory(t, ctx, conn, clusterID, key)
	if len(history) != wantRows {
		t.Fatalf("read %d rows back, want %d: %+v", len(history), wantRows, history)
	}
	checkpoints := 0
	for _, row := range history {
		if row.eventType != "Checkpoint" {
			continue
		}
		checkpoints++
		if row.data == "" || row.diff == "" {
			t.Errorf("Checkpoint row at %s must carry both data and diff (data empty=%v, diff empty=%v)",
				row.ts, row.data == "", row.diff == "")
		}
	}
	if want := checkpointMutations / checkpointCadence; checkpoints != want {
		t.Fatalf("history holds %d Checkpoint rows, want %d (cadence %d over %d mutations): %+v",
			checkpoints, want, checkpointCadence, checkpointMutations, history)
	}

	// --- The published recipe, executed verbatim (docs/SCHEMA.md) ---
	//
	// 1. The newest data-bearing row at or before the target instant is the base.
	//    A Checkpoint's own diff is already reflected in its data, so it is never
	//    re-applied — the replay starts strictly *after* the base row.
	base := -1
	for i, row := range history {
		if row.data != "" {
			base = i
		}
	}
	if base < 0 {
		t.Fatalf("no data-bearing row in the object's history: %+v", history)
	}
	// Bounded replay is the whole point: the walk back from the newest row can
	// never exceed the cadence.
	if replaySteps := len(history) - 1 - base; replaySteps >= checkpointCadence {
		t.Errorf("replay needs %d diffs, which is not bounded by the cadence %d",
			replaySteps, checkpointCadence)
	}

	// 2. Apply the RFC 6902 patch of every subsequent row, in ts order.
	state := []byte(history[base].data)
	for _, row := range history[base+1:] {
		patch, err := jsonpatch.DecodePatch([]byte(row.diff))
		if err != nil {
			t.Fatalf("decoding the patch written at %s: %v", row.ts, err)
		}
		state, err = patch.Apply(state)
		if err != nil {
			t.Fatalf("applying the patch written at %s: %v", row.ts, err)
		}
	}

	// 3. Compare with the live object, normalized by the very function the write
	//    path uses. Both sides are canonicalized (re-marshalled from a decoded
	//    document) because a patch application preserves the input's key order
	//    while normalization emits Go's sorted-key order — byte-equality is a
	//    claim about the JSON document, not about a serializer's whitespace or
	//    ordering choices.
	live, err := pipeline.NormalizedJSON(lister.current(), nil)
	if err != nil {
		t.Fatalf("NormalizedJSON(live object): %v", err)
	}
	gotJSON := canonicalJSON(t, state)
	wantJSON := canonicalJSON(t, live)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("reconstruction mismatch\n got: %s\nwant: %s", gotJSON, wantJSON)
	}

	// The reconstruction also has to agree with the row that recorded it: sha256 is
	// the hash of the normalized state, so a matching digest proves the recipe lands
	// on the same bytes the operator hashed rather than on something merely
	// equivalent after canonicalization.
	liveHash, err := pipeline.ObjectHash(lister.current(), nil)
	if err != nil {
		t.Fatalf("ObjectHash(live object): %v", err)
	}
	if last := history[len(history)-1]; last.sha256 != liveHash {
		t.Errorf("last row's sha256 = %s, want the live object's hash %s", last.sha256, liveHash)
	}
}

// historyRow is one resource_states row as the reconstruction reads it.
type historyRow struct {
	ts        time.Time
	eventType string
	data      string
	diff      string
	sha256    string
}

// readHistory returns one object's rows in ts order — the exact read the published
// recipe performs, minus its `ts <=` cutoff (this test reconstructs "now", so every
// row is at or before the target instant).
//
// FINAL is used for the same reason the recipe specifies it: resource_states is a
// ReplacingMergeTree and the write path is at-least-once, so an unmerged duplicate
// would otherwise be replayed twice — and applying one object's patch twice is not
// idempotent (a "remove" op fails the second time, an "add" duplicates).
func readHistory(t *testing.T, ctx context.Context, conn chdriver.Conn,
	clusterID string, key pipeline.Key) []historyRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
        SELECT ts, event_type, data, diff, sha256
        FROM resource_states FINAL
        WHERE cluster_id = ? AND api_group = ? AND kind = ? AND namespace = ? AND name = ?
        ORDER BY ts`, clusterID, key.Group, key.Kind, key.Namespace, key.Name)
	if err != nil {
		t.Fatalf("reading object history: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("closing history rows: %v", err)
		}
	}()

	var history []historyRow
	for rows.Next() {
		var row historyRow
		if err := rows.Scan(&row.ts, &row.eventType, &row.data, &row.diff, &row.sha256); err != nil {
			t.Fatalf("scanning a history row: %v", err)
		}
		history = append(history, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating object history: %v", err)
	}
	return history
}

// waitForResourceRows blocks until resource_states holds at least want rows for
// clusterID, so the read never races the writer's batch flush.
func waitForResourceRows(t *testing.T, ctx context.Context, conn chdriver.Conn, clusterID string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var count uint64
		row := conn.QueryRow(ctx, "SELECT count() FROM resource_states WHERE cluster_id = ?", clusterID)
		if err := row.Scan(&count); err != nil {
			t.Fatalf("counting resource_states rows: %v", err)
		}
		if int(count) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d resource_states rows, have %d", want, count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// canonicalJSON re-marshals a JSON document so two documents that differ only in
// key order or insignificant whitespace compare equal.
func canonicalJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decoding JSON for canonicalization: %v\n%s", err, raw)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("re-encoding canonical JSON: %v", err)
	}
	return out
}

// checkpointDeployment builds the object's revision-th state: one changed image
// tag plus one changed label per revision, which is the shape of an ordinary small
// update — a few dozen diff bytes against a much larger object, so the size
// trigger never fires and the cadence is what produces every Checkpoint.
func checkpointDeployment(name string, revision int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             "uid-replayed",
			"resourceVersion": fmt.Sprintf("%d", revision),
			"labels":          map[string]any{"app": "demo", "revision": fmt.Sprintf("r%d", revision)},
		},
		"spec": map[string]any{
			"replicas": int64(3),
			"selector": map[string]any{"matchLabels": map[string]any{"app": "demo"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "demo"}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name":  "app",
					"image": fmt.Sprintf("registry.example.com/demo:v%d", revision),
					"env": []any{
						map[string]any{"name": "LOG_LEVEL", "value": "info"},
						map[string]any{"name": "REVISION", "value": fmt.Sprintf("%d", revision)},
					},
				}}},
			},
		},
		"status": map[string]any{"readyReplicas": int64(3), "observedGeneration": int64(revision)},
	}}
}

// checkpointLister is a one-object pipeline.ListerRegistry: the watch cache as this
// test drives it. The scope is always active — this test is about replay, not about
// scope epochs.
type checkpointLister struct {
	mu  sync.Mutex
	obj *unstructured.Unstructured
}

func (l *checkpointLister) Get(_ pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.obj, l.obj != nil, true, nil
}

func (l *checkpointLister) set(obj *unstructured.Unstructured) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.obj = obj
}

func (l *checkpointLister) current() *unstructured.Unstructured {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.obj
}

// singleWriterRouter is a pipeline.SinkRouter that always resolves to one writer —
// the real CHWriter under test, so the Checkpoint cadence is read off the same
// object production reads it from.
type singleWriterRouter struct{ writer sink.Writer }

func (r singleWriterRouter) WriterFor(sink.ID) (sink.Writer, bool) { return r.writer, true }
