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

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// TestLastKnownStatesReportsUnclosedIncarnationsIntegration proves the per-UID
// grouping against a real ClickHouse (Task 1.12).
//
// One identity carries three incarnations: UID-A, whose history ends in a Deleted
// row (closed out, and therefore not the warm-up's business); UID-B, which was
// only ever Added (its death was never recorded — the delete-and-recreate the
// operator was down for); and UID-C, a later Added that is the current one. The
// query must report B and C and not A, each with its own sha256, api_version and
// timestamp, so the warm-up can seed C and close out B from history alone.
//
// The api_version column deliberately differs per incarnation: a close-out is
// dated *and versioned* from the incarnation it closes, not from whatever the
// identity looks like now.
//
// Runs only under `make test-integration` (build tag `integration`), which stands
// up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestLastKnownStatesReportsUnclosedIncarnationsIntegration(t *testing.T) {
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
	// Start from empty tables: this test counts rows, so anything an earlier run
	// left behind would make it lie. See the note in
	// writer_idempotency_integration_test.go for why the teardown cannot do this.
	dropOperatorTables(ctx, t, conn)
	defer func() {
		_ = conn.Close()
	}()

	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}

	const (
		clusterID = "reincarnation-cluster"
		group     = "apps"
		kind      = "Deployment"
		namespace = "default"
		name      = "web"
	)
	base := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)

	// Every row for one identity, oldest first. The insert order is deliberately
	// not the chronological order for UID-B and UID-C, so the test also proves the
	// answer comes from ts rather than from insertion order.
	history := []sink.Record{
		{
			Timestamp: base, ClusterID: clusterID, EventType: "Added",
			APIGroup: group, APIVersion: "v1beta1", Kind: kind, Namespace: namespace, Name: name,
			UID: "uid-a", Data: `{"kind":"Deployment"}`, SHA256: "sha-a1",
		},
		{
			Timestamp: base.Add(time.Hour), ClusterID: clusterID, EventType: "Deleted",
			APIGroup: group, APIVersion: "v1beta1", Kind: kind, Namespace: namespace, Name: name,
			UID: "uid-a",
		},
		{
			Timestamp: base.Add(4 * time.Hour), ClusterID: clusterID, EventType: "Added",
			APIGroup: group, APIVersion: "v1", Kind: kind, Namespace: namespace, Name: name,
			UID: "uid-c", Data: `{"kind":"Deployment"}`, SHA256: "sha-c1",
		},
		{
			Timestamp: base.Add(2 * time.Hour), ClusterID: clusterID, EventType: "Added",
			APIGroup: group, APIVersion: "v1beta2", Kind: kind, Namespace: namespace, Name: name,
			UID: "uid-b", Data: `{"kind":"Deployment"}`, SHA256: "sha-b1",
		},
		{
			Timestamp: base.Add(3 * time.Hour), ClusterID: clusterID, EventType: "Modified",
			APIGroup: group, APIVersion: "v1beta2", Kind: kind, Namespace: namespace, Name: name,
			UID: "uid-b", Diff: `[{"op":"replace","path":"/spec/replicas","value":2}]`, SHA256: "sha-b2",
		},
	}

	reg := prometheus.NewRegistry()
	metrics := pipeline.NewPipelineMetrics(reg).ForSink(testSinkName)
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

	for _, rec := range history {
		committed := make(chan bool, 1)
		if err := w.Enqueue(wctx, sink.Job{Record: rec, Commit: func(ok bool) { committed <- ok }}); err != nil {
			t.Fatalf("Enqueue %s/%s: %v", rec.UID, rec.EventType, err)
		}
		select {
		case ok := <-committed:
			if !ok {
				t.Fatalf("the writer reported the %s row for %s as failed", rec.EventType, rec.UID)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out waiting for the %s row for %s to settle", rec.EventType, rec.UID)
		}
	}

	states, err := w.LastKnownStates(ctx, sink.ScopeFilter{
		ClusterID: clusterID,
		APIGroup:  group,
		Kind:      kind,
	})
	if err != nil {
		t.Fatalf("LastKnownStates: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("LastKnownStates returned %d states, want exactly 2 (uid-b and uid-c): %+v", len(states), states)
	}

	byUID := make(map[string]sink.KnownState, len(states))
	for _, st := range states {
		if st.Namespace != namespace || st.Name != name {
			t.Errorf("state %+v is not for %s/%s", st, namespace, name)
		}
		byUID[st.UID] = st
	}
	if _, closed := byUID["uid-a"]; closed {
		t.Error("uid-a was reported despite its own history ending in a Deleted row")
	}

	want := map[string]sink.KnownState{
		// uid-b's death was never recorded, so it is still reported — and it is
		// reported at its *own* last event (the Modified at +3h), which is the
		// timestamp its close-out will carry.
		"uid-b": {UID: "uid-b", SHA256: "sha-b2", APIVersion: "v1beta2", TS: base.Add(3 * time.Hour)},
		"uid-c": {UID: "uid-c", SHA256: "sha-c1", APIVersion: "v1", TS: base.Add(4 * time.Hour)},
	}
	for uid, expected := range want {
		got, ok := byUID[uid]
		if !ok {
			t.Errorf("%s is missing from the answer: %+v", uid, states)
			continue
		}
		if got.SHA256 != expected.SHA256 {
			t.Errorf("%s sha256 = %q, want %q", uid, got.SHA256, expected.SHA256)
		}
		if got.APIVersion != expected.APIVersion {
			t.Errorf("%s api_version = %q, want its own %q", uid, got.APIVersion, expected.APIVersion)
		}
		if !got.TS.Equal(expected.TS) {
			t.Errorf("%s ts = %s, want its own last event at %s", uid, got.TS.UTC(), expected.TS)
		}
	}

	// The classification the warm-up makes: the greatest ts is the current
	// incarnation, everything else is a death nobody recorded.
	if !byUID["uid-c"].TS.After(byUID["uid-b"].TS) {
		t.Errorf("uid-c ts (%s) must be the greatest; uid-b is at %s",
			byUID["uid-c"].TS.UTC(), byUID["uid-b"].TS.UTC())
	}
}
