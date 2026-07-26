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

// TestScopeEpochRoundTripIntegration exercises the scope log against a real
// ClickHouse: transitions written through the dedicated batcher, then read back
// through the two scope-epoch queries the warm/GC coordinator's correctness rests
// on.
//
// It exists because those two queries cannot be validated by a unit test. A
// query-shape assertion proves the SQL says what we meant; only ClickHouse can prove
// the SQL is valid, that argMax over a LowCardinality column behaves as assumed, and
// that the strict `ts <` cutoff really excludes the epoch's own Started row — the
// check that stops a brand-new scope's pre-history from being recorded as deletions.
//
// Runs only under `make test-integration` (build tag `integration`), which stands up
// a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestScopeEpochRoundTripIntegration(t *testing.T) {
	// Run as if on a non-UTC host: the epoch cutoff is only exact if neither the
	// insert nor the query lets the process's local zone in (see pinLocalZone).
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

	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}

	metrics := pipeline.NewPipelineMetrics(prometheus.NewRegistry())
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

	const clusterID = "epoch-cluster"
	// Three scopes, each with the history a different warm-up verdict depends on:
	//   open      — Started and never closed (a crashed process): GC must run.
	//   closed    — Started then properly Stopped: GC must not run.
	//   fresh     — no history at all (a brand-new rule): GC must not run.
	open := sink.ScopeFilter{ClusterID: clusterID, APIGroup: "apps", Kind: "Deployment", Namespace: "team-a"}
	closed := sink.ScopeFilter{ClusterID: clusterID, APIGroup: "apps", Kind: "Deployment", Namespace: "team-b"}
	clusterWide := sink.ScopeFilter{ClusterID: clusterID, APIGroup: "", Kind: "Node"}
	fresh := sink.ScopeFilter{ClusterID: clusterID, APIGroup: "apps", Kind: "StatefulSet", Namespace: "team-a"}

	base := time.Date(2026, 7, 26, 8, 0, 0, 0, time.UTC)
	events := []sink.ScopeEvent{
		{Action: sink.ScopeActionStarted, Scope: open, APIVersion: "v1", RuleRef: "team-a/rule", TS: base},
		{Action: sink.ScopeActionStarted, Scope: closed, APIVersion: "v1", RuleRef: "team-b/rule", TS: base},
		{Action: sink.ScopeActionStopped, Scope: closed, APIVersion: "v1", RuleRef: "team-b/rule", TS: base.Add(time.Hour)},
		{Action: sink.ScopeActionStarted, Scope: clusterWide, APIVersion: "v1", RuleRef: "cluster-rule", TS: base},
	}
	for _, event := range events {
		if err := w.EnqueueScopeEvent(wctx, event); err != nil {
			t.Fatalf("EnqueueScopeEvent(%s %s): %v", event.Action, event.Scope.Namespace, err)
		}
	}

	// The batcher is asynchronous, so wait for the rows to land rather than assuming
	// they have.
	waitForScopeRows(t, ctx, conn, clusterID, len(events))

	// Each row carries the instant its transition was observed, unshifted by the
	// operator's local zone.
	var earliest time.Time
	tsRow := conn.QueryRow(ctx, "SELECT min(ts) FROM watch_scopes WHERE cluster_id = ?", clusterID)
	if err := tsRow.Scan(&earliest); err != nil {
		t.Fatalf("stored ts scan: %v", err)
	}
	if !earliest.Equal(base) {
		t.Fatalf("earliest recorded transition = %s, want %s (local zone leaked into the binding)",
			earliest.UTC(), base)
	}

	// --- The epoch check, as of well after every recorded transition ---
	asOf := base.Add(24 * time.Hour)
	cases := []struct {
		name   string
		filter sink.ScopeFilter
		want   bool
	}{
		{name: "an epoch left open means the scope was being watched", filter: open, want: true},
		{name: "a properly closed epoch means it was not", filter: closed, want: false},
		{name: "a cluster-wide scope is matched by its empty namespace", filter: clusterWide, want: true},
		{name: "a scope with no history at all is not", filter: fresh, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := w.ScopeWasActive(ctx, tc.filter, asOf)
			if err != nil {
				t.Fatalf("ScopeWasActive: %v", err)
			}
			if got != tc.want {
				t.Errorf("ScopeWasActive(%+v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}

	// The strict cutoff is what makes the check race-free: asked as of the instant
	// its own Started row carries, an otherwise-fresh scope must still report "not
	// previously watched".
	if got, err := w.ScopeWasActive(ctx, open, base); err != nil {
		t.Fatalf("ScopeWasActive at the epoch's own instant: %v", err)
	} else if got {
		t.Error("the epoch check counted the scope's own Started row as a previous epoch")
	}

	// A namespaced scope's history must not answer for the cluster-wide scope over the
	// same kind: they are different scopes with independent epochs.
	deploymentsClusterWide := sink.ScopeFilter{ClusterID: clusterID, APIGroup: "apps", Kind: "Deployment"}
	if got, err := w.ScopeWasActive(ctx, deploymentsClusterWide, asOf); err != nil {
		t.Fatalf("ScopeWasActive (cluster-wide Deployments): %v", err)
	} else if got {
		t.Error("a namespaced scope's open epoch answered for the cluster-wide scope over the same kind")
	}

	// --- The boot-reconciliation enumeration ---
	scopes, err := w.ActiveScopes(ctx, clusterID)
	if err != nil {
		t.Fatalf("ActiveScopes: %v", err)
	}
	wantOpen := map[sink.ScopeFilter]bool{open: false, clusterWide: false}
	for _, scope := range scopes {
		seen, expected := wantOpen[scope]
		if !expected {
			t.Errorf("ActiveScopes returned a scope whose epoch is closed or absent: %+v", scope)
			continue
		}
		if seen {
			t.Errorf("ActiveScopes returned %+v twice", scope)
		}
		wantOpen[scope] = true
	}
	for scope, seen := range wantOpen {
		if !seen {
			t.Errorf("ActiveScopes did not report the open scope %+v", scope)
		}
	}

	// Another cluster's scopes are invisible: the sink is a multi-cluster store, and a
	// boot pass must never close an epoch belonging to a different operator.
	other, err := w.ActiveScopes(ctx, "some-other-cluster")
	if err != nil {
		t.Fatalf("ActiveScopes (other cluster): %v", err)
	}
	if len(other) != 0 {
		t.Errorf("ActiveScopes leaked %d scopes from another cluster: %+v", len(other), other)
	}
}

// dropOperatorTables removes both operator tables so a test starts from a known
// empty state. It is fatal on failure: silently continuing against leftover rows is
// how a counting assertion turns into a false pass.
func dropOperatorTables(ctx context.Context, t *testing.T, conn chdriver.Conn) {
	t.Helper()
	for _, table := range []string{tableResourceStates, tableWatchScopes} {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("dropping %s: %v", table, err)
		}
	}
}

// waitForScopeRows blocks until watch_scopes holds at least want rows for
// clusterID, so an assertion never races the batcher's flush.
func waitForScopeRows(t *testing.T, ctx context.Context, conn chdriver.Conn, clusterID string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var count uint64
		row := conn.QueryRow(ctx, "SELECT count() FROM watch_scopes WHERE cluster_id = ?", clusterID)
		if err := row.Scan(&count); err != nil {
			t.Fatalf("counting watch_scopes rows: %v", err)
		}
		if int(count) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d watch_scopes rows, have %d", want, count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
