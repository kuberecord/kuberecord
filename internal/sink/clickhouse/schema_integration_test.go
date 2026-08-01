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

	"github.com/yelzhy/kubestream/internal/sink"
)

// TestSchemaRoundTripIntegration proves a real ClickHouse round-trip on the
// schema-v1 tables: auto-create the shipped DDL, validate it, insert a row via
// the exact writer query/args, then read it back through the warm-up query.
// Runs only under `make test-integration` (build tag `integration`), which
// stands up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestSchemaRoundTripIntegration(t *testing.T) {
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
	// Drop the throwaway tables on the way out so a persistent ClickHouse can be
	// re-targeted cleanly; the dockerized default target is discarded anyway.
	defer func() {
		_ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableResourceStates)
		_ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableWatchScopes)
		_ = conn.Close()
	}()

	// 1. Auto-create the shipped DDL (idempotent).
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}
	// Running it twice must be a no-op, proving idempotency.
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema (second run): %v", err)
	}

	// 2. Validate the live schema matches schema v1 exactly.
	if err := validateSchema(ctx, conn, database); err != nil {
		t.Fatalf("validateSchema: %v", err)
	}

	// 3. Insert one row via the exact writer query and positional args.
	rec := sink.Record{
		Timestamp:       time.Now().UTC(),
		ClusterID:       "it-cluster",
		EventType:       "Added",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "roundtrip",
		UID:             "uid-roundtrip",
		ResourceVersion: "123",
		Labels:          map[string]string{"app": "demo"},
		Data:            `{"kind":"Deployment"}`,
		SHA256:          "abc123",
	}
	if err := conn.Exec(ctx, insertResourceStateQuery, insertArgs(rec)...); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	// 4. Read it back via the warm-up query shape (api_group + kind + cluster_id).
	rows, err := conn.Query(ctx, `
        SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
        FROM resource_states
        WHERE api_group = ? AND kind = ? AND cluster_id = ?
        GROUP BY namespace, name
        HAVING argMax(event_type, ts) != 'Deleted'
    `, rec.APIGroup, rec.Kind, rec.ClusterID)
	if err != nil {
		t.Fatalf("warm query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got int
	for rows.Next() {
		var namespace, name, uid, sha string
		if err := rows.Scan(&namespace, &name, &uid, &sha); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got++
		if namespace != rec.Namespace || name != rec.Name || uid != rec.UID || sha != rec.SHA256 {
			t.Errorf("row mismatch: got (%s/%s uid=%s sha=%s), want (%s/%s uid=%s sha=%s)",
				namespace, name, uid, sha, rec.Namespace, rec.Name, rec.UID, rec.SHA256)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 warm-query row, got %d", got)
	}
}

// TestSchemaForwardCompatibilityIntegration is the freeze gate's tolerance test
// (Task 2.6): under the additive-only policy in docs/SCHEMA.md a table may be
// migrated ahead of the operator that writes to it, so an operator meeting a
// column it has never heard of must carry on rather than degrade its sink.
//
// Only a live ClickHouse can prove this. The fake-conn unit test proves
// validateSchema ignores unknown columns, but tolerance is worthless if the
// *write* path then fails: it is ClickHouse that decides whether an INSERT
// naming 15 of 16 columns is legal and what the omitted one ends up holding.
// So this asserts the whole contract — validate, insert through the real writer
// and scope-writer queries, read back through the warm-up query shape, and
// confirm the unknown columns took their declared defaults.
func TestSchemaForwardCompatibilityIntegration(t *testing.T) {
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
	// Start from empty tables: this test adds columns, so a table left behind by
	// an earlier test would already carry them and prove nothing.
	dropOperatorTables(ctx, t, conn)
	defer func() {
		dropOperatorTables(context.Background(), t, conn)
		_ = conn.Close()
	}()

	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}

	// Simulate a future additive migration, in both shapes the policy allows: a
	// DEFAULT-ed column and a Nullable one, appended and outside the sort key.
	migrations := []string{
		"ALTER TABLE " + tableResourceStates + " ADD COLUMN IF NOT EXISTS future_note String DEFAULT 'unset'",
		"ALTER TABLE " + tableResourceStates + " ADD COLUMN IF NOT EXISTS future_policy Nullable(String)",
		"ALTER TABLE " + tableWatchScopes + " ADD COLUMN IF NOT EXISTS future_operator_version Nullable(String)",
	}
	for _, stmt := range migrations {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("applying %q: %v", stmt, err)
		}
	}

	// 1. Validation tolerates the unknown columns on both tables.
	if err := validateSchema(ctx, conn, database); err != nil {
		t.Fatalf("validateSchema must ignore unknown extra columns, got: %v", err)
	}

	// 2. The write path still writes. Both operator queries name their columns
	// explicitly, so the unknown ones are simply not mentioned.
	rec := sink.Record{
		Timestamp:       time.Now().UTC(),
		ClusterID:       "fwdcompat-cluster",
		EventType:       "Added",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "fwdcompat",
		UID:             "uid-fwdcompat",
		ResourceVersion: "1",
		Labels:          map[string]string{"app": "demo"},
		Data:            `{"kind":"Deployment"}`,
		SHA256:          "fwdcompat-sha",
	}
	if err := conn.Exec(ctx, insertResourceStateQuery, insertArgs(rec)...); err != nil {
		t.Fatalf("insert into a table with extra columns: %v", err)
	}
	if err := conn.Exec(ctx, insertScopeEventQuery,
		rec.Timestamp, rec.ClusterID, rec.APIGroup, rec.APIVersion, rec.Kind, rec.Namespace,
		string(sink.ScopeActionStarted), "streamrule/default/fwdcompat",
	); err != nil {
		t.Fatalf("insert scope event into a table with extra columns: %v", err)
	}

	// 3. The read path still reads: the warm-up query shape selects named
	// columns, so extra ones neither appear nor interfere.
	rows, err := conn.Query(ctx, `
        SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
        FROM resource_states
        WHERE api_group = ? AND kind = ? AND cluster_id = ?
        GROUP BY namespace, name
        HAVING argMax(event_type, ts) != 'Deleted'
    `, rec.APIGroup, rec.Kind, rec.ClusterID)
	if err != nil {
		t.Fatalf("warm query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got int
	for rows.Next() {
		var namespace, name, uid, sha string
		if err := rows.Scan(&namespace, &name, &uid, &sha); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got++
		if namespace != rec.Namespace || name != rec.Name || uid != rec.UID || sha != rec.SHA256 {
			t.Errorf("row mismatch: got (%s/%s uid=%s sha=%s), want (%s/%s uid=%s sha=%s)",
				namespace, name, uid, sha, rec.Namespace, rec.Name, rec.UID, rec.SHA256)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 warm-query row, got %d", got)
	}

	// 4. The unknown columns took their declared defaults — which is what makes
	// rows written by an operator that predates a migration valid rows.
	var note string
	var policy *string
	if err := conn.QueryRow(ctx,
		"SELECT future_note, future_policy FROM "+tableResourceStates+" WHERE name = ?", rec.Name,
	).Scan(&note, &policy); err != nil {
		t.Fatalf("reading the extra columns back: %v", err)
	}
	if note != "unset" {
		t.Errorf("DEFAULT-ed extra column = %q, want %q", note, "unset")
	}
	if policy != nil {
		t.Errorf("Nullable extra column = %q, want NULL", *policy)
	}
}

// envOrDefault returns the named environment variable's value, or def if unset.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
