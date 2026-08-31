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

// The other half of the boundary stated at the top of conformance_test.go: the
// fake proves the Go logic, and this run proves the SQL semantics.
//
// It runs the *same* suite against a real server, so the claims the stand-in can
// only assume are checked where they are actually decided — that FINAL collapses
// an unmerged duplicate, that a DateTime64(9) bound bound as a string compares at
// nanosecond precision, that hasAny over a LowCardinality array means what this
// package reads it as, and that JSONExtractString reaches into both Event
// spellings.
//
// Runs only under `make test-integration` (build tag `integration`), which stands
// up a dockerized ClickHouse and points CH_TEST_ADDR at it.

package clickhouse

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	schemaddl "github.com/kuberecord/kuberecord/deploy/clickhouse/schema"
	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The INSERT statements the harness seeds through. They are the frozen schema's
// own column order, written out here because this file is a *writer* against a
// schema it otherwise only reads — and a column list that drifted from the DDL
// would shift every value one place with nothing to catch it but a wrong answer.
const (
	insertStateQuery = `INSERT INTO resource_states (
        ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid,
        resource_version, labels, actors, data, diff, sha256
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertScopeQuery = `INSERT INTO watch_scopes (
        ts, cluster_id, api_group, api_version, kind, namespace, action, rule_ref
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// TestQueryConformanceIntegration runs the read-plane contract against a real
// ClickHouse.
func TestQueryConformanceIntegration(t *testing.T) {
	conn := openIntegrationConn(t)
	conformance.RunQuerySuite(t, func(t *testing.T) conformance.Harness {
		faulting := &faultingConn{Conn: conn}
		engine, err := New(faulting)
		if err != nil {
			t.Fatalf("building an engine over the integration connection: %v", err)
		}
		return conformance.Harness{
			Engine: engine,
			Seed:   func(h conformance.History) error { return seedServer(t, conn, h) },
			// The corpus becomes rows in the shipped tables, through the same INSERT
			// the history fixtures travel: the flush labels carry nothing this
			// backend stores, so the row rendering is the whole of the translation.
			SeedCorpus: func(c conformance.Corpus) error {
				return seedServer(t, conn, c.History())
			},
			SetStreamFault: faulting.setFault,
			Capabilities:   fakeCapabilities(),
		}
	})
}

// TestFinalDedupesUnmergedDuplicateIntegration is the one claim the stand-in can
// only emulate, checked where it is actually decided.
//
// The operator's write path is at-least-once: a lost acknowledgement after a
// successful insert makes the poison-isolation path re-insert a byte-identical
// row, which collides on the full sort key and is collapsed by
// ReplacingMergeTree — but only on merge, which happens when it happens. Until
// then the table holds the row twice, and a timeline read without FINAL renders
// one recorded change as two.
//
// The test proves both halves: that the duplicate is genuinely there (a raw count
// sees two rows) and that this backend's read does not see it.
func TestFinalDedupesUnmergedDuplicateIntegration(t *testing.T) {
	conn := openIntegrationConn(t)
	ctx := context.Background()

	history := conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(0, query.EventAdded, uidA, []string{actorKubectl}), `{"spec":{"replicas":1}}`),
	}}
	if err := seedServer(t, conn, history); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	// A second, byte-identical insert: exactly what a re-inserted row is.
	if err := insertStates(ctx, conn, history.Rows); err != nil {
		t.Fatalf("re-inserting the duplicate: %v", err)
	}

	if raw := countStates(t, ctx, conn, false); raw != 2 {
		t.Fatalf("a raw count sees %d rows, want 2; without an unmerged duplicate on the table this "+
			"test proves nothing about FINAL", raw)
	}
	if deduped := countStates(t, ctx, conn, true); deduped != 1 {
		t.Fatalf("a FINAL count sees %d rows, want 1", deduped)
	}

	engine, err := New(conn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	}()

	from, to := fixtureWindow()
	got := drainTimeline(t, engine, query.TimelineQuery{Ref: testRef(), From: from, To: to})
	if len(got) != 1 {
		t.Errorf("the timeline rendered %d changes over one recorded change with an unmerged duplicate "+
			"beside it; a duplicated row in an audit timeline says the cluster did something twice", len(got))
	}
}

// TestEventCorrelationAcrossBothGroupSpellingsIntegration proves the one piece of
// this backend's SQL the conformance suite never reaches.
//
// The contract has no IncludeEvents property — Event correlation is a ClickHouse
// mapping concern rather than a read-plane obligation — so nothing else here
// checks that JSONExtractString with a two-segment path means what this package
// reads it as, that coalesce over nullIf picks the populated spelling, or that
// `api_group IN (”, 'events.k8s.io')` catches both. Those are exactly the claims
// a stand-in can only assume, and getting one wrong would drop half a cluster's
// commentary with nothing in the output marking the gap (Task 3.1).
func TestEventCorrelationAcrossBothGroupSpellingsIntegration(t *testing.T) {
	conn := openIntegrationConn(t)
	if err := seedServer(t, conn, eventFixture()); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	engine, err := New(conn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	}()

	from, to := fixtureWindow()
	got := drainTimeline(t, engine, query.TimelineQuery{
		Ref: testRef(), From: from, To: to, IncludeEvents: true,
	})

	wantTypes := []string{
		query.EventAdded, query.EventKubernetes, query.EventModified, query.EventKubernetes,
	}
	gotTypes := make([]string, 0, len(got))
	for _, c := range got {
		gotTypes = append(gotTypes, c.EventType)
	}
	if !slices.Equal(gotTypes, wantTypes) {
		t.Fatalf("the merged timeline is %v, want %v — the two changes with one Event from each API "+
			"spelling interleaved in ts order. A missing Event means the subject predicate did not reach "+
			"into that spelling's key", gotTypes, wantTypes)
	}
	if !strings.Contains(got[1].Data, "involvedObject") {
		t.Errorf("the first merged row is %q, want the core-group Event that names its subject in "+
			"involvedObject", got[1].Data)
	}
	if !strings.Contains(got[3].Data, "regarding") {
		t.Errorf("the second merged row is %q, want the events.k8s.io Event that names its subject in "+
			"regarding", got[3].Data)
	}
}

// countStates counts the rows of resource_states, with or without FINAL.
func countStates(t *testing.T, ctx context.Context, conn driver.Conn, final bool) uint64 {
	t.Helper()
	from := "resource_states"
	if final {
		from += " FINAL"
	}
	rows, err := conn.Query(ctx, "SELECT count() FROM "+from)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("closing the count rows: %v", err)
		}
	}()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("scanning the count: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	return count
}

// testDatabase is this suite's own ClickHouse database.
//
// It is not the shared `default` one the sink's integration tests use, and that
// is not tidiness: `go test` runs package binaries in parallel, and this suite
// truncates resource_states between every property while internal/sink/clickhouse
// drops and recreates it. Sharing a database would let the two delete each
// other's fixtures nondeterministically, and the failure would look like a
// backend bug rather than like the scheduling accident it is.
const testDatabase = "kuberecord_query_it"

// openIntegrationConn dials the dockerized server, gives this suite a private
// database, and applies the shipped DDL into it.
//
// The pool is pinned to a single connection, and that is not tuning: the
// contract's early-close property asserts that an abandoned iterator leaves no
// goroutine running, measured as a count against a baseline. A pool free to open a
// second connection mid-suite would grow that count for a reason that has nothing
// to do with the iterator, and the property would fail for the wrong reason —
// which is worse than not running it, because somebody would eventually relax the
// property to make it stop.
func openIntegrationConn(t *testing.T) driver.Conn {
	t.Helper()

	options := func(database string, pooled bool) *chdriver.Options {
		opts := &chdriver.Options{
			Addr: []string{envOrDefault("CH_TEST_ADDR", "127.0.0.1:9000")},
			Auth: chdriver.Auth{
				Database: database,
				Username: envOrDefault("CH_TEST_USER", "default"),
				Password: os.Getenv("CH_TEST_PASSWORD"),
			},
			Protocol:    chdriver.Native,
			DialTimeout: 5 * time.Second,
			ReadTimeout: 30 * time.Second,
		}
		if pooled {
			opts.MaxOpenConns = 1
			opts.MaxIdleConns = 1
		}
		return opts
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The private database has to be created through a connection to an existing
	// one, so this opens twice: once to create it, once to work in it.
	bootstrap, err := chdriver.Open(options(envOrDefault("CH_TEST_DB", "default"), false))
	if err != nil {
		t.Fatalf("opening a bootstrap connection: %v", err)
	}
	if err := bootstrap.Exec(ctx, "DROP DATABASE IF EXISTS "+testDatabase); err != nil {
		t.Fatalf("dropping a stale %s: %v", testDatabase, err)
	}
	if err := bootstrap.Exec(ctx, "CREATE DATABASE "+testDatabase); err != nil {
		t.Fatalf("creating %s: %v", testDatabase, err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("closing the bootstrap connection: %v", err)
	}

	conn, err := chdriver.Open(options(testDatabase, true))
	if err != nil {
		t.Fatalf("opening a connection to %s: %v", testDatabase, err)
	}
	t.Cleanup(func() {
		// Best effort on the way out; the dockerized target is discarded anyway,
		// and a persistent one is left clean for the next run.
		if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+testDatabase); err != nil {
			t.Logf("dropping %s: %v", testDatabase, err)
		}
		if err := conn.Close(); err != nil {
			t.Logf("closing the connection: %v", err)
		}
	})

	if err := applySchema(ctx, conn); err != nil {
		t.Fatalf("applying the shipped DDL: %v", err)
	}
	return conn
}

// applySchema executes the shipped DDL files in filename order.
//
// The files are the ones frozen as a public API in Task 2.6 and embedded for the
// operator's own auto-create path, read here rather than restated: a query backend
// tested against a schema written out in its own test file would be tested against
// its author's memory of the schema.
func applySchema(ctx context.Context, conn driver.Conn) error {
	entries, err := schemaddl.FS.ReadDir(".")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)

	for _, name := range names {
		ddl, err := schemaddl.FS.ReadFile(name)
		if err != nil {
			return err
		}
		// The native protocol executes a single statement per Exec; trimming a
		// trailing ';' avoids an empty second statement being parsed.
		if err := conn.Exec(ctx, strings.TrimRight(strings.TrimSpace(string(ddl)), ";")); err != nil {
			return fmt.Errorf("executing %s: %w", name, err)
		}
	}
	return nil
}

// seedServer makes a conformance History the server's recorded past.
//
// It truncates first. Every property plants its own history and asserts on the
// whole of what comes back, so a row left behind by the previous property would
// not merely add noise — it would be counted.
func seedServer(t *testing.T, conn driver.Conn, h conformance.History) error {
	t.Helper()
	ctx := context.Background()

	for _, table := range []string{tableResourceStates, tableWatchScopes} {
		if err := conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncating %s: %w", table, err)
		}
	}
	if err := insertStates(ctx, conn, h.Rows); err != nil {
		return err
	}
	return insertScopes(ctx, conn, h.Scopes)
}

// insertStates writes recorded changes into resource_states.
//
// ts is bound as a time.Time — an instant — and never as a formatted datetime
// string. The driver parses such a string client-side and reinterprets its digits
// in time.Local, so a machine outside UTC would seed every fixture shifted by its
// own offset and the nanosecond-precision property would fail for a reason that
// has nothing to do with this backend. (Reads are the mirror image; see
// chTimeFormat.)
func insertStates(ctx context.Context, conn driver.Conn, rows []conformance.Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, insertStateQuery)
	if err != nil {
		return fmt.Errorf("preparing a %s batch: %w", tableResourceStates, err)
	}
	for _, r := range rows {
		labels := r.Change.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		actors := r.Change.Actors
		if actors == nil {
			actors = []string{}
		}
		appendErr := batch.Append(
			r.Change.TS.UTC(), r.Ref.ClusterID, r.Change.EventType, r.Ref.APIGroup, r.Change.APIVersion,
			r.Ref.Kind, r.Ref.Namespace, r.Ref.Name, r.Change.UID, r.Change.ResourceVersion,
			labels, actors, r.Change.Data, r.Change.Diff, r.Change.SHA256,
		)
		if appendErr != nil {
			return fmt.Errorf("appending a %s row: %w", tableResourceStates, appendErr)
		}
	}
	return batch.Send()
}

// insertScopes writes watch-scope transitions into watch_scopes.
//
// The cluster comes from conformance.FixtureClusterID: the fixture's transitions
// carry no cluster of their own, and the coverage fixture seeds no rows beside
// them to infer one from, so this is the only place the two halves can be joined
// up. api_version is provenance and never identity, so any recorded value is
// truthful; "v1" is what an informer on the core group would have reported.
func insertScopes(ctx context.Context, conn driver.Conn, scopes []conformance.ScopeTransition) error {
	if len(scopes) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, insertScopeQuery)
	if err != nil {
		return fmt.Errorf("preparing a %s batch: %w", tableWatchScopes, err)
	}
	for _, s := range scopes {
		appendErr := batch.Append(
			s.TS.UTC(), conformance.FixtureClusterID, s.APIGroup, "v1",
			s.Kind, s.Namespace, string(s.Action), s.RuleRef,
		)
		if appendErr != nil {
			return fmt.Errorf("appending a %s row: %w", tableWatchScopes, appendErr)
		}
	}
	return batch.Send()
}

// faultingConn is a driver.Conn that can break the next change stream part-way
// through.
//
// The suite's fault lever is mandatory and there is no way to make a real server
// die on cue, so the fault is injected at the seam the engine is handed: the
// connection it was constructed with. The engine is unchanged and the failure
// travels its real path — surfacing through the driver rows' own Err, which is
// exactly where a dropped connection would put it.
//
// What this does *not* prove is that ClickHouse fails this way; that is the one
// thing in this file the stand-in and the server are equally unable to
// demonstrate, and it is a property of the driver rather than of this backend.
type faultingConn struct {
	driver.Conn
	fault *conformance.StreamFault
}

func (c *faultingConn) setFault(f *conformance.StreamFault) { c.fault = f }

func (c *faultingConn) Query(ctx context.Context, sqlText string, args ...any) (driver.Rows, error) {
	rows, err := c.Conn.Query(ctx, sqlText, args...)
	if err != nil || c.fault == nil {
		return rows, err
	}
	// Only the change stream is broken. The newest-incarnation probe runs first
	// and would otherwise consume the fault, turning a mid-stream failure into a
	// refused query — a different property than the one under test.
	parsed, parseErr := parseStatement(sqlText, args)
	if parseErr != nil || parsed.projection != fakeChangeColumns {
		return rows, nil
	}
	return &faultingRows{Rows: rows, fault: c.fault}, nil
}

// faultingRows delivers a fixed number of rows and then reports the injected
// failure through Err.
//
// Rows already delivered stay delivered and Next simply stops yielding, which is
// how a dropped connection really behaves — and why a reader that only checked
// Scan's error would see a short, clean result set.
type faultingRows struct {
	driver.Rows
	fault     *conformance.StreamFault
	delivered int
}

func (r *faultingRows) broken() bool { return r.delivered >= r.fault.AfterChanges }

func (r *faultingRows) Next() bool {
	if r.broken() {
		return false
	}
	return r.Rows.Next()
}

func (r *faultingRows) Scan(dest ...any) error {
	if err := r.Rows.Scan(dest...); err != nil {
		return err
	}
	r.delivered++
	return nil
}

func (r *faultingRows) Err() error {
	if r.broken() {
		return r.fault.Err
	}
	return r.Rows.Err()
}

// envOrDefault reads an environment variable, falling back when it is unset.
func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
