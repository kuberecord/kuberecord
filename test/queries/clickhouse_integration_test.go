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

package queries

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	schemaddl "github.com/yelzhy/kuberecord/deploy/clickhouse/schema"
)

// testDatabase is this suite's own ClickHouse database.
//
// It is not the shared `default` one the sink's integration tests use, and that
// is not tidiness: `go test` runs package binaries in parallel, and two suites
// dropping and recreating resource_states in the same database would delete each
// other's fixtures nondeterministically. A private database also makes the
// frozen-column assertion below exact — nothing but the shipped DDL has ever run
// against these tables.
const testDatabase = "kuberecord_queries_it"

// demoCluster is the cluster_id every fixture row carries. It matches
// demoVariableValues, which is what makes "the query ran" and "the query returned
// rows" the same assertion.
const demoCluster = "demo-cluster"

// TestPublishedQueriesRunAgainstFrozenSchemaIntegration is Task 3.2's third
// acceptance criterion: every query kubestream publishes — the recipes in
// docs/QUERIES.md and every statement inside the four product dashboards — is
// executed against a real ClickHouse whose tables were built from
// deploy/clickhouse/schema and nothing else.
//
// That construction is the proof. The criterion asks that every query use only
// frozen-schema columns; rather than pattern-matching identifiers out of SQL,
// which would be both fragile and easy to fool, the test gives ClickHouse a table
// that has the frozen columns and no others and lets its parser answer. A query
// naming a column that is not frozen fails with UNKNOWN_IDENTIFIER, and a query
// naming one that was quietly added outside the freeze fails the extra-column
// assertion first.
//
// Runs only under `make test-integration` (build tag `integration`), which stands
// up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestPublishedQueriesRunAgainstFrozenSchemaIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	conn := connectToTestDatabase(ctx, t)
	applyShippedDDL(ctx, t, conn)
	assertOnlyFrozenColumns(ctx, t, conn)
	fixture := seedDemoData(ctx, t, conn)

	for _, path := range queryLibraries {
		t.Run(shortSource(path), func(t *testing.T) {
			library, err := FromMarkdown(path)
			if err != nil {
				t.Fatalf("FromMarkdown: %v", err)
			}
			if len(library) == 0 {
				t.Fatal("this document holds no SQL blocks; the check would pass vacuously")
			}
			for _, q := range library {
				t.Run(shortSource(q.Source), func(t *testing.T) {
					params := Parameters(q.SQL)
					for name := range params {
						if _, ok := fixture.params[name]; !ok {
							t.Fatalf("query declares parameter {%s}, which the demo fixture has no value for; "+
								"add one to the fixture in seedDemoData so this query is exercised on real data", name)
						}
					}
					bound, err := BindValues(params, fixture.params)
					if err != nil {
						t.Fatalf("bind parameters: %v", err)
					}
					queryCtx := chdriver.Context(ctx, chdriver.WithParameters(chdriver.Parameters(bound)))
					assertReturnsRows(queryCtx, t, conn, q.SQL)
				})
			}
		})
	}

	for _, path := range productDashboards {
		t.Run(shortSource(path), func(t *testing.T) {
			dash, err := FromDashboard(path)
			if err != nil {
				t.Fatalf("FromDashboard: %v", err)
			}
			vals, err := DemoValues(dash)
			if err != nil {
				t.Fatalf("DemoValues: %v", err)
			}
			for _, q := range dash.Queries {
				t.Run(shortSource(q.Source), func(t *testing.T) {
					stmt, err := Interpolate(q.SQL, vals)
					if err != nil {
						t.Fatalf("interpolate: %v", err)
					}
					assertReturnsRows(ctx, t, conn, stmt)
				})
			}
		})
	}
}

// assertReturnsRows executes one statement and requires at least one row back.
//
// Requiring rows, not merely a successful execution, is deliberate: a query
// filtered on a column that exists but is never populated the way the author
// assumed parses fine, runs fine, and is useless. The demo fixture is built to
// satisfy every shipped query, so an empty result means either the query or the
// fixture is wrong — and both are worth stopping for.
func assertReturnsRows(ctx context.Context, t *testing.T, conn driver.Conn, stmt string) {
	t.Helper()

	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		t.Fatalf("execute failed: %v\n\n%s", err, stmt)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close rows: %v", err)
		}
	}()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read rows: %v\n\n%s", err, stmt)
	}
	if count == 0 {
		t.Errorf("query returned no rows against the demo fixture:\n\n%s", stmt)
	}
}

func connectToTestDatabase(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()

	addr := envOrDefault("CH_TEST_ADDR", "127.0.0.1:9000")
	username := envOrDefault("CH_TEST_USER", "default")
	password := os.Getenv("CH_TEST_PASSWORD")

	options := func(database string) *chdriver.Options {
		return &chdriver.Options{
			Addr:        []string{addr},
			Auth:        chdriver.Auth{Database: database, Username: username, Password: password},
			Protocol:    chdriver.Native,
			DialTimeout: 5 * time.Second,
			ReadTimeout: 30 * time.Second,
		}
	}

	// The private database has to be created through a connection to an existing
	// one, so this opens twice: once to create it, once to work in it.
	bootstrap, err := chdriver.Open(options(envOrDefault("CH_TEST_DB", "default")))
	if err != nil {
		t.Fatalf("open bootstrap connection: %v", err)
	}
	if err := bootstrap.Exec(ctx, "DROP DATABASE IF EXISTS "+testDatabase); err != nil {
		t.Fatalf("drop stale test database: %v", err)
	}
	if err := bootstrap.Exec(ctx, "CREATE DATABASE "+testDatabase); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("close bootstrap connection: %v", err)
	}

	conn, err := chdriver.Open(options(testDatabase))
	if err != nil {
		t.Fatalf("open connection to %s: %v", testDatabase, err)
	}
	t.Cleanup(func() {
		// Best effort on the way out; the dockerized target is discarded anyway,
		// and a persistent one is left clean for the next run.
		if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+testDatabase); err != nil {
			t.Logf("dropping %s: %v", testDatabase, err)
		}
		if err := conn.Close(); err != nil {
			t.Logf("closing connection: %v", err)
		}
	})
	return conn
}

// applyShippedDDL creates the tables from deploy/clickhouse/schema, and only from
// there. It repeats the sink's own auto-create loop rather than calling it
// because that function is unexported in internal/sink/clickhouse; what matters
// for this test is the source of the DDL, which is the same embedded filesystem.
func applyShippedDDL(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()

	entries, err := schemaddl.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded schema: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("the embedded schema holds no DDL")
	}

	for _, name := range names {
		ddl, err := schemaddl.FS.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		stmt := strings.TrimRight(strings.TrimSpace(string(ddl)), ";")
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
}

// assertOnlyFrozenColumns is what turns a successful query execution into a
// statement about the frozen schema.
//
// Without it, "the query ran" would only mean the columns exist in whatever table
// happens to be there. With it, the live tables are known to carry the frozen
// columns and nothing else, so any column a query names is by construction a
// frozen one.
func assertOnlyFrozenColumns(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()

	frozen, err := FrozenColumns()
	if err != nil {
		t.Fatalf("FrozenColumns: %v", err)
	}

	for table, want := range frozen {
		rows, err := conn.Query(ctx,
			"SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position",
			testDatabase, table)
		if err != nil {
			t.Fatalf("introspect %s: %v", table, err)
		}
		var live []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan column name: %v", err)
			}
			live = append(live, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read columns of %s: %v", table, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close columns of %s: %v", table, err)
		}

		if strings.Join(live, ",") != strings.Join(want, ",") {
			t.Fatalf("%s in the test database is not the frozen schema\n live: %v\nwant: %v",
				table, live, want)
		}
	}
}

// demoFixture is what was seeded, in the form the queries need it: the parameter
// values that make a docs/QUERIES.md recipe select the fixture's own rows.
type demoFixture struct {
	params map[string]string
}

// seedDemoData writes a small cluster's worth of history: one object that
// flaps, one that a human edited outside GitOps, one that was deleted, a
// cluster-scoped object, and the Kubernetes Events for the flapper.
//
// It is deliberately hand-written rather than driven through the pipeline. The
// pipeline's own output is already asserted by the sink's integration tests; what
// this suite needs is a fixture whose shape it fully controls, so that a query
// returning nothing is unambiguously the query's fault.
func seedDemoData(ctx context.Context, t *testing.T, conn driver.Conn) demoFixture {
	t.Helper()

	const (
		apiUID     = "11111111-1111-1111-1111-111111111111"
		legacyUID  = "22222222-2222-2222-2222-222222222222"
		gitopsMgr  = "argocd-controller"
		humanMgr   = "kubectl-edit"
		statusMgr  = "kube-controller-manager"
		fullState  = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api"},"spec":{"replicas":2}}`
		patchState = `[{"op":"replace","path":"/spec/replicas","value":3}]`
	)

	// Anchored well inside the 30-day window the macros expand to, and ordered so
	// the flapper's Modified rows are spread over several interval buckets.
	base := time.Now().UTC().Add(-2 * time.Hour)
	at := func(minutes int) time.Time { return base.Add(time.Duration(minutes) * time.Minute) }

	type row struct {
		ts        time.Time
		eventType string
		apiGroup  string
		version   string
		kind      string
		namespace string
		name      string
		uid       string
		rv        string
		labels    map[string]string
		actors    []string
		data      string
		diff      string
		sha       string
	}

	appLabels := map[string]string{"app": "api"}
	rows := []row{
		// The flapper: created by GitOps, then modified repeatedly — once by a
		// human, which is what makes it show up as non-GitOps drift too.
		{at(0), "Added", "apps", "v1", "Deployment", "demo", "api", apiUID, "100", appLabels,
			[]string{gitopsMgr}, fullState, "", "sha-api-0"},
		{at(5), "Modified", "apps", "v1", "Deployment", "demo", "api", apiUID, "110", appLabels,
			[]string{gitopsMgr}, "", patchState, "sha-api-1"},
		{at(9), "Modified", "apps", "v1", "Deployment", "demo", "api", apiUID, "120", appLabels,
			[]string{humanMgr}, "", patchState, "sha-api-2"},
		{at(14), "Modified", "apps", "v1", "Deployment", "demo", "api", apiUID, "130", appLabels,
			[]string{gitopsMgr, statusMgr}, "", patchState, "sha-api-3"},
		// A Checkpoint carries both full state and the diff it stood in for.
		{at(20), "Checkpoint", "apps", "v1", "Deployment", "demo", "api", apiUID, "140", appLabels,
			[]string{gitopsMgr}, fullState, patchState, "sha-api-4"},

		// A second kind in the same namespace, so the kind filters have something
		// to exclude.
		{at(2), "Added", "", "v1", "ConfigMap", "demo", "settings", "33333333-3333-3333-3333-333333333333",
			"200", nil, []string{humanMgr}, `{"apiVersion":"v1","kind":"ConfigMap","data":{"k":"v"}}`, "", "sha-cm-0"},
		{at(11), "Modified", "", "v1", "ConfigMap", "demo", "settings", "33333333-3333-3333-3333-333333333333",
			"210", nil, []string{humanMgr}, "", `[{"op":"replace","path":"/data/k","value":"v2"}]`, "sha-cm-1"},

		// A second namespace, so the namespace heatmap and the busiest-namespaces
		// table have more than one row.
		{at(3), "Added", "apps", "v1", "Deployment", "kube-system", "coredns",
			"44444444-4444-4444-4444-444444444444", "300", nil, []string{statusMgr},
			`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"coredns"}}`, "", "sha-dns-0"},
		{at(12), "Modified", "apps", "v1", "Deployment", "kube-system", "coredns",
			"44444444-4444-4444-4444-444444444444", "310", nil, []string{statusMgr}, "",
			`[{"op":"replace","path":"/spec/replicas","value":2}]`, "sha-dns-1"},

		// Written while the operator was still warming: the event type that exists
		// precisely so an unwarm write is not mistaken for a creation.
		{at(1), "Snapshot", "apps", "v1", "Deployment", "demo", "worker",
			"55555555-5555-5555-5555-555555555555", "400", nil, []string{gitopsMgr},
			`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"worker"}}`, "", "sha-wrk-0"},

		// An object that existed and then did not: the subject of the
		// "what did this deleted object look like" recipe. Deleted rows carry no
		// data, no diff, no sha256 and no actors, exactly as the schema specifies.
		{at(4), "Added", "apps", "v1", "Deployment", "demo", "legacy", legacyUID, "500", nil,
			[]string{gitopsMgr},
			`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"legacy"},"spec":{"replicas":1}}`,
			"", "sha-leg-0"},
		{at(18), "Deleted", "apps", "v1", "Deployment", "demo", "legacy", legacyUID, "510", nil,
			nil, "", "", ""},

		// A cluster-scoped object, so the '' namespace has a representative and the
		// heatmap's (cluster-scoped) bucket is populated.
		{at(6), "Added", "rbac.authorization.k8s.io", "v1", "ClusterRole", "", "viewer",
			"66666666-6666-6666-6666-666666666666", "600", nil, []string{gitopsMgr},
			`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"viewer"}}`,
			"", "sha-cr-0"},

		// Kubernetes Events for the flapper, in both API spellings, so the
		// coalesce over involvedObject/regarding is exercised on both sides.
		{at(7), "Added", "", "v1", "Event", "demo", "api.17c0", "77777777-7777-7777-7777-777777777777",
			"700", nil, []string{"kubelet"},
			`{"apiVersion":"v1","kind":"Event","type":"Warning","reason":"BackOff",` +
				`"message":"Back-off restarting failed container","count":3,` +
				`"involvedObject":{"kind":"Deployment","namespace":"demo","name":"api","uid":"` + apiUID + `"}}`,
			"", "sha-ev-0"},
		{at(10), "Modified", "", "v1", "Event", "demo", "api.17c0", "77777777-7777-7777-7777-777777777777",
			"710", nil, []string{"kubelet"},
			`{"apiVersion":"v1","kind":"Event","type":"Warning","reason":"BackOff",` +
				`"message":"Back-off restarting failed container","count":7,` +
				`"involvedObject":{"kind":"Deployment","namespace":"demo","name":"api","uid":"` + apiUID + `"}}`,
			"", "sha-ev-1"},
		{at(13), "Added", "events.k8s.io", "v1", "Event", "demo", "api.17c1",
			"88888888-8888-8888-8888-888888888888", "720", nil, []string{"deployment-controller"},
			`{"apiVersion":"events.k8s.io/v1","kind":"Event","type":"Normal","reason":"ScalingReplicaSet",` +
				`"message":"Scaled up replica set api-abc to 3","deprecatedCount":1,` +
				`"regarding":{"kind":"Deployment","namespace":"demo","name":"api","uid":"` + apiUID + `"}}`,
			"", "sha-ev-2"},
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO resource_states (ts, cluster_id, event_type, api_group, "+
		"api_version, kind, namespace, name, uid, resource_version, labels, actors, data, diff, sha256)")
	if err != nil {
		t.Fatalf("prepare resource_states batch: %v", err)
	}
	for _, r := range rows {
		labels := r.labels
		if labels == nil {
			labels = map[string]string{}
		}
		actors := r.actors
		if actors == nil {
			actors = []string{}
		}
		if err := batch.Append(r.ts, demoCluster, r.eventType, r.apiGroup, r.version, r.kind,
			r.namespace, r.name, r.uid, r.rv, labels, actors, r.data, r.diff, r.sha); err != nil {
			t.Fatalf("append %s/%s: %v", r.namespace, r.name, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send resource_states batch: %v", err)
	}

	// watch_scopes is created by the shipped DDL and checked for frozen columns,
	// but deliberately left empty: no query on this page reads it, and a fixture
	// nothing asserts against is a fixture that rots.

	// The parameter values that point a docs/QUERIES.md recipe at the fixture
	// above. Every parameter any published recipe declares must appear here; the
	// library sub-test fails loudly if one does not, rather than binding a default
	// that would quietly select nothing.
	windowStart := base.Add(-30 * time.Minute).Format("2006-01-02 15:04:05.000")
	windowEnd := base.Add(90 * time.Minute).Format("2006-01-02 15:04:05.000")
	return demoFixture{params: map[string]string{
		"cluster":   demoCluster,
		"group":     "apps",
		"kind":      "Deployment",
		"namespace": "demo",
		"name":      "api",
		"uid":       apiUID,
		"manager":   gitopsMgr,
		"threshold": "3",
		"from":      windowStart,
		"to":        windowEnd,
		"at":        windowEnd,
		"t":         base.Add(10 * time.Minute).Format("2006-01-02 15:04:05.000"),
	}}
}

// shortSource trims a source label down to something readable as a subtest name.
func shortSource(source string) string {
	trimmed := strings.TrimPrefix(source, repoPath()+"/")
	return strings.ReplaceAll(trimmed, " ", "_")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
