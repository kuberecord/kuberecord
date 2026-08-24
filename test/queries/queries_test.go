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
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// repoPath resolves a path relative to this source file rather than to the
// working directory, so the suite behaves the same under `go test ./...` from the
// repository root and under an editor running one test from this directory.
func repoPath(elems ...string) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate the test source file")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(append([]string{root}, elems...)...)
}

// shortSource trims a source label down to something readable as a subtest name.
func shortSource(source string) string {
	trimmed := strings.TrimPrefix(source, repoPath()+"/")
	return strings.ReplaceAll(trimmed, " ", "_")
}

// queryLibraries are the Markdown documents that publish runnable SQL, and
// productDashboards are the four ClickHouse-reading dashboards Task 3.2 ships.
// operator-health.json is deliberately absent: it queries Prometheus and carries
// no SQL.
//
// The README is in the list because its "first five queries" are the first SQL
// anyone runs — copy-pasted before they have read the library, the schema or
// anything else — so they are exactly the statements that must not rot. Every
// check the library gets, they get.
var (
	queryLibraries = []string{
		repoPath("docs", "QUERIES.md"),
		repoPath("README.md"),
	}
	productDashboards = []string{
		repoPath("deploy", "grafana", "object-timeline.json"),
		repoPath("deploy", "grafana", "drift-by-actor.json"),
		repoPath("deploy", "grafana", "flap-report.json"),
		repoPath("deploy", "grafana", "namespace-activity.json"),
	}
)

func TestFromMarkdown(t *testing.T) {
	doc := "# Title\n\nProse.\n\n```sql\nSELECT 1;\n```\n\n" +
		"More prose.\n\n```console\n$ not sql\n```\n\n```sql\nSELECT 2\nFROM t;\n```\n"
	path := filepath.Join(t.TempDir(), "doc.md")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	got, err := FromMarkdown(path)
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	want := []string{"SELECT 1;", "SELECT 2\nFROM t;"}
	if len(got) != len(want) {
		t.Fatalf("extracted %d blocks, want %d: %+v", len(got), len(want), got)
	}
	for i, q := range got {
		if q.SQL != want[i] {
			t.Errorf("block %d is %q, want %q", i, q.SQL, want[i])
		}
		if !strings.HasPrefix(q.Source, path+":") {
			t.Errorf("block %d source is %q, want a %s:<line> location", i, q.Source, path)
		}
	}
	// The console-fenced block must not have been taken: that fence is how a
	// document says "this snippet is illustrative, do not run it".
	for _, q := range got {
		if strings.Contains(q.SQL, "not sql") {
			t.Errorf("a non-sql fence was extracted: %q", q.SQL)
		}
	}
}

func TestFromDashboard(t *testing.T) {
	doc := `{
	  "uid": "u", "title": "T",
	  "templating": {"list": [
	    {"type": "datasource", "name": "datasource", "query": "grafana-clickhouse-datasource"},
	    {"type": "query", "name": "cluster", "query": {"rawSql": "SELECT DISTINCT cluster_id FROM resource_states"}},
	    {"type": "textbox", "name": "threshold", "query": "10"}
	  ]},
	  "panels": [
	    {"id": 1, "title": "P", "targets": [
	      {"refId": "A", "rawSql": "SELECT 1"},
	      {"refId": "B", "expr": "up"}
	    ]}
	  ]
	}`
	path := filepath.Join(t.TempDir(), "dash.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	dash, err := FromDashboard(path)
	if err != nil {
		t.Fatalf("FromDashboard: %v", err)
	}
	if dash.UID != "u" || dash.Title != "T" {
		t.Errorf("identity is %q/%q, want u/T", dash.UID, dash.Title)
	}
	if got, want := dash.VariableNames(), []string{"cluster", "threshold"}; !slices.Equal(got, want) {
		t.Errorf("VariableNames() = %v, want %v (the datasource picker is not a SQL variable)", got, want)
	}
	// Two statements: the variable's, and the one panel target that carries SQL.
	// The textbox variable's string query and the PromQL target contribute none.
	if len(dash.Queries) != 2 {
		t.Fatalf("extracted %d queries, want 2: %+v", len(dash.Queries), dash.Queries)
	}
	if !strings.Contains(dash.Queries[0].Source, `variable "cluster"`) {
		t.Errorf("first query source is %q, want it to name the cluster variable", dash.Queries[0].Source)
	}
	if dash.Queries[1].SQL != "SELECT 1" {
		t.Errorf("panel query is %q, want %q", dash.Queries[1].SQL, "SELECT 1")
	}
}

func TestInterpolate(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		vals    Values
		want    string
		wantErr string
	}{
		{
			name: "time filter expands around the named column",
			sql:  "WHERE $__timeFilter(ts)",
			want: "WHERE (ts >= " + macroFromTime + " AND ts <= " + macroToTime + ")",
		},
		{
			name: "time interval buckets the named column",
			sql:  "SELECT $__timeInterval(ts) AS time",
			want: "SELECT toStartOfInterval(ts, INTERVAL 60 second) AS time",
		},
		{
			name: "bare from and to macros",
			sql:  "WHERE ts BETWEEN $__fromTime AND $__toTime",
			want: "WHERE ts BETWEEN " + macroFromTime + " AND " + macroToTime,
		},
		{
			name: "interval seconds",
			sql:  "INTERVAL $__interval_s second",
			want: "INTERVAL 60 second",
		},
		{
			name: "single-valued sqlstring is quoted",
			sql:  "cluster_id = ${cluster:sqlstring}",
			vals: Values{"cluster": {"prod"}},
			want: "cluster_id = 'prod'",
		},
		{
			name: "multi-valued sqlstring becomes an IN list",
			sql:  "namespace IN (${namespace:sqlstring})",
			vals: Values{"namespace": {"demo", "kube-system"}},
			want: "namespace IN ('demo','kube-system')",
		},
		{
			name: "embedded quotes are doubled, not dropped",
			sql:  "name = ${name:sqlstring}",
			vals: Values{"name": {"it's"}},
			want: "name = 'it''s'",
		},
		{
			name: "csv renders unquoted",
			sql:  "-- ${kind:csv}",
			vals: Values{"kind": {"a", "b"}},
			want: "-- a,b",
		},
		{
			name: "unformatted single value substitutes verbatim",
			sql:  "HAVING count() >= ${threshold}",
			vals: Values{"threshold": {"10"}},
			want: "HAVING count() >= 10",
		},
		{
			name:    "a variable the dashboard does not declare is an error",
			sql:     "namespace = ${namespcae:sqlstring}",
			vals:    Values{"namespace": {"demo"}},
			wantErr: "does not declare",
		},
		{
			name:    "an unformatted multi-value variable is an error",
			sql:     "namespace = ${namespace}",
			vals:    Values{"namespace": {"a", "b"}},
			wantErr: "use :sqlstring or :csv",
		},
		{
			name:    "an unimplemented format is an error, not a silent passthrough",
			sql:     "namespace = ${namespace:json}",
			vals:    Values{"namespace": {"demo"}},
			wantErr: "does not implement",
		},
		{
			name:    "an unknown macro is left behind and reported",
			sql:     "WHERE $__unknownMacro(ts)",
			wantErr: "unexpanded Grafana macro",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Interpolate(tt.sql, tt.vals)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Interpolate(%q) succeeded with %q, want an error containing %q", tt.sql, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error is %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Interpolate(%q): %v", tt.sql, err)
			}
			if got != tt.want {
				t.Errorf("Interpolate(%q)\n got: %s\nwant: %s", tt.sql, got, tt.want)
			}
		})
	}
}

func TestParameters(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]string
	}{
		{
			name: "plain parameters",
			sql:  "WHERE cluster_id = {cluster:String} AND ts <= {at:DateTime64(9, 'UTC')}",
			want: map[string]string{"cluster": "String", "at": "DateTime64(9, 'UTC')"},
		},
		{
			name: "a Grafana variable is not a ClickHouse parameter",
			sql:  "WHERE cluster_id = ${cluster:sqlstring}",
			want: map[string]string{},
		},
		{
			name: "both spellings in one statement",
			sql:  "WHERE a = ${x:sqlstring} AND b = {y:String}",
			want: map[string]string{"y": "String"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Parameters(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("Parameters(%q) = %v, want %v", tt.sql, got, tt.want)
			}
			for name, chType := range tt.want {
				if got[name] != chType {
					t.Errorf("parameter %q typed %q, want %q", name, got[name], chType)
				}
			}
		})
	}
}

func TestBindValues(t *testing.T) {
	params := map[string]string{
		"cluster": "String",
		"at":      "DateTime64(9, 'UTC')",
		"n":       "UInt32",
	}

	got, err := BindValues(params, map[string]string{"cluster": "demo-cluster"})
	if err != nil {
		t.Fatalf("BindValues: %v", err)
	}
	if got["cluster"] != "demo-cluster" {
		t.Errorf("override was not honoured: cluster = %q", got["cluster"])
	}
	if got["at"] == "" {
		t.Error("a DateTime64 parameter with no override got no default value")
	}
	if got["n"] != "0" {
		t.Errorf("UInt32 default is %q, want %q", got["n"], "0")
	}

	if _, err := BindValues(map[string]string{"weird": "Tuple(String, UInt8)"}, nil); err == nil {
		t.Error("a parameter type with no defined default was accepted; it must be an error, not a guess")
	}
}

// TestFrozenColumnsMatchTheShippedDDL pins the parse against the frozen schema
// itself. The column lists are written out rather than counted so that a column
// added to the DDL — which the additive-only policy permits — is a deliberate
// edit here too, and so that a parser that silently dropped one would fail.
func TestFrozenColumnsMatchTheShippedDDL(t *testing.T) {
	got, err := FrozenColumns()
	if err != nil {
		t.Fatalf("FrozenColumns: %v", err)
	}

	want := map[string][]string{
		"resource_states": {
			"ts", "cluster_id", "event_type", "api_group", "api_version", "kind",
			"namespace", "name", "uid", "resource_version", "labels", "actors",
			"data", "diff", "sha256",
		},
		"watch_scopes": {
			"ts", "cluster_id", "api_group", "api_version", "kind", "namespace",
			"action", "rule_ref",
		},
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d tables, want %d: %v", len(got), len(want), got)
	}
	for table, columns := range want {
		if !slices.Equal(got[table], columns) {
			t.Errorf("%s columns\n got: %v\nwant: %v", table, got[table], columns)
		}
	}
}

// TestDemoValuesCoverEveryDashboardVariable is the guard that keeps the demo
// fixture and the dashboards in step. A new variable with no demo value would
// otherwise make the query test skip whatever it gates.
func TestDemoValuesCoverEveryDashboardVariable(t *testing.T) {
	for _, path := range productDashboards {
		t.Run(filepath.Base(path), func(t *testing.T) {
			dash, err := FromDashboard(path)
			if err != nil {
				t.Fatalf("FromDashboard: %v", err)
			}
			vals, err := DemoValues(dash)
			if err != nil {
				t.Fatalf("DemoValues: %v", err)
			}
			for _, name := range dash.VariableNames() {
				if len(vals[name]) == 0 {
					t.Errorf("variable %q has no demo value", name)
				}
			}
		})
	}
}

// TestShippedQueriesInterpolate is the half of the acceptance criterion that
// needs no database, and therefore runs in `make test` on every push: every
// query in the library and in every dashboard expands to something with no
// unresolved macro and no unresolved variable left in it.
//
// A dashboard query that references a variable its own dashboard does not declare
// fails here — the mistake that is invisible in Grafana, where the reference just
// renders as empty text.
func TestShippedQueriesInterpolate(t *testing.T) {
	for _, path := range queryLibraries {
		t.Run(filepath.Base(path), func(t *testing.T) {
			library, err := FromMarkdown(path)
			if err != nil {
				t.Fatalf("FromMarkdown: %v", err)
			}
			if len(ByDialect(library, DialectClickHouse)) == 0 {
				t.Fatal("this document holds no ClickHouse SQL blocks; the check would pass vacuously")
			}
			for _, q := range ByDialect(library, DialectClickHouse) {
				// The libraries use ClickHouse-native {name:Type} parameters rather
				// than Grafana variables, so interpolation is a no-op that must still
				// leave the statement untouched — and must not mistake a parameter for
				// a variable reference.
				got, err := Interpolate(q.SQL, nil)
				if err != nil {
					t.Errorf("%s: %v", q.Source, err)
					continue
				}
				if got != q.SQL {
					t.Errorf("%s: interpolation rewrote a parameterised library query:\n%s", q.Source, got)
				}
				if len(Parameters(q.SQL)) == 0 && strings.Contains(q.SQL, "{") {
					t.Errorf("%s: statement contains a brace but declares no parameters; check the spelling", q.Source)
				}
			}
		})
	}

	for _, path := range productDashboards {
		t.Run(filepath.Base(path), func(t *testing.T) {
			dash, err := FromDashboard(path)
			if err != nil {
				t.Fatalf("FromDashboard: %v", err)
			}
			if len(dash.Queries) == 0 {
				t.Fatal("dashboard carries no SQL; this check would pass vacuously")
			}
			vals, err := DemoValues(dash)
			if err != nil {
				t.Fatalf("DemoValues: %v", err)
			}
			for _, q := range dash.Queries {
				got, err := Interpolate(q.SQL, vals)
				if err != nil {
					t.Errorf("%s: %v", q.Source, err)
					continue
				}
				if strings.Contains(got, "${") || strings.Contains(got, "$__") {
					t.Errorf("%s: interpolation left an unresolved reference:\n%s", q.Source, got)
				}
			}
		})
	}
}
