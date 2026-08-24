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

// Package queries loads the SQL kuberecord publishes to its users — the recipes
// in docs/QUERIES.md and the queries embedded in the Grafana dashboards under
// deploy/grafana — and prepares it to be executed against a real ClickHouse.
//
// It exists because published SQL is the one part of this project that nothing
// compiles. A column renamed in the DDL, a macro spelled wrong, a panel
// referencing a dashboard variable that was later removed: all of them ship
// silently and surface as an empty panel or a copy-pasted query that errors in
// front of the user who trusted it. Task 3.2's acceptance criterion answers that
// by running every shipped query against the integration ClickHouse, and this
// package is the part of that check which can be unit-tested without a database —
// extraction, macro expansion, and variable interpolation.
//
// The execution half lives in clickhouse_integration_test.go (build tag
// `integration`), which builds its tables from deploy/clickhouse/schema alone, so
// "the query ran" is exactly "the query touched only frozen-schema columns".
//
// Since Task 7.2 the library publishes recipes for two backends, and a fenced
// block therefore declares which engine it is written for and what may be done
// with it. The marker is the second word of the fence's info string — GitHub
// derives its syntax highlighting from the first word alone, so ```sql duckdb
// still renders as SQL while being unambiguous to this package:
//
//	```sql                    a ClickHouse recipe: executed against ClickHouse
//	```sql duckdb             a DuckDB recipe: executed against the S3 archive
//	```sql duckdb-parameters  the reader-edited variable block; parsed, never run
//	```sql duckdb-setup       the session preamble; executed before every recipe
//	```sql athena             DDL validated by structure only — CI has no AWS
//
// The split is load bearing rather than tidy. A DuckDB recipe handed to
// ClickHouse fails on read_json_auto and tells a maintainer nothing, and the
// Athena DDL's `${cluster_id}` projection template is indistinguishable from a
// Grafana variable reference, so a single undifferentiated pool of "sql" would
// have made both suites lie.
package queries

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	schemaddl "github.com/yelzhy/kuberecord/deploy/clickhouse/schema"
)

// Query is one published SQL statement together with a human-readable statement
// of where it came from. Source is carried rather than derived because a failure
// message that says "docs/QUERIES.md:214" sends a maintainer to the right place,
// while one that says "query 7 of 23" does not.
type Query struct {
	// Source names the file and position the SQL was read from.
	Source string
	// SQL is the statement exactly as published, macros and variables unexpanded.
	SQL string
	// Dialect is the engine the block declared itself for. It is never empty: an
	// unmarked ```sql fence is DialectClickHouse, which is what every fence in the
	// library meant before a second backend existed.
	Dialect Dialect
}

// Dialect is the engine a published block is written for, and with it what this
// package may do with the block. See the package comment for the fence markers
// that select each one.
type Dialect string

const (
	// DialectClickHouse is an unmarked ```sql fence: a recipe for the ClickHouse
	// backend, executed against a real ClickHouse built from the shipped DDL.
	DialectClickHouse Dialect = "clickhouse"
	// DialectDuckDB is a recipe for the S3 archive, executed through the duckdb
	// CLI against a real object store.
	DialectDuckDB Dialect = "duckdb"
	// DialectDuckDBParameters is the variable block a reader edits before running
	// anything. It is parsed for the names it declares and never executed: its
	// values name somebody else's bucket.
	DialectDuckDBParameters Dialect = "duckdb-parameters"
	// DialectDuckDBSetup is the session preamble — extension load, credentials,
	// and the globs derived from the parameters. It is executed verbatim ahead of
	// every DuckDB recipe, which is what makes the published setup a tested claim
	// rather than an illustration.
	DialectDuckDBSetup Dialect = "duckdb-setup"
	// DialectAthena is DDL for a query engine CI cannot reach. It is validated by
	// structure alone; see AthenaTable.
	DialectAthena Dialect = "athena"
)

// dialects is the closed set of markers a fence may carry. It is closed on
// purpose: a mistyped marker must fail loudly rather than land in a bucket
// nothing executes, which is the silent-skip failure mode this project has
// already been bitten by twice.
var dialects = map[string]Dialect{
	"":                  DialectClickHouse,
	"duckdb":            DialectDuckDB,
	"duckdb-parameters": DialectDuckDBParameters,
	"duckdb-setup":      DialectDuckDBSetup,
	"athena":            DialectAthena,
}

// Variable is one Grafana template variable declared by a dashboard.
//
// The declared set is what makes interpolation an assertion rather than a
// convenience: a panel may only reference variables its own dashboard declares,
// so a query naming ${namespcae} fails instead of quietly rendering as an empty
// filter that matches everything.
type Variable struct {
	Name string
	Type string
	// Multi reports whether Grafana lets the user select several values, which is
	// what decides whether a query should be interpolating it into an IN list.
	Multi bool
}

// Dashboard is the subset of a Grafana dashboard this package reads: its
// identity, the variables it declares, and every SQL statement inside it —
// panel targets and variable definitions alike, because a broken variable query
// leaves every panel that filters on it unusable.
type Dashboard struct {
	Path      string
	UID       string
	Title     string
	Variables []Variable
	Queries   []Query
}

// VariableNames returns the declared variable names, excluding the datasource
// picker — that one is resolved by Grafana itself and never appears in SQL.
func (d *Dashboard) VariableNames() []string {
	names := make([]string, 0, len(d.Variables))
	for _, v := range d.Variables {
		if v.Type == "datasource" {
			continue
		}
		names = append(names, v.Name)
	}
	sort.Strings(names)
	return names
}

// sqlFence matches a fenced ```sql block in Markdown, capturing the rest of the
// info string as the dialect marker. Only sql-tagged fences are taken, which is
// also how a document declares that a snippet is *not* meant to run: fence it as
// text or console and this package will not try to execute it.
//
// The marker group requires a leading space, so ```sqlite is not a `sql` fence
// with the marker "ite" — it is simply not one of ours.
var sqlFence = regexp.MustCompile("(?s)```sql([ \t][^\n]*)?\n(.*?)\n```")

// FromMarkdown extracts every SQL block from a Markdown document, each tagged
// with the dialect its fence declared.
//
// An unrecognised marker is an error rather than a skipped block: a document that
// publishes a recipe no suite runs is exactly the state this package exists to
// prevent.
func FromMarkdown(path string) ([]Query, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := string(raw)

	matches := sqlFence.FindAllStringSubmatchIndex(text, -1)
	out := make([]Query, 0, len(matches))
	for _, m := range matches {
		// The line number of the fence itself, so the Source reads like a location
		// an editor can jump to.
		line := 1 + strings.Count(text[:m[0]], "\n")
		source := fmt.Sprintf("%s:%d", path, line)

		marker := ""
		if m[2] >= 0 {
			marker = strings.TrimSpace(text[m[2]:m[3]])
		}
		dialect, ok := dialects[marker]
		if !ok {
			return nil, fmt.Errorf("%s: fence marker %q is not one this package knows; "+
				"see the dialect list in its package comment", source, marker)
		}

		out = append(out, Query{
			Source:  source,
			SQL:     text[m[4]:m[5]],
			Dialect: dialect,
		})
	}
	return out, nil
}

// ByDialect returns the blocks written for one engine, in document order.
//
// Callers filter rather than being handed a pre-split map because every suite
// wants to assert that its own slice is non-empty, and a map lookup that yields
// nothing is indistinguishable from a document that stopped publishing the thing
// the suite was built to check.
func ByDialect(queries []Query, dialect Dialect) []Query {
	out := make([]Query, 0, len(queries))
	for _, q := range queries {
		if q.Dialect == dialect {
			out = append(out, q)
		}
	}
	return out
}

// dashboardDoc is the decode target for a Grafana dashboard. It is deliberately
// partial: this package has an opinion about queries and variables and none at
// all about panel styling, and a struct that mirrored the whole model would need
// updating every time a field was added to a fieldConfig.
type dashboardDoc struct {
	UID        string `json:"uid"`
	Title      string `json:"title"`
	Templating struct {
		List []struct {
			Name  string          `json:"name"`
			Type  string          `json:"type"`
			Multi bool            `json:"multi"`
			Query json.RawMessage `json:"query"`
		} `json:"list"`
	} `json:"templating"`
	Panels []struct {
		ID      int    `json:"id"`
		Title   string `json:"title"`
		Targets []struct {
			RefID  string `json:"refId"`
			RawSQL string `json:"rawSql"`
		} `json:"targets"`
	} `json:"panels"`
}

// FromDashboard reads a Grafana dashboard and returns its declared variables and
// every SQL statement it carries.
//
// Dashboards that query Prometheus rather than ClickHouse simply yield no
// queries; the operator-health dashboard is such a case, and passing it here is
// harmless rather than an error.
func FromDashboard(path string) (*Dashboard, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc dashboardDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}

	dash := &Dashboard{Path: path, UID: doc.UID, Title: doc.Title}
	for _, v := range doc.Templating.List {
		dash.Variables = append(dash.Variables, Variable{Name: v.Name, Type: v.Type, Multi: v.Multi})

		// A datasource variable's query is a plain string naming a plugin; a query
		// variable's is an object whose rawSql is the statement. Anything else
		// carries no SQL and is skipped rather than rejected.
		var q struct {
			RawSQL string `json:"rawSql"`
		}
		if len(v.Query) == 0 || json.Unmarshal(v.Query, &q) != nil || q.RawSQL == "" {
			continue
		}
		dash.Queries = append(dash.Queries, Query{
			Source: fmt.Sprintf("%s variable %q", path, v.Name),
			SQL:    q.RawSQL,
		})
	}

	for _, p := range doc.Panels {
		for _, t := range p.Targets {
			if t.RawSQL == "" {
				continue
			}
			dash.Queries = append(dash.Queries, Query{
				Source: fmt.Sprintf("%s panel %d %q target %s", path, p.ID, p.Title, t.RefID),
				SQL:    t.RawSQL,
			})
		}
	}
	return dash, nil
}

// Values are the dashboard-variable values to interpolate with, one entry per
// variable name. A slice rather than a string because Grafana multi-value
// variables render as lists, and a test that only ever substituted one value
// would never exercise the IN clauses the dashboards are built on.
type Values map[string][]string

// demoVariableValues is the canonical value of every template variable the
// shipped dashboards declare, and the contract between the two halves of this
// package's testing: the unit tests interpolate with these, and the integration
// test seeds ClickHouse rows that match them, so a query that interpolates and
// runs also returns rows rather than silently proving nothing.
//
// Adding a variable to a dashboard without adding it here fails
// TestDemoValuesCoverEveryDashboardVariable, which is deliberate — a variable
// with no demo value is a variable the query test cannot exercise.
var demoVariableValues = map[string][]string{
	"cluster":        {"demo-cluster"},
	"kind":           {"Deployment", "ConfigMap"},
	"namespace":      {"demo", "kube-system"},
	"name":           {"api"},
	"event_type":     {"Added", "Modified", "Deleted", "Snapshot", "Checkpoint"},
	"gitops_manager": {"argocd-controller"},
	"threshold":      {"3"},
}

// DemoValues returns the values to interpolate one dashboard's queries with.
//
// Multi-select variables get the whole list so the IN clauses are exercised with
// more than one element; single-select ones get exactly one value, because a
// single-select variable is interpolated into an equality and a list there would
// not be a stricter test but a syntax error.
func DemoValues(d *Dashboard) (Values, error) {
	out := Values{}
	for _, v := range d.Variables {
		if v.Type == "datasource" {
			continue
		}
		values, ok := demoVariableValues[v.Name]
		if !ok {
			return nil, fmt.Errorf("%s declares variable %q, which has no demo value", d.Path, v.Name)
		}
		if !v.Multi {
			values = values[:1]
		}
		out[v.Name] = values
	}
	return out, nil
}

// Time-range substitutions for the Grafana macros. A window relative to now
// rather than a fixed date: the fixture rows an integration test writes are
// stamped at the moment it runs, and a hard-coded window would silently stop
// containing them.
const (
	macroFromTime = "(now64(9, 'UTC') - toIntervalDay(30))"
	macroToTime   = "now64(9, 'UTC')"
	// macroBucketSeconds stands in for the interval Grafana derives from the panel
	// width. A minute rather than something larger so a fixture spanning tens of
	// minutes lands in several buckets: a bucketed query that collapsed to one row
	// would still pass, but would stop demonstrating that it buckets at all.
	macroBucketSeconds = 60
)

var (
	macroTimeFilter   = regexp.MustCompile(`\$__timeFilter\(([^)]*)\)`)
	macroTimeInterval = regexp.MustCompile(`\$__timeInterval\(([^)]*)\)`)
	// variableRef matches ${name} and ${name:format}. Only the braced spelling is
	// supported, and the dashboards use only that: bare $name has no boundary and
	// so silently swallows a following identifier character.
	variableRef = regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-zA-Z_]+))?\}`)
)

// Interpolate expands the Grafana macros and template variables in a published
// query, turning it into SQL a database can be asked to run.
//
// It is deliberately strict in one place: a reference to a variable that is not
// in vals is an error, not an empty substitution. Grafana's own behaviour there
// is to render nothing, which turns `namespace = ` into a syntax error at best
// and `namespace IN ()` into a filter that matches nothing at worst — the exact
// failure this package exists to catch before a user meets it.
func Interpolate(sqlText string, vals Values) (string, error) {
	out := macroTimeFilter.ReplaceAllStringFunc(sqlText, func(m string) string {
		column := strings.TrimSpace(macroTimeFilter.FindStringSubmatch(m)[1])
		return fmt.Sprintf("(%s >= %s AND %s <= %s)", column, macroFromTime, column, macroToTime)
	})
	out = macroTimeInterval.ReplaceAllStringFunc(out, func(m string) string {
		column := strings.TrimSpace(macroTimeInterval.FindStringSubmatch(m)[1])
		return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d second)", column, macroBucketSeconds)
	})
	out = strings.ReplaceAll(out, "$__fromTime", macroFromTime)
	out = strings.ReplaceAll(out, "$__toTime", macroToTime)
	out = strings.ReplaceAll(out, "$__interval_s", fmt.Sprintf("%d", macroBucketSeconds))

	var firstErr error
	out = variableRef.ReplaceAllStringFunc(out, func(m string) string {
		parts := variableRef.FindStringSubmatch(m)
		name, format := parts[1], parts[2]
		values, ok := vals[name]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("query references ${%s}, which the dashboard does not declare", name)
			}
			return m
		}
		rendered, err := renderVariable(name, format, values)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return rendered
	})
	if firstErr != nil {
		return "", firstErr
	}

	if leftover := strings.Index(out, "$__"); leftover >= 0 {
		return "", fmt.Errorf("unexpanded Grafana macro near %q", excerpt(out, leftover))
	}
	return out, nil
}

// renderVariable applies one of Grafana's interpolation formats.
//
// Only the formats the shipped dashboards use are implemented, and an unknown
// one is an error: silently falling back to the raw value would make this
// helper's agreement with Grafana untestable, and the whole point of running
// these queries is that what the test executes is what Grafana sends.
func renderVariable(name, format string, values []string) (string, error) {
	switch format {
	case "sqlstring", "singlequote":
		quoted := make([]string, 0, len(values))
		for _, v := range values {
			quoted = append(quoted, "'"+strings.ReplaceAll(v, "'", "''")+"'")
		}
		return strings.Join(quoted, ","), nil
	case "csv":
		return strings.Join(values, ","), nil
	case "", "raw":
		// Unformatted, Grafana substitutes a single value verbatim. Multi-valued
		// variables render as a glob there, which no SQL dialect accepts, so a
		// dashboard that does it is making a mistake worth failing on.
		if len(values) != 1 {
			return "", fmt.Errorf("${%s} is interpolated unformatted but holds %d values; "+
				"use :sqlstring or :csv for a multi-value variable", name, len(values))
		}
		return values[0], nil
	default:
		return "", fmt.Errorf("${%s:%s} uses an interpolation format this helper does not implement", name, format)
	}
}

// chParam matches a ClickHouse-native query parameter, `{name:Type}`.
//
// The leading `$` is captured rather than excluded because Go's regexp has no
// lookbehind and a Grafana `${var:sqlstring}` is otherwise indistinguishable
// from a parameter; matches carrying the `$` are dropped by the caller.
var chParam = regexp.MustCompile(`(\$?)\{([a-zA-Z_][a-zA-Z0-9_]*):([a-zA-Z0-9_(),' ]+)\}`)

// Parameters returns the ClickHouse-native query parameters a statement declares,
// as a map of name to declared type. These are the `{cluster:String}` placeholders
// docs/QUERIES.md uses so its recipes are copy-pasteable into clickhouse-client
// with `--param_cluster=…` rather than needing hand-editing.
func Parameters(sqlText string) map[string]string {
	out := map[string]string{}
	for _, m := range chParam.FindAllStringSubmatch(sqlText, -1) {
		if m[1] == "$" {
			continue // a Grafana variable reference, not a ClickHouse parameter
		}
		out[m[2]] = m[3]
	}
	return out
}

// BindValues produces a value for every declared parameter: the caller's own
// value where it has one, and otherwise a benign default chosen from the declared
// type so that a query with a parameter nobody thought to supply still executes.
//
// An unrecognised type is an error rather than a guess. A parameter this helper
// cannot type is a parameter whose query would run with a value that means
// something other than what the author intended, and a query that runs on the
// wrong value proves nothing.
func BindValues(params, overrides map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(params))
	for name, chType := range params {
		if v, ok := overrides[name]; ok {
			out[name] = v
			continue
		}
		def, err := defaultForType(chType)
		if err != nil {
			return nil, fmt.Errorf("parameter {%s:%s}: %w", name, chType, err)
		}
		out[name] = def
	}
	return out, nil
}

func defaultForType(chType string) (string, error) {
	base := strings.TrimSpace(chType)
	if idx := strings.Index(base, "("); idx >= 0 {
		base = base[:idx]
	}
	switch base {
	case "String", "LowCardinality":
		return "", nil
	case "DateTime", "DateTime64", "Date":
		return "2026-01-01 00:00:00.000", nil
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64":
		return "0", nil
	case "Float32", "Float64":
		return "0", nil
	default:
		return "", fmt.Errorf("no default value is defined for this type")
	}
}

// createTable matches the head of one shipped DDL statement.
var createTable = regexp.MustCompile(`(?i)CREATE TABLE IF NOT EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)`)

// FrozenColumns reads the shipped DDL and returns the column names of each table,
// which is the definition of the frozen v1 schema (docs/SCHEMA.md).
//
// It parses deploy/clickhouse/schema rather than restating the columns, so the
// frozen set the tests assert against cannot drift from the frozen set the
// operator creates. Only names are extracted: types are the sink's concern and
// are already validated against a live table by internal/sink/clickhouse.
func FrozenColumns() (map[string][]string, error) {
	entries, err := schemaddl.FS.ReadDir(".")
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		ddl, err := schemaddl.FS.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		table, columns, err := parseCreateTable(string(ddl))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		out[table] = columns
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no DDL found in the embedded schema; the frozen-column check would pass vacuously")
	}
	return out, nil
}

// parseCreateTable pulls the table name and its column names out of one CREATE
// TABLE statement: everything between the opening parenthesis on its own line and
// the closing one, minus comments and blank lines.
func parseCreateTable(ddl string) (string, []string, error) {
	loc := createTable.FindStringSubmatchIndex(ddl)
	if loc == nil {
		return "", nil, fmt.Errorf("no CREATE TABLE IF NOT EXISTS statement")
	}
	table := ddl[loc[2]:loc[3]]
	// Take the column list from *after* the statement head, so a parenthesis in a
	// leading file comment cannot be mistaken for the start of it.
	_, columnList, found := strings.Cut(ddl[loc[1]:], "(")
	if !found {
		return "", nil, fmt.Errorf("no column list")
	}

	var columns []string
	for line := range strings.SplitSeq(columnList, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ")") {
			return table, columns, nil
		}
		if idx := strings.Index(trimmed, "--"); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
		}
		if trimmed == "" {
			continue
		}
		columns = append(columns, strings.TrimRight(strings.Fields(trimmed)[0], ","))
	}
	return "", nil, fmt.Errorf("column list is never closed")
}

// excerpt renders a short window around an offset for an error message.
func excerpt(s string, at int) string {
	end := min(at+40, len(s))
	return s[at:end]
}
