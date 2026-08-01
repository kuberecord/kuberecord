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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"

	schemaddl "github.com/yelzhy/kubestream/deploy/clickhouse/schema"
)

const (
	tableResourceStates = "resource_states"
	tableWatchScopes    = "watch_scopes"
)

// schemaColumn is one required column and the exact type system.columns must
// report for it. The type strings deliberately omit the CODEC clauses present
// in the DDL — codecs are a storage-level detail ClickHouse does not surface in
// system.columns.type, so validating against them would always spuriously fail.
type schemaColumn struct {
	name   string
	chType string
}

// requiredColumns is the sink's schema contract: every column the operator
// depends on, per table, with the type system.columns is expected to report.
// It is kept in lockstep with the shipped DDL (deploy/clickhouse/schema); a
// live table that drifts from this degrades its own ClickHouseSink (via the
// health probe's SchemaValid condition) rather than letting the operator write
// rows that would silently mismatch the frozen public schema.
//
// It is a *required* set, not an exhaustive one: a live table may carry columns
// absent from this map and still validate. That tolerance is a guarantee of the
// frozen schema (docs/SCHEMA.md, "Stability & Versioning"), not an accident of
// how validateSchema iterates — under the additive-only policy a table may be
// migrated ahead of the operator that writes to it, and an operator that
// degraded its sink over a column it has no opinion about would turn a
// deliberately safe migration order into an outage. The write path is explicit
// about its columns for the same reason (see insertResourceStateQuery), so an
// unknown column simply takes its DEFAULT.
var requiredColumns = map[string][]schemaColumn{
	tableResourceStates: {
		{"ts", "DateTime64(9, 'UTC')"},
		{"cluster_id", "LowCardinality(String)"},
		{"event_type", "LowCardinality(String)"},
		{"api_group", "LowCardinality(String)"},
		{"api_version", "LowCardinality(String)"},
		{"kind", "LowCardinality(String)"},
		{"namespace", "String"},
		{"name", "String"},
		{"uid", "String"},
		{"resource_version", "String"},
		{"labels", "Map(LowCardinality(String), String)"},
		{"actors", "Array(LowCardinality(String))"},
		{"data", "String"},
		{"diff", "String"},
		{"sha256", "String"},
	},
	tableWatchScopes: {
		{"ts", "DateTime64(9, 'UTC')"},
		{"cluster_id", "LowCardinality(String)"},
		{"api_group", "LowCardinality(String)"},
		{"api_version", "LowCardinality(String)"},
		{"kind", "LowCardinality(String)"},
		{"namespace", "String"},
		{"action", "LowCardinality(String)"},
		{"rule_ref", "String"},
	},
}

// schemaMismatchError is returned by validateSchema when the live schema is
// readable but does not match requiredColumns. It is distinct from a transient
// query error so the retry loop can stop (a mismatch will not fix itself) while
// still retrying a ClickHouse that is merely unreachable. Each discrepancy
// names the offending table/column so operators can pinpoint the drift.
type schemaMismatchError struct {
	discrepancies []string
}

func (e *schemaMismatchError) Error() string {
	return fmt.Sprintf("clickhouse schema mismatch: %s", strings.Join(e.discrepancies, "; "))
}

// introspectColumns reads system.columns for both operator tables in one
// round-trip and returns table -> (column -> reported type). A table that does
// not exist simply yields no rows and therefore no entry in the outer map.
func introspectColumns(ctx context.Context, conn driver.Conn, database string) (map[string]map[string]string, error) {
	rows, err := conn.Query(ctx, `
        SELECT table, name, type
        FROM system.columns
        WHERE database = ? AND table IN (?, ?)
    `, database, tableResourceStates, tableWatchScopes)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	observed := make(map[string]map[string]string)
	for rows.Next() {
		var table, name, chType string
		if err := rows.Scan(&table, &name, &chType); err != nil {
			return nil, err
		}
		if observed[table] == nil {
			observed[table] = make(map[string]string)
		}
		observed[table][name] = chType
	}
	// A mid-stream error surfaces here, not from Next(); treating a partial read
	// as a valid (short) schema would be exactly the silent corruption this
	// validation exists to prevent.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return observed, nil
}

// validateSchema verifies the live ClickHouse schema against requiredColumns.
// A nil return means every required column of both tables is present with the
// expected type. A *schemaMismatchError names each discrepancy. Any other
// error is transient (ClickHouse unreachable, query failed) and the caller
// should retry rather than degrade readiness.
func validateSchema(ctx context.Context, conn driver.Conn, database string) error {
	observed, err := introspectColumns(ctx, conn, database)
	if err != nil {
		return err
	}

	var discrepancies []string
	// Deterministic table order so log output and error messages are stable.
	for _, table := range []string{tableResourceStates, tableWatchScopes} {
		cols := observed[table]
		if cols == nil {
			discrepancies = append(discrepancies, fmt.Sprintf("table %q is missing", table))
			continue
		}
		for _, req := range requiredColumns[table] {
			got, ok := cols[req.name]
			if !ok {
				discrepancies = append(discrepancies,
					fmt.Sprintf("table %q is missing column %q (expected type %s)", table, req.name, req.chType))
				continue
			}
			if got != req.chType {
				discrepancies = append(discrepancies,
					fmt.Sprintf("table %q column %q has type %s, expected %s", table, req.name, got, req.chType))
			}
		}
	}

	if len(discrepancies) > 0 {
		return &schemaMismatchError{discrepancies: discrepancies}
	}
	return nil
}

// autoCreateSchema executes the shipped DDL files in filename order. Every
// statement is CREATE TABLE IF NOT EXISTS, so this is idempotent and safe to
// run on every start. Only invoked when --ch-auto-create-schema is set; the
// default remains "the operator never mutates ClickHouse DDL on its own".
func autoCreateSchema(ctx context.Context, conn driver.Conn) error {
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
	sort.Strings(names)

	for _, name := range names {
		ddl, err := schemaddl.FS.ReadFile(name)
		if err != nil {
			return err
		}
		// The native protocol executes a single statement per Exec; trimming a
		// trailing ';' avoids an empty second statement being parsed.
		stmt := strings.TrimRight(strings.TrimSpace(string(ddl)), ";")
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("executing %s: %w", name, err)
		}
	}
	return nil
}

// autoCreateSchemaWithRetry applies the shipped DDL, retrying a ClickHouse that
// is not reachable yet until ctx is cancelled.
//
// It runs from the instance's own Start goroutine (see CHWriter.Start), so a
// backend that is down at boot delays only that sink's first write, never the
// manager's startup and never any other sink.
//
// Validation is deliberately *not* done here. Since Task 1.10 each sink is a CR
// with its own health, and the schema verdict belongs on that CR: the sink
// runtime's probe loop calls Probe (which validates) and reports the result as
// the ClickHouseSink's SchemaValid condition. A second, process-wide validation
// pass would only be able to report itself through a log line and a readiness
// flag that would take every *other* sink down with it.
//
//nolint:logcheck
func autoCreateSchemaWithRetry(ctx context.Context, conn driver.Conn, log logr.Logger) {
	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = 30 * time.Second
	eb.MaxElapsedTime = 0 // retry forever — only ctx cancellation gives up

	err := backoff.Retry(func() error {
		if err := autoCreateSchema(ctx, conn); err != nil {
			log.Error(err, "⚠️ Failed to auto-create ClickHouse schema, retrying")
			return err
		}
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		return // ctx cancelled before the DDL could be applied
	}
	log.Info("🗄️ ClickHouse schema auto-create applied (idempotent)")
}
