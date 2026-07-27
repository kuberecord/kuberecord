//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
)

// This file is how the suite reads the sink: it runs SQL inside the ClickHouse
// pod with `kubectl exec` and decodes clickhouse-client's JSONEachRow output.
//
// Talking to the server through the pod rather than through the Go driver over
// a `kubectl port-forward` is a deliberate reliability trade. Every assertion
// here runs inside an Eventually, so a query is executed tens of times per
// scenario; a forwarded connection that drops mid-suite is the classic e2e
// flake, and there is no forwarded connection to drop this way. The cost is
// process-spawn latency per poll, which is irrelevant next to the timeouts the
// scenarios actually wait on.

// Event types written to the resource_states.event_type column. They are
// spelled here rather than imported because the pipeline writes them as string
// literals (internal/pipeline/process.go) — there is no exported constant to
// share, and inventing one purely for the tests would put the definition on the
// wrong side of the contract.
const (
	eventAdded    = "Added"
	eventModified = "Modified"
	eventDeleted  = "Deleted"
	// eventSnapshot is what a cache-miss is tagged with while its scope has not
	// finished warming from the sink's history. It is the deliberate,
	// safe-direction alternative to Added (see Pipeline.MarkScopeWarm), so a test
	// asking "was this object's creation recorded?" — rather than "was it
	// recorded under this exact tag?" — has to accept either.
	eventSnapshot = "Snapshot"
)

// API groups and kinds the scenarios watch. Named constants because the same
// literals appear in rule manifests, ClickHouse filters and assertions alike,
// and a typo in one of the three would otherwise read as "no rows yet" until
// the Eventually timed out.
const (
	groupCore       = ""
	groupApps       = "apps"
	groupNetworking = "networking.k8s.io"

	kindDeployment = "Deployment"
	kindNode       = "Node"
	kindIngress    = "Ingress"
)

// resourceRow is one row of the resource_states table.
//
// The JSON tags are the column names: clickhouse-client's JSONEachRow format
// emits one object per row keyed by column, so this struct is also the
// suite's assertion that the frozen schema still carries the columns the
// acceptance criteria talk about. ts is kept as the string ClickHouse renders
// rather than a time.Time because nothing here asserts on wall-clock values —
// only on ordering, which the query does.
type resourceRow struct {
	TS              string            `json:"ts"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"`
	APIGroup        string            `json:"api_group"`
	APIVersion      string            `json:"api_version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	Actors          []string          `json:"actors"`
	Data            string            `json:"data"`
	Diff            string            `json:"diff"`
	SHA256          string            `json:"sha256"`
}

// scopeRow is one row of the watch_scopes table — a scope epoch transition.
type scopeRow struct {
	TS         string `json:"ts"`
	ClusterID  string `json:"cluster_id"`
	APIGroup   string `json:"api_group"`
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Action     string `json:"action"`
	RuleRef    string `json:"rule_ref"`
}

// objectFilter narrows a resource_states query.
//
// Group, Kind and Namespace are *always* applied, including when empty: the
// empty group is the core group and the empty namespace is a cluster-scoped
// object, so treating either as "unset" would silently widen a query that meant
// to be exact. Name, UID and EventTypes are optional and skipped when empty,
// which is what lets one filter ask "every row for this object" and another
// "every Deleted row in this namespace".
//
// EventTypes is a set rather than a single value because some claims are about a
// class of rows rather than one tag — "the object's creation was recorded once"
// is true of an Added row and equally of the Snapshot row an un-warmed scope
// writes instead.
type objectFilter struct {
	Group      string
	Kind       string
	Namespace  string
	Name       string
	UID        string
	EventTypes []string
}

func (f objectFilter) where() string {
	clauses := []string{
		"cluster_id = " + chLiteral(clusterID),
		"api_group = " + chLiteral(f.Group),
		"kind = " + chLiteral(f.Kind),
		"namespace = " + chLiteral(f.Namespace),
	}
	if f.Name != "" {
		clauses = append(clauses, "name = "+chLiteral(f.Name))
	}
	if f.UID != "" {
		clauses = append(clauses, "uid = "+chLiteral(f.UID))
	}
	if len(f.EventTypes) > 0 {
		quoted := make([]string, 0, len(f.EventTypes))
		for _, eventType := range f.EventTypes {
			quoted = append(quoted, chLiteral(eventType))
		}
		clauses = append(clauses, "event_type IN ("+strings.Join(quoted, ", ")+")")
	}
	return strings.Join(clauses, " AND ")
}

// scopeQuery narrows a watch_scopes query. Every field follows the same
// always-applied / optional split as objectFilter.
type scopeQuery struct {
	Group     string
	Kind      string
	Namespace string
	Action    string
	RuleRef   string
}

func (q scopeQuery) where() string {
	clauses := []string{
		"cluster_id = " + chLiteral(clusterID),
		"api_group = " + chLiteral(q.Group),
		"kind = " + chLiteral(q.Kind),
		"namespace = " + chLiteral(q.Namespace),
	}
	if q.Action != "" {
		clauses = append(clauses, "action = "+chLiteral(q.Action))
	}
	if q.RuleRef != "" {
		clauses = append(clauses, "rule_ref = "+chLiteral(q.RuleRef))
	}
	return strings.Join(clauses, " AND ")
}

// resourceRows returns every matching resource_states row, oldest first.
//
// FINAL is not optional here. resource_states is a ReplacingMergeTree, and the
// operator's write path is at-least-once: a lost acknowledgement re-inserts a
// byte-identical row that only collapses on merge. Counting without FINAL would
// therefore make "exactly one Deleted row" a race against a background merge —
// see docs/SCHEMA.md, "Delivery semantics".
func resourceRows(f objectFilter) ([]resourceRow, error) {
	return chSelect[resourceRow](
		"SELECT * FROM resource_states FINAL WHERE " + f.where() + " ORDER BY ts ASC")
}

// scopeRows returns every matching watch_scopes row, oldest first. watch_scopes
// is a plain MergeTree, so no FINAL: each transition is written once.
func scopeRows(q scopeQuery) ([]scopeRow, error) {
	return chSelect[scopeRow](
		"SELECT * FROM watch_scopes WHERE " + q.where() + " ORDER BY ts ASC")
}

// chSelect runs one query and decodes its JSONEachRow output into T.
func chSelect[T any](query string) ([]T, error) {
	out, err := kubectl("exec", "-n", clickHouseNamespace, "deploy/"+clickHouseDeployment, "--",
		"clickhouse-client",
		"--user", clickHouseUser,
		"--password", chPassword,
		"--database", clickHouseDatabase,
		"--format", "JSONEachRow",
		"--query", query)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", query, err)
	}

	var rows []T
	for line := range strings.SplitSeq(out, "\n") {
		// kubectl may interleave its own notices (container defaulting, for
		// instance) with the command's stdout, and utils.Run merges stderr in.
		// Only JSON object lines are result rows.
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var row T
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("decode row %q from query %q: %w", line, query, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// chLiteral renders s as a ClickHouse string literal.
//
// Every value that reaches it is an identifier this suite itself chose (a
// namespace, an object name, a UID the API server minted), so this is quoting
// for correctness — a name containing a quote would produce a syntax error and
// a confusing test failure — rather than a defence against injection from an
// untrusted source.
func chLiteral(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
