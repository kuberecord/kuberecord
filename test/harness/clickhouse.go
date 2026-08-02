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

package harness

import (
	"encoding/json"
	"fmt"
	"strings"
)

// This file is how a suite reads the sink: it runs SQL inside the ClickHouse pod
// with `kubectl exec` and decodes clickhouse-client's JSONEachRow output.
//
// Talking to the server through the pod rather than through the Go driver over a
// `kubectl port-forward` is a deliberate reliability trade. Every assertion runs
// inside an Eventually, so a query is executed tens of times per scenario; a
// forwarded connection that drops mid-suite is the classic e2e flake, and there
// is no forwarded connection to drop this way. The cost is process-spawn latency
// per poll, which is irrelevant next to the timeouts the scenarios actually wait
// on. It matters more for the chaos suite, whose whole subject is a backend that
// keeps disappearing: a query issued while the server is down fails cleanly with
// a non-zero exit rather than leaving a wedged client behind.

// Event types written to the resource_states.event_type column. They are spelled
// here rather than imported because the pipeline writes them as string literals
// (internal/pipeline/process.go) — there is no exported constant to share, and
// inventing one purely for the tests would put the definition on the wrong side
// of the contract.
const (
	EventAdded    = "Added"
	EventModified = "Modified"
	EventDeleted  = "Deleted"
	// EventSnapshot is what a cache-miss is tagged with while its scope has not
	// finished warming from the sink's history. It is the deliberate,
	// safe-direction alternative to Added (see Pipeline.MarkScopeWarm), so a test
	// asking "was this object's creation recorded?" — rather than "was it recorded
	// under this exact tag?" — has to accept either.
	EventSnapshot = "Snapshot"
)

// CreationEvents are the two tags an object's first appearance can carry.
//
// Which one it gets depends on whether the scope had finished warming from the
// sink's history when the pipeline first saw the object — Added if it had,
// Snapshot if it had not (Pipeline.MarkScopeWarm). A test that only cares
// *whether* the appearance was recorded, or how many times, must therefore count
// both; asserting Added specifically is only sound where the scenario has an
// explicit barrier proving the scope was already warm.
var CreationEvents = []string{EventAdded, EventSnapshot}

// API groups and kinds the scenarios watch. Named constants because the same
// literals appear in rule manifests, ClickHouse filters and assertions alike, and
// a typo in one of the three would otherwise read as "no rows yet" until the
// Eventually timed out.
const (
	GroupCore       = ""
	GroupApps       = "apps"
	GroupNetworking = "networking.k8s.io"
	// GroupEvents is the modern Events API group. Both it and GroupCore serve
	// KindEvent off the same storage, which is why the Events scenarios run the
	// same assertions twice, once per spelling.
	GroupEvents = "events.k8s.io"

	KindDeployment = "Deployment"
	KindNode       = "Node"
	KindIngress    = "Ingress"
	KindConfigMap  = "ConfigMap"
	KindEvent      = "Event"
)

// ClickHouse addresses one suite's backend: which pod to exec into, which
// credentials to use, and which cluster_id every query filters on.
//
// cluster_id is part of the addressing rather than of each query because it is
// constant for a suite (one operator instance serves one cluster, Invariant 7)
// and omitting it from even one query would silently widen that query to rows
// another run wrote.
type ClickHouse struct {
	// Namespace and Deployment locate the server pod to exec into.
	Namespace  string
	Deployment string
	// User, Password and Database authenticate clickhouse-client.
	User     string
	Password string
	Database string
	// ClusterID is the value the operator under test stamps every row with.
	ClusterID string
}

// ResourceRow is one row of the resource_states table.
//
// The JSON tags are the column names: clickhouse-client's JSONEachRow format
// emits one object per row keyed by column, so this struct is also the suites'
// assertion that the frozen schema still carries the columns the acceptance
// criteria talk about. TS is kept as the string ClickHouse renders rather than a
// time.Time because nothing here asserts on wall-clock values — only on
// ordering, which the query does.
type ResourceRow struct {
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

// ScopeRow is one row of the watch_scopes table — a scope epoch transition.
type ScopeRow struct {
	TS         string `json:"ts"`
	ClusterID  string `json:"cluster_id"`
	APIGroup   string `json:"api_group"`
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Action     string `json:"action"`
	RuleRef    string `json:"rule_ref"`
}

// ObjectFilter narrows a resource_states query.
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
type ObjectFilter struct {
	Group      string
	Kind       string
	Namespace  string
	Name       string
	UID        string
	EventTypes []string
}

// WithEvent narrows a filter to the given event types. Filters are values, so
// this returns a copy and the caller's object filter stays reusable for the next
// question it asks.
func WithEvent(filter ObjectFilter, eventTypes ...string) ObjectFilter {
	filter.EventTypes = eventTypes
	return filter
}

func (f ObjectFilter) where(clusterID string) string {
	clauses := []string{
		"cluster_id = " + Literal(clusterID),
		"api_group = " + Literal(f.Group),
		"kind = " + Literal(f.Kind),
		"namespace = " + Literal(f.Namespace),
	}
	if f.Name != "" {
		clauses = append(clauses, "name = "+Literal(f.Name))
	}
	if f.UID != "" {
		clauses = append(clauses, "uid = "+Literal(f.UID))
	}
	if len(f.EventTypes) > 0 {
		quoted := make([]string, 0, len(f.EventTypes))
		for _, eventType := range f.EventTypes {
			quoted = append(quoted, Literal(eventType))
		}
		clauses = append(clauses, "event_type IN ("+strings.Join(quoted, ", ")+")")
	}
	return strings.Join(clauses, " AND ")
}

// ScopeQuery narrows a watch_scopes query. Every field follows the same
// always-applied / optional split as ObjectFilter.
type ScopeQuery struct {
	Group     string
	Kind      string
	Namespace string
	Action    string
	RuleRef   string
}

// WithAction narrows a scope query to one transition. Like WithEvent it copies,
// so one ScopeQuery value describes a scope and is then asked about both edges.
func WithAction(query ScopeQuery, action string) ScopeQuery {
	query.Action = action
	return query
}

func (q ScopeQuery) where(clusterID string) string {
	clauses := []string{
		"cluster_id = " + Literal(clusterID),
		"api_group = " + Literal(q.Group),
		"kind = " + Literal(q.Kind),
		"namespace = " + Literal(q.Namespace),
	}
	if q.Action != "" {
		clauses = append(clauses, "action = "+Literal(q.Action))
	}
	if q.RuleRef != "" {
		clauses = append(clauses, "rule_ref = "+Literal(q.RuleRef))
	}
	return strings.Join(clauses, " AND ")
}

// ResourceRows returns every matching resource_states row, oldest first.
//
// FINAL is not optional here. resource_states is a ReplacingMergeTree, and the
// operator's write path is at-least-once: a lost acknowledgement re-inserts a
// byte-identical row that only collapses on merge. Counting without FINAL would
// therefore make "exactly one Deleted row" a race against a background merge —
// see docs/SCHEMA.md, "Delivery semantics".
func (ch *ClickHouse) ResourceRows(f ObjectFilter) ([]ResourceRow, error) {
	return Select[ResourceRow](ch,
		"SELECT * FROM resource_states FINAL WHERE "+f.where(ch.ClusterID)+" ORDER BY ts ASC")
}

// ScopeRows returns every matching watch_scopes row, oldest first. watch_scopes
// is a plain MergeTree, so no FINAL: each transition is written once.
func (ch *ClickHouse) ScopeRows(q ScopeQuery) ([]ScopeRow, error) {
	return Select[ScopeRow](ch,
		"SELECT * FROM watch_scopes WHERE "+q.where(ch.ClusterID)+" ORDER BY ts ASC")
}

// DuplicateDelete is one object identity that carries more than one Deleted row —
// the shape of the standing invariant every chaos scenario ends on.
type DuplicateDelete struct {
	UID   string `json:"uid"`
	Name  string `json:"name"`
	Count string `json:"c"`
}

// DuplicateDeletes returns every UID with more than one Deleted row for this
// cluster. An empty result is the invariant Task 2.1 requires every scenario to
// hold: a disappearance is recorded once, whatever failed on the way there.
//
// It is deliberately unscoped by namespace or kind. The claim is about the whole
// audit trail, so a scenario that duplicated a deletion in someone else's
// namespace should still fail — and because the suites run Ordered and Serial,
// the only writer is the scenario under test plus the ones before it.
//
// The count comes back as a string because clickhouse-client renders UInt64 as
// one in JSONEachRow; nothing here needs it as a number, only as evidence in the
// failure message.
func (ch *ClickHouse) DuplicateDeletes() ([]DuplicateDelete, error) {
	return Select[DuplicateDelete](ch, `
		SELECT uid, any(name) AS name, toString(count()) AS c
		FROM resource_states FINAL
		WHERE cluster_id = `+Literal(ch.ClusterID)+` AND event_type = `+Literal(EventDeleted)+`
		GROUP BY uid HAVING count() > 1`)
}

// Exec runs a statement that returns no rows (DDL, for instance). It exists for
// the chaos suite's poison-row scenario, which installs a CHECK constraint to
// make one specific row un-insertable, and must remove it again afterwards.
func (ch *ClickHouse) Exec(query string) error {
	_, err := ch.run(query)
	return err
}

// Select runs one query and decodes its JSONEachRow output into T.
//
// It is a function rather than a method because Go does not allow type
// parameters on methods; the receiver is passed explicitly instead.
func Select[T any](ch *ClickHouse, query string) ([]T, error) {
	out, err := ch.run(query)
	if err != nil {
		return nil, err
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

// run executes one clickhouse-client invocation inside the server pod.
func (ch *ClickHouse) run(query string) (string, error) {
	out, err := Kubectl("exec", "-n", ch.Namespace, "deploy/"+ch.Deployment, "--",
		"clickhouse-client",
		"--user", ch.User,
		"--password", ch.Password,
		"--database", ch.Database,
		"--format", "JSONEachRow",
		"--query", query)
	if err != nil {
		return "", fmt.Errorf("query %q: %w", query, err)
	}
	return out, nil
}

// Literal renders s as a ClickHouse string literal.
//
// Every value that reaches it is an identifier a suite itself chose (a
// namespace, an object name, a UID the API server minted), so this is quoting
// for correctness — a name containing a quote would produce a syntax error and a
// confusing test failure — rather than a defence against injection from an
// untrusted source.
func Literal(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
