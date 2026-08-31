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

// The stand-in connection the unit tests and the conformance suite run against.
//
// Read the boundary note at the top of conformance_test.go before trusting
// anything this file proves: no fake executes SQL, so what is under test here is
// the Go half, and the SQL half is proven by the integration run.
//
// This stand-in does two things, and the split is what makes it worth having:
//
//  1. It *pins the statement*. Every statement is parsed back into a projection,
//     a table, a FINAL marker, a predicate list and a tail, and anything it does
//     not recognise is a named failure rather than a best guess. The forms it
//     recognises are spelled out here rather than imported from sql.go on
//     purpose: a stand-in that asked the production builder what the production
//     builder emits would agree with it by construction, and a pin that cannot
//     disagree pins nothing.
//
//  2. It *evaluates the intended semantics* over rows really seeded, using the
//     arguments really bound, in the order they were really bound. So which
//     filter value reaches which placeholder, how rows are scanned, and how a
//     mid-stream failure propagates are all under test for real.
//
// It also emulates the one server-side behaviour this backend's correctness rests
// on. Every seeded row is stored **twice**, byte-identically, exactly as an
// at-least-once write path leaves an unmerged duplicate in a ReplacingMergeTree —
// and the duplicate is collapsed only for a statement carrying FINAL. A read that
// lost FINAL therefore does not quietly keep passing here; it returns every row
// twice and fails the suite on the row count.

package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The projections this stand-in knows how to answer, spelled out independently of
// sql.go. See the file comment for why they are not imported.
const (
	fakeChangeColumns = "ts, event_type, api_version, uid, resource_version, labels, actors, data, diff, sha256"
	fakeReplayColumns = "ts, event_type, data, diff, sha256"
	fakeUIDColumn     = "uid"
	fakeSpanColumns   = "uid, min(ts) AS first_seen, max(ts) AS last_seen, " +
		"countIf(event_type = 'Deleted') AS deletions"
	fakeScopeColumns = "api_group, kind, namespace, action, rule_ref, ts"

	// fakeClusterIDColumn is the cluster-identity probe, which both tables answer
	// and which is the only read in this package with no WHERE clause at all.
	fakeClusterIDColumn = "DISTINCT cluster_id"
)

// fakeEventGroups is the both-spellings predicate, likewise spelled out here.
const fakeEventGroups = "api_group IN ('', 'events.k8s.io')"

// fakeSubjectMatch renders the Event subject predicate this stand-in will answer.
//
// Spelling it out is what makes the both-spellings requirement testable: a
// backend that dropped the `regarding` half — losing every Event captured through
// events.k8s.io/v1 — would emit a predicate this stand-in refuses by name, rather
// than one it happens to evaluate the same way.
func fakeSubjectMatch(field string) string {
	return "coalesce(nullIf(JSONExtractString(data, 'involvedObject', '" + field + "'), ''), " +
		"nullIf(JSONExtractString(data, 'regarding', '" + field + "'), '')) = ?"
}

// subjectFields are the four fields an Event's subject may be matched on.
var subjectFields = []string{"kind", "namespace", "name", "uid"}

// stateRow is one resource_states row, column for column.
type stateRow struct {
	ts              time.Time
	clusterID       string
	eventType       string
	apiGroup        string
	apiVersion      string
	kind            string
	namespace       string
	name            string
	uid             string
	resourceVersion string
	labels          map[string]string
	actors          []string
	data            string
	diff            string
	sha256          string
}

// sortKey is the ReplacingMergeTree ORDER BY tuple: what a duplicate collides on,
// and therefore what FINAL collapses.
func (r stateRow) sortKey() string {
	return strings.Join([]string{
		r.clusterID, r.apiGroup, r.kind, r.namespace, r.name,
		r.ts.UTC().Format(time.RFC3339Nano),
	}, "\x00")
}

// scopeRow is one watch_scopes row.
type scopeRow struct {
	ts        time.Time
	clusterID string
	apiGroup  string
	kind      string
	namespace string
	action    string
	ruleRef   string
}

// fakeStore is the storage behind the stand-in connection.
type fakeStore struct {
	mu     sync.Mutex
	states []stateRow
	scopes []scopeRow
	fault  *conformance.StreamFault
}

func newFakeStore() *fakeStore { return &fakeStore{} }

// seed makes a conformance History the store's recorded past.
//
// Every row lands twice. That is not a quirk of the fixture: the operator's write
// path re-inserts a byte-identical row after a lost acknowledgement, and until a
// merge runs the table really does hold both. Seeding the duplicate is what turns
// "this read carries FINAL" from a claim about text into a claim about the answer.
func (s *fakeStore) seed(h conformance.History) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = nil
	s.scopes = nil

	for _, r := range h.Rows {
		row := stateRow{
			ts:              r.Change.TS,
			clusterID:       r.Ref.ClusterID,
			eventType:       r.Change.EventType,
			apiGroup:        r.Ref.APIGroup,
			apiVersion:      r.Change.APIVersion,
			kind:            r.Ref.Kind,
			namespace:       r.Ref.Namespace,
			name:            r.Ref.Name,
			uid:             r.Change.UID,
			resourceVersion: r.Change.ResourceVersion,
			// The columns are Map and Array and are not nullable, so the server
			// hands back an empty container rather than a nil one. Emulating that
			// keeps a caller from learning a nil-ness this backend does not have.
			labels: emptyIfNil(r.Change.Labels),
			actors: sliceOrEmpty(r.Change.Actors),
			data:   r.Change.Data,
			diff:   r.Change.Diff,
			sha256: r.Change.SHA256,
		}
		s.states = append(s.states, row, row)
	}
	for _, t := range h.Scopes {
		s.scopes = append(s.scopes, scopeRow{
			ts: t.TS,
			// The scope log stores a cluster and the fixture's transitions do not
			// carry one, so the harness stamps the suite's own — see
			// conformance.FixtureClusterID.
			clusterID: conformance.FixtureClusterID,
			apiGroup:  t.APIGroup,
			kind:      t.Kind,
			namespace: t.Namespace,
			action:    string(t.Action),
			ruleRef:   t.RuleRef,
		})
	}
	return nil
}

// seedCorpus plants the shared agreement corpus in the stand-in store.
//
// The corpus's flush labels are dropped rather than honoured, and that is the
// truthful mapping rather than a shortcut: this backend stores one row per recorded
// change, so two changes written in one flush and two written in two are the same
// two rows. A store that recorded the distinction would be modelling something the
// real table does not have.
//
// This is the stand-in, so what it proves is the engine's Go logic against a known
// row set. The authoritative seeding is the INSERT in integration_test.go, against
// the shipped DDL — see test/agreement for which of the two is the run that counts.
func (s *fakeStore) seedCorpus(c conformance.Corpus) error { return s.seed(c.History()) }

func emptyIfNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return maps.Clone(m)
}

func sliceOrEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return slices.Clone(s)
}

// setFault installs the fault the stand-in applies to its next change stream;
// nil clears it.
func (s *fakeStore) setFault(f *conformance.StreamFault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// fakeConn is a driver.Conn whose Query answers from a fakeStore.
//
// Embedding driver.Conn satisfies the rest of the interface; Query is the only
// method this backend uses, which is itself worth knowing — an engine that
// started issuing Exec or PrepareBatch would fail here with a nil-pointer panic
// rather than silently writing to a read plane.
type fakeConn struct {
	driver.Conn
	store *fakeStore
	// seen records every statement the engine emitted, so a test can assert over
	// all of them rather than over the ones it thought to build by hand. It is
	// guarded by store.mu rather than by a second mutex of its own: every write to
	// it happens on the same call that reads the store.
	seen []string
}

func (c *fakeConn) Query(_ context.Context, sqlText string, args ...any) (driver.Rows, error) {
	c.store.mu.Lock()
	c.seen = append(c.seen, sqlText)
	c.store.mu.Unlock()

	parsed, err := parseStatement(sqlText, args)
	if err != nil {
		return nil, err
	}
	return c.store.evaluate(parsed)
}

// statements returns every statement this connection has been asked to run.
func (c *fakeConn) statements() []string {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	return slices.Clone(c.seen)
}

// parsedStatement is a statement taken apart into the pieces the stand-in
// evaluates.
type parsedStatement struct {
	projection string
	table      string
	final      bool
	predicates []string
	tail       []string
	args       []any
	sql        string
}

// parseStatement takes a statement apart, insisting on the canonical layout
// renderSelect produces.
//
// Insisting is the point. A stand-in that tolerated a free-form statement would
// have to guess at the clauses it could not place, and a guess is exactly what a
// pin must not make.
func parseStatement(sqlText string, args []any) (parsedStatement, error) {
	parsed := parsedStatement{args: args, sql: sqlText}
	lines := strings.Split(sqlText, "\n")
	if len(lines) < 3 {
		return parsed, fmt.Errorf("stand-in: %q is not a statement this harness can take apart", sqlText)
	}

	var found struct{ selectLine, fromLine, whereLine bool }
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case line == "":
		case strings.HasPrefix(line, "SELECT "):
			parsed.projection = strings.TrimPrefix(line, "SELECT ")
			found.selectLine = true
		case strings.HasPrefix(line, "FROM "):
			table := strings.TrimPrefix(line, "FROM ")
			parsed.final = strings.HasSuffix(table, " FINAL")
			parsed.table = strings.TrimSuffix(table, " FINAL")
			found.fromLine = true
		case strings.HasPrefix(line, "WHERE "):
			parsed.predicates = append(parsed.predicates, strings.TrimPrefix(line, "WHERE "))
			found.whereLine = true
		case strings.HasPrefix(line, "AND "):
			parsed.predicates = append(parsed.predicates, strings.TrimPrefix(line, "AND "))
		default:
			parsed.tail = append(parsed.tail, line)
		}
	}
	if !found.selectLine || !found.fromLine {
		return parsed, fmt.Errorf("stand-in: %q lacks a SELECT or a FROM", sqlText)
	}
	// A missing WHERE is admitted for one projection and refused for every other,
	// by name. The cluster-identity probe genuinely has nothing to filter by — it
	// is the question asked when the caller cannot yet name a cluster — while for
	// any other read a vanished WHERE is a predicate list that came out empty by
	// accident, and evaluating that as "match everything" is precisely the silent
	// widening this stand-in exists to catch.
	if !found.whereLine && parsed.projection != fakeClusterIDColumn {
		return parsed, fmt.Errorf("stand-in: %q lacks a WHERE, and only the cluster-identity probe "+
			"may read without one", sqlText)
	}
	return parsed, nil
}

// evaluate answers a parsed statement, or refuses it by name.
func (s *fakeStore) evaluate(p parsedStatement) (driver.Rows, error) {
	switch p.table {
	case tableResourceStates:
		// The correctness requirement of this backend, checked before anything
		// else: a read of a ReplacingMergeTree without a dedup form can return a
		// row twice, and a duplicated row in an audit timeline is a claim the
		// cluster did something twice.
		if !p.final {
			return nil, fmt.Errorf(
				"stand-in: this read of %s carries no FINAL (nor any argMax/LIMIT 1 BY reduction), so an "+
					"unmerged duplicate would be rendered as a second change: %s",
				tableResourceStates, collapse(p.sql))
		}
		return s.evaluateStates(p)
	case tableWatchScopes:
		return s.evaluateScopes(p)
	}
	return nil, fmt.Errorf("stand-in: no table named %q in the frozen schema: %s", p.table, collapse(p.sql))
}

// evaluateStates answers one of the four reads of resource_states.
func (s *fakeStore) evaluateStates(p parsedStatement) (driver.Rows, error) {
	rows, err := s.selectStates(p)
	if err != nil {
		return nil, err
	}
	switch p.projection {
	case fakeChangeColumns:
		return s.projectChanges(p, rows)
	case fakeReplayColumns:
		return projectReplay(p, rows)
	case fakeUIDColumn:
		return projectNewestUID(p, rows)
	case fakeSpanColumns:
		return projectSpans(p, rows)
	case fakeClusterIDColumn:
		return projectClusterIDs(p, clusterIDsOf(rows))
	}
	return nil, fmt.Errorf("stand-in: no read of %s projects %q: %s",
		tableResourceStates, p.projection, collapse(p.sql))
}

// selectStates applies the WHERE clause and then collapses duplicates the way
// FINAL does.
func (s *fakeStore) selectStates(p parsedStatement) ([]stateRow, error) {
	matchers, err := stateMatchers(p)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	stored := slices.Clone(s.states)
	s.mu.Unlock()

	var kept []stateRow
	seen := map[string]bool{}
	for _, row := range stored {
		if !slices.ContainsFunc(matchers, func(m func(stateRow) bool) bool { return !m(row) }) {
			if seen[row.sortKey()] {
				continue
			}
			seen[row.sortKey()] = true
			kept = append(kept, row)
		}
	}
	return kept, nil
}

// stateMatchers turns the predicate list into row tests, consuming the bound
// arguments in the order their placeholders appear.
//
// Consuming in order is what makes this a real check on the Go side: a builder
// that bound the namespace where the name belongs would produce a statement this
// function reads happily and a result the property rejects.
func stateMatchers(p parsedStatement) ([]func(stateRow) bool, error) {
	cursor := &argCursor{args: p.args}
	matchers := make([]func(stateRow) bool, 0, len(p.predicates))

	for _, predicate := range p.predicates {
		matcher, err := stateMatcher(predicate, cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, collapse(p.sql))
		}
		matchers = append(matchers, matcher)
	}
	if err := cursor.exhausted(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, collapse(p.sql))
	}
	return matchers, nil
}

// stateMatcher builds the test for one predicate of a resource_states read.
func stateMatcher(predicate string, cursor *argCursor) (func(stateRow) bool, error) {
	switch predicate {
	case "cluster_id = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.clusterID })
	case "api_group = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.apiGroup })
	case "kind = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.kind })
	case "namespace = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.namespace })
	case "name = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.name })
	case "uid = ?":
		return equalsColumn(cursor, func(r stateRow) string { return r.uid })
	case fakeEventGroups:
		return func(r stateRow) bool { return r.apiGroup == "" || r.apiGroup == "events.k8s.io" }, nil
	case "ts >= ?":
		bound, err := cursor.instant()
		if err != nil {
			return nil, err
		}
		return func(r stateRow) bool { return !r.ts.Before(bound) }, nil
	case "ts <= ?":
		bound, err := cursor.instant()
		if err != nil {
			return nil, err
		}
		return func(r stateRow) bool { return !r.ts.After(bound) }, nil
	case "hasAny(actors, ?)":
		wanted, err := cursor.strings()
		if err != nil {
			return nil, err
		}
		return func(r stateRow) bool { return hasAny(r.actors, wanted) }, nil
	case "NOT hasAny(actors, ?)":
		wanted, err := cursor.strings()
		if err != nil {
			return nil, err
		}
		return func(r stateRow) bool { return !hasAny(r.actors, wanted) }, nil
	}

	for _, field := range subjectFields {
		if predicate != fakeSubjectMatch(field) {
			continue
		}
		want, err := cursor.str()
		if err != nil {
			return nil, err
		}
		return func(r stateRow) bool { return eventSubject(r.data, field) == want }, nil
	}
	return nil, fmt.Errorf("stand-in: no predicate of a %s read is spelled %q, so this harness cannot "+
		"evaluate what the backend asked for", tableResourceStates, predicate)
}

// equalsColumn is the common case: a column compared against a bound string.
func equalsColumn(cursor *argCursor, column func(stateRow) string) (func(stateRow) bool, error) {
	want, err := cursor.str()
	if err != nil {
		return nil, err
	}
	return func(r stateRow) bool { return column(r) == want }, nil
}

// hasAny is ClickHouse's array-intersection test.
func hasAny(have, wanted []string) bool {
	return slices.ContainsFunc(have, func(a string) bool { return slices.Contains(wanted, a) })
}

// eventSubject reads one field of an Event's subject out of its recorded state,
// trying both spellings exactly as the coalesce does.
func eventSubject(data, field string) string {
	var doc map[string]any
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		return ""
	}
	for _, holder := range []string{"involvedObject", "regarding"} {
		subject, ok := doc[holder].(map[string]any)
		if !ok {
			continue
		}
		if value, ok := subject[field].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

// projectChanges answers the change-stream read, applying the ordering, the
// pushed-down limit, and any installed stream fault.
func (s *fakeStore) projectChanges(p parsedStatement, rows []stateRow) (driver.Rows, error) {
	ordered, limit, err := orderAndLimit(p, rows)
	if err != nil {
		return nil, err
	}

	out := make([][]any, 0, len(ordered))
	for _, r := range ordered {
		out = append(out, []any{
			r.ts, r.eventType, r.apiVersion, r.uid, r.resourceVersion,
			maps.Clone(r.labels), slices.Clone(r.actors), r.data, r.diff, r.sha256,
		})
	}
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}

	s.mu.Lock()
	fault := s.fault
	s.mu.Unlock()
	return newFakeRows(out, fault), nil
}

// projectReplay answers the reconstruction read.
func projectReplay(p parsedStatement, rows []stateRow) (driver.Rows, error) {
	if !slices.Equal(p.tail, []string{"ORDER BY ts"}) {
		return nil, fmt.Errorf("stand-in: a replay read must be ordered oldest first, and this one ends "+
			"%q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}
	ordered := slices.Clone(rows)
	slices.SortStableFunc(ordered, func(a, b stateRow) int { return a.ts.Compare(b.ts) })

	out := make([][]any, 0, len(ordered))
	for _, r := range ordered {
		out = append(out, []any{r.ts, r.eventType, r.data, r.diff, r.sha256})
	}
	return newFakeRows(out, nil), nil
}

// projectNewestUID answers the newest-incarnation probe.
func projectNewestUID(p parsedStatement, rows []stateRow) (driver.Rows, error) {
	if !slices.Equal(p.tail, []string{"ORDER BY ts DESC", "LIMIT 1"}) {
		return nil, fmt.Errorf("stand-in: the newest-incarnation probe must be the newest row alone, and "+
			"this one ends %q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}
	ordered := slices.Clone(rows)
	slices.SortStableFunc(ordered, func(a, b stateRow) int { return b.ts.Compare(a.ts) })

	var out [][]any
	if len(ordered) > 0 {
		out = append(out, []any{ordered[0].uid})
	}
	return newFakeRows(out, nil), nil
}

// clusterIDsOf reads the cluster column off a set of record rows.
func clusterIDsOf(rows []stateRow) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.clusterID)
	}
	return ids
}

// projectClusterIDs answers the cluster-identity probe over either table.
//
// It applies the DISTINCT the projection asks for and the ordering the tail asks
// for, rather than assuming either: the engine's contract promises a sorted,
// duplicate-free list, and a stand-in that sorted unbidden would let a statement
// that had lost its ORDER BY keep passing.
func projectClusterIDs(p parsedStatement, ids []string) (driver.Rows, error) {
	if !slices.Equal(p.tail, []string{"ORDER BY cluster_id"}) {
		return nil, fmt.Errorf("stand-in: the cluster-identity probe must arrive sorted, and this one "+
			"ends %q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}
	distinct := slices.Clone(ids)
	slices.Sort(distinct)
	distinct = slices.Compact(distinct)

	out := make([][]any, 0, len(distinct))
	for _, id := range distinct {
		out = append(out, []any{id})
	}
	return newFakeRows(out, nil), nil
}

// projectSpans answers the per-incarnation aggregate.
func projectSpans(p parsedStatement, rows []stateRow) (driver.Rows, error) {
	if !slices.Equal(p.tail, []string{"GROUP BY uid", "ORDER BY first_seen"}) {
		return nil, fmt.Errorf("stand-in: the incarnation aggregate must group by uid and order by its "+
			"first sighting, and this one ends %q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}

	type span struct {
		first, last time.Time
		deletions   uint64
	}
	spans := map[string]*span{}
	order := make([]string, 0, len(rows))
	for _, r := range rows {
		got, ok := spans[r.uid]
		if !ok {
			spans[r.uid] = &span{first: r.ts, last: r.ts, deletions: deletionCount(r)}
			order = append(order, r.uid)
			continue
		}
		if r.ts.Before(got.first) {
			got.first = r.ts
		}
		if r.ts.After(got.last) {
			got.last = r.ts
		}
		got.deletions += deletionCount(r)
	}
	slices.SortStableFunc(order, func(a, b string) int { return spans[a].first.Compare(spans[b].first) })

	out := make([][]any, 0, len(order))
	for _, uid := range order {
		s := spans[uid]
		out = append(out, []any{uid, s.first, s.last, s.deletions})
	}
	return newFakeRows(out, nil), nil
}

func deletionCount(r stateRow) uint64 {
	if r.eventType == query.EventDeleted {
		return 1
	}
	return 0
}

// evaluateScopes answers the watch-scope read.
func (s *fakeStore) evaluateScopes(p parsedStatement) (driver.Rows, error) {
	if p.final {
		return nil, fmt.Errorf("stand-in: %s is a plain MergeTree with nothing to collapse, so FINAL on "+
			"it is a cost with no return: %s", tableWatchScopes, collapse(p.sql))
	}
	if p.projection == fakeClusterIDColumn {
		s.mu.Lock()
		stored := slices.Clone(s.scopes)
		s.mu.Unlock()
		ids := make([]string, 0, len(stored))
		for _, row := range stored {
			ids = append(ids, row.clusterID)
		}
		return projectClusterIDs(p, ids)
	}
	if p.projection != fakeScopeColumns {
		return nil, fmt.Errorf("stand-in: no read of %s projects %q: %s",
			tableWatchScopes, p.projection, collapse(p.sql))
	}
	if !slices.Equal(p.tail, []string{"ORDER BY api_group, kind, namespace, ts"}) {
		return nil, fmt.Errorf("stand-in: a scope read must arrive grouped by scope and oldest first "+
			"within each, and this one ends %q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}

	matchers, err := scopeMatchers(p)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	stored := slices.Clone(s.scopes)
	s.mu.Unlock()

	var kept []scopeRow
	for _, row := range stored {
		if !slices.ContainsFunc(matchers, func(m func(scopeRow) bool) bool { return !m(row) }) {
			kept = append(kept, row)
		}
	}
	slices.SortStableFunc(kept, func(a, b scopeRow) int {
		if c := strings.Compare(a.apiGroup, b.apiGroup); c != 0 {
			return c
		}
		if c := strings.Compare(a.kind, b.kind); c != 0 {
			return c
		}
		if c := strings.Compare(a.namespace, b.namespace); c != 0 {
			return c
		}
		return a.ts.Compare(b.ts)
	})

	out := make([][]any, 0, len(kept))
	for _, r := range kept {
		out = append(out, []any{r.apiGroup, r.kind, r.namespace, r.action, r.ruleRef, r.ts})
	}
	return newFakeRows(out, nil), nil
}

// scopeMatchers turns a scope read's predicates into row tests.
func scopeMatchers(p parsedStatement) ([]func(scopeRow) bool, error) {
	cursor := &argCursor{args: p.args}
	matchers := make([]func(scopeRow) bool, 0, len(p.predicates))

	for _, predicate := range p.predicates {
		var (
			matcher func(scopeRow) bool
			err     error
		)
		switch predicate {
		case "cluster_id = ?":
			matcher, err = equalsScope(cursor, func(r scopeRow) string { return r.clusterID })
		case "api_group = ?":
			matcher, err = equalsScope(cursor, func(r scopeRow) string { return r.apiGroup })
		case "kind = ?":
			matcher, err = equalsScope(cursor, func(r scopeRow) string { return r.kind })
		case "(namespace = ? OR namespace = '')":
			// The covering reading: a query for one namespace matches that
			// namespace's own scope *and* the all-namespaces scope, because a
			// cluster-wide rule really was watching the object.
			var want string
			if want, err = cursor.str(); err == nil {
				matcher = func(r scopeRow) bool { return r.namespace == want || r.namespace == "" }
			}
		default:
			err = fmt.Errorf("stand-in: no predicate of a %s read is spelled %q", tableWatchScopes, predicate)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, collapse(p.sql))
		}
		matchers = append(matchers, matcher)
	}
	if err := cursor.exhausted(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, collapse(p.sql))
	}
	return matchers, nil
}

func equalsScope(cursor *argCursor, column func(scopeRow) string) (func(scopeRow) bool, error) {
	want, err := cursor.str()
	if err != nil {
		return nil, err
	}
	return func(r scopeRow) bool { return column(r) == want }, nil
}

// orderAndLimit reads a change stream's tail: the direction, and a limit if one
// was pushed down.
func orderAndLimit(p parsedStatement, rows []stateRow) ([]stateRow, int, error) {
	if len(p.tail) == 0 || len(p.tail) > 2 {
		return nil, 0, fmt.Errorf("stand-in: a change stream ends with an ordering and at most a limit, "+
			"and this one ends %q: %s", strings.Join(p.tail, " / "), collapse(p.sql))
	}

	var descending bool
	switch p.tail[0] {
	case "ORDER BY ts":
	case "ORDER BY ts DESC":
		descending = true
	default:
		return nil, 0, fmt.Errorf("stand-in: a change stream is ordered by ts in one direction or the "+
			"other, and this one says %q: %s", p.tail[0], collapse(p.sql))
	}

	limit := 0
	if len(p.tail) == 2 {
		text, ok := strings.CutPrefix(p.tail[1], "LIMIT ")
		if !ok {
			return nil, 0, fmt.Errorf("stand-in: %q is not a limit: %s", p.tail[1], collapse(p.sql))
		}
		parsed, err := strconv.Atoi(text)
		if err != nil {
			return nil, 0, fmt.Errorf("stand-in: %q is not a number of rows: %s", p.tail[1], collapse(p.sql))
		}
		limit = parsed
	}

	ordered := slices.Clone(rows)
	slices.SortStableFunc(ordered, func(a, b stateRow) int {
		if descending {
			return b.ts.Compare(a.ts)
		}
		return a.ts.Compare(b.ts)
	})
	return ordered, limit, nil
}

// argCursor hands out the bound arguments in the order their placeholders
// appear, refusing a type that does not match the column.
type argCursor struct {
	args []any
	at   int
}

func (c *argCursor) next() (any, error) {
	if c.at >= len(c.args) {
		return nil, fmt.Errorf("stand-in: the statement has more placeholders than the %d arguments bound",
			len(c.args))
	}
	value := c.args[c.at]
	c.at++
	return value, nil
}

func (c *argCursor) str() (string, error) {
	value, err := c.next()
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("stand-in: argument %d is a %T, want a string", c.at-1, value)
	}
	return text, nil
}

func (c *argCursor) strings() ([]string, error) {
	value, err := c.next()
	if err != nil {
		return nil, err
	}
	list, ok := value.([]string)
	if !ok {
		return nil, fmt.Errorf("stand-in: argument %d is a %T, want a []string for an array predicate",
			c.at-1, value)
	}
	return list, nil
}

// instant parses a datetime argument the way the server parses the literal.
//
// It insists on a string. A time.Time bound here would be rendered by the driver
// at second precision, which is a defect this backend has to avoid rather than a
// style choice — see chTimeFormat — so a bound instant is a finding, not a
// convenience to be accommodated.
func (c *argCursor) instant() (time.Time, error) {
	value, err := c.next()
	if err != nil {
		return time.Time{}, err
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, fmt.Errorf(
			"stand-in: argument %d is a %T, want a datetime string; a bound time.Time is rendered by the "+
				"driver at second precision, which would blunt a nanosecond bound", c.at-1, value)
	}
	parsed, err := time.ParseInLocation(chTimeFormat, text, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("stand-in: argument %d, %q, is not a %q datetime: %w",
			c.at-1, text, chTimeFormat, err)
	}
	return parsed, nil
}

// exhausted reports arguments the statement never had a placeholder for.
func (c *argCursor) exhausted() error {
	if c.at != len(c.args) {
		return fmt.Errorf("stand-in: %d arguments were bound but only %d placeholders consumed them, so "+
			"one of them is landing in a column it was not meant for", len(c.args), c.at)
	}
	return nil
}

// fakeRows is a driver.Rows over an in-memory result set that can be made to
// break part-way through.
//
// The break is modelled the way a dropped connection really behaves, and that
// shape is the whole point: rows already delivered stay delivered, Next simply
// stops yielding, and the failure surfaces through Err — never through Next or
// Scan. A reader that only checked Scan's error would see a short, clean result
// set, which is exactly the mistake the mid-stream property exists to catch.
type fakeRows struct {
	driver.Rows
	rows      [][]any
	at        int
	delivered int
	// breakAfter is how many rows may be delivered before the stream dies; -1 when
	// it is healthy.
	breakAfter int
	err        error
	closed     bool
}

func newFakeRows(rows [][]any, fault *conformance.StreamFault) *fakeRows {
	out := &fakeRows{rows: rows, breakAfter: -1}
	if fault != nil {
		out.breakAfter, out.err = fault.AfterChanges, fault.Err
	}
	return out
}

func (r *fakeRows) broken() bool {
	return r.err != nil && r.breakAfter >= 0 && r.delivered >= r.breakAfter
}

func (r *fakeRows) Next() bool {
	if r.closed || r.broken() {
		return false
	}
	return r.at < len(r.rows)
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.at >= len(r.rows) {
		return errors.New("stand-in: Scan called past the end of the result set")
	}
	row := r.rows[r.at]
	r.at++
	r.delivered++
	if len(dest) != len(row) {
		return fmt.Errorf("stand-in: the read scanned %d columns, but the result carries %d",
			len(dest), len(row))
	}
	for i, d := range dest {
		if err := assignColumn(d, row[i]); err != nil {
			return fmt.Errorf("stand-in: column %d: %w", i, err)
		}
	}
	return nil
}

func (r *fakeRows) Err() error {
	if r.broken() {
		return r.err
	}
	return nil
}

// Close records the release. It reports an error when called twice, which is how
// the iterators' own guard against a double close is proven to be doing something
// — the contract makes Close safe to repeat, and a guard nothing tests is a guard
// that can quietly go away.
func (r *fakeRows) Close() error {
	if r.closed {
		return errors.New("stand-in: rows closed twice; the driver's cursor is already released")
	}
	r.closed = true
	return nil
}

// assignColumn writes one column value into a scan target.
func assignColumn(dest, value any) error {
	switch target := dest.(type) {
	case *string:
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("scanning a %T into a *string", value)
		}
		*target = text
	case *time.Time:
		instant, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("scanning a %T into a *time.Time", value)
		}
		*target = instant
	case *uint64:
		count, ok := value.(uint64)
		if !ok {
			return fmt.Errorf("scanning a %T into a *uint64", value)
		}
		*target = count
	case *[]string:
		list, ok := value.([]string)
		if !ok {
			return fmt.Errorf("scanning a %T into a *[]string", value)
		}
		*target = list
	case *map[string]string:
		m, ok := value.(map[string]string)
		if !ok {
			return fmt.Errorf("scanning a %T into a *map[string]string", value)
		}
		*target = m
	default:
		return fmt.Errorf("no column of the frozen schema scans into a %T", dest)
	}
	return nil
}

// collapse renders a multi-line statement on one line for an error message.
func collapse(sqlText string) string { return strings.Join(strings.Fields(sqlText), " ") }
