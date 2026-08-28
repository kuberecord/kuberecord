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

package conformance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// canonicalJSON re-serializes a JSON document with sorted object keys.
//
// This is the canonicalization docs/SCHEMA.md's "verifying a reconstruction" step
// specifies, and encoding/json performs it for free: marshalling a map emits its
// keys in sorted order. Decoding to any and re-encoding is therefore the whole of
// it.
//
// Numbers pass through float64, which is what an engine decoding the same document
// with encoding/json will also do — the two sides agree because they take the
// identical path. That agreement is only free for values float64 represents
// exactly, which is why the fixtures hold nothing but strings, booleans, small
// integers, objects and arrays. A fixture carrying a float or an integer past 2^53
// would make a byte-comparison depend on formatting rather than on content, and the
// property would be measuring encoding/json instead of the backend.
func canonicalJSON(raw []byte) ([]byte, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("re-encode: %w", err)
	}
	return out, nil
}

// canonicalValue canonicalizes an already-decoded document, so a reconstructed
// map and a fixture's source text can be compared as bytes.
func canonicalValue(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return canonicalJSON(raw)
}

// sha256Hex is the hex SHA-256 the schema's sha256 column holds.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// mustCanonicalJSON is canonicalJSON for fixture text, which is a compile-time
// constant of this package: text that fails to parse is a bug in the suite, not a
// finding about a backend, and there is no useful way for a property to carry on.
func mustCanonicalJSON(raw string) []byte {
	out, err := canonicalJSON([]byte(raw))
	if err != nil {
		panic("conformance: fixture document is not valid JSON: " + err.Error())
	}
	return out
}

// seed plants a property's history in the backend.
func seed(t conformanceT, h Harness, history History) {
	t.Helper()
	if err := h.Seed(history); err != nil {
		t.Fatalf("conformance: Harness.Seed reported %v; the property cannot assert against a history "+
			"the backend does not hold", err)
	}
}

// closeEngine closes the engine at the end of a property and reports a failure to
// do so.
//
// Close is part of the contract — it releases what the engine itself created, and
// calling it more than once is safe — so a non-nil error here is a finding rather
// than housekeeping noise.
func closeEngine(t conformanceT, e query.QueryEngine) {
	if err := e.Close(); err != nil {
		t.Errorf("conformance: QueryEngine.Close reported %v; the suite closes the engine at the end of "+
			"every property, and a Close that fails leaves whatever it holds to the next one", err)
	}
}

// closeIterator closes an iterator and reports a failure to do so. The contract
// makes Close safe at any point and safe to repeat, so the suite closes on every
// path — including after an early break, which is the normal shape whenever a limit
// is satisfied.
func closeIterator(t conformanceT, it query.ChangeIterator) {
	if err := it.Close(); err != nil {
		t.Errorf("conformance: ChangeIterator.Close reported %v; Close is safe at any point and safe to "+
			"repeat, so a caller that breaks out of the loop must not be punished for it", err)
	}
}

// timeline runs a query and fails the property if the engine refuses it outright.
func timeline(t conformanceT, h Harness, q query.TimelineQuery) query.ChangeIterator {
	t.Helper()
	it, err := h.Engine.Timeline(context.Background(), q)
	if err != nil {
		t.Fatalf("conformance: Timeline(%s) returned %v; the property expected the engine to answer it",
			describeQuery(q), err)
	}
	return it
}

// collect drains an iterator into a slice, closing it and checking Err on every
// path — the usage shape query.ChangeIterator documents, with the post-loop error
// check that is not optional.
func collect(t conformanceT, it query.ChangeIterator) []query.Change {
	t.Helper()
	defer closeIterator(t, it)

	var got []query.Change
	for it.Next() {
		got = append(got, it.Change())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("conformance: the iterator failed mid-stream with %v; the property expected a complete "+
			"result set", err)
	}
	return got
}

// timelineChanges is the whole of the usual shape: ask, drain, close, check.
func timelineChanges(t conformanceT, h Harness, q query.TimelineQuery) []query.Change {
	t.Helper()
	return collect(t, timeline(t, h, q))
}

// engineChanges is timelineChanges against an engine the harness did not supply.
//
// It exists for the filter agreement property, which poses one query to two engines
// — the backend under test and the suite's own non-pushdown reference — and so
// cannot go through the Harness for both.
func engineChanges(t conformanceT, e query.QueryEngine, q query.TimelineQuery) []query.Change {
	t.Helper()
	it, err := e.Timeline(context.Background(), q)
	if err != nil {
		t.Fatalf("conformance: Timeline(%s) returned %v; the property expected the engine to answer it",
			describeQuery(q), err)
	}
	return collect(t, it)
}

// changesEqual compares two changes as a caller renders them.
//
// Timestamps are compared with Equal rather than ==, because a backend is entitled
// to return an instant in whatever location it stores — what must survive is the
// instant, to the nanosecond, not the monotonic reading or the zone.
func changesEqual(a, b query.Change) bool {
	return a.TS.Equal(b.TS) &&
		a.EventType == b.EventType &&
		a.UID == b.UID &&
		a.ResourceVersion == b.ResourceVersion &&
		a.APIVersion == b.APIVersion &&
		a.Data == b.Data &&
		a.Diff == b.Diff &&
		a.SHA256 == b.SHA256 &&
		slices.Equal(a.Actors, b.Actors) &&
		maps.Equal(a.Labels, b.Labels)
}

// describeChange renders a change for a failure message: enough to identify the
// row without pasting a whole object into the output.
func describeChange(c query.Change) string {
	return fmt.Sprintf("{ts=%s event=%s uid=%s rv=%s actors=%v}",
		c.TS.UTC().Format(time.RFC3339Nano), c.EventType, c.UID, c.ResourceVersion, c.Actors)
}

// describeChanges renders a whole result set, one row per line, so a mismatch can
// be read rather than decoded.
func describeChanges(changes []query.Change) string {
	if len(changes) == 0 {
		return "(no changes)"
	}
	var b strings.Builder
	for i, c := range changes {
		fmt.Fprintf(&b, "\n  [%d] %s", i, describeChange(c))
	}
	return b.String()
}

// describeQuery renders a timeline query for a failure message.
func describeQuery(q query.TimelineQuery) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s %s/%s", q.Ref.APIGroup, q.Ref.Kind, q.Ref.Namespace, q.Ref.Name)
	if !q.From.IsZero() || !q.To.IsZero() {
		fmt.Fprintf(&b, " window=[%s,%s]", formatBound(q.From), formatBound(q.To))
	}
	if q.UID != "" {
		fmt.Fprintf(&b, " uid=%s", q.UID)
	}
	if q.AllIncarnations {
		b.WriteString(" allIncarnations")
	}
	if len(q.Actors) > 0 {
		fmt.Fprintf(&b, " actors=%v", q.Actors)
	}
	if len(q.ExcludeActors) > 0 {
		fmt.Fprintf(&b, " excludeActors=%v", q.ExcludeActors)
	}
	if len(q.FieldPaths) > 0 {
		fmt.Fprintf(&b, " fieldPaths=%v", q.FieldPaths)
	}
	if q.Limit > 0 {
		fmt.Fprintf(&b, " limit=%d", q.Limit)
	}
	if q.Reverse {
		b.WriteString(" reverse")
	}
	return b.String()
}

// formatBound renders one side of a window, spelling an absent bound as such
// rather than as the zero time — the distinction ErrTimeBoundRequired turns on.
func formatBound(t time.Time) string {
	if t.IsZero() {
		return "unbounded"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// assertChanges compares a result set against the expected one and, on a mismatch,
// prints both in full.
//
// Printing both is the point. A property that reported only "got 4, want 5" would
// leave the backend author to reconstruct which row went missing, and for the
// ordering and incarnation properties the identity of the offending row *is* the
// diagnosis.
func assertChanges(t conformanceT, got, want []query.Change, when string) {
	t.Helper()
	if len(got) == len(want) {
		mismatch := -1
		for i := range got {
			if !changesEqual(got[i], want[i]) {
				mismatch = i
				break
			}
		}
		if mismatch < 0 {
			return
		}
		t.Errorf("conformance: %s: row %d differs.\ngot: %s\nwant: %s\nfull result:%s\nfull expectation:%s",
			when, mismatch, describeChange(got[mismatch]), describeChange(want[mismatch]),
			describeChanges(got), describeChanges(want))
		return
	}
	t.Errorf("conformance: %s: got %d changes, want %d.\nfull result:%s\nfull expectation:%s",
		when, len(got), len(want), describeChanges(got), describeChanges(want))
}

// assertAscending checks that changes are in non-decreasing ts order and, where
// the fixture spaced them apart, strictly increasing.
//
// It is separate from assertChanges because order and content fail differently: a
// backend that returned the right rows in the wrong order has one bug, and a
// message naming the two rows that are out of sequence says so far better than a
// row-by-row diff of a shuffled list.
func assertOrdered(t conformanceT, got []query.Change, reverse bool, when string) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1].TS, got[i].TS
		out := cur.Before(prev)
		if reverse {
			out = prev.Before(cur)
		}
		if out {
			t.Errorf("conformance: %s: rows %d and %d are out of ts order (%s then %s); the contract emits "+
				"in ts order, or reverse ts order when asked, and a caller renders them in the order they "+
				"arrive", when, i-1, i,
				prev.UTC().Format(time.RFC3339Nano), cur.UTC().Format(time.RFC3339Nano))
			return
		}
	}
}

// retained reports whether a backend with these capabilities can hold this row.
//
// It is the one place the suite reasons about a reduced backend, and it is shared
// between the expectations a property builds and the reference engine, so the two
// cannot drift. Today only deletions are at stake: an engine declaring no Deletions
// never receives a Deleted row, so a history seeded with one is stored without it
// and a property must expect the same.
func retained(caps query.Capabilities, r Row) bool {
	return caps.Deletions || r.Change.EventType != query.EventDeleted
}

// retainedRows drops from a history the rows a backend with these capabilities
// cannot hold.
func retainedRows(rows []Row, caps query.Capabilities) []Row {
	kept := make([]Row, 0, len(rows))
	for _, r := range rows {
		if retained(caps, r) {
			kept = append(kept, r)
		}
	}
	return kept
}

// expected renders the rows of a history as the changes a query for ref should
// return, in the order given, dropping what the backend cannot hold.
func expected(caps query.Capabilities, rows []Row) []query.Change {
	kept := retainedRows(rows, caps)
	want := make([]query.Change, 0, len(kept))
	for _, r := range kept {
		want = append(want, r.Change)
	}
	return want
}

// reversed copies a change slice back to front, for the expectations of a reverse
// query.
func reversed(changes []query.Change) []query.Change {
	out := slices.Clone(changes)
	slices.Reverse(out)
	return out
}

// firstN takes the first n of a slice, or all of it when there are fewer — the
// emission-order reading TimelineQuery.Limit specifies.
func firstN(changes []query.Change, n int) []query.Change {
	if n >= len(changes) {
		return changes
	}
	return changes[:n]
}

// waitFor polls cond until it holds or the deadline passes.
//
// It exists for the leak check, which cannot be an instantaneous assertion: a
// goroutine released by Close is scheduled, not stopped, so the honest question is
// whether it goes away rather than whether it has already gone.
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// goroutines reports the current goroutine count, after giving the runtime a
// moment to reap what has already finished.
//
// This is goleak's core check without goleak's import: the suite's dependency
// budget is the standard library plus internal/query, and a leak assertion is not
// worth widening it for. What is lost is the stack filtering that would name the
// leaked goroutine; what is kept is the property that matters — an iterator closed
// early must not leave anything running.
func goroutines() int {
	runtime.Gosched()
	return runtime.NumGoroutine()
}
