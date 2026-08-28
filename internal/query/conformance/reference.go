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

// referenceEngine is a complete, deliberately unoptimised QueryEngine over an
// in-memory History. It exists for one property and is reused by two.
//
// The property it exists for is the filter agreement clause: a filtered result must
// be identical whether the predicate was pushed into the backend or applied to rows
// the engine had already read. The suite only ever holds one engine at a time, so
// there would be nothing to compare against — unless the suite carries a
// non-pushdown implementation of its own. This is that implementation: every
// predicate here is applied client-side, after the rows have been selected, which
// is exactly the half of the comparison a pushdown backend cannot supply.
//
// The properties do not lean on it alone. Each filter case also states, as a list
// of row indices, which rows must survive — because two implementations of the same
// rule agree with each other far more readily than either agrees with the contract,
// and a bug shared by the reference and the backend would otherwise pass.
//
// Its second use is as the compliant engine the non-vacuity tests need: an engine
// the suite must have nothing to say about, so that "the suite rejected this
// backend" means something. The broken fixtures in fakes_test.go are built by
// wrapping it, which keeps every deliberate flaw out of the shipped package.

package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// referenceEngine answers the read-plane contract from a slice of rows.
type referenceEngine struct {
	rows   []Row
	scopes []ScopeTransition
	caps   query.Capabilities
}

// newReferenceEngine builds an engine over the history a backend with these
// capabilities would have been able to store.
//
// The history is filtered through retained, not stored whole: a backend declaring
// no deletions never receives a Deleted row, and a reference that kept one would be
// answering a question the backend under test was never asked.
func newReferenceEngine(h History, caps query.Capabilities) *referenceEngine {
	return &referenceEngine{
		rows:   retainedRows(h.Rows, caps),
		scopes: slices.Clone(h.Scopes),
		caps:   caps,
	}
}

// Capabilities reports what this engine can answer. No round trip, no failure, the
// same value for its lifetime.
func (e *referenceEngine) Capabilities() query.Capabilities { return e.caps }

// Close releases nothing, because an in-memory engine holds nothing, and is safe to
// repeat.
func (e *referenceEngine) Close() error { return nil }

// Timeline selects, filters, orders and limits — in that order, and the order
// matters. Incarnation selection happens before the predicates, so that an actor
// filter cannot change *which* incarnation is the newest one; a backend that
// filtered first would answer a question about the wrong object whenever the newest
// incarnation's changes were all made by an excluded actor.
func (e *referenceEngine) Timeline(_ context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	if e.caps.TimeBoundRequired && q.From.IsZero() && q.To.IsZero() {
		return nil, fmt.Errorf("timeline for %s/%s: %w", q.Ref.Kind, q.Ref.Name, query.ErrTimeBoundRequired)
	}

	rows := e.window(e.forRef(q.Ref), q.From, q.To)
	rows = selectIncarnation(rows, q.UID, q.AllIncarnations)

	changes := make([]query.Change, 0, len(rows))
	for _, r := range rows {
		if keepsFilters(r.Change, q) {
			changes = append(changes, r.Change)
		}
	}

	slices.SortStableFunc(changes, func(a, b query.Change) int { return a.TS.Compare(b.TS) })
	if q.Reverse {
		slices.Reverse(changes)
	}
	if q.Limit > 0 && len(changes) > q.Limit {
		changes = changes[:q.Limit]
	}
	// Never faulted: the reference engine is the *correct* half of the filter
	// agreement comparison and the base every broken fixture is layered over, so
	// injecting a failure is the layer's business and not this one's.
	return newSliceIterator(changes, nil), nil
}

// StateAt executes the reconstruction recipe: find the base, replay forward, stop
// at a deletion.
func (e *referenceEngine) StateAt(
	_ context.Context, ref query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	rows := e.forRef(ref)
	rows = slices.DeleteFunc(rows, func(r Row) bool { return r.Change.TS.After(at) })
	slices.SortStableFunc(rows, func(a, b Row) int { return a.Change.TS.Compare(b.Change.TS) })
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows for %s/%s at or before %s: %w",
			ref.Kind, ref.Name, at.UTC().Format(time.RFC3339Nano), query.ErrObjectNotFound)
	}

	// Empty uid means the newest incarnation alive at or before the instant, which
	// is the one owning the last row — never a blend of two (Invariant 7).
	if uid == "" {
		uid = rows[len(rows)-1].Change.UID
	}
	rows = slices.DeleteFunc(rows, func(r Row) bool { return r.Change.UID != uid })

	// Step 4, applied first because it is terminal: a deletion for this incarnation
	// means the object did not exist at the instant, whatever history precedes it.
	if slices.ContainsFunc(rows, func(r Row) bool { return r.Change.EventType == query.EventDeleted }) {
		return nil, fmt.Errorf("incarnation %s was deleted at or before %s: %w",
			uid, at.UTC().Format(time.RFC3339Nano), query.ErrObjectNotFound)
	}

	// Step 2: the base is the *last* full-state row at or before the instant.
	// Everything before it is irrelevant, which is the bound checkpointing buys.
	base := -1
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].Change.Data != "" {
			base = i
			break
		}
	}
	if base < 0 {
		return nil, fmt.Errorf("history for %s/%s holds no full-state row at or before %s, so its base "+
			"predates the retention window rather than the object being absent: %w",
			ref.Kind, ref.Name, at.UTC().Format(time.RFC3339Nano), query.ErrObjectNotFound)
	}

	return replay(rows, base)
}

// replay walks forward from the base row, applying each subsequent row's patch and
// letting a full-state row replace the document outright.
//
// The base row's own diff is skipped. On a checkpoint the two describe the same
// transition — data is the state *after* the diff — so applying it would produce a
// document that never existed.
func replay(rows []Row, base int) (*query.Reconstruction, error) {
	var doc any
	if err := json.Unmarshal([]byte(rows[base].Change.Data), &doc); err != nil {
		return nil, fmt.Errorf("decode base state at %s: %w", rows[base].Change.TS, err)
	}

	applied := 0
	last := rows[base].Change
	for _, r := range rows[base+1:] {
		switch {
		case r.Change.Data != "":
			if err := json.Unmarshal([]byte(r.Change.Data), &doc); err != nil {
				return nil, fmt.Errorf("decode full state at %s: %w", r.Change.TS, err)
			}
		case r.Change.Diff != "":
			next, err := applyPatch(doc, r.Change.Diff)
			if err != nil {
				return nil, fmt.Errorf("apply patch at %s: %w", r.Change.TS, err)
			}
			doc = next
			applied++
		}
		last = r.Change
	}

	object, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("reconstructed state is %T, not a JSON object", doc)
	}
	return &query.Reconstruction{
		Object:         object,
		BaseTS:         rows[base].Change.TS,
		BaseEvent:      rows[base].Change.EventType,
		PatchesApplied: applied,
		SHA256:         last.SHA256,
	}, nil
}

// Coverage pairs the scope log's transitions into intervals.
func (e *referenceEngine) Coverage(_ context.Context, q query.ScopeQuery) ([]query.ScopeInterval, error) {
	byScope := map[scopeKey][]ScopeTransition{}
	for _, tr := range e.scopes {
		k := scopeKey{group: tr.APIGroup, kind: tr.Kind, namespace: tr.Namespace}
		byScope[k] = append(byScope[k], tr)
	}

	var out []query.ScopeInterval
	for k, transitions := range byScope {
		if !coversScope(q, k) {
			continue
		}
		slices.SortStableFunc(transitions, func(a, b ScopeTransition) int { return a.TS.Compare(b.TS) })
		for _, iv := range pairScope(k, transitions) {
			if overlapsWindow(iv, q.From, q.To) {
				out = append(out, iv)
			}
		}
	}
	slices.SortStableFunc(out, func(a, b query.ScopeInterval) int { return a.From.Compare(b.From) })
	return out, nil
}

// scopeKey is one watched scope, with the scope log's own reading of namespace:
// empty is the all-namespaces scope itself, not a wildcard.
type scopeKey struct {
	group     string
	kind      string
	namespace string
}

// coversScope applies ScopeQuery's matching rules, including the covering reading
// of Namespace: a query for one namespace matches that namespace's own scope *and*
// the all-namespaces scope, because a cluster-wide rule genuinely was watching the
// object and reporting otherwise would answer "never observed" about something that
// was observed the whole time.
func coversScope(q query.ScopeQuery, k scopeKey) bool {
	if q.APIGroup != "" && q.APIGroup != k.group {
		return false
	}
	if q.Kind != "" && q.Kind != k.kind {
		return false
	}
	if q.Namespace != "" && k.namespace != "" && k.namespace != q.Namespace {
		return false
	}
	return true
}

// pairScope walks one scope's transitions in order, opening on Started and closing
// on Stopped, leaving an unmatched trailing Started open.
func pairScope(k scopeKey, transitions []ScopeTransition) []query.ScopeInterval {
	var out []query.ScopeInterval
	open := -1
	for _, tr := range transitions {
		switch tr.Action {
		case ScopeStarted:
			if open >= 0 {
				// A Started with one already open adds nothing: the scope was
				// already being watched, and inventing a zero-length gap would
				// report an outage that never happened.
				continue
			}
			out = append(out, query.ScopeInterval{
				APIGroup: k.group, Kind: k.kind, Namespace: k.namespace,
				RuleRef: tr.RuleRef, From: tr.TS,
			})
			open = len(out) - 1
		case ScopeStopped:
			if open < 0 {
				continue
			}
			stop := tr.TS
			out[open].To = &stop
			open = -1
		}
	}
	return out
}

// overlapsWindow reports whether an interval intersects the query's window. An
// interval that merely overlaps is returned whole, not clipped: the caller is told
// when the scope really opened and closed, since a coverage answer trimmed to the
// question would make a scope opened last year look as though it opened when the
// window did.
func overlapsWindow(iv query.ScopeInterval, from, to time.Time) bool {
	if !to.IsZero() && iv.From.After(to) {
		return false
	}
	if !from.IsZero() && iv.To != nil && iv.To.Before(from) {
		return false
	}
	return true
}

// Incarnations lists the distinct UIDs recorded for ref in the window, oldest first
// by FirstSeen.
func (e *referenceEngine) Incarnations(
	_ context.Context, ref query.ObjectRef, from, to time.Time,
) ([]query.Incarnation, error) {
	if e.caps.TimeBoundRequired && from.IsZero() && to.IsZero() {
		return nil, fmt.Errorf("incarnations of %s/%s: %w", ref.Kind, ref.Name, query.ErrTimeBoundRequired)
	}

	rows := e.window(e.forRef(ref), from, to)
	index := map[string]*query.Incarnation{}
	for _, r := range rows {
		got, ok := index[r.Change.UID]
		if !ok {
			index[r.Change.UID] = &query.Incarnation{
				UID:       r.Change.UID,
				FirstSeen: r.Change.TS,
				LastSeen:  r.Change.TS,
				Deleted:   r.Change.EventType == query.EventDeleted,
			}
			continue
		}
		if r.Change.TS.Before(got.FirstSeen) {
			got.FirstSeen = r.Change.TS
		}
		if r.Change.TS.After(got.LastSeen) {
			got.LastSeen = r.Change.TS
		}
		got.Deleted = got.Deleted || r.Change.EventType == query.EventDeleted
	}

	out := make([]query.Incarnation, 0, len(index))
	for _, inc := range index {
		out = append(out, *inc)
	}
	slices.SortStableFunc(out, func(a, b query.Incarnation) int { return a.FirstSeen.Compare(b.FirstSeen) })
	return out, nil
}

// forRef selects the rows recorded for one canonical identity. No API version is
// consulted: identity is version-agnostic, and an object observed at two versions is
// one object with one history (Invariant 7).
func (e *referenceEngine) forRef(ref query.ObjectRef) []Row {
	var out []Row
	for _, r := range e.rows {
		if r.Ref == ref {
			out = append(out, r)
		}
	}
	return out
}

// window clips rows to an inclusive time bound, with a zero bound meaning unbounded
// on that side.
func (e *referenceEngine) window(rows []Row, from, to time.Time) []Row {
	var out []Row
	for _, r := range rows {
		if !from.IsZero() && r.Change.TS.Before(from) {
			continue
		}
		if !to.IsZero() && r.Change.TS.After(to) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// selectIncarnation applies the default that keeps two objects from becoming one
// timeline: a pinned UID wins outright, AllIncarnations keeps everything, and
// otherwise only the newest incarnation in the window survives.
//
// "Newest" is the incarnation owning the most recent row, which is what an engineer
// investigating right now means by it.
func selectIncarnation(rows []Row, uid string, all bool) []Row {
	if uid != "" {
		return slices.DeleteFunc(slices.Clone(rows), func(r Row) bool { return r.Change.UID != uid })
	}
	if all || len(rows) == 0 {
		return rows
	}
	newest := rows[0]
	for _, r := range rows[1:] {
		if r.Change.TS.After(newest.Change.TS) {
			newest = r
		}
	}
	return slices.DeleteFunc(slices.Clone(rows), func(r Row) bool { return r.Change.UID != newest.Change.UID })
}

// keepsFilters applies the actor and field-path predicates, client-side and in the
// documented order: Actors narrows, ExcludeActors then wins on conflict.
func keepsFilters(c query.Change, q query.TimelineQuery) bool {
	if len(q.Actors) > 0 && !slices.ContainsFunc(c.Actors, func(a string) bool { return slices.Contains(q.Actors, a) }) {
		return false
	}
	if slices.ContainsFunc(c.Actors, func(a string) bool { return slices.Contains(q.ExcludeActors, a) }) {
		return false
	}
	return matchesFieldPaths(c, q.FieldPaths)
}

// matchesFieldPaths reports whether a change touches one of the requested paths.
//
// A row carrying no patch is kept regardless of the filter. Those rows are the
// boundaries of the object's existence — a first sighting, a snapshot, a deletion —
// and a filtered timeline that dropped them would show a history with no beginning
// and no end.
func matchesFieldPaths(c query.Change, paths []string) bool {
	if len(paths) == 0 || c.Diff == "" {
		return true
	}
	var ops []patchOp
	if err := json.Unmarshal([]byte(c.Diff), &ops); err != nil {
		// An undecodable patch is kept rather than dropped: silently discarding a
		// row because its diff could not be parsed would turn a rendering problem
		// into a missing entry in an audit timeline.
		return true
	}
	for _, op := range ops {
		dotted := dottedPath(op.Path)
		for _, want := range paths {
			if dotted == want || strings.HasPrefix(dotted, want+".") {
				return true
			}
		}
	}
	return false
}

// dottedPath converts an RFC 6901 JSON Pointer into the dotted grammar
// TimelineQuery.FieldPaths uses, so "/spec/template/spec/containers/0/image"
// becomes "spec.template.spec.containers.0.image" and a filter on
// "spec.template.spec.containers" matches it by prefix.
func dottedPath(pointer string) string {
	segments := make([]string, 0, 8)
	for seg := range strings.SplitSeq(strings.TrimPrefix(pointer, "/"), "/") {
		seg = strings.ReplaceAll(seg, "~1", "/")
		seg = strings.ReplaceAll(seg, "~0", "~")
		segments = append(segments, seg)
	}
	return strings.Join(segments, ".")
}

// ---------------------------------------------------------------------------
// A minimal RFC 6902 applier
// ---------------------------------------------------------------------------

// The RFC 6902 operations this applier supports, as constants because they are
// matched in several places and a typo would silently make a patch a no-op.
const (
	opAdd     = "add"
	opReplace = "replace"
	opRemove  = "remove"
)

// patchOp is one JSON Patch operation.
type patchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

// applyPatch applies an RFC 6902 patch to a decoded document.
//
// It supports add, replace and remove over objects and arrays, including the "-"
// append token, and nothing else. That is the whole of what this package's fixtures
// use, and writing the little that is needed is what keeps the suite's dependency
// budget at the standard library — a suite that pulled in a patch library to test
// backends would be measuring them against that library's reading of the RFC rather
// than against the contract.
func applyPatch(doc any, patch string) (any, error) {
	var ops []patchOp
	if err := json.Unmarshal([]byte(patch), &ops); err != nil {
		return nil, fmt.Errorf("decode patch: %w", err)
	}
	for _, op := range ops {
		var value any
		if len(op.Value) > 0 {
			if err := json.Unmarshal(op.Value, &value); err != nil {
				return nil, fmt.Errorf("decode value of %s %s: %w", op.Op, op.Path, err)
			}
		}
		next, err := applyAt(doc, pointerTokens(op.Path), op.Op, value)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", op.Op, op.Path, err)
		}
		doc = next
	}
	return doc, nil
}

// pointerTokens splits a JSON Pointer into its unescaped segments.
func pointerTokens(pointer string) []string {
	if pointer == "" || pointer == "/" {
		return nil
	}
	segments := make([]string, 0, 8)
	for seg := range strings.SplitSeq(strings.TrimPrefix(pointer, "/"), "/") {
		seg = strings.ReplaceAll(seg, "~1", "/")
		seg = strings.ReplaceAll(seg, "~0", "~")
		segments = append(segments, seg)
	}
	return segments
}

// applyAt walks to the addressed node and applies the operation, returning the node
// its caller must store.
//
// Returning the node rather than mutating through a pointer is what makes array
// operations work: appending to a slice produces a new header, so the parent has to
// be told about it.
func applyAt(node any, tokens []string, op string, value any) (any, error) {
	if len(tokens) == 0 {
		if op == opRemove {
			return nil, fmt.Errorf("cannot remove the document root")
		}
		return value, nil
	}
	switch n := node.(type) {
	case map[string]any:
		return applyInObject(n, tokens, op, value)
	case []any:
		return applyInArray(n, tokens, op, value)
	default:
		return nil, fmt.Errorf("cannot descend into %T at %q", node, tokens[0])
	}
}

// applyInObject handles the object half of applyAt.
func applyInObject(n map[string]any, tokens []string, op string, value any) (any, error) {
	key := tokens[0]
	if len(tokens) > 1 {
		child, ok := n[key]
		if !ok {
			return nil, fmt.Errorf("path segment %q is absent", key)
		}
		next, err := applyAt(child, tokens[1:], op, value)
		if err != nil {
			return nil, err
		}
		n[key] = next
		return n, nil
	}
	switch op {
	case opAdd, opReplace:
		n[key] = value
	case opRemove:
		if _, ok := n[key]; !ok {
			return nil, fmt.Errorf("member %q is absent", key)
		}
		delete(n, key)
	default:
		return nil, fmt.Errorf("unsupported operation %q", op)
	}
	return n, nil
}

// applyInArray handles the array half of applyAt, including the "-" append token.
func applyInArray(n []any, tokens []string, op string, value any) (any, error) {
	token := tokens[0]
	if token == "-" {
		if len(tokens) > 1 || op != opAdd {
			return nil, fmt.Errorf(`"-" addresses the position past the end and only "add" may use it`)
		}
		return append(n, value), nil
	}
	idx, err := strconv.Atoi(token)
	if err != nil {
		return nil, fmt.Errorf("array index %q is not a number", token)
	}
	// The position one past the end addresses only an append, and only for "add";
	// every other operation needs an element that is already there.
	appending := op == opAdd && len(tokens) == 1
	if idx < 0 || idx > len(n) || (idx == len(n) && !appending) {
		return nil, fmt.Errorf("array index %d is out of range for a %d-element array", idx, len(n))
	}
	if len(tokens) > 1 {
		next, err := applyAt(n[idx], tokens[1:], op, value)
		if err != nil {
			return nil, err
		}
		n[idx] = next
		return n, nil
	}
	switch op {
	case opAdd:
		return slices.Insert(n, idx, value), nil
	case opReplace:
		n[idx] = value
		return n, nil
	case opRemove:
		return slices.Delete(n, idx, idx+1), nil
	default:
		return nil, fmt.Errorf("unsupported operation %q", op)
	}
}

// ---------------------------------------------------------------------------
// The iterator
// ---------------------------------------------------------------------------

// sliceIterator streams a materialized result set, optionally breaking part-way
// through.
//
// A real backend streams from its storage and this one does not, which is fine: the
// obligations the suite checks are about what an iterator *promises* — that Err
// reports a failure rather than letting the loop end quietly, that Close is safe at
// any point and safe to repeat, and that a change handed out is the caller's to
// keep.
type sliceIterator struct {
	changes []query.Change
	fault   *StreamFault

	index  int
	cur    query.Change
	err    error
	closed bool
}

func newSliceIterator(changes []query.Change, fault *StreamFault) *sliceIterator {
	return &sliceIterator{changes: changes, fault: fault}
}

func (it *sliceIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.fault != nil && it.index >= it.fault.AfterChanges {
		it.err = it.fault.Err
		return false
	}
	if it.index >= len(it.changes) {
		return false
	}
	it.cur = copyChange(it.changes[it.index])
	it.index++
	return true
}

func (it *sliceIterator) Change() query.Change { return it.cur }

func (it *sliceIterator) Err() error { return it.err }

func (it *sliceIterator) Close() error {
	it.closed = true
	return nil
}

// copyChange hands out a change nothing will overwrite.
//
// The contract is explicit that the caller owns everything reachable from a change,
// and it is explicit because the alternative is quiet: a caller appending rows to a
// slice is doing an ordinary thing, and a recycled labels map would give every row
// the last row's labels with nothing anywhere reporting an error.
func copyChange(c query.Change) query.Change {
	c.Actors = slices.Clone(c.Actors)
	c.Labels = maps.Clone(c.Labels)
	return c
}
