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

// Package query defines the read plane: the questions a human asks of recorded
// history, and the contract a storage backend satisfies in order to be asked
// them. It is the seam the command-line client is written against, so that the
// client is a consumer of the frozen schema rather than of any one backend.
//
// # Why this is not an extension of the write-side reader
//
// The sink contract already has a read half, and growing it to serve this would
// have been the smaller diff and the worse design (D16). That reader exists for
// two narrow operator needs — warming a dedup cache at start-up, and deciding
// whether an object missing from the cluster is a genuine deletion — and it is
// reachable from the hot path's dependency graph. Analyst queries have opposite
// pressures: they stream unbounded result sets, they reconstruct state by
// replaying patches, and they must report what they *cannot* answer. Putting
// both behind one interface would mean every conformance property had to be
// qualified by which caller it applied to, and the record-writing path would
// carry a dependency on types it never uses.
//
// So the two contracts stay apart, and this package holds only the read side.
//
// # No backend is named here
//
// Nothing in this package refers to a storage technology, by identifier or by
// comment. That is enforced by a test rather than by discipline, because the
// moment one backend's vocabulary leaks into the contract, the contract stops
// being the thing every backend is measured against and quietly becomes a
// description of whichever one was written first.
//
// # Honesty about what a backend cannot do
//
// Backends differ in what their storage can express, and those differences are
// declared through [Capabilities] rather than discovered by a caller getting a
// surprising answer. An engine that cannot record deletions must say so; it
// must never fabricate one, and the renderer must never let its silence read as
// "nothing was deleted" (Invariant 4). An empty result is likewise never
// self-explanatory: "nothing changed" and "nothing was watching" are different
// facts, and [QueryEngine.Coverage] is how a caller tells them apart
// (Invariant 9).
package query

import (
	"context"
	"time"
)

// QueryEngine answers questions about recorded history for one cluster's worth
// of data in one storage backend.
//
// Implementations are constructed by their own packages with an already-configured
// connection or object source: this contract deliberately says nothing about
// dialing, credentials or endpoints, so that resolving *where* the data lives
// stays a concern of the command-line client and never of the query semantics.
//
// An engine is safe for use by one caller at a time unless its own documentation
// promises more. Iterators returned by [QueryEngine.Timeline] are not safe for
// concurrent use at all.
//
// The name repeats the package's, which Go style would normally discourage. It is
// kept because QueryEngine is what the architecture decision record and every
// task in this phase call the contract, and code that disagrees with the record
// about the name of its own central abstraction costs more than the stutter does.
type QueryEngine interface {
	// Timeline streams the recorded changes matching q as a cursor.
	//
	// It is a cursor and not a slice because an object that has flapped for a
	// week can hold six figures of changes, and materializing all of them to
	// render the last twenty would make the cost of the flagship command a
	// function of the object's entire history rather than of the window that was
	// asked for.
	//
	// Changes are emitted in ts order, or reverse ts order when q.Reverse is set,
	// with the nanosecond precision the schema records — two changes one
	// microsecond apart must not collapse into an arbitrary order.
	//
	// The caller owns the returned iterator and must Close it on every path,
	// including an early break out of the Next loop.
	//
	// Errors: ErrTimeBoundRequired when Capabilities().TimeBoundRequired is set
	// and q carries no time bound; ErrNoCoverage when the engine can *prove* that
	// no watch scope ever covered q.Ref. An engine that cannot prove it returns an
	// iterator that yields nothing and leaves the distinction to Coverage — an
	// empty iterator is never on its own a statement that nothing happened
	// (Invariant 9).
	Timeline(ctx context.Context, q TimelineQuery) (ChangeIterator, error)

	// StateAt reconstructs what ref looked like at the instant at, by finding the
	// newest data-bearing row at or before at and replaying the patches recorded
	// after it. The procedure is the one docs/SCHEMA.md specifies, and the
	// returned Reconstruction reports which base row was used and how many
	// patches were applied so that a reader can audit the answer rather than
	// trust it.
	//
	// uid pins the reconstruction to one incarnation. Empty means the newest
	// incarnation alive at or before at — never a blend of two, since a
	// (namespace, name) pair may span several UIDs and splicing them together
	// would reconstruct an object that never existed (Invariant 7).
	//
	// Errors: ErrObjectNotFound when the object did not exist at at — never
	// observed, or terminally deleted for the incarnation in question — and,
	// wrapped, when history holds rows for it but no data-bearing row to start
	// from, which means the base predates the retention window rather than that
	// the object was absent. ErrCapabilityUnsupported when the engine cannot
	// reconstruct state at all.
	StateAt(ctx context.Context, ref ObjectRef, at time.Time, uid string) (*Reconstruction, error)

	// Coverage returns the intervals during which the scopes matching q were
	// actually being watched, oldest first, with a still-open interval carrying a
	// nil To.
	//
	// This is the mechanism behind Invariant 9 and the reason an empty timeline is
	// explicable. A caller that renders "no changes" without consulting it cannot
	// tell an object that sat untouched from an object nobody was watching, and
	// those two answers lead an engineer at 02:47 in opposite directions.
	//
	// Errors: ErrCapabilityUnsupported when the engine has no scope log to read.
	// An engine with a scope log that holds no matching interval returns an empty
	// slice and a nil error — that is the "nothing was watching" answer, which is
	// a result and not a failure.
	Coverage(ctx context.Context, q ScopeQuery) ([]ScopeInterval, error)

	// Incarnations lists the distinct UIDs recorded for ref within [from, to],
	// oldest first by FirstSeen.
	//
	// It exists so that a caller can say "there are two other incarnations of this
	// name" before rendering one of them. A timeline that silently splices two
	// incarnations is worse than no timeline (Invariant 7), and a timeline that
	// shows one without admitting the others is the same mistake told quietly.
	//
	// A zero from or to means unbounded on that side, subject to the same
	// ErrTimeBoundRequired rule as Timeline.
	Incarnations(ctx context.Context, ref ObjectRef, from, to time.Time) ([]Incarnation, error)

	// Capabilities reports what this engine can and cannot answer. It must not
	// perform a round trip, must not fail, and must return the same value for the
	// engine's whole lifetime: callers consult it while composing a query and
	// again while rendering the result, and a capability set that changed in
	// between would make the notice printed to the user disagree with the data
	// beside it.
	Capabilities() Capabilities

	// Close releases the engine's resources. It does not close a connection the
	// caller supplied and still owns; what it releases is whatever the engine
	// itself created. Calling it more than once is safe.
	Close() error
}

// Capabilities is a backend's own declaration of what its storage can express.
//
// Every field below has a rendering consequence, and that is the point: a
// capability nobody acts on is a comment. Declaration is checked against detected
// behaviour by the conformance suite, in both directions, so a backend cannot
// claim a capability it lacks or quietly gain one it never declared.
type Capabilities struct {
	// Backend is a short, stable identifier for the implementation, surfaced as
	// metadata.backend in structured output so a scripted consumer can tell which
	// engine produced a result — and, when two answers disagree, which one to
	// trust for the question asked.
	//
	// It is stable in the sense that a release must not rename it: people pin
	// scripts to it. Conventionally the storage technology's own lowercase name.
	Backend string `json:"backend"`

	// Deletions reports whether this engine's history can contain Deleted rows at
	// all.
	//
	// Rendering consequence: when false, a timeline that simply stops must carry
	// an explicit notice that the object may have been deleted without the
	// deletion ever being recorded, pointing the reader at coverage instead. A
	// renderer must never synthesize a Deleted row to close the gap (Invariant 4).
	// History with no deletions in it is indistinguishable from history of a
	// cluster where nothing was deleted, and this flag is the only thing that
	// tells them apart.
	Deletions bool `json:"deletions"`

	// ServerSideFilter reports whether actor and field-path predicates are pushed
	// into the backend rather than applied to rows the engine has already read.
	//
	// Rendering consequence: none whatsoever on the content. A filtered result is
	// required to be identical either way, which is exactly the agreement property
	// the conformance suite pins, because the alternative is two backends
	// answering the same question differently and neither of them being wrong
	// about its own implementation. The consequence is on *cost*: when false,
	// TimelineQuery.Limit does not bound the work performed, so a caller warns
	// before a wide window and reports scan progress rather than appearing hung.
	ServerSideFilter bool `json:"server_side_filter"`

	// PointQuery reports whether the engine can seek to one object's history
	// without reading the window around it.
	//
	// Rendering consequence: when false, a single-object query costs the whole
	// partition range it lands in, so a caller renders a scan estimate and asks
	// for confirmation before starting, and offers a circuit breaker. Making that
	// trade visible is what keeps a zero-infrastructure deployment honest instead
	// of merely slow.
	PointQuery bool `json:"point_query"`

	// TimeBoundRequired reports whether every query must carry a time bound.
	//
	// Rendering consequence: a caller supplies its default window rather than
	// issuing an unbounded query, and an explicitly unbounded request is refused
	// up front with ErrTimeBoundRequired and a message naming the flag that fixes
	// it — rather than being started and never finishing, which is the same
	// outcome with none of the explanation.
	TimeBoundRequired bool `json:"time_bound_required"`
}
