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

// Package clickhouse answers the read-plane contract from the frozen v1 schema.
//
// It is the first query backend, and it is deliberately a *reader* of two tables
// rather than a client of the operator that writes them. Nothing here imports the
// sink, the pipeline or the watch manager: the schema is the public API between
// the two halves (Task 2.6), and coupling the read plane to the write plane's
// runtime would make every refactor of the hot path a release of the query path.
//
// # Why every read of resource_states carries FINAL
//
// resource_states is a ReplacingMergeTree, and the operator's write path is
// at-least-once. A lost acknowledgement after a successful insert makes the
// poison-isolation path re-insert a byte-identical row, which collides on the
// full sort key and is collapsed — but only on merge, which happens when it
// happens. Until then the table really does hold the row twice.
//
// A naive SELECT would therefore render one change as two in an audit timeline:
// the same scale-down, at the same nanosecond, twice, with nothing to say the
// cluster did not do it twice. That is not a cosmetic defect and it is not an
// optimization to fix it later; it is the difference between a record and a
// plausible-looking fiction. So FINAL is on every read of that table, without
// exception, and a test enumerates the statements this package emits to say so.
//
// watch_scopes is a plain MergeTree whose rows are written once each. There is
// nothing to collapse there, and FINAL on it would be a cost with no return.
//
// # What is pushed down and what is not
//
// The identity, the time bound, the incarnation and the actor predicates are all
// pushed into WHERE, which is what makes a single-object timeline a sorted-range
// read against the (cluster_id, api_group, kind, namespace, name, ts) sort key
// however large the table has grown.
//
// Field-path predicates are applied to rows already read, through the contract's
// own query.MatchesFieldPaths, and that is a considered exception rather than an
// omission — see timelineStatement on why the SQL form would be brittle and why
// the scan is identical either way. The conformance suite's agreement property is
// what proves the two paths cannot disagree about the answer.
package clickhouse

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Compile-time proof that this backend still satisfies the read-plane contract,
// asserted where the implementation lives rather than at wiring time.
var _ query.QueryEngine = (*Engine)(nil)

// BackendName is what this engine calls itself in structured output.
//
// It is surfaced as metadata.backend so a scripted consumer can attribute a
// result to the engine that produced it — and, when two backends disagree, decide
// which to trust for the question asked. People pin scripts to it, so a release
// must not rename it.
//
// It is exported because `kuberecord version` lists the engines a binary was
// built with (Task 12.1). A CLI that spelled the name a second time would
// eventually spell it differently from what metadata.backend carries, and the
// reader comparing the two would have no way to tell which was the typo.
const BackendName = "clickhouse"

// Engine answers the read-plane contract over one already-configured ClickHouse
// connection.
//
// It is safe for use by one caller at a time. The iterators it returns are not
// safe for concurrent use at all, which is the contract's own rule and not an
// extra restriction: an iterator holds driver rows, and driver rows are a cursor
// over one query.
type Engine struct {
	conn driver.Conn

	// ownsConn records that this engine dialled the connection itself and must
	// therefore close it. It is false for every engine built by New and true only
	// for one built by Dial, which is what keeps the single sentence in the
	// contract — Close releases what the engine itself created — true of both.
	ownsConn bool

	// closed makes Close idempotent. It is atomic rather than mutex-guarded
	// because it is read on no path but Close itself; what it buys is that a
	// caller which both defers a Close and calls one explicitly — the documented,
	// ordinary shape — is not punished for it.
	closed atomic.Bool
}

// New builds an engine over a connection the caller has already opened and still
// owns.
//
// It does not dial, and it takes no endpoint, credential or timeout. That is the
// contract's rule (see query.QueryEngine) and it is what keeps the question
// "where does this cluster's history live?" a concern of the command-line client
// — which has a kubeconfig, a sink CR and a flag set to answer it with — rather
// than of the query semantics, which have no business knowing.
//
// It follows that Close never closes conn. The caller opened it and may well be
// using it for something else; an engine that closed a connection it was lent
// would break whatever else held it, at a distance, for a reason nothing in the
// call names.
func New(conn driver.Conn) (*Engine, error) {
	if conn == nil {
		return nil, errors.New("clickhouse query engine: a connection is required; construction and " +
			"credential handling belong to the caller, so there is nothing for this package to dial")
	}
	return &Engine{conn: conn}, nil
}

// Capabilities reports what this engine can answer. No round trip, no failure,
// and the same value for the engine's lifetime — a caller reads it while
// composing a query and again while rendering the result, so a set that changed
// in between would print a notice contradicting the data beside it.
//
// Deletions is true: the schema records a Deleted row for a live delete, a
// reincarnation close-out and a startup GC alike, so a timeline that simply stops
// really does mean the object was not deleted while it was being watched.
//
// ServerSideFilter is true, with one exception stated plainly here rather than
// left to be discovered: field-path predicates are applied to rows already read.
// The flag's documented consequence is on *cost*, and the cost is unaffected —
// Timeline returns the diff column on every row regardless, so filtering on it
// server-side would read the same bytes and merely transfer fewer. The
// predicates that decide how much of the table is *scanned* — identity, window,
// incarnation, actors — are all pushed down.
//
// PointQuery is true: the sort key leads with the identity tuple, so one object's
// history is a contiguous range rather than a scan of the window it lands in.
//
// TimeBoundRequired is false: an unbounded query is a range read over one
// object's rows, not a scan of the table, so refusing it would deny a caller an
// answer this engine can give cheaply.
func (e *Engine) Capabilities() query.Capabilities {
	return query.Capabilities{
		Backend:           BackendName,
		Deletions:         true,
		ServerSideFilter:  true,
		PointQuery:        true,
		TimeBoundRequired: false,
	}
}

// Close releases what this engine created.
//
// For an engine built by New that is nothing: iterators own the driver rows they
// stream, and the connection belongs to the caller — closing a connection it was
// lent would break whatever else held it, at a distance, for a reason nothing in
// the call names. For an engine built by Dial it is the connection, because
// nobody else has a reference with which to close it.
//
// It is idempotent either way, which is the contract's own promise and the reason
// the flag is flipped by a compare-and-swap rather than a store: the documented,
// ordinary shape is a caller that both defers a Close and calls one explicitly,
// and a dialled engine must not hand the driver a second Close for that.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	if !e.ownsConn {
		return nil
	}
	if err := e.conn.Close(); err != nil {
		return fmt.Errorf("clickhouse query engine: closing the connection it dialled: %w", err)
	}
	return nil
}

// ensureOpen refuses a read issued after Close.
//
// The alternative is not "it works anyway" — it is that a use-after-close reaches
// the driver and comes back as whatever that connection's state happens to
// produce, which for a caller is a failure with no name and no obvious author. A
// closed engine is a bug in the caller, and this is what makes it say so.
func (e *Engine) ensureOpen() error {
	if e.closed.Load() {
		return errors.New("clickhouse query engine: the engine is closed")
	}
	return nil
}
