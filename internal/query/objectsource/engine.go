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

// The read-plane engine over the jsonl-v1 object archive: the half of this
// package that answers questions, on top of the half that fetches bytes.
//
// # Why this backend exists
//
// It removes the database from the evaluation path. An archive synced to a laptop,
// a mounted volume or a bucket is enough to answer what changed, who changed it
// and what the object looked like — with no server to run, no schema to migrate
// and no credential beyond the one that reads the archive. That is the whole of
// the zero-infrastructure story, and it is pure Go by decision (D18): no cgo, no
// embedded database, because the static cross-compile is what makes the
// command-line client six archives from one build.
//
// # What it costs, stated rather than hidden
//
// There is no index. One object's history is not seekable, so a question about one
// object costs the partitions its window lands in — every object in them, listed,
// fetched and decompressed. The engine declares that (PointQuery false,
// TimeBoundRequired true) rather than presenting itself as equivalent to an
// indexed store, and it exposes the scan's size up front (query.ScanEstimator) so
// a caller can show the trade instead of appearing to hang. Wide analytics belong
// in the documented recipes; this engine answers narrow questions honestly.
//
// # What it cannot answer, and why that is not a bug
//
// An archive of this format never receives a deletion (D12), so Capabilities
// reports Deletions false. That is the truthful declaration and it is the whole
// reason the flag exists: history with no deletions in it is indistinguishable
// from history of a cluster where nothing was deleted, and a renderer must be told
// which it is holding. This engine never synthesizes a Deleted row to close a
// timeline that merely ended (Invariant 4).

package objectsource

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Compile-time proof that this backend satisfies the read-plane contract and its
// optional estimating half, asserted where the implementation lives rather than at
// wiring time.
var (
	_ query.QueryEngine   = (*Engine)(nil)
	_ query.ScanEstimator = (*Engine)(nil)
)

// backendName is what this engine calls itself in structured output.
//
// It is surfaced as metadata.backend so a scripted consumer can attribute a result
// to the engine that produced it. It names the *seam* rather than a storage
// technology, and deliberately so: one engine answers from a bucket and from a
// directory with identical code, and a name that picked one of them would be wrong
// half the time it was read. People pin scripts to it, so a release must not
// rename it.
const backendName = "objectsource"

// The defaults, each with a consequence worth reading before it is changed.
const (
	// DefaultObjectSpan is how far past its own partition an object is assumed to
	// carry records, and therefore how far below a query's lower bound the
	// partition range is widened (see partitionPrefixes).
	//
	// It defaults to an hour because an hour is the *ceiling* the writer's
	// configuration allows, which makes the default correct against any legally
	// configured archive with nothing to configure here. The cost is one extra
	// hour partition at the bottom edge of a window; the alternative — defaulting
	// to the writer's own default and being wrong for anything else — would drop
	// records that exist, in the one direction nobody would think to check.
	DefaultObjectSpan = time.Hour

	// NoObjectSpan disables the widening. It is spelled out because "no widening"
	// is a claim about how the archive was written, not a default anybody should
	// arrive at by leaving a field zero.
	NoObjectSpan = -time.Nanosecond

	// DefaultConcurrency is how many objects are fetched at once.
	//
	// Eight is chosen against a remote store, where a scan is latency-bound and
	// serial fetching is the whole cost: it is enough to keep several requests in
	// flight without turning one query into a burst that a shared bucket notices.
	// Peak memory is proportional to it, not to the archive, which is what makes it
	// a number a person can reason about.
	DefaultConcurrency = 8

	// DefaultStateLookback bounds how far back a state reconstruction walks
	// looking for a full-state row to replay from.
	//
	// It exists because StateAt carries an instant and no window, so without a
	// bound "this object was never recorded" and "this archive is empty" would both
	// be answered by walking to the beginning of time. Thirty days is comfortably
	// past any checkpoint cadence, and a reconstruction that exhausts it says so —
	// it does not report the object as absent.
	DefaultStateLookback = 30 * 24 * time.Hour
)

// Options configures an engine. The zero value is usable for an archive written to
// the root of its store by a legally configured writer.
type Options struct {
	// Prefix is the archive's own key prefix — the sink's spec.prefix — without a
	// leading or trailing slash. Empty is an ordinary configuration: a store
	// dedicated to one archive.
	Prefix string

	// Concurrency caps how many objects are fetched and decoded at once. Zero
	// selects DefaultConcurrency.
	Concurrency int

	// ObjectSpan is how far past its own partition an object may carry records —
	// the writer's maxObjectAge. Zero selects DefaultObjectSpan; a negative value
	// (NoObjectSpan) disables the widening.
	//
	// A caller that can read the sink's configuration should pass its actual value:
	// it is the difference between listing one extra partition and listing an
	// unnecessary one.
	ObjectSpan time.Duration

	// StateLookback bounds the backward walk StateAt performs. Zero selects
	// DefaultStateLookback.
	StateLookback time.Duration
}

// Engine answers the read-plane contract over one archive.
//
// It is safe for use by one caller at a time, which is the contract's own rule.
// The source underneath it is safe for concurrent use and is used that way — a
// scan lists and fetches from several goroutines at once — but two concurrent
// queries against one engine are not promised anything, and nothing needs them to
// be.
type Engine struct {
	src           ObjectSource
	prefix        string
	concurrency   int
	objectSpan    time.Duration
	stateLookback time.Duration

	// closed makes Close idempotent and gives a use-after-close a stated error.
	// It is atomic because the documented, ordinary shape is a caller that both
	// defers a Close and calls one explicitly.
	closed atomic.Bool
}

// NewEngine builds an engine over a source the caller has already opened and still
// owns.
//
// It does not resolve where the archive lives, and takes no endpoint, credential
// or directory: that is the contract's rule (see query.QueryEngine), and it is what
// keeps "which archive is this?" a question for the command-line client — which has
// a flag set and a sink to answer it with — rather than for the query semantics.
//
// It follows that Close never closes src. The caller built it and may well be using
// it for something else; an engine that closed a source it was lent would break
// whatever else held it, at a distance, for a reason nothing in the call names.
//
// The name is NewEngine rather than New because this package also constructs
// sources (NewLocal), and one of the two would otherwise be the unnamed default —
// which for a package holding both a seam and a backend is a coin toss a reader
// should not have to make at the call site.
func NewEngine(src ObjectSource, opts Options) (*Engine, error) {
	if src == nil {
		return nil, errors.New("objectsource query engine: a source is required; resolving where the " +
			"archive lives belongs to the caller, so there is nothing for this package to open")
	}

	e := &Engine{
		src:           src,
		prefix:        opts.Prefix,
		concurrency:   opts.Concurrency,
		objectSpan:    opts.ObjectSpan,
		stateLookback: opts.StateLookback,
	}
	if e.concurrency <= 0 {
		e.concurrency = DefaultConcurrency
	}
	switch {
	case e.objectSpan == 0:
		e.objectSpan = DefaultObjectSpan
	case e.objectSpan < 0:
		e.objectSpan = 0
	}
	if e.stateLookback <= 0 {
		e.stateLookback = DefaultStateLookback
	}
	return e, nil
}

// Capabilities reports what this engine can answer. No round trip, no failure, and
// the same value for the engine's lifetime — a caller reads it while composing a
// query and again while rendering the result, so a set that changed in between
// would print a notice contradicting the data beside it.
//
// Deletions is false, and it is the truthful declaration rather than a limitation
// worth apologising for. This archive is written by a Writer-only tier that never
// receives a deletion (D12), so a timeline that simply stops must be rendered with
// an explicit notice that the object may have been deleted without the deletion
// ever being recorded, pointing the reader at Coverage instead.
//
// ServerSideFilter is false: there is nothing on the other side of the seam to push
// a predicate into. Every predicate is applied to lines already decoded — which is
// why the contract requires a filtered result to be identical either way, and why
// the predicates themselves are the contract's (query.MatchesActors,
// query.MatchesFieldPaths) rather than this package's reading of them.
//
// PointQuery is false: there is no index, so one object's history costs the
// partitions its window lands in. A caller renders an estimate and offers a
// circuit breaker instead of pretending the cost is a lookup.
//
// TimeBoundRequired is true: without a window there is no partition range, and an
// unbounded scan of an archive is indistinguishable from a hang. Refusing up front
// with a sentinel a caller can turn into "name the flag that fixes it" is the
// kinder outcome.
func (e *Engine) Capabilities() query.Capabilities {
	return query.Capabilities{
		Backend:           backendName,
		Deletions:         false,
		ServerSideFilter:  false,
		PointQuery:        false,
		TimeBoundRequired: true,
	}
}

// Close releases what this engine created, which is nothing: a scan joins its
// fetches before it returns, and the source belongs to the caller.
//
// It is therefore a no-op that records having run, and is safe to call more than
// once. Saying so explicitly rather than leaving the method empty matters because
// the contract promises idempotence, and a promise nothing implements is one a
// later change can quietly break.
func (e *Engine) Close() error {
	e.closed.Store(true)
	return nil
}

// ensureOpen refuses a read issued after Close.
//
// The alternative is not "it works anyway" — it is that a use-after-close reaches
// a source the caller may since have closed too, and comes back as whatever that
// produces: a failure with no name and no obvious author.
func (e *Engine) ensureOpen() error {
	if e.closed.Load() {
		return errors.New("objectsource query engine: the engine is closed")
	}
	return nil
}

// requireWindow refuses a query this engine cannot bound.
//
// Both sides are required, not just one. A zero bound means unbounded on that side,
// and an unbounded side is a scan with no end: to the beginning of the archive, or
// to whatever partition the clock is in and every empty one before it. The contract
// gives this its own sentinel precisely so a caller can turn the refusal into a
// message naming the flag that fixes it, which is more use than a scan nobody can
// tell from a hang.
func requireWindow(from, to time.Time) error {
	switch {
	case from.IsZero() && to.IsZero():
		return fmt.Errorf("%w: this archive has no index, so a query with neither a start nor an end "+
			"would scan every partition it holds", query.ErrTimeBoundRequired)
	case from.IsZero():
		return fmt.Errorf("%w: the query ends at %s but has no start, and this archive has no index to "+
			"walk backwards from", query.ErrTimeBoundRequired, formatInstant(to))
	case to.IsZero():
		return fmt.Errorf("%w: the query starts at %s but has no end, so its partition range would run "+
			"to whichever partition the clock is in", query.ErrTimeBoundRequired, formatInstant(from))
	case to.Before(from):
		return fmt.Errorf("%w: the query ends at %s, before it starts at %s",
			query.ErrTimeBoundRequired, formatInstant(to), formatInstant(from))
	default:
		return nil
	}
}

// describeRef renders an identity for an error message: enough to find the object
// without pasting a struct into the output.
func describeRef(ref query.ObjectRef) string {
	group := ref.APIGroup
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s %s/%s", group, ref.Kind, ref.Namespace, ref.Name)
}

// formatInstant renders a timestamp for an error message at the precision the
// format records it.
func formatInstant(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
