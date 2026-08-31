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

// Package conformance is the executable form of the read-plane contract: the
// properties every query backend must uphold, written against internal/query
// alone so that a backend passes or fails them on its own merits.
//
// # Why it exists before the second backend
//
// With one implementation the properties live in that implementation's tests,
// which is merely redundant. With two, the second backend re-derives them from
// the prose in query.go and gets one wrong — and the one most likely to be got
// wrong here is per-incarnation handling, because it is the property whose
// failure still produces a plausible-looking answer. A timeline that splices two
// UIDs under one (namespace, name) reads as a coherent account of an object's
// life; it is an account of something that never happened, and nothing in the
// output says so (Invariant 7). That failure mode is why this package is a gate
// and not a convenience.
//
// A backend adopts it from its own test package:
//
//	func TestQueryConformance(t *testing.T) {
//	    conformance.RunQuerySuite(t, func(t *testing.T) conformance.Harness {
//	        return conformance.Harness{Engine: ..., Seed: ..., ...}
//	    })
//	}
//
// # What the suite owns and what the harness owns
//
// The suite owns the questions and the expected answers: it composes a known
// history, hands it to the harness to store, and asserts on what the engine reads
// back. The harness owns everything backend-specific — how history is written,
// how the stream is broken, and what the backend declares itself capable of.
//
// The declaration is mandatory and is checked in both directions, exactly as the
// sink suite checks its own (Task 5.4). A backend that cannot record deletions is
// a legitimate design (D12), but an omission nobody wrote down is one the suite
// cannot tell from an oversight, and a badge nobody can interpret is worse than no
// badge (D11). See capabilities.go for the mechanism and for which flags are
// checked against observed behaviour rather than only against the engine's own
// report.
//
// # Dependency budget
//
// internal/query and the standard library. No backend, no internal/sink, and no
// third-party test helper — a suite that reached for one of those would be
// measuring backends against something other than the contract. deps_test.go
// enforces it rather than leaving it to review.
package conformance

import (
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Row is one recorded change together with the identity it belongs to.
//
// The pairing is necessary because query.Change deliberately carries no identity
// columns: an iterator's rows all describe the object the caller asked for, so
// repeating its name on every row would be noise in the contract people script
// against. A seed, by contrast, plants several objects and several incarnations at
// once, so it has to say which is which.
type Row struct {
	// Ref is the canonical identity this change belongs to.
	Ref query.ObjectRef
	// Change is the recorded transition, exactly as the engine must read it back.
	Change query.Change
}

// ScopeAction is a watch-scope transition's direction, spelled as the scope log's
// own action column spells it.
type ScopeAction string

const (
	// ScopeStarted opens a scope: the recorder began watching it.
	ScopeStarted ScopeAction = "Started"
	// ScopeStopped closes one. It says the recorder stopped watching, and
	// emphatically not that the objects in the scope were deleted.
	ScopeStopped ScopeAction = "Stopped"
)

// ScopeTransition is one entry in a seeded watch-scope log.
//
// History seeds transitions rather than finished intervals on purpose. Pairing a
// Stopped to its Started, and leaving an unmatched trailing Started open, is the
// work Coverage actually has to do; a harness handed finished intervals would be
// asserting that it can hand them back, which is a property of the harness and not
// of the backend.
type ScopeTransition struct {
	// Action is ScopeStarted or ScopeStopped.
	Action ScopeAction
	// APIGroup is the group watched; empty is the core group, as a value rather
	// than a wildcard.
	APIGroup string
	// Kind is the kind watched.
	Kind string
	// Namespace is the namespace watched, with the scope log's own reading: empty
	// is the all-namespaces scope itself.
	Namespace string
	// RuleRef names the rule that opened or closed the scope.
	RuleRef string
	// TS is when the transition happened.
	TS time.Time
}

// History is the known past a property asserts against: what was recorded, and
// what was being watched while it was recorded.
//
// Both halves travel together because half the read plane's obligations are about
// their relationship. An empty timeline is only explicable next to coverage, and a
// suite that could seed one without the other could not pose the question
// Invariant 9 exists to answer.
type History struct {
	// Rows are the recorded changes, in no particular order — a backend is free to
	// store them however it likes, and the engine is what must impose ts order on
	// the way out.
	Rows []Row
	// Scopes are the watch-scope transitions, likewise unordered.
	Scopes []ScopeTransition
}

// StreamFault breaks the backend's next Timeline part-way through: the iterator
// delivers AfterChanges changes and then fails with Err.
//
// It exists because the streaming contract's sharpest obligation — a backend that
// died halfway must surface through Err rather than end the loop quietly — has no
// natural occurrence a test can wait for. A caller that mistakes a truncated
// stream for a complete one renders a partial audit history as a whole one, which
// for the flagship command is the worst available outcome: the row that would have
// explained the outage is missing and nothing says a row is missing (Invariant 4).
//
// AfterChanges of zero breaks the stream before its first change. That is still a
// failed read, but it is not the case the property is about, so the suite always
// asks for at least one change to be delivered first.
type StreamFault struct {
	// AfterChanges is how many changes the iterator delivers before it breaks.
	AfterChanges int
	// Err is the failure Err must report. The suite matches on identity, so a
	// backend must surface this value — wrapped is fine, replaced is not.
	Err error
}

// Harness is everything the suite needs from a backend that the query.QueryEngine
// interface itself cannot express: a live engine, a way to plant a known past, a
// way to break the stream, and the declaration of what this backend can answer.
//
// It is a struct of fields rather than an interface so that a property added later
// can ask for a new lever without invalidating harnesses that predate it — the
// same reason the sink suite's is one.
//
// A fresh Harness is built per property, so no property inherits another's
// backend state, and the suite closes the engine when the property ends.
type Harness struct {
	// Engine is the live QueryEngine under test.
	//
	// It must already be connected to whatever storage Seed writes into. The suite
	// says nothing about dialling, credentials or endpoints — resolving where the
	// data lives is the command-line client's concern and never the query
	// semantics' (see query.QueryEngine).
	Engine query.QueryEngine

	// Seed makes History the backend's recorded past. The suite calls it exactly
	// once per property, before any query, and it must not return until the
	// history is readable through Engine — a backend with an asynchronous write
	// path waits here rather than leaving the suite to poll, because "not yet
	// visible" and "not stored" are indistinguishable from the read side and only
	// the harness can tell them apart.
	//
	// A row this backend cannot represent is dropped rather than refused: an
	// archive tier that never receives a deletion (D12) is not a broken harness,
	// and the capability declaration is what makes the drop legible. Refusing
	// would force the suite to compose a different history per backend, which is
	// the opposite of what a backend-agnostic suite is for.
	Seed func(History) error

	// SeedCorpus makes the shared agreement corpus this backend's recorded past,
	// through the same writing path an operator's records travel — rows into the
	// tables an engine reads, encoded artifacts into the store an engine lists.
	//
	// "Through the backend's own path" is the whole obligation, and it is the one
	// worth stating in the contract rather than leaving to each harness. A seeding
	// that assembled the backend's internal representation by hand would let the
	// agreement suite certify that two engines agree about a shape neither of them
	// actually stores — which is a stronger-sounding claim than the per-backend
	// suites already make, and a weaker one.
	//
	// Like Seed it must not return until the corpus is readable through Engine,
	// and it drops rather than refuses a record this backend cannot represent, for
	// the same reasons.
	//
	// It is mandatory. Made optional it would be declined by whichever backend
	// found it hardest to implement, which is the backend whose storage differs
	// most from the others' — that is, exactly the backend most likely to disagree
	// with them, and the only one an agreement test was ever needed for.
	SeedCorpus func(Corpus) error

	// SetStreamFault installs the fault the backend applies to its next Timeline;
	// nil clears it.
	//
	// It is mandatory rather than optional, and that is a deliberate cost imposed
	// on every harness. Timeline is the whole of the mandatory contract here, and
	// the property this lever serves is the one standing between a partial history
	// and a partial history that looks complete. Made optional, it would be
	// switched off by whichever backend found it hardest to inject — which is the
	// backend whose streaming is least likely to be right.
	SetStreamFault func(*StreamFault)

	// Capabilities is what this backend claims it can answer, and it is mandatory.
	//
	// Build it with DeclareCapabilities. The zero value is "nobody thought about
	// this" rather than "this backend declares nothing", and every property rejects
	// it: see capabilities.go for why the distinction cannot be collapsed.
	Capabilities CapabilitySet
}

// validate fails the property immediately, naming the field, when a harness is
// incomplete or its declaration disagrees with its engine.
//
// It is Fatalf rather than skip on purpose: a half-filled harness means the
// backend is not under test, and this package exists to stop that from reading as
// a pass.
func (h Harness) validate(t conformanceT) {
	t.Helper()
	switch {
	case h.Engine == nil:
		t.Fatalf("conformance: Harness.Engine is nil; the suite needs a live QueryEngine to ask questions of")
	case h.Seed == nil:
		t.Fatalf("conformance: Harness.Seed is nil; the suite cannot plant the history it asserts against")
	case h.SetStreamFault == nil:
		t.Fatalf("conformance: Harness.SetStreamFault is nil; the suite cannot break the stream, and the " +
			"property that stops a truncated history from reading as a complete one would certify nothing")
	case h.SeedCorpus == nil:
		t.Fatalf("conformance: Harness.SeedCorpus is nil; this backend cannot be seeded from the shared " +
			"corpus, so nothing anywhere checks that it agrees with any other backend about identical " +
			"history — only that it agrees with what its own harness wrote")
	}
	requireCapabilityAgreement(t, h)
}

// validateForAgreement is the narrower gate the agreement suite goes through.
//
// It asks for what that suite actually uses: a live engine, the shared seeding
// path, and a capability declaration the engine agrees with — since the whole of
// the agreement suite's reasoning about where two backends may legitimately differ
// is derived from that declaration.
//
// It deliberately does not require Seed or SetStreamFault. Those exist to serve the
// per-property suite, and demanding them here would mean a harness built to compare
// two live backends had to carry a fault injector it never uses — which is how a
// lever ends up implemented as a no-op that lies the first time somebody does use
// it.
func (h Harness) validateForAgreement(t conformanceT) {
	t.Helper()
	switch {
	case h.Engine == nil:
		t.Fatalf("conformance: Harness.Engine is nil; the agreement suite needs a live QueryEngine on " +
			"both sides to have anything to compare")
	case h.SeedCorpus == nil:
		t.Fatalf("conformance: Harness.SeedCorpus is nil; the agreement suite plants one corpus in both " +
			"backends and compares the answers, so a backend that cannot be seeded from it cannot take " +
			"part")
	}
	requireCapabilityAgreement(t, h)
}

// conformanceT is the slice of *testing.T the properties use.
//
// The indirection has one purpose, and it is the one this package insists on: it
// lets the suite be run against a recorder instead of a real test, so the package
// can prove that each property *fails* when handed an engine that violates it (see
// nonvacuity_test.go). A suite that asserts nothing passes everything, and without
// this seam there would be no way to tell the two apart from the inside.
//
// *testing.T satisfies it as-is; Fatalf must abandon the property (runtime.Goexit)
// in any other implementation, exactly as testing does.
type conformanceT interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// property is one named contract obligation and the code that checks it. The
// table is addressable by name so the non-vacuity tests can run a single property
// against a deliberately broken engine and assert it objects.
type property struct {
	name string
	run  func(t conformanceT, h Harness)
}

// queryProperties is the read-plane contract as executable obligations. Every
// backend must pass all of them; the list is the definition of "a QueryEngine",
// and a new obligation belongs here rather than in any one backend's tests.
//
// Nothing here is optional. Unlike the sink contract, which splits its optional
// halves into separate interfaces a backend may decline to implement, every engine
// implements the whole of QueryEngine — the differences between backends are
// declared as capability *values*, and the properties below consult those values
// rather than being skipped by them.
func queryProperties() []property {
	return []property{
		{name: propCapabilities, run: capabilitiesAreStable},
		{name: propOrderAscending, run: orderingAscending},
		{name: propOrderReverse, run: orderingReverse},
		{name: propOrderNanoseconds, run: orderingNanosecondPrecision},
		{name: propOrderLimit, run: orderingLimitTakesFromEmissionEnd},
		{name: propIncarnationNewest, run: incarnationNewestByDefault},
		{name: propIncarnationAll, run: incarnationAllIncarnations},
		{name: propIncarnationPinned, run: incarnationPinnedUID},
		{name: propIncarnationEnumerated, run: incarnationEnumerated},
		{name: propDeletionVisible, run: deletionVisibility},
		{name: propReconstructBase, run: reconstructionBase},
		{name: propReconstructCheckpoint, run: reconstructionCheckpointNotDoubleApplied},
		{name: propReconstructFidelity, run: reconstructionFidelity},
		{name: propReconstructPreHistory, run: reconstructionBeforeHistory},
		{name: propCoverageOpen, run: coverageOpenInterval},
		{name: propCoverageClosed, run: coverageClosedIntervalsInOrder},
		{name: propCoverageNamespace, run: coverageCoveringNamespace},
		{name: propTimeBounds, run: timeBoundsUnboundedQuery},
		{name: propStreamEarlyClose, run: streamingEarlyCloseDoesNotLeak},
		{name: propStreamMidError, run: streamingMidStreamErrorSurfaces},
		{name: propFilterActorInclude, run: filtersActorInclude},
		{name: propFilterActorExclude, run: filtersActorExclude},
		{name: propFilterFieldPaths, run: filtersFieldPaths},
		{name: propFilterAgreement, run: filtersAgreeWithNonPushdown},
	}
}

// propertyByName finds a property in the table. It exists for the non-vacuity
// tests, which run one property at a time against an engine built to violate it.
func propertyByName(name string) (property, bool) {
	for _, p := range queryProperties() {
		if p.name == name {
			return p, true
		}
	}
	return property{}, false
}

// runProperty is the single entry point both RunQuerySuite and the non-vacuity
// tests go through, so a property is validated identically whether a real backend
// or a broken fixture is on the other end.
//
// The engine is closed here rather than left to the harness because Close is part
// of the contract and a property that ended in a Fatalf would otherwise leak
// whatever the engine holds. The deferred close still runs: Fatalf abandons the
// property with runtime.Goexit, which runs deferred calls exactly as testing does.
func runProperty(t conformanceT, p property, h Harness) {
	t.Helper()
	h.validate(t)
	defer closeEngine(t, h.Engine)
	p.run(t, h)
}

// RunQuerySuite asserts every property of the read-plane contract against the
// backend newEngine builds, one separately named subtest per property.
//
// newEngine is called once per property with that subtest's *testing.T — never
// once for the whole suite — because every property plants its own history and
// closes the engine when it ends. Registering the backend's own teardown on the
// *testing.T it is handed is the harness's business; the suite's close is not a
// substitute for it.
func RunQuerySuite(t *testing.T, newEngine func(t *testing.T) Harness) {
	t.Helper()
	if newEngine == nil {
		t.Fatalf("conformance: RunQuerySuite needs a non-nil harness constructor")
	}
	for _, p := range queryProperties() {
		t.Run(p.name, func(t *testing.T) {
			runProperty(t, p, newEngine(t))
		})
	}
}
