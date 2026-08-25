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

// This file is the capability half of the suite: which optional contracts a
// backend *claims* to implement, which ones it actually implements, what happens
// when those two disagree, and the harness levers each half needs.
//
// The declaration is the mechanism; the skip is only the explanation. A backend
// that omits StateReader is a legitimate design (D12's S3 archive tier is exactly
// that), so the suite must not fail it — but an omission nobody is obliged to
// write down is indistinguishable from an oversight, and the whole point of D11
// is that a badge nobody can interpret is worse than no badge. So every harness
// states in code, reviewed in the PR, which optional halves its backend
// implements (Harness.Capabilities), and every optional suite compares that claim
// against the type assertion SinkManager.newLiveSink itself makes. Disagreement
// in either direction fails the build.
//
// It has to be a declaration rather than a louder skip, for two reasons.
//
// The first is that the channel a skip speaks on is muted in the command everyone
// runs. `go test` surfaces t.Logf output only for a test that failed, and a
// t.Skipf message only under -v; `make test` passes no -v, and adding it is not
// the fix — it would flood every run to make one line visible. So a message that
// named the interface, listed the properties that consequently certify nothing
// and stated what the runtime turns off would, in the default run, reach nobody
// at all. A skip cannot carry a guarantee it cannot deliver. Do not "simplify"
// the declaration away and put the guarantee back on that channel.
//
// The second is that a skip cannot see the failure mode that matters most. Two
// situations read identically from outside: a backend that genuinely cannot read
// its own history, and a backend whose reader exists but whose method set drifted
// out of the interface — a renamed method, a changed signature, a value receiver
// where a pointer is handed over. The second is a full-capability backend
// silently downgraded to Writer-only, because newLiveSink's assertion fails
// exactly as the suite's does and every test stays green. Comparing the claim
// against the assertion is what makes that drift unrepresentable.
//
// What is left for the skip is the courtesy explanation attached to a declared,
// deliberate omission: missingCapabilityMessage below still names the interface,
// the properties that certify nothing and the runtime consequence, for the reader
// who runs one suite under -v and wants to know what it did not do.

package conformance

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// ReadFault breaks the backend's next read part-way through: the read delivers
// AfterRows rows and then reports Err.
//
// It exists because the read contract's sharpest obligation — "a partial read
// must be reported as an error, never as a short-but-successful result" — has no
// natural occurrence a test can wait for. A caller that mistakes a truncated scan
// for a complete one warms its dedup cache from half the history and then
// suppresses every change to the objects it never learned about, which is the
// silent-audit-gap failure this package exists to prevent.
//
// AfterRows of zero breaks the read before its first row; that is still a failed
// read, but it is not the case the property is about, so the suite always asks
// for at least one row to be delivered first.
type ReadFault struct {
	// AfterRows is how many rows the read delivers before it breaks.
	AfterRows int
	// Err is the failure the broken read must report.
	Err error
}

// ProbeOutcome is the state the suite asks a backend to arrange for its next
// health probe. See Harness.SetProbeOutcome for why it is declared rather than
// injected as an error value.
type ProbeOutcome string

const (
	// ProbeHealthy is a reachable backend carrying the schema the operator writes
	// against.
	ProbeHealthy ProbeOutcome = "Healthy"
	// ProbeSchemaMismatch is a backend that answers but whose schema is not the
	// one the operator writes. It is the outcome that must be classifiable, since
	// it will not fix itself with time and needs a human.
	ProbeSchemaMismatch ProbeOutcome = "SchemaMismatch"
	// ProbeUnreachable is a backend that does not answer at all: refused, timed
	// out, or rejected the credentials.
	ProbeUnreachable ProbeOutcome = "Unreachable"
)

// Capability is one optional half of the sink contract, spelled as a Go reader
// meets it, so a declaration and a failure message can both be pasted into a grep
// and land on the interface they name.
//
// It is a named type rather than a bare string because its values are what a
// harness declares, and a plain string parameter would invite a backend to
// declare a name this suite has never heard of.
type Capability string

// The optional contracts a harness may declare. There are exactly three because
// these are the three interfaces SinkManager.newLiveSink asserts on, and a
// declaration is only worth anything when it is checked against an assertion the
// runtime really makes.
const (
	// CapStateReader is sink.StateReader: reading the backend's own history, which
	// is what cache warm-up, zombie garbage-collection and boot reconciliation are
	// built on.
	CapStateReader Capability = "sink.StateReader"
	// CapScopeEventWriter is sink.ScopeEventWriter: durably recording watch-scope
	// epochs, which is what keeps "we stopped watching" distinguishable from "it
	// was deleted".
	CapScopeEventWriter Capability = "sink.ScopeEventWriter"
	// CapProber is sink.Prober: answering a health and schema probe, which is what
	// puts a SchemaValid verdict on the sink's CR instead of discovering drift when
	// a write fails.
	CapProber Capability = "sink.Prober"
)

// capScopeEpochReads labels the read half's scope-epoch questions, which need a
// scope log to have been written before they can be asked. A backend could in
// principle answer them from history planted some other way, but none does: the
// two interfaces are two halves of one story, and a StateReader without a
// ScopeEventWriter has nothing to read.
//
// It is a label and not a Capability: a harness declares the interfaces its
// backend implements, never the suites those interfaces happen to satisfy, so
// this half's declaration is derived from the two real ones (optionalSuite.requires).
const capScopeEpochReads = string(CapStateReader) + " together with " + string(CapScopeEventWriter)

// declarableCapabilities is every Capability a harness may name, in the order a
// failure message lists them. It exists so "you declared something I do not
// recognise" can name the alternatives instead of making the reader find them.
func declarableCapabilities() []Capability {
	return []Capability{CapStateReader, CapScopeEventWriter, CapProber}
}

// CapabilitySet is a backend's declaration of which optional halves of the sink
// contract it implements: the claim each optional suite checks against what the
// backend turns out to be.
//
// The declaration is mandatory, and "none of them" has to be said out loud, so
// the type carries an explicit marker. The zero value is not the empty set — it
// is "nobody thought about this", and every suite rejects it, because a suite
// cannot tell that apart from D12's deliberate Writer-only archive tier. The
// marker is an unexported field only DeclareCapabilities sets, which is what
// makes an accidental or forged "declared" value unrepresentable outside this
// package: a backend cannot produce one with a struct literal, and nobody can
// drop the constructor call without a compile error.
type CapabilitySet struct {
	// declared separates DeclareCapabilities() — an explicit, reviewed "this
	// backend implements no optional half" — from a Harness that never mentioned
	// capabilities at all.
	declared bool
	caps     []Capability
}

// DeclareCapabilities is how a harness states which optional halves of the sink
// contract its backend implements.
//
// Call it with no arguments to declare a Writer-only backend (D12's archive
// tier). That is a legitimate design, and this is how it stays auditable: an
// omission that was declared can be reviewed in the PR that declares it, whereas
// an omission that was merely never mentioned is what the suite has no way to
// distinguish from a mistake.
func DeclareCapabilities(caps ...Capability) CapabilitySet {
	return CapabilitySet{declared: true, caps: slices.Clone(caps)}
}

// declares reports whether every capability in want was declared. It is "every"
// rather than "any" because one half — the scope-epoch reads — needs two
// interfaces at once, and a backend that declared only one of them has not
// declared that half.
func (c CapabilitySet) declares(want ...Capability) bool {
	for _, w := range want {
		if !slices.Contains(c.caps, w) {
			return false
		}
	}
	return true
}

// unknown returns the declared names that are not capabilities this suite knows,
// in declaration order.
//
// A misspelling would otherwise read as "not declared" and be reported as an
// implemented-but-undeclared capability: a correct rejection with a diagnosis
// that sends the reader looking for the wrong bug.
func (c CapabilitySet) unknown() []Capability {
	var bad []Capability
	for _, got := range c.caps {
		if !slices.Contains(declarableCapabilities(), got) {
			bad = append(bad, got)
		}
	}
	return bad
}

// joinCapabilities renders a capability list for a failure message, so every name
// in it can still be pasted into a grep.
func joinCapabilities(caps []Capability) string {
	names := make([]string, 0, len(caps))
	for _, c := range caps {
		names = append(names, string(c))
	}
	return strings.Join(names, ", ")
}

// optionalSuite is one optional half of the contract as the suite runs it: how to
// detect it, what to say when it is absent, and what to assert when it is there.
type optionalSuite struct {
	// group is the subtest the half's properties are nested under, so a skip is a
	// single line in the output rather than one per property.
	group string
	// capability is the interface, named exactly as it is declared.
	capability string
	// requires is the capabilities a harness must have declared for this half to
	// run. It is a set rather than a single name because one half — the scope-epoch
	// reads — is two interfaces at once, and there is no fourth declarable
	// capability standing for it: a harness declares the interfaces its backend
	// implements, never the suites those interfaces happen to satisfy.
	requires []Capability
	// consequence is what the operator loses by this backend not implementing it —
	// the half of a skip message that tells a reader whether to care.
	consequence string
	// implements reports whether this backend's Writer satisfies the half. It is
	// a type assertion, deliberately the same one SinkManager.newLiveSink makes.
	implements func(sink.Writer) bool
	properties func() []property
}

// stateReaderSuite is the read half's object-history properties: everything that
// can be asked of a StateReader without a scope log existing first.
func stateReaderSuite() optionalSuite {
	return optionalSuite{
		group:      "StateReader",
		capability: string(CapStateReader),
		requires:   []Capability{CapStateReader},
		consequence: "A Writer-only sink runs with cache warm-up, zombie garbage-collection and boot " +
			"reconciliation of scope epochs disabled, and tags every record as a permanent Snapshot (D12). " +
			"That degradation is legitimate, but it must be a declared capability limit rather than an " +
			"unnoticed one, so it is reported here instead of passing quietly.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.StateReader); return ok },
		properties: stateReaderProperties,
	}
}

// scopeEpochReadSuite is the read half's other two questions — was this scope
// open in a previous epoch, and which scopes did a previous process leave open —
// which can only be asked of a backend that also records scope epochs.
func scopeEpochReadSuite() optionalSuite {
	return optionalSuite{
		group:      "StateReaderScopeEpoch",
		capability: capScopeEpochReads,
		requires:   []Capability{CapStateReader, CapScopeEventWriter},
		consequence: "Without both halves there is no scope log to read back, so the epoch questions the " +
			"warm/GC coordinator asks of history cannot be answered and boot reconciliation is disabled " +
			"for this backend.",
		implements: func(w sink.Writer) bool {
			_, reads := w.(sink.StateReader)
			_, writes := w.(sink.ScopeEventWriter)
			return reads && writes
		},
		properties: scopeEpochReadProperties,
	}
}

func scopeEventWriterSuite() optionalSuite {
	return optionalSuite{
		group:      "ScopeEventWriter",
		capability: string(CapScopeEventWriter),
		requires:   []Capability{CapScopeEventWriter},
		consequence: "A sink that cannot record scope epochs simply never receives them, and the operator's " +
			"audit trail loses the \"we stopped watching\" versus \"it was deleted\" distinction for that " +
			"sink alone.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.ScopeEventWriter); return ok },
		properties: scopeEventWriterProperties,
	}
}

func proberSuite() optionalSuite {
	return optionalSuite{
		group:      "Prober",
		capability: string(CapProber),
		requires:   []Capability{CapProber},
		consequence: "A backend that cannot be probed gets no probe loop at all: its CR carries no " +
			"SchemaValid verdict, and an unreachable or drifted backend is discovered only when a write " +
			"fails.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.Prober); return ok },
		properties: proberProperties,
	}
}

// optionalSuites is every optional half, in the order RunWriterSuite runs them.
func optionalSuites() []optionalSuite {
	return []optionalSuite{stateReaderSuite(), scopeEpochReadSuite(), scopeEventWriterSuite(), proberSuite()}
}

// RunStateReaderSuite asserts the sink.StateReader contract against the backend
// newWriter builds, skipping loudly when it implements no read half.
//
// It runs two groups, because the read half asks two kinds of question and the
// second kind needs a scope log to exist: the object-history properties need only
// a StateReader, while ScopeWasActive and ActiveScopes also need the backend to
// implement sink.ScopeEventWriter, which is how the suite plants the epochs it
// then reads back.
func RunStateReaderSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, stateReaderSuite())
	runOptionalSuite(t, newWriter, scopeEpochReadSuite())
}

// RunScopeEventWriterSuite asserts the sink.ScopeEventWriter contract against the
// backend newWriter builds, skipping loudly when it records no scope epochs.
func RunScopeEventWriterSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, scopeEventWriterSuite())
}

// RunProberSuite asserts the sink.Prober contract against the backend newWriter
// builds, skipping loudly when it answers no health probe.
func RunProberSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, proberSuite())
}

// runOptionalSuite is the shared shape of all four: compare the claim against the
// backend, then either run the half or explain the omission.
//
// The comparison costs one throwaway harness, built on the group's own *testing.T
// and never started. That is deliberate rather than reusing a property's harness:
// the answer decides whether any property runs at all, and asking it once keeps
// the skip to one line instead of repeating it per property.
func runOptionalSuite(t *testing.T, newWriter func(t *testing.T) Harness, s optionalSuite) {
	t.Helper()
	if newWriter == nil {
		t.Fatalf("conformance: running the %s suite needs a non-nil harness constructor", s.capability)
	}
	t.Run(s.group, func(t *testing.T) {
		if !capabilityGate(t, newWriter(t), s) {
			t.Logf("%s", missingCapabilityMessage(s))
			t.Skipf("conformance: this backend does not implement %s", s.capability)
		}
		for _, p := range s.properties() {
			t.Run(p.name, func(t *testing.T) {
				runProperty(t, p, newWriter(t))
			})
		}
	})
}

// capabilityGate compares what a harness declared against what its Writer really
// is, and reports whether this half's properties should run. Agreement on absent
// returns false, which is the caller's cue to skip with the courtesy explanation;
// either disagreement is fatal here and nothing runs.
//
// It takes a conformanceT rather than the *testing.T it is called with, for the
// reason the rest of the suite does: both disagreement paths are Fatalf paths, and
// a Fatalf nobody can observe is exactly the vacuity this package tests itself for
// (nonvacuity_test.go drives this function through a recorder). The skip stays
// with the caller, since only testing can skip.
func capabilityGate(t conformanceT, h Harness, s optionalSuite) bool {
	t.Helper()
	if h.Writer == nil {
		t.Fatalf("conformance: Harness.Writer is nil; the suite cannot tell which optional halves this backend implements")
	}
	h.requireCapabilityDeclaration(t)

	declared := h.Capabilities.declares(s.requires...)
	// Deliberately the same assertion SinkManager.newLiveSink makes: a claim is
	// only worth checking against the question the runtime itself asks.
	detected := s.implements(h.Writer)

	switch {
	case declared && !detected:
		t.Fatalf("conformance: this harness declares %s but its Writer does not satisfy that type assertion — "+
			"the very one SinkManager.newLiveSink makes, so the runtime would build this sink with the half "+
			"switched off and say nothing: a full-capability backend silently downgraded to Writer-only, with "+
			"every test green. This is the method-set-drift case — a renamed method, a changed signature, a "+
			"value receiver where a pointer is handed over — so fix the method set; withdrawing the "+
			"declaration would only hide the drift again. Declared: %s",
			s.capability, joinCapabilities(s.requires))
	case detected && !declared:
		t.Fatalf("conformance: this backend's Writer implements %s but the harness never declared it, so this "+
			"suite would have certified obligations the author never reviewed. Declare it — add %s to "+
			"Harness.Capabilities via conformance.DeclareCapabilities — once the properties it turns on have "+
			"been read, or stop implementing the interface",
			s.capability, joinCapabilities(s.requires))
	}
	return declared && detected
}

// requireCapabilityDeclaration fails a harness that never declared which optional
// halves its backend implements, or that declared a name this suite does not know.
//
// It is a Fatalf for the same reason the nil-lever checks in Harness.validate are:
// an undeclared harness is one whose author has not said what is under test, and
// Phase 5 exists to stop that reading as a pass. It is shared between validate and
// capabilityGate because the two see a harness on different paths — a skipped
// optional half never runs a property, so validate alone would never reach the
// harness whose declaration is missing.
func (h Harness) requireCapabilityDeclaration(t conformanceT) {
	t.Helper()
	if !h.Capabilities.declared {
		t.Fatalf("conformance: Harness.Capabilities was never declared; build it with "+
			"conformance.DeclareCapabilities(...), naming the optional halves this backend implements "+
			"(any of %s). A backend implementing none of them declares the empty set explicitly — "+
			"DeclareCapabilities() with no arguments — because the zero value means \"nobody thought about "+
			"this\", which no suite can tell apart from D12's deliberate Writer-only archive tier",
			joinCapabilities(declarableCapabilities()))
	}
	if bad := h.Capabilities.unknown(); len(bad) > 0 {
		t.Fatalf("conformance: Harness.Capabilities declares %s, which names no optional half of the sink "+
			"contract; want any of %s. An unrecognised name would otherwise read as \"not declared\" and be "+
			"reported as an implemented-but-undeclared capability, sending the reader after the wrong bug",
			joinCapabilities(bad), joinCapabilities(declarableCapabilities()))
	}
}

// missingCapabilityMessage is the whole of what a skip has to say: which contract
// is absent, which obligations therefore went unchecked, and what the runtime
// turns off as a result.
//
// It is a function rather than an inline Logf so the non-vacuity tests can assert
// on its content directly — a skip message that stopped naming the capability
// would put the suite straight back into the silence this file exists to prevent,
// and no test that merely observes a skipped subtest could tell.
func missingCapabilityMessage(s optionalSuite) string {
	names := make([]string, 0, len(s.properties()))
	for _, p := range s.properties() {
		names = append(names, s.group+"/"+p.name)
	}
	return fmt.Sprintf(
		"conformance: this backend's Writer does not implement %s, so the following properties were NOT "+
			"checked and this suite certifies nothing about them: %s. %s",
		s.capability, strings.Join(names, ", "), s.consequence)
}

// The harness levers the optional halves need, as per-property validators. They
// are Fatalf for the same reason the mandatory validate is: a harness missing the
// lever a property reaches for means that property is not testing the backend,
// and Phase 5 exists to stop that reading as a pass.
//
// They are checked per property rather than per suite so a backend is only ever
// failed for a field the property actually uses.
func requireReadFault(t conformanceT, h Harness) {
	t.Helper()
	if h.SetReadFault == nil {
		t.Fatalf("conformance: Harness.SetReadFault is nil but this backend implements %s; "+
			"the suite cannot break a read part-way through, and the partial-read obligation is the one "+
			"that silently truncates a warm-up", CapStateReader)
	}
}

func requireScopeWrites(t conformanceT, h Harness) {
	t.Helper()
	if h.ScopeWrites == nil {
		t.Fatalf("conformance: Harness.ScopeWrites is nil but this backend implements %s; "+
			"the suite cannot see which scope transitions the backend recorded, so it cannot tell one "+
			"recorded epoch from two or from none", CapScopeEventWriter)
	}
}

func requireProbeOutcome(t conformanceT, h Harness) {
	t.Helper()
	if h.SetProbeOutcome == nil {
		t.Fatalf("conformance: Harness.SetProbeOutcome is nil but this backend implements %s; "+
			"the suite cannot arrange an unhealthy backend, and a probe that is only ever asked about a "+
			"healthy one classifies nothing", CapProber)
	}
}

// allProperties is every obligation this package asserts, mandatory and optional
// alike. The non-vacuity tests walk it to prove each one can fail.
func allProperties() []property {
	all := writerProperties()
	for _, s := range optionalSuites() {
		all = append(all, s.properties()...)
	}
	return all
}
