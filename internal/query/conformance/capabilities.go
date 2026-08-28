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

// This file is the capability half of the suite: what a backend claims it can
// answer, what it turns out to answer, what happens when those disagree, and why
// two of the four flags are checked against behaviour while two can only be
// checked against the claim.
//
// The shape is the sink suite's (Task 5.4) and the reasoning is the same. A
// reduced capability set is a legitimate design — D12's archive tier records no
// deletions at all and is not thereby broken — so the suite must not fail it. But
// an omission nobody is obliged to write down is indistinguishable from an
// oversight, and the whole point of gating backends (D11) is that a badge nobody
// can interpret is worse than no badge. So every harness states in code, reviewed
// in the PR, what its backend can answer, and the suite compares that claim
// against the engine.
//
// What differs from the sink suite is where "detected" comes from. There, the
// optional halves are separate interfaces and detection is the very type assertion
// the runtime makes. Here every engine implements the whole of QueryEngine and the
// differences are values on Capabilities(), so detection has two layers:
//
//  1. Reported — what Capabilities() says. This is what the command-line client
//     reads while composing a query and again while rendering the result, so it is
//     the runtime's own question, and the harness's declaration is checked against
//     it for every property (Harness.validate).
//
//  2. Observed — what the engine actually does. Two of the four flags have a
//     visible consequence a query can provoke: Deletions (does a seeded Deleted row
//     come back?) and TimeBoundRequired (is an unbounded query refused?). Those are
//     checked in both directions by the properties that provoke them, because a
//     backend whose report and behaviour disagree is one whose rendered notice
//     contradicts the data printed beside it.
//
// ServerSideFilter and PointQuery have no such probe, and that is by design rather
// than an omission here. Their documented consequence is on *cost*, not on
// content: a filtered result is required to be byte-identical whether the predicate
// was pushed down or applied to rows already read, which is exactly what the
// Filters/AgreesWithNonPushdown property pins. An engine cannot be caught lying
// about them from outside, and a suite that pretended otherwise would be inventing
// a check it cannot perform.

package conformance

import (
	"slices"
	"strings"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Capability is one thing a query backend may declare itself able to answer,
// spelled exactly as the field on query.Capabilities is spelled, so a declaration
// and a failure message can both be pasted into a grep and land on the field they
// name.
//
// It is a named type rather than a bare string because its values are what a
// harness declares, and a plain string parameter would invite a backend to declare
// a name this suite has never heard of.
type Capability string

// The capabilities a harness may declare. There are exactly four because these are
// the four flags query.Capabilities carries, and a declaration is only worth
// anything when it is checked against something the runtime really asks.
const (
	// CapDeletions is query.Capabilities.Deletions: this backend's history can
	// contain Deleted rows at all. Declaring it falsely is the failure the
	// deletion-visibility property exists for — a timeline that simply stops looks
	// identical to one that ended in a deletion, and only this flag tells a reader
	// which they are looking at.
	CapDeletions Capability = "Deletions"
	// CapServerSideFilter is query.Capabilities.ServerSideFilter: actor and
	// field-path predicates are pushed into the backend rather than applied to rows
	// already read. Declaration-only — see the file comment.
	CapServerSideFilter Capability = "ServerSideFilter"
	// CapPointQuery is query.Capabilities.PointQuery: one object's history can be
	// sought without reading the window around it. Declaration-only, for the same
	// reason.
	CapPointQuery Capability = "PointQuery"
	// CapTimeBoundRequired is query.Capabilities.TimeBoundRequired: every query
	// must carry a time bound, and an unbounded one is refused up front rather than
	// started and never finished.
	CapTimeBoundRequired Capability = "TimeBoundRequired"
)

// declarableCapabilities is every Capability a harness may name, in the order a
// failure message lists them. It exists so "you declared something I do not
// recognise" can name the alternatives instead of making the reader find them.
func declarableCapabilities() []Capability {
	return []Capability{CapDeletions, CapServerSideFilter, CapPointQuery, CapTimeBoundRequired}
}

// CapabilitySet is a backend's declaration of what its query engine can answer:
// the claim every property checks against the engine itself.
//
// The declaration is mandatory, and "none of them" has to be said out loud, so the
// type carries an explicit marker. The zero value is not the empty set — it is
// "nobody thought about this", and the suite rejects it, because a suite cannot
// tell that apart from a deliberately minimal backend. The marker is an unexported
// field only DeclareCapabilities sets, which is what makes an accidental or forged
// "declared" value unrepresentable outside this package: a backend cannot produce
// one with a struct literal, and nobody can drop the constructor call without a
// compile error.
type CapabilitySet struct {
	// declared separates DeclareCapabilities() — an explicit, reviewed "this
	// backend answers none of the optional questions" — from a Harness that never
	// mentioned capabilities at all.
	declared bool
	caps     []Capability
}

// DeclareCapabilities is how a harness states what its query backend can answer.
//
// Call it with no arguments to declare a backend that claims none of them. That is
// a legitimate design, and this is how it stays auditable: an omission that was
// declared can be reviewed in the PR that declares it, whereas an omission that was
// merely never mentioned is what the suite has no way to distinguish from a
// mistake.
//
// Declaring by name rather than by handing over a query.Capabilities literal is
// deliberate. The literal form makes a declaration a copy of the engine's own
// source, which is the one thing a cross-check must not be; a named list is written
// by a person who has read what each name turns on.
func DeclareCapabilities(caps ...Capability) CapabilitySet {
	return CapabilitySet{declared: true, caps: slices.Clone(caps)}
}

// declares reports whether c was declared.
func (c CapabilitySet) declares(want Capability) bool {
	return slices.Contains(c.caps, want)
}

// unknown returns the declared names that are not capabilities this suite knows,
// in declaration order.
//
// A misspelling would otherwise read as "not declared" and be reported as an
// implemented-but-undeclared capability: a correct rejection with a diagnosis that
// sends the reader looking for the wrong bug.
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

// reports reads one capability off a query.Capabilities value.
//
// The switch is exhaustive over declarableCapabilities by construction: a fifth
// flag added to the contract without a case here would be silently unchecked, so
// the default is a panic rather than a false — an unreachable state that must stay
// unreachable loudly.
func reports(caps query.Capabilities, c Capability) bool {
	switch c {
	case CapDeletions:
		return caps.Deletions
	case CapServerSideFilter:
		return caps.ServerSideFilter
	case CapPointQuery:
		return caps.PointQuery
	case CapTimeBoundRequired:
		return caps.TimeBoundRequired
	default:
		panic("conformance: unknown capability " + string(c) + "; declarableCapabilities and reports have drifted apart")
	}
}

// requireCapabilityDeclaration fails a harness that never declared what its backend
// can answer, or that declared a name this suite does not know.
//
// It is a Fatalf for the same reason the nil-lever checks in Harness.validate are:
// an undeclared harness is one whose author has not said what is under test, and
// this package exists to stop that reading as a pass.
func (h Harness) requireCapabilityDeclaration(t conformanceT) {
	t.Helper()
	if !h.Capabilities.declared {
		t.Fatalf("conformance: Harness.Capabilities was never declared; build it with "+
			"conformance.DeclareCapabilities(...), naming what this backend can answer (any of %s). "+
			"A backend claiming none of them declares the empty set explicitly — DeclareCapabilities() "+
			"with no arguments — because an omission the suite was never told about is one it cannot "+
			"tell from an oversight",
			joinCapabilities(declarableCapabilities()))
	}
	if bad := h.Capabilities.unknown(); len(bad) > 0 {
		t.Fatalf("conformance: Harness.Capabilities declares %s, which this suite does not recognise; "+
			"a misspelling reads as 'not declared' and would be reported as an undeclared capability, "+
			"sending you after the wrong bug. Declarable: %s",
			joinCapabilities(bad), joinCapabilities(declarableCapabilities()))
	}
}

// requireCapabilityAgreement compares the harness's declaration against what the
// engine reports, flag by flag, and fails on disagreement in either direction.
//
// It runs for every property rather than once, because the answer decides what
// several of them expect: a property that consulted a declaration the engine
// disagrees with would assert the wrong thing and pass, which is the vacuity this
// package tests itself for.
//
// Note what it deliberately does not do: it calls Capabilities() and nothing else.
// The contract says that call must not perform a round trip and must not fail, so
// a gate that provoked one would be testing a different method than the one the
// command-line client uses while rendering.
func requireCapabilityAgreement(t conformanceT, h Harness) {
	t.Helper()
	h.requireCapabilityDeclaration(t)

	reported := h.Engine.Capabilities()
	for _, c := range declarableCapabilities() {
		declared := h.Capabilities.declares(c)
		switch {
		case declared && !reports(reported, c):
			t.Fatalf("conformance: this harness declares %s but the engine's own Capabilities() reports it "+
				"false. Capabilities() is what the command-line client reads while composing a query and "+
				"again while rendering the result, so the runtime would answer this backend's questions with "+
				"the capability switched off and the harness would still certify the properties it turns on. "+
				"Fix whichever of the two is wrong; withdrawing the declaration is only correct if the "+
				"backend genuinely cannot do it", c)
		case !declared && reports(reported, c):
			t.Fatalf("conformance: this engine reports %s but the harness never declared it, so the suite "+
				"would have certified obligations the author never reviewed. Add %s to "+
				"conformance.DeclareCapabilities once the properties it turns on have been read, or stop "+
				"reporting it", c, c)
		}
	}
}

// declaredCapabilities renders the declaration as the query.Capabilities value it
// asserts the engine reports.
//
// Properties build their expectations from this rather than from the engine's own
// report, so that an engine which lies about itself is caught by the property
// rather than quietly excused by it. The two are already known to agree by the time
// any property runs — requireCapabilityAgreement saw to that — which is exactly
// what makes using the declaration safe and using the report pointless.
//
// Backend is deliberately left empty: it is not a capability, it is provenance, and
// it is checked separately for being non-empty and stable.
func (c CapabilitySet) declaredCapabilities() query.Capabilities {
	return query.Capabilities{
		Deletions:         c.declares(CapDeletions),
		ServerSideFilter:  c.declares(CapServerSideFilter),
		PointQuery:        c.declares(CapPointQuery),
		TimeBoundRequired: c.declares(CapTimeBoundRequired),
	}
}

// propCapabilities is the subtest name for the capability property below.
//
// Note what it is *not* named for. The declaration-versus-report agreement is not a
// property, because a property runs once and that agreement has to hold for all of
// them: it is checked in Harness.validate, before every single obligation in the
// table, so that no property can build an expectation from a declaration the engine
// disagrees with. What is left for a property of its own is the part validate cannot
// see from one call — that the report does not change underneath a caller.
const propCapabilities = "Capabilities/StableReport"

// capabilitiesAreStable: the engine names itself, and answers the same way twice.
//
// Both halves have a consequence a reader can be misled by. Backend is surfaced as
// metadata in structured output so a scripted consumer can tell which engine
// produced a result — and, when two answers disagree, which one to trust for the
// question asked; an engine that returned nothing there leaves that consumer with a
// result it cannot attribute.
//
// Stability matters because a caller consults Capabilities twice: once while
// composing a query, and again while rendering what came back. A capability set that
// changed in between would print a notice that contradicts the data beside it —
// "this backend records no deletions" above a timeline that just ended in one, or
// worse, nothing at all above a timeline that merely stopped.
func capabilitiesAreStable(t conformanceT, h Harness) {
	t.Helper()

	first := h.Engine.Capabilities()
	if first.Backend == "" {
		t.Errorf("conformance: Capabilities().Backend is empty. It is surfaced as metadata.backend in " +
			"structured output so a scripted consumer can attribute a result to the engine that produced " +
			"it, and it is a stable identifier people pin scripts to — conventionally the storage " +
			"technology's own lowercase name")
	}
	for i := range 3 {
		if again := h.Engine.Capabilities(); again != first {
			t.Fatalf("conformance: Capabilities() returned %+v on call %d, having returned %+v on the "+
				"first. A caller reads it while composing a query and again while rendering the result, "+
				"so a set that changes in between prints a notice that contradicts the data beside it",
				again, i+2, first)
		}
	}
}
