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

// This file is why the suite can be trusted. A conformance suite is a claim about
// backends nobody has written yet, and the failure mode of such a claim is silence:
// a property that asserts nothing passes every backend, and the badge it hands out
// is worse than no badge at all, because it retires the scrutiny that would have
// caught the bug. An untested backend certified is worse than an untested backend.
//
// So the suite is tested in both directions. The two "passes" tests show the
// properties can be satisfied — by a full-capability engine and by a truthfully
// reduced one, since a suite whose capability-conditional expectations had never
// been executed would be half untested. TestQuerySuiteIsNonVacuous shows each
// property rejects an engine built to violate it. Neither test alone means anything:
// a suite that always failed would pass the second, and a suite that always passed
// would pass the first.

package conformance

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// propertyTimeout bounds one property run against a broken fixture. A property that
// hangs has not rejected anything, so the runner reports the hang separately rather
// than letting it count as a rejection.
const propertyTimeout = 90 * time.Second

// recordingT is a conformanceT that captures failures instead of reporting them, so
// a test can assert that a property failed.
//
// Fatalf must abandon the property the way testing does, or the code after a fatal
// assertion would run against state the assertion just declared broken.
// runtime.Goexit is how testing itself does it, and it still runs deferred calls —
// which is what lets runProperty's deferred close release the engine even when a
// property gives up early.
type recordingT struct {
	mu       sync.Mutex
	failures []string
	logs     []string
}

func (r *recordingT) Helper() {}

func (r *recordingT) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recordingT) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}

func (r *recordingT) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures) > 0
}

// first is the first recorded failure, for the log line that shows *why* a property
// rejected a fixture. A property failing for an unrelated reason would prove nothing
// about the obligation it is supposed to enforce.
func (r *recordingT) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.failures) == 0 {
		return ""
	}
	return r.failures[0]
}

// runPropertyIsolated runs one property against one harness on its own goroutine
// (Fatalf needs to be able to abandon it) and reports what it recorded, plus whether
// it terminated at all.
func runPropertyIsolated(p property, h Harness) (*recordingT, bool) {
	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProperty(rec, p, h)
	}()
	select {
	case <-done:
		return rec, true
	case <-time.After(propertyTimeout):
		return rec, false
	}
}

// recordValidate drives Harness.validate through the recorder, for the capability
// tests: a Fatalf on a real T is not observable from the test that provoked it,
// which is the reason the whole suite is written against conformanceT.
func recordValidate(h Harness) *recordingT {
	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.validate(rec)
	}()
	<-done
	return rec
}

// TestQuerySuitePassesCompliantEngine is half of the non-vacuity argument: the
// properties are satisfiable. A fake with no flaws set is an ordinary, correct
// engine, and the suite must have nothing to say about it.
func TestQuerySuitePassesCompliantEngine(t *testing.T) {
	RunQuerySuite(t, func(t *testing.T) Harness {
		h, cleanup := newFakeHarness(flaws{})
		t.Cleanup(cleanup)
		return h
	})
}

// TestQuerySuitePassesReducedEngine is the other shape of correct backend: one that
// records no deletions, pushes nothing down, cannot seek to a single object, and
// demands a time bound on every query.
//
// It has to pass for the same reason the archive tier is a legitimate design (D12):
// a reduced capability set truthfully declared is not a defect. And it has to be
// *run*, because every capability-conditional expectation in this suite is dead code
// until some engine exercises the other branch.
func TestQuerySuitePassesReducedEngine(t *testing.T) {
	RunQuerySuite(t, func(t *testing.T) Harness {
		h, cleanup := newReducedHarness()
		t.Cleanup(cleanup)
		return h
	})
}

// TestQuerySuiteIsNonVacuous runs each property against an engine that violates it
// and asserts the property fails.
//
// Every property in the table is covered by at least one fixture, and the walk at
// the end is what proves it. A fixture may well fail properties beyond the ones
// listed — an engine that blanks the UID breaks several at once — so the list is
// what each fixture must *at minimum* be caught by, not an exhaustive account of its
// damage.
func TestQuerySuiteIsNonVacuous(t *testing.T) {
	fixtures := []struct {
		name    string
		flaws   flaws
		what    string
		catches []string
	}{
		{
			name:    "reverseOrder",
			flaws:   flaws{reverseOrder: true},
			what:    "emits newest first when oldest first was asked for, and takes a limit from the far end",
			catches: []string{propOrderAscending, propOrderReverse, propOrderLimit},
		},
		{
			name:    "coarseTimestamps",
			flaws:   flaws{coarseTimestamps: true},
			what:    "rounds every timestamp to the second, losing the order of everything inside each one",
			catches: []string{propOrderNanoseconds},
		},
		{
			name:  "mergeIncarnations",
			flaws: flaws{mergeIncarnations: true},
			what: "splices two UIDs under one name into a single timeline, blanks the UID a reader " +
				"would key on, and reports the two incarnations as one",
			catches: []string{
				propIncarnationNewest, propIncarnationAll, propIncarnationPinned, propIncarnationEnumerated,
			},
		},
		{
			name:    "fabricateDeletion",
			flaws:   flaws{fabricateDeletion: true},
			what:    "synthesizes a Deleted row to close a timeline that merely ended",
			catches: []string{propDeletionVisible},
		},
		{
			name:    "ignoreCheckpoints",
			flaws:   flaws{ignoreCheckpoints: true},
			what:    "treats a checkpoint as an ordinary modification and replays from the first sighting",
			catches: []string{propReconstructBase},
		},
		{
			name:    "doubleApplyCheckpoint",
			flaws:   flaws{doubleApplyCheckpoint: true},
			what:    "applies a checkpoint's own diff on top of the state that diff already produced",
			catches: []string{propReconstructCheckpoint},
		},
		{
			name:    "dropLastPatch",
			flaws:   flaws{dropLastPatch: true},
			what:    "stops one patch short while reporting the digest of the row it never reached",
			catches: []string{propReconstructFidelity},
		},
		{
			name:    "stateBeforeHistory",
			flaws:   flaws{stateBeforeHistory: true},
			what:    "substitutes the earliest recorded state for an instant that predates all of history",
			catches: []string{propReconstructPreHistory},
		},
		{
			name:  "mangleCoverage",
			flaws: flaws{mangleCoverage: true},
			what: "closes intervals that are still open, returns them newest first, and drops the " +
				"all-namespaces scope from a namespaced query",
			catches: []string{propCoverageOpen, propCoverageClosed, propCoverageNamespace},
		},
		{
			name:    "scanUnbounded",
			flaws:   flaws{scanUnbounded: true},
			what:    "declares that a time bound is required and then answers an unbounded query anyway",
			catches: []string{propTimeBounds},
		},
		{
			name:    "leakOnClose",
			flaws:   flaws{leakOnClose: true},
			what:    "leaves a goroutine running after the iterator is closed",
			catches: []string{propStreamEarlyClose},
		},
		{
			name:    "truncateOnError",
			flaws:   flaws{truncateOnError: true},
			what:    "reports a mid-stream backend failure as the end of the result set",
			catches: []string{propStreamMidError},
		},
		{
			name:    "ignoreActorInclude",
			flaws:   flaws{ignoreActorInclude: true},
			what:    "ignores the Actors filter",
			catches: []string{propFilterActorInclude},
		},
		{
			name:    "ignoreExcludeActors",
			flaws:   flaws{ignoreExcludeActors: true},
			what:    "ignores the ExcludeActors filter",
			catches: []string{propFilterActorExclude, propFilterAgreement},
		},
		{
			name:    "ignoreFieldPaths",
			flaws:   flaws{ignoreFieldPaths: true},
			what:    "ignores the FieldPaths filter",
			catches: []string{propFilterFieldPaths},
		},
		{
			name:    "unstableCapabilities",
			flaws:   flaws{unstableCapabilities: true},
			what:    "renames itself on every call, so a rendered notice can contradict the data beside it",
			catches: []string{propCapabilities},
		},
	}

	covered := map[string]bool{}
	for _, f := range fixtures {
		for _, name := range f.catches {
			covered[name] = true
		}
		t.Run(f.name, func(t *testing.T) {
			for _, name := range f.catches {
				t.Run(name, func(t *testing.T) {
					p, ok := propertyByName(name)
					if !ok {
						t.Fatalf("no property named %q; the fixture table names one the suite does not run",
							name)
					}
					h, cleanup := newFakeHarness(f.flaws)
					defer cleanup()

					rec, terminated := runPropertyIsolated(p, h)
					if !terminated {
						t.Fatalf("%s did not terminate within %s against an engine that %s: a property "+
							"that hangs rejects nothing", name, propertyTimeout, f.what)
					}
					if !rec.failed() {
						t.Fatalf("%s passed against an engine that %s: the property asserts nothing about "+
							"the obligation it is named for", name, f.what)
					}
					t.Logf("%s rejected it: %s", name, truncate(rec.first()))
				})
			}
		})
	}

	// A property with no fixture behind it is untested machinery: it could be
	// asserting nothing and this file would never notice.
	for _, p := range queryProperties() {
		if !covered[p.name] {
			t.Errorf("property %s has no fixture proving it can fail; add one to the table above", p.name)
		}
	}
}

// TestPropertyNamesAreUnique guards the assumption both the non-vacuity table and
// propertyByName rest on. Two properties sharing a name would mean a fixture
// silently exercising the wrong one — proving the obligation it names to be untested
// while reporting the opposite.
func TestPropertyNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range queryProperties() {
		if seen[p.name] {
			t.Errorf("two properties are named %q; propertyByName resolves only the first, so the "+
				"non-vacuity fixture for the other proves nothing", p.name)
		}
		seen[p.name] = true
	}
}

// The three tests below are the non-vacuity proof for the capability declaration
// itself: a harness that never declared, one claiming what its engine denies, and
// one whose engine reports what it never claimed.
//
// They sit outside the fixture table on purpose, and not because they were awkward
// to fit. That table pairs a *property* with an engine built to violate it, and the
// walk at its end is what proves no obligation went unproved. These three are
// harness-level: they fail in validate before any property runs, and there is no
// property they could be attached to, so adding rows for them would put three
// non-obligations into the table and weaken the very coverage argument they would be
// joining.

// TestCapabilityDeclarationIsMandatory: a harness that never said what its backend
// can answer is rejected rather than defaulted.
//
// Defaulting is the tempting alternative and it is the one that ruins the gate. An
// undeclared harness and a deliberately minimal one look identical from here, and
// only the second may pass — otherwise "this backend records no deletions" becomes a
// thing a suite can conclude on an author's behalf, and nobody ever reviews it.
func TestCapabilityDeclarationIsMandatory(t *testing.T) {
	h, cleanup := newFakeHarness(flaws{})
	defer cleanup()
	h.Capabilities = CapabilitySet{}

	rec := recordValidate(h)
	if !rec.failed() {
		t.Fatalf("an undeclared harness was accepted; the zero CapabilitySet is \"nobody thought about " +
			"this\", and a suite that cannot tell it from a reviewed omission certifies whichever it " +
			"happens to be handed")
	}
	if !strings.Contains(rec.first(), "DeclareCapabilities") {
		t.Errorf("the rejection does not name the constructor that fixes it: %s", truncate(rec.first()))
	}

	// A misspelling must be rejected as a misspelling. Left to read as "not
	// declared", it would surface as an undeclared-capability failure and send the
	// reader after the wrong bug.
	h.Capabilities = DeclareCapabilities(Capability("Deletion"))
	rec = recordValidate(h)
	if !rec.failed() {
		t.Fatalf("a harness declaring an unrecognised capability was accepted")
	}
	if !strings.Contains(rec.first(), "Deletion") {
		t.Errorf("the rejection does not quote the unrecognised name: %s", truncate(rec.first()))
	}
}

// TestCapabilityGateRejectsADeclarationTheEngineDenies is the direction that catches
// a harness whose author has read the properties and an engine that cannot honour
// them.
//
// A caller consults Capabilities() while composing a query and again while rendering
// the result. If the engine says it records no deletions and the harness says it
// does, the suite would certify the deletion obligations against a backend the
// runtime will treat as unable to meet them — and the notice printed above a
// timeline would contradict the rows beneath it.
func TestCapabilityGateRejectsADeclarationTheEngineDenies(t *testing.T) {
	h, cleanup := newReducedHarness()
	defer cleanup()
	h.Capabilities = DeclareCapabilities(CapDeletions, CapTimeBoundRequired)

	rec := recordValidate(h)
	if !rec.failed() {
		t.Fatalf("a harness declaring Deletions over an engine that reports it false was accepted")
	}
	if !strings.Contains(rec.first(), string(CapDeletions)) {
		t.Errorf("the rejection does not name the disputed capability: %s", truncate(rec.first()))
	}
}

// TestCapabilityGateRejectsAnUndeclaredCapabilityTheEngineReports is the other
// direction: an engine that can do more than its harness ever claimed.
//
// It is a failure and not a bonus. Every capability turns on obligations, and a
// harness that never declared one has an author who never reviewed them — so the
// suite would print a green result for properties nobody signed off on, which is the
// certified-untested outcome this whole file exists to prevent.
func TestCapabilityGateRejectsAnUndeclaredCapabilityTheEngineReports(t *testing.T) {
	h, cleanup := newFakeHarness(flaws{})
	defer cleanup()
	h.Capabilities = DeclareCapabilities(CapServerSideFilter, CapPointQuery)

	rec := recordValidate(h)
	if !rec.failed() {
		t.Fatalf("a harness that never declared Deletions over an engine reporting it was accepted")
	}
	if !strings.Contains(rec.first(), string(CapDeletions)) {
		t.Errorf("the rejection does not name the undeclared capability: %s", truncate(rec.first()))
	}
}

// failureExcerpt is how much of a rejection to echo: enough to see which obligation
// objected, short enough that sixteen fixtures do not bury the run.
const failureExcerpt = 260

// truncate flattens and shortens a failure message for a log line.
func truncate(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= failureExcerpt {
		return s
	}
	return s[:failureExcerpt] + "…"
}
