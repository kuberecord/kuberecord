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
// backends nobody has written yet, and the failure mode of such a claim is
// silence: a property that asserts nothing passes every backend, and the badge it
// hands out is worse than no badge at all, because it retires the scrutiny that
// would have caught the bug.
//
// So the suite is tested in both directions. TestWriterSuitePassesCompliantWriter
// shows the properties can be satisfied; TestWriterSuiteIsNonVacuous shows each
// one rejects a Writer built to violate it. Neither test alone means anything —
// a suite that always failed would pass the second and a suite that always
// passed would pass the first.
package conformance

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// propertyTimeout bounds one property run against a broken fixture. A property
// that hangs has not rejected anything, so the runner reports the hang separately
// rather than letting it count as a rejection.
const propertyTimeout = 90 * time.Second

// recordingT is a conformanceT that captures failures instead of reporting them,
// so a test can assert that a property failed.
//
// Fatalf must abandon the property the way testing does, or the code after a
// fatal assertion would run against state the assertion just declared broken.
// runtime.Goexit is how testing itself does it, and it still runs deferred calls —
// which is what lets a property's `defer r.stop()` shut the Writer down even when
// it gives up early.
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

// first is the first recorded failure, for the log line that shows *why* the
// property rejected the fixture — a property failing for an unrelated reason
// would prove nothing about the obligation it is supposed to enforce.
func (r *recordingT) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.failures) == 0 {
		return ""
	}
	return r.failures[0]
}

// runPropertyIsolated runs one property against one harness on its own goroutine
// (Fatalf needs to be able to abandon it) and reports what it recorded, plus
// whether it terminated at all.
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

// TestWriterSuitePassesCompliantWriter is the other half of the non-vacuity
// argument: the properties are satisfiable. fakeWriter with no fixture switches
// set is an ordinary, correct Writer, and the suite must have nothing to say
// about it.
func TestWriterSuitePassesCompliantWriter(t *testing.T) {
	RunWriterSuite(t, func(*testing.T) Harness { return newFakeHarness(fakeOpts{}) })
}

// TestWriterSuiteIsNonVacuous runs each property against a Writer that violates
// it and asserts the property fails.
//
// Every property in the table is covered by at least one fixture. A fixture may
// well fail properties beyond the ones listed — a Writer that lies about durability
// breaks several at once — so the list is what each fixture must *at minimum* be
// caught by, not an exhaustive account of its damage.
func TestWriterSuiteIsNonVacuous(t *testing.T) {
	fixtures := []struct {
		name    string
		opts    fakeOpts
		what    string
		catches []string
	}{
		{
			name: "doubleCommit",
			opts: fakeOpts{doubleCommit: true},
			what: "settles every job twice",
			catches: []string{
				propExactlyOnceSuccess,
				propExactlyOnceFailure,
				propExactlyOnceCancelled,
				propExactlyOnceDrain,
				propNoLostJobs,
				propStorm,
			},
		},
		{
			name:    "dropOnDrain",
			opts:    fakeOpts{dropOnDrain: true},
			what:    "abandons queued work at shutdown instead of flushing it",
			catches: []string{propExactlyOnceDrain, propDrainOrdering},
		},
		{
			name:    "lyingCommit",
			opts:    fakeOpts{lyingCommit: true},
			what:    "reports a refused write as durably written",
			catches: []string{propExactlyOnceFailure, propNoLostJobs},
		},
		{
			name:    "unboundedEnqueue",
			opts:    fakeOpts{unboundedEnqueue: true},
			what:    "blocks on a full queue past its own timeout",
			catches: []string{propEnqueueBounded},
		},
		{
			name:    "nonIdempotent",
			opts:    fakeOpts{nonIdempotent: true},
			what:    "re-stamps each record as it stores it, so a replay never collapses",
			catches: []string{propIdempotency},
		},
		{
			name:    "collapseIncarnations",
			opts:    fakeOpts{collapseIncarnations: true},
			what:    "folds an identity's incarnations into one last-known state",
			catches: []string{propPerIncarnation},
		},
		{
			name:    "keepTombstones",
			opts:    fakeOpts{keepTombstones: true},
			what:    "reports incarnations whose own latest event is a deletion",
			catches: []string{propTombstoned},
		},
		{
			name:    "shortRead",
			opts:    fakeOpts{shortRead: true},
			what:    "returns the rows a broken read delivered, with a nil error",
			catches: []string{propPartialRead},
		},
		{
			name:    "ignoreAsOf",
			opts:    fakeOpts{ignoreAsOf: true},
			what:    "answers the epoch probe from the whole scope log, ignoring the caller's cutoff",
			catches: []string{propScopeAsOf},
		},
		{
			name:    "keepStoppedScopesActive",
			opts:    fakeOpts{keepStoppedScopesActive: true},
			what:    "enumerates every scope it ever saw started, closed ones included",
			catches: []string{propActiveScopes},
		},
		{
			name:    "coalesceScopeEvents",
			opts:    fakeOpts{coalesceScopeEvents: true},
			what:    "records only the first transition per scope, losing the Stopped that followed",
			catches: []string{propScopeEventsRecordedOnce},
		},
		{
			name:    "swallowScopeRejection",
			opts:    fakeOpts{swallowScopeRejection: true},
			what:    "tells the caller a transition it dropped was accepted",
			catches: []string{propScopeRejectionSurfaced},
		},
		{
			name:    "unclassifiedSchemaError",
			opts:    fakeOpts{unclassifiedSchemaError: true},
			what:    "reports a schema mismatch as a bare error the manager cannot classify",
			catches: []string{propProbeSchema},
		},
		{
			name:    "schemaErrorForEverything",
			opts:    fakeOpts{schemaErrorForEverything: true},
			what:    "classifies an unreachable backend as a schema failure",
			catches: []string{propProbeUnreachable},
		},
		{
			name:    "probeAlwaysFails",
			opts:    fakeOpts{probeAlwaysFails: true},
			what:    "refuses the probe even against a healthy backend",
			catches: []string{propProbeHealthy},
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
						t.Fatalf("no property named %q; the fixture table names one the suite does not run", name)
					}
					rec, terminated := runPropertyIsolated(p, newFakeHarness(f.opts))
					if !terminated {
						t.Fatalf("%s did not terminate within %s against a writer that %s: a property that hangs "+
							"rejects nothing", name, propertyTimeout, f.what)
					}
					if !rec.failed() {
						t.Fatalf("%s passed against a writer that %s: the property asserts nothing about the "+
							"obligation it is named for", name, f.what)
					}
					t.Logf("%s rejected it: %s", name, truncate(rec.first(), 220))
				})
			}
		})
	}

	// A property with no fixture behind it is untested machinery: it could be
	// asserting nothing and this file would never notice. The walk covers the
	// optional halves too — a property that only some backends run is exactly the
	// one nobody would think to check.
	for _, p := range allProperties() {
		if !covered[p.name] {
			t.Errorf("property %s has no fixture proving it can fail; add one to the table above", p.name)
		}
	}
}

// TestPropertyNamesAreUnique guards the assumption both the non-vacuity table and
// propertyByName rest on. Names became addressable across five tables when the
// optional halves landed, and two tables agreeing on one would mean a fixture
// silently exercising the wrong property — proving the obligation it names to be
// untested while reporting the opposite.
func TestPropertyNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range allProperties() {
		if seen[p.name] {
			t.Errorf("two properties are named %q; propertyByName resolves only the first, so the "+
				"non-vacuity fixture for the other proves nothing", p.name)
		}
		seen[p.name] = true
	}
}

// TestOptionalSuitesSkipLoudlyForAWriterOnlyBackend is the other half of the
// capability argument, and the one the acceptance criteria single out: a
// Writer-only backend that *declares* itself Writer-only must be skipped, not
// failed — and the skip must say so.
//
// The harness it runs is built with an explicitly-declared empty capability set,
// which is what the whole shape rests on: omitting an optional half is a
// legitimate design under D12, whereas omitting the declaration is a harness
// nobody reviewed, and only the first of those may pass.
//
// The two halves are checked separately because neither is visible from the
// other. That the suites run green over a backend implementing none of the
// optional contracts is observable from here; what the skip actually *said* is
// not, since a skipped subtest reports nothing a parent can inspect. So the
// message is built by a function this test can call directly, and it is checked
// against the one thing a reader needs from it: the name of the interface that
// is missing, and the properties that consequently certify nothing. That the skip
// message is unreadable in the default `make test` run is exactly why it is the
// explanation and not the mechanism — see the two proofs below.
func TestOptionalSuitesSkipLoudlyForAWriterOnlyBackend(t *testing.T) {
	newHarness := func(*testing.T) Harness { return newWriterOnlyHarness(newFakeWriter(fakeOpts{})) }

	// Green, not red: omitting an optional half is a legitimate design (D12), so
	// the suites must have nothing to fail it for.
	RunStateReaderSuite(t, newHarness)
	RunScopeEventWriterSuite(t, newHarness)
	RunProberSuite(t, newHarness)

	declaredWriterOnly := newWriterOnlyHarness(newFakeWriter(fakeOpts{}))
	if !declaredWriterOnly.Capabilities.declared {
		t.Fatalf("the Writer-only harness does not declare its (empty) capability set; the suites above " +
			"would have been failing it for an undeclared harness rather than skipping a declared omission")
	}
	for _, s := range optionalSuites() {
		t.Run(s.group, func(t *testing.T) {
			if declaredWriterOnly.Capabilities.declares(s.requires...) {
				t.Fatalf("the Writer-only harness declares %s; the empty declaration is the whole of what "+
					"makes this a skip rather than a failure", s.capability)
			}
			if s.implements(declaredWriterOnly.Writer) {
				t.Fatalf("a Writer-only backend was detected as implementing %s; the suites would have run "+
					"against a backend that does not have that half", s.capability)
			}
			msg := missingCapabilityMessage(s)
			if !strings.Contains(msg, s.capability) {
				t.Errorf("the skip message does not name %s: %q; a skip that does not say what is missing "+
					"is indistinguishable from a pass", s.capability, msg)
			}
			for _, p := range s.properties() {
				if !strings.Contains(msg, p.name) {
					t.Errorf("the skip message does not name the unchecked property %s: %q", p.name, msg)
				}
			}
		})
	}
}

// The two tests below are the non-vacuity proof for the capability declaration
// itself, in both directions: a claim the backend cannot honour, and a contract
// the backend implements without ever claiming it.
//
// They sit outside TestWriterSuiteIsNonVacuous's fixture table on purpose, and
// not because they were awkward to fit. That table pairs a *property* with a
// Writer built to violate it, and the walk over allProperties() at its end is
// what proves no obligation went unproved. These two are harness-level: they fail
// before any property runs and there is no property they could be attached to, so
// adding rows for them would put two non-obligations into allProperties() and
// weaken the very coverage argument they would be joining. Written here they cost
// that argument nothing.
//
// Both drive capabilityGate through the recorder rather than a *testing.T,
// because a Fatalf on a real T is not observable from the test that provoked it —
// the same reason the whole suite is written against conformanceT.

// recordCapabilityGate runs the capability gate against a recorder on its own
// goroutine, since Fatalf abandons the caller the way testing's does, and returns
// what it recorded.
func recordCapabilityGate(h Harness, s optionalSuite) *recordingT {
	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		capabilityGate(rec, h, s)
	}()
	<-done
	return rec
}

// TestCapabilityGateRejectsADeclaredCapabilityTheBackendDoesNotHave is the
// method-set-drift direction: a harness claiming sink.StateReader over a Writer
// that does not satisfy the assertion.
//
// This is the case a skip can never catch, and the reason the declaration exists.
// Detection alone sees a Writer-only backend and skips; the runtime's own
// newLiveSink sees the same thing and builds the sink with its read half switched
// off. A full-capability backend is then silently degraded — warm-up, zombie GC
// and boot reconciliation all quietly disabled — with every test green.
//
// The halves this fixture did not claim must still skip: a fixture that failed
// every suite would prove nothing about the one it is aimed at.
func TestCapabilityGateRejectsADeclaredCapabilityTheBackendDoesNotHave(t *testing.T) {
	h := newWriterOnlyHarness(newFakeWriter(fakeOpts{}))
	h.Capabilities = DeclareCapabilities(CapStateReader)

	for _, s := range optionalSuites() {
		t.Run(s.group, func(t *testing.T) {
			rec := recordCapabilityGate(h, s)
			if !h.Capabilities.declares(s.requires...) {
				if rec.failed() {
					t.Fatalf("the gate failed the %s half, which this harness never claimed: %s",
						s.capability, truncate(rec.first(), 220))
				}
				t.Logf("%s was neither claimed nor implemented, so it is skipped rather than failed", s.capability)
				return
			}
			if !rec.failed() {
				t.Fatalf("the gate accepted a harness declaring %s over a Writer that does not implement it; "+
					"the runtime would run this sink degraded and nothing would say so", s.capability)
			}
			// The message has to name the interface (so it can be grepped), the
			// runtime's own assertion (so the reader knows the degradation is real and
			// not a test artefact) and the diagnosis, which is drift and not absence.
			for _, want := range []string{s.capability, "newLiveSink", "drift"} {
				if !strings.Contains(rec.first(), want) {
					t.Errorf("the failure does not mention %q: %s", want, truncate(rec.first(), 400))
				}
			}
			t.Logf("declared-but-absent %s rejected: %s", s.capability, truncate(rec.first(), 260))
		})
	}
}

// TestCapabilityGateRejectsAnImplementedCapabilityTheHarnessNeverDeclared is the
// other direction: a backend implementing all three optional halves whose harness
// claims none of them.
//
// Nothing about that is visible at runtime — the sink works — which is precisely
// the problem: the suite would have run, and passed, obligations the author never
// read, and a backend author's "we do not support that" would silently be a
// backend that does. The declaration is the review artefact, so an undeclared
// contract is a build failure.
func TestCapabilityGateRejectsAnImplementedCapabilityTheHarnessNeverDeclared(t *testing.T) {
	h := newFakeHarness(fakeOpts{})
	h.Capabilities = DeclareCapabilities()

	for _, s := range optionalSuites() {
		t.Run(s.group, func(t *testing.T) {
			rec := recordCapabilityGate(h, s)
			if !rec.failed() {
				t.Fatalf("the gate accepted a harness declaring nothing over a Writer that implements %s; "+
					"the suite would have certified obligations nobody reviewed", s.capability)
			}
			for _, want := range []string{s.capability, "DeclareCapabilities"} {
				if !strings.Contains(rec.first(), want) {
					t.Errorf("the failure does not mention %q: %s", want, truncate(rec.first(), 400))
				}
			}
			t.Logf("implemented-but-undeclared %s rejected: %s", s.capability, truncate(rec.first(), 260))
		})
	}
}

// TestEveryOptionalSuiteRequiresACapability guards the assumption capabilityGate
// rests on: a suite whose requires list is empty is declared by every harness,
// including one that declared nothing, so it would run its properties without ever
// comparing a claim against the backend — the muted-channel hole reopened from the
// inside, and by construction invisible to the two proofs above.
func TestEveryOptionalSuiteRequiresACapability(t *testing.T) {
	for _, s := range optionalSuites() {
		if len(s.requires) == 0 {
			t.Errorf("the %s suite names no required capability, so every harness declares it by "+
				"default and its properties run unchecked against the declaration", s.group)
		}
		for _, c := range s.requires {
			if !slices.Contains(declarableCapabilities(), c) {
				t.Errorf("the %s suite requires %q, which no harness can declare: the half would be "+
					"permanently skipped and nothing would say why", s.group, c)
			}
		}
	}
}

// TestOptionalHarnessValidationRejectsAMissingLever covers the way a backend that
// *does* implement an optional half could still be certified without being
// tested: a harness that never wired up the lever the half's properties need.
// Each omission must be fatal and must name the field.
func TestOptionalHarnessValidationRejectsAMissingLever(t *testing.T) {
	cases := []struct {
		name     string
		validate func(conformanceT, Harness)
		mutBy    func(h *Harness)
		want     string
	}{
		{"noScopeWrites", requireScopeWrites, func(h *Harness) { h.ScopeWrites = nil }, "Harness.ScopeWrites"},
		{"noReadFault", requireReadFault, func(h *Harness) { h.SetReadFault = nil }, "Harness.SetReadFault"},
		{"noProbeOutcome", requireProbeOutcome, func(h *Harness) { h.SetProbeOutcome = nil }, "Harness.SetProbeOutcome"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newFakeHarness(fakeOpts{})
			tc.mutBy(&h)
			rec := &recordingT{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				tc.validate(rec, h)
			}()
			<-done
			if !rec.failed() {
				t.Fatalf("the validator accepted a harness with no %s", tc.want)
			}
			if !strings.Contains(rec.first(), tc.want) {
				t.Fatalf("the validator failed with %q, want it to name %s", rec.first(), tc.want)
			}
		})
	}
}

// TestHarnessValidationRejectsIncompleteHarness covers the other way a backend
// could be certified without being tested: a harness that omits what the suite
// needs. Each omission must be fatal and must name the field.
func TestHarnessValidationRejectsIncompleteHarness(t *testing.T) {
	full := newFakeHarness(fakeOpts{})
	cases := []struct {
		name  string
		mutBy func(h *Harness)
		want  string
	}{
		{"noWriter", func(h *Harness) { h.Writer = nil }, "Harness.Writer"},
		{"noEvents", func(h *Harness) { h.Events = nil }, "Harness.Events"},
		{"noFault", func(h *Harness) { h.SetFault = nil }, "Harness.SetFault"},
		{"noLogicalKey", func(h *Harness) { h.LogicalKey = nil }, "Harness.LogicalKey"},
		{"noDedup", func(h *Harness) { h.Dedup = "" }, "Harness.Dedup"},
		// The zero CapabilitySet is the "I did not think about this" the declaration
		// exists to make unrepresentable, so validate must refuse it exactly as it
		// refuses a nil lever — an undeclared harness has not said what is under test.
		{"noCapabilities", func(h *Harness) { h.Capabilities = CapabilitySet{} }, "Harness.Capabilities"},
		// A misspelling is not a declaration of nothing, it is a declaration of
		// something that does not exist. Left to the gate it would read as "not
		// declared" and be reported as an undeclared capability, which is a correct
		// rejection with a diagnosis pointing at the wrong file.
		{"unknownCapability", func(h *Harness) { h.Capabilities = DeclareCapabilities("sink.Statereader") },
			"Harness.Capabilities"},
		{"noCapacity", func(h *Harness) { h.QueueCapacity = 0 }, "Harness.QueueCapacity"},
		{"shortTimeout", func(h *Harness) { h.EnqueueTimeout = time.Millisecond }, "Harness.EnqueueTimeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := full
			tc.mutBy(&h)
			rec := &recordingT{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.withDefaults().validate(rec)
			}()
			<-done
			if !rec.failed() {
				t.Fatalf("validate accepted a harness with no %s", tc.want)
			}
			if !strings.Contains(rec.first(), tc.want) {
				t.Fatalf("validate failed with %q, want it to name %s", rec.first(), tc.want)
			}
		})
	}
}

// TestDefaultSettleWithinApplies pins the one field a harness may leave zero.
func TestDefaultSettleWithinApplies(t *testing.T) {
	h := Harness{}.withDefaults()
	if h.SettleWithin != defaultSettleWithin {
		t.Fatalf("SettleWithin defaulted to %s, want %s", h.SettleWithin, defaultSettleWithin)
	}
	h = Harness{SettleWithin: time.Minute}.withDefaults()
	if h.SettleWithin != time.Minute {
		t.Fatalf("SettleWithin was overwritten to %s, want the harness's own minute", h.SettleWithin)
	}
}

// truncate keeps a captured failure message readable in the log line that reports
// which obligation caught a fixture.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
