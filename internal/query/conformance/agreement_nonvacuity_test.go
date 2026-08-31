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

// This file is why the agreement suite can be trusted, and it exists for a failure
// mode that is specific to a cross-backend comparison and much quieter than the
// property suite's.
//
// A suite that seeds two backends from the same variable and then compares nothing
// meaningful passes every pair it is ever handed. Worse, it *looks* like the
// strongest test in the repository while doing so: two real engines, a real corpus,
// a green run. So the checks are driven here against pairs that are deliberately
// seeded with different pasts, and each check must object — naming the question and
// the field, because "these two backends disagree" without either is a result
// nobody can act on at 02:47.
//
// The pair is a full-capability engine and a truthfully reduced one, which is the
// shape the real run has (an indexed store beside an archive tier that records no
// deletions). Running the non-vacuity argument against two identical engines would
// leave every capability-derived branch in agreement.go unexecuted, which is the
// half most likely to be silently wrong.

package conformance

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// The names the two sides of a fixture pair report, so a failure message shows
// which engine gave which answer rather than printing one name twice.
const (
	agreementSideA = "reference-indexed"
	agreementSideB = "reference-archive"
)

// namedHarness gives a fake engine a backend name of its own.
//
// The suite prints Capabilities().Backend on both sides of every disagreement, and
// two fixtures reporting the identical name would produce a message in which the
// two answers cannot be told apart — which is the one thing a divergence report
// must never do.
func namedHarness(h Harness, name string) Harness {
	h.Engine.(*fakeEngine).report.Backend = name
	return h
}

// agreeingPair is the compliant pair: a full-capability engine and a truthfully
// reduced one, both correct.
func agreeingPair() (a, b Harness, cleanup func()) {
	ha, closeA := newFakeHarness(flaws{})
	hb, closeB := newReducedHarness()
	return namedHarness(ha, agreementSideA), namedHarness(hb, agreementSideB), func() {
		closeA()
		closeB()
	}
}

// TestAgreementSuitePassesTwoAgreeingBackends is half of the argument: the
// comparison is satisfiable.
//
// Both engines are correct and both are seeded from AgreementCorpus, so the suite
// must have nothing to say about them — including about the two places their
// declarations make them differ, which have to be recognised as declared rather than
// reported as disagreement.
func TestAgreementSuitePassesTwoAgreeingBackends(t *testing.T) {
	a, b, cleanup := agreeingPair()
	defer cleanup()
	RunAgreementSuite(t, a, b)
}

// TestAgreementSuiteRejectsOneBackendTwice guards the premise of the whole run.
//
// Comparing an engine with itself agrees by construction, and a pair wired up by
// accident to the same backend would produce the greenest possible result while
// certifying nothing at all.
func TestAgreementSuiteRejectsOneBackendTwice(t *testing.T) {
	ha, closeA := newFakeHarness(flaws{})
	defer closeA()
	hb, closeB := newFakeHarness(flaws{})
	defer closeB()

	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runAgreementSuiteOn(rec, ha, hb)
	}()
	<-done

	if !rec.failed() {
		t.Fatalf("the agreement suite accepted two harnesses reporting the same backend; comparing an " +
			"implementation with itself is agreement by construction")
	}
	if !strings.Contains(rec.first(), fakeBackend) {
		t.Errorf("the rejection does not name the backend reported twice: %s", truncate(rec.first()))
	}
}

// runAgreementSuiteOn is the same-backend guard, reachable from a recorder.
//
// RunAgreementSuite takes a *testing.T because it runs its checks as subtests, and a
// Fatalf on a real T is not observable from the test that provoked it — the reason
// the whole suite is written against conformanceT in the first place.
func runAgreementSuiteOn(t conformanceT, a, b Harness) {
	t.Helper()
	a.validateForAgreement(t)
	b.validateForAgreement(t)
	if backendOf(a) == backendOf(b) {
		t.Fatalf("conformance: both sides of the agreement suite report the backend %q", backendOf(a))
	}
}

// divergence is one way two backends can be made to hold different pasts, and the
// checks that must notice.
type divergence struct {
	name string
	what string
	// mutate is the corpus the second backend is seeded with; nil seeds it with the
	// shared one, for a fixture whose defect is in the engine rather than the past.
	mutate func(Corpus) Corpus
	// flaws is the deliberate defect the first backend's engine carries. The zero
	// value is an engine with none.
	flaws flaws
	// catches are the checks this fixture must be rejected by, at minimum. A fixture
	// may well trip others.
	catches []caught
}

// caught is one check a divergence must be rejected by, and what its rejection has
// to say.
type caught struct {
	check string
	// names are substrings the rejection must contain: the question that disagreed
	// and, where one field carries the disagreement, that field. A failure saying
	// only "these two differ" leaves the reader to find the divergence themselves,
	// which for two backends and a corpus of ten records is most of the work.
	names []string
}

// TestAgreementSuiteIsNonVacuous seeds one backend with a mutated corpus and
// requires the comparison to fail, naming what disagreed.
func TestAgreementSuiteIsNonVacuous(t *testing.T) {
	fixtures := []divergence{
		{
			name:   "shiftedTimestamp",
			what:   "moves the earlier of two nanosecond-adjacent changes past the later one",
			mutate: corpusWithShiftedTimestamp,
			// The ordering dimension. Two changes a nanosecond apart is the pair a
			// backend storing at a coarser precision reorders, and the reordering is
			// invisible in every rendering that does not print sub-second digits.
			catches: []caught{
				{check: checkTimelines, names: []string{"Plain", "ts"}},
				{check: checkState, names: []string{"OnTheNanosecondAdjacentChange"}},
			},
		},
		{
			name:   "relabelledIncarnation",
			what:   "records the newest incarnation under a different UID",
			mutate: corpusWithRelabelledIncarnation,
			// The incarnation dimension, in the shape that is hardest to see: the
			// same number of rows, at the same instants, about a different object.
			catches: []caught{
				{check: checkTimelines, names: []string{"Plain", "uid"}},
				{check: checkIncarnations, names: []string{"uid="}},
			},
		},
		{
			name:   "droppedIncarnation",
			what:   "never recorded the older incarnation at all",
			mutate: corpusWithoutOlderIncarnation,
			catches: []caught{
				{check: checkTimelines, names: []string{"AllIncarnations"}},
				{check: checkIncarnations, names: []string{"incarnation(s)"}},
				{check: checkState, names: []string{"PinnedToTheOlderIncarnation"}},
			},
		},
		{
			name:   "shiftedScope",
			what:   "closes the first watch interval twenty minutes late",
			mutate: corpusWithShiftedScope,
			catches: []caught{
				{check: checkCoverage, names: []string{"apps/Deployment", "interval"}},
			},
		},
		{
			name:  "unboundedDespiteDeclaration",
			what:  "declares that a time bound is required and then answers an unbounded query anyway",
			flaws: flaws{scanUnbounded: true},
			catches: []caught{
				{check: checkTimeBounds, names: []string{"TimeBoundRequired"}},
			},
		},
	}

	covered := map[string]bool{}
	for _, f := range fixtures {
		for _, c := range f.catches {
			covered[c.check] = true
		}
		t.Run(f.name, func(t *testing.T) {
			for _, c := range f.catches {
				t.Run(c.check, func(t *testing.T) {
					runDivergence(t, f, c)
				})
			}
		})
	}

	// A check with no fixture behind it is untested machinery: it could be comparing
	// nothing and this file would never notice.
	for _, c := range agreementChecks() {
		if !covered[c.name] {
			t.Errorf("agreement check %s has no fixture proving it can fail; add one to the table above",
				c.name)
		}
	}
}

// runDivergence drives one check against one divergent pair and asserts it objects.
func runDivergence(t *testing.T, f divergence, expect caught) {
	t.Helper()

	checkName := expect.check
	c, ok := agreementCheckByName(checkName)
	if !ok {
		t.Fatalf("no agreement check named %q; the fixture table names one the suite does not run",
			checkName)
	}

	ha, closeA := newFakeHarness(f.flaws)
	defer closeA()
	hb, closeB := newReducedHarness()
	defer closeB()

	corpusB := AgreementCorpus()
	if f.mutate != nil {
		corpusB = f.mutate(corpusB)
	}

	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runAgreementCheck(rec, c, namedHarness(ha, agreementSideA), namedHarness(hb, agreementSideB),
			AgreementCorpus(), corpusB)
	}()

	select {
	case <-done:
	case <-time.After(propertyTimeout):
		t.Fatalf("%s did not terminate within %s against a pair that %s: a check that hangs rejects "+
			"nothing", checkName, propertyTimeout, f.what)
	}

	if !rec.failed() {
		t.Fatalf("%s passed against a pair where one backend %s: the check compares nothing about the "+
			"answers it collects, which is the exact failure a cross-backend suite has to be tested for",
			checkName, f.what)
	}
	for _, want := range expect.names {
		if !strings.Contains(rec.first(), want) {
			t.Errorf("%s rejected the pair, but its message never names %q, so a reader is told two "+
				"backends disagree without being told about what: %s", checkName, want, truncate(rec.first()))
		}
	}
	t.Logf("%s rejected it: %s", checkName, truncate(rec.first()))
}

// TestAgreementCheckNamesAreUnique guards the assumption both the fixture table and
// agreementCheckByName rest on. Two checks sharing a name would mean a fixture
// silently exercising the wrong one — proving the comparison it names to be untested
// while reporting the opposite.
func TestAgreementCheckNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range agreementChecks() {
		if seen[c.name] {
			t.Errorf("two agreement checks are named %q; agreementCheckByName resolves only the first",
				c.name)
		}
		seen[c.name] = true
	}
}

// ---------------------------------------------------------------------------
// The mutated corpora
// ---------------------------------------------------------------------------
//
// Each one is a single, named edit to the shared corpus, and each lives here rather
// than beside the corpus itself for the reason the broken engines do: a mutation
// switch in corpus.go would ship, and the first person to reach for it would find a
// supported way to make the two sides differ.

// corpusWithShiftedTimestamp moves the earlier of the two nanosecond-adjacent
// records to one nanosecond *after* the later one, so the two swap places.
//
// Nothing else about the record changes, which is what makes it the right probe: the
// same rows, the same contents, one order — and an agreement suite that compared
// sets rather than sequences, or compared timestamps at anything coarser than the
// nanosecond, would call the two answers equal.
func corpusWithShiftedTimestamp(c Corpus) Corpus {
	out := cloneCorpus(c)
	for i := range out.Records {
		if out.Records[i].Change.TS.Equal(corpusAt(corpusNanoFirst)) {
			out.Records[i].Change.TS = corpusAt(corpusNanoSecond + 1)
		}
	}
	slices.SortStableFunc(out.Records, func(x, y CorpusRecord) int {
		return x.Change.TS.Compare(y.Change.TS)
	})
	return out
}

// corpusWithRelabelledIncarnation records the newest incarnation's changes under a
// different UID.
//
// This is the divergence that is hardest to see and worst to have: the same number
// of rows, at the same instants, with the same contents, describing a different
// object. Every rendering of it looks right.
func corpusWithRelabelledIncarnation(c Corpus) Corpus {
	const otherUID = "cccccccc-3333-4333-8333-cccccccccccc"
	out := cloneCorpus(c)
	for i := range out.Records {
		if out.Records[i].Change.UID == corpusUIDB {
			out.Records[i].Change.UID = otherUID
		}
	}
	return out
}

// corpusWithoutOlderIncarnation drops every record of the incarnation that wore the
// name first.
//
// The name is reused constantly in a real cluster, and a backend that lost the
// earlier object would answer "this is the history of payments/checkout" with the
// history of only its most recent occupant — which reads as a complete account.
func corpusWithoutOlderIncarnation(c Corpus) Corpus {
	out := cloneCorpus(c)
	kept := out.Records[:0]
	for _, r := range out.Records {
		if r.Change.UID != corpusUIDA {
			kept = append(kept, r)
		}
	}
	out.Records = kept
	return out
}

// corpusWithShiftedScope closes the first watch interval later than it was closed.
//
// Coverage is what makes an empty timeline explicable (Invariant 9), so two backends
// disagreeing about when somebody stopped watching is two backends disagreeing about
// whether "nothing changed" or "nobody was looking" is the right reading of the same
// silence.
func corpusWithShiftedScope(c Corpus) Corpus {
	out := cloneCorpus(c)
	for i := range out.Scopes {
		if out.Scopes[i].Action == ScopeStopped {
			out.Scopes[i].TS = out.Scopes[i].TS.Add(20 * time.Minute)
		}
	}
	return out
}

// cloneCorpus copies a corpus far enough that a mutation cannot reach the shared
// one.
//
// Only the records and the transitions are copied; the slices and maps reachable
// from a change are shared, and every mutation above replaces a field rather than
// writing through one.
func cloneCorpus(c Corpus) Corpus {
	return Corpus{Records: slices.Clone(c.Records), Scopes: slices.Clone(c.Scopes)}
}
