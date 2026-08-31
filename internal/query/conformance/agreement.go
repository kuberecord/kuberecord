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

// This file is the cross-backend half of the gate: two live engines, one corpus,
// the same questions, and the answers required to match.
//
// # Why passing the property suite is not enough
//
// Every backend runs RunQuerySuite and every backend passes it legitimately. But
// each is seeded by its own harness, from its own fixture-building code, into its
// own storage shape. What that certifies is that each engine is internally
// consistent with what its own harness wrote. It says nothing about whether two
// engines agree with each other about *identical* history — and the properties this
// contract is built on are all arguments about reading a given history. Resolving
// the incarnation before the predicates are applied, and settling a short-circuited
// walk on the UID a full scan would have picked, are correctness claims of exactly
// that shape.
//
// So a divergence — two backends resolving a different incarnation, ordering
// nanosecond-adjacent changes differently, disagreeing about which row is the
// reconstruction base — would be invisible to the property suite, because no
// property ever hands both the same past and compares the two answers. This one
// does.
//
// # Where the two are allowed to differ, and how that is expressed
//
// They are allowed to differ exactly where their declarations say so, and the
// difference is asserted by name rather than tolerated. A difference that is merely
// tolerated is a difference nobody will notice becoming a bug.
//
// Two flags have a visible consequence here and both are handled by deriving from
// Capabilities, never by branching on which backend is which:
//
//   - Deletions. A backend declaring it false never receives the corpus's Deleted
//     record, so its answers omit it. Each question states how many deletions the
//     corpus places in its answer, and the assertion is positive on both sides: the
//     declaring backend must return exactly that many, and the other exactly none.
//     Comparing the two is then done over the rows they both could hold.
//   - TimeBoundRequired. A backend declaring it needs a window on every question, so
//     the corpus window is supplied to whichever backend asks for one and the
//     unbounded form is posed as its own check — where the refusal is required from
//     the declaring side and an answer from the other.
//
// A hand-written `if backend == …` inside an assertion would be a failure of this
// file's whole purpose, and there is not one.
//
// # What a disagreement means
//
// It is a finding. Two backends that disagree about anything they both claim to
// support means one of them is wrong, and which one is a human's decision. Nothing
// here should ever be relaxed, and no capability flag should ever be invented, to
// make a real divergence go quiet.

package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The named agreement checks. They are addressable by name for the same reason the
// properties are: the non-vacuity tests run one at a time against a pair of
// backends seeded from deliberately different corpora, and a check that could not
// be named could not be shown to object.
const (
	checkTimelines    = "Agreement/Timelines"
	checkTimeBounds   = "Agreement/UnboundedQuery"
	checkIncarnations = "Agreement/Incarnations"
	checkState        = "Agreement/StateAt"
	checkCoverage     = "Agreement/Coverage"
)

// agreementCheck is one named comparison between two live backends.
type agreementCheck struct {
	name string
	run  func(t conformanceT, a, b Harness)
}

// agreementChecks is the whole of what the two backends are required to agree
// about. A new question belongs here rather than in either backend's own tests.
func agreementChecks() []agreementCheck {
	return []agreementCheck{
		{name: checkTimelines, run: agreeOnTimelines},
		{name: checkTimeBounds, run: agreeOnUnboundedQuery},
		{name: checkIncarnations, run: agreeOnIncarnations},
		{name: checkState, run: agreeOnReconstructions},
		{name: checkCoverage, run: agreeOnCoverage},
	}
}

// agreementCheckByName finds a check in the table, for the non-vacuity tests.
func agreementCheckByName(name string) (agreementCheck, bool) {
	for _, c := range agreementChecks() {
		if c.name == name {
			return c, true
		}
	}
	return agreementCheck{}, false
}

// RunAgreementSuite seeds one corpus into two backends and requires them to answer
// the same questions the same way.
//
// Both harnesses are seeded once, before any check, rather than per check: the
// corpus is a single declarative history and re-planting it for every question
// would multiply the cost of the run against a real store without changing a single
// answer.
//
// The engines are closed when the suite ends. Close is part of the contract, and a
// suite that abandoned two live engines would leak whatever they hold for the rest
// of the test binary.
func RunAgreementSuite(t *testing.T, a, b Harness) {
	t.Helper()

	a.validateForAgreement(t)
	b.validateForAgreement(t)
	defer closeEngine(t, a.Engine)
	defer closeEngine(t, b.Engine)

	if backendOf(a) == backendOf(b) {
		t.Fatalf("conformance: both sides of the agreement suite report the backend %q. The suite exists "+
			"to compare two independent implementations against one history; running it against one "+
			"implementation twice compares a thing with itself and certifies nothing", backendOf(a))
	}

	corpus := AgreementCorpus()
	seedCorpus(t, a, corpus)
	seedCorpus(t, b, corpus)

	for _, c := range agreementChecks() {
		t.Run(c.name, func(t *testing.T) { c.run(t, a, b) })
	}
}

// runAgreementCheck is the single entry point the non-vacuity tests go through, so
// a check is driven identically whether two real backends or two deliberately
// divergent fixtures are on the other end.
//
// It takes the corpus each side is seeded with rather than seeding both from
// AgreementCorpus, which is the whole of what a divergence fixture needs: one
// backend planted with a mutated past, and the check required to notice.
func runAgreementCheck(t conformanceT, c agreementCheck, a, b Harness, corpusA, corpusB Corpus) {
	t.Helper()
	a.validateForAgreement(t)
	b.validateForAgreement(t)
	defer closeEngine(t, a.Engine)
	defer closeEngine(t, b.Engine)

	seedCorpus(t, a, corpusA)
	seedCorpus(t, b, corpusB)
	c.run(t, a, b)
}

// seedCorpus plants the corpus in one backend.
func seedCorpus(t conformanceT, h Harness, c Corpus) {
	t.Helper()
	if err := h.SeedCorpus(c); err != nil {
		t.Fatalf("conformance: %s could not be seeded from the shared corpus: %v; there is nothing to "+
			"compare until both backends hold the same history", backendOf(h), err)
	}
}

// backendOf names a harness's engine, for a failure message that has to say which
// of the two disagreed.
func backendOf(h Harness) string { return h.Engine.Capabilities().Backend }

// ---------------------------------------------------------------------------
// Capability-derived shaping
// ---------------------------------------------------------------------------

// boundedFor supplies the corpus window to a backend that requires one, and leaves
// the question unbounded for a backend that does not.
//
// This is how TimeBoundRequired shapes which query is asked of which backend
// *without* the suite ever knowing which backend it is talking to. The window
// covers the whole corpus with an hour of room at each end, so the two forms of the
// question have the same answer and the comparison stays a comparison.
func boundedFor(caps query.Capabilities, q query.TimelineQuery, c Corpus) query.TimelineQuery {
	if !caps.TimeBoundRequired {
		return q
	}
	q.From, q.To = c.Window()
	return q
}

// projectChanges renders an answer as the backend described by target could have
// given it: the rows that backend's storage can hold, in the order they arrived.
//
// Today there is exactly one such rule, and it is the archive tier's (D12): a
// backend whose history can hold no Deleted row never received one, so its answer
// omits it. Everything else the two backends store, they store alike, and a second
// rule here would need a capability flag to derive it from — which is the point of
// keeping the projection derived rather than written out per backend.
func projectChanges(changes []query.Change, target query.Capabilities) []query.Change {
	if target.Deletions {
		return changes
	}
	kept := make([]query.Change, 0, len(changes))
	for _, c := range changes {
		if c.EventType != query.EventDeleted {
			kept = append(kept, c)
		}
	}
	return kept
}

// countDeletions counts the Deleted rows in an answer.
func countDeletions(changes []query.Change) int {
	n := 0
	for _, c := range changes {
		if c.EventType == query.EventDeleted {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Timelines
// ---------------------------------------------------------------------------

// agreementQuery is one question both backends are asked, and what the corpus says
// about the part of the answer their declarations make them differ over.
type agreementQuery struct {
	// name identifies the question in a subtest name and in every failure message
	// it produces.
	name string
	// query is the question in its unbounded, unlimited form. Bounds are supplied
	// per backend from Capabilities; the limit is posed as a second question, so
	// that a limited answer can be checked against the unlimited one it is a prefix
	// of rather than compared across backends whose unlimited answers differ.
	query query.TimelineQuery
	// limit, when non-zero, is also asked and required to be the first `limit`
	// changes of the same backend's own unlimited answer.
	limit int
	// deletions is how many Deleted rows the corpus places in this question's
	// answer. It is stated rather than counted so the assertion is positive on both
	// sides: a backend that records deletions must return exactly this many, and one
	// that does not must return none.
	deletions int
	// why states what the question is really pinning, for the failure message.
	why string
}

// agreementQueries is the table both backends answer.
//
// Ref is filled in by the runner from the corpus, so a question here states only
// what makes it different from the others.
func agreementQueries() []agreementQuery {
	return []agreementQuery{
		{
			name: "Plain", deletions: 1,
			why: "the flagship question: one object's history, newest incarnation, nothing filtered",
		},
		{
			name: "Reversed", query: query.TimelineQuery{Reverse: true}, deletions: 1,
			why: "the same rows newest first; a backend that sorts in memory and one that reads " +
				"backwards must produce the identical sequence",
		},
		{
			name: "Limited", limit: 3, deletions: 1,
			why: "a limit takes the first rows of the emission order, which is what makes it cheap; " +
				"a backend that took them from the far end would answer a different question",
		},
		{
			name: "ReversedLimited", query: query.TimelineQuery{Reverse: true}, limit: 3, deletions: 1,
			why: "the newest few changes — the shape a short-circuiting backend answers without " +
				"reading the window, and the one it can settle on the wrong rows",
		},
		{
			name:  "ActorFiltered",
			query: query.TimelineQuery{Actors: []string{corpusActorHelm}}, deletions: 0,
			why: "an actor predicate, pushed down on one backend and applied to rows already read on " +
				"the other; the results are required to be identical either way",
		},
		{
			name:  "ActorExcluded",
			query: query.TimelineQuery{ExcludeActors: []string{corpusActorKubectl}}, deletions: 1,
			why: "an exclusion keeps a change that records no actors at all, which is the reading " +
				"that decides whether a deletion survives it",
		},
		{
			name:  "FieldFiltered",
			query: query.TimelineQuery{FieldPaths: []string{"spec.replicas"}}, deletions: 1,
			why: "a field-path predicate keeps every row carrying no patch — the first sighting, the " +
				"full-state fallback, the deletion — because those are the boundaries of the object's " +
				"existence",
		},
		{
			name:  "AllIncarnations",
			query: query.TimelineQuery{AllIncarnations: true}, deletions: 1,
			why: "both objects that wore this name, in ts order across the two, still keyed by UID",
		},
		{
			name:  "PinnedOlderIncarnation",
			query: query.TimelineQuery{UID: corpusUIDA}, deletions: 0,
			why: "the older incarnation, which is never closed — so a backend resolving it by anything " +
				"other than its rows would answer about the wrong object",
		},
		{
			name:  "PinnedNewerIncarnation",
			query: query.TimelineQuery{UID: corpusUIDB}, deletions: 1,
			why: "the newer incarnation, pinned explicitly rather than resolved",
		},
	}
}

// agreeOnTimelines: both backends answer every question in the table the same way,
// down to the nanosecond and the row.
func agreeOnTimelines(t conformanceT, a, b Harness) {
	t.Helper()
	corpus := AgreementCorpus()

	for _, q := range agreementQueries() {
		base := q.query
		base.Ref = corpus.Ref()
		base.Limit = 0

		gotA := agreementTimeline(t, a, boundedFor(capsOf(a), base, corpus), q.name)
		gotB := agreementTimeline(t, b, boundedFor(capsOf(b), base, corpus), q.name)

		assertAgreedChanges(t, a, b, q, gotA, gotB)
		if q.limit > 0 {
			assertLimitIsAPrefix(t, a, q, base, corpus, gotA)
			assertLimitIsAPrefix(t, b, q, base, corpus, gotB)
		}
	}
}

// assertAgreedChanges is the comparison itself: the two answers, over the rows both
// backends can hold, must be the same rows in the same order — and the rows only
// one of them can hold must be exactly the deletions the corpus placed there.
func assertAgreedChanges(t conformanceT, a, b Harness, q agreementQuery, gotA, gotB []query.Change) {
	t.Helper()

	assertDeclaredDeletions(t, a, q, gotA)
	assertDeclaredDeletions(t, b, q, gotB)

	commonA := projectChanges(gotA, capsOf(b))
	commonB := projectChanges(gotB, capsOf(a))
	if len(commonA) == len(commonB) {
		mismatch := -1
		for i := range commonA {
			if !changesEqual(commonA[i], commonB[i]) {
				mismatch = i
				break
			}
		}
		if mismatch < 0 {
			return
		}
		t.Errorf("conformance: %s: %s and %s disagree at row %d about the field(s) named below.\n"+
			"%s: %s\n%s: %s\n%s\nfull %s answer:%s\nfull %s answer:%s\nThis question pins: %s.\n"+
			"A disagreement here is a correctness defect in one of the two backends; do not adjust the "+
			"corpus or relax this assertion to make it stop.",
			q.name, backendOf(a), backendOf(b), mismatch,
			backendOf(a), describeChange(commonA[mismatch]),
			backendOf(b), describeChange(commonB[mismatch]),
			describeChangeDifference(commonA[mismatch], commonB[mismatch]),
			backendOf(a), describeChanges(gotA), backendOf(b), describeChanges(gotB), q.why)
		return
	}
	t.Errorf("conformance: %s: over the rows both backends can hold, %s returned %d change(s) and %s "+
		"returned %d.\nfull %s answer:%s\nfull %s answer:%s\nThis question pins: %s.\n"+
		"A disagreement here is a correctness defect in one of the two backends; do not adjust the "+
		"corpus or relax this assertion to make it stop.",
		q.name, backendOf(a), len(commonA), backendOf(b), len(commonB),
		backendOf(a), describeChanges(gotA), backendOf(b), describeChanges(gotB), q.why)
}

// assertDeclaredDeletions holds one backend to its own declaration about deletions,
// positively and in both directions.
//
// This is the clause that keeps a declared difference from being a skipped case. A
// backend that records deletions must return every one the corpus put in this
// answer; a backend that does not must return none — and if it returned one it
// would be fabricating a row its storage never received, which is the failure
// Invariant 4 exists for.
func assertDeclaredDeletions(t conformanceT, h Harness, q agreementQuery, got []query.Change) {
	t.Helper()

	found := countDeletions(got)
	switch {
	case capsOf(h).Deletions && found != q.deletions:
		t.Errorf("conformance: %s: %s declares Deletions and returned %d deleted row(s) for this "+
			"question, but the corpus places %d there.\nfull answer:%s",
			q.name, backendOf(h), found, q.deletions, describeChanges(got))
	case !capsOf(h).Deletions && found != 0:
		t.Errorf("conformance: %s: %s declares Deletions false — its history never receives a deletion "+
			"at all — yet its answer holds %d deleted row(s). A synthesized deletion closes a timeline "+
			"that merely ended, and tells a reader an object is gone when nothing recorded it going.\n"+
			"full answer:%s", q.name, backendOf(h), found, describeChanges(got))
	}
}

// assertLimitIsAPrefix requires a limited answer to be the first rows of the same
// backend's own unlimited answer.
//
// The limited answers are deliberately not compared across backends. Two backends
// whose unlimited answers legitimately differ by a deletion have limited answers
// that differ by a whole row at the far end — which is arithmetic and not a
// disagreement, and comparing them directly would report it as one. What must hold,
// and is checked on each backend separately, is that the limit took the emission
// order's own prefix: that is the rule that makes a limited query cheap, and taking
// from the far end would mean reading the whole window and sorting it.
func assertLimitIsAPrefix(
	t conformanceT, h Harness, q agreementQuery, base query.TimelineQuery, c Corpus, unlimited []query.Change,
) {
	t.Helper()

	limited := base
	limited.Limit = q.limit
	got := agreementTimeline(t, h, boundedFor(capsOf(h), limited, c), q.name+"/limited")

	want := firstN(unlimited, q.limit)
	assertChanges(t, got, want, fmt.Sprintf("%s: %s answered the limited form of this question with "+
		"rows that are not the first %d of its own unlimited answer", q.name, backendOf(h), q.limit))
}

// agreementTimeline runs one question against one backend and drains it.
func agreementTimeline(t conformanceT, h Harness, q query.TimelineQuery, name string) []query.Change {
	t.Helper()
	it, err := h.Engine.Timeline(context.Background(), q)
	if err != nil {
		t.Fatalf("conformance: %s: %s refused Timeline(%s) with %v; both backends are expected to answer "+
			"this question", name, backendOf(h), describeQuery(q), err)
	}
	return collect(t, it)
}

// describeChangeDifference names the fields two changes differ in, so a failure
// says *what* disagreed rather than leaving the reader to diff two rendered rows.
//
// Naming the field is the whole value of the message. "These two rows differ" sends
// somebody comparing timestamps by eye; "ts differs" or "uid differs" is a
// diagnosis, and for a cross-backend divergence the field is very nearly the bug.
func describeChangeDifference(a, b query.Change) string {
	var differing []string
	add := func(field, left, right string) {
		differing = append(differing, fmt.Sprintf("  %s: %s vs %s", field, left, right))
	}
	if !a.TS.Equal(b.TS) {
		add("ts", a.TS.UTC().Format(time.RFC3339Nano), b.TS.UTC().Format(time.RFC3339Nano))
	}
	if a.EventType != b.EventType {
		add("event_type", a.EventType, b.EventType)
	}
	if a.UID != b.UID {
		add("uid", a.UID, b.UID)
	}
	if a.ResourceVersion != b.ResourceVersion {
		add("resource_version", a.ResourceVersion, b.ResourceVersion)
	}
	if a.APIVersion != b.APIVersion {
		add("api_version", a.APIVersion, b.APIVersion)
	}
	if !slices.Equal(a.Actors, b.Actors) {
		add("actors", fmt.Sprintf("%v", a.Actors), fmt.Sprintf("%v", b.Actors))
	}
	if a.Data != b.Data {
		add("data", a.Data, b.Data)
	}
	if a.Diff != b.Diff {
		add("diff", a.Diff, b.Diff)
	}
	if a.SHA256 != b.SHA256 {
		add("sha256", a.SHA256, b.SHA256)
	}
	if len(differing) == 0 {
		return "  (the two rows compare unequal but no named field differs; labels are the remaining " +
			"candidate)"
	}
	return "fields that disagree:\n" + joinLines(differing)
}

// joinLines renders a list of already-indented lines.
func joinLines(lines []string) string {
	var b bytes.Buffer
	for i, l := range lines {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(l)
	}
	return b.String()
}

// capsOf is the declaration a harness made, which every expectation here is built
// from.
//
// The declaration rather than the engine's own report, for the reason the property
// suite gives: an engine that lies about itself must be caught by the assertion
// rather than quietly excused by it. The two are already known to agree —
// validateForAgreement saw to that before any check ran.
func capsOf(h Harness) query.Capabilities { return h.Capabilities.declaredCapabilities() }

// ---------------------------------------------------------------------------
// The unbounded question
// ---------------------------------------------------------------------------

// agreeOnUnboundedQuery: the one question the two backends answer differently by
// declaration, asserted as the declared difference rather than skipped.
//
// A backend declaring TimeBoundRequired refuses an unbounded query up front with
// ErrTimeBoundRequired, and a backend that does not declare it answers — with the
// same rows its bounded form returned, since the corpus window covers all of its
// history. Both halves matter: a backend that declared the requirement and then
// answered anyway would be one whose caller stops supplying a window, and the day
// the archive grows the query never finishes.
func agreeOnUnboundedQuery(t conformanceT, a, b Harness) {
	t.Helper()
	corpus := AgreementCorpus()
	for _, h := range []Harness{a, b} {
		assertUnboundedBehaviour(t, h, corpus)
	}
}

// assertUnboundedBehaviour holds one backend to its TimeBoundRequired declaration.
func assertUnboundedBehaviour(t conformanceT, h Harness, c Corpus) {
	t.Helper()
	unbounded := query.TimelineQuery{Ref: c.Ref()}

	it, err := h.Engine.Timeline(context.Background(), unbounded)
	if capsOf(h).TimeBoundRequired {
		if it != nil {
			closeIterator(t, it)
		}
		if !errors.Is(err, query.ErrTimeBoundRequired) {
			t.Errorf("conformance: %s declares TimeBoundRequired but answered an unbounded query with "+
				"%v, want query.ErrTimeBoundRequired. The sentinel is what lets a caller refuse the "+
				"query up front and name the flag that fixes it, instead of starting a scan that never "+
				"finishes", backendOf(h), err)
		}
		return
	}
	if err != nil {
		t.Fatalf("conformance: %s does not declare TimeBoundRequired but refused an unbounded query "+
			"with %v", backendOf(h), err)
	}

	got := collect(t, it)
	from, to := c.Window()
	want := agreementTimeline(t, h, query.TimelineQuery{Ref: c.Ref(), From: from, To: to}, "Unbounded")
	assertChanges(t, got, want, fmt.Sprintf("%s answered the unbounded form of the plain question "+
		"differently from the bounded one, though the window covers the whole corpus", backendOf(h)))
}

// ---------------------------------------------------------------------------
// Incarnations
// ---------------------------------------------------------------------------

// agreeOnIncarnations: both backends report the same objects having worn this name,
// in the same order, first seen at the same instants.
//
// LastSeen and Deleted are the two fields a declared capability moves, and both are
// checked against what that backend's own storage could hold rather than against
// the other's answer: on a backend that never receives a deletion, the newest
// incarnation's last recorded change is the change before the one that removed it,
// and Deleted is false because no deletion is in the history — which is a fact about
// the archive and not about the cluster.
func agreeOnIncarnations(t conformanceT, a, b Harness) {
	t.Helper()
	corpus := AgreementCorpus()

	gotA := incarnationsOf(t, a, corpus)
	gotB := incarnationsOf(t, b, corpus)

	if len(gotA) != len(gotB) {
		t.Fatalf("conformance: %s reports %d incarnation(s) of %s/%s and %s reports %d. A "+
			"(namespace, name) pair may be worn by several objects, and two backends counting them "+
			"differently means one of them is splicing or splitting a history.\n%s: %s\n%s: %s",
			backendOf(a), len(gotA), corpusNS, corpusName, backendOf(b), len(gotB),
			backendOf(a), describeIncarnations(gotA), backendOf(b), describeIncarnations(gotB))
	}

	for i := range gotA {
		x, y := gotA[i], gotB[i]
		if x.UID != y.UID || !x.FirstSeen.Equal(y.FirstSeen) {
			t.Errorf("conformance: incarnation %d disagrees between backends.\n%s: %s\n%s: %s\n"+
				"UID and FirstSeen are decided by rows both backends hold, so a difference here is a "+
				"correctness defect in one of them",
				i, backendOf(a), describeIncarnation(x), backendOf(b), describeIncarnation(y))
			continue
		}
		assertIncarnationTail(t, a, x)
		assertIncarnationTail(t, b, y)
	}
}

// assertIncarnationTail checks the two fields a deletion moves, against what this
// backend's declaration says it can hold.
func assertIncarnationTail(t conformanceT, h Harness, got query.Incarnation) {
	t.Helper()

	wantLast, wantDeleted := corpusIncarnationTail(got.UID, capsOf(h))
	if !got.LastSeen.Equal(wantLast) {
		t.Errorf("conformance: %s reports incarnation %s last seen at %s, want %s. On a backend "+
			"declaring Deletions=%t that is the last change its storage can hold for this incarnation",
			backendOf(h), got.UID, got.LastSeen.UTC().Format(time.RFC3339Nano),
			wantLast.UTC().Format(time.RFC3339Nano), capsOf(h).Deletions)
	}
	if got.Deleted != wantDeleted {
		t.Errorf("conformance: %s reports incarnation %s Deleted=%t, want %t. False does not mean the "+
			"object still exists — on a backend that records no deletions it never will — and a reader "+
			"has to qualify the field by the capability rather than read it as a fact about the cluster",
			backendOf(h), got.UID, got.Deleted, wantDeleted)
	}
}

// corpusIncarnationTail is what the corpus says one incarnation's last change and
// deletion state are, on a backend with these capabilities.
func corpusIncarnationTail(uid string, caps query.Capabilities) (lastSeen time.Time, deleted bool) {
	for _, r := range AgreementCorpus().Records {
		if r.Change.UID != uid || !retained(caps, r.Row) {
			continue
		}
		if r.Change.TS.After(lastSeen) {
			lastSeen = r.Change.TS
		}
		if r.Change.EventType == query.EventDeleted {
			deleted = true
		}
	}
	return lastSeen, deleted
}

// incarnationsOf asks one backend which objects wore the corpus's name.
func incarnationsOf(t conformanceT, h Harness, c Corpus) []query.Incarnation {
	t.Helper()
	from, to := c.Window()
	if !capsOf(h).TimeBoundRequired {
		from, to = time.Time{}, time.Time{}
	}
	got, err := h.Engine.Incarnations(context.Background(), c.Ref(), from, to)
	if err != nil {
		t.Fatalf("conformance: %s could not enumerate the incarnations of %s/%s: %v",
			backendOf(h), corpusNS, corpusName, err)
	}
	return got
}

// describeIncarnation and describeIncarnations render an enumeration for a failure
// message.
func describeIncarnation(in query.Incarnation) string {
	return fmt.Sprintf("{uid=%s first=%s last=%s deleted=%t}", in.UID,
		in.FirstSeen.UTC().Format(time.RFC3339Nano), in.LastSeen.UTC().Format(time.RFC3339Nano),
		in.Deleted)
}

func describeIncarnations(list []query.Incarnation) string {
	if len(list) == 0 {
		return "(none)"
	}
	lines := make([]string, 0, len(list))
	for i, in := range list {
		lines = append(lines, fmt.Sprintf("  [%d] %s", i, describeIncarnation(in)))
	}
	return "\n" + joinLines(lines)
}

// ---------------------------------------------------------------------------
// Reconstruction
// ---------------------------------------------------------------------------

// agreementInstant is one instant both backends are asked to reconstruct.
type agreementInstant struct {
	name  string
	after time.Duration
	uid   string
	// afterDeletion marks the instants past the corpus's deletion, where the two
	// backends legitimately part company: one holds the deletion and reports the
	// object gone, the other never received it and reports the last state it has.
	afterDeletion bool
	why           string
}

// agreementInstants are the instants the reconstruction is compared at: on a
// change, between two, and past the end.
func agreementInstants() []agreementInstant {
	return []agreementInstant{
		{
			name: "OnTheNanosecondAdjacentChange", after: corpusNanoFirst,
			why: "exactly on the earlier of two changes one nanosecond apart — the instant a backend " +
				"storing at a coarser precision would resolve to the later one",
		},
		{
			name: "BetweenTwoChanges", after: corpusCheckpoint - time.Minute,
			why: "between two changes, where the answer is the older one's state and not the newer's",
		},
		{
			name: "OnTheCheckpoint", after: corpusCheckpoint,
			why: "on a row carrying both full state and the diff that produced it: the base is the row " +
				"itself and its own diff must not be replayed over the state that diff already made",
		},
		{
			name: "OnTheFullStateFallback", after: corpusFullState,
			why: "on a modification that carries full data because no patch could be produced: it is " +
				"its own base and replaces the document rather than patching it",
		},
		{
			name: "AfterTheStraddlingFlush", after: corpusAfterHour + time.Minute,
			why: "past a flush that crossed an hour boundary, so the base and the patches after it are " +
				"not all where a partition-pruning reader would first look",
		},
		{
			name: "PinnedToTheOlderIncarnation", after: corpusAfterHour, uid: corpusUIDA,
			why: "an instant when the newer incarnation is live, pinned to the older one — which must " +
				"produce the older object's last state and never a blend of the two",
		},
		{
			name: "AfterTheDeletion", after: corpusDeletedB + time.Minute, afterDeletion: true,
			why: "past the recorded deletion, which is the one instant the two backends are declared " +
				"to answer differently",
		},
	}
}

// agreeOnReconstructions: both backends rebuild the same document from the same
// history, and report the same evidence for how.
//
// The provenance is compared as hard as the document, because it is not
// diagnostics: BaseTS and PatchesApplied are what let a reader judge an assertion
// about the past somebody may act on. Two backends producing the same document from
// different bases means one of them found a different row to start from, and the
// next document they are asked for is where that will show.
func agreeOnReconstructions(t conformanceT, a, b Harness) {
	t.Helper()
	corpus := AgreementCorpus()

	for _, in := range agreementInstants() {
		if in.afterDeletion {
			assertDeclaredDeletionDivergence(t, a, b, in, corpus)
			continue
		}
		gotA, errA := reconstruct(a, corpus, in)
		gotB, errB := reconstruct(b, corpus, in)
		if errA != nil || errB != nil {
			t.Errorf("conformance: StateAt %s: %s returned %v and %s returned %v; the corpus holds a "+
				"full-state row before that instant for both. This instant pins: %s",
				in.name, backendOf(a), errA, backendOf(b), errB, in.why)
			continue
		}
		assertReconstructionsAgree(t, a, b, in, gotA, gotB)
	}
}

// assertReconstructionsAgree compares two reconstructions field by field.
func assertReconstructionsAgree(
	t conformanceT, a, b Harness, in agreementInstant, gotA, gotB *query.Reconstruction,
) {
	t.Helper()

	docA, errA := canonicalValue(gotA.Object)
	docB, errB := canonicalValue(gotB.Object)
	if errA != nil || errB != nil {
		t.Fatalf("conformance: StateAt %s: a reconstructed object could not be re-encoded (%s: %v, "+
			"%s: %v)", in.name, backendOf(a), errA, backendOf(b), errB)
	}

	switch {
	case !bytes.Equal(docA, docB):
		t.Errorf("conformance: StateAt %s: the two backends reconstructed different documents.\n"+
			"%s: %s\n%s: %s\nThis instant pins: %s", in.name,
			backendOf(a), docA, backendOf(b), docB, in.why)
	case !gotA.BaseTS.Equal(gotB.BaseTS):
		t.Errorf("conformance: StateAt %s: the two backends replayed from different base rows — %s "+
			"from %s, %s from %s. They agree on the document today; a base chosen differently is the "+
			"same disagreement waiting for a history where it changes the answer. This instant pins: %s",
			in.name, backendOf(a), gotA.BaseTS.UTC().Format(time.RFC3339Nano),
			backendOf(b), gotB.BaseTS.UTC().Format(time.RFC3339Nano), in.why)
	case gotA.BaseEvent != gotB.BaseEvent:
		t.Errorf("conformance: StateAt %s: %s reports a base event of %q and %s reports %q",
			in.name, backendOf(a), gotA.BaseEvent, backendOf(b), gotB.BaseEvent)
	case gotA.PatchesApplied != gotB.PatchesApplied:
		t.Errorf("conformance: StateAt %s: %s applied %d patch(es) over its base and %s applied %d, "+
			"having produced the same document. One of them is reading a row the other is not. "+
			"This instant pins: %s", in.name, backendOf(a), gotA.PatchesApplied,
			backendOf(b), gotB.PatchesApplied, in.why)
	case gotA.SHA256 != gotB.SHA256:
		t.Errorf("conformance: StateAt %s: %s reports sha256 %q and %s reports %q; the digest is the "+
			"one recorded for the last row the replay consumed, so a difference means they finished on "+
			"different rows", in.name, backendOf(a), gotA.SHA256, backendOf(b), gotB.SHA256)
	}
}

// assertDeclaredDeletionDivergence states, by name, what each backend must answer
// at an instant past the recorded deletion.
//
// The two really do differ here and the difference is correct. What must not happen
// is for it to go unstated: a backend that records deletions reports the object
// gone, and one that never receives them reports the last state it holds — which is
// why a renderer over such an archive has to carry the notice that a timeline which
// merely stopped may be a timeline that ended.
func assertDeclaredDeletionDivergence(t conformanceT, a, b Harness, in agreementInstant, c Corpus) {
	t.Helper()
	for _, h := range []Harness{a, b} {
		got, err := reconstruct(h, c, in)
		if capsOf(h).Deletions {
			if !errors.Is(err, query.ErrObjectNotFound) {
				t.Errorf("conformance: StateAt %s: %s declares Deletions and holds the corpus's "+
					"deletion, so an instant after it has no state — it returned (%v, %v), want "+
					"query.ErrObjectNotFound", in.name, backendOf(h), got, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("conformance: StateAt %s: %s declares Deletions false, so its history simply ends "+
				"before this instant with the object's last recorded state intact; it returned %v. The "+
				"archive holds no deletion to stop at, and inventing one would close a timeline that "+
				"merely ended", in.name, backendOf(h), err)
			continue
		}
		doc, encErr := canonicalValue(got.Object)
		if encErr != nil {
			t.Errorf("conformance: StateAt %s: %s produced a state that could not be re-encoded: %v",
				in.name, backendOf(h), encErr)
			continue
		}
		if want := mustCanonicalJSON(corpusStateB6); !bytes.Equal(doc, want) {
			t.Errorf("conformance: StateAt %s: %s reconstructed %s, want the last state its archive "+
				"holds, %s", in.name, backendOf(h), doc, want)
		}
	}
}

// reconstruct asks one backend for a state.
func reconstruct(h Harness, c Corpus, in agreementInstant) (*query.Reconstruction, error) {
	return h.Engine.StateAt(context.Background(), c.Ref(), corpusAt(in.after), in.uid)
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

// agreeOnCoverage: both backends read the same scope log the same way.
//
// Nothing in Capabilities moves this answer, so the two are compared outright. It
// is the answer that makes an empty timeline explicable (Invariant 9), and two
// backends pairing the same transitions into different intervals would tell an
// engineer at 02:47 opposite things about whether anybody was watching.
func agreeOnCoverage(t conformanceT, a, b Harness) {
	t.Helper()
	corpus := AgreementCorpus()

	for _, q := range agreementScopeQueries(corpus) {
		gotA := coverageOf(t, a, q)
		gotB := coverageOf(t, b, q)
		assertIntervalsAgree(t, a, b, q, gotA, gotB)
	}
}

// agreementScopeQueries are the coverage questions the corpus declares scopes for:
// the namespaced scope that was watched, dropped and picked up again, and the
// all-namespaces scope over a different kind that covers an object in a namespace
// it never names.
func agreementScopeQueries(c Corpus) []query.ScopeQuery {
	from, to := c.Window()
	return []query.ScopeQuery{
		{
			ClusterID: FixtureClusterID, APIGroup: corpusGroup, Kind: corpusKind,
			Namespace: corpusNS, From: from, To: to,
		},
		{
			ClusterID: FixtureClusterID, APIGroup: "", Kind: corpusCoveringKind,
			Namespace: corpusCoveringNS, From: from, To: to,
		},
	}
}

// assertIntervalsAgree compares two coverage answers interval by interval.
func assertIntervalsAgree(
	t conformanceT, a, b Harness, q query.ScopeQuery, gotA, gotB []query.ScopeInterval,
) {
	t.Helper()

	if len(gotA) != len(gotB) {
		t.Errorf("conformance: coverage for %s/%s in %q: %s reports %d interval(s) and %s reports %d. "+
			"An interval wrongly closed is the difference between \"nobody is watching this now\" and "+
			"\"we are watching it and nothing has happened\".\n%s:%s\n%s:%s",
			q.APIGroup, q.Kind, q.Namespace, backendOf(a), len(gotA), backendOf(b), len(gotB),
			backendOf(a), describeIntervals(gotA), backendOf(b), describeIntervals(gotB))
		return
	}
	for i := range gotA {
		if intervalsEqual(gotA[i], gotB[i]) {
			continue
		}
		t.Errorf("conformance: coverage for %s/%s in %q: interval %d disagrees.\n%s: %s\n%s: %s",
			q.APIGroup, q.Kind, q.Namespace, i,
			backendOf(a), describeInterval(gotA[i]), backendOf(b), describeInterval(gotB[i]))
	}
}

// intervalsEqual compares two intervals as a caller renders them, with an open one
// distinguishable from one closed at any instant.
func intervalsEqual(x, y query.ScopeInterval) bool {
	if x.APIGroup != y.APIGroup || x.Kind != y.Kind || x.Namespace != y.Namespace ||
		x.RuleRef != y.RuleRef || !x.From.Equal(y.From) {
		return false
	}
	switch {
	case x.To == nil && y.To == nil:
		return true
	case x.To == nil || y.To == nil:
		return false
	default:
		return x.To.Equal(*y.To)
	}
}
