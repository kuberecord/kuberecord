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

// This file holds the one history two backends are asked to agree about.
//
// # Why a second kind of fixture
//
// The fixtures in fixtures.go are per-property: each one is the smallest history
// that can pose one question, and each backend is seeded with it from that
// backend's own harness. Every backend therefore passes the suite on its own
// merits — which proves each engine is internally consistent with what its own
// harness wrote, and proves nothing whatsoever about whether two engines agree
// with each other about identical history.
//
// That gap is the one that matters most for the properties this contract is built
// on. Resolving the incarnation before applying predicates, and settling a
// short-circuited walk on the UID a full scan would have picked, are both
// correctness arguments about *reading a given history*. Two backends resolving a
// different incarnation, ordering nanosecond-adjacent changes differently, or
// disagreeing about which row is the reconstruction base would be invisible to a
// per-backend suite, because no property ever hands both the same past and
// compares the two answers.
//
// So this corpus is a single declarative history, seeded into every backend
// through that backend's own writing path, and agreement.go asks both the same
// questions and requires the same answers.
//
// # What it may and may not say
//
// It is written in the vocabulary of query.Change and the frozen schema's columns,
// and in nothing else. No partition, no key, no table, no row group: a corpus that
// named one backend's storage shape would be a description of that backend, and
// the agreement it certified would be agreement about a shape only one of them
// stores.
//
// The one apparent exception is CorpusRecord.Flush, and it is not one. A flush is
// a fact about how history was *recorded* — which records the recorder handed to
// its sink in one go — and every sink has them. What each backend does with that
// fact is its own business, and the two legitimately differ: a backend storing
// rows has no use for it at all, while a backend that batches records into objects
// files the whole flush under the first record's own instant. See Flushes.
//
// # Size
//
// Fourteen records and four scope transitions, sized for coverage of the named
// cases below and not for volume. Seeding it costs one batch insert against a
// table, or fifteen small objects against a store; the whole agreement run is well
// under ten seconds against dockerized backends, which is what keeps it a test
// somebody runs rather than one somebody skips.
//
// # Two identities, and why
//
// Ten of the records are one Deployment's history, and four are Kubernetes Events
// about a Pod that has no history at all. The second identity exists because a
// corpus with one well-recorded object cannot pose the question both backends got
// wrong: they resolved an incarnation before anything else and returned an empty
// answer when the object had no rows of its own, which made the Events about such an
// object unreachable while leaving every question about a recorded object correct
// (D40, Task 18.6).

package conformance

import (
	"fmt"
	"slices"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// CorpusRecord is one recorded change of the shared corpus, together with the
// identity it belongs to and the flush it was recorded in.
//
// It embeds Row rather than restating it, so that a backend whose seeding path
// already takes Rows — every backend that stores one row per change — needs no
// translation at all.
type CorpusRecord struct {
	Row

	// Flush names the writer flush this record was recorded in: records sharing a
	// flush were handed to the sink together, and an empty Flush means this record
	// was alone in its own.
	//
	// It is a property of the recording rather than of any storage, which is why it
	// belongs in a corpus that may not mention storage. A backend that stores one
	// row per change ignores it entirely and is right to. A backend that batches
	// records into a single stored artifact writes one artifact per flush — and
	// that is the only way this corpus can pose the straddling case at all, since
	// an artifact filed under the instant of its first record legitimately goes on
	// accepting records stamped after the boundary it was filed under.
	Flush string
}

// Corpus is the declarative history every backend is seeded with, and the ground
// truth the agreement assertions are stated against.
type Corpus struct {
	// Records are the recorded changes, oldest first.
	Records []CorpusRecord
	// Scopes are the watch-scope transitions that were open while they were
	// recorded, oldest first.
	Scopes []ScopeTransition
}

// Rows renders the corpus as the rows a backend storing one row per change writes.
//
// The flush labels are dropped here rather than hidden, because for such a backend
// they carry no information: two rows written in one flush and two rows written in
// two are the same two rows, and a seeding path that pretended otherwise would be
// inventing a distinction its storage does not have.
func (c Corpus) Rows() []Row {
	rows := make([]Row, 0, len(c.Records))
	for _, r := range c.Records {
		rows = append(rows, r.Row)
	}
	return rows
}

// History renders the corpus in the shape Harness.Seed already takes, for a
// backend whose corpus seeding is its ordinary seeding.
func (c Corpus) History() History {
	return History{Rows: c.Rows(), Scopes: slices.Clone(c.Scopes)}
}

// Flushes groups the records into the flushes they were recorded in, oldest flush
// first and each flush's own records oldest first.
//
// Records with no flush label come back in groups of one, which is what makes the
// grouping safe to apply unconditionally: a backend that batches can walk this and
// write one artifact per group without having to special-case the records that
// were alone.
//
// Ordering is by the flush's *first* record, not by its last, because that is the
// instant a batching writer files the artifact under — and the whole point of the
// straddling case is that a later record in the same flush is stamped past it.
func (c Corpus) Flushes() [][]Row {
	var groups [][]Row
	index := make(map[string]int, len(c.Records))

	for _, r := range c.Records {
		if r.Flush == "" {
			groups = append(groups, []Row{r.Row})
			continue
		}
		at, seen := index[r.Flush]
		if !seen {
			index[r.Flush] = len(groups)
			groups = append(groups, []Row{r.Row})
			continue
		}
		groups[at] = append(groups[at], r.Row)
	}

	for _, g := range groups {
		slices.SortStableFunc(g, func(a, b Row) int { return a.Change.TS.Compare(b.Change.TS) })
	}
	slices.SortStableFunc(groups, func(a, b []Row) int {
		return a[0].Change.TS.Compare(b[0].Change.TS)
	})
	return groups
}

// Ref is the object the corpus's *state* records describe.
func (c Corpus) Ref() query.ObjectRef { return corpusRef() }

// EventsOnlyRef is the corpus's second identity: an object named by Kubernetes
// Events and holding no state records at all.
//
// It is here because the property it poses cannot be posed with one identity. An
// object's own history and the Events about it are independent queries and neither
// gates the other (D40), and the way both backends got that wrong was by resolving
// an incarnation first and returning early when the object had no rows of its own —
// which made the Events unreachable while leaving every question about an object
// that *does* have rows correct. A corpus with a single, well-recorded identity
// cannot tell those apart.
//
// A Pod, deliberately: it is the shape the field report arrived in, and the shape
// the quickstart produces, where a rule captures Events, Deployments and ConfigMaps
// and every Pod in the namespace is named by Events and recorded nowhere else.
func (c Corpus) EventsOnlyRef() query.ObjectRef { return corpusEventsOnlyRef() }

// EventsOnlySubjectUID is the incarnation the corpus's Events name.
//
// The Events carry it in their subject, exactly as a real Event's involvedObject
// does, and no state record anywhere in the corpus does — an incarnation whose
// existence is recorded only in the commentary about it. A question pinning it and
// a question pinning CorpusUnrecordedUID are therefore two different questions, and
// both are asked.
const EventsOnlySubjectUID = "dddddddd-4444-4444-8444-dddddddddddd"

// CorpusUnrecordedUID is an incarnation no record of the corpus names, in either
// half. It is what "a pinned incarnation that was never recorded" means.
const CorpusUnrecordedUID = "ffffffff-6666-4666-8666-ffffffffffff"

// Window is the time bound the corpus fits inside, with room at each end.
//
// It exists because one backend may declare Capabilities.TimeBoundRequired and the
// other may not, and the agreement suite has to be able to ask both of them the
// same question. Supplying this window to the backend that needs one is how that
// difference is expressed as a capability rather than as a branch on a backend's
// name — see boundedFor.
func (c Corpus) Window() (from, to time.Time) {
	return corpusEpoch.Add(-time.Hour), corpusEpoch.Add(2 * time.Hour)
}

// corpusEpoch is the instant the corpus is dated from.
//
// It is fixed rather than relative to now, for the reason suiteEpoch is: a failure
// message names the same timestamps today as it did in the log somebody pasted last
// week. The minute is chosen so that the straddling flush below crosses an hour
// boundary twenty minutes later, which is the case that has to exist and cannot be
// arranged after the fact.
var corpusEpoch = time.Date(2026, 4, 7, 11, 40, 0, 0, time.UTC)

// The identity the corpus records history for. It is deliberately not the
// per-property fixtures' identity: a failure message naming payments/checkout is
// unambiguously about the shared corpus, and a backend that leaked one fixture's
// rows into another's read cannot do so unnoticed.
const (
	corpusGroup  = "apps"
	corpusKind   = "Deployment"
	corpusNS     = "payments"
	corpusName   = "checkout"
	corpusAPIVer = "apps/v1"

	// corpusUIDA and corpusUIDB are two incarnations under one (namespace, name).
	// A is the older one and is deliberately never closed: incarnation resolution
	// has to pick B because B's rows are newer, not because A's history ended in a
	// deletion — which is a distinction that would otherwise be invisible on a
	// backend that records no deletions at all.
	corpusUIDA = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	corpusUIDB = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"

	// The events-only subject: a Pod in the same namespace, in the core group, with
	// no state record of its own anywhere in the corpus. Its name is a real replica
	// set suffix rather than a tidy one, because a subject predicate is matched on
	// the name and a name with punctuation in it is the one a JSON path extraction
	// can mangle.
	corpusEventsOnlyKind   = "Pod"
	corpusEventsOnlyName   = "checkout-7d4f-abcde"
	corpusEventsOnlyAPIVer = "v1"

	// The actors the corpus attributes changes to. corpusActorUnknown is the
	// literal the recorder writes when a field manager is missing, empty or not a
	// string; it is a real actor name that a filter must match exactly, and not a
	// marker a reader may treat as absence.
	corpusActorKubectl    = "kubectl"
	corpusActorController = "kube-controller-manager"
	corpusActorHelm       = "helm"
	corpusActorArgo       = "argocd-controller"
	corpusActorUnknown    = "unknown"

	// corpusActorKubelet writes the corpus's Events. An Event's actors are the field
	// managers of the Event object — whoever wrote the Event, never whoever changed
	// the object it is about — so this is a name no state record of the corpus
	// carries, which makes an actor predicate leaking into the commentary visible.
	corpusActorKubelet = "kubelet"
)

// The kind and the second group a Kubernetes Event is recorded under.
//
// The core group is the empty string and is spelled as a value rather than named,
// for the reason the scope log's namespace is: a wildcard spelling of it would read
// as "any group" at exactly the call sites that mean "the core one".
const (
	corpusEventKind        = "Event"
	corpusEventGroupModern = "events.k8s.io"
)

// corpusFlushRotation is the one flush holding more than a single record.
//
// Its two records sit either side of an hour boundary, so a backend that files a
// flush under its first record's instant stores them together under the earlier
// hour while the later one is stamped into the next. That is a shape a backend
// with a time-partitioned layout has to reason about explicitly and a backend with
// an index gets for free, and it is exactly the kind of asymmetry that can produce
// two different answers to one question.
const corpusFlushRotation = "rotation"

// The offsets the corpus records at, named because the agreement assertions and
// any failure they produce refer to them.
const (
	corpusAddedA     = 0
	corpusModifiedA  = 5 * time.Minute
	corpusAddedB     = 10 * time.Minute
	corpusNanoFirst  = 12*time.Minute + 1
	corpusNanoSecond = 12*time.Minute + 2
	corpusCheckpoint = 15 * time.Minute
	corpusFullState  = 18 * time.Minute
	corpusBeforeHour = 19*time.Minute + 59900*time.Millisecond
	corpusAfterHour  = 20*time.Minute + 100*time.Millisecond
	corpusDeletedB   = 25 * time.Minute

	// The Events about the events-only subject, interleaved with the Deployment's
	// own history rather than parked beside it: two backends that ordered a
	// correlated Event by the wrong instant would still agree if every Event sat
	// outside the range the rest of the corpus occupies.
	// All four sit inside the hour the corpus opens in, and deliberately: the
	// straddling flush is the *only* thing this corpus files under one partition and
	// stamps into the next, and TestTheAgreementCorpusReallyStraddlesAnHour asserts
	// that by requiring the later partition to hold nothing else at all. An Event
	// past the boundary would be a second, unrelated occupant of it and would make
	// that guard fail without anything being wrong.
	corpusEventScheduled = 3 * time.Minute
	corpusEventPulling   = 8 * time.Minute
	corpusEventFailed    = 14 * time.Minute
	corpusEventKilling   = 18*time.Minute + 30*time.Second
	corpusScopeOpen      = -40 * time.Minute
	corpusScopeClose     = -20 * time.Minute
	corpusScopeReopen    = -10 * time.Minute
	corpusScopeWide      = -5 * time.Minute
)

// The rules that opened and closed the corpus's scopes, and the kind the
// all-namespaces scope watches.
const (
	corpusRuleFirst  = "streamrule/payments/checkout"
	corpusRuleSecond = "streamrule/payments/checkout-v2"
	corpusRuleWide   = "clusterstreamrule/configmaps"

	corpusCoveringKind = "ConfigMap"
	corpusCoveringNS   = "kube-system"
)

// The states the corpus passes through, written out rather than derived, so the
// expectation and any replay are independent accounts of the same history.
//
// The documents hold nothing but strings, booleans, small integers, objects and
// arrays — see canonicalJSON for why that constraint is load-bearing.
const (
	corpusStateA0 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{"replicas":1}}`
	corpusStateA1 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{"replicas":2}}`

	corpusStateB0 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"public"},"paused":false,"replicas":4,"revisions":["r1"]}}`
	corpusStateB1 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"public"},"paused":false,"replicas":5,"revisions":["r1"]}}`
	corpusStateB2 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"public"},"paused":true,"replicas":5,"revisions":["r1"]}}`
	corpusStateB3 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"public"},"paused":true,"replicas":5,"revisions":["r1","r2"]}}`

	// The state the recorder could not produce a patch for. It differs from the
	// one before it in two places at once — a redaction policy took effect over
	// spec.env.TOKEN in the same observation that changed the replica count — and
	// the row therefore carries full data and no diff at all. A replay must
	// *replace* the document with it rather than look for a patch to apply.
	corpusStateB4 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"[REDACTED]"},"paused":true,"replicas":6,"revisions":["r1","r2"]}}`

	corpusStateB5 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"[REDACTED]"},"paused":true,"replicas":7,"revisions":["r1","r2"]}}`
	corpusStateB6 = `{"kind":"Deployment","metadata":{"name":"checkout"},"spec":{` +
		`"env":{"TOKEN":"[REDACTED]"},"paused":false,"replicas":7,"revisions":["r1","r2"]}}`
)

// The patches between those states.
//
// The checkpoint's own patch appends to an array on purpose, exactly as the
// reconstruction fixture's does: a checkpoint carries both the patch and the state
// that patch produced, and applying it a second time on top of that state has to be
// *visible*. An append run twice leaves a duplicate; a replace run twice is
// indistinguishable from a replace run once and would let a double-applying backend
// agree with a correct one.
const (
	corpusPatchA1 = `[{"op":"replace","path":"/spec/replicas","value":2}]`
	corpusPatchB1 = `[{"op":"replace","path":"/spec/replicas","value":5}]`
	corpusPatchB2 = `[{"op":"replace","path":"/spec/paused","value":true}]`
	corpusPatchB3 = `[{"op":"add","path":"/spec/revisions/-","value":"r2"}]`
	corpusPatchB5 = `[{"op":"replace","path":"/spec/replicas","value":7}]`
	corpusPatchB6 = `[{"op":"replace","path":"/spec/paused","value":false}]`
)

// corpusRef is the object the corpus records state history for.
func corpusRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: FixtureClusterID,
		APIGroup:  corpusGroup,
		Kind:      corpusKind,
		Namespace: corpusNS,
		Name:      corpusName,
	}
}

// corpusEventsOnlyRef is the object the corpus records only commentary about.
func corpusEventsOnlyRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: FixtureClusterID,
		APIGroup:  "",
		Kind:      corpusEventsOnlyKind,
		Namespace: corpusNS,
		Name:      corpusEventsOnlyName,
	}
}

// corpusAt is the instant a corpus offset falls on.
func corpusAt(after time.Duration) time.Time { return corpusEpoch.Add(after) }

// corpusSpec is one record of the corpus, written the way a person reads history,
// plus the flush it was recorded in.
//
// It is changeSpec with a flush and a full-state escape hatch, rather than
// changeSpec itself, because the corpus needs one row buildRows cannot express: a
// Modified carrying full data and no diff. Encoding that as a variant here keeps
// buildRows describing exactly the row shapes the per-property fixtures use.
type corpusSpec struct {
	after  time.Duration
	event  string
	uid    string
	actors []string
	// state is the object's full state after this change: the document a full-state
	// row stores, and the document every non-deletion row's sha256 is the digest of.
	state string
	// diff is the RFC 6902 patch this row recorded, where it recorded one.
	diff string
	// fullState forces a Modified row to carry its state in the data column, which
	// is what the recorder writes when it could not produce a patch.
	fullState bool
	// flush names the writer flush this record was handed to the sink in.
	flush string
}

// AgreementCorpus is the shared history both backends are seeded with.
//
// Every case named below is one where two independently written engines could
// plausibly disagree, and each is here because the disagreement would be quiet:
// the wrong answer looks like an answer.
//
//   - Two incarnations under one (namespace, name), the older never closed. A
//     timeline that spliced them is a coherent account of an object that never
//     existed (Invariant 7), and "the newest incarnation" has to be decided by the
//     rows rather than by a deletion that one backend cannot even store.
//   - Two changes one nanosecond apart. The schema records at nanosecond precision;
//     a backend that ordered them by anything coarser puts the effect before the
//     cause, and does so only for the pair that is close enough to matter.
//   - A Checkpoint carrying both data and the diff that produced it. Whether a
//     replay bases itself on it, and whether it re-applies its diff, are two
//     separate ways to be wrong and both produce a plausible document.
//   - A Modified carrying full data from the diff-failure fallback, which must
//     replace the document and not be searched for a patch.
//   - A flush straddling an hour boundary: two records handed to the sink together,
//     the second stamped after the hour the first was filed under.
//   - A redacted value carrying the [REDACTED] sentinel, which is stored content
//     and not an absence.
//   - An actor set with several managers, and one recorded as `unknown` — a real
//     name a filter must match, not a marker meaning "no actor".
//   - A deletion, which one backend can store and the other never receives (D12).
//     The disagreement it causes is correct, declared, and asserted by name.
//   - Four Kubernetes Events about a *second* object that has no state records at
//     all, in both API spellings. An Event names its subject in its own row, so the
//     two halves of a merged timeline are independent queries and neither gates the
//     other (D40) — and a backend that resolves an incarnation first and stops when
//     there is none answers this question with an emptiness it never measured, which
//     is what both of them did until Task 18.6. It is the one case in this corpus
//     that no fake can exhibit (D42), which is exactly why it belongs to the pair of
//     live engines rather than to either one's own tests.
func AgreementCorpus() Corpus {
	specs := []corpusSpec{
		{after: corpusAddedA, event: query.EventAdded, uid: corpusUIDA,
			actors: []string{corpusActorKubectl}, state: corpusStateA0},
		{after: corpusModifiedA, event: query.EventModified, uid: corpusUIDA,
			// Two field managers on one change, sorted as the schema records them.
			actors: []string{corpusActorController, corpusActorKubectl},
			state:  corpusStateA1, diff: corpusPatchA1},

		// The name is reused: a second object, no relation to the first beyond the
		// (namespace, name) it inherits. The first is left unclosed.
		{after: corpusAddedB, event: query.EventAdded, uid: corpusUIDB,
			actors: []string{corpusActorHelm}, state: corpusStateB0},

		// One nanosecond apart.
		{after: corpusNanoFirst, event: query.EventModified, uid: corpusUIDB,
			actors: []string{corpusActorUnknown}, state: corpusStateB1, diff: corpusPatchB1},
		{after: corpusNanoSecond, event: query.EventModified, uid: corpusUIDB,
			actors: []string{corpusActorHelm}, state: corpusStateB2, diff: corpusPatchB2},

		{after: corpusCheckpoint, event: query.EventCheckpoint, uid: corpusUIDB,
			actors: []string{corpusActorArgo, corpusActorHelm, corpusActorUnknown},
			state:  corpusStateB3, diff: corpusPatchB3},

		{after: corpusFullState, event: query.EventModified, uid: corpusUIDB,
			actors: []string{corpusActorHelm}, state: corpusStateB4, fullState: true},

		// One flush, two records, an hour boundary between them.
		{after: corpusBeforeHour, event: query.EventModified, uid: corpusUIDB,
			actors: []string{corpusActorKubectl}, state: corpusStateB5, diff: corpusPatchB5,
			flush: corpusFlushRotation},
		{after: corpusAfterHour, event: query.EventModified, uid: corpusUIDB,
			actors: []string{corpusActorKubectl}, state: corpusStateB6, diff: corpusPatchB6,
			flush: corpusFlushRotation},

		{after: corpusDeletedB, event: query.EventDeleted, uid: corpusUIDB},
	}

	// The two identities' records are concatenated and then ordered by instant, so
	// that Records keeps the "oldest first" the type promises across both. The order
	// is not what either backend keys on — a table sorts on insert and an archive
	// files by partition — but a corpus whose own declaration was out of order would
	// be read by somebody as evidence about ordering.
	records := append(buildCorpusRecords(corpusRef(), 700, specs), corpusEventRecords()...)
	slices.SortStableFunc(records, func(a, b CorpusRecord) int {
		return a.Change.TS.Compare(b.Change.TS)
	})

	return Corpus{Records: records, Scopes: corpusScopes()}
}

// corpusEventSpec is one Kubernetes Event of the corpus, written the way a person
// reads a cluster's commentary.
type corpusEventSpec struct {
	after    time.Duration
	apiGroup string
	name     string
	reason   string
}

// corpusEventRecords are the Events naming the events-only subject.
//
// # Why four, and why both spellings
//
// Four is what the field report found sitting in the sink while the command-line
// client reported none, and the reasons are the four that matter most to an
// engineer: a Pod that was scheduled, pulled an image, failed to schedule and was
// killed. Two of them are
// about something that failed to exist or is ceasing to, which is the class D32 says
// capture-time correlation cannot serve.
//
// Both spellings, because v1/Event and events.k8s.io/v1/Event are one storage behind
// two APIs and a cluster's rules may name either. A backend correlating one of them
// would return half the commentary with nothing in the answer marking it short — and
// on this identity the *whole* answer is commentary, so half of it is the difference
// between two Events and four.
//
// # What the records carry, and what they deliberately do not
//
// Each Event is an ordinary Added record of the Event object itself: its own kind,
// its own name, its own uid, its own actors — a kubelet, which no state record of the
// corpus attributes anything to, so commentary leaking through an actor predicate is
// visible. The subject travels in the data, which is the only place an Event names it
// and the only place a read-time correlation may look.
//
// The subject carries EventsOnlySubjectUID, exactly as a real involvedObject does. No
// state record names that uid, which is what makes a timeline pinned to it a question
// worth asking: the incarnation is recorded only in the commentary about it.
//
// Each is alone in its own flush. They arrived from the Event stream rather than from
// the Deployment's, so batching them together with the object's own history would be
// describing a recording that did not happen.
func corpusEventRecords() []CorpusRecord {
	specs := []corpusEventSpec{
		{after: corpusEventScheduled, apiGroup: "", name: "checkout-7d4f.17a9e1", reason: "Scheduled"},
		{after: corpusEventPulling, apiGroup: corpusEventGroupModern,
			name: "checkout-7d4f.17a9e2", reason: "Pulling"},
		{after: corpusEventFailed, apiGroup: "", name: "checkout-7d4f.17a9e3", reason: "FailedScheduling"},
		{after: corpusEventKilling, apiGroup: corpusEventGroupModern,
			name: "checkout-7d4f.17a9e4", reason: "Killing"},
	}

	subject := corpusEventsOnlyRef()
	records := make([]CorpusRecord, 0, len(specs))
	for i, spec := range specs {
		// The subject key is the one that group spells it with: involvedObject in the
		// core group, regarding in events.k8s.io. Writing both would let a backend that
		// reads only one of them pass.
		key, apiVersion := "involvedObject", corpusEventsOnlyAPIVer
		if spec.apiGroup != "" {
			key, apiVersion = "regarding", corpusEventGroupModern+"/v1"
		}
		data := mustCanonicalJSON(fmt.Sprintf(
			`{"reason":%q,%q:{"kind":%q,"namespace":%q,"name":%q,"uid":%q}}`,
			spec.reason, key, subject.Kind, subject.Namespace, subject.Name, EventsOnlySubjectUID))

		records = append(records, CorpusRecord{Row: Row{
			Ref: query.ObjectRef{
				ClusterID: FixtureClusterID,
				APIGroup:  spec.apiGroup,
				Kind:      corpusEventKind,
				Namespace: corpusNS,
				Name:      spec.name,
			},
			Change: query.Change{
				TS:              corpusAt(spec.after),
				EventType:       query.EventAdded,
				UID:             fmt.Sprintf("event-%d", i+1),
				ResourceVersion: fmt.Sprintf("%d", 800+i),
				APIVersion:      apiVersion,
				Actors:          []string{corpusActorKubelet},
				Data:            string(data),
				SHA256:          sha256Hex(data),
			},
		}})
	}
	return records
}

// corpusScopes is the watch-scope log the corpus declares: one scope watched,
// dropped and picked up again by a second rule, plus a still-open all-namespaces
// scope over a different kind.
//
// The unmatched trailing Started is the interesting one, for the reason the
// coverage fixture's is: an interval wrongly closed is the difference between
// "nobody is watching this now" and "we are watching it and nothing has happened",
// and two backends pairing transitions differently would give an engineer opposite
// readings of the same log.
func corpusScopes() []ScopeTransition {
	return []ScopeTransition{
		{Action: ScopeStarted, APIGroup: corpusGroup, Kind: corpusKind, Namespace: corpusNS,
			RuleRef: corpusRuleFirst, TS: corpusAt(corpusScopeOpen)},
		{Action: ScopeStopped, APIGroup: corpusGroup, Kind: corpusKind, Namespace: corpusNS,
			RuleRef: corpusRuleFirst, TS: corpusAt(corpusScopeClose)},
		{Action: ScopeStarted, APIGroup: corpusGroup, Kind: corpusKind, Namespace: corpusNS,
			RuleRef: corpusRuleSecond, TS: corpusAt(corpusScopeReopen)},
		{Action: ScopeStarted, APIGroup: "", Kind: corpusCoveringKind, Namespace: "",
			RuleRef: corpusRuleWide, TS: corpusAt(corpusScopeWide)},
	}
}

// buildCorpusRecords turns specs into records, filling the data, diff and sha256
// columns from the schema's own rule about which event type carries what.
//
// It is buildRows' sibling rather than a call into it, because of the one row
// buildRows deliberately cannot produce: a Modified whose patch could not be
// computed and which therefore carries full state. Teaching buildRows about it
// would let a per-property fixture produce that row by accident; keeping the two
// apart means each says exactly what its own fixtures need.
func buildCorpusRecords(ref query.ObjectRef, firstRV int, specs []corpusSpec) []CorpusRecord {
	records := make([]CorpusRecord, 0, len(specs))
	for i, s := range specs {
		c := query.Change{
			TS:              corpusAt(s.after),
			EventType:       s.event,
			UID:             s.uid,
			ResourceVersion: fmt.Sprintf("%d", firstRV+i),
			APIVersion:      corpusAPIVer,
			Actors:          s.actors,
		}
		switch s.event {
		case query.EventAdded, query.EventSnapshot:
			c.Data = string(mustCanonicalJSON(s.state))
			c.SHA256 = sha256Hex([]byte(c.Data))
		case query.EventCheckpoint:
			// Data is the state *after* the diff this row also records, which is why
			// a replay must not apply that diff on top of it.
			c.Data = string(mustCanonicalJSON(s.state))
			c.Diff = s.diff
			c.SHA256 = sha256Hex([]byte(c.Data))
		case query.EventModified:
			c.SHA256 = sha256Hex(mustCanonicalJSON(s.state))
			if s.fullState {
				c.Data = string(mustCanonicalJSON(s.state))
			} else {
				c.Diff = s.diff
			}
		case query.EventDeleted:
			// No data, no diff, no hash and no actors: there is no live object left
			// to attribute one to.
			c.Actors = nil
		default:
			panic("conformance: the corpus uses an event type it does not know: " + s.event)
		}
		records = append(records, CorpusRecord{Row: Row{Ref: ref, Change: c}, Flush: s.flush})
	}
	return records
}
