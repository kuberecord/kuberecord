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
// Ten records and four scope transitions, sized for coverage of the named cases
// below and not for volume. Seeding it costs one batch insert against a table, or
// eleven small objects against a store; the whole agreement run is well under ten
// seconds against dockerized backends, which is what keeps it a test somebody runs
// rather than one somebody skips.

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

// Ref is the object every record of the corpus describes.
func (c Corpus) Ref() query.ObjectRef { return corpusRef() }

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

	// The actors the corpus attributes changes to. corpusActorUnknown is the
	// literal the recorder writes when a field manager is missing, empty or not a
	// string; it is a real actor name that a filter must match exactly, and not a
	// marker a reader may treat as absence.
	corpusActorKubectl    = "kubectl"
	corpusActorController = "kube-controller-manager"
	corpusActorHelm       = "helm"
	corpusActorArgo       = "argocd-controller"
	corpusActorUnknown    = "unknown"
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
	corpusAddedA      = 0
	corpusModifiedA   = 5 * time.Minute
	corpusAddedB      = 10 * time.Minute
	corpusNanoFirst   = 12*time.Minute + 1
	corpusNanoSecond  = 12*time.Minute + 2
	corpusCheckpoint  = 15 * time.Minute
	corpusFullState   = 18 * time.Minute
	corpusBeforeHour  = 19*time.Minute + 59900*time.Millisecond
	corpusAfterHour   = 20*time.Minute + 100*time.Millisecond
	corpusDeletedB    = 25 * time.Minute
	corpusScopeOpen   = -40 * time.Minute
	corpusScopeClose  = -20 * time.Minute
	corpusScopeReopen = -10 * time.Minute
	corpusScopeWide   = -5 * time.Minute
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

// corpusRef is the object the corpus records history for.
func corpusRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: FixtureClusterID,
		APIGroup:  corpusGroup,
		Kind:      corpusKind,
		Namespace: corpusNS,
		Name:      corpusName,
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

	return Corpus{
		Records: buildCorpusRecords(corpusRef(), 700, specs),
		Scopes:  corpusScopes(),
	}
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
