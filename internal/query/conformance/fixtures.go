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

// This file holds the known pasts the properties assert against.
//
// Every fixture is small enough to read in one screen and is built from literal
// documents and literal RFC 6902 patches rather than from a diffing library. That
// is not only the dependency budget talking: a fixture whose patches were computed
// would make the expectation a function of the same code the property is checking,
// and the point of a hand-written base, a hand-written patch and a hand-written
// result is that all three can be read and disagreed with.
//
// The documents hold nothing but strings, booleans, small integers, objects and
// arrays. See canonicalJSON for why that constraint is load-bearing rather than
// stylistic.

package conformance

import (
	"fmt"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// suiteEpoch is the instant every fixture is dated from.
//
// It is fixed rather than relative to now so that a failure message names the same
// timestamps today as it did in the log somebody pasted last week, and so that a
// backend storing at a coarser precision than the schema's fails on the fixture's
// own spacing rather than on whatever the clock happened to be doing.
var suiteEpoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// FixtureClusterID is the cluster every fixture records its history under, and
// the value a harness must stamp on whatever its backend stores a cluster
// identity in.
//
// It is exported for one reason, and the reason is a hole a harness cannot
// otherwise fill: ScopeTransition carries no cluster of its own, and the coverage
// fixture seeds scopes without a single Row beside them, so there is nothing a
// backend could infer the cluster from. A ScopeQuery still arrives asking about
// this one. Without this constant a harness would either hardcode the literal —
// duplicating it once per backend, to fail obscurely the day it changed — or
// store its scope log outside the identity its engine really filters on, which
// would quietly excuse the very predicate the coverage properties exist to check.
const FixtureClusterID = "conformance"

// The identities every fixture uses.
const (
	fixtureGroup  = "apps"
	fixtureKind   = "Deployment"
	fixtureNS     = "default"
	fixtureName   = "checkout"
	fixtureAPIVer = "apps/v1"

	// uidA and uidB are two incarnations under one (namespace, name). They are the
	// whole of the property most likely to be got wrong: a name may be reused, and
	// a timeline that splices the two is a coherent-looking account of an object
	// that never existed (Invariant 7).
	uidA = "11111111-1111-1111-1111-111111111111"
	uidB = "22222222-2222-2222-2222-222222222222"

	// The actors the filter fixture attributes changes to.
	actorKubectl    = "kubectl"
	actorController = "kube-controller-manager"
	actorHelm       = "helm"
)

// fixtureRef is the object every fixture records history for.
func fixtureRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: FixtureClusterID,
		APIGroup:  fixtureGroup,
		Kind:      fixtureKind,
		Namespace: fixtureNS,
		Name:      fixtureName,
	}
}

// changeSpec is one row of a fixture, written the way a person reads history
// rather than the way a row stores it.
//
// The difference is state. A recorded row carries a full document only when its
// event type says so, but every non-deletion row carries the *digest* of the state
// it left the object in, so a spec names that state once and buildRows works out
// which columns it lands in. Encoding the schema's own rule here rather than in
// each fixture is what keeps a fixture from accidentally describing a row shape no
// backend would ever store.
type changeSpec struct {
	// after places the row at suiteEpoch + after.
	after time.Duration
	// event is one of the query package's event-type constants.
	event string
	// uid is the incarnation this row belongs to.
	uid string
	// actors are the field managers seen on the object. Ignored on a deletion,
	// which attributes to nobody.
	actors []string
	// state is the object's full state *after* this change: the document a
	// full-state row stores, and the document every non-deletion row's sha256 is
	// the digest of.
	state string
	// diff is the RFC 6902 patch this row recorded, on a Modified or a Checkpoint.
	diff string
	// labels are the object's labels at observation.
	labels map[string]string
}

// buildRows turns specs into rows, filling the data, diff and sha256 columns from
// the schema's own rule about which event type carries what.
//
// firstRV starts the resourceVersion sequence. Rows carry distinct, ascending
// resource versions because a property comparing whole changes needs every field
// to be a real one — an expectation built from rows that all shared a version
// would pass a backend that returned the right count of the wrong rows.
func buildRows(ref query.ObjectRef, firstRV int, specs []changeSpec) []Row {
	rows := make([]Row, 0, len(specs))
	for i, s := range specs {
		c := query.Change{
			TS:              suiteEpoch.Add(s.after),
			EventType:       s.event,
			UID:             s.uid,
			ResourceVersion: fmt.Sprintf("%d", firstRV+i),
			APIVersion:      fixtureAPIVer,
			Actors:          s.actors,
			Labels:          s.labels,
		}
		switch s.event {
		case query.EventAdded, query.EventSnapshot:
			c.Data = string(mustCanonicalJSON(s.state))
			c.SHA256 = sha256Hex([]byte(c.Data))
		case query.EventCheckpoint:
			// A checkpoint carries both: data is the state *after* the diff it
			// also records, which is why a replay must not apply that diff on top
			// of it.
			c.Data = string(mustCanonicalJSON(s.state))
			c.Diff = s.diff
			c.SHA256 = sha256Hex([]byte(c.Data))
		case query.EventModified:
			c.Diff = s.diff
			c.SHA256 = sha256Hex(mustCanonicalJSON(s.state))
		case query.EventDeleted:
			// No data, no diff, no hash and no actors: there is no live object left
			// to attribute one to, and the emptiness is the honest answer rather
			// than a missing one.
			c.Actors = nil
		default:
			panic("conformance: fixture uses an event type buildRows does not know: " + s.event)
		}
		rows = append(rows, Row{Ref: ref, Change: c})
	}
	return rows
}

// at is the instant a fixture's nth offset falls on, for a property that needs to
// name one in a query.
func at(after time.Duration) time.Time { return suiteEpoch.Add(after) }

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

// orderingOffsets are the instants the ordering fixture records at.
//
// The first three are one nanosecond apart, which is the whole point of the
// fixture: the schema records at nanosecond precision, and a backend that stored at
// microseconds would collapse them into an arbitrary order. Two changes a
// microsecond apart are two changes, and an audit timeline that reordered them
// would put the effect before the cause.
var orderingOffsets = []time.Duration{0, 1, 2, time.Second, 2 * time.Second, 3 * time.Second}

// orderingHistory is one object, one incarnation, six changes.
func orderingHistory() History {
	specs := make([]changeSpec, 0, len(orderingOffsets))
	for i, off := range orderingOffsets {
		s := changeSpec{
			after:  off,
			event:  query.EventModified,
			uid:    uidA,
			actors: []string{actorKubectl},
			state:  fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, i+1),
			diff:   fmt.Sprintf(`[{"op":"replace","path":"/spec/replicas","value":%d}]`, i+1),
		}
		if i == 0 {
			s.event = query.EventAdded
			s.diff = ""
		}
		specs = append(specs, s)
	}
	return History{Rows: buildRows(fixtureRef(), 100, specs)}
}

// ---------------------------------------------------------------------------
// Incarnations
// ---------------------------------------------------------------------------

// incarnationOffsets place uidA's three changes before uidB's two, so "the newest
// incarnation" is unambiguous and a merge is visible as extra rows rather than as
// a different order.
var incarnationOffsets = []time.Duration{0, time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute}

// incarnationHistory is two UIDs under one (namespace, name): the object was
// deleted and something else was created with the same name.
//
// This is the shape Kubernetes produces constantly — a Deployment deleted and
// reapplied, a StatefulSet's PVC recreated — and the shape a backend gets wrong by
// answering "the history of default/checkout" instead of "the history of this
// incarnation of default/checkout".
func incarnationHistory() History {
	off := incarnationOffsets
	specs := []changeSpec{
		{after: off[0], event: query.EventAdded, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":1}}`},
		{after: off[1], event: query.EventModified, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":2}}`,
			diff:  `[{"op":"replace","path":"/spec/replicas","value":2}]`},
		{after: off[2], event: query.EventDeleted, uid: uidA},
		{after: off[3], event: query.EventAdded, uid: uidB, actors: []string{actorHelm},
			state: `{"kind":"Deployment","spec":{"replicas":9}}`},
		{after: off[4], event: query.EventModified, uid: uidB, actors: []string{actorHelm},
			state: `{"kind":"Deployment","spec":{"replicas":8}}`,
			diff:  `[{"op":"replace","path":"/spec/replicas","value":8}]`},
	}
	return History{Rows: buildRows(fixtureRef(), 200, specs)}
}

// incarnationRows splits a seeded incarnation history by UID, so a property can
// state its expectation as "uidB's rows" rather than by index.
func incarnationRows(h History, uid string) []Row {
	var out []Row
	for _, r := range h.Rows {
		if r.Change.UID == uid {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Reconstruction
// ---------------------------------------------------------------------------

// The instants the reconstruction fixture records at, named because the property
// asserts on which one a replay based itself on.
const (
	reconAdded       = 0
	reconFirstPatch  = time.Minute
	reconSecondPatch = 2 * time.Minute
	reconCheckpoint  = 3 * time.Minute
	reconLastPatch   = 4 * time.Minute
)

// The states the reconstruction fixture passes through, written out rather than
// derived, so the expectation and the replay are independent accounts of the same
// history.
const (
	reconState0 = `{"kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":1,"paused":false,"history":["r1"]}}`
	reconState1 = `{"kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":2,"paused":false,"history":["r1"]}}`
	reconState2 = `{"kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":2,"paused":true,"history":["r1"]}}`
	reconState3 = `{"kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":2,"paused":true,"history":["r1","r2"]}}`
	reconState4 = `{"kind":"Deployment","metadata":{"name":"checkout"},` +
		`"spec":{"replicas":5,"paused":true,"history":["r1","r2"]}}`
)

// The patches between them.
//
// The checkpoint's own patch appends to an array on purpose. A checkpoint carries
// both the patch and the state that patch produced, and applying it a second time
// on top of that state must be *visible*: an append run twice leaves a duplicate,
// whereas a replace run twice is indistinguishable from a replace run once and
// would let a double-applying backend pass.
const (
	reconPatch1 = `[{"op":"replace","path":"/spec/replicas","value":2}]`
	reconPatch2 = `[{"op":"replace","path":"/spec/paused","value":true}]`
	reconPatch3 = `[{"op":"add","path":"/spec/history/-","value":"r2"}]`
	reconPatch4 = `[{"op":"replace","path":"/spec/replicas","value":5}]`
)

// reconstructionHistory is one incarnation walked from a first sighting through
// two patches, a checkpoint, and a final patch.
//
// It is built to pin the three things a replay can get wrong: which row it based
// itself on, whether it applied the checkpoint's own diff on top of the state that
// diff already produced, and whether it stopped a patch short.
func reconstructionHistory() History {
	specs := []changeSpec{
		{after: reconAdded, event: query.EventAdded, uid: uidA,
			actors: []string{actorKubectl}, state: reconState0},
		{after: reconFirstPatch, event: query.EventModified, uid: uidA,
			actors: []string{actorKubectl}, state: reconState1, diff: reconPatch1},
		{after: reconSecondPatch, event: query.EventModified, uid: uidA,
			actors: []string{actorController}, state: reconState2, diff: reconPatch2},
		{after: reconCheckpoint, event: query.EventCheckpoint, uid: uidA,
			actors: []string{actorController}, state: reconState3, diff: reconPatch3},
		{after: reconLastPatch, event: query.EventModified, uid: uidA,
			actors: []string{actorKubectl}, state: reconState4, diff: reconPatch4},
	}
	return History{Rows: buildRows(fixtureRef(), 300, specs)}
}

// ---------------------------------------------------------------------------
// Deletion
// ---------------------------------------------------------------------------

// deletionOffsets place the deletion last, which is the shape that makes the
// capability matter: a timeline that ends in a deletion and a timeline that ends
// because the archive holds no deletions look identical from the outside.
var deletionOffsets = []time.Duration{0, time.Minute, 2 * time.Minute}

// deletionHistory is one incarnation created, changed and deleted.
func deletionHistory() History {
	off := deletionOffsets
	specs := []changeSpec{
		{after: off[0], event: query.EventAdded, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":1}}`},
		{after: off[1], event: query.EventModified, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":3}}`,
			diff:  `[{"op":"replace","path":"/spec/replicas","value":3}]`},
		{after: off[2], event: query.EventDeleted, uid: uidA},
	}
	return History{Rows: buildRows(fixtureRef(), 500, specs)}
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

// The scope fixture's instants and the rules that opened and closed each scope.
const (
	scopeFirstStart  = 0
	scopeFirstStop   = time.Hour
	scopeSecondStart = 2 * time.Hour
	scopeWideStart   = 30 * time.Minute

	scopeRuleFirst  = "streamrule/default/deployments"
	scopeRuleSecond = "streamrule/default/deployments-v2"
	scopeRuleWide   = "clusterstreamrule/configmaps"

	// coveringKind is watched by an all-namespaces scope, which is what the
	// covering reading of ScopeQuery.Namespace is for: a cluster-wide rule really
	// was watching an object in kube-system, and answering "never observed" about
	// it would be false.
	coveringKind = "ConfigMap"
	coveringNS   = "kube-system"
)

// coverageHistory is one scope watched, dropped and picked up again by a second
// rule, plus a still-open all-namespaces scope over a different kind.
//
// The unmatched second Started is the interesting row: it is what a backend has to
// leave open rather than pair with the Stopped that preceded it, and an interval
// wrongly closed is the difference between "nobody is watching this now" and "we
// are watching it and nothing has happened".
func coverageHistory() History {
	return History{Scopes: []ScopeTransition{
		{Action: ScopeStarted, APIGroup: fixtureGroup, Kind: fixtureKind, Namespace: fixtureNS,
			RuleRef: scopeRuleFirst, TS: at(scopeFirstStart)},
		{Action: ScopeStopped, APIGroup: fixtureGroup, Kind: fixtureKind, Namespace: fixtureNS,
			RuleRef: scopeRuleFirst, TS: at(scopeFirstStop)},
		{Action: ScopeStarted, APIGroup: fixtureGroup, Kind: fixtureKind, Namespace: fixtureNS,
			RuleRef: scopeRuleSecond, TS: at(scopeSecondStart)},
		{Action: ScopeStarted, APIGroup: "", Kind: coveringKind, Namespace: "",
			RuleRef: scopeRuleWide, TS: at(scopeWideStart)},
	}}
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

// The filter fixture's instants, named because the expectations are stated as
// which rows survive.
var filterOffsets = []time.Duration{0, time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute, 5 * time.Minute}

// The paths the filter fixture's patches touch, in the dotted grammar
// TimelineQuery.FieldPaths uses.
const (
	pathReplicas   = "spec.replicas"
	pathContainers = "spec.template.spec.containers"
)

// filterHistory is one incarnation whose changes were made by different actors and
// touched different parts of the object.
//
// Two of its rows carry no patch at all — the first sighting and the deletion —
// and they are there because a field-path filter must keep them. They are the
// boundaries of the object's existence, and a filtered timeline that dropped them
// would show a history with no beginning and no end and imply the object had
// neither.
func filterHistory() History {
	off := filterOffsets
	specs := []changeSpec{
		{after: off[0], event: query.EventAdded, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":1}}`},
		{after: off[1], event: query.EventModified, uid: uidA, actors: []string{actorKubectl},
			state: `{"kind":"Deployment","spec":{"replicas":2}}`,
			diff:  `[{"op":"replace","path":"/spec/replicas","value":2}]`},
		{after: off[2], event: query.EventModified, uid: uidA, actors: []string{actorController},
			state: `{"kind":"Deployment","status":{"readyReplicas":2}}`,
			diff:  `[{"op":"replace","path":"/status/readyReplicas","value":2}]`},
		{after: off[3], event: query.EventModified, uid: uidA,
			actors: []string{actorController, actorKubectl},
			state:  `{"kind":"Deployment","spec":{"image":"checkout:v2"}}`,
			diff:   `[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"checkout:v2"}]`},
		{after: off[4], event: query.EventModified, uid: uidA, actors: []string{actorHelm},
			state: `{"kind":"Deployment","metadata":{"labels":{"team":"payments"}}}`,
			diff:  `[{"op":"add","path":"/metadata/labels/team","value":"payments"}]`},
		{after: off[5], event: query.EventDeleted, uid: uidA},
	}
	return History{Rows: buildRows(fixtureRef(), 400, specs)}
}

// filterCase is one filter and the rows it must keep, named by index into the
// filter fixture.
//
// Stating the expectation as indices rather than as a predicate is deliberate: a
// predicate would be a second implementation of the filter, and two implementations
// of the same rule agree with each other far more readily than either agrees with
// the contract.
type filterCase struct {
	name  string
	query query.TimelineQuery
	keeps []int
	// why explains what the case is really pinning, for the failure message.
	why string
}

// expectedIndices renders the named rows of a fixture as the changes a query must
// return, dropping those this backend could never have stored.
func expectedIndices(caps query.Capabilities, rows []Row, indices []int) []query.Change {
	want := make([]query.Change, 0, len(indices))
	for _, i := range indices {
		if retained(caps, rows[i]) {
			want = append(want, rows[i].Change)
		}
	}
	return want
}
