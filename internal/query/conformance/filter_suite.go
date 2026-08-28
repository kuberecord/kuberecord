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

package conformance

import (
	"slices"

	"github.com/kuberecord/kuberecord/internal/query"
)

const (
	propFilterActorInclude = "Filters/ActorInclude"
	propFilterActorExclude = "Filters/ActorExclude"
	propFilterFieldPaths   = "Filters/FieldPaths"
	propFilterAgreement    = "Filters/AgreesWithNonPushdown"
)

// filterCases are the queries the filter properties pose, with the rows each must
// keep named by their index into filterHistory.
//
// Every case is stated as indices rather than as a predicate on purpose. A
// predicate would be a second implementation of the filtering rules, and two
// implementations of one rule agree with each other far more readily than either
// agrees with the contract — which is also why the agreement property below is not
// on its own enough.
func filterCases() []filterCase {
	base := func() query.TimelineQuery { return fixtureQuery() }

	include := base()
	include.Actors = []string{actorKubectl}

	exclude := base()
	exclude.ExcludeActors = []string{actorController}

	both := base()
	both.Actors = []string{actorKubectl}
	both.ExcludeActors = []string{actorKubectl}

	replicas := base()
	replicas.FieldPaths = []string{pathReplicas}

	containers := base()
	containers.FieldPaths = []string{pathContainers}

	return []filterCase{
		{
			name:  "ActorInclude",
			query: include,
			keeps: []int{0, 1, 3},
			why: "a non-empty Actors list keeps the changes at least one of those actors touched — and " +
				"necessarily drops every deletion, which records no actors at all",
		},
		{
			name:  "ActorExclude",
			query: exclude,
			keeps: []int{0, 1, 4, 5},
			why: "ExcludeActors drops a change if any of its actors is named, so a change made by two " +
				"actors is dropped when either is excluded",
		},
		{
			name:  "ActorInBothLists",
			query: both,
			keeps: nil,
			why: "ExcludeActors is applied after Actors and wins on conflict: the narrower, safer reading " +
				"when a caller has contradicted itself",
		},
		{
			name:  "FieldPathShallow",
			query: replicas,
			keeps: []int{0, 1, 5},
			why: "a field-path filter keeps the changes whose patch touches the path, plus every row that " +
				"carries no patch — the first sighting and the deletion are the boundaries of the " +
				"object's existence and a filtered timeline without them implies it had neither",
		},
		{
			name:  "FieldPathPrefix",
			query: containers,
			keeps: []int{0, 3, 5},
			why: "the dotted grammar matches by prefix, so spec.template.spec.containers matches a patch " +
				"on spec.template.spec.containers.0.image",
		},
	}
}

// runFilterCases asserts the named cases against the engine's own answer.
func runFilterCases(t conformanceT, h Harness, names ...string) {
	t.Helper()
	history := filterHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	for _, c := range filterCases() {
		if len(names) > 0 && !slices.Contains(names, c.name) {
			continue
		}
		got := timelineChanges(t, h, c.query)
		want := expectedIndices(caps, history.Rows, c.keeps)
		assertChanges(t, got, want, c.name+": "+c.why)
	}
}

// filtersActorInclude: a non-empty Actors list narrows the timeline to the changes
// those actors touched.
func filtersActorInclude(t conformanceT, h Harness) {
	t.Helper()
	runFilterCases(t, h, "ActorInclude")
}

// filtersActorExclude: ExcludeActors drops the changes those actors touched, and
// wins over Actors when a caller names the same actor twice.
func filtersActorExclude(t conformanceT, h Harness) {
	t.Helper()
	runFilterCases(t, h, "ActorExclude", "ActorInBothLists")
}

// filtersFieldPaths: a field-path filter keeps the changes touching the path, and
// keeps every row that carries no patch at all.
func filtersFieldPaths(t conformanceT, h Harness) {
	t.Helper()
	runFilterCases(t, h, "FieldPathShallow", "FieldPathPrefix")
}

// filtersAgreeWithNonPushdown: a filtered result is identical whether the predicate
// was pushed into the backend or applied to rows already read.
//
// The contract makes this a requirement rather than an aspiration, because the
// alternative is two backends answering the same question differently with neither
// of them wrong about its own implementation — and an engineer comparing an archive
// against a database concluding that one of them lost rows.
//
// The comparison is against a reference engine this package carries: a complete,
// deliberately unoptimised implementation that applies every predicate client-side.
// It is the non-pushdown half a single backend under test cannot supply. The
// absolute expectations above are what keep the pair honest — a bug the reference
// and the backend happened to share would satisfy this property and fail those.
func filtersAgreeWithNonPushdown(t conformanceT, h Harness) {
	t.Helper()
	history := filterHistory()
	seed(t, h, history)

	caps := h.Capabilities.declaredCapabilities()
	reference := newReferenceEngine(history, caps)
	defer closeEngine(t, reference)

	for _, c := range filterCases() {
		got := engineChanges(t, h.Engine, c.query)
		want := engineChanges(t, reference, c.query)
		assertChanges(t, got, want,
			c.name+": the backend under test and a non-pushdown engine over the same history disagree. "+
				"A filtered result is required to be identical either way ("+c.why+")")
	}
}
