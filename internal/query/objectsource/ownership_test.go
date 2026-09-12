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

package objectsource

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// ownershipOptions is the engine configuration the ownership tests use.
//
// A non-empty prefix deliberately, for the reason the suite's own fixtures use one:
// an empty prefix is the simpler configuration and the one a bug in prefix handling
// would survive.
func ownershipOptions() Options { return Options{Prefix: "audit"} }

// TestOwnershipReadsTheEdgesOfAScope asserts the whole answer against the shared
// fixture, which is what makes it an agreement with the table backend rather than
// with a past written for this file (D42).
func TestOwnershipReadsTheEdgesOfAScope(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := engineOver(t, fixture.History, ownershipOptions())

	got, err := engine.Ownership(context.Background(), fixture.Query)
	if err != nil {
		t.Fatalf("Ownership: %v", err)
	}
	if !reflect.DeepEqual(got, fixture.Objects) {
		t.Fatalf("the ownership read returned\n%+v\nwant\n%+v", got, fixture.Objects)
	}
}

// TestOwnershipRefusesAnUnboundedWindow keeps the ownership read under the same
// rule every other question here obeys.
//
// There is no index to seek with, so an unbounded ownership read is a scan of the
// whole archive — and this backend refuses those up front rather than starting one
// and never finishing, which is the same outcome with none of the explanation.
func TestOwnershipRefusesAnUnboundedWindow(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, spy := engineOver(t, fixture.History, ownershipOptions())

	q := fixture.Query
	q.From, q.To = time.Time{}, time.Time{}
	_, err := engine.Ownership(context.Background(), q)
	if err == nil {
		t.Fatal("an unbounded ownership read was accepted")
	}
	if len(spy.opened()) != 0 {
		t.Fatalf("the refusal still opened %d objects; a question this backend cannot answer must "+
			"cost nothing", len(spy.opened()))
	}
}

// TestOwnershipReportsAnUnreadableObject is the difference between this read and a
// timeline, and it is the reason it is spelled as a failure.
//
// A timeline delivers what it read and reports the rest, because a missing change is
// a missing change. A missing row here is a missing *edge*, and a walk over a tree
// with an absent edge reports the subtree below it as not existing — the traversal
// form of the empty result Invariant 9 forbids.
func TestOwnershipReportsAnUnreadableObject(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, spy := engineOver(t, fixture.History, ownershipOptions())

	// Discover the keys by running the read once, then refuse one of them.
	if _, err := engine.Ownership(context.Background(), fixture.Query); err != nil {
		t.Fatalf("Ownership: %v", err)
	}
	keys := spy.opened()
	if len(keys) == 0 {
		t.Fatal("the ownership read opened nothing, so there is no object to make unreadable")
	}
	spy.refuseKey(keys[0], errors.New("the archive refused this object"))

	if _, err := engine.Ownership(context.Background(), fixture.Query); err == nil {
		t.Fatal("an ownership read that could not read one of its objects returned a tree anyway; " +
			"a short answer here is a tree with an edge silently missing from it")
	}
}

// TestTimelineCorrelatesTheEventsOfASubject is the read-plane half of `--owned` at
// this backend: one scan, one merged stream, the object's own changes still the
// root's.
func TestTimelineCorrelatesTheEventsOfASubject(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := engineOver(t, fixture.History, ownershipOptions())

	root := fixture.Objects[2].Ref    // the Deployment
	subject := fixture.Objects[0].Ref // the Pod the fixture's Event names

	var events, changes int
	for _, change := range drain(t, engine, query.TimelineQuery{
		Ref: root, From: fixture.Query.From, To: fixture.Query.To,
		IncludeEvents: true, Subjects: []query.ObjectRef{subject},
	}) {
		if change.EventType == query.EventKubernetes {
			events++
			continue
		}
		changes++
	}
	if events != 1 {
		t.Fatalf("the timeline carried %d Events, want the one naming %s", events, subject.Name)
	}
	if changes != 1 {
		t.Fatalf("the timeline carried %d of the object's own changes, want the Deployment's one. "+
			"Subjects widens the Event correlation and never the state half", changes)
	}
}

// TestSubjectsDoNotInheritThePinnedIncarnation is the trap correlatedEvent.root
// exists around.
//
// A dependent does not share its owner's UID, so a pin applied to the whole merged
// stream would correlate the root's Events and drop the tree's — a well-formed
// answer with the widening silently undone by the narrowing beside it.
func TestSubjectsDoNotInheritThePinnedIncarnation(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := engineOver(t, fixture.History, ownershipOptions())

	var events int
	for _, change := range drain(t, engine, query.TimelineQuery{
		Ref: fixture.Objects[2].Ref, From: fixture.Query.From, To: fixture.Query.To,
		UID:           fixture.Objects[2].UID,
		IncludeEvents: true, Subjects: []query.ObjectRef{fixture.Objects[0].Ref},
	}) {
		if change.EventType == query.EventKubernetes {
			events++
		}
	}
	if events != 1 {
		t.Fatalf("a pinned root returned %d Events for its tree, want 1", events)
	}
}
