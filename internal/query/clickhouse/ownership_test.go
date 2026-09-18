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

package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// TestOwnershipReadsTheEdgesOfAScope asserts the whole answer against the shared
// fixture, which is what makes it an agreement with the archive backend rather than
// with a past written for this file.
func TestOwnershipReadsTheEdgesOfAScope(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := seededEngine(t, fixture.History)

	got, err := engine.Ownership(context.Background(), fixture.Query)
	if err != nil {
		t.Fatalf("Ownership: %v", err)
	}
	if !reflect.DeepEqual(got, fixture.Objects) {
		t.Fatalf("the ownership read returned\n%+v\nwant\n%+v", got, fixture.Objects)
	}
}

// TestOwnershipLeavesKubernetesEventsOutOfTheGroup pins the exclusion in the
// *statement*, which is the only place it is observable.
//
// An Event owns nothing and is owned by nothing, so a read that folded them would
// return the identical tree — and would pay for the whole of a cluster's commentary
// to do it. A cost decision whose absence cannot change the answer is exactly the
// kind that has to be pinned where it is written down.
func TestOwnershipLeavesKubernetesEventsOutOfTheGroup(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, conn := seededEngine(t, fixture.History)

	if _, err := engine.Ownership(context.Background(), fixture.Query); err != nil {
		t.Fatalf("Ownership: %v", err)
	}

	statements := conn.statements()
	if len(statements) != 1 {
		t.Fatalf("an ownership read issued %d statements, want one: %v", len(statements), statements)
	}
	if !strings.Contains(statements[0], "NOT (kind = 'Event' AND api_group IN ('', 'events.k8s.io'))") {
		t.Fatalf("the ownership read does not exclude Kubernetes Events:\n%s", statements[0])
	}
	if !strings.Contains(statements[0], "GROUP BY api_group, kind, namespace, name, uid") {
		t.Fatalf("the ownership read is not grouped per object, so it returns a row per change:\n%s",
			statements[0])
	}
	if strings.Contains(statements[0], "SELECT data") ||
		strings.Contains(statements[0], ", data,") {
		t.Fatalf("the ownership read projects the whole document rather than the references:\n%s",
			statements[0])
	}
}

// TestOwnershipOverEveryNamespaceDropsTheNamespacePredicate covers the cluster-scoped
// root, whose dependents may be anywhere.
//
// Narrowing to the object's own (absent) namespace would be the cheaper read and the
// wrong answer, so the predicate is absent rather than bound to the empty string —
// which would match only the objects that have no namespace.
func TestOwnershipOverEveryNamespaceDropsTheNamespacePredicate(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, conn := seededEngine(t, fixture.History)

	q := fixture.Query
	q.Namespace = ""
	got, err := engine.Ownership(context.Background(), q)
	if err != nil {
		t.Fatalf("Ownership: %v", err)
	}

	if strings.Contains(conn.statements()[0], "namespace = ?") {
		t.Fatalf("an unrestricted ownership read still carries a namespace predicate:\n%s",
			conn.statements()[0])
	}
	if len(got) != len(fixture.Objects)+1 {
		t.Fatalf("an unrestricted read returned %d objects, want %d: the fixture holds one more "+
			"in another namespace", len(got), len(fixture.Objects)+1)
	}
}

// TestOwnershipRefusesAfterClose is the contract every call of this engine keeps: a
// closed engine answers nothing rather than dialling again.
func TestOwnershipRefusesAfterClose(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := seededEngine(t, fixture.History)
	if err := engine.Close(); err != nil {
		t.Fatalf("closing the engine: %v", err)
	}

	if _, err := engine.Ownership(context.Background(), fixture.Query); err == nil {
		t.Fatal("Ownership answered after Close")
	}
}

// TestTimelineCorrelatesTheEventsOfASubject is the read-plane half of `--owned` at
// this backend: a timeline about one object carrying the Events of another.
//
// The subject here has no state rows of its own in the fixture's namespace beyond
// one modification, which is exactly the shape a walked tree produces — and the
// point of the assertion is that the object's own changes stay the root's.
func TestTimelineCorrelatesTheEventsOfASubject(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := seededEngine(t, fixture.History)

	root := fixture.Objects[2].Ref    // the Deployment
	subject := fixture.Objects[0].Ref // the Pod the fixture's Event names

	q := query.TimelineQuery{
		Ref: root, From: fixture.Query.From, To: fixture.Query.To,
		IncludeEvents: true, Subjects: []query.ObjectRef{subject},
	}

	var events, changes int
	for _, change := range drainTimeline(t, engine, q) {
		if change.EventType == query.EventKubernetes {
			events++
			continue
		}
		changes++
	}
	if events != 1 {
		t.Fatalf("the timeline carried %d Events, want the one naming %s — a subject's Events are "+
			"correlated by (kind, namespace, name) out of the Event's own payload",
			events, subject.Name)
	}
	if changes != 1 {
		t.Fatalf("the timeline carried %d of the object's own changes, want the Deployment's one. "+
			"Subjects widens the Event correlation and never the state half: a change carries no "+
			"identity, so a merged multi-object state stream could not be attributed", changes)
	}
}

// TestSubjectsDoNotInheritThePinnedIncarnation is the trap the disjunction exists
// around.
//
// A dependent does not share its owner's UID, so a pin applied to every alternative
// would correlate the root's Events and none of the tree's — the widening silently
// undone by the narrowing beside it, with a well-formed answer either way.
func TestSubjectsDoNotInheritThePinnedIncarnation(t *testing.T) {
	fixture := conformance.OwnershipHistory()
	engine, _ := seededEngine(t, fixture.History)

	q := query.TimelineQuery{
		Ref: fixture.Objects[2].Ref, From: fixture.Query.From, To: fixture.Query.To,
		UID:           fixture.Objects[2].UID,
		IncludeEvents: true, Subjects: []query.ObjectRef{fixture.Objects[0].Ref},
	}

	var events int
	for _, change := range drainTimeline(t, engine, q) {
		if change.EventType == query.EventKubernetes {
			events++
		}
	}
	if events != 1 {
		t.Fatalf("a pinned root returned %d Events for its tree, want 1", events)
	}
}
