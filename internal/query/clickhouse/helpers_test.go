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

// The histories this package's own tests assert against, and the plumbing that
// puts an engine in front of them.
//
// They are separate from the conformance suite's fixtures on purpose. The suite's
// fixtures belong to the contract and are shared by every backend; these belong to
// the questions only this backend raises — Event correlation across two API
// spellings, a limit composed with a client-side filter, a base row that has aged
// out of the retention window — none of which the contract has an opinion about.

package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The identities and actors these fixtures use. testRef in sql_test.go is the
// object they all describe.
const (
	actorKubectl    = "kubectl"
	actorController = "kube-controller-manager"
	actorHelm       = "helm"

	uidA = "aaaaaaaa-0000-0000-0000-000000000001"
	uidB = "bbbbbbbb-0000-0000-0000-000000000002"
)

// testEpoch is the instant every fixture here is dated from — fixed, so a failure
// message names the same timestamps today as in a log pasted last week.
func testEpoch() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC) }

// fixtureWindow brackets every fixture with room to spare.
func fixtureWindow() (time.Time, time.Time) {
	return testEpoch().Add(-time.Hour), testEpoch().Add(24 * time.Hour)
}

// after is the instant a fixture's nth offset falls on.
func after(d time.Duration) time.Time { return testEpoch().Add(d) }

// seededEngine puts an engine in front of a stand-in connection holding history.
//
// It returns the connection too, because several assertions here are about what
// the engine *said* rather than about what it got back.
func seededEngine(t *testing.T, history conformance.History) (*Engine, *fakeConn) {
	t.Helper()
	store := newFakeStore()
	if err := store.seed(history); err != nil {
		t.Fatalf("seeding the stand-in: %v", err)
	}
	conn := &fakeConn{store: store}
	engine, err := New(conn)
	if err != nil {
		t.Fatalf("building an engine over the stand-in connection: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})
	return engine, conn
}

// drainTimeline runs a timeline query the way the contract documents: drain,
// close on every path, and check Err after the loop.
func drainTimeline(t *testing.T, engine *Engine, q query.TimelineQuery) []query.Change {
	t.Helper()
	it, err := engine.Timeline(context.Background(), q)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("closing the iterator: %v", err)
		}
	}()

	var changes []query.Change
	for it.Next() {
		changes = append(changes, it.Change())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("the iterator failed mid-stream: %v", err)
	}
	return changes
}

// fixtureRow builds one recorded change of the fixture object.
func fixtureRow(offset time.Duration, eventType, uid string, actors []string) conformance.Row {
	return conformance.Row{
		Ref: testRef(),
		Change: query.Change{
			TS:              after(offset),
			EventType:       eventType,
			UID:             uid,
			APIVersion:      "apps/v1",
			ResourceVersion: "1",
			Actors:          actors,
		},
	}
}

// withData returns a row carrying full state.
func withData(r conformance.Row, data string) conformance.Row {
	r.Change.Data = data
	return r
}

// withDiff returns a row carrying a patch.
func withDiff(r conformance.Row, diff string) conformance.Row {
	r.Change.Diff = diff
	return r
}

// incarnationFixture is one name worn by two objects: created, changed, deleted,
// and created again.
//
// It is the shape Kubernetes produces constantly, and the shape a backend gets
// wrong by answering "the history of payments/checkout" instead of "the history of
// this incarnation of payments/checkout".
func incarnationFixture() conformance.History {
	return conformance.History{Rows: []conformance.Row{
		withData(fixtureRow(0, query.EventAdded, uidA, []string{actorKubectl}),
			`{"spec":{"replicas":1}}`),
		withDiff(fixtureRow(time.Minute, query.EventModified, uidA, []string{actorController}),
			`[{"op":"replace","path":"/spec/replicas","value":2}]`),
		fixtureRow(2*time.Minute, query.EventDeleted, uidA, nil),
		withData(fixtureRow(3*time.Minute, query.EventAdded, uidB, []string{actorHelm}),
			`{"spec":{"replicas":9}}`),
		withDiff(fixtureRow(4*time.Minute, query.EventModified, uidB, []string{actorHelm}),
			`[{"op":"replace","path":"/spec/replicas","value":8}]`),
	}}
}

// eventRow builds one recorded Kubernetes Event, in whichever of the two API
// groups the caller names.
//
// The subject key is spelled the way that group spells it: involvedObject in the
// core group, regarding in events.k8s.io. That difference is the whole reason this
// helper takes the group rather than defaulting to one.
func eventRow(offset time.Duration, apiGroup, name, reason string) conformance.Row {
	subject := "involvedObject"
	apiVersion := "v1"
	if apiGroup != "" {
		subject = "regarding"
		apiVersion = "events.k8s.io/v1"
	}
	ref := testRef()
	data := `{"reason":"` + reason + `","` + subject + `":{"kind":"Deployment","namespace":"` +
		ref.Namespace + `","name":"` + ref.Name + `","uid":"` + uidB + `"}}`

	return conformance.Row{
		Ref: query.ObjectRef{
			ClusterID: ref.ClusterID,
			APIGroup:  apiGroup,
			Kind:      "Event",
			Namespace: ref.Namespace,
			Name:      name,
		},
		Change: query.Change{
			TS:              after(offset),
			EventType:       query.EventAdded,
			UID:             "event-" + name,
			APIVersion:      apiVersion,
			ResourceVersion: "1",
			// An Event's actors are the field managers of the Event object: the
			// controller that wrote it, never whoever changed the object it is
			// about. The Event fixture attributes them to an actor the object's own
			// changes never carry, so a filter leaking across is visible.
			Actors: []string{"kubelet"},
			Data:   data,
		},
	}
}

// eventsOnlySubject is an object that has Events and no history of its own.
//
// A Pod, deliberately, because that is the shape the field report arrived in and
// the shape the quickstart produces: its rule captures Events, Deployments and
// ConfigMaps, so every Pod in the namespace is named by Events that were recorded
// and has not one row of its own. The kind is in the core group, so the fixture
// also spans the group boundary against the Deployment the rest of this file is
// about.
func eventsOnlySubject() query.ObjectRef {
	ref := testRef()
	return query.ObjectRef{
		ClusterID: ref.ClusterID, APIGroup: "", Kind: "Pod",
		Namespace: ref.Namespace, Name: "checkout-7d4f-abcde",
	}
}

// eventsOnlySubjectUID is the incarnation the Events name.
//
// The Events carry it, exactly as a real Event's involvedObject does, and nothing
// else in the fixture does — which is the point. It is an incarnation whose
// existence is recorded only in the commentary about it.
const eventsOnlySubjectUID = "dddddddd-0000-0000-0000-000000000004"

// unrecordedUID is an incarnation nothing in any fixture names.
const unrecordedUID = "eeeeeeee-0000-0000-0000-000000000005"

// eventsOnlyFixture is four Events about an object with no rows of its own.
//
// Four because that is how many the field report found sitting in ClickHouse while
// the CLI said there were none, and both spellings because a fixture that captured
// one of them would leave the other half of the coalesce untested on the very path
// this fixture exists for.
func eventsOnlyFixture() conformance.History {
	return conformance.History{Rows: []conformance.Row{
		eventRowAbout(30*time.Second, "", "pod.scheduled", "Scheduled", eventsOnlySubject()),
		eventRowAbout(70*time.Second, "events.k8s.io", "pod.pulling", "Pulling", eventsOnlySubject()),
		eventRowAbout(110*time.Second, "", "pod.failed", "FailedScheduling", eventsOnlySubject()),
		eventRowAbout(150*time.Second, "events.k8s.io", "pod.killing", "Killing", eventsOnlySubject()),
	}}
}

// eventRowAbout builds one recorded Kubernetes Event naming an arbitrary subject.
//
// It is eventRow with the subject as a parameter rather than fixed to the fixture
// Deployment, which is what an Event about an object with no rows of its own needs.
// The two are kept apart rather than merged because eventRow's callers assert about
// commentary *interleaved* with an object's changes, and a helper whose subject is
// a parameter everywhere would let one of those fixtures name the wrong object
// without the reading of the test changing at all.
func eventRowAbout(
	offset time.Duration, apiGroup, name, reason string, subject query.ObjectRef,
) conformance.Row {
	key := "involvedObject"
	apiVersion := "v1"
	if apiGroup != "" {
		key = "regarding"
		apiVersion = "events.k8s.io/v1"
	}
	data := `{"reason":"` + reason + `","` + key + `":{"kind":"` + subject.Kind +
		`","namespace":"` + subject.Namespace + `","name":"` + subject.Name +
		`","uid":"` + eventsOnlySubjectUID + `"}}`

	return conformance.Row{
		Ref: query.ObjectRef{
			ClusterID: subject.ClusterID,
			APIGroup:  apiGroup,
			Kind:      "Event",
			Namespace: subject.Namespace,
			Name:      name,
		},
		Change: query.Change{
			TS:              after(offset),
			EventType:       query.EventAdded,
			UID:             "event-" + name,
			APIVersion:      apiVersion,
			ResourceVersion: "1",
			Actors:          []string{"kubelet"},
			Data:            data,
		},
	}
}
