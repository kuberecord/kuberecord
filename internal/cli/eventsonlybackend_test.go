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

// Task 17.3's Events-only notice, fired over an engine that had the bug.
//
// # Why this file exists beside the golden tests that already cover the notice
//
// TestTimelineExplainsAnEventsOnlyTimeline pins the whole rendering of all three
// states, in both colour modes, and it passes — against this package's fake
// engine, which answers an Event query from a slice that has nothing to do with
// whether the object has changes of its own. That is the contract the notice was
// written against, and modelling it is what a fake is for.
//
// It is also exactly why the notice had never once fired for a user. Both real
// backends resolved an incarnation before anything else and returned an empty
// iterator when the object had no records of its own, so the Events query was
// never issued and no page of Event rows ever reached the renderer (D40, D42, Task
// 18.6). A fake cannot exhibit an early return taken before the query it stands
// in for.
//
// So this drives RunTimeline over a *real* query engine — the object archive, over
// a directory, which is the backend `--source` opens and the one whose defect this
// test would have caught — and asserts the notice arrives. No container: the
// archive is fifteen small files in a temp dir, written through the same fixture
// writer the read plane's own suites use.
//
// The rendering itself is deliberately not re-pinned here. The golden files own
// that, down to the severity register each line is painted in; what this owns is
// that the path is reachable at all, which is the half no fake can assert.
package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// The archive fixture's identities and instants.
//
// The subject is a Pod, because that is the shape the field report arrived in and
// the shape the quickstart produces: its rule captures Events, Deployments and
// ConfigMaps, so every Pod in the namespace is named by Events that were recorded
// and holds not one record of its own.
const (
	archiveSubjectName = "checkout-7d4f-abcde"
	archiveSubjectUID  = "dddddddd-0000-0000-0000-000000000004"
	archiveEventsRule  = "clusterstreamrule/all-events"
	archivePrefix      = "audit"
)

// archiveEpoch is the instant the fixture archive is dated from — fixed, so a
// failure names the same timestamps today as in a log pasted last week.
func archiveEpoch() time.Time { return time.Date(2026, 8, 28, 14, 0, 0, 0, time.UTC) }

// archiveSubject is the object the timeline is asked about.
func archiveSubject() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: fixtureCluster,
		APIGroup:  "",
		Kind:      "Pod",
		Namespace: "payments",
		Name:      archiveSubjectName,
	}
}

// TestAnEventsOnlyTimelineOverARealBackendExplainsItself is the end-to-end half of
// Task 18.6: a real engine, a real archive, and the notice on standard error.
func TestAnEventsOnlyTimelineOverARealBackendExplainsItself(t *testing.T) {
	engine := archiveEngine(t)

	request := cli.TimelineRequest{
		Ref:        archiveSubject(),
		From:       archiveEpoch().Add(-time.Hour),
		To:         archiveEpoch().Add(time.Hour),
		Now:        archiveEpoch().Add(time.Hour),
		Limit:      100,
		WithEvents: true,
	}

	var out, errOut bytes.Buffer
	err := cli.RunTimeline(context.Background(), &resolve.Backend{Engine: engine, ClusterID: fixtureCluster},
		request, ioStreams(&out, &errOut), render.Options{Width: goldenWidth})
	if err != nil {
		// Exit 0 with a notice, not the no-coverage finding: the command produced
		// evidence — real, correlated Events — and ErrNoCoverage's own sentence
		// describes a silence that is not on the page.
		t.Fatalf("an Events-only timeline is a notice, not a finding: %v", err)
	}

	stdout, stderr := out.String(), errOut.String()

	// The document: four Event rows, which is the half that did not arrive before
	// this fix and the half the notice is about.
	for _, reason := range []string{"Scheduled", "Pulling", "FailedScheduling", "Killing"} {
		if !strings.Contains(stdout, reason) {
			t.Errorf("the timeline does not render the Event %q; the object has no records of its own, "+
				"so an engine that resolved an incarnation first and stopped would render an empty "+
				"page here:\n%s", reason, stdout)
		}
	}

	// The notice: Task 17.3's sentence, which has existed since Phase 17 and could
	// not fire against either real backend until now.
	wants := []string{
		"every row here is a Kubernetes Event",
		"nothing was ever watching Pod payments/" + archiveSubjectName,
		"The Events are here because a rule captures Events",
		"scopes",
	}
	for _, want := range wants {
		if !strings.Contains(stderr, want) {
			t.Errorf("the Events-only notice does not say %q, so a reader is shown a page of Event "+
				"rows and left to conclude the Pod never changed:\n%s", want, stderr)
		}
	}
	if strings.Contains(stdout, "every row here is a Kubernetes Event") {
		t.Errorf("the notice was written to standard output, which corrupts the document:\n%s", stdout)
	}
}

// archiveEngine opens a real object-archive engine over a directory holding the
// fixture.
func archiveEngine(t *testing.T) query.QueryEngine {
	t.Helper()

	dir := t.TempDir()
	if _, err := archivetest.WriteDir(dir, archivePrefix, archiveHistory()); err != nil {
		t.Fatalf("writing the fixture archive: %v", err)
	}
	source, err := objectsource.NewLocal(dir)
	if err != nil {
		t.Fatalf("opening the fixture archive at %q: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("closing the fixture source: %v", err)
		}
	})
	engine, err := objectsource.NewEngine(source, objectsource.Options{Prefix: archivePrefix})
	if err != nil {
		t.Fatalf("building an engine over the fixture archive: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})
	return engine
}

// archiveHistory is the recorded past the quickstart actually produces: a rule
// streaming Kubernetes Events, no rule streaming Pods, and four Events about one.
//
// The scope log matters as much as the records. It carries the Event scope and
// nothing for Pods, which is what makes coverage answer "nothing was ever watching
// this kind" and selects the one of the notice's three states this test is about.
func archiveHistory() conformance.History {
	return conformance.History{
		Rows: []conformance.Row{
			archiveEventRow(2*time.Minute, "", "pod.scheduled", "Scheduled"),
			archiveEventRow(3*time.Minute, "events.k8s.io", "pod.pulling", "Pulling"),
			archiveEventRow(4*time.Minute, "", "pod.failed", "FailedScheduling"),
			archiveEventRow(5*time.Minute, "events.k8s.io", "pod.killing", "Killing"),
		},
		Scopes: []conformance.ScopeTransition{{
			Action:    conformance.ScopeStarted,
			APIGroup:  "",
			Kind:      "Event",
			Namespace: "",
			RuleRef:   archiveEventsRule,
			TS:        archiveEpoch().Add(-30 * time.Minute),
		}},
	}
}

// archiveEventRow builds one recorded Kubernetes Event naming the subject.
//
// The subject key is the one its group spells it with — involvedObject in the core
// group, regarding in events.k8s.io — so a correlation that reached for one of them
// alone would render two rows here instead of four.
func archiveEventRow(offset time.Duration, apiGroup, name, reason string) conformance.Row {
	key, apiVersion := "involvedObject", "v1"
	if apiGroup != "" {
		key, apiVersion = "regarding", "events.k8s.io/v1"
	}
	subject := archiveSubject()
	data := fmt.Sprintf(`{"type":"Warning","reason":%q,"message":"about %s",%q:`+
		`{"kind":%q,"namespace":%q,"name":%q,"uid":%q}}`,
		reason, subject.Name, key, subject.Kind, subject.Namespace, subject.Name, archiveSubjectUID)

	return conformance.Row{
		Ref: query.ObjectRef{
			ClusterID: subject.ClusterID,
			APIGroup:  apiGroup,
			Kind:      "Event",
			Namespace: subject.Namespace,
			Name:      name,
		},
		Change: query.Change{
			TS:              archiveEpoch().Add(offset),
			EventType:       query.EventAdded,
			UID:             "event-" + name,
			ResourceVersion: "1",
			APIVersion:      apiVersion,
			// An Event's actors are the field managers of the Event object: whoever
			// wrote the Event, never whoever changed the object it is about.
			Actors: []string{"kubelet"},
			Data:   data,
		},
	}
}
