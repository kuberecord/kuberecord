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

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The rendering of `timeline`, asserted against golden files.
//
// Golden files rather than assembled expectations because the thing under test is
// a whole page of characters whose alignment, elision and stream split all matter
// at once, and a test that asserted them field by field would assert everything
// except how it reads. The files are checked in, so a change to any of it shows up
// in review as the diff a reader would have seen.
//
// Each file holds both streams, because the document and its qualifications are
// one invocation's output and the split between them is itself under test: a
// notice that migrated to stdout would corrupt a pipe, and one that vanished would
// be a silence Invariants 4 and 5 forbid.

// updateGolden rewrites the files instead of comparing against them.
//
//	go test ./internal/cli/ -run Timeline -update
var updateGolden = flag.Bool("update", false, "rewrite the timeline golden files")

// The stream markers inside a golden file.
const (
	stdoutMarker = "=== stdout ===\n"
	stderrMarker = "=== stderr ===\n"
)

// The fixture's identity. It is one object, in one cluster, so that every golden
// file below is a variation on a single story rather than five unrelated ones.
const (
	fixtureCluster = "prod-eu-1"
	fixtureUID     = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	priorUID       = "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
)

// goldenWidth is the terminal the golden files were laid out for.
//
// Fixed, and passed explicitly rather than read from the terminal, because a
// golden file generated in one window and compared in another would fail for a
// reason that has nothing to do with the code.
const goldenWidth = 120

// fixtureRef is the object every golden file is about.
func fixtureRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: fixtureCluster,
		APIGroup:  "apps",
		Kind:      "Deployment",
		Namespace: "payments",
		Name:      "checkout",
	}
}

// at builds a fixture timestamp, which is always UTC and always to the
// millisecond the narrow table renders.
func at(clock string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, clock)
	if err != nil {
		panic(err)
	}
	return parsed
}

// fixtureState is the Deployment as it stood at its first sighting.
//
// It carries the memory limit the flagship row moves, the fields the multi-
// operation row touches, and an annotation whose key contains dots and a slash —
// which is what makes the golden output prove that RFC 6901's ~1 escape is undone
// before the path is joined with dots.
const fixtureState = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"checkout",` +
	`"namespace":"payments","annotations":{"deployment.kubernetes.io/revision":"1"}},` +
	`"spec":{"replicas":3,"minReadySeconds":10,"template":{"spec":{"containers":[{"name":"checkout",` +
	`"image":"registry.example/checkout:1.4.2","resources":{"limits":{"cpu":"2","memory":"2Gi"}}}]}}}}`

// checkoutHistory is the fixture's recorded history, oldest first.
func checkoutHistory() []query.Change {
	return []query.Change{
		{
			TS: at("2026-08-28T14:02:58.001Z"), EventType: query.EventAdded, UID: fixtureUID,
			Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "1001",
			APIVersion: "apps/v1", Data: fixtureState,
		},
		{
			TS: at("2026-08-28T14:03:11.482Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "1002", APIVersion: "apps/v1",
			Diff: `[{"op":"replace","path":"/spec/template/spec/containers/0/resources/limits/memory",` +
				`"value":"512Mi"}]`,
		},
		{
			TS: at("2026-08-28T14:05:02.117Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"kube-controller-manager"}, ResourceVersion: "1003", APIVersion: "apps/v1",
			Diff: `[{"op":"replace","path":"/spec/replicas","value":5},` +
				`{"op":"add","path":"/spec/paused","value":true},` +
				`{"op":"remove","path":"/spec/minReadySeconds"}]`,
		},
		{
			TS: at("2026-08-28T14:09:40.900Z"), EventType: query.EventModified, UID: fixtureUID,
			ResourceVersion: "1004", APIVersion: "apps/v1",
			Diff: `[{"op":"replace","path":"/metadata/annotations/deployment.kubernetes.io~1revision",` +
				`"value":"2"}]`,
		},
	}
}

// checkoutIncarnations is the listing that matches checkoutHistory.
func checkoutIncarnations() []query.Incarnation {
	return []query.Incarnation{{
		UID:       fixtureUID,
		FirstSeen: at("2026-08-28T14:02:58.001Z"),
		LastSeen:  at("2026-08-28T14:09:40.900Z"),
	}}
}

// watchedSince is a scope opened before the fixture's history and still open.
func watchedSince(from string, rule string) []query.ScopeInterval {
	return []query.ScopeInterval{{
		APIGroup: "apps", Kind: "Deployment", RuleRef: rule, From: at(from),
	}}
}

// clickHouseCapabilities are the shipped ClickHouse engine's, restated here so a
// rendering test does not import a backend to learn them.
func clickHouseCapabilities() query.Capabilities {
	return query.Capabilities{
		Backend: "clickhouse", Deletions: true, ServerSideFilter: true, PointQuery: true,
	}
}

// archiveCapabilities are the shipped object-archive engine's: no deletions
// (D12), no pushdown, no point query, and a mandatory time bound.
func archiveCapabilities() query.Capabilities {
	return query.Capabilities{Backend: "objectsource", TimeBoundRequired: true}
}

// defaultRequest is a bare `timeline deploy/checkout -n payments`.
func defaultRequest() cli.TimelineRequest {
	return cli.TimelineRequest{
		Ref:   fixtureRef(),
		Now:   at("2026-08-28T15:00:00Z"),
		Limit: 100,
	}
}

// ioStreams pairs two buffers as the streams a command writes to.
//
// The two halves stay separate for the reason every assertion in this file rests
// on: the split between the document and its qualifications is itself under test,
// and a combined buffer would let a notice migrate to stdout unnoticed.
func ioStreams(out, errOut io.Writer) genericiooptions.IOStreams {
	return genericiooptions.IOStreams{In: strings.NewReader(""), Out: out, ErrOut: errOut}
}

// runTimeline drives the command against a fake engine and returns both streams.
func runTimeline(
	t *testing.T, engine *fakeEngine, request cli.TimelineRequest, opts render.Options,
) (stdout, stderr string, err error) {
	t.Helper()

	if opts.Width == 0 {
		opts.Width = goldenWidth
	}
	var out, errOut bytes.Buffer
	streams := ioStreams(&out, &errOut)
	backend := &cli.Backend{Engine: engine, ClusterID: fixtureCluster}

	err = cli.RunTimeline(context.Background(), backend, request, streams, opts)
	assertDrained(t, engine)
	return out.String(), errOut.String(), err
}

// assertGolden compares both streams against the checked-in file.
func assertGolden(t *testing.T, name, stdout, stderr string) {
	t.Helper()

	path := filepath.Join("testdata", "timeline", name+".golden")
	got := stdoutMarker + stdout + stderrMarker + stderr

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/ -run Timeline -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// TestTimelineRendersTheFlagshipTable is the row the release exists for: one
// line naming the actor and the field that changed.
func TestTimelineRendersTheFlagshipTable(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "flagship", stdout, stderr)

	// The row the acceptance criteria name, asserted by content as well as by
	// golden file: a golden file that drifted would keep passing after being
	// regenerated, and this line is the one thing that must not.
	const flagship = "~ spec.…containers[0].resources.limits.memory: 2Gi → 512Mi"
	if !strings.Contains(stdout, flagship) {
		t.Errorf("the flagship row is missing.\nwant a line containing %q\ngot:\n%s", flagship, stdout)
	}
}

// TestTimelineRendersEveryOperationWithFull covers --full.
func TestTimelineRendersEveryOperationWithFull(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{Full: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "full", stdout, stderr)
}

// TestTimelineRendersWideColumns covers -o wide: full UIDs, resource versions,
// and the nanosecond precision the schema records.
func TestTimelineRendersWideColumns(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{Wide: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "wide", stdout, stderr)
}

// TestTimelineReversesOnlyTheDisplayOrder pins the answer to the question
// --reverse asks.
//
// It reorders rows; it does not select different ones. The query is asked for the
// newest first either way, because that is the shape both backends answer cheaply
// — the object archive stops walking partitions once a reverse-limited query's
// limit is filled.
func TestTimelineReversesOnlyTheDisplayOrder(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := defaultRequest()
	request.Reverse = true
	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "reverse", stdout, stderr)

	if !engine.queries[0].Reverse {
		t.Error("--reverse changed the query's own order; it must only change the rendering, " +
			"or a limited query stops selecting the newest changes")
	}
}

// TestTimelineBannersOtherIncarnations covers Invariant 7's visible half.
func TestTimelineBannersOtherIncarnations(t *testing.T) {
	engine := twoIncarnations()

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "multi-incarnation", stdout, stderr)

	if engine.queries[0].UID != fixtureUID {
		t.Errorf("the query was pinned to %q, want the newest incarnation %q: an unpinned query would "+
			"let the header and the rows disagree about which object is shown",
			engine.queries[0].UID, fixtureUID)
	}
}

// TestTimelineShowsEveryIncarnationWithAColumn covers --all-incarnations.
func TestTimelineShowsEveryIncarnationWithAColumn(t *testing.T) {
	engine := twoIncarnations()

	request := defaultRequest()
	request.AllIncarnations = true
	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "all-incarnations", stdout, stderr)

	if engine.queries[0].UID != "" || !engine.queries[0].AllIncarnations {
		t.Errorf("--all-incarnations produced query %+v; it must not pin a UID", engine.queries[0])
	}
}

// twoIncarnations is a name that has belonged to two objects: an older one that
// was deleted, and the one wearing it now.
func twoIncarnations() *fakeEngine {
	history := append([]query.Change{
		{
			TS: at("2026-08-28T14:01:10.004Z"), EventType: query.EventAdded, UID: priorUID,
			Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "907",
			APIVersion: "apps/v1", Data: fixtureState,
		},
		{
			TS: at("2026-08-28T14:02:57.883Z"), EventType: query.EventDeleted, UID: priorUID,
			ResourceVersion: "941", APIVersion: "apps/v1",
		},
	}, checkoutHistory()...)

	return &fakeEngine{
		caps:      clickHouseCapabilities(),
		changes:   history,
		intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		incarnations: []query.Incarnation{
			{
				UID:       priorUID,
				FirstSeen: at("2026-08-28T14:01:10.004Z"),
				LastSeen:  at("2026-08-28T14:02:57.883Z"),
				Deleted:   true,
			},
			{
				UID:       fixtureUID,
				FirstSeen: at("2026-08-28T14:02:58.001Z"),
				LastSeen:  at("2026-08-28T14:09:40.900Z"),
			},
		},
	}
}

// TestTimelineInterleavesKubernetesEvents covers --with-events, including the
// warning glyph and both API spellings of an Event.
func TestTimelineInterleavesKubernetesEvents(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		events: []query.Change{
			{
				TS: at("2026-08-28T14:03:20.310Z"), EventType: query.EventKubernetes,
				UID: "e1", Actors: []string{"kube-controller-manager"}, APIVersion: "v1",
				Data: `{"type":"Normal","reason":"ScalingReplicaSet",` +
					`"message":"Scaled up replica set checkout-7d4f to 5","source":{"component":"deployment-controller"}}`,
			},
			{
				TS: at("2026-08-28T14:06:44.020Z"), EventType: query.EventKubernetes,
				UID: "e2", APIVersion: "events.k8s.io/v1",
				Data: `{"type":"Warning","reason":"FailedCreate",` +
					`"note":"pods \"checkout-7d4f-\" is forbidden: exceeded quota",` +
					`"reportingController":"replicaset-controller"}`,
			},
		},
	}

	request := defaultRequest()
	request.WithEvents = true
	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "with-events", stdout, stderr)

	if !engine.queries[0].IncludeEvents {
		t.Error("--with-events did not reach the query")
	}
}

// TestTimelineExplainsAnEmptyResultAgainstCoverage is the "nothing changed" half
// of Invariant 9: the scope was watched across the window, so the silence is real.
func TestTimelineExplainsAnEmptyResultAgainstCoverage(t *testing.T) {
	engine := &fakeEngine{
		caps:      clickHouseCapabilities(),
		intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := defaultRequest()
	request.From = at("2026-08-01T00:00:00Z")
	request.To = at("2026-08-28T15:00:00Z")

	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "empty-with-coverage", stdout, stderr)
}

// TestTimelineWarnsWhenTheObjectWasNotObservedEarlier is the other half: the
// window reaches back past the moment anything started watching, so the silence
// before that moment says nothing at all.
func TestTimelineWarnsWhenTheObjectWasNotObservedEarlier(t *testing.T) {
	engine := &fakeEngine{
		caps:      clickHouseCapabilities(),
		intervals: watchedSince("2026-08-20T11:30:00Z", "StreamRule/payments/checkout-audit"),
	}

	request := defaultRequest()
	request.From = at("2026-08-01T00:00:00Z")
	request.To = at("2026-08-28T15:00:00Z")

	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "empty-not-observed-before", stdout, stderr)
}

// TestTimelineExitsThreeWhenNothingWasWatching is the sharp end of Invariant 9.
//
// Every other tool in this space renders this as a successful empty result. That
// collapse is what sends an engineer away believing an object sat untouched when
// in truth nobody was recording it.
func TestTimelineExitsThreeWhenNothingWasWatching(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities()}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err == nil {
		t.Fatal("RunTimeline succeeded; an object no scope ever covered is a finding, not an empty result")
	}
	if !errors.Is(err, query.ErrNoCoverage) {
		t.Errorf("the failure does not carry query.ErrNoCoverage, so nothing maps it to an exit code: %v", err)
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitNoCoverage {
		t.Errorf("ExitCodeFor = %d, want %d", code, cli.ExitNoCoverage)
	}
	assertGolden(t, "empty-without-coverage", stdout, stderr+"error: "+err.Error()+"\n")
}

// TestTimelineNoticesABackendThatCannotRecordDeletions is the capability notice.
//
// An archive holds no Deleted rows at all (D12), so a timeline over one always
// simply stops. Without the notice that silence reads as "the object is still
// there", which is a claim the data cannot support.
func TestTimelineNoticesABackendThatCannotRecordDeletions(t *testing.T) {
	engine := &fakeEngine{
		caps:         archiveCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "deletions-unsupported", stdout, stderr)

	// The same invocation must also have supplied the window this backend
	// requires, rather than issuing an unbounded query it would refuse.
	if engine.queries[0].From.IsZero() || engine.queries[0].To.IsZero() {
		t.Errorf("the query was unbounded against a backend that requires a bound: %+v", engine.queries[0])
	}
}

// TestTimelineSuppressesPriorValuesUnderAFilter is the honesty half of the
// old-value replay.
//
// A filtered result is not a consecutive run of history, so replaying only the
// surviving patches over a real base state would produce a document the object
// was never in. The arrow goes rather than the numbers being invented.
func TestTimelineSuppressesPriorValuesUnderAFilter(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := defaultRequest()
	request.FieldPaths = []string{"spec.template.spec.containers"}
	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "filtered", stdout, stderr)

	if strings.Contains(stdout, "2Gi →") {
		t.Error("a prior value was rendered from a non-consecutive run of history")
	}
}

// TestTimelineJoinsSeveralFieldManagers covers the ACTOR column being
// first-class: an object edited by two managers names both.
func TestTimelineJoinsSeveralFieldManagers(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		changes: []query.Change{{
			TS: at("2026-08-28T14:03:11.482Z"), EventType: query.EventModified, UID: fixtureUID,
			// Sorted and distinct is the read plane's guarantee; the renderer
			// joins them and does not re-sort.
			Actors: []string{"deployment-controller", "kubectl-client-side-apply"},
			Diff:   `[{"op":"replace","path":"/spec/replicas","value":5}]`,
		}},
	}

	stdout, _, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stdout, "deployment-controller,kubectl-client-side-apply") {
		t.Errorf("the managers were not comma-joined:\n%s", stdout)
	}
}

// TestTimelinePassesItsPredicatesToTheQuery is the wiring of the filter flags.
//
// They are pushed into the query rather than applied here, so that a backend
// declaring ServerSideFilter can push them further still — and so the CLI never
// grows a second reading of a predicate the contract already defines.
func TestTimelinePassesItsPredicatesToTheQuery(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := defaultRequest()
	request.Actors = []string{"kubectl-client-side-apply"}
	request.ExcludeActors = []string{"kube-controller-manager"}
	request.FieldPaths = []string{"spec.template.spec.containers"}
	request.From = at("2026-08-28T14:00:00Z")
	request.To = at("2026-08-28T15:00:00Z")
	request.Limit = 25

	if _, _, err := runTimeline(t, engine, request, render.Options{}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	got := engine.queries[0]
	switch {
	case len(got.Actors) != 1 || got.Actors[0] != "kubectl-client-side-apply":
		t.Errorf("--actor did not reach the query: %+v", got.Actors)
	case len(got.ExcludeActors) != 1:
		t.Errorf("--exclude-actor did not reach the query: %+v", got.ExcludeActors)
	case len(got.FieldPaths) != 1 || got.FieldPaths[0] != "spec.template.spec.containers":
		t.Errorf("--field did not reach the query: %+v", got.FieldPaths)
	case got.Limit != 25:
		t.Errorf("--limit did not reach the query: %d", got.Limit)
	case !got.From.Equal(request.From) || !got.To.Equal(request.To):
		t.Errorf("the window did not reach the query: %s to %s", got.From, got.To)
	}
}

// TestTimelinePinsAnExplicitUID covers --uid, and the notice for a UID that names
// nothing in the window.
//
// Not an error, because the UID may be perfectly real and simply outside --since,
// and refusing the command would make the user guess which of the two it was.
func TestTimelinePinsAnExplicitUID(t *testing.T) {
	engine := twoIncarnations()

	request := defaultRequest()
	request.UID = priorUID
	stdout, _, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if engine.queries[0].UID != priorUID {
		t.Errorf("--uid did not pin the query: %q", engine.queries[0].UID)
	}
	if strings.Contains(stdout, "14:09:40.900") {
		t.Errorf("the pinned timeline shows another incarnation's rows:\n%s", stdout)
	}

	engine = twoIncarnations()
	request.UID = "a-uid-from-another-window"
	_, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stderr, "no changes are recorded for uid a-uid-from-another-window") {
		t.Errorf("a --uid matching nothing was not reported:\n%s", stderr)
	}
}

// TestTimelineDegradesWhenIncarnationsCannotBeListed covers the path no shipped
// backend takes and every future one might.
//
// The timeline is still answerable, so it is still answered; what must not happen
// is the degradation being silent, or a table that may span two incarnations
// losing the column that tells them apart (Invariant 7).
func TestTimelineDegradesWhenIncarnationsCannotBeListed(t *testing.T) {
	engine := twoIncarnations()
	engine.incarnationsErr = query.ErrCapabilityUnsupported

	request := defaultRequest()
	request.AllIncarnations = true
	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stderr, "cannot list incarnations") {
		t.Errorf("the degradation was not reported:\n%s", stderr)
	}
	if !strings.Contains(stdout, "UID") {
		t.Errorf("the UID column went missing exactly when it mattered:\n%s", stdout)
	}
	for _, uid := range []string{fixtureUID[:8], priorUID[:8]} {
		if !strings.Contains(stdout, uid) {
			t.Errorf("incarnation %s is not distinguishable in the table:\n%s", uid, stdout)
		}
	}
}

// TestTimelineDegradesWhenCoverageIsUnsupported is Invariant 9 against a backend
// that cannot answer it.
//
// The command must not present the emptiness as a result, and must not fail
// either: it says which of the two facts it cannot tell apart.
func TestTimelineDegradesWhenCoverageIsUnsupported(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), coverageErr: query.ErrCapabilityUnsupported}

	stdout, stderr, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v; an unanswerable coverage question degrades, it does not fail", err)
	}
	if !strings.Contains(stderr, "has no scope log") {
		t.Errorf("the emptiness was presented without saying why it could not be explained:\n%s", stderr)
	}
	if !strings.Contains(stdout, "Coverage: not reported by this backend") {
		t.Errorf("the header claimed something about coverage it could not know:\n%s", stdout)
	}
}

// TestTimelineFailsWhenCoverageItselfFails separates a backend that cannot answer
// from one that would not.
//
// The first degrades; the second is a failure, and reporting it as "no coverage"
// would turn a broken connection into a finding about the cluster.
func TestTimelineFailsWhenCoverageItselfFails(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), coverageErr: errors.New("connection refused")}

	if _, _, err := runTimeline(t, engine, defaultRequest(), render.Options{}); err == nil {
		t.Fatal("a backend that would not answer was reported as one with nothing to say")
	} else if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
		t.Errorf("ExitCodeFor = %d, want %d", code, cli.ExitRuntimeError)
	}
}

// TestTimelineLimitDefaultsToOneHundred pins the documented default.
func TestTimelineLimitDefaultsToOneHundred(t *testing.T) {
	streamPair, _, _ := streams()
	root, _ := cli.NewRootCommand(cli.StandaloneName, streamPair)

	for _, command := range root.Commands() {
		if command.Name() != "timeline" {
			continue
		}
		if got := command.Flags().Lookup("limit").DefValue; got != "100" {
			t.Errorf("--limit defaults to %s, want 100", got)
		}
		return
	}
	t.Fatal("the timeline command is not in the tree")
}

// TestTimelineCompletesAHalfWindowForABackendThatNeedsBoth covers the case that
// would otherwise report ErrTimeBoundRequired naming the flag the user had just
// used.
//
// An engine requiring a bound requires both of them, so `--since 3d` alone is a
// question it will not take. Completing it and saying which end was supplied
// answers what was asked; refusing it would read as a bug in the tool.
func TestTimelineCompletesAHalfWindowForABackendThatNeedsBoth(t *testing.T) {
	engine := &fakeEngine{
		caps:         archiveCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := defaultRequest()
	request.From = at("2026-08-28T14:00:00Z")

	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if got := engine.queries[0]; !got.From.Equal(request.From) || !got.To.Equal(request.Now) {
		t.Errorf("the window was completed to %s..%s, want %s..%s (the caller's start, and now)",
			got.From, got.To, request.From, request.Now)
	}
	if !strings.Contains(stderr, "needs both ends of a window") {
		t.Errorf("the completed end was not announced:\n%s", stderr)
	}
	if !strings.Contains(stdout, "2Gi → 512Mi") {
		t.Errorf("the question was not answered:\n%s", stdout)
	}
}

// TestTimelineNamesTheFlagThatFixesARefusedQuery covers the failure a flag can
// actually fix, for an engine that refuses a window this command did not build.
func TestTimelineNamesTheFlagThatFixesARefusedQuery(t *testing.T) {
	engine := &fakeEngine{
		caps:         clickHouseCapabilities(),
		incarnations: checkoutIncarnations(),
		timelineErr:  query.ErrTimeBoundRequired,
	}

	_, _, err := runTimeline(t, engine, defaultRequest(), render.Options{})
	if err == nil {
		t.Fatal("a refused query was reported as a successful empty result")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Errorf("the message does not name the flag that fixes it: %v", err)
	}
}

// TestTheHeaderNamesTheObjectsUIDNotAnEventsUID guards the one place the two
// could be confused.
//
// A merged Event row carries the Event object's UID, so when the incarnation
// listing has failed and --with-events puts an Event at the top, a header taking
// the first row's UID would name the identity of a message *about* the object
// rather than of the object.
func TestTheHeaderNamesTheObjectsUIDNotAnEventsUID(t *testing.T) {
	engine := &fakeEngine{
		caps:            clickHouseCapabilities(),
		changes:         checkoutHistory(),
		incarnationsErr: query.ErrCapabilityUnsupported,
		intervals:       watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		events: []query.Change{{
			// Newer than every change, so it is the first row rendered.
			TS: at("2026-08-28T14:30:00.000Z"), EventType: query.EventKubernetes, UID: "event-uid",
			Data: `{"type":"Normal","reason":"ScalingReplicaSet","message":"Scaled up"}`,
		}},
	}

	request := defaultRequest()
	request.WithEvents = true
	stdout, _, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stdout, "UID:      "+fixtureUID) {
		t.Errorf("the header does not name the object's own incarnation:\n%s", stdout)
	}
}
