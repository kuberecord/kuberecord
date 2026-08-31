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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `blame` against a history built for the one case a naive implementation gets
// wrong.
//
// Two actors write overlapping paths, and the later of them writes an *interior*
// node: argocd replaces the whole `resources` block that kubectl had reached into
// four minutes earlier. A blame that matched only exact pointers would still
// credit kubectl with the memory limit, and would do it confidently, in a column
// somebody is about to take to a change-review meeting. Every test below is a
// variation on that fixture, so the golden files read as one story.

// blameHistory is the fixture's recorded history, oldest first.
//
// It is separate from checkoutHistory because attribution needs what a timeline
// does not: a second actor, an interior write that shadows an earlier leaf write,
// and a removal — none of which the timeline fixture has a use for, and all of
// which would make its golden files harder to read for no gain there.
func blameHistory() []query.Change {
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
			// The interior write. It replaces the block holding both limits, so it is
			// the change that last wrote the memory limit — four minutes after the
			// change that named that limit directly, and by a different actor.
			TS: at("2026-08-28T14:07:20.044Z"), EventType: query.EventModified, UID: fixtureUID,
			Actors: []string{"argocd-application-controller"}, ResourceVersion: "1004",
			APIVersion: "apps/v1",
			Diff: `[{"op":"replace","path":"/spec/template/spec/containers/0/resources",` +
				`"value":{"limits":{"cpu":"4","memory":"1Gi"}}}]`,
		},
		{
			TS: at("2026-08-28T14:09:40.900Z"), EventType: query.EventModified, UID: fixtureUID,
			ResourceVersion: "1005", APIVersion: "apps/v1",
			Diff: `[{"op":"replace","path":"/metadata/annotations/deployment.kubernetes.io~1revision",` +
				`"value":"2"}]`,
		},
	}
}

// blamedCheckoutEngine is the fixture backend every test below starts from.
//
// replay is set so that StateAt reconstructs from the fixture's own history
// through query.Replay, rather than handing back a document of its own. The
// acceptance criterion is about *which* row the replay starts from, and a fake
// that ignored the instant it was asked for would certify nothing about it.
func blamedCheckoutEngine() *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      blameHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		replay:       true,
	}
}

// defaultBlameRequest is a bare `blame deploy/checkout -n payments`.
func defaultBlameRequest() cli.BlameRequest {
	return cli.BlameRequest{Timeline: cli.TimelineRequest{
		Ref:           fixtureRef(),
		Now:           at("2026-08-28T15:00:00Z"),
		NoPriorValues: true,
	}}
}

// hasRow reports whether text holds a row made of exactly these cells.
//
// The padding that lines the columns up is normalized away, because an assertion
// written with it in would be an assertion about the width of the widest actor
// name in the fixture — which changes when a test adds one, and fails somewhere
// that has nothing to do with what broke. The golden files are what pin the
// layout; these assertions pin the content.
func hasRow(text string, cells ...string) bool {
	want := strings.Join(cells, " ")
	for line := range strings.SplitSeq(text, "\n") {
		if strings.Join(strings.Fields(line), " ") == want {
			return true
		}
	}
	return false
}

// runBlame drives the command against a fake engine and returns both streams.
func runBlame(
	t *testing.T, engine *fakeEngine, request cli.BlameRequest, opts render.Options,
) (stdout, stderr string, err error) {
	t.Helper()

	if opts.Width == 0 {
		opts.Width = goldenWidth
	}
	var out, errOut bytes.Buffer
	backend := &resolve.Backend{Engine: engine, ClusterID: fixtureCluster}

	err = cli.RunBlame(context.Background(), backend, request, ioStreams(&out, &errOut), opts)
	assertDrained(t, engine)
	return out.String(), errOut.String(), err
}

// TestBlameAttributesEveryField is the command's flagship output, and the
// interior-write case is what it is asserted on.
func TestBlameAttributesEveryField(t *testing.T) {
	stdout, stderr, err := runBlame(t, blamedCheckoutEngine(), defaultBlameRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "flagship", stdout, stderr)

	// Asserted by content as well as by golden file: a golden regenerated after a
	// regression would keep passing, and these are the rows the command exists for.
	for _, want := range [][]string{
		{"spec.template.spec.containers[0].resources.limits.memory", "2026-08-28 14:07:20.044",
			"argocd-application-controller"},
		{"spec.replicas", "2026-08-28 14:05:02.117", "kube-controller-manager"},
	} {
		if !hasRow(stdout, want...) {
			t.Errorf("the flagship attribution is missing.\nwant the row %v\ngot:\n%s", want, stdout)
		}
	}

	// The trap, stated as its own assertion: the memory limit was written directly
	// by kubectl at 14:03 and then again by argocd at 14:07, as part of a block
	// replace that never names it. Crediting kubectl is the wrong answer that looks
	// right.
	if hasRow(stdout, "spec.template.spec.containers[0].resources.limits.memory",
		"2026-08-28 14:03:11.482", "kubectl-client-side-apply") {
		t.Errorf("a field is credited to the last change that named it rather than to the last "+
			"change that wrote it:\n%s", stdout)
	}
}

// TestBlameMarksARemovedField covers the second question the command answers,
// which nothing else in the output can: who deleted a field that is no longer
// there to be listed.
func TestBlameMarksARemovedField(t *testing.T) {
	stdout, _, err := runBlame(t, blamedCheckoutEngine(), defaultBlameRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}

	if !strings.Contains(stdout, "spec.minReadySeconds  "+render.RemovedMarker) {
		t.Errorf("a field the window deleted is missing from the table, or is not marked:\n%s", stdout)
	}
	if !hasRow(stdout, "spec.minReadySeconds", render.RemovedMarker,
		"2026-08-28 14:05:02.117", "kube-controller-manager") {
		t.Errorf("the removal is not attributed to the change that made it:\n%s", stdout)
	}
}

// TestBlameStartsFromTheStateBeforeTheWindow is the acceptance criterion about
// bounded windows.
//
// The window opens after two of the five changes, so most of the object's fields
// were last written outside it. They must still be listed, marked, rather than
// omitted — an omission would read as an object that does not have those fields —
// and nothing inside the window may be credited with them.
func TestBlameStartsFromTheStateBeforeTheWindow(t *testing.T) {
	engine := blamedCheckoutEngine()
	request := defaultBlameRequest()
	request.Timeline.From = at("2026-08-28T14:04:00Z")

	stdout, stderr, err := runBlame(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "bounded-window", stdout, stderr)

	// A field never touched in the window: present, and honest about why it has no
	// attribution.
	if !hasRow(stdout, "metadata.name", render.BeforeWindow, "-") {
		t.Errorf("a field whose last write predates the window is missing or unmarked:\n%s", stdout)
	}
	// The replay anchored on the newest full-state row at or before the window
	// start, which is the Added row two changes earlier, with the patch between
	// them applied.
	if !strings.Contains(stdout, "Base:     2026-08-28T14:02:58Z (Added) plus 1 patch") {
		t.Errorf("the header does not name the row the attribution started from:\n%s", stdout)
	}

	// One reconstruction, not two: the prior-value replay `timeline` and `diff` run
	// would be a second round trip to recover values this table never prints.
	if engine.stateCalls != 1 {
		t.Errorf("%d reconstructions were requested, want 1", engine.stateCalls)
	}
	if strings.Contains(stderr, "prior values") {
		t.Errorf("blame printed a notice about prior values, which it does not render:\n%s", stderr)
	}
}

// TestBlameCollapsesAtDepth covers --depth, which is what keeps a fat object
// readable.
func TestBlameCollapsesAtDepth(t *testing.T) {
	request := defaultBlameRequest()
	request.Depth = 2

	stdout, stderr, err := runBlame(t, blamedCheckoutEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "depth", stdout, stderr)

	// The four fields under spec.template collapse into one row that says so. A
	// collapsed row without its count would claim a single field last changed then.
	if !hasRow(stdout, "spec.template", "2026-08-28 14:07:20.044", "4", "argocd-application-controller") {
		t.Errorf("a collapsed row does not carry the newest write under it, or its field count:\n%s",
			stdout)
	}
}

// TestBlameFiltersFields covers --field, which selects fields here rather than
// whole changes.
func TestBlameFiltersFields(t *testing.T) {
	engine := blamedCheckoutEngine()
	request := defaultBlameRequest()
	request.Fields = []string{"spec.template.spec.containers"}

	stdout, stderr, err := runBlame(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "field-filter", stdout, stderr)

	if strings.Contains(stdout, "spec.replicas") {
		t.Errorf("--field did not narrow the table:\n%s", stdout)
	}
	// The predicate is a display decision. Pushing it into the query would make the
	// rows a non-consecutive slice of history, and the replay over it would
	// attribute fields to changes that did not write them.
	if len(engine.queries) != 1 {
		t.Fatalf("%d timeline queries were issued, want 1", len(engine.queries))
	}
	if len(engine.queries[0].FieldPaths) != 0 {
		t.Errorf("the field predicate was pushed into the query as %v, which would leave the replay "+
			"attributing fields to changes that did not write them", engine.queries[0].FieldPaths)
	}
}

// TestBlameExplainsAFilterThatMatchesNothing keeps the two emptinesses apart: a
// mistyped path and an object with no fields lead a reader in opposite directions.
func TestBlameExplainsAFilterThatMatchesNothing(t *testing.T) {
	request := defaultBlameRequest()
	request.Fields = []string{"spec.nonesuch"}

	stdout, stderr, err := runBlame(t, blamedCheckoutEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	if strings.Contains(stdout, render.BeforeWindow) {
		t.Errorf("the filter kept rows it did not match:\n%s", stdout)
	}
	if !strings.Contains(stderr, "has no field at or beneath spec.nonesuch") {
		t.Errorf("an emptiness the filter produced is presented as an empty object:\n%s", stderr)
	}
}

// TestBlameWide covers -o wide: the resource version a reader takes to a
// controller's own logs, and the full precision the schema records.
func TestBlameWide(t *testing.T) {
	stdout, stderr, err := runBlame(
		t, blamedCheckoutEngine(), defaultBlameRequest(), render.Options{Wide: true})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "wide", stdout, stderr)
}

// TestBlameNotesADeletion is the case where the fields listed are not the fields
// the object has, because it does not have any.
func TestBlameNotesADeletion(t *testing.T) {
	engine := blamedCheckoutEngine()
	engine.changes = append(engine.changes, query.Change{
		TS: at("2026-08-28T14:12:00.000Z"), EventType: query.EventDeleted, UID: fixtureUID,
		ResourceVersion: "1006", APIVersion: "apps/v1",
	})

	stdout, stderr, err := runBlame(t, engine, defaultBlameRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	if !strings.Contains(stderr, "was deleted at 2026-08-28T14:12:00Z") {
		t.Errorf("a table of fields is presented for an object that no longer exists, unqualified:\n%s",
			stderr)
	}
	if !strings.Contains(stdout, "spec.replicas") {
		t.Errorf("the fields the object held before it was deleted were dropped:\n%s", stdout)
	}
}

// TestBlameDegradesWhenStateCannotBeReconstructed is Invariant 5 for this
// command: a backend that cannot rebuild the state still answers for the fields
// the window's own patches name.
func TestBlameDegradesWhenStateCannotBeReconstructed(t *testing.T) {
	engine := blamedCheckoutEngine()
	engine.stateErr = query.ErrCapabilityUnsupported
	request := defaultBlameRequest()
	request.Timeline.From = at("2026-08-28T14:04:00Z")

	stdout, stderr, err := runBlame(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	if !strings.Contains(stderr, "this backend cannot reconstruct state") {
		t.Errorf("the degradation was not reported:\n%s", stderr)
	}
	if !strings.Contains(stdout, "spec.replicas") {
		t.Errorf("the fields the window's own patches name were lost with the state:\n%s", stdout)
	}
	if strings.Contains(stdout, "metadata.name") {
		t.Errorf("a field was listed that no state could have supplied:\n%s", stdout)
	}
}

// TestBlameSeedsFromAFullStateRowInTheWindow is the other half of the
// degradation: the oldest full state already read becomes the base, and is
// treated as a base rather than as a change.
//
// A snapshot observing an object is not a change that wrote its fields, and
// crediting it with all of them would put a name against every field of an object
// it merely saw.
func TestBlameSeedsFromAFullStateRowInTheWindow(t *testing.T) {
	engine := blamedCheckoutEngine()
	engine.stateErr = query.ErrObjectNotFound
	engine.changes[0] = query.Change{
		TS: at("2026-08-28T14:02:58.001Z"), EventType: query.EventSnapshot, UID: fixtureUID,
		Actors: []string{"kubectl-client-side-apply"}, ResourceVersion: "1001",
		APIVersion: "apps/v1", Data: fixtureState,
	}

	stdout, stderr, err := runBlame(t, engine, defaultBlameRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	if !strings.Contains(stdout, "Base:     2026-08-28T14:02:58Z (Snapshot), the oldest full state") {
		t.Errorf("the header does not name the row the attribution fell back to:\n%s", stdout)
	}
	if !hasRow(stdout, "metadata.name", render.BeforeWindow, "-") {
		t.Errorf("a snapshot was credited with the fields of the object it observed:\n%s", stdout)
	}
	if !strings.Contains(stderr, "no state survives from before this window") {
		t.Errorf("the fallback was not reported:\n%s", stderr)
	}
}

// TestBlameExitsThreeWhenNothingWasWatching carries Invariant 9 into the command.
//
// An object with no recorded history and no state is not an object with no
// fields, and the difference is the whole of what exit 3 says.
func TestBlameExitsThreeWhenNothingWasWatching(t *testing.T) {
	engine := blamedCheckoutEngine()
	engine.changes = nil
	engine.incarnations = nil
	engine.intervals = nil

	stdout, stderr, err := runBlame(t, engine, defaultBlameRequest(), render.Options{})
	if err == nil {
		t.Fatal("a scope nobody watched was reported as an object with no fields")
	}
	if code := exit.CodeFor(err); code != exit.NoCoverage {
		t.Errorf("exit code %d, want %d", code, exit.NoCoverage)
	}
	if !errors.Is(err, query.ErrNoCoverage) {
		t.Errorf("the failure does not carry query.ErrNoCoverage, so nothing maps it to an exit code: %v",
			err)
	}
	// The finding is written by the caller that turns an error into an exit code,
	// so the golden carries it the way `timeline`'s does: what a person sees is
	// both streams and the line that ends the invocation.
	assertGoldenIn(t, "blame", "empty-without-coverage", stdout, stderr+"error: "+err.Error()+"\n")
}

// TestBlameListsFieldsWhenNothingChangedInTheWindow is the third answer Invariant
// 9 distinguishes, and the one only this command can render usefully: the scope
// was watched, nothing changed, and the object's fields are still worth listing —
// every one of them older than the window.
func TestBlameListsFieldsWhenNothingChangedInTheWindow(t *testing.T) {
	engine := blamedCheckoutEngine()
	request := defaultBlameRequest()
	request.Timeline.From = at("2026-08-28T14:30:00Z")
	request.Timeline.To = at("2026-08-28T15:00:00Z")

	stdout, stderr, err := runBlame(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	assertGoldenIn(t, "blame", "quiet-window", stdout, stderr)

	if !strings.Contains(stdout, render.BeforeWindow) {
		t.Errorf("a window with no changes in it produced no field list at all:\n%s", stdout)
	}
	if !strings.Contains(stderr, "nothing changed in that period") {
		t.Errorf("the silence was not explained against coverage:\n%s", stderr)
	}
}

// TestBlameStructuredFormats pins the D19 envelope for the fifth kind.
func TestBlameStructuredFormats(t *testing.T) {
	for _, format := range []struct {
		name   string
		format render.StructuredFormat
	}{
		{"json", render.StructuredJSON},
		{"jsonl", render.StructuredJSONL},
		{"yaml", render.StructuredYAML},
	} {
		t.Run(format.name, func(t *testing.T) {
			request := defaultBlameRequest()
			request.Timeline.From = at("2026-08-28T14:04:00Z")
			request.Timeline.Structured = format.format
			request.Fields = []string{"spec"}

			stdout, stderr, err := runBlame(t, blamedCheckoutEngine(), request, render.Options{})
			if err != nil {
				t.Fatalf("RunBlame: %v", err)
			}
			assertGoldenIn(t, "blame", format.name, stdout, stderr)
		})
	}
}

// TestBlameStructuredMarksAnUnattributedField is the contract's own version of
// the "(before window)" cell.
//
// A consumer reading ts alone cannot tell a null that means "older than the
// window" from a field that has no answer for some other reason, which is why the
// boolean exists and why this asserts both halves of it.
func TestBlameStructuredMarksAnUnattributedField(t *testing.T) {
	request := defaultBlameRequest()
	request.Timeline.From = at("2026-08-28T14:04:00Z")
	request.Timeline.Structured = render.StructuredJSONL
	request.Fields = []string{"metadata.name"}

	stdout, _, err := runBlame(t, blamedCheckoutEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunBlame: %v", err)
	}
	for _, want := range []string{`"attributed":false`, `"ts":null`, `"actors":[]`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("a field older than the window does not carry %s:\n%s", want, stdout)
		}
	}
}
