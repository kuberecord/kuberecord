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

// `diff` against the same fixture `timeline` uses, so the two commands can be
// read side by side and seen to be answering the same question.
//
// The golden files carry both streams for the reason the timeline's do: the split
// between the document and its qualifications is itself under test.

// runDiff drives the command against a fake engine and returns both streams.
func runDiff(
	t *testing.T, engine *fakeEngine, request cli.DiffRequest, opts render.Options,
) (stdout, stderr string, err error) {
	t.Helper()

	if opts.Width == 0 {
		opts.Width = goldenWidth
	}
	var out, errOut bytes.Buffer
	backend := &resolve.Backend{Engine: engine, ClusterID: fixtureCluster}

	err = cli.RunDiff(context.Background(), backend, request, ioStreams(&out, &errOut), opts)
	assertDrained(t, engine)
	return out.String(), errOut.String(), err
}

// defaultDiffRequest is a bare `diff deploy/checkout -n payments`.
func defaultDiffRequest() cli.DiffRequest {
	return cli.DiffRequest{Timeline: defaultRequest()}
}

// watchedCheckoutEngine is the fixture backend every test below starts from.
func watchedCheckoutEngine() *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}
}

// TestDiffRendersHunks is the command's flagship output: path, the value that
// went, the value that arrived, under a header naming when and who.
func TestDiffRendersHunks(t *testing.T) {
	stdout, stderr, err := runDiff(t, watchedCheckoutEngine(), defaultDiffRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "flagship", stdout, stderr)

	// The hunk the acceptance criteria describe, asserted by content as well as
	// by golden file: a golden file regenerated after a regression would keep
	// passing, and these three lines are what the command exists to print.
	for _, want := range []string{
		"~ spec.template.spec.containers[0].resources.limits.memory",
		"- 2Gi",
		"+ 512Mi",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the flagship hunk is missing.\nwant a line containing %q\ngot:\n%s", want, stdout)
		}
	}
}

// TestDiffAttributesEachChange checks the other half of a hunk: when it happened
// and who was seen on the object.
func TestDiffAttributesEachChange(t *testing.T) {
	stdout, _, err := runDiff(t, watchedCheckoutEngine(), defaultDiffRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}

	const want = "2026-08-28 14:03:11.482 UTC  Modified  kubectl-client-side-apply"
	if !strings.Contains(stdout, want) {
		t.Errorf("a change is not attributed.\nwant a line containing %q\ngot:\n%s", want, stdout)
	}
}

// TestDiffOldestFirst covers --reverse, which reorders the blocks and must not
// select different ones.
func TestDiffOldestFirst(t *testing.T) {
	request := defaultDiffRequest()
	request.Timeline.Reverse = true

	stdout, stderr, err := runDiff(t, watchedCheckoutEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "reverse", stdout, stderr)
}

// TestDiffWide covers -o wide, which adds the resource version a reader takes to
// a controller's own logs.
func TestDiffWide(t *testing.T) {
	stdout, stderr, err := runDiff(
		t, watchedCheckoutEngine(), defaultDiffRequest(), render.Options{Wide: true})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "wide", stdout, stderr)
}

// TestDiffFieldFilterKeepsPriorValues is the criterion behind the client-side
// filter.
//
// A path predicate pushed into the query would make the returned rows a
// non-consecutive slice of history, which switches the state replay off and
// leaves every hunk with a "+" and no "-" — the command's whole point, removed by
// one of its own flags. So the query goes out unfiltered and the narrowing
// happens afterwards, and this asserts both halves of that: the hunk still has
// its old value, and the query really did carry no field predicate.
func TestDiffFieldFilterKeepsPriorValues(t *testing.T) {
	engine := watchedCheckoutEngine()
	request := defaultDiffRequest()
	request.Timeline.DisplayFieldPaths = []string{"spec.replicas"}

	stdout, stderr, err := runDiff(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "field-filter", stdout, stderr)

	if !strings.Contains(stdout, "- 3") || !strings.Contains(stdout, "+ 5") {
		t.Errorf("the filtered hunk lost the value it replaced:\n%s", stdout)
	}
	if len(engine.queries) != 1 {
		t.Fatalf("%d timeline queries were issued, want 1", len(engine.queries))
	}
	if len(engine.queries[0].FieldPaths) != 0 {
		t.Errorf("the field predicate was pushed into the query as %v, which would make the rows "+
			"non-consecutive and cost every hunk its old value", engine.queries[0].FieldPaths)
	}
	if !strings.Contains(stderr, "the rest were read and replayed") {
		t.Errorf("the reader is not told how many changes were examined:\n%s", stderr)
	}

	// The predicate selects whole changes, not individual operations: it is
	// query.MatchesFieldPaths unchanged, so `diff --field` and `timeline --field`
	// agree about which changes a path selects. The other fields the same change
	// moved are shown as the context they are.
	if !strings.Contains(stdout, "+ spec.paused") {
		t.Errorf("a selected change was shown without the other fields it moved at the same "+
			"instant:\n%s", stdout)
	}
}

// TestDiffFieldFilterThatMatchesNothing is the emptiness a filter produces, which
// is a different fact from an empty window and must not be explained as one.
func TestDiffFieldFilterThatMatchesNothing(t *testing.T) {
	engine := watchedCheckoutEngine()
	// A window that starts after the first sighting, so no boundary row is in
	// range. query.MatchesFieldPaths keeps a row carrying no patch whatever the
	// predicate — a first sighting is the beginning of the object's existence and
	// a filter that dropped it would show a history with no beginning — so the
	// only way to reach a genuinely empty filtered result is a window of nothing
	// but patches. The seed state is what the replay anchors on in its place.
	engine.state = fixtureState

	request := defaultDiffRequest()
	request.Timeline.From = at("2026-08-28T14:03:00Z")
	request.Timeline.DisplayFieldPaths = []string{"spec.nonesuch"}

	stdout, stderr, err := runDiff(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "field-filter-empty", stdout, stderr)

	if !strings.Contains(stderr, "the window itself is not empty") {
		t.Errorf("an emptiness the filter produced is presented as an empty window:\n%s", stderr)
	}
	if strings.Contains(stderr, "nothing changed in that period") {
		t.Errorf("the filter's emptiness was explained against coverage, which answers a question "+
			"nobody asked:\n%s", stderr)
	}
}

// TestDiffExitCodeReportsChangesFound is git-diff's contract: 1 when there is
// something to see.
func TestDiffExitCodeReportsChangesFound(t *testing.T) {
	request := defaultDiffRequest()
	request.ExitCode = true

	stdout, stderr, err := runDiff(t, watchedCheckoutEngine(), request, render.Options{})
	if err == nil {
		t.Fatal("--exit-code returned no error, so the process would exit 0 with changes on screen")
	}
	if code := exit.CodeFor(err); code != exit.RuntimeError {
		t.Errorf("exit code %d, want %d", code, exit.RuntimeError)
	}
	if stdout == "" {
		t.Error("--exit-code suppressed the document it is reporting on")
	}
	if !strings.Contains(stderr, "--exit-code reports that as exit 1") {
		t.Errorf("the reader is not told what the exit code means:\n%s", stderr)
	}

	// The code is a finding rather than a failure, so nothing may print "error:".
	var coded *exit.Error
	if !errors.As(err, &coded) || !coded.Quiet {
		t.Errorf("the changes-found result is not quiet, so `error: …` would be printed over a "+
			"successful query: %#v", err)
	}
}

// TestDiffExitCodeIsZeroWithoutChanges is the other half of the contract.
func TestDiffExitCodeIsZeroWithoutChanges(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.changes = nil
	request := defaultDiffRequest()
	request.ExitCode = true

	_, stderr, err := runDiff(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("--exit-code with no changes returned %v, want nil so the process exits 0", err)
	}
	if !strings.Contains(stderr, "no changes recorded") {
		t.Errorf("an empty result was presented without an explanation:\n%s", stderr)
	}
}

// TestDiffNoCoverageOutranksExitCode is Invariant 9 against a flag that would
// otherwise hide it.
//
// A script told "no changes" when nothing was ever watching has been given the
// one answer the invariant exists to prevent, so the finding keeps its own exit
// code whatever --exit-code asked for.
func TestDiffNoCoverageOutranksExitCode(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.changes = nil
	engine.intervals = nil
	request := defaultDiffRequest()
	request.ExitCode = true

	_, stderr, err := runDiff(t, engine, request, render.Options{})
	if err == nil {
		t.Fatal("a scope nobody watched was reported as success")
	}
	if code := exit.CodeFor(err); code != exit.NoCoverage {
		t.Errorf("exit code %d, want %d", code, exit.NoCoverage)
	}
	if !errors.Is(err, query.ErrNoCoverage) {
		t.Errorf("the finding does not carry the read plane's own sentinel: %v", err)
	}
	if strings.Contains(stderr, "--exit-code reports") {
		t.Errorf("--exit-code claimed changes were found for a scope nobody watched:\n%s", stderr)
	}
}

// TestDiffEmptyWindowWithCoverage is the third answer Invariant 9 distinguishes:
// something was watching, and nothing changed.
func TestDiffEmptyWindowWithCoverage(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.changes = nil

	stdout, stderr, err := runDiff(t, engine, defaultDiffRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "empty-with-coverage", stdout, stderr)
}

// TestDiffWarnsWhenDeletionsCannotBeRecorded carries Invariant 4 into the second
// command that renders a history.
//
// A diff that simply stops is as misleading as a timeline that does, and the
// notice is the honest rendering of a silence the backend cannot fill.
func TestDiffWarnsWhenDeletionsCannotBeRecorded(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.caps = archiveCapabilities()
	request := defaultDiffRequest()
	request.Timeline.From = at("2026-08-28T00:00:00Z")
	request.Timeline.To = at("2026-08-28T15:00:00Z")

	_, stderr, err := runDiff(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	if !strings.Contains(stderr, "does not record deletions") {
		t.Errorf("a backend that cannot record deletions produced no notice:\n%s", stderr)
	}
}
