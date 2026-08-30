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
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// What a cold scan has to say for itself before it starts, and what stops it.
//
// The behaviour under test is the difference between the zero-infrastructure mode
// being an honest trade and being a tool that hangs: the cost is stated before the
// first object is fetched, a wide window is a decision rather than an accident, and
// a scan that runs away is stopped by a flag that names itself.
//
// Everything here drives the command through cli.RunTimeline with a fake engine
// that declares the two optional halves of the read plane, because that is exactly
// what a real archive engine is from the command's point of view. The terminal is
// not faked: whether a prompt can be asked for is a field the command fills in from
// its streams (cli.ScanOptions), which is what makes this testable without a
// pseudo-terminal — the same split render.Options already keeps for the width.

// The estimate the acceptance criteria spell out: `~1,240 objects, ~3.1 GiB`.
const (
	fixtureScanObjects = 1240
	fixtureScanBytes   = 3328599655
	fixtureScanFigures = "~1,240 objects, ~3.1 GiB"
)

// scanningEngine is a fakeEngine that also answers the read plane's optional
// halves: what a scan will cost, and how far it has got.
//
// It reports its progress from inside Timeline, before consulting the context, so
// that a circuit breaker tripped by that progress is observed by the same call —
// which is what a real engine does, where the breaker fires from a fetch goroutine
// while its siblings are still running.
type scanningEngine struct {
	*fakeEngine

	estimate    query.ScanEstimate
	estimateErr error
	estimates   int

	// emits is the progress this engine reports when a timeline is asked for.
	emits  []query.ScanProgress
	report func(query.ScanProgress)
}

func newScanningEngine(caps query.Capabilities) *scanningEngine {
	return &scanningEngine{
		fakeEngine: &fakeEngine{
			caps:         caps,
			changes:      checkoutHistory(),
			incarnations: checkoutIncarnations(),
			intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
		},
		estimate: query.ScanEstimate{
			Objects: fixtureScanObjects, Bytes: fixtureScanBytes, Partitions: 24,
		},
	}
}

func (e *scanningEngine) EstimateScan(
	_ context.Context, _ string, _, _ time.Time,
) (query.ScanEstimate, error) {
	e.estimates++
	if e.estimateErr != nil {
		return query.ScanEstimate{}, e.estimateErr
	}
	return e.estimate, nil
}

func (e *scanningEngine) SetScanProgress(report func(query.ScanProgress)) { e.report = report }

func (e *scanningEngine) Timeline(
	ctx context.Context, q query.TimelineQuery,
) (query.ChangeIterator, error) {
	for _, progress := range e.emits {
		if e.report != nil {
			e.report(progress)
		}
	}
	// After the reporting and before any work, which is where a real scan finds a
	// cancellation: the breaker fires from the progress callback, so the fetch that
	// notices is the one after it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.fakeEngine.Timeline(ctx, q)
}

// scanRequest is a `timeline` over a window of the given width, ending at the
// fixture's present.
func scanRequest(window time.Duration, opts cli.ScanOptions) cli.TimelineRequest {
	request := defaultRequest()
	request.To = request.Now
	request.From = request.Now.Add(-window)
	request.Scan = opts
	return request
}

// runScan drives the command and returns both streams, with stdin under the test's
// control so that the confirmation can be answered.
func runScan(
	t *testing.T, engine *scanningEngine, request cli.TimelineRequest, stdin string,
) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	streams := genericiooptions.IOStreams{
		In: strings.NewReader(stdin), Out: &out, ErrOut: &errOut,
	}
	backend := &cli.Backend{Engine: engine, ClusterID: fixtureCluster}

	err = cli.RunTimeline(context.Background(), backend, request,
		streams, render.Options{Width: goldenWidth})
	assertDrained(t, engine.fakeEngine)
	return out.String(), errOut.String(), err
}

// TestColdScanStatesWhatTheQuestionWillCost is the estimate rendered before the
// scan, which is the whole of what makes PointQuery=false an honest declaration
// rather than a footnote.
func TestColdScanStatesWhatTheQuestionWillCost(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())

	stdout, stderr, err := runScan(t, engine, scanRequest(6*time.Hour, cli.ScanOptions{}), "")
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	// The window is named beside the figures. The estimate is written before the
	// notices — it exists to be read before the scan — so at this moment nothing
	// else on the terminal has said which window the number describes.
	const want = fixtureScanFigures + " to scan for 6h"
	if !strings.Contains(stderr, want) {
		t.Errorf("the estimate is missing from stderr.\nwant a line containing %q\ngot:\n%s",
			want, stderr)
	}
	if engine.estimates != 1 {
		t.Errorf("the scan was estimated %d times, want exactly once", engine.estimates)
	}
	if strings.Contains(stdout, "objects") {
		t.Errorf("the estimate reached stdout, where the document is:\n%s", stdout)
	}
	// A narrow window states its cost and gets on with it. A tool that asked
	// permission for every question would train people to stop reading the question.
	if strings.Contains(stderr, "[y/N]") {
		t.Errorf("a %s window asked for confirmation; only one wider than %s should:\n%s",
			cli.DescribeSpan(6*time.Hour), cli.DescribeSpan(cli.ConfirmWindow), stderr)
	}
	if len(engine.queries) != 1 {
		t.Errorf("%d timeline queries were issued, want 1", len(engine.queries))
	}
}

// TestColdScanAsksBeforeAWideWindow covers the confirmation itself: the prompt is
// the estimate with a question on the end of it.
func TestColdScanAsksBeforeAWideWindow(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())
	request := scanRequest(30*24*time.Hour, cli.ScanOptions{Interactive: true})

	_, stderr, err := runScan(t, engine, request, "y\n")
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	const want = fixtureScanFigures + " — continue? [y/N] "
	if !strings.Contains(stderr, want) {
		t.Errorf("the confirmation is missing.\nwant %q\ngot:\n%s", want, stderr)
	}
	if len(engine.queries) != 1 {
		t.Errorf("%d timeline queries were issued after a yes, want 1", len(engine.queries))
	}
}

// TestColdScanStopsWhenTheConfirmationIsDeclined pins the direction [y/N]
// promises: anything that is not a yes reads nothing at all.
func TestColdScanStopsWhenTheConfirmationIsDeclined(t *testing.T) {
	for name, answer := range map[string]string{
		"an explicit no": "n\n",
		"a bare newline": "\n",
		"a closed stdin": "",
		"a typo":         "yep\n",
	} {
		t.Run(name, func(t *testing.T) {
			engine := newScanningEngine(archiveCapabilities())
			request := scanRequest(30*24*time.Hour, cli.ScanOptions{Interactive: true})

			stdout, stderr, err := runScan(t, engine, request, answer)
			if err == nil {
				t.Fatal("a declined confirmation succeeded; a question that was never asked of the " +
					"backend must not exit as though it had been answered")
			}
			if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
				t.Errorf("a declined confirmation exits %d, want %d", code, cli.ExitRuntimeError)
			}
			if len(engine.queries) != 0 {
				t.Errorf("%d queries were issued after a refusal", len(engine.queries))
			}
			if stdout != "" {
				t.Errorf("a refused scan wrote a document to stdout:\n%s", stdout)
			}
			// The way out has to be in the message, or the refusal is a dead end.
			for _, hint := range []string{"--since", "--max-objects", "--yes"} {
				if !strings.Contains(err.Error(), hint) {
					t.Errorf("the refusal does not mention %s: %v", hint, err)
				}
			}
			if !strings.Contains(stderr, "[y/N]") {
				t.Errorf("the prompt itself never reached stderr:\n%s", stderr)
			}
		})
	}
}

// TestColdScanAssumesYesWhenItCannotAsk is the half that keeps a script from
// hanging: a prompt nobody can answer is not asked, and why it was not asked is
// said out loud.
func TestColdScanAssumesYesWhenItCannotAsk(t *testing.T) {
	for name, testCase := range map[string]struct {
		opts cli.ScanOptions
		want string
	}{
		"not a terminal": {
			opts: cli.ScanOptions{},
			want: "Not a terminal, so the confirmation was assumed.",
		},
		"--yes": {
			opts: cli.ScanOptions{AssumeYes: true, Interactive: true},
			want: "Confirmed by --yes.",
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine := newScanningEngine(archiveCapabilities())
			request := scanRequest(30*24*time.Hour, testCase.opts)

			// Stdin says no. Neither case may read it: one has nobody at the keyboard,
			// and the other has already been answered on the command line.
			_, stderr, err := runScan(t, engine, request, "n\n")
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if !strings.Contains(stderr, testCase.want) {
				t.Errorf("stderr does not explain why nothing was asked.\nwant %q\ngot:\n%s",
					testCase.want, stderr)
			}
			if strings.Contains(stderr, "[y/N]") {
				t.Errorf("a prompt was written to an invocation that cannot answer one:\n%s", stderr)
			}
			if !strings.Contains(stderr, fixtureScanFigures) {
				t.Errorf("the cost went unstated even though nothing was asked:\n%s", stderr)
			}
			if len(engine.queries) != 1 {
				t.Errorf("%d timeline queries were issued, want 1", len(engine.queries))
			}
		})
	}
}

// TestColdScanDoesNotGuardAnIndexedBackend keys the whole guard on the declared
// capability rather than on a backend's name (D17).
//
// ClickHouse seeks to the object's rows, so the window is a predicate rather than
// the work: an estimate, a prompt and a progress line would all be describing a
// cost that does not exist.
func TestColdScanDoesNotGuardAnIndexedBackend(t *testing.T) {
	engine := newScanningEngine(clickHouseCapabilities())
	request := scanRequest(90*24*time.Hour, cli.ScanOptions{Interactive: true, ShowProgress: true})

	_, stderr, err := runScan(t, engine, request, "n\n")
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if engine.estimates != 0 {
		t.Errorf("an indexed backend was asked for a scan estimate %d times", engine.estimates)
	}
	if engine.report != nil {
		t.Error("a progress callback was installed on an indexed backend")
	}
	for _, unwanted := range []string{"to scan", "[y/N]", "scanning "} {
		if strings.Contains(stderr, unwanted) {
			t.Errorf("an indexed backend was narrated as a cold scan (%q):\n%s", unwanted, stderr)
		}
	}
}

// TestColdScanReportsAnEstimateItCouldNotMake is Invariant 5 at this seam: the
// degradation is announced, and it does not become the failure.
func TestColdScanReportsAnEstimateItCouldNotMake(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())
	engine.estimateErr = errors.New("the bucket refused a listing")

	_, stderr, err := runScan(t, engine, scanRequest(6*time.Hour, cli.ScanOptions{}), "")
	if err != nil {
		t.Fatalf("RunTimeline: %v; a scan whose warning could not be assembled is still answerable", err)
	}
	if !strings.Contains(stderr, "could not be estimated") {
		t.Errorf("a failed estimate was swallowed; silence in front of a scan reads as \"this is "+
			"cheap\":\n%s", stderr)
	}
	if !strings.Contains(stderr, "the bucket refused a listing") {
		t.Errorf("the reason the estimate failed was dropped:\n%s", stderr)
	}
	if len(engine.queries) != 1 {
		t.Errorf("%d timeline queries were issued, want 1", len(engine.queries))
	}
}

// TestColdScanBreaksTheCircuitAtMaxObjects is the bound on the work that --limit
// cannot supply: without an index, a hundred newest changes still cost every
// object in the window.
func TestColdScanBreaksTheCircuitAtMaxObjects(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())
	engine.emits = []query.ScanProgress{
		{Objects: 1, Bytes: 100}, {Objects: 2, Bytes: 200}, {Objects: 3, Bytes: 300},
	}
	request := scanRequest(6*time.Hour, cli.ScanOptions{MaxObjects: 2})

	stdout, _, err := runScan(t, engine, request, "")
	if err == nil {
		t.Fatal("a scan past its circuit breaker succeeded; a result that stopped early must not " +
			"present itself as a complete one")
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
		t.Errorf("a tripped breaker exits %d, want %d", code, cli.ExitRuntimeError)
	}
	// The flag has to be named. "context canceled" is what the query actually
	// failed with, and it tells the reader nothing they can act on.
	for _, want := range []string{"--max-objects=2", "3 objects", "--since"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure does not mention %q: %v", want, err)
		}
	}
	if stdout != "" {
		t.Errorf("a scan stopped by its breaker wrote a document anyway:\n%s", stdout)
	}
}

// TestColdScanPaintsProgressToStderr covers the narration and the two rules it
// keeps: it goes to stderr, and it goes nowhere at all when nothing is watching.
func TestColdScanPaintsProgressToStderr(t *testing.T) {
	for name, testCase := range map[string]struct {
		show bool
		want bool
	}{
		"a terminal": {show: true, want: true},
		"a pipe":     {show: false, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			engine := newScanningEngine(archiveCapabilities())
			engine.emits = []query.ScanProgress{{Objects: 7, Bytes: 2 << 20}}
			request := scanRequest(6*time.Hour, cli.ScanOptions{ShowProgress: testCase.show})

			stdout, stderr, err := runScan(t, engine, request, "")
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}

			const line = "scanning 7/1,240 objects, 2.0 MiB read"
			if got := strings.Contains(stderr, line); got != testCase.want {
				t.Errorf("progress on stderr = %t, want %t.\nwant %q\ngot:\n%q",
					got, testCase.want, line, stderr)
			}
			if strings.Contains(stdout, "scanning") {
				t.Errorf("progress reached stdout, which is the document:\n%q", stdout)
			}
			// The line is transient: it is blanked before the notices are written, or
			// it would sit above them on the terminal as though it were one of them.
			erased := "\r" + strings.Repeat(" ", len(line)) + "\r"
			if testCase.want && !strings.Contains(stderr, erased) {
				t.Errorf("the progress line was never erased:\n%q", stderr)
			}
		})
	}
}

// TestColdScanDefaultsToADayAgainstAnUnindexedBackend is the AC's default window,
// asserted on the query rather than on the notice: what the backend was actually
// asked is the part that costs.
func TestColdScanDefaultsToADayAgainstAnUnindexedBackend(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())
	request := defaultRequest()
	request.Scan = cli.ScanOptions{}

	if _, _, err := runScan(t, engine, request, ""); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if len(engine.queries) != 1 {
		t.Fatalf("%d timeline queries were issued, want 1", len(engine.queries))
	}

	asked := engine.queries[0]
	if want := request.Now.Add(-cli.DefaultWindow); !asked.From.Equal(want) {
		t.Errorf("the default window starts at %s, want %s (%s)",
			asked.From, want, cli.DescribeSpan(cli.DefaultWindow))
	}
	if !asked.To.Equal(request.Now) {
		t.Errorf("the default window ends at %s, want now (%s)", asked.To, request.Now)
	}
	// The default is narrow enough that it can never be the thing that triggers a
	// confirmation; if it were, every bare invocation would ask.
	if cli.DefaultWindow > cli.ConfirmWindow {
		t.Errorf("the default window (%s) is wider than the one that needs confirming (%s)",
			cli.DescribeSpan(cli.DefaultWindow), cli.DescribeSpan(cli.ConfirmWindow))
	}
}

// TestInterruptedScanFailsRatherThanLookingShort is Ctrl-C reaching the query
// through the context, and the answer that comes back saying so.
//
// The engine returns whatever its context does, exactly as the archive engine does
// when its fetches are abandoned. What is asserted is that the command does not
// present the rows it had: a timeline short by an unknown amount, rendered as
// though it were complete, is the failure Invariant 4 exists to prevent, and it is
// worse here than a slow answer because nothing distinguishes the two.
func TestInterruptedScanFailsRatherThanLookingShort(t *testing.T) {
	engine := newScanningEngine(archiveCapabilities())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out, errOut bytes.Buffer
	streams := genericiooptions.IOStreams{In: strings.NewReader(""), Out: &out, ErrOut: &errOut}
	backend := &cli.Backend{Engine: engine, ClusterID: fixtureCluster}

	err := cli.RunTimeline(ctx, backend, scanRequest(6*time.Hour, cli.ScanOptions{}),
		streams, render.Options{Width: goldenWidth})
	if err == nil {
		t.Fatal("an interrupted scan succeeded")
	}
	if code := cli.ExitCodeFor(err); code != cli.ExitRuntimeError {
		t.Errorf("an interrupted scan exits %d, want %d", code, cli.ExitRuntimeError)
	}
	if out.String() != "" {
		t.Errorf("an interrupted scan wrote a document to stdout:\n%s", out.String())
	}
	// The breaker's sentence must not be borrowed for a cancellation nobody
	// configured: the two reasons a scan ends arrive as the same context error.
	if strings.Contains(err.Error(), "--max-objects") {
		t.Errorf("an interruption was reported as a circuit breaker: %v", err)
	}
	assertDrained(t, engine.fakeEngine)
}

// TestRunContextReportsAnInterruption covers the top of the process: whatever a
// command made of a cancellation, an interrupted invocation exits non-zero and
// says the word.
//
// The check is deliberately not conditioned on the command failing. A command that
// swallowed its own cancellation and returned nil would otherwise exit 0, and the
// exit code is the half a wrapper script reads.
func TestRunContextReportsAnInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out, errOut bytes.Buffer
	streams := genericiooptions.IOStreams{In: strings.NewReader(""), Out: &out, ErrOut: &errOut}

	// `config view` needs no backend and no cluster, so what is under test is the
	// interruption handling rather than a resolution failure that happens to occur
	// alongside it.
	code := cli.RunContext(ctx, []string{"kuberecord", "config", "view"}, streams)
	if code != cli.ExitRuntimeError {
		t.Errorf("an interrupted invocation exits %d, want %d", code, cli.ExitRuntimeError)
	}
	if !strings.Contains(errOut.String(), "interrupted") {
		t.Errorf("stderr does not say the run was interrupted:\n%s", errOut.String())
	}
}
