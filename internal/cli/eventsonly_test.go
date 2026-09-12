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

// `timeline --events-only`: the rows it keeps, the question it changes, and the
// error it must not raise.
//
// # The trap, stated once
//
// A Deployment's own timeline and the Events about it are answered against two
// different watch scopes, and every sentence this CLI writes about a silence is
// measured against one of them. --events-only moves the subject from the first to
// the second — that is the whole feature — and the cost of forgetting it is not a
// wrong word. uncoveredNoChanges turns "no changes, and nothing was watching this
// kind" into query.ErrNoCoverage at exit 3, so a Pod nobody watched, with four
// perfectly good Events recorded about it, would come back as a failure about the
// state the reader had just excluded — suppressing the answer to report the absence
// of something nobody asked for.
//
// TestEventsOnlyDoesNotRaiseTheStateScopesNoCoverageFinding is the guard, and it is
// named so that nobody deletes it while tidying.

package cli_test

import (
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// eventsOnlyRequest is `timeline deploy/checkout -n payments --events-only`, over
// the bounded window every Event fixture in this package uses.
//
// It sets EventsOnly and deliberately not WithEvents, which is the implication
// under test everywhere below: a request carrying one of the pair must behave as
// though it carried both, or the flag is a switch that turns nothing on.
func eventsOnlyRequest() cli.TimelineRequest {
	request := defaultRequest()
	request.From = at("2026-08-01T00:00:00Z")
	request.To = at("2026-08-28T15:00:00Z")
	request.EventsOnly = true
	return request
}

// watchedObjectWithEvents is the ordinary case: an object with its own history, a
// scope that covered it, Events about it, and a rule that recorded them.
func watchedObjectWithEvents() *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      shortHistory(),
		incarnations: checkoutIncarnations(),
		events:       checkoutEvents(),
		intervals: append(deploymentScope(),
			eventsWatchedBy("", "ClusterStreamRule/all-events")),
	}
}

// TestEventsOnlyRendersTheEventsAndNoneOfTheObjectsOwnChanges is the flag doing
// what it says, with the header that goes with it.
func TestEventsOnlyRendersTheEventsAndNoneOfTheObjectsOwnChanges(t *testing.T) {
	for mode, color := range map[string]bool{"": false, "-color": true} {
		t.Run("events present"+mode, func(t *testing.T) {
			engine := watchedObjectWithEvents()

			stdout, stderr, err := runTimeline(t, engine, eventsOnlyRequest(),
				render.Options{Color: color, EventsOnly: true})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			assertGolden(t, "events-only-flag-present"+mode, stdout, stderr)
		})
	}
}

// TestEventsOnlyImpliesWithEvents pins the composition the acceptance criteria ask
// for: the two flags compose, and requiring both would be pedantic.
//
// The assertion is on the query rather than on the page, because the page cannot
// tell the difference between "the Events were asked for and none exist" and "the
// Events were never asked for". That is the same indistinguishability D31 is about,
// one layer down.
func TestEventsOnlyImpliesWithEvents(t *testing.T) {
	engine := watchedObjectWithEvents()

	if _, _, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if len(engine.queries) != 1 {
		t.Fatalf("the command issued %d queries, want 1: %+v", len(engine.queries), engine.queries)
	}
	if !engine.queries[0].IncludeEvents {
		t.Errorf("--events-only did not ask for Events: %+v. The flag implies --with-events, and a "+
			"query carrying EventsOnly without IncludeEvents asks for no Events and excludes "+
			"everything else — which is an empty answer by construction", engine.queries[0])
	}
	if !engine.queries[0].EventsOnly {
		t.Errorf("--events-only did not reach the query: %+v", engine.queries[0])
	}
}

// TestEventsOnlyIssuesNoStateQuery is the cost property, asserted with a counting
// fake.
//
// Three reads of the object's own rows are available to this command and none of
// them may happen: the timeline's state half, the incarnation listing that fills
// the header's UID, and the prior-value reconstruction that recovers what each
// patch replaced. Every one of them is invisible in the output — a second round
// trip looks exactly like no second round trip — which is why this is a counter and
// not an eye.
func TestEventsOnlyIssuesNoStateQuery(t *testing.T) {
	engine := watchedObjectWithEvents()

	if _, _, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{}); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	if engine.stateScans != 0 {
		t.Errorf("the backend read the object's own history %d time(s); --events-only exists so "+
			"that it does not. On a backend with no index that scan is the expensive half of the "+
			"question, and one that read it and dropped the rows afterwards would cost exactly what "+
			"it cost before", engine.stateScans)
	}
	if engine.incarnationCalls != 0 {
		t.Errorf("the incarnations were listed %d time(s); that is the same rows under another "+
			"name, bought to print a UID the page does not support", engine.incarnationCalls)
	}
	if engine.stateCalls != 0 {
		t.Errorf("%d state reconstruction(s) ran; an Event carries no patch, so there is no prior "+
			"value to recover and no replay to anchor", engine.stateCalls)
	}
	for i, q := range engine.queries {
		if !q.EventsOnly {
			t.Errorf("query %d asks for the object's own rows: %+v", i, q)
		}
	}
}

// TestEventsOnlyReportsTheCoverageOfEvents is the header half of the substitution.
//
// The rows come from the Event scope, so the coverage line has to be about the
// Event scope — and it has to say so, because a coverage summary is a well-formed
// interval with a rule reference on it either way and a reader has nothing else to
// tell the two apart (D45).
func TestEventsOnlyReportsTheCoverageOfEvents(t *testing.T) {
	engine := watchedObjectWithEvents()

	stdout, _, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	if !strings.Contains(stdout, "Coverage (Events):") {
		t.Errorf("the header does not say which scope its coverage is about:\n%s", stdout)
	}
	if !strings.Contains(stdout, "ClusterStreamRule/all-events") {
		t.Errorf("the header reports a coverage that is not the Event scope's; the rule that "+
			"opened it is the evidence a reader checks:\n%s", stdout)
	}
	if strings.Contains(stdout, "ClusterStreamRule/all-workloads") {
		t.Errorf("the header reports the object's own scope beneath a page of Events, which is a "+
			"header describing a different subject from the rows under it:\n%s", stdout)
	}

	// And the question actually asked was the Event one. A header built from the
	// right sentence and the wrong query would read identically.
	asked := engine.scopeQueries
	if len(asked) != 1 || asked[0].Kind != eventKindName {
		t.Errorf("the command asked %d coverage question(s) and the first was about %q, want one "+
			"about %q: the answer is spent twice — the header states it and the empty case is "+
			"measured against it — so asking twice would be two round trips for one question, and "+
			"asking two different questions is how they come to disagree",
			len(asked), scopeKindOf(asked), eventKindName)
	}
}

// scopeKindOf names the first scope a command asked about, for a failure message.
func scopeKindOf(queries []query.ScopeQuery) string {
	if len(queries) == 0 {
		return "nothing"
	}
	return queries[0].Kind
}

// TestEventsOnlyDoesNotRaiseTheStateScopesNoCoverageFinding is the guard this task
// exists around. Do not delete it while tidying.
//
// The fixture is the field report's own shape and the quickstart's: a rule captures
// Events, so the Pod is named by four of them, and no rule captures Pods, so its
// own scope has never been watched. Under --with-events that is the no-coverage
// finding — correctly, because the reader asked about the object. Under
// --events-only they did not, and raising it would exit 3 over the absence of
// something they excluded, with a page of correlated Events sitting on stdout above
// the error.
func TestEventsOnlyDoesNotRaiseTheStateScopesNoCoverageFinding(t *testing.T) {
	engine := commentaryOnlyEngine([]query.ScopeInterval{
		eventsWatchedBy("", "ClusterStreamRule/all-events"),
	})

	stdout, stderr, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{})
	if err != nil {
		t.Fatalf("an events-only timeline with Events on the page failed with %v (exit %d). The "+
			"object's own scope is uncovered and that is not this question: the state rows were "+
			"excluded by the reader, so an error about them answers something nobody asked and "+
			"suppresses an answer that is right there", err, exit.CodeFor(err))
	}

	// The evidence is on the page, which is what makes the finding wrong rather than
	// merely unhelpful.
	for _, reason := range []string{"ScalingReplicaSet", "FailedCreate"} {
		if !strings.Contains(stdout, reason) {
			t.Errorf("the Event %q is not on the page, so this test is no longer about a finding "+
				"that would have suppressed one:\n%s", reason, stdout)
		}
	}

	// And nothing on stderr says the word either, since the notice paths reach the
	// same conclusion through different sentences.
	for _, absent := range []string{"nothing was ever watching", "no changes recorded"} {
		if strings.Contains(stderr, absent) {
			t.Errorf("stderr explains the absence of state rows (%q), which is the question "+
				"--events-only excluded:\n%s", absent, stderr)
		}
	}

	// The object's own scope was never consulted at all, which is the half a
	// rephrased message would have left in place: a round trip to measure something
	// the answer does not depend on.
	for _, q := range engine.scopeQueries {
		if q.Kind != eventKindName {
			t.Errorf("the object's own scope was consulted under --events-only: %+v", q)
		}
	}
}

// TestEventsOnlyDoesNotRaiseTheStateScopesNoCoverageFindingWhenStructured is the
// same guard on the streaming path.
//
// It is a second sequence and therefore a second place for the branch to be
// forgotten (see timelinestream.go), and this is the branch whose cost is an exit
// code rather than a sentence — `-o json` is what a script reads, and a script is
// exactly what exit 3 was reserved for.
func TestEventsOnlyDoesNotRaiseTheStateScopesNoCoverageFindingWhenStructured(t *testing.T) {
	engine := commentaryOnlyEngine([]query.ScopeInterval{
		eventsWatchedBy("", "ClusterStreamRule/all-events"),
	})

	request := eventsOnlyRequest()
	request.Structured = render.StructuredJSON

	stdout, _, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("the structured events-only timeline failed with %v (exit %d)", err, exit.CodeFor(err))
	}

	var envelope struct {
		Metadata struct {
			Coverage struct {
				Available bool                  `json:"available"`
				Intervals []query.ScopeInterval `json:"intervals"`
			} `json:"coverage"`
		} `json:"metadata"`
		Items []struct {
			EventType string `json:"event_type"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("the envelope does not parse: %v\n%s", err, stdout)
	}

	if len(envelope.Items) != 2 {
		t.Errorf("the envelope carries %d item(s), want the 2 Events", len(envelope.Items))
	}
	for i, item := range envelope.Items {
		if item.EventType != query.EventKubernetes {
			t.Errorf("item %d is %q, want %q", i, item.EventType, query.EventKubernetes)
		}
	}

	// metadata.coverage follows the header: a consumer comparing two runs is owed
	// the same substitution a reader is, and the intervals name the kind they are
	// about so the narrowing is legible in the data rather than only in the prose.
	for _, interval := range envelope.Metadata.Coverage.Intervals {
		if interval.Kind != eventKindName {
			t.Errorf("metadata.coverage carries an interval about %q; under --events-only the "+
				"coverage reported is the Event scope's: %+v", interval.Kind, interval)
		}
	}
	if len(envelope.Metadata.Coverage.Intervals) == 0 {
		t.Error("metadata.coverage carries no interval, so the assertion above checked nothing")
	}
}

// TestEventsOnlyExplainsAnEmptyAnswer covers the three states an empty page can be
// in, in both colour modes.
//
// They are the states explainNoEvents already distinguished for --with-events, and
// the point of routing through it is that they stay one implementation: Events
// recorded and none about this object, no rule recording Events at all, and a
// backend with no scope log to say either way. What changes is the register — under
// --events-only this is the primary message rather than a footnote to a page of
// changes — and the flag it names, which has to be the one the reader typed.
func TestEventsOnlyExplainsAnEmptyAnswer(t *testing.T) {
	tests := map[string]struct {
		golden    string
		intervals []query.ScopeInterval
		coverErr  error
	}{
		// Events were being recorded and this object drew none. The silence is real
		// and the interval is the evidence for saying so.
		"Events were watched and none happened": {
			golden: "events-only-flag-nothing-recorded",
			intervals: append(deploymentScope(),
				eventsWatchedBy("", "ClusterStreamRule/all-events")),
		},
		// The quickstart's own state: no rule streams Events, so the flag is
		// correct, the archive is correct, and the answer is empty for a reason the
		// reader can fix — in four lines they can copy.
		"no rule streams Events": {
			golden:    "events-only-flag-not-watched",
			intervals: deploymentScope(),
		},
		// No scope log at all. The two states above cannot be told apart, and the
		// notice says exactly that rather than picking one.
		"the backend cannot say": {
			golden:   "events-only-flag-cannot-say",
			coverErr: query.ErrCapabilityUnsupported,
		},
	}

	for name, test := range tests {
		for mode, color := range map[string]bool{"": false, "-color": true} {
			t.Run(name+mode, func(t *testing.T) {
				engine := withEventsEngine(test.intervals)
				engine.coverageErr = test.coverErr

				stdout, stderr, err := runTimeline(t, engine, eventsOnlyRequest(),
					render.Options{Color: color, EventsOnly: true})
				if err != nil {
					// Exit 0 with the explanation, never the no-coverage finding. The
					// reader narrowed a question; the narrowing came back empty; and
					// which kind of empty it is, is what the notice is for.
					t.Fatalf("an empty events-only timeline is a notice, not a finding: %v (exit %d)",
						err, exit.CodeFor(err))
				}
				assertGolden(t, test.golden+mode, stdout, stderr)
			})
		}
	}
}

// TestEventsOnlyNamesItselfInTheExplanation is the half the golden files cannot
// assert on their own, because they would pass just as well against the other
// flag's name.
func TestEventsOnlyNamesItselfInTheExplanation(t *testing.T) {
	engine := withEventsEngine(deploymentScope())

	_, stderr, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stderr, eventsOnlyFlagName) {
		t.Errorf("the explanation does not name the flag that was passed:\n%s", stderr)
	}
	if strings.Contains(stderr, "--with-events") {
		t.Errorf("the explanation names --with-events to a reader who typed %s, which is a notice "+
			"answering somebody else's invocation:\n%s", eventsOnlyFlagName, stderr)
	}
}

// TestEventsOnlyReportsThePredicatesItLeftInert is D31 over the flags that cannot
// act.
//
// --actor and --field narrow the object's own changes, and --all-incarnations names
// the incarnations of them; --events-only excludes all three subjects. They are
// accepted rather than refused — they contradict nothing, they simply have nothing
// to bite on — which is precisely why the page has to say so: a filter that removed
// nothing and a filter that was ignored produce the identical output.
func TestEventsOnlyReportsThePredicatesItLeftInert(t *testing.T) {
	engine := watchedObjectWithEvents()

	request := eventsOnlyRequest()
	request.Actors = []string{"kube-controller-manager"}
	request.FieldPaths = []string{"spec.replicas"}
	request.AllIncarnations = true

	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	for _, want := range []string{
		"--actor kube-controller-manager", "--field spec.replicas", "--all-incarnations",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the notice does not name %q, so a reader cannot see which of their flags did "+
				"nothing:\n%s", want, stderr)
		}
	}

	// The predicates reached neither the query nor a probe. Pushing them down would
	// be harmless on today's backends and dishonest tomorrow: they would be
	// predicates over a stream they were never about.
	if len(engine.queries) != 1 {
		t.Fatalf("the command issued %d queries, want 1: an inert predicate must not buy the "+
			"unfiltered probe predicateNotice runs: %+v", len(engine.queries), engine.queries)
	}

	// And the page is the page it would have been without them.
	for _, reason := range []string{"ScalingReplicaSet", "FailedCreate"} {
		if !strings.Contains(stdout, reason) {
			t.Errorf("the Event %q is missing, so a predicate that cannot act nonetheless did:\n%s",
				reason, stdout)
		}
	}
}

// TestEventsOnlySaysNothingAboutPredicatesNobodyPassed keeps the notice off the
// ordinary invocation.
//
// A line that appears whether or not the reader did the thing it is about is a line
// that teaches them to skip the stream — which is the failure Task 15.6 recorded
// for --full's hint and the one the conditional emission rule exists to prevent.
func TestEventsOnlySaysNothingAboutPredicatesNobodyPassed(t *testing.T) {
	engine := watchedObjectWithEvents()

	_, stderr, err := runTimeline(t, engine, eventsOnlyRequest(), render.Options{EventsOnly: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if strings.Contains(stderr, "narrows the object's own changes") {
		t.Errorf("a bare --events-only was told which of its predicates did nothing:\n%s", stderr)
	}
}

// TestTheEventsHintAppearsOnlyWhereTheFlagWouldRemoveSomething is the footer's
// conditional emission, stated as the three cases it distinguishes.
//
// The flag is discoverable without a command because it is named at the moment its
// absence is visible, which is the rule --full's footer already follows. What
// "visible" means here is two kinds of row on one page: a document of Events alone
// would lose nothing to the flag, and a document with no Events at all would be
// offered a flag that empties it.
func TestTheEventsHintAppearsOnlyWhereTheFlagWouldRemoveSomething(t *testing.T) {
	const hint = "pass --events-only"

	tests := map[string]struct {
		engine  func() *fakeEngine
		request func() cli.TimelineRequest
		opts    render.Options
		want    bool
		why     string
	}{
		"both kinds of row": {
			engine:  watchedObjectWithEvents,
			request: withEventsRequest,
			want:    true,
			why:     "the flag would remove the state rows, which is what makes its absence visible",
		},
		"no events at all": {
			engine:  func() *fakeEngine { return withEventsEngine(deploymentScope()) },
			request: withEventsRequest,
			want:    false,
			why: "a hint under a page with no Events on it offers a flag that would empty the " +
				"page, and teaches a reader to stop reading footers",
		},
		"events only": {
			engine: func() *fakeEngine {
				return commentaryOnlyEngine([]query.ScopeInterval{
					eventsWatchedBy("", "ClusterStreamRule/all-events"),
				})
			},
			request: withEventsRequest,
			want:    false,
			why: "every row is already an Event, so the flag would remove nothing from the page — " +
				"and the notice above it has just said so in those words",
		},
		"the flag is already on": {
			engine:  watchedObjectWithEvents,
			request: eventsOnlyRequest,
			opts:    render.Options{EventsOnly: true},
			want:    false,
			why:     "a line advertising a flag the reader has just used reads as the tool not noticing",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			request := test.request()
			request.From = at("2026-08-01T00:00:00Z")
			request.To = at("2026-08-28T15:00:00Z")

			_, stderr, err := runTimeline(t, test.engine(), request, test.opts)
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if got := strings.Contains(stderr, hint); got != test.want {
				t.Errorf("the footer names --events-only: %t, want %t — %s\n%s",
					got, test.want, test.why, stderr)
			}
		})
	}
}

// TestThereIsNoEventsCommand records D51 where a contributor would test for it.
//
// `kuberecord events <resource>` asks what `timeline` asks and hides rows of the
// answer, so it would be a second surface over one question — roughly eighty
// percent of `timeline`'s local flags plus the window flags, --tz, -o and the
// resolution chain, drifting the first time somebody answers "does this flag apply
// to `events` too?" wrongly. The name is kept unspent for the namespace-wide Event
// search `timeline` structurally cannot express, which would deserve it.
func TestThereIsNoEventsCommand(t *testing.T) {
	root, _ := cli.NewRootCommand(options.StandaloneName, ioStreams(io.Discard, io.Discard))

	for _, command := range root.Commands() {
		if command.Name() == "events" {
			t.Errorf("the tree carries an `events` command. Events-only is a filter on one " +
				"question and it is a flag on `timeline` (D51); the name is reserved for the " +
				"namespace-wide search that would be a differently-shaped question")
		}
		if slices.Contains(command.Aliases, "events") {
			t.Errorf("`%s` answers to the alias `events`, which is the same decision reached "+
				"sideways", command.Name())
		}
	}

	// Non-vacuity: a walk over an empty tree would pass this forever.
	if len(root.Commands()) < 5 {
		t.Fatalf("the command walk found %d commands; the tree has more, so this check is "+
			"measuring nothing", len(root.Commands()))
	}
}
