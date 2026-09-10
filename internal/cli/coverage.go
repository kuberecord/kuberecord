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

package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Invariant 9, made into output.
//
// "Nothing changed" and "nothing was watching" are different facts, and an
// engineer who is handed the second dressed as the first closes an investigation
// that should have started one. Every timeline holding none of the object's own
// changes therefore goes through this file, and leaves it as one of three
// answers:
//
//   - Nothing was ever watching this scope. That is not an empty result at all;
//     it is a finding, and it exits 3 so a script can tell it from one.
//   - Something was watching, but only from an instant later than the window
//     asked about. The silence before that instant is unexplained, and the notice
//     names the rule that opened the scope so the reader can go and look at it.
//   - Something was watching across the whole window. Then the silence is real:
//     the object was watched and did not change, and the confirmed interval is
//     printed as the evidence for it.
//
// The header carries a coverage summary on every invocation, not only an empty
// one, because a timeline whose rows stop at a scope's edge is as misleading as
// an empty one and the reader has to be able to see the edge.

// coverageUnavailable is the coverage summary for a backend with no scope log.
//
// It is a phrase and not a blank, because the header field exists to answer "was
// anything watching" and leaving it empty answers "no" — which is a different and
// unfounded claim.
const coverageUnavailable = "not reported by this backend"

// coverageSummary renders the header's coverage line.
func coverageSummary(intervals []query.ScopeInterval, err error) string {
	if err != nil {
		return coverageUnavailable
	}
	switch len(intervals) {
	case 0:
		return "none recorded for this scope"
	case 1:
		return describeInterval(intervals[0])
	}
	// query.CoverageOf returns them oldest first, so the span is the first
	// interval's start to the last end — and any still-open interval makes the
	// whole span open, because the recorder is watching now.
	return fmt.Sprintf("%s %s %s, %d intervals",
		render.FormatInstant(intervals[0].From), render.Arrow, describeSpanEnd(intervals), len(intervals))
}

// describeInterval renders one watched period and the rule that opened it.
func describeInterval(interval query.ScopeInterval) string {
	end := "open"
	if interval.To != nil {
		end = render.FormatInstant(*interval.To)
	}
	return fmt.Sprintf("%s %s %s (%s)",
		render.FormatInstant(interval.From), render.Arrow, end, describeRule(interval))
}

// describeSpanEnd is where several intervals collectively stop.
func describeSpanEnd(intervals []query.ScopeInterval) string {
	var latest time.Time
	for _, interval := range intervals {
		if interval.To == nil {
			return "open"
		}
		if interval.To.After(latest) {
			latest = *interval.To
		}
	}
	return render.FormatInstant(latest)
}

// describeRule names the rule that opened a scope, or says that it is not
// recorded.
//
// An interval closed by a recovery pass whose rule no longer exists carries no
// rule reference, and that is a real state rather than a gap to be papered over:
// reporting it blank inside the parentheses would read as a rule named by the
// empty string.
func describeRule(interval query.ScopeInterval) string {
	if interval.RuleRef == "" {
		return "rule not recorded"
	}
	return interval.RuleRef
}

// explainNoChanges turns a timeline holding none of the object's own changes
// into the reason for it.
//
// It was explainEmpty until Task 17.3, and the rename is the finding. Read-time
// correlation extracts an Event's involvedObject from the Event row itself and
// matches it against the address on the command line, never consulting the
// subject's own rows — so a rule capturing v1/Event and not the subject's kind
// gives working Events and no state at all. That document is not empty and was
// therefore never explained, though it is a timeline in which the object appears
// never to have changed. It did; nobody was watching it.
//
// So "empty" is read as empty of the object's history rather than empty of rows,
// and one switch over the scope log serves both readings. Only the wording differs
// — through describeNoChanges, and through uncoveredNoChanges where the
// consequence differs too. A reader with Event rows in front of them is not
// looking at a blank page and must be told what the rows they can see are; a
// reader with nothing in front of them must be told the window was not silent by
// accident. The reasoning above the wording is shared on purpose: a second switch
// is a second place for the two readings to come to disagree about the same scope
// log (Invariant 9).
//
// The error it returns is the no-coverage finding, wrapping query.ErrNoCoverage
// so that exit.CodeFor gives it exit code 3 without this call site having to know
// the number. Everything else is a notice: the command succeeded, and what it
// found was silence with an explanation attached.
//
// events is the Event scope's coverage, unasked. It is a function rather than an
// answer because exactly one branch below spends it — the finding, under
// --with-events — and a caller that resolved it eagerly would buy a round trip
// for every empty timeline in order to serve one of them. Deferring it also keeps
// the gate in a single place: the condition under which the question is worth
// asking is the condition under which the clause is printed, and those are the
// same three lines. See uncoveredNoChanges.
func explainNoChanges(
	request TimelineRequest, from, to time.Time, shape timelineShape, coverage coverageAnswer,
	events eventCoverageFunc,
) ([]render.Notice, error) {
	if shape.changes {
		return nil, nil
	}
	object := describeObject(request.Ref)
	window := options.DescribeWindow(from, to)
	lead := describeNoChanges(shape, object, window)

	if coverage.Gap != nil {
		return []render.Notice{{
			Text: fmt.Sprintf("%s, and this backend has no scope log: "+
				"it cannot say whether that means nothing changed or nothing was watching", lead),
		}}, nil
	}

	if len(coverage.Intervals) == 0 {
		return uncoveredNoChanges(request, shape, object, events)
	}

	earliest := coverage.Intervals[0]
	if from.IsZero() || earliest.From.After(from) {
		return []render.Notice{{
			Text: fmt.Sprintf("%s, but %s was not being watched before "+
				"%s, when %s opened the scope: a change before then would not have been recorded",
				lead, describeKind(request.Ref),
				render.FormatInstant(earliest.From), describeRule(earliest)),
		}}, nil
	}

	return []render.Notice{{
		Text: fmt.Sprintf("%s. The scope was confirmed watched over "+
			"%s, so nothing changed in that period", lead, describeInterval(earliest)),
	}}, nil
}

// describeNoChanges opens the sentence with what the reader is actually looking
// at.
//
// The two leads carry the same fact and answer different first questions. A blank
// page prompts "did anything happen?", and the plain lead answers it. A page of
// Kubernetes Events prompts nothing at all — it looks like an answer — so the
// Events-only lead has to say that every row on it is an Event before the clause
// about coverage can mean anything, or the reader reads a statement about the
// object's history as a statement about the rows in front of them.
//
// It is one of only two places the readings differ — uncoveredNoChanges is the
// other, and differs in consequence rather than in words. Every tail in
// explainNoChanges is a single string both leads are pasted onto, which is what
// keeps a change to how this CLI reads a scope log from having to be made twice.
func describeNoChanges(shape timelineShape, object, window string) string {
	if shape.events {
		return fmt.Sprintf("every row here is a Kubernetes Event: no change to %s is recorded in %s",
			object, window)
	}
	return fmt.Sprintf("no changes recorded for %s in %s", object, window)
}

// uncoveredNoChanges is the one state whose consequence differs, not merely its
// wording.
//
// With no rows at all, nothing was ever watching the scope and the command has
// produced no evidence of anything: that is the finding Invariant 9 reserves exit
// 3 for, and a script is entitled to tell it from an answer.
//
// With Event rows, the command has produced evidence — real, correlated, worth
// reading — and the sentence about it has to be true of what is on the page.
// query.ErrNoCoverage's own words are not: "this silence is not evidence that it
// did not change" describes a silence that is not there. So the Events-only case
// is a notice at exit 0, which is also what this path already returned before it
// said anything at all, and no consumer's exit-code handling changes under a
// release whose subject is explaining things better.
//
// Both spellings name `scopes` for the same reason: the route out of "nothing was
// watching this kind" is to go and look at what is, and it is the next thing to
// type rather than the next thing to read (D34).
//
// # Why the finding absorbs --with-events
//
// The no-rows branch is the one path where this file answers a question the reader
// asked and eventsNotice's question at once, and until Task 18.5 it answered them
// separately: a Pod nobody had ever watched produced this finding and then a
// second, longer notice explaining that no rule streams Events, complete with the
// three lines of YAML that would add one. Both were true. Neither was useful in
// that order — a rule capturing Events would still record nothing about an object
// no rule covers, so the fix the second one printed was the fix for a problem the
// reader does not have yet.
//
// So the clause is here, in the finding, and the notice is withheld at the call
// site (gatherChanges). It appears only under --with-events, because nobody else
// asked about Events and a sentence answering an unasked question is the noise
// this is removing.
//
// # Why the clause asks a question of its own (Task 18.7)
//
// It shipped stating a shared cause it had never checked. The signature was
// (request, shape, object): no coverage of any kind reached it, so "no Event
// about it was recorded either, which is the same absence" was asserted on the
// strength of the flag having been passed. It was true in the case field testing
// happened to produce and false in the one it did not — a rule that genuinely
// does not capture Events makes these two gaps with two fixes, and telling that
// reader they have one sends them to correlation instead of to their rule.
//
// So the clause consults the Event scope, through the same eventScopeQuery and
// askCoverage the standalone notice uses (see eventCoverage): two formulations of
// one question is how the two answers come to differ. The three states are
// explainNoChanges's own, in its order and for its reason — one absence with one
// fix, two absences with two, and an inability that is reported rather than
// resolved into a guess.
//
// # Ordering with Task 18.6
//
// Neither task substitutes for the other, and both are needed for this sentence
// to mean anything. 18.6 made Events *reachable*: both backends resolved an
// incarnation first and returned an empty iterator when the object had no rows of
// its own, so for exactly the objects that reach this branch the Events query was
// never issued. 18.7 makes the sentence about them *true*. With 18.6 alone this
// clause still misreports an Event scope nothing was watching; with 18.7 alone it
// reports accurately about a query that never ran.
func uncoveredNoChanges(
	request TimelineRequest, shape timelineShape, object string, events eventCoverageFunc,
) ([]render.Notice, error) {
	kind := describeKind(request.Ref)
	if shape.events {
		// The rows are the answer to the Event question and no coverage read can
		// improve on them: Events about this object are demonstrably recorded,
		// because they are on the page. Asking anyway would buy a round trip to
		// confirm what the reader is looking at.
		return []render.Notice{{
			Text: fmt.Sprintf("every row here is a Kubernetes Event: nothing was ever watching %s %s "+
				"in cluster %q, so its own changes were never recorded. The Events are here because a "+
				"rule captures Events, not because this object is watched; the `%s` command lists what "+
				"is being recorded", kind, object, request.Ref.ClusterID, scopesCommand),
		}}, nil
	}
	clause, fix := eventsClause(request, events)
	return nil, fmt.Errorf("%w: nothing was ever watching %s %s in cluster %q, so this silence is "+
		"not evidence that it did not change%s; the `%s` command lists what is being recorded%s",
		query.ErrNoCoverage, kind, object, request.Ref.ClusterID, clause, scopesCommand, fix)
}

// eventCoverageFunc asks the Event scope's coverage question, once and only if
// it is asked at all.
//
// The one implementation is eventCoverage, which lives beside the notice that
// shares it; this names the shape so that the reasoning in coverage.go stays a
// function of answers rather than of a backend and a context. It returns
// askCoverage's own pair: a coverageAnswer whose Gap is a backend with no scope
// log, and an error that is a scope log which exists and could not be read.
type eventCoverageFunc func() (coverageAnswer, error)

// eventsClause is the --with-events half of the finding: what is known about the
// Event scope, and the fix when there is one.
//
// Two strings rather than one because the finding's route out — the `scopes`
// command — ends its sentence, and one of the states has something to print after
// it. The clause is spliced before that route and the fix after it, so every state
// shares a single spelling of the sentence it qualifies and a single spelling of
// the route, which is what keeps a change to either from having to be made once
// per state.
//
// Four cases for explainNoChanges's three states: the inability is spelled twice,
// once for a backend that has no scope log and once for a scope log that could
// not be read, because Invariant 4 asks the second to name what failed and the
// first has nothing to name. Neither resolves into a guess, which is the whole of
// what makes it one state.
//
// A failed coverage read is degraded into words rather than returned, exactly as
// explainNoEvents degrades it: the finding is already this invocation's failure
// and carries the exit code a script reads, and replacing it with the sub-query's
// failure would report the wrong one of the two. The cause is named rather than
// swallowed (Invariant 4).
func eventsClause(request TimelineRequest, events eventCoverageFunc) (clause, fix string) {
	if !request.WithEvents {
		// Nobody asked. See the paragraph above the function this serves.
		return "", ""
	}

	coverage, err := events()
	switch {
	case err != nil:
		// askCoverage's own words name the scope it could not read, so the cause is
		// pasted on bare rather than introduced a second time.
		return " — and whether any Kubernetes Event about it was recorded cannot be said: " +
			err.Error(), ""
	case coverage.Gap != nil:
		return " — and whether any Kubernetes Event about it was recorded cannot be said: this " +
			"backend has no scope log", ""
	case len(coverage.Intervals) == 0:
		// Two gaps, named as two. Fixing either leaves the other, so the fix for the
		// second is printed here rather than left to a notice that is not reached —
		// and it is printed after the route, because the route is the next thing to
		// type and a block of YAML is the next thing to read (D34).
		return " — and no rule streams Events to this sink either, so these are two gaps rather " +
				"than one: capturing this object's kind would record its changes, and Events would " +
				"still be missing until a rule names them as well",
			".\nAdd Events to a rule as well:\n" + eventRuleFragment()
	}

	// The claim the whole task is about, now with the evidence for it beside it.
	// The earliest interval, as every other confirmed-coverage state in this file
	// names the earliest one: it is the oldest evidence there is that something was
	// recording, and describeInterval prints both of its ends so the reader can
	// judge how much of their window it covers rather than taking the sentence's
	// word for it.
	return " — and no Kubernetes Event about it was recorded either, which is the same absence " +
		"rather than a second one to fix: Events were confirmed recorded over " +
		describeInterval(coverage.Intervals[0]), ""
}

// Invariant 9 applied to a sub-query.
//
// `--with-events` asks a second question inside the first one, and until Task
// 16.1 it was the only question this CLI answered with nothing at all: an archive
// holding no Event rows produced output byte-identical to a bare invocation, so
// the flag named in the README's hero block and in `--help`'s examples was
// indistinguishable from a flag that had been ignored. Nothing was broken — the
// quickstart's rule watches Deployments and ConfigMaps and no rule streams Events
// — and "nothing was broken" is precisely the state a reader cannot tell from the
// output.
//
// What follows is explainNoChanges's shape, deliberately, and the two are meant to be
// read together: the same three states, distinguished the same way, in the same
// order, so that a change to how this CLI reasons about a silence is made once
// rather than in two places that then drift. The one difference is the
// consequence. explainNoChanges can return a finding, because a timeline with no
// coverage behind it is not an answer; this returns only notices, because the
// object's own history may be complete and interesting and it is the commentary
// beside it that is missing.

// The kinds a Kubernetes Event is recorded under.
//
// Both spellings, because v1/Event and events.k8s.io/v1/Event are one storage
// behind two APIs and a cluster's rules may name either — the operator records
// each stream under whichever api_group its rule asked for, so a scope log can
// hold either or both. Consulting one of them would report "Events were not
// watched" to somebody whose rule names the other, which is the false conclusion
// this whole file exists to prevent.
//
// They are spelled here rather than imported. internal/pipeline states the same
// pair for the write path (ephemeralKind) and internal/query/clickhouse states it
// for the read path (mergeEvents), and D20 puts the first of those out of the
// CLI's reach on purpose: the CLI is a client of the frozen schema, not of the
// operator's runtime. A copy that names its sources is the shape that decision
// asks for.
const (
	eventKind        = "Event"
	eventGroupCore   = ""
	eventGroupModern = "events.k8s.io"
)

// eventScopeQuery asks whether Events were being recorded where the object was.
//
// The kind is pinned and the group deliberately is not. ScopeQuery.APIGroup reads
// an empty value as *every group* rather than as the core group — the core group
// is itself the empty string and cannot be spelled — so this is the only query
// that reaches both Event spellings at once, and eventIntervals narrows the
// answer back to them. Asking twice would not have been two questions: a query
// for every group already contains the one for events.k8s.io.
//
// The namespace carries ScopeQuery's covering reading, exactly as the object's
// own scope query does: a cluster-wide rule streaming Events genuinely was
// recording the ones about this object.
func eventScopeQuery(request TimelineRequest, from, to time.Time) query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: request.Ref.ClusterID,
		Kind:      eventKind,
		Namespace: request.Ref.Namespace,
		From:      from,
		To:        to,
	}
}

// eventIntervals keeps the intervals that are about Kubernetes Events.
//
// The query above had to ask about every group to reach the core one, so the
// answer may carry a kind named Event that is not a Kubernetes Event — a custom
// resource in somebody's own group. Counting it would report that Events were
// being recorded when they were not, which is the more expensive of the two
// mistakes available here: a reader told "Events were watched, none happened"
// stops looking, while one told "Events were not watched" goes and reads the rule.
func eventIntervals(intervals []query.ScopeInterval) []query.ScopeInterval {
	kept := make([]query.ScopeInterval, 0, len(intervals))
	for _, interval := range intervals {
		if interval.APIGroup == eventGroupCore || interval.APIGroup == eventGroupModern {
			kept = append(kept, interval)
		}
	}
	return kept
}

// eventRuleFragment is the three lines of YAML that add Events to a rule.
//
// One spelling, two readers. explainNoEvents prints it to somebody whose object
// is watched and whose Events are not; eventsClause prints it to somebody with
// neither, where it is the second of two fixes. The gap is the same gap and the
// remedy is the same remedy, and two copies of it is how one of them comes to
// name a group, a version or a kind the other does not.
//
// The core spelling is given because it is the shorter of the two the operator
// records under, and a rule naming either gets the same stream (see eventKind).
func eventRuleFragment() string {
	return fmt.Sprintf("    - group: %q\n      version: v1\n      kind: %s",
		eventGroupCore, eventKind)
}

// explainNoEvents says why --with-events interleaved nothing.
//
// It is not reached at all when the object's own scope had no coverage: that
// finding absorbs this one, and gatherChanges is where the gate lives. What is
// left here is a document whose subject *was* being watched, so every state below
// is a statement about Events alone.
//
// coverage is the answer to eventScopeQuery, already narrowed by eventIntervals,
// and readErr is a scope log that exists and could not be read. The three states
// are explainNoChanges's:
//
//   - The backend cannot say, because it has no scope log — or, here, because
//     reading it failed. Both are reported as the inability they are rather than
//     resolved into a guess.
//   - Nothing was watching Events. That is the quickstart's own state, and it is
//     the one with a fix, so the fix is printed rather than described.
//   - Events were being recorded. Then the silence is real, and the interval that
//     confirms it is the evidence for the claim.
//
// The third case's claim is scoped to the confirmed interval rather than to the
// window. A rule that started streaming Events halfway through the window covers
// half of it, and "nothing was said about this object" would be a statement about
// the other half that the scope log does not support.
func explainNoEvents(
	request TimelineRequest, from, to time.Time, coverage coverageAnswer, readErr error,
) render.Notice {
	object := describeObject(request.Ref)
	window := options.DescribeWindow(from, to)

	switch {
	case readErr != nil:
		// A scope log this backend has and could not read. It is the same
		// inability as the case below and is reported as one, with the failure
		// named: a notice that dropped it would be the silent error Invariant 4
		// forbids, and a command that failed over it would throw away a timeline
		// that had already been gathered — in the streaming case, already written.
		return render.Notice{Text: fmt.Sprintf(
			"--with-events found no Events for %s in %s, and the watch scopes could not be read to "+
				"say why: %v", object, window, readErr)}
	case coverage.Gap != nil:
		return render.Notice{Text: fmt.Sprintf(
			"--with-events found no Events for %s in %s, and this backend has no scope log: it cannot "+
				"say whether that means nothing was recorded about it or that no rule streams Events "+
				"to this sink", object, window)}
	case len(coverage.Intervals) == 0:
		// The fix is three lines of YAML and every other route to it — read the
		// rule, find the field, learn that `group` is the empty string for a core
		// kind — is longer than printing it. Both Event spellings work here and
		// the core one is given, because it is the shorter of the two and a rule
		// naming either gets the same stream.
		return render.Notice{Text: "--with-events found no Events: no rule streams Events to this " +
			"sink.\nAdd them to a rule and they will appear here:\n" + eventRuleFragment()}
	}

	// The earliest interval, as explainNoChanges names the earliest one: it is the
	// oldest evidence there is that something was recording, and describeInterval
	// prints both ends of it so a reader can see for themselves how much of their
	// window it covers.
	return render.Notice{Text: fmt.Sprintf(
		"--with-events found no Events for %s in %s. Events were confirmed recorded over %s, so "+
			"nothing was said about it while that scope was open",
		object, window, describeInterval(coverage.Intervals[0]))}
}

// Invariant 9 applied to a predicate, and D31's fourth instance.
//
// `timeline`'s --actor, --exclude-actor and --field are pushed into the *query*.
// That is the right place for them — a backend that can filter should — and it is
// why the emptiness they produce is invisible from here: the rows never arrive,
// so a filtered timeline that matched nothing is byte-identical to a window in
// which nothing happened. explainNoChanges was then handed that emptiness and did
// what it exists to do, which in this one case is to state something false:
//
//	no changes recorded for payments/checkout in the last 24 hours. The scope was
//	confirmed watched over 2026-07-02T09:14:00Z → open, so nothing changed in that
//	period
//
// A hundred changes had been recorded and a predicate removed all of them. Worse
// than the sentence is the exit code: a scope log with no interval for the scope
// turns the same path into query.ErrNoCoverage, so a filter matching nothing
// could report "nothing was ever watching" and exit 3 — the one code this release
// tells people to script against.
//
// `diff` never reached that state because its --field narrows the *rendering*
// (TimelineRequest.DisplayFieldPaths), which leaves displayFilterNotice holding
// both counts. This is the same finding for the predicates that are gone before
// anything can be counted, and the answer is to go and ask: one query, the same
// window and the same incarnation, with the predicates taken out.
//
// The shape below is explainNoEvents's and explainNoChanges's, deliberately, because
// all three are one piece of reasoning about a silence and a change to how this
// CLI thinks about silences should be made once. The three states are theirs too.

// explainNoMatches says why a predicate matched nothing, and whether the
// emptiness has been accounted for.
//
// hadChanges is what the unfiltered probe found and probeErr is its failure. The
// second return value says the emptiness now has an explanation better than
// coverage can give, and is what suppresses explainNoChanges at the call site — the
// same gate displayFilterNotice's counts already open for `diff --field`.
//
// It is true for a failed probe as well as for a successful one, and that is the
// deliberate half. A probe that could not run leaves "the filter did it" and
// "the window is empty" equally possible, and of the two available mistakes —
// saying nothing, or asserting the one this file exists to prevent — only the
// second is unrecoverable for the reader. So the inability is reported as an
// inability and coverage is not invited to answer a question it was not asked.
//
// A probe that found nothing is the case that returns false: the window really
// is empty of changes, the predicate is not what emptied it, and explainNoChanges's
// three answers are the right ones. That is also what keeps the no-coverage
// finding — and its exit 3 — reachable under a filter, since a window nobody was
// watching holds no changes to find.
func explainNoMatches(
	request TimelineRequest, from, to time.Time, hadChanges bool, probeErr error,
) (render.Notice, bool) {
	object := describeObject(request.Ref)
	window := options.DescribeWindow(from, to)
	predicates := describePredicates(request)

	switch {
	case probeErr != nil:
		return render.Notice{Text: fmt.Sprintf(
			"%s matched nothing for %s in %s, and the same window could not be re-read without it "+
				"to say whether there was anything to match: %v",
			predicates, object, window, probeErr)}, true
	case hadChanges:
		return render.Notice{Text: fmt.Sprintf(
			"changes are recorded for %s in %s and %s matched none of them; the window itself is "+
				"not empty, so this is the filter's answer rather than the object's",
			object, window, predicates)}, true
	}
	return render.Notice{}, false
}

// describePredicates names the flags that were in force, with their values.
//
// The flags are `timeline`'s, and only `timeline` can reach this: `diff` and
// `blame` narrow their rendering rather than their query, so TimelineRequest.filtered
// is false for both and the notice cannot be printed under a command that would
// reject the flags it names. That is a property worth keeping rather than a
// coincidence, which is why the affordance sweep asserts it.
//
// The values are printed because "--actor matched nothing" is not actionable and
// "--actor kube-controller-manager matched nothing" is: the most common cause is
// a field manager spelled the way a person remembers it rather than the way the
// API server records it, and seeing the string back is what makes that visible.
func describePredicates(request TimelineRequest) string {
	var parts []string
	if len(request.Actors) > 0 {
		parts = append(parts, "--actor "+strings.Join(request.Actors, ", "))
	}
	if len(request.ExcludeActors) > 0 {
		parts = append(parts, "--exclude-actor "+strings.Join(request.ExcludeActors, ", "))
	}
	if len(request.FieldPaths) > 0 {
		parts = append(parts, "--field "+strings.Join(request.FieldPaths, ", "))
	}
	return joinClauses(parts)
}

// joinClauses reads a list back as a sentence rather than as a list.
//
// "--actor a, --field b" reads as two items of one flag's value where
// "--actor a and --field b" reads as two flags, which is the distinction the
// notice depends on being obvious.
func joinClauses(parts []string) string {
	switch len(parts) {
	case 0:
		// Unreachable: the caller gates on TimelineRequest.filtered, which is true
		// only when one of the three is non-empty. Stated rather than assumed away,
		// because the alternative is a notice with a hole where its subject was.
		return "the filter in force"
	case 1:
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}
