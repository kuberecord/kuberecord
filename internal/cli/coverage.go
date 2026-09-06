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
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Invariant 9, made into output.
//
// "Nothing changed" and "nothing was watching" are different facts, and an
// engineer who is handed the second dressed as the first closes an investigation
// that should have started one. Every empty timeline therefore goes through this
// file, and leaves it as one of three answers:
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

// explainEmpty turns an empty timeline into the reason for it.
//
// The error it returns is the no-coverage finding, wrapping query.ErrNoCoverage
// so that exit.CodeFor gives it exit code 3 without this call site having to know
// the number. Everything else is a notice: the command succeeded, and what it
// found was silence with an explanation attached.
func explainEmpty(
	request TimelineRequest, from, to time.Time, hasRows bool, coverage coverageAnswer,
) ([]render.Notice, error) {
	if hasRows {
		return nil, nil
	}
	object := describeObject(request.Ref)
	window := options.DescribeWindow(from, to)

	if coverage.Gap != nil {
		return []render.Notice{{
			Text: fmt.Sprintf("no changes recorded for %s in %s, and this backend has no scope log: "+
				"it cannot say whether that means nothing changed or nothing was watching",
				object, window),
		}}, nil
	}

	if len(coverage.Intervals) == 0 {
		return nil, fmt.Errorf("%w: nothing was ever watching %s %s in cluster %q, so this silence is "+
			"not evidence that it did not change; the `%s` command lists what is being recorded",
			query.ErrNoCoverage, describeKind(request.Ref), object, request.Ref.ClusterID, scopesCommand)
	}

	earliest := coverage.Intervals[0]
	if from.IsZero() || earliest.From.After(from) {
		return []render.Notice{{
			Text: fmt.Sprintf("no changes recorded for %s in %s, but %s was not being watched before "+
				"%s, when %s opened the scope: a change before then would not have been recorded",
				object, window, describeKind(request.Ref),
				render.FormatInstant(earliest.From), describeRule(earliest)),
		}}, nil
	}

	return []render.Notice{{
		Text: fmt.Sprintf("no changes recorded for %s in %s. The scope was confirmed watched over "+
			"%s, so nothing changed in that period", object, window, describeInterval(earliest)),
	}}, nil
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
// What follows is explainEmpty's shape, deliberately, and the two are meant to be
// read together: the same three states, distinguished the same way, in the same
// order, so that a change to how this CLI reasons about a silence is made once
// rather than in two places that then drift. The one difference is the
// consequence. explainEmpty can return a finding, because a timeline with no
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

// explainNoEvents says why --with-events interleaved nothing.
//
// coverage is the answer to eventScopeQuery, already narrowed by eventIntervals,
// and readErr is a scope log that exists and could not be read. The three states
// are explainEmpty's:
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
		return render.Notice{Text: fmt.Sprintf(
			"--with-events found no Events: no rule streams Events to this sink.\n"+
				"Add them to a rule and they will appear here:\n"+
				"    - group: %q\n"+
				"      version: v1\n"+
				"      kind: %s", eventGroupCore, eventKind)}
	}

	// The earliest interval, as explainEmpty names the earliest one: it is the
	// oldest evidence there is that something was recording, and describeInterval
	// prints both ends of it so a reader can see for themselves how much of their
	// window it covers.
	return render.Notice{Text: fmt.Sprintf(
		"--with-events found no Events for %s in %s. Events were confirmed recorded over %s, so "+
			"nothing was said about it while that scope was open",
		object, window, describeInterval(coverage.Intervals[0]))}
}
