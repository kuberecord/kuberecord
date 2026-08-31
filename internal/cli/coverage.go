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
			Warning: true,
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
			Warning: true,
		}}, nil
	}

	return []render.Notice{{
		Text: fmt.Sprintf("no changes recorded for %s in %s. The scope was confirmed watched over "+
			"%s, so nothing changed in that period", object, window, describeInterval(earliest)),
	}}, nil
}
