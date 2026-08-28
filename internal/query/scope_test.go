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

package query_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// scopeEpoch is the instant these fixtures are dated from.
var scopeEpoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// at is the instant an offset falls on.
func at(d time.Duration) time.Time { return scopeEpoch.Add(d) }

// scopeKind is the kind every fixture here watches. One kind is enough: what the
// pairing distinguishes scopes by is the whole (group, kind, namespace) tuple, and the
// namespace is the half that varies below.
const scopeKind = "Deployment"

// started and stopped build the two transitions a scope log holds.
func started(namespace, rule string, offset time.Duration) query.ScopeChange {
	return query.ScopeChange{
		APIGroup: "apps", Kind: scopeKind, Namespace: namespace,
		Action: query.ScopeStarted, RuleRef: rule, TS: at(offset),
	}
}

func stopped(namespace, rule string, offset time.Duration) query.ScopeChange {
	return query.ScopeChange{
		APIGroup: "apps", Kind: scopeKind, Namespace: namespace,
		Action: query.ScopeStopped, RuleRef: rule, TS: at(offset),
	}
}

// describe renders an interval for a failure message, spelling an open one as open
// rather than as a zero timestamp — the distinction the pointer exists for.
func describe(iv query.ScopeInterval) string {
	to := "open"
	if iv.To != nil {
		to = iv.To.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("{%s ns=%q %s..%s rule=%s}",
		iv.Kind, iv.Namespace, iv.From.UTC().Format(time.RFC3339), to, iv.RuleRef)
}

func describeAll(intervals []query.ScopeInterval) string {
	if len(intervals) == 0 {
		return "(none)"
	}
	var out strings.Builder
	for i, iv := range intervals {
		fmt.Fprintf(&out, "\n  [%d] %s", i, describe(iv))
	}
	return out.String()
}

// TestCoverageOfPairsEpochs covers the pairing rule, whose interesting cases are all
// quiet ones: nothing throws, and a mistake produces a coverage answer that reads
// plausibly and is false about when the recorder was watching.
func TestCoverageOfPairsEpochs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		changes []query.ScopeChange
		want    []string
		why     string
	}{
		{
			name: "a closed epoch",
			changes: []query.ScopeChange{
				started("payments", "rule-a", 0),
				stopped("payments", "rule-a", time.Hour),
			},
			want: []string{"2026-03-01T12:00:00Z..2026-03-01T13:00:00Z"},
		},
		{
			name: "an unmatched Started stays open",
			changes: []query.ScopeChange{
				started("payments", "rule-a", 0),
			},
			want: []string{"2026-03-01T12:00:00Z..open"},
			why: "a process that exits writes no Stopped, so closing it at the last entry would " +
				"turn \"we are watching this and nothing has happened\" into \"nobody is watching\"",
		},
		{
			name: "watched, dropped, and picked up again",
			changes: []query.ScopeChange{
				started("payments", "rule-a", 0),
				stopped("payments", "rule-a", time.Hour),
				started("payments", "rule-b", 2*time.Hour),
			},
			want: []string{
				"2026-03-01T12:00:00Z..2026-03-01T13:00:00Z",
				"2026-03-01T14:00:00Z..open",
			},
			why: "the trailing Started must not be paired with the Stopped that preceded it",
		},
		{
			name: "a second Started while one is open adds nothing",
			changes: []query.ScopeChange{
				started("payments", "rule-a", 0),
				started("payments", "rule-b", time.Hour),
				stopped("payments", "rule-a", 2*time.Hour),
			},
			want: []string{"2026-03-01T12:00:00Z..2026-03-01T14:00:00Z"},
			why: "the scope was already being watched, and a fresh interval would report a " +
				"zero-length gap in coverage that never occurred",
		},
		{
			name: "a Stopped with nothing open is ignored",
			changes: []query.ScopeChange{
				stopped("payments", "rule-a", 0),
				started("payments", "rule-a", time.Hour),
			},
			want: []string{"2026-03-01T13:00:00Z..open"},
		},
		{
			name: "transitions arrive in any order",
			changes: []query.ScopeChange{
				stopped("payments", "rule-a", time.Hour),
				started("payments", "rule-a", 0),
			},
			want: []string{"2026-03-01T12:00:00Z..2026-03-01T13:00:00Z"},
			why: "a backend fanning in over several objects has no order to offer, so the pairing " +
				"orders them itself rather than inventing epochs out of the order they arrived",
		},
		{
			name: "two scopes over one kind keep independent epochs",
			changes: []query.ScopeChange{
				started("payments", "rule-a", 0),
				started("", "rule-wide", time.Hour),
				stopped("payments", "rule-a", 2*time.Hour),
				stopped("", "rule-wide", 3*time.Hour),
			},
			want: []string{
				"2026-03-01T12:00:00Z..2026-03-01T14:00:00Z",
				"2026-03-01T13:00:00Z..2026-03-01T15:00:00Z",
			},
			why: "a rule pinned to one namespace and a cluster-wide rule are two scopes, and " +
				"pairing their transitions together would invent an outage in one from a " +
				"transition in the other — or close one of them at the other's Stopped",
		},
		{
			name:    "an empty log is an empty answer",
			changes: nil,
			want:    nil,
			why:     "\"nothing was watching\" is a result rather than a failure",
		},
		{
			name: "an action nobody recognises is ignored rather than guessed at",
			changes: []query.ScopeChange{
				{APIGroup: "apps", Kind: "Deployment", Action: "Paused", TS: at(0)},
				started("payments", "rule-a", time.Hour),
			},
			want: []string{"2026-03-01T13:00:00Z..open"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := query.CoverageOf(tc.changes, time.Time{}, time.Time{})
			if len(got) != len(tc.want) {
				t.Fatalf("CoverageOf returned %d intervals, want %d. %s%s",
					len(got), len(tc.want), tc.why, describeAll(got))
			}
			for i := range got {
				span := got[i].From.UTC().Format(time.RFC3339) + ".."
				if got[i].To == nil {
					span += "open"
				} else {
					span += got[i].To.UTC().Format(time.RFC3339)
				}
				if span != tc.want[i] {
					t.Errorf("interval %d is %s, want %s. %s", i, span, tc.want[i], tc.why)
				}
			}
		})
	}
}

// TestCoverageOfReportsTheOpeningRule: an interval names the rule that opened it.
//
// A Stopped entry's rule may name a different rule, or none at all when a recovery pass
// closed a scope whose rule no longer exists, and the opener is the one a reader can
// still go and look at.
func TestCoverageOfReportsTheOpeningRule(t *testing.T) {
	t.Parallel()

	got := query.CoverageOf([]query.ScopeChange{
		started("payments", "rule-that-opened-it", 0),
		stopped("payments", "", time.Hour),
	}, time.Time{}, time.Time{})

	if len(got) != 1 || got[0].RuleRef != "rule-that-opened-it" {
		t.Errorf("CoverageOf reported %s, want the rule that opened the interval", describeAll(got))
	}
}

// TestCoverageOfKeepsOverlappingIntervalsWhole: the window selects intervals, it does
// not trim them.
//
// Trimming would make a scope opened last year and still open look as though it opened
// when the window did — a false statement about when the recorder started watching, which
// is the one thing a coverage answer exists to be right about.
func TestCoverageOfKeepsOverlappingIntervalsWhole(t *testing.T) {
	t.Parallel()

	log := []query.ScopeChange{
		// Long over before the window.
		started("payments", "old", -48*time.Hour),
		stopped("payments", "old", -47*time.Hour),
		// Spans the window entirely.
		started("payments", "spanning", -24*time.Hour),
	}

	from, to := at(0), at(time.Hour)
	got := query.CoverageOf(log, from, to)
	if len(got) != 1 {
		t.Fatalf("CoverageOf returned %d intervals, want only the one overlapping the window%s",
			len(got), describeAll(got))
	}
	if !got[0].From.Equal(at(-24 * time.Hour)) {
		t.Errorf("the overlapping interval opens at %s, want the instant the scope really opened "+
			"(%s) rather than the start of the window", got[0].From.UTC().Format(time.RFC3339),
			at(-24*time.Hour).UTC().Format(time.RFC3339))
	}

	// An interval that opens after the window is excluded, and one that closed before it
	// starts is too — those are the two edges the selection is about.
	future := query.CoverageOf([]query.ScopeChange{
		started("payments", "later", 48*time.Hour),
	}, from, to)
	if len(future) != 0 {
		t.Errorf("an interval opening after the window was returned%s", describeAll(future))
	}
}

// TestCoverageOfDoesNotMutateItsInput: a backend may hand over the slice it read into.
//
// The pairing sorts to make the walk meaningful, and sorting the caller's slice in place
// would reorder a scope log somebody else still holds — the kind of action-at-a-distance
// that surfaces as a second, differently-wrong answer from an unrelated call.
func TestCoverageOfDoesNotMutateItsInput(t *testing.T) {
	t.Parallel()

	log := []query.ScopeChange{
		stopped("payments", "rule-a", time.Hour),
		started("payments", "rule-a", 0),
	}
	query.CoverageOf(log, time.Time{}, time.Time{})

	if log[0].Action != query.ScopeStopped || !log[0].TS.Equal(at(time.Hour)) {
		t.Errorf("CoverageOf reordered the log it was given: %v", log)
	}
}
