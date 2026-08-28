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

package query

import (
	"slices"
	"strings"
	"time"
)

// The actions a scope log records, spelled as the recorded value spells them.
//
// They live in the contract because every backend's scope log stores the same two
// words and [CoverageOf] branches on them: a literal repeated per backend is a
// typo waiting to become an interval that never closes.
const (
	// ScopeStarted opens a scope: the recorder began watching it.
	ScopeStarted = "Started"
	// ScopeStopped closes one. It says the recorder stopped watching, and
	// emphatically not that the objects in the scope were deleted.
	ScopeStopped = "Stopped"
)

// ScopeChange is one entry of a scope log, as a backend reads it back.
//
// It is the input [CoverageOf] consumes and nothing more. A backend materializes
// its own scope log into this — rows, or lines, or whatever it stores — so that
// the pairing rule below has one definition rather than one per storage layout.
type ScopeChange struct {
	// APIGroup is the group watched; empty is the core group, as a value rather
	// than a wildcard.
	APIGroup string
	// Kind is the kind watched.
	Kind string
	// Namespace is the namespace watched, with the scope log's own reading:
	// empty is the all-namespaces scope itself.
	Namespace string
	// Action is ScopeStarted or ScopeStopped. Anything else is ignored rather
	// than guessed at — the enum is open for the same reason the event types are.
	Action string
	// RuleRef names the rule that opened or closed the scope.
	RuleRef string
	// TS is when the transition happened.
	TS time.Time
}

// CoverageOf turns a scope log into the intervals [QueryEngine.Coverage] answers
// with: grouped by scope, paired oldest first, and kept when they overlap the
// window.
//
// This is the single definition of the pairing rule, and it is in the contract
// rather than in a backend for the same reason [Replay] is: the interesting cases
// are quiet ones. An unmatched trailing Started must stay open, a second Started
// while one is open must add nothing, and an interval that merely overlaps the
// window must be returned *whole* rather than clipped. A backend that got any of
// the three wrong would return a coherent-looking coverage answer that is false
// about when the recorder was watching — which is the one thing a coverage answer
// exists to be right about.
//
// changes may arrive in any order and may describe any number of scopes: they are
// grouped and ordered here. Ordering is not left to the caller because a backend
// whose storage returns them unordered (as any store that fans in over several
// objects does) would otherwise have to re-implement the sort that makes pairing
// meaningful, and a pairing walked out of order invents epochs.
func CoverageOf(changes []ScopeChange, from, to time.Time) []ScopeInterval {
	ordered := slices.Clone(changes)
	slices.SortStableFunc(ordered, func(a, b ScopeChange) int {
		if c := strings.Compare(a.APIGroup, b.APIGroup); c != 0 {
			return c
		}
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		return a.TS.Compare(b.TS)
	})

	var intervals []ScopeInterval
	for start := 0; start < len(ordered); {
		end := start
		for end < len(ordered) && sameScope(ordered[end], ordered[start]) {
			end++
		}
		for _, interval := range pairScope(ordered[start:end]) {
			if overlapsWindow(interval, from, to) {
				intervals = append(intervals, interval)
			}
		}
		start = end
	}

	slices.SortStableFunc(intervals, func(a, b ScopeInterval) int { return a.From.Compare(b.From) })
	return intervals
}

// sameScope reports whether two transitions belong to one scope.
//
// Namespace is part of the identity, with the scope log's own reading rather than
// a ScopeQuery's: a rule pinned to one namespace and a cluster-wide rule over the
// same kind are two scopes with two independent epochs, and pairing their
// transitions together would invent an outage in one from a transition in the
// other.
func sameScope(a, b ScopeChange) bool {
	return a.APIGroup == b.APIGroup && a.Kind == b.Kind && a.Namespace == b.Namespace
}

// pairScope walks one scope's transitions in order, opening on Started and
// closing on Stopped.
//
// An unmatched trailing Started is left open, and that is the load-bearing case:
// a process that exits writes no Stopped entry, so a scope being watched right now
// and a scope left open by a process that never came back look the same from here,
// and both of them are genuinely still open in the log. Closing one at the last
// entry would turn "we are watching this and nothing has happened" into "nobody is
// watching this" — the opposite conclusion.
//
// A second Started while one is open adds nothing. The scope was already being
// watched, and starting a fresh interval would report a zero-length gap in
// coverage that never occurred.
func pairScope(transitions []ScopeChange) []ScopeInterval {
	var intervals []ScopeInterval
	open := -1
	for _, t := range transitions {
		switch t.Action {
		case ScopeStarted:
			if open >= 0 {
				continue
			}
			intervals = append(intervals, ScopeInterval{
				APIGroup:  t.APIGroup,
				Kind:      t.Kind,
				Namespace: t.Namespace,
				// The rule that *opened* the interval. A Stopped entry's rule may name
				// a different rule, or none at all when a recovery pass closed a scope
				// whose rule no longer exists, and the opener is the one a reader can
				// still go and look at.
				RuleRef: t.RuleRef,
				From:    t.TS,
			})
			open = len(intervals) - 1
		case ScopeStopped:
			if open < 0 {
				continue
			}
			stop := t.TS
			intervals[open].To = &stop
			open = -1
		}
	}
	return intervals
}

// overlapsWindow reports whether an interval intersects the query's window.
//
// An interval that merely overlaps is returned whole rather than clipped to the
// question. Trimming it would make a scope opened last year and still open look as
// though it opened when the window did, which is a false statement about when the
// recorder started watching.
func overlapsWindow(interval ScopeInterval, from, to time.Time) bool {
	if !to.IsZero() && interval.From.After(to) {
		return false
	}
	if !from.IsZero() && interval.To != nil && interval.To.Before(from) {
		return false
	}
	return true
}
