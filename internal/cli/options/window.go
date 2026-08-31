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

package options

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// --since and --until, and the default window a backend forces.
//
// # One flag, two grammars
//
// kubectl splits these into --since (a duration) and --since-time (an instant),
// which is two flags for one question and a lookup every time. Here one flag
// takes either: "6h" and "2026-08-20T14:00:00Z" are both answers to "since
// when", and the parser can tell them apart without ambiguity because no
// timestamp starts with a digit followed by a duration unit.
//
// # Days and weeks
//
// Go's ParseDuration stops at hours, on the correct grounds that a day is not
// always 24 hours. For an audit window that objection does not survive contact
// with the user: an engineer asking for "the last three days" wants 72 hours and
// is not thinking about the clocks going back. Both are accepted, with the Go
// parser tried first so that "1h30m" keeps its exact meaning.

// DefaultWindow is the span applied when a backend refuses unbounded queries and
// the user named neither end.
//
// Twenty-four hours, because the backend that needs it is the object archive,
// which has no index: the window is not a filter there, it is the set of
// partitions that will be listed and decompressed. A default nobody chose should
// be small enough to answer in the time a person will wait for it and visibly a
// default, and --since is one flag away when the answer is older than a day. When
// it is applied it is announced, and an empty result names it.
//
// It is deliberately applied only where a bound is *required*. An indexed backend
// is asked the unbounded question, because the change an engineer is hunting at
// 02:47 is as likely to be six weeks old as six hours, and a default window there
// would hide it behind a flag they did not know to pass — at no saving, since the
// window is a predicate rather than the work.
const DefaultWindow = 24 * time.Hour

// ConfirmWindow is how wide a window may be, against a backend that has to scan
// for its answer, before the scan is something the user has to say yes to.
//
// A week. Below it the estimate is printed and the scan simply runs, because a
// tool that asks permission for every question trains people to stop reading the
// question. Above it the cost stops being incidental — a week of a busy cluster's
// partitions is already thousands of objects to fetch and decompress, and the
// windows that hurt are the ones typed casually ("--since 90d") rather than the
// ones chosen. So the line is drawn where a wide window stops being a rounding
// error and starts being a decision, and the decision is handed to the person
// making it, with the figures beside it.
//
// It bounds nothing on its own: --yes, and a non-interactive invocation, pass it
// without argument. What it buys is that nobody waits ten minutes for a scan they
// did not know they had asked for.
const ConfirmWindow = 7 * 24 * time.Hour

// The instant layouts accepted by --since and --until, most specific first.
//
// A bare date is read as UTC midnight rather than as local midnight, because
// every timestamp this CLI prints is UTC and a window whose edges moved with the
// operator's laptop would not line up with the rows it selected.
var instantLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// ParseInstant reads a --since or --until value into an absolute instant.
//
// A duration is subtracted from now, so both flags read as "ago": --until 1h
// ends the window an hour ago, which is what somebody narrowing in on an
// incident means by it.
//
// now is an argument rather than a call to time.Now so that a test can fix it,
// and so that the two ends of one window are computed against the same instant —
// a --since and a --until evaluated a microsecond apart would produce a window
// whose width depended on how fast the process started.
func ParseInstant(value string, now time.Time) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, exit.UsageErrorf("the time bound is empty: give a duration such as 6h or 3d, " +
			"or an instant such as 2026-08-20T14:00:00Z")
	}

	if duration, ok := parseWindowDuration(trimmed); ok {
		if duration < 0 {
			return time.Time{}, exit.UsageErrorf(
				"%q is a negative duration: these bounds are read as \"ago\", so a window ending "+
					"in the future cannot be asked for", value)
		}
		return now.Add(-duration), nil
	}

	for _, layout := range instantLayouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed.UTC(), nil
		}
	}

	return time.Time{}, exit.UsageErrorf(
		"%q is neither a duration (6h, 90m, 3d, 2w) nor an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z)", value)
}

// parseWindowDuration reads a duration in Go's grammar, extended with days and
// weeks.
//
// The boolean rather than an error is deliberate: failing to parse as a duration
// is not a failure, it is the signal to try the instant layouts, and returning an
// error here would mean the caller had to decide which of two errors to report
// for a value that was simply a date.
func parseWindowDuration(value string) (time.Duration, bool) {
	if duration, err := time.ParseDuration(value); err == nil {
		return duration, true
	}

	var (
		total     time.Duration
		rest      = value
		converted bool
	)
	for rest != "" {
		digits := 0
		for digits < len(rest) && (isDigit(rest[digits]) || rest[digits] == '.') {
			digits++
		}
		if digits == 0 {
			break
		}
		unit := digits
		for unit < len(rest) && !isDigit(rest[unit]) && rest[unit] != '.' {
			unit++
		}

		hours, ok := hoursPerUnit(rest[digits:unit])
		if !ok {
			// Not one of the units this function adds. Whatever is left is Go's
			// to parse, and handing it over unchanged is what makes "1d6h" work.
			break
		}
		count, err := strconv.ParseFloat(rest[:digits], 64)
		if err != nil {
			return 0, false
		}
		total += time.Duration(count * hours * float64(time.Hour))
		rest = rest[unit:]
		converted = true
	}

	if !converted {
		return 0, false
	}
	if rest != "" {
		remainder, err := time.ParseDuration(rest)
		if err != nil {
			return 0, false
		}
		total += remainder
	}
	return total, true
}

// hoursPerUnit reports how many hours a unit this package adds is worth.
func hoursPerUnit(unit string) (float64, bool) {
	switch unit {
	case "d":
		return 24, true
	case "w":
		return 24 * 7, true
	}
	return 0, false
}

// isDigit reports whether b is an ASCII digit. The standard library's version
// lives behind unicode and would accept Devanagari digits in a duration, which
// time.ParseDuration would then reject with a worse message.
func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// DescribeWindow renders a window for a message.
//
// An unbounded end is spelled "now" rather than left blank, because a sentence
// explaining an empty result has to name both edges to be an explanation at all.
func DescribeWindow(from, to time.Time) string {
	switch {
	case from.IsZero() && to.IsZero():
		return "all recorded history"
	case from.IsZero():
		return fmt.Sprintf("everything up to %s", render.FormatInstant(to))
	case to.IsZero():
		return fmt.Sprintf("%s to now", render.FormatInstant(from))
	}
	return fmt.Sprintf("%s to %s", render.FormatInstant(from), render.FormatInstant(to))
}
