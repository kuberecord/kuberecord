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

package render

import "time"

// The frame a rendered instant is in, and why it travels as a value.
//
// # The defect this closes
//
// A timeline row read `2026-09-10 22:41:13.263` and was reported as a wrong local
// time. It was not wrong: it was UTC, the column header said so, and the schema
// column is DateTime64(9, 'UTC'). What was wrong is that the marker had moved
// from the value to the header. A header scrolls off a nine-row table and it does
// not travel when a timestamp is pasted into a ticket — where a bare instant that
// *looks* local and is not is worse than one that is (D45).
//
// So every layout below ends in a frame. The narrow one gained a `Z`; the wide
// and header ones always had it.
//
// # Why it is a value and not a package-level setting
//
// This package renders as a pure function of what it is handed: no context, no
// clock, no terminal (see the package doc). A process-wide zone would make the
// golden files depend on the environment the test ran in, which is the one thing
// they exist not to do. Zone therefore rides on Options like Width and Color, and
// is an explicit parameter everywhere prose is built outside this package.
//
// # The zero value is UTC, deliberately
//
// UTC is the default (D46) and a Zone nobody set is a Zone that renders it. That
// is what lets an Options built for a test — or for a surface with no instants in
// it — stay correct without knowing this type exists, and it is why the field can
// be added to Options without touching a single existing construction of one.

// Zone is the time zone one invocation renders its human-facing instants in.
//
// Structured output does not carry one. `ts` is a machine contract whose meaning
// may not depend on the shell that produced it (D19, D46), so the envelope path
// formats through UTC below and is never handed the invocation's zone at all —
// which is a property of the call graph rather than of a conditional, and so
// cannot be lost to a refactor that forgets the conditional.
type Zone struct {
	// loc is the location instants are converted into. Nil is UTC, so that the
	// zero value of Zone is the default rather than a panic waiting to happen.
	loc *time.Location

	// label is what a column heading calls this frame: an IANA name, or "local"
	// where the platform cannot name the local zone. Empty for UTC.
	//
	// A name rather than an offset, and that is the decision. Europe/Warsaw is
	// +02:00 in September and +01:00 in January, so a heading carrying a fixed
	// offset would contradict half the rows of a table spanning the change —
	// which is this task's own defect, reintroduced one layer along. The name
	// cannot disagree with any row, and every value carries its exact offset.
	label string
}

// UTC is the frame everything defaults to, and the one structured output is
// always in.
//
// It is a var of the zero value rather than a constant because Zone holds a
// pointer; nothing mutates it, and every method has a value receiver.
var UTC = Zone{}

// NewZone returns the frame for loc, labelled the way a column heading needs it.
//
// A nil location, time.UTC and a location named "UTC" all collapse onto UTC, so
// that `--tz utc` — which is legal to state — produces output byte-identical to
// passing no flag at all rather than output that merely resembles it.
//
// label is what the caller called the zone, which is not always what Go can call
// it back: time.Local.String() answers "Local" on a host whose zone came from
// /etc/localtime, and a heading reading `TIME (Local)` names nothing. The caller
// passes the spelling the user typed; an empty one falls back to the location's
// own name, and a location that cannot name itself is labelled "local".
func NewZone(loc *time.Location, label string) Zone {
	if loc == nil || loc == time.UTC || loc.String() == "UTC" {
		return UTC
	}
	if label == "" {
		label = loc.String()
	}
	if label == "" || label == "Local" {
		label = "local"
	}
	return Zone{loc: loc, label: label}
}

// IsUTC reports whether this frame is the default one.
func (z Zone) IsUTC() bool { return z.loc == nil }

// Label names the frame for a column heading or a help string.
func (z Zone) Label() string {
	if z.IsUTC() {
		return "UTC"
	}
	return z.label
}

// TimeColumn is the timeline's TIME heading, carrying the frame its column is in.
//
// The heading keeps a frame even though every value now carries one, because it
// had one before this task and removing it would be taking information away in a
// change whose whole subject is not having enough. What it may never do is
// disagree with the column beneath it, which is why the label is a zone name.
func (z Zone) TimeColumn() string { return columnTime + " (" + z.Label() + ")" }

// Narrow renders an instant for a table read by eye.
func (z Zone) Narrow(ts time.Time) string {
	if z.IsUTC() {
		return ts.UTC().Format(narrowTimeLayout)
	}
	return ts.In(z.loc).Format(narrowZonedTimeLayout)
}

// Wide renders an instant at the precision the schema records.
func (z Zone) Wide(ts time.Time) string {
	if z.IsUTC() {
		return ts.UTC().Format(wideTimeLayout)
	}
	return ts.In(z.loc).Format(wideZonedTimeLayout)
}

// Instant renders a timestamp for a header field, a notice or an error message.
//
// It is the layout every sentence in this CLI spells an instant in, and it is a
// method rather than a package function so that a call site cannot fail to say
// which frame it means. The sites that must stay UTC whatever was asked for —
// the structured envelope's coverage summary, the reconstructed object's
// provenance block — spell `render.UTC.Instant` in the source, where a reviewer
// reads it.
func (z Zone) Instant(ts time.Time) string {
	if z.IsUTC() {
		return ts.UTC().Format(headerTimeLayout)
	}
	return ts.In(z.loc).Format(headerZonedTimeLayout)
}
