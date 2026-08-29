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

// Package render turns a read-plane answer into the characters an engineer
// reads at 02:47.
//
// # Why it is a package and not a method
//
// Everything here is a pure function of data the caller already holds: no
// context, no engine, no clock, no terminal. That is what makes the golden-file
// tests in this repository worth having — they exercise the real renderer with a
// fixed width and colour off, rather than a reimplementation of it that happens
// to agree today. It is also what lets `diff` (Task 11.4) and the structured
// envelope (Task 11.5) share the path formatting and the patch summarization
// instead of growing second readings of both.
//
// # What it will not do
//
// It never invents a row. A backend that cannot record deletions produces a
// timeline that simply stops, and nothing here closes the gap with a synthesized
// Deleted row — the notice on stderr is the honest rendering of that silence
// (Invariant 4). It never drops a row either: an operation whose patch will not
// decode is rendered as the undecodable operation it is, because an audit
// timeline missing an entry is worse than one carrying an ugly cell.
//
// # Widths are counted in runes
//
// The output contains U+2026, U+2192 and U+26A0 by design, so byte length is the
// wrong measure and rune count is used throughout. Rune count is itself an
// approximation — it is wrong for a double-width CJK glyph — and it is the one
// taken deliberately: the alternative is a Unicode width table as a dependency,
// to straighten a column in output whose content is API field paths and field
// manager names.
package render

import (
	"strings"
	"unicode/utf8"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Ellipsis marks text this package shortened, in a path's middle or a value's
// tail. It is one rune so that a column budget spent on the marker is one
// column, and it is exported because a test asserting an elision has to name
// the thing it is asserting.
const Ellipsis = "…"

// Arrow separates an operation's old value from its new one.
const Arrow = "→"

// WarningGlyph prefixes the summary of a Warning-type Kubernetes Event.
//
// It sits at the front of the CHANGE cell rather than inside the EVENT cell
// because it qualifies the message, not the row's kind: the EVENT cell already
// says the row is an Event, and widening that column for a glyph would take the
// space from the one column this release exists to show.
const WarningGlyph = "⚠"

// DefaultWidth is the column budget used when output is not going to a terminal.
//
// A pipe has no width, and the two honest answers are "assume nothing and never
// elide" or "assume a common terminal". The second is chosen because the first
// makes `kuberecord timeline … | less` emit lines several hundred characters
// wide for exactly the objects whose paths were worth eliding, and because a
// caller that genuinely wants everything has --full, which never elides.
const DefaultWidth = 120

// minChangeWidth is the floor for the CHANGE column.
//
// Below it the column stops being able to hold a path and a value at all, and
// the layout would be trading the release's flagship output for the alignment of
// the columns to its left. When the budget cannot cover it, the line is allowed
// to overflow instead: a wrapped line is legible and a truncated one is not.
const minChangeWidth = 32

// minPathWidth is the floor an elided path is allowed to shrink to before the
// value is shortened instead.
//
// A path elided past this says nothing — "spec.…y" identifies no field — so the
// remaining pressure is put on the value, which degrades more gracefully because
// its head is the informative part.
const minPathWidth = 24

// Options is one invocation's rendering surface.
//
// It is passed rather than derived because every input to it is a property of
// where the output is going, and the whole of this package's testability rests
// on those being arguments: a renderer that read the terminal's width and the
// environment's NO_COLOR itself would have golden files that changed with the
// window they were generated in.
type Options struct {
	// Width is the column budget for a line, or zero for DefaultWidth. The
	// command sets it from the terminal when stdout is one.
	Width int

	// Color enables ANSI sequences. The decision behind it — the --color mode,
	// NO_COLOR, and whether stdout is a terminal — belongs to the caller.
	Color bool

	// Wide adds the columns `-o wide` asks for and prints timestamps at the
	// nanosecond precision the schema records, rather than the millisecond
	// precision that reads well in a narrow table.
	Wide bool

	// Full prints every operation of every patch, unelided, beneath its row.
	// Nothing is shortened under it: that is what the flag is for.
	Full bool
}

// width reports the column budget, resolving zero to the default.
func (o Options) width() int {
	if o.Width <= 0 {
		return DefaultWidth
	}
	return o.Width
}

// The ANSI sequences this package uses, kept to a set small enough that a reader
// of the output can learn what each one means.
//
// They are raw escape strings rather than a colour library because that is the
// whole of the requirement: a dependency here would bring a package-level
// enabled flag and a global writer, and this package deliberately has neither.
const (
	ansiReset   = "\x1b[0m"
	ansiDim     = "\x1b[2m"
	ansiRed     = "\x1b[31m"
	ansiGreen   = "\x1b[32m"
	ansiYellow  = "\x1b[33m"
	ansiBlue    = "\x1b[34m"
	ansiMagenta = "\x1b[35m"
	ansiCyan    = "\x1b[36m"
)

// palette paints text, or does not.
//
// The disabled palette returns its argument unchanged rather than returning a
// nil function a caller has to check, so every call site reads the same whether
// colour is on or off — and so that a site that forgot the check cannot exist.
type palette struct{ enabled bool }

// paint wraps text in an ANSI sequence when colour is enabled.
//
// It is deliberately applied *after* every width decision has been made. The
// escape sequences carry no display width, and a layout computed over painted
// text would pad every coloured cell by the length of its escape codes — which
// is how a coloured table acquires a wobble that only appears on a terminal.
func (p palette) paint(sequence, text string) string {
	if !p.enabled || text == "" {
		return text
	}
	return sequence + text + ansiReset
}

func (p palette) dim(text string) string { return p.paint(ansiDim, text) }
func (p palette) red(text string) string { return p.paint(ansiRed, text) }

// eventColor is the colour an event type is rendered in.
//
// The mapping is by meaning rather than by aesthetics: the two rows an engineer
// scans a timeline for are the ones that bracket an object's existence, so
// Deleted is red and Added is green, and the ordinary Modified traffic is yellow
// so that it does not compete with them. An unrecognised type — the enum is open
// (see query.EventAdded) — is left unpainted rather than coerced into a
// neighbour's colour.
func (p palette) eventColor(eventType string) string {
	switch eventType {
	case query.EventAdded:
		return p.paint(ansiGreen, eventType)
	case query.EventModified:
		return p.paint(ansiYellow, eventType)
	case query.EventDeleted:
		return p.paint(ansiRed, eventType)
	case query.EventSnapshot:
		return p.paint(ansiBlue, eventType)
	case query.EventCheckpoint:
		return p.paint(ansiCyan, eventType)
	case query.EventKubernetes:
		return p.paint(ansiMagenta, eventType)
	}
	return eventType
}

// displayWidth is how many columns text occupies. See the package doc on why
// this counts runes.
func displayWidth(text string) int { return utf8.RuneCountInString(text) }

// pad right-aligns nothing and left-aligns everything, which is what a table of
// timestamps, enum values, names and free text wants.
func pad(text string, width int) string {
	if gap := width - displayWidth(text); gap > 0 {
		return text + strings.Repeat(" ", gap)
	}
	return text
}

// truncate shortens text to width, marking that it did.
//
// The marker costs one of the columns, which is the point: a cell shortened
// without a mark is a cell a reader believes. Below two columns there is no room
// for both a marker and a character, so the text is cut without one — an
// unreachable case in this package's own layout, handled rather than assumed
// away.
func truncate(text string, width int) string {
	if width <= 0 {
		return ""
	}
	if displayWidth(text) <= width {
		return text
	}
	if width == 1 {
		return Ellipsis
	}
	runes := []rune(text)
	return string(runes[:width-1]) + Ellipsis
}
