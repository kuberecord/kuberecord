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

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The timeline document, and the rule about which stream each half of it goes
// to.
//
// Standard output carries the document: the header and the table, and nothing
// else. Standard error carries everything that qualifies the document — the
// multi-incarnation banner, the capability notice, the explanation of an empty
// result — alongside the backend-resolution notices the resolver already writes
// there.
//
// One sentence states the rule: stdout is the data, stderr explains it. It is
// what makes `kuberecord timeline … | wc -l` count changes, and it is the same
// split the package documentation of internal/cli already commits to for the
// sake of `-o json | jq`. It also means a notice is never optional for fear of
// corrupting a pipe, which matters, because under Invariants 4 and 5 the notices
// are the honest half of the output.

// Column headings. They are constants because the layout measures them, the
// golden files assert them, and a heading that drifted from what the tests pin
// would be a silent change to something people script `awk` against.
const (
	// columnTime is the heading without its frame. The frame is appended by
	// Zone.TimeColumn, because the column's contents are in whatever zone the
	// invocation asked for and a heading that named a different one would be the
	// disagreement Task 18.9 exists to close, one layer along.
	columnTime     = "TIME"
	columnUID      = "UID"
	columnEvent    = "EVENT"
	columnRevision = "RESOURCE VERSION"
	columnActor    = "ACTOR"
	columnChange   = "CHANGE"
)

// gutter separates two columns. Two spaces, as kubectl uses, so that an eye
// scanning down a column does not have to find its edge.
const gutter = "  "

// UnknownActor is what an actorless change renders as.
//
// A deletion never records actors — there is no live object left to read field
// managers off (see query.Change.Actors) — and neither does a row whose managed
// fields were empty. The word is dimmed rather than left blank because a blank
// cell reads as "nobody", and "we do not know who" is the true statement.
const UnknownActor = "unknown"

// incarnationsLabel is the header row that replaces UID when every incarnation
// in the window is being listed. It is a constant because renderHeader branches
// on it, and a label matched by literal in two places is a label that eventually
// only matches in one.
const incarnationsLabel = "Incarnations"

// coverageLabel is the header row whose *value* changes weight, which is why it
// is a constant for the reason incarnationsLabel is one: renderHeader branches on
// it in order to decide the tier, and a label matched by literal in two places is
// a label that eventually only matches in one.
const coverageLabel = "Coverage"

// uidPrefixLength is how much of a UID a narrow table shows.
//
// Eight hexadecimal characters distinguish the two or three incarnations a
// reused name actually has, and the full values are printed in the header so
// that one can be pasted into --uid. Showing all thirty-six on every row would
// take a third of the terminal from the CHANGE column, which is the column the
// release exists for.
const uidPrefixLength = 8

// The timestamp layouts. The narrow one is milliseconds because a table is read
// by eye; the wide one is the full precision the schema records, because two
// changes a microsecond apart are two changes and `-o wide` is where a reader
// goes to tell them apart.
//
// The wide layout spells its fractional digits out rather than using
// time.RFC3339Nano, which trims trailing zeros: that would render one row as
// .9 and its neighbour as .482913004, and a column of timestamps at varying
// precision reads as data of varying precision.
//
// The narrow layout keeps its space in place of RFC 3339's `T` and its
// milliseconds, and it is not allowed to keep dropping the trailing marker for
// them: a column heading is not attached to the data. `TIME (UTC)` scrolls off a
// nine-row table and does not travel when a row is pasted into a post-mortem,
// where `2026-09-10 22:41:13.263` reads as a local time two hours from the
// instant it names (D45).
//
// The first three are the UTC layouts and their trailing `Z` is a literal — Go
// reads `Z` as a marker only when `0700` or `07:00` follows it. The zoned three
// below spell that suffix out, so a non-UTC frame renders an explicit numeric
// offset and never a bare local time. That prohibition is the whole reason --tz
// is a flag rather than something a reader does with a shell alias.
const (
	narrowTimeLayout = "2006-01-02 15:04:05.000Z"
	wideTimeLayout   = "2006-01-02T15:04:05.000000000Z"
	headerTimeLayout = "2006-01-02T15:04:05Z"

	narrowZonedTimeLayout = "2006-01-02 15:04:05.000-07:00"
	wideZonedTimeLayout   = "2006-01-02T15:04:05.000000000-07:00"
	headerZonedTimeLayout = "2006-01-02T15:04:05-07:00"
)

// TimelineRow is one change, decoded as far as rendering needs it.
type TimelineRow struct {
	// Change is the row as the read plane returned it.
	Change query.Change

	// Ops are Change.Diff decoded, with Op.Old filled in by the caller wherever
	// a state replay established the value an operation replaced.
	Ops []Op

	// PatchErr says why Change.Diff could not be decoded, and is empty when it
	// could. It is rendered rather than swallowed: a row whose patch will not
	// parse is still a change that happened, and dropping it would take an entry
	// out of an audit timeline to spare a reader an ugly cell.
	PatchErr string
}

// Notice is one line of explanation for the document.
//
// There is no severity on it, and that is the decision rather than an omission.
// Every notice this CLI writes is a line the reader would draw a false
// conclusion without — a window with no state before it, a backend that records
// no deletions, a scan that is the work, a row the column could not hold — which
// is one tier, the Warning tier, and a struct field offering a second one would
// be an invitation to render some of them quietly (D30).
type Notice struct {
	// Text is the sentence, without a prefix or a trailing newline.
	//
	// It may hold newlines, and a few notices need to: one that answers "how do
	// I fix this?" with a fragment of YAML has to print the fragment in a shape
	// the reader can copy, and prose that spelled the same three fields inline
	// would be prose they have to reassemble. Every line is rendered in the same
	// tier, because the block is one notice and not a notice with quieter lines
	// after it.
	//
	// Indentation inside the text is relative: renderNotices aligns the
	// continuation lines under the first one, so an author writes the shape the
	// block has rather than the shape the marker leaves room for. Nothing here
	// should know how wide WarningMarker is.
	Text string
}

// TimelineDocument is a rendered timeline, ready to be written.
//
// Every field is already a string because the decisions behind them — which
// incarnation, how to summarize coverage, whether a window was defaulted —
// belong to the command that consulted the backend, not to the renderer. Keeping
// them out of here is what lets the golden-file tests drive this package with
// nothing but data.
type TimelineDocument struct {
	// Kind is the object's group and kind, "apps/Deployment", or the bare kind
	// for the core group.
	Kind string
	// Object is "namespace/name", or the bare name for a cluster-scoped kind.
	Object string
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// UID is the incarnation being shown. Empty when no row was found and none
	// could be named.
	UID string
	// Incarnations holds every UID in the window, set only when the caller asked
	// for all of them. When it is set the table grows a UID column, because a
	// single table spanning two incarnations must never read as one history
	// (Invariant 7).
	Incarnations []string
	// Coverage is the pre-rendered coverage summary for the header.
	Coverage string
	// CoverageAbsent reports that the summary above says nothing was watching.
	// See documentHeader.CoverageAbsent.
	CoverageAbsent bool
	// Rows are the changes, in the order they are to be displayed.
	Rows []TimelineRow
	// Notices are written to standard error, in order.
	Notices []Notice
}

// showUID reports whether the table carries a UID column.
func (d TimelineDocument) showUID(opts Options) bool {
	return len(d.Incarnations) > 0 || opts.Wide
}

// header is the block of facts every document in this package opens with.
//
// It is a separate type rather than renderHeader taking a TimelineDocument
// because `diff` opens with the same block and must not open with a *nearly* the
// same one: the five facts a reader needs before the first row means anything are
// the same five whatever shape the rows take, and two renderings of them would
// eventually disagree about whether coverage was stated.
func (d TimelineDocument) header() documentHeader {
	return documentHeader{
		Kind:           d.Kind,
		Object:         d.Object,
		Cluster:        d.Cluster,
		UID:            d.UID,
		Incarnations:   d.Incarnations,
		Coverage:       d.Coverage,
		CoverageAbsent: d.CoverageAbsent,
	}
}

// documentHeader is the identity block shared by every rendered document.
type documentHeader struct {
	// Kind is the object's group and kind, "apps/Deployment", or the bare kind
	// for the core group.
	Kind string
	// Object is "namespace/name", or the bare name for a cluster-scoped kind.
	Object string
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// UID is the incarnation being shown, empty when none could be named.
	UID string
	// Incarnations holds every UID in the window, listed in place of UID when a
	// command is showing all of them.
	Incarnations []string
	// Window is the pre-rendered window the question was asked over, and is
	// written only when a command sets it.
	//
	// `timeline` and `diff` leave it empty: their rows carry timestamps, so the
	// window is visible in the answer itself. `blame` sets it because its rows
	// carry an attribution rather than a change, and a cell reading
	// "(before window)" is unreadable without the window it is before.
	Window string
	// Base names the recorded row a reconstruction or an attribution started
	// from, and is written only when a command sets it.
	//
	// It is provenance of the kind `get --at` states in its own header: an answer
	// assembled from history is a claim about the past, and naming the row it was
	// assembled from is what lets a reader judge it rather than trust it.
	Base string
	// Coverage is the pre-rendered coverage summary.
	Coverage string
	// CoverageAbsent reports that the summary above says nothing was ever
	// watching this scope, which is what puts the value in the Warning tier.
	//
	// A flag rather than a comparison against the sentence, because the sentence
	// is written by the command that consulted the scope log and this package
	// cannot read a claim out of prose. It is the same division every other field
	// here follows: the command decides, the renderer renders.
	//
	// It is deliberately not set for a backend that has no scope log. That state
	// is a permanent property of an archive tier (D12) rather than a finding about
	// this object, and a tier spent on every invocation against one is a tier
	// spent on nothing — the header says `not reported by this backend` at full
	// weight and the notice on stderr carries the consequence.
	CoverageAbsent bool
}

// WriteTimeline writes the document to out and its notices to errOut.
//
// Both writes are checked. The document's is the command's whole answer, so a
// failure to write it has to become the command's failure; the notices' is
// checked for the same reason it is checked everywhere else in this CLI, and is
// reported rather than discarded because a notice that did not arrive is a
// qualification the reader never saw.
func WriteTimeline(out, errOut io.Writer, doc TimelineDocument, opts Options) error {
	// Rendered even when there is nowhere to write it, because the count of rows
	// the CHANGE column could not hold falls out of the layout and the footer on
	// the other stream is built from it.
	table, shortened := renderTimeline(doc, opts)
	if out != nil {
		if _, err := io.WriteString(out, table); err != nil {
			return fmt.Errorf("writing the timeline: %w", err)
		}
	}

	notices := doc.Notices
	if hint := fullHint(shortened, opts); hint != "" {
		// Appended to a copy: doc is the caller's, and growing its slice in place
		// would put a rendering decision into a value the command still holds.
		notices = append(append([]Notice(nil), notices...), Notice{Text: hint})
	}
	if errOut == nil || len(notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(notices, opts)); err != nil {
		return fmt.Errorf("writing the timeline's notices: %w", err)
	}
	return nil
}

// fullHint is the footer that names --full, or nothing.
//
// # Why it is conditional, and why it is one line
//
// The flag is named where its absence is visible and nowhere else. A hint on
// every row would put "(--full)" a hundred times under a timeline of
// single-field edits, which is a worse noise than the problem it answers; a hint
// printed unconditionally would appear under documents where nothing was
// shortened, and a footer that says something untrue about the output above it
// teaches a reader to stop looking at footers. So it is emitted once, and only
// when the column actually withheld something.
//
// It is silent under --full for the obvious reason and the important one: the
// flag is already on, so there is nothing left to name, and a line advertising a
// flag the reader has just used would read as the tool not having noticed.
//
// The count is there because it is the difference between "some of this is
// summarized" and knowing whether the row you are looking at is one of them.
func fullHint(shortened int, opts Options) string {
	if shortened == 0 || opts.Full {
		return ""
	}
	verb := "rows are"
	if shortened == 1 {
		verb = "row is"
	}
	return fmt.Sprintf("%d %s shortened to fit the %s column; pass --full to print every operation",
		shortened, verb, columnChange)
}

// WriteNotices writes a document's qualifications to errOut, and nothing to
// stdout.
//
// It exists for the structured renderings, whose document is written by a Stream
// rather than by one of the WriteX functions above, and which must still put
// every qualification on the other stream. Exported rather than duplicated at
// those call sites because the marker, the spacing and the colour of a notice are
// part of how this CLI reads, and a second implementation of them would drift.
func WriteNotices(errOut io.Writer, notices []Notice, opts Options) error {
	if errOut == nil || len(notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(notices, opts)); err != nil {
		return fmt.Errorf("writing the document's notices: %w", err)
	}
	return nil
}

// renderNotices builds the stderr half.
//
// The prefix is WarningMarker rather than the resolver's "→" so that the two are
// distinguishable in a terminal where they arrive together: one says where the
// data came from, the other says what to be careful about in it. It is the half
// of the severity that survives NO_COLOR, a redirected stream and a golden file,
// which is why it is a character and not only a colour.
//
// The marker itself is left unpainted and the sentence is what carries the tier.
// A painted marker in front of unpainted prose was what this used to be, and it
// put the whole of the severity into one character while the sentence — the part
// that is actually read — rendered at the same weight as the table above it.
//
// A notice spanning several lines is painted a line at a time and marked only on
// the first, which is the shape the unreachable-sink diagnostic already renders
// its paragraph in. Both halves of that matter. Marking every line would put a
// column of "!" down the side of a block somebody is reading, and one colour span
// straddling the newlines would be a single escape sequence that anything reading
// stderr a line at a time — a pager without -R, a log collector — splits down the
// middle.
//
// The continuation lines are indented to the marker's own width, so that a block
// hangs under its first character and the notice's text can be written with the
// relative shape it has. An empty line is left empty rather than padded: trailing
// whitespace is invisible on a terminal and permanent in a golden file.
func renderNotices(notices []Notice, opts Options) string {
	severity := NewSeverity(opts.Color)
	hang := strings.Repeat(" ", len(WarningMarker)+1)

	var built strings.Builder
	for _, notice := range notices {
		for i, line := range strings.Split(notice.Text, "\n") {
			switch {
			case i == 0:
				built.WriteString(WarningMarker + " ")
			case line != "":
				built.WriteString(hang)
			}
			built.WriteString(severity.Warning(line) + "\n")
		}
	}
	return built.String()
}

// renderTimeline builds the stdout half: the header, a blank line, and the
// table.
//
// It also reports how many rows the CHANGE column could not show whole, which is
// a fact only the layout knows — the column's width is whatever the other columns
// left over — and which the footer on the other stream is built from.
func renderTimeline(doc TimelineDocument, opts Options) (string, int) {
	severity := NewSeverity(opts.Color)
	p := severity.palette

	var built strings.Builder
	built.WriteString(renderHeader(doc.header(), severity))
	if len(doc.Rows) == 0 {
		// No table, not an empty one. Why the result is empty is on stderr, where
		// every other qualification of the document is; a header row with nothing
		// under it would imply the question was answered and the answer was none.
		return built.String(), 0
	}
	built.WriteString("\n")
	table, shortened := renderTable(doc, opts, p)
	built.WriteString(table)
	return built.String(), shortened
}

// renderHeader renders the five facts a reader needs before the first row means
// anything: which kind, which object, which cluster, which incarnation, and
// whether anything was watching.
//
// It takes a Severity rather than a palette because the last of those five is not
// always a fact of the same weight as the other four. `Coverage: none recorded for
// this scope` is the whole of Invariant 9's finding, restated at length by an
// `error:` two lines below it, and rendering it at the weight of a cluster name
// left the header and the error disagreeing about how much it mattered — so the
// value goes into the Warning tier when there was no coverage at all (D30, Task
// 18.5). The labels stay in the provenance tier they were always in, which is what
// keeps the amber on the fact rather than on the word in front of it.
func renderHeader(doc documentHeader, severity Severity) string {
	type field struct{ label, value string }

	fields := []field{
		{"Kind", doc.Kind},
		{"Object", doc.Object},
		{"Cluster", doc.Cluster},
	}
	switch {
	case len(doc.Incarnations) > 0:
		fields = append(fields, field{incarnationsLabel, ""})
	case doc.UID != "":
		fields = append(fields, field{"UID", doc.UID})
	}
	// Only when a command set them, so that the header of a document that has no
	// use for either is exactly the header it was before they existed — which is
	// what the checked-in golden files of the other commands assert.
	if doc.Window != "" {
		fields = append(fields, field{"Window", doc.Window})
	}
	if doc.Base != "" {
		fields = append(fields, field{"Base", doc.Base})
	}
	fields = append(fields, field{coverageLabel, doc.Coverage})

	labelWidth := 0
	for _, f := range fields {
		labelWidth = max(labelWidth, displayWidth(f.label))
	}

	var built strings.Builder
	for _, f := range fields {
		// The label is painted and *then* padded, so the escape sequences never
		// enter the width arithmetic that lines the values up.
		label := severity.dim(f.label+":") + strings.Repeat(" ", labelWidth-displayWidth(f.label)) + " "
		if f.label == incarnationsLabel {
			built.WriteString(label + renderIncarnations(doc, labelWidth+2))
			continue
		}
		value := f.value
		// No marker in front of it, unlike a notice: this is a labelled field in a
		// block of labelled fields, and a `!` here would add a character that the
		// uncoloured rendering has to carry too — which is the one thing the tiers
		// may not do to a line (TestColourIsNothingButColour). The label already
		// says what the value is about; what the tier adds is how much it matters.
		if f.label == coverageLabel && doc.CoverageAbsent {
			value = severity.Warning(value)
		}
		built.WriteString(label + value + "\n")
	}
	return built.String()
}

// renderIncarnations lists every UID in the window, one per line, marking the
// one whose rows would have been shown by default.
//
// They are listed in full rather than abbreviated because the header is where a
// reader goes to get a UID to paste into --uid, and a prefix is not a UID.
func renderIncarnations(doc documentHeader, indent int) string {
	var built strings.Builder
	for i, uid := range doc.Incarnations {
		if i > 0 {
			built.WriteString(strings.Repeat(" ", indent))
		}
		built.WriteString(uid)
		if uid == doc.UID {
			built.WriteString(" (current)")
		}
		built.WriteString("\n")
	}
	return built.String()
}

// renderTable lays the changes out, giving every column the width its content
// needs and the CHANGE column whatever is left.
//
// The layout is computed over unpainted text and the colour applied afterwards,
// because ANSI escapes have no display width: padding computed over painted
// cells is padding that includes the escape sequences, which is how a coloured
// table acquires a wobble that never shows up in a test with colour off.
func renderTable(doc TimelineDocument, opts Options, p palette) (string, int) {
	showUID := doc.showUID(opts)

	headings := []string{opts.Zone.TimeColumn()}
	if showUID {
		headings = append(headings, columnUID)
	}
	headings = append(headings, columnEvent)
	if opts.Wide {
		headings = append(headings, columnRevision)
	}
	headings = append(headings, columnActor, columnChange)

	// Every column but the last is measured; the last absorbs the slack.
	fixed := make([]string, len(headings)-1)
	copy(fixed, headings[:len(headings)-1])
	widths := make([]int, len(fixed))
	for i, heading := range fixed {
		widths[i] = displayWidth(heading)
	}

	plain := make([][]string, 0, len(doc.Rows))
	for _, row := range doc.Rows {
		cells := plainCells(row, showUID, opts)
		for i := range widths {
			widths[i] = max(widths[i], displayWidth(cells[i]))
		}
		plain = append(plain, cells)
	}

	spent := 0
	for _, width := range widths {
		spent += width + len(gutter)
	}
	changeWidth := max(opts.width()-spent, minChangeWidth)

	var built strings.Builder
	shortened := 0
	built.WriteString(p.dim(strings.TrimRight(headerLine(headings, widths), " ")) + "\n")
	for i, row := range doc.Rows {
		built.WriteString(renderRow(row, plain[i], fixed, widths, changeWidth, opts, p))
		if elided(row, changeWidth) {
			shortened++
		}
	}
	return built.String(), shortened
}

// headerLine lays the headings out over the measured widths.
func headerLine(headings []string, widths []int) string {
	var built strings.Builder
	for i, heading := range headings {
		if i > 0 {
			built.WriteString(gutter)
		}
		if i < len(widths) {
			built.WriteString(pad(heading, widths[i]))
			continue
		}
		built.WriteString(heading)
	}
	return built.String()
}

// plainCells renders every column but CHANGE, unpainted, so the layout can be
// measured.
func plainCells(row TimelineRow, showUID bool, opts Options) []string {
	cells := []string{formatTimestamp(row.Change.TS, opts.Wide, opts.Zone)}
	if showUID {
		cells = append(cells, formatUID(row.Change.UID, opts.Wide))
	}
	cells = append(cells, row.Change.EventType)
	if opts.Wide {
		cells = append(cells, valueOrDash(row.Change.ResourceVersion))
	}
	cells = append(cells, actorCell(row))
	return cells
}

// renderRow writes one change, and the operations beneath it when --full is set.
func renderRow(
	row TimelineRow, cells, headings []string, widths []int, changeWidth int, opts Options, p palette,
) string {
	var built strings.Builder
	for i, cell := range cells {
		if i > 0 {
			built.WriteString(gutter)
		}
		built.WriteString(paintCell(cell, headings[i], p) + strings.Repeat(" ", max(widths[i]-displayWidth(cell), 0)))
		if i == len(cells)-1 {
			built.WriteString(gutter)
		}
	}
	built.WriteString(changeCell(row, changeWidth, p))
	built.WriteString("\n")

	if opts.Full {
		if lines := fullLines(row, changeWidth, p); len(lines) > 0 {
			for _, line := range lines {
				built.WriteString(fullIndent + line + "\n")
			}
			// The half of the separation that is not colour, and the reason the
			// blank line is here rather than being left to the tier. Under
			// --color=never, under NO_COLOR and in a redirected file the
			// provenance tier is the identity function, so an eleven-operation
			// block would run into the next timestamp with nothing between them —
			// which is the state this task was reported from.
			//
			// It is written only for a row that actually expanded, so a timeline
			// where the CHANGE column held everything is byte for byte the
			// document it was before any of this existed.
			built.WriteString("\n")
		}
	}
	return built.String()
}

// paintCell applies the colour a column earns.
//
// It is keyed on the column's heading rather than on its position, because which
// columns are present depends on --all-incarnations and -o wide: a switch on an
// index would go on compiling and start painting the wrong column the day a flag
// inserts one.
func paintCell(cell, heading string, p palette) string {
	switch heading {
	case columnEvent:
		return p.eventColor(cell)
	case columnActor:
		if cell == UnknownActor {
			return p.dim(cell)
		}
	}
	return cell
}

// changeCell decides what the flagship column says for one row.
//
// The order of the branches is the order of certainty. A merged Kubernetes Event
// describes something that happened *to* the object and is read from its own
// data; a deletion is a fact with no patch to summarize; a single operation is
// the row the release exists to print; several operations are counted rather
// than crushed; and a row with neither a patch nor state says so in words rather
// than as a blank cell.
func changeCell(row TimelineRow, width int, p palette) string {
	if row.Change.EventType == query.EventKubernetes {
		return eventCell(row, width, p)
	}
	if row.Change.EventType == query.EventDeleted {
		return truncate(describeDeleted, width)
	}
	switch {
	case row.PatchErr != "":
		return truncate("unreadable patch: "+row.PatchErr, width)
	case len(row.Ops) == 1:
		return opText(row.Ops[0], width)
	case len(row.Ops) > 1:
		return multiOpSummary(len(row.Ops))
	case row.Change.Diff != "":
		return truncate(describeEmptyPatch, width)
	}
	return truncate(describeState(row.Change), width)
}

// describeState says what a row carrying full state and no patch is.
func describeState(change query.Change) string {
	if change.Data == "" {
		return describeNoDetail
	}
	switch change.EventType {
	case query.EventSnapshot:
		return describeSnapshot
	case query.EventCheckpoint:
		return describeCheckpoint
	case query.EventModified:
		// A modification with state and no patch is the fallback path: the diff
		// could not be produced, so the row carries the whole object instead.
		// Saying so distinguishes it from a first sighting, which carries state
		// for a completely different reason.
		return describeNoPatch
	}
	return describeFullState
}

// eventCell renders a merged Kubernetes Event.
func eventCell(row TimelineRow, width int, p palette) string {
	detail, ok := ParseEvent(row.Change.Data)
	if !ok {
		return truncate("Kubernetes Event with no data recorded", width)
	}
	summary := detail.Summary()
	if !detail.Warning() {
		return truncate(summary, width)
	}
	// The glyph is painted after the width decision, and the width is measured on
	// the unpainted string, so a red glyph costs exactly the one column it
	// occupies on screen.
	fitted := truncate(summary, width)
	return p.red(WarningGlyph) + strings.TrimPrefix(fitted, WarningGlyph)
}

// fullIndent hangs an expanded operation under the row it belongs to.
//
// Four columns rather than the gutter's two, so that the block sits clear of the
// TIME column's left edge and a reader scanning down the timestamps is scanning
// down a straight line with nothing in it.
const fullIndent = "    "

// fullLines renders every operation of a patch, unelided, for --full.
//
// A single operation the summary already showed whole is not repeated: --full
// expands what the column collapsed, and doubling every line of a timeline of
// single-field edits would make the flag cost more than it gives. A single
// operation the column *did* shorten is expanded, which is the case the flag
// exists for.
//
// # Why the block recedes instead of the row standing out
//
// An eleven-operation patch expands into a wall with no visible boundary between
// one timestamp's changeset and the next, and the obvious answer — emphasise the
// row above it — is the wrong one twice over. Emphasis is the line that must
// survive its block being skimmed (see severity.go), so emphasising every row in
// a screen of rows emphasises none of them and spends a tier on the way; and a
// timestamp is not more *severe* than the operations beneath it, it is
// structurally their parent. Expressing structure through a severity register is
// the category error the closed vocabulary (D27) exists to refuse, so Emphasis is
// not spent here and is not spent anywhere in this document.
//
// What is left is contrast, which is the move Task 15.5 already made for `get`:
// the recorded object became findable by dimming the wrapper around it rather
// than by brightening it. Here the expanded operations are the detail the reader
// asked to see and the row is the spine, so the detail takes the provenance tier
// and the row is not touched at all.
//
// The operation's glyph stays at full intensity inside the dimmed line. +, - and
// ~ are how a reader scans a block for the *kind* of change in it, and dimming
// them uniformly would flatten the one signal the block has; opTextParts is the
// split that allows it without the renderer having to be rearranged for it.
func fullLines(row TimelineRow, changeWidth int, p palette) []string {
	// The table's own colour decision rather than a second reading of it, and the
	// vocabulary rather than the mechanism underneath it: a call to p.dim here
	// would be a register chosen at a call site, which is the drift severity.go
	// exists to stop.
	severity := Severity{palette: p}

	if row.PatchErr != "" {
		// The block recedes whole, this line included. The failure is already
		// stated at full intensity in the CHANGE cell above — this is its
		// expansion and not a second announcement of it — so Invariant 4 is
		// carried by the spine rather than by the detail hanging off it.
		return []string{severity.Provenance("patch could not be decoded: " + row.PatchErr)}
	}
	if !elided(row, changeWidth) {
		return nil
	}
	lines := make([]string, 0, len(row.Ops))
	for _, op := range row.Ops {
		marker, detail := opTextParts(op)
		lines = append(lines, marker+severity.Provenance(detail))
	}
	return lines
}

// elided reports whether the CHANGE column showed this row with something held
// back.
//
// Two cases, and they are the two --full answers: a patch of several operations
// is summarized as a count, and a single operation the column was too narrow for
// is shortened. A row whose one operation fitted whole has nothing behind it, and
// counting it would put a footer under a document where every character of every
// patch is already on the screen.
//
// It is the predicate fullLines decides by, deliberately: the footer promises
// that --full will show more, and a second reading of "more" would eventually
// promise it for a row the flag prints nothing extra for. The undecodable-patch
// row is the one case handled by fullLines and not here — the flag does expand
// its wording, but the cell above it already carries the same failure, so
// advertising the flag for it would be advertising a rephrasing.
func elided(row TimelineRow, changeWidth int) bool {
	switch row.Change.EventType {
	case query.EventKubernetes, query.EventDeleted:
		// changeCell answers both of these from the row itself and never from its
		// operations, so whatever a patch column held there is not what the cell
		// is showing and --full would not expand it.
		return false
	}
	switch len(row.Ops) {
	case 0:
		return false
	case 1:
		return opText(row.Ops[0], changeWidth) != opText(row.Ops[0], 0)
	}
	return true
}

// formatTimestamp renders a change's instant at the precision the format asks
// for, in the frame the invocation asked for.
//
// The frame is on the value rather than only on the heading above it, and the
// default is UTC because the schema column is UTC and docs/QUERIES.md is UTC: a
// CLI showing local time while the SQL shows UTC would be two views of one audit
// trail disagreeing (D46).
func formatTimestamp(ts time.Time, wide bool, zone Zone) string {
	if wide {
		return zone.Wide(ts)
	}
	return zone.Narrow(ts)
}

// formatUID abbreviates a UID for the table, or does not for -o wide.
func formatUID(uid string, wide bool) string {
	if wide || len(uid) <= uidPrefixLength {
		return valueOrDash(uid)
	}
	return uid[:uidPrefixLength] + Ellipsis
}

// actorCell names who a row is attributed to.
//
// The field managers come first and are joined as the read plane sorted them: no
// re-sorting happens here, because that would be a second reading of an ordering
// the contract already fixed and the two would eventually drift.
//
// A merged Kubernetes Event falls back to the controller that reported it. Its
// field managers are the managers of the Event object, which a cluster may or may
// not have recorded, and "kubelet" is a better answer to "who" than "unknown" is
// for a row that is entirely about what kubelet had to say.
func actorCell(row TimelineRow) string {
	if len(row.Change.Actors) > 0 {
		return strings.Join(row.Change.Actors, ",")
	}
	if row.Change.EventType == query.EventKubernetes {
		if detail, ok := ParseEvent(row.Change.Data); ok && detail.Reporter != "" {
			return detail.Reporter
		}
	}
	return UnknownActor
}

// valueOrDash renders an absent value as a dash rather than as a gap.
func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
