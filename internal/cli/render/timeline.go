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
	columnTime     = "TIME (UTC)"
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
const (
	narrowTimeLayout = "2006-01-02 15:04:05.000"
	wideTimeLayout   = "2006-01-02T15:04:05.000000000Z"
	headerTimeLayout = "2006-01-02T15:04:05Z"
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
type Notice struct {
	// Text is the sentence, without a prefix or a trailing newline.
	Text string
	// Warning marks a notice about something missing or unprovable, as opposed
	// to one merely reporting what was chosen.
	Warning bool
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
		Kind:         d.Kind,
		Object:       d.Object,
		Cluster:      d.Cluster,
		UID:          d.UID,
		Incarnations: d.Incarnations,
		Coverage:     d.Coverage,
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
	// Coverage is the pre-rendered coverage summary.
	Coverage string
}

// WriteTimeline writes the document to out and its notices to errOut.
//
// Both writes are checked. The document's is the command's whole answer, so a
// failure to write it has to become the command's failure; the notices' is
// checked for the same reason it is checked everywhere else in this CLI, and is
// reported rather than discarded because a notice that did not arrive is a
// qualification the reader never saw.
func WriteTimeline(out, errOut io.Writer, doc TimelineDocument, opts Options) error {
	if out != nil {
		if _, err := io.WriteString(out, renderTimeline(doc, opts)); err != nil {
			return fmt.Errorf("writing the timeline: %w", err)
		}
	}
	if errOut == nil || len(doc.Notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(doc.Notices, opts)); err != nil {
		return fmt.Errorf("writing the timeline's notices: %w", err)
	}
	return nil
}

// renderNotices builds the stderr half.
//
// The prefix is "!" rather than the resolver's "→" so that the two are
// distinguishable in a terminal where they arrive together: one says where the
// data came from, the other says what to be careful about in it.
func renderNotices(notices []Notice, opts Options) string {
	p := palette{enabled: opts.Color}
	var built strings.Builder
	for _, notice := range notices {
		marker := "!"
		if notice.Warning {
			marker = p.red("!")
		}
		built.WriteString(marker + " " + notice.Text + "\n")
	}
	return built.String()
}

// renderTimeline builds the stdout half: the header, a blank line, and the
// table.
func renderTimeline(doc TimelineDocument, opts Options) string {
	p := palette{enabled: opts.Color}

	var built strings.Builder
	built.WriteString(renderHeader(doc.header(), p))
	if len(doc.Rows) == 0 {
		// No table, not an empty one. Why the result is empty is on stderr, where
		// every other qualification of the document is; a header row with nothing
		// under it would imply the question was answered and the answer was none.
		return built.String()
	}
	built.WriteString("\n")
	built.WriteString(renderTable(doc, opts, p))
	return built.String()
}

// renderHeader renders the five facts a reader needs before the first row means
// anything: which kind, which object, which cluster, which incarnation, and
// whether anything was watching.
func renderHeader(doc documentHeader, p palette) string {
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
	fields = append(fields, field{"Coverage", doc.Coverage})

	labelWidth := 0
	for _, f := range fields {
		labelWidth = max(labelWidth, displayWidth(f.label))
	}

	var built strings.Builder
	for _, f := range fields {
		// The label is painted and *then* padded, so the escape sequences never
		// enter the width arithmetic that lines the values up.
		label := p.dim(f.label+":") + strings.Repeat(" ", labelWidth-displayWidth(f.label)) + " "
		if f.label == incarnationsLabel {
			built.WriteString(label + renderIncarnations(doc, labelWidth+2))
			continue
		}
		built.WriteString(label + f.value + "\n")
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
func renderTable(doc TimelineDocument, opts Options, p palette) string {
	showUID := doc.showUID(opts)

	headings := []string{columnTime}
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
	built.WriteString(p.dim(strings.TrimRight(headerLine(headings, widths), " ")) + "\n")
	for i, row := range doc.Rows {
		built.WriteString(renderRow(row, plain[i], fixed, widths, changeWidth, opts, p))
	}
	return built.String()
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
	cells := []string{formatTimestamp(row.Change.TS, opts.Wide)}
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
		for _, line := range fullLines(row, changeWidth) {
			built.WriteString("    " + line + "\n")
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

// fullLines renders every operation of a patch, unelided, for --full.
//
// A single operation the summary already showed whole is not repeated: --full
// expands what the column collapsed, and doubling every line of a timeline of
// single-field edits would make the flag cost more than it gives. A single
// operation the column *did* shorten is expanded, which is the case the flag
// exists for.
func fullLines(row TimelineRow, changeWidth int) []string {
	if row.PatchErr != "" {
		return []string{"patch could not be decoded: " + row.PatchErr}
	}
	if len(row.Ops) == 0 {
		return nil
	}
	if len(row.Ops) == 1 && opText(row.Ops[0], changeWidth) == opText(row.Ops[0], 0) {
		return nil
	}
	lines := make([]string, 0, len(row.Ops))
	for _, op := range row.Ops {
		lines = append(lines, opText(op, 0))
	}
	return lines
}

// formatTimestamp renders a change's instant at the precision the format asks
// for. Everything is UTC: the header column says so, and a timeline whose rows
// were in local time would be unusable the moment two engineers compared them.
func formatTimestamp(ts time.Time, wide bool) string {
	if wide {
		return ts.UTC().Format(wideTimeLayout)
	}
	return ts.UTC().Format(narrowTimeLayout)
}

// FormatInstant renders a timestamp for the header and for a notice.
//
// It is exported because the command builds the coverage summary and the
// empty-result explanation, and those must not spell an instant differently from
// the way the document does.
func FormatInstant(ts time.Time) string { return ts.UTC().Format(headerTimeLayout) }

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
