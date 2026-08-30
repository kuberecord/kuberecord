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
)

// Per-field attribution: which recorded change last wrote each field of an
// object.
//
// `timeline` and `diff` are organized by change — here is an instant, here is
// what moved. This is organized the other way round, by field, which is the shape
// of the question somebody actually arrives with: not "what happened at 14:05"
// but "who set this replica count, and when".
//
// # Why an unattributed field is a row rather than an omission
//
// A bounded window holds only some of an object's history, so most of a fat
// object's fields were last written before it. Dropping those rows would leave a
// table that reads as the object's whole field list while being a list of the
// fields that happened to move recently — and a reader scanning for a field that
// is not there would conclude it does not exist. They are rendered with
// BeforeWindow in the LAST CHANGED cell instead, which says exactly what is known:
// the field is there, and its last write is older than the window that was asked
// about.
//
// # Why a removed field is a row too
//
// A field deleted inside the window is not in the object any more, so nothing in
// the end state would list it. It is kept, marked, because "who removed the memory
// limit" is one of the two questions this command exists to answer, and a table
// that silently omitted the answer would be the class of silence Invariant 4
// forbids.

// Column headings for the blame table. Constants for the reason every other
// table's are: the layout measures them, the golden files assert them, and a
// heading that drifted would silently change something people `awk` against.
const (
	columnField       = "FIELD"
	columnLastChanged = "LAST CHANGED"
	columnFields      = "FIELDS"
)

// BeforeWindow is what the LAST CHANGED cell says for a field whose last write
// predates the window.
//
// It is a phrase and not a blank, and the distinction is the whole of this row's
// honesty: a blank cell reads as "never written", which is a claim about the
// object, while this says the write happened and is outside what was read. The
// header's Window and Base lines are what anchor the phrase to instants.
//
// Exported because a test asserting the acceptance criterion has to name the
// thing it is asserting.
const BeforeWindow = "(before window)"

// RemovedMarker follows the path of a field whose last write deleted it.
//
// Two spaces before it because it annotates the path rather than being part of
// it: a reader copying the path into --field must be able to see where the path
// ends.
const RemovedMarker = "(removed)"

// BlameRow is one field's attribution, decoded as far as rendering needs it.
type BlameRow struct {
	// Path is the dotted display path with bracketed indices — the grammar the
	// table prints everywhere else and the one --field accepts.
	Path string

	// Pointer is the RFC 6901 pointer Path was rendered from, carried so that
	// structured output can offer both spellings the way a Hunk does.
	Pointer string

	// Attributed reports whether the rest of this row describes a change that was
	// actually read.
	//
	// False means the field's last write is older than the window, which is a
	// different statement from "it was never written" and from "nobody knows who
	// wrote it". It is a field of its own rather than a zero TS being tested,
	// because a consumer testing a timestamp for zero is a consumer that will one
	// day be handed a zero timestamp that means something else.
	Attributed bool

	// TS is when the last write happened, valid only when Attributed.
	TS time.Time

	// Actors are the field managers seen on the change that last wrote this path.
	// Empty is the honest answer for a change that recorded none, not a missing
	// one — see query.Change.Actors.
	Actors []string

	// UID is the incarnation the attributing change belongs to.
	UID string

	// ResourceVersion is that change's resourceVersion, which `-o wide` shows
	// because it is the value a reader takes to a controller's own logs.
	ResourceVersion string

	// EventType is that change's event type, carried for structured output.
	EventType string

	// Removed reports that the last write deleted the path, so the field is not
	// part of the object any more.
	Removed bool

	// Fields is how many of the object's fields this row stands for: one, unless
	// --depth collapsed a subtree into it.
	//
	// It is what keeps a collapsed row from reading as a single field: "spec.template
	// last changed at 14:03" is a much weaker statement when twelve fields sit
	// under it, and the count is how a reader sees that they do.
	Fields int
}

// BlameDocument is a rendered attribution table, ready to be written.
type BlameDocument struct {
	// Kind is the object's group and kind, "apps/Deployment", or the bare kind
	// for the core group.
	Kind string
	// Object is "namespace/name", or the bare name for a cluster-scoped kind.
	Object string
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// UID is the incarnation being attributed.
	UID string
	// Window is the pre-rendered window the attribution covers. It is part of the
	// answer rather than decoration: every BeforeWindow cell in the table is
	// relative to it.
	Window string
	// Base names the recorded row the replay started from, or says it could not
	// be established.
	Base string
	// Coverage is the pre-rendered coverage summary for the header.
	Coverage string
	// Rows are the fields, in the order they are to be displayed.
	Rows []BlameRow
	// Notices are written to standard error, in order.
	Notices []Notice
}

// header is the identity block this document opens with.
func (d BlameDocument) header() documentHeader {
	return documentHeader{
		Kind: d.Kind, Object: d.Object, Cluster: d.Cluster, UID: d.UID,
		Window: d.Window, Base: d.Base, Coverage: d.Coverage,
	}
}

// WriteBlame writes the table to out and its notices to errOut.
//
// The split is the one the whole CLI keeps: stdout is the data, stderr explains
// it. Both writes are checked, because a notice that did not arrive is a
// qualification the reader never saw.
func WriteBlame(out, errOut io.Writer, doc BlameDocument, opts Options) error {
	if out != nil {
		if _, err := io.WriteString(out, renderBlame(doc, opts)); err != nil {
			return fmt.Errorf("writing the attribution: %w", err)
		}
	}
	if errOut == nil || len(doc.Notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(doc.Notices, opts)); err != nil {
		return fmt.Errorf("writing the attribution's notices: %w", err)
	}
	return nil
}

// renderBlame builds the stdout half: the header, a blank line, and the table.
func renderBlame(doc BlameDocument, opts Options) string {
	p := palette{enabled: opts.Color}

	var built strings.Builder
	built.WriteString(renderHeader(doc.header(), p))
	if len(doc.Rows) == 0 {
		// No table, not an empty one — the choice every document in this package
		// makes. A heading row with nothing under it implies the question was
		// answered and the answer was none, and why there is nothing to attribute
		// is on stderr with every other qualification.
		return built.String()
	}
	built.WriteString("\n")
	built.WriteString(renderBlameTable(doc, opts, p))
	return built.String()
}

// renderBlameTable lays the fields out.
//
// The elastic column is the *first* one here, which is the one difference from
// every other table in this package and is forced by the acceptance criteria's
// column order: FIELD is both the widest content and the column a reader scans
// down, so it leads. Everything to its right is measured and FIELD takes what is
// left, elided through the same Elide the CHANGE column uses — which keeps the
// leaf, because the leaf is what identifies the field.
func renderBlameTable(doc BlameDocument, opts Options, p palette) string {
	showFields := collapsed(doc.Rows)

	headings := []string{columnField, columnLastChanged}
	if opts.Wide {
		headings = append(headings, columnRevision)
	}
	if showFields {
		headings = append(headings, columnFields)
	}
	headings = append(headings, columnActor)

	plain := make([][]string, 0, len(doc.Rows))
	widths := make([]int, len(headings))
	for i, heading := range headings {
		widths[i] = displayWidth(heading)
	}
	for _, row := range doc.Rows {
		cells := blameCells(row, showFields, opts)
		for i := range widths {
			widths[i] = max(widths[i], displayWidth(cells[i]))
		}
		plain = append(plain, cells)
	}

	widths[0] = fieldWidth(widths, opts)
	for i, cells := range plain {
		plain[i][0] = fieldCell(doc.Rows[i], cells[0], widths[0])
	}

	var built strings.Builder
	built.WriteString(p.dim(strings.TrimRight(headerLine(headings, widths[:len(widths)-1]), " ")) + "\n")
	for i, row := range doc.Rows {
		built.WriteString(strings.TrimRight(
			renderBlameRow(row, plain[i], headings, widths, p), " ") + "\n")
	}
	return built.String()
}

// fieldWidth is what is left for FIELD once every other column has what it needs.
//
// The floor is the one an elided path is allowed to shrink to anywhere else in
// this package: below it a path says nothing, and a table that had crushed its
// only identifying column to line up a timestamp would have spent the budget on
// the wrong thing. When the budget cannot cover the floor the line overflows,
// because a wrapped line is legible and a truncated one is not.
func fieldWidth(widths []int, opts Options) int {
	spent := 0
	for _, width := range widths[1:] {
		spent += width + len(gutter)
	}
	return max(min(widths[0], opts.width()-spent), minPathWidth)
}

// fieldCell fits one path into the FIELD column, keeping the removal marker.
//
// The marker is never the thing given up. A path elided to nothing still says
// something about the tail of a field; a row that lost its "(removed)" would say
// the field is present, which is the one reading this table must not offer.
func fieldCell(row BlameRow, cell string, width int) string {
	if !row.Removed {
		return Elide(cell, width)
	}
	suffix := gutter + RemovedMarker
	return Elide(row.Path, max(width-displayWidth(suffix), 1)) + suffix
}

// blameCells renders one row's columns, unpainted, so the layout can be measured.
func blameCells(row BlameRow, showFields bool, opts Options) []string {
	cells := []string{blameField(row), lastChangedCell(row, opts.Wide)}
	if opts.Wide {
		cells = append(cells, valueOrDash(row.ResourceVersion))
	}
	if showFields {
		cells = append(cells, fmt.Sprint(row.Fields))
	}
	return append(cells, blameActor(row))
}

// blameField is the FIELD cell before it is fitted to the column.
func blameField(row BlameRow) string {
	if row.Removed {
		return row.Path + gutter + RemovedMarker
	}
	return row.Path
}

// lastChangedCell is when the field was last written, or the statement that the
// write is older than the window.
func lastChangedCell(row BlameRow, wide bool) string {
	if !row.Attributed {
		return BeforeWindow
	}
	return formatTimestamp(row.TS, wide)
}

// blameActor names who made the last write.
//
// An unattributed row gets a dash rather than UnknownActor, and the difference is
// not cosmetic: UnknownActor says a change was read and recorded no field
// managers, while this row's change was never read at all. The LAST CHANGED cell
// beside it is what says which of the two a reader is looking at.
func blameActor(row BlameRow) string {
	if !row.Attributed {
		return valueOrDash("")
	}
	if len(row.Actors) == 0 {
		return UnknownActor
	}
	return strings.Join(row.Actors, ",")
}

// renderBlameRow lays one field out over the measured widths.
//
// Colour is applied here, after every width decision, for the reason it is
// everywhere else in this package: ANSI escapes carry no display width, and
// padding computed over painted cells is padding that includes the escape
// sequences.
func renderBlameRow(row BlameRow, cells, headings []string, widths []int, p palette) string {
	var built strings.Builder
	for i, cell := range cells {
		if i > 0 {
			built.WriteString(gutter)
		}
		built.WriteString(paintBlameCell(row, cell, headings[i], p))
		if i < len(cells)-1 {
			built.WriteString(strings.Repeat(" ", max(widths[i]-displayWidth(cell), 0)))
		}
	}
	return built.String()
}

// paintBlameCell applies the colour a cell earns.
//
// A removed field is red for the reason a remove hunk is: the vocabulary every
// diff tool has already taught everybody. The two cells that say something is
// *not* known — the window statement and the dash beside it — are dimmed so the
// eye reads them as the renderer speaking rather than as content.
func paintBlameCell(row BlameRow, cell, heading string, p palette) string {
	switch heading {
	case columnField:
		if row.Removed {
			return p.paint(ansiRed, cell)
		}
	case columnLastChanged:
		if !row.Attributed {
			return p.dim(cell)
		}
	case columnActor:
		if !row.Attributed || cell == UnknownActor {
			return p.dim(cell)
		}
	}
	return cell
}

// collapsed reports whether any row stands for more than one field, which is what
// gives the table its FIELDS column.
//
// It is asked of the rows rather than of the --depth flag, because a depth that
// happened to collapse nothing must not add a column of ones — and because a row
// that does collapse something has to carry its count whichever flag produced it.
func collapsed(rows []BlameRow) bool {
	for _, row := range rows {
		if row.Fields > 1 {
			return true
		}
	}
	return false
}
