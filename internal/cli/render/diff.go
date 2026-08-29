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

// The detail view, once the timeline has named a suspect.
//
// A timeline row has one column for the change and has to summarize five
// operations as "~5 ops". A diff has a whole page, so it spends it: each change
// opens with when it happened and who was seen on it, and every operation gets
// its path, the value it destroyed and the value it wrote, on lines of their own
// in the vocabulary every diff tool has already taught everybody.
//
// # Two things this renderer will not do
//
// It will not fill the terminal. A PodTemplate's container array or a
// CustomResourceDefinition's OpenAPI schema arrives as a single JSON value tens
// of kilobytes long, and a change that replaces one produces a value that would
// scroll a screen off the top by itself. Values are cut at MaxValueRunes and
// operations at MaxOpsPerChange, both with the count of what was withheld and the
// flag that shows it — a cut nobody is told about is a cut that reads as the
// whole answer.
//
// It will not present the redaction sentinel as a value. A stored "[REDACTED]" is
// what the archive holds, and rendered bare it is indistinguishable from a
// ConfigMap whose value genuinely is that string. It is marked instead, and dimmed
// so the eye does not read it as content.

// RedactionSentinel is the value a redaction policy wrote in place of a scrubbed
// one, as it is stored and therefore as it is read back.
//
// It is copied from internal/pipeline/redact.go rather than imported. D20 forbids
// this CLI from depending on the operator's runtime, and the depguard rule that
// enforces it is explicit that a copy naming its source is the accepted shape:
// the import itself is the coupling, not the constant. If that spelling ever
// changes, the write path stops producing this value and rows written afterwards
// stop being marked — which is why the source is named here rather than left to be
// found.
const RedactionSentinel = "[REDACTED]"

// redactionMarker is what follows a redacted value, so that nobody reads the
// sentinel as a literal string an object actually held.
//
// Two spaces before it because it is an annotation on the value rather than part
// of it, and the whole thing is dimmed for the same reason: it is the renderer
// speaking, not the archive.
const redactionMarker = "  (redacted by policy)"

// MaxValueRunes is how much of one value a hunk shows before cutting it.
//
// Two hundred is enough for every value an engineer reads at a glance — an image
// reference, a resource quantity, a node selector, a longish annotation — and
// short enough that a container array or an OpenAPI schema cannot take the
// screen. Beyond it the shape of the value is already lost, so more characters
// buy nothing that --full does not buy properly.
const MaxValueRunes = 200

// MaxOpsPerChange is how many operations of one change are shown before the rest
// are counted.
//
// Twenty because a change with more than that is a bulk rewrite — a re-apply, a
// controller rewriting a whole status — and the reader's next question is "what
// kind of change was this", which a count answers better than eighty lines do.
const MaxOpsPerChange = 20

// The indents. The op line sits under its change's header and the values sit
// under the op, so that the eye can find a path without reading the values and
// find a value without losing the path.
const (
	opIndent    = "  "
	valueIndent = "      "
)

// DiffDocument is a rendered diff, ready to be written.
//
// It carries the same identity block a timeline does — deliberately, since a diff
// read without knowing which incarnation of which object in which cluster it
// describes is a page of values attached to nothing.
type DiffDocument struct {
	// Kind is the object's group and kind, "apps/Deployment", or the bare kind
	// for the core group.
	Kind string
	// Object is "namespace/name", or the bare name for a cluster-scoped kind.
	Object string
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// UID is the incarnation being shown.
	UID string
	// Coverage is the pre-rendered coverage summary for the header.
	Coverage string
	// Changes are the recorded changes, in the order they are to be displayed.
	Changes []TimelineRow
	// Notices are written to standard error, in order.
	Notices []Notice
}

// header is the identity block this document opens with.
func (d DiffDocument) header() documentHeader {
	return documentHeader{
		Kind: d.Kind, Object: d.Object, Cluster: d.Cluster, UID: d.UID, Coverage: d.Coverage,
	}
}

// WriteDiff writes the document to out and its notices to errOut.
//
// The split is the one the whole CLI keeps: stdout is the data, stderr explains
// it. Both writes are checked, because a notice that did not arrive is a
// qualification the reader never saw.
func WriteDiff(out, errOut io.Writer, doc DiffDocument, opts Options) error {
	if out != nil {
		if _, err := io.WriteString(out, renderDiff(doc, opts)); err != nil {
			return fmt.Errorf("writing the diff: %w", err)
		}
	}
	if errOut == nil || len(doc.Notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(doc.Notices, opts)); err != nil {
		return fmt.Errorf("writing the diff's notices: %w", err)
	}
	return nil
}

// renderDiff builds the stdout half: the header, then one block per change.
func renderDiff(doc DiffDocument, opts Options) string {
	p := palette{enabled: opts.Color}

	var built strings.Builder
	built.WriteString(renderHeader(doc.header(), p))
	for _, change := range doc.Changes {
		// A blank line before each block rather than after, so the document never
		// ends in trailing whitespace and every block is separated from whatever
		// precedes it, header included.
		built.WriteString("\n")
		built.WriteString(renderChangeBlock(change, opts, p))
	}
	return built.String()
}

// renderChangeBlock renders one recorded change: when, what and who, then the
// operations beneath it.
func renderChangeBlock(row TimelineRow, opts Options, p palette) string {
	var built strings.Builder
	built.WriteString(changeHeaderLine(row, opts, p) + "\n")
	for _, line := range hunkLines(row, opts, p) {
		built.WriteString(line + "\n")
	}
	return built.String()
}

// changeHeaderLine names the instant, the kind of change and the actor.
//
// Attribution leads rather than trails because it is what a reader scans for: the
// question that brought them here is "who did this and when", and the operations
// are the answer to a question they only ask once they have found the block.
//
// -o wide adds the resource version, which is the value a reader takes to a
// controller's own logs to line an event up against a reconcile.
func changeHeaderLine(row TimelineRow, opts Options, p palette) string {
	parts := []string{
		diffTimestamp(row.Change.TS, opts.Wide),
		p.eventColor(row.Change.EventType),
	}
	if opts.Wide {
		parts = append(parts, "rv "+valueOrDash(row.Change.ResourceVersion))
	}

	actor := actorCell(row)
	if actor == UnknownActor {
		actor = p.dim(actor)
	}
	return strings.Join(append(parts, actor), gutter)
}

// diffTimestamp renders a change's instant for a header line.
//
// The narrow form says "UTC" in words because, unlike the table, a diff has no
// column heading to carry it — and a timestamp whose zone a reader has to assume
// is a timestamp two engineers will eventually disagree about.
func diffTimestamp(ts time.Time, wide bool) string {
	if wide {
		return formatTimestamp(ts, true)
	}
	return formatTimestamp(ts, false) + " UTC"
}

// hunkLines renders the body of one change.
//
// A change with no operations to show still produces a line. An empty block would
// read as a change that did nothing, and every one of these rows — a first
// sighting, a snapshot, a deletion, a patch that would not decode — is something
// happening.
func hunkLines(row TimelineRow, opts Options, p palette) []string {
	switch {
	case row.Change.EventType == query.EventDeleted:
		return []string{opIndent + describeDeleted}
	case row.PatchErr != "":
		return []string{opIndent + "unreadable patch: " + row.PatchErr}
	case len(row.Ops) == 0 && row.Change.Diff != "":
		return []string{opIndent + describeEmptyPatch}
	case len(row.Ops) == 0:
		return []string{opIndent + describeState(row.Change)}
	}

	shown := row.Ops
	withheld := 0
	if !opts.Full && len(shown) > MaxOpsPerChange {
		withheld = len(shown) - MaxOpsPerChange
		shown = shown[:MaxOpsPerChange]
	}

	lines := make([]string, 0, len(shown)*3+1)
	for _, op := range shown {
		lines = append(lines, hunk(op, opts, p)...)
	}
	if withheld > 0 {
		lines = append(lines, opIndent+p.dim(fmt.Sprintf(
			"%s and %d more %s (--full)", Ellipsis, withheld, plural(withheld, "operation"))))
	}
	return lines
}

// hunk renders one operation: its path, the value it destroyed, the value it
// wrote.
//
// The path line carries the operation's own glyph and colour — green for an add,
// red for a remove, yellow for a replace — so that the kind of change is legible
// before any value is read. The value lines then repeat the glyph, because a
// reader scanning only the values needs to know which way each one goes.
func hunk(op Op, opts Options, p palette) []string {
	path := DisplayPath(op.Path)
	if budget := opts.width() - displayWidth(opIndent) - 2; budget > 0 {
		path = Elide(path, budget)
	}

	lines := []string{opIndent + p.opColor(op.Type, glyph(op.Type)+" "+path)}
	switch op.Type {
	case OpAdd:
		lines = append(lines, valueLine(glyphAdd, valueText(op.Value), opts, p))
	case OpRemove:
		lines = append(lines, oldValueLine(op, opts, p))
	case OpReplace:
		lines = append(lines, oldValueLine(op, opts, p))
		lines = append(lines, valueLine(glyphAdd, valueText(op.Value), opts, p))
	default:
		// move and copy carry a source pointer rather than a value, and this
		// project records neither; naming the source is the only rendering of
		// them that is not an invention.
		if op.From != "" {
			return append(lines, valueIndent+p.dim("from "+DisplayPath(op.From)))
		}
		lines = append(lines, valueLine(glyphAdd, valueText(op.Value), opts, p))
	}
	return lines
}

// oldValueLine renders the value an operation destroyed, or says that the replay
// could not establish it.
//
// The absence is stated rather than left as a missing line, because a hunk with
// only a "+" reads as a field that had no value before — which is a claim about
// the object, and a different one from "the past could not be reconstructed". The
// notice on stderr says why; this keeps the shape of the hunk honest while the
// reader looks for it.
func oldValueLine(op Op, opts Options, p palette) string {
	if !op.OldKnown {
		return valueIndent + p.dim(glyphRemove+" (prior value not established)")
	}
	return valueLine(glyphRemove, valueOf(op.Old), opts, p)
}

// valueLine renders one value under its path, cut and marked if need be.
func valueLine(marker, value string, opts Options, p palette) string {
	rendered, redacted := fitValue(value, opts.Full)
	line := valueIndent + p.opColor(markerOp(marker), marker+" "+rendered)
	if redacted {
		line += p.dim(redactionMarker)
	}
	return line
}

// fitValue cuts a value to MaxValueRunes and reports whether it is the redaction
// sentinel.
//
// The count is of bytes rather than of the runes the cut was measured in, which
// is deliberate rather than an oversight: the number exists to tell a reader how
// much data is behind the marker, and bytes are what the archive holds and what
// `--full` will print. Runes are what the cut is measured in because everything
// in this package measures display in runes.
func fitValue(value string, full bool) (string, bool) {
	if value == RedactionSentinel {
		return value, true
	}
	if full || displayWidth(value) <= MaxValueRunes {
		return value, false
	}
	runes := []rune(value)
	head := string(runes[:MaxValueRunes])
	remainder := len(value) - len(head)
	return fmt.Sprintf("%s%s(%d more %s, --full)",
		head, Ellipsis, remainder, plural(remainder, "byte")), false
}

// markerOp maps a value line's marker back to the operation whose colour it
// takes, so that a "-" line is red under a replace exactly as it is under a
// remove.
func markerOp(marker string) string {
	if marker == glyphRemove {
		return OpRemove
	}
	return OpAdd
}

// opColor paints text in the colour an operation earns.
//
// The mapping is the one every diff tool uses and nobody has to be taught:
// something arrived is green, something went is red, something moved in place is
// yellow. An operation this project does not emit is left unpainted rather than
// coerced into a neighbour's colour, for the same reason glyph leaves it its own
// name.
func (p palette) opColor(opType, text string) string {
	switch opType {
	case OpAdd:
		return p.paint(ansiGreen, text)
	case OpRemove:
		return p.paint(ansiRed, text)
	case OpReplace:
		return p.paint(ansiYellow, text)
	}
	return text
}

// plural spells a count's noun, so a notice reads as a sentence rather than as a
// log line.
func plural(count int, noun string) string {
	if count == 1 {
		return noun
	}
	return noun + "s"
}
