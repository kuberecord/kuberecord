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
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// The CHANGE column is the reason the release exists, and this file decides what
// goes in it.
//
// # Why the old value is not in the patch
//
// A recorded diff is whatever internal/pipeline's ComputeDiff emitted, which is
// wI2L/jsondiff under its default options. That library's Operation type has an
// OldValue field tagged `json:"-"`, so what reaches storage is the RFC 6902
// triple of op, path and the *new* value and nothing else. "2Gi → 512Mi"
// therefore cannot be read off a row; the value on the left comes from replaying
// the object's state up to that row, which the command does once per incarnation
// and hands back through Op.Old.
//
// When that replay could not run — the base aged out of the retention window, or
// the backend cannot reconstruct state — Op.OldKnown is false and the cell
// renders the new value alone. It does not render a blank, a dash or a guess:
// the notice on stderr says the arrow is missing because the past could not be
// established, which is a different statement from "the field had no value".

// The glyphs an operation is rendered with.
//
// They are single characters because the column they share is the one under
// pressure, and because the three that matter — something appeared, something
// went, something changed — are the vocabulary diff tools have already taught
// everybody who will read this output.
const (
	glyphAdd     = "+"
	glyphRemove  = "-"
	glyphReplace = "~"
)

// The RFC 6902 operation names. Only these three are ever recorded by this
// project (see internal/pipeline.ComputeDiff, which passes no Factorize or
// Invertible option), and the others are handled anyway: the patch column is
// data read back from storage, and a renderer that panicked or blanked on an
// operation it did not expect would lose an audit row to a format change.
//
// They are exported because the command decides which operations are worth
// reconstructing a prior value for, and a second spelling of "replace" in that
// decision is a spelling that eventually disagrees with this one.
const (
	OpAdd     = "add"
	OpRemove  = "remove"
	OpReplace = "replace"
)

// The CHANGE cell of a row that carries no patch.
//
// Each one says what the row *is* rather than leaving the cell empty, because an
// empty cell in an audit timeline reads as "nothing happened here" and every one
// of these rows is something happening.
const (
	describeFullState  = "full state recorded"
	describeSnapshot   = "full state recorded (snapshot)"
	describeCheckpoint = "full state recorded (checkpoint)"
	describeNoPatch    = "full state recorded (no patch produced)"
	describeDeleted    = "object deleted"
	describeNoDetail   = "no state and no patch recorded"
	describeEmptyPatch = "patch recorded with no operations"
)

// Op is one operation of a recorded patch, decoded for rendering.
//
// Old and OldKnown are filled in by the caller rather than decoded from the
// patch, for the reason at the top of this file. They are two fields and not one
// nil-able value because a JSON null is a value an object really can hold, and
// collapsing "the field was null" into "we do not know what the field was" would
// be exactly the fabrication Invariant 4 forbids.
type Op struct {
	// Type is the RFC 6902 operation name, as recorded.
	Type string
	// Path is the RFC 6901 pointer, as recorded. DisplayPath renders it.
	Path string
	// From is the source pointer of a move or copy, empty otherwise.
	From string
	// Value is the operation's new value, absent on a remove.
	Value json.RawMessage
	// Old is the value the operation replaced or removed, when a replay
	// established it.
	Old any
	// OldKnown reports whether Old is an answer rather than an absence.
	OldKnown bool
}

// PatchOps decodes a recorded diff column into the operations it holds.
//
// The caller keeps the error rather than this package swallowing it, because an
// undecodable patch is a fact about the row that belongs in the output: the row
// still renders, saying that its patch could not be read, instead of vanishing
// or pretending to be a change with no operations in it.
func PatchOps(diff string) ([]Op, error) {
	if strings.TrimSpace(diff) == "" {
		return nil, nil
	}
	var wire []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		From  string          `json:"from"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal([]byte(diff), &wire); err != nil {
		return nil, fmt.Errorf("decoding the recorded patch: %w", err)
	}
	ops := make([]Op, 0, len(wire))
	for _, entry := range wire {
		ops = append(ops, Op{Type: entry.Op, Path: entry.Path, From: entry.From, Value: entry.Value})
	}
	return ops, nil
}

// glyph is the marker an operation is rendered with.
//
// An operation this project does not emit keeps its own name rather than
// borrowing a glyph: "move" rendered as "~" would claim a value changed in place
// when it moved, and the row would be quietly wrong rather than visibly unusual.
func glyph(opType string) string {
	switch opType {
	case OpAdd:
		return glyphAdd
	case OpRemove:
		return glyphRemove
	case OpReplace:
		return glyphReplace
	}
	return opType
}

// opText renders one operation into at most width columns, or without limit when
// width is zero.
//
// The fitting order is the priority order: the operation's glyph and the leaf of
// its path are never given up, the interior of the path goes first, and only then
// do the values shorten — old before new, because the reader is looking at the
// row to find out what the field became.
//
// # Why nothing here is painted
//
// This is the one cell whose *contents* are laid out rather than padded, and an
// ANSI escape carries no display width. A dimmed arrow inside it would be counted
// by the arithmetic above and not by the terminal, so a coloured run would elide
// paths harder than an uncoloured one and the two would disagree about what the
// row says. Colour that survives is applied to whole cells after their widths are
// settled — see paintCell — and the arrow does without.
func opText(op Op, width int) string {
	prefix := glyph(op.Type) + " "
	path := DisplayPath(op.Path)
	values := opValues(op)

	if width <= 0 {
		return prefix + path + valueSuffix(values)
	}

	budget := width - displayWidth(prefix) - displayWidth(valueSuffix(values))
	if budget < displayWidth(path) {
		path = Elide(path, max(budget, minPathWidth))
	}

	remaining := width - displayWidth(prefix) - displayWidth(path) - displayWidth(": ")
	if values != "" && remaining < displayWidth(values) {
		values = fitValues(op, remaining)
	}
	return truncate(prefix+path+valueSuffix(values), width)
}

// valueSuffix attaches the separator only when there is something to separate.
//
// A remove whose prior value could not be established renders as "- spec.foo",
// and a trailing ": " on it would read as a field set to the empty string.
func valueSuffix(values string) string {
	if values == "" {
		return ""
	}
	return ": " + values
}

// opValues renders the value half of an operation, unfitted.
func opValues(op Op) string {
	newValue := valueText(op.Value)
	switch op.Type {
	case OpAdd:
		return newValue
	case OpRemove:
		if op.OldKnown {
			return valueOf(op.Old)
		}
		return ""
	case OpReplace:
		if op.OldKnown {
			return valueOf(op.Old) + " " + Arrow + " " + newValue
		}
		return newValue
	}
	// move and copy carry a source rather than a value, and this project records
	// neither; naming the source is the only rendering of them that is not a lie.
	if op.From != "" {
		return DisplayPath(op.From)
	}
	return newValue
}

// fitValues shortens an operation's values to avail columns.
//
// Old and new are shortened towards each other rather than the pair being
// truncated at its tail, because a tail truncation of "2Gi → 512Mi" removes the
// new value — the half the reader came for — and leaves the old one intact.
func fitValues(op Op, avail int) string {
	if avail <= 0 {
		return ""
	}
	newValue := valueText(op.Value)
	if op.Type != OpReplace || !op.OldKnown {
		return truncate(opValues(op), avail)
	}

	oldValue := valueOf(op.Old)
	separator := " " + Arrow + " "
	remaining := avail - displayWidth(separator)
	if remaining < 2 {
		// No room for both halves and the arrow between them. The new value alone
		// is the more useful of the two.
		return truncate(newValue, avail)
	}

	oldBudget := min(displayWidth(oldValue), remaining/2)
	newBudget := remaining - oldBudget
	if newBudget > displayWidth(newValue) {
		newBudget = displayWidth(newValue)
		oldBudget = min(displayWidth(oldValue), remaining-newBudget)
	}
	return truncate(oldValue, oldBudget) + " " + Arrow + " " + truncate(newValue, newBudget)
}

// valueOf renders a value the caller established by replaying state.
//
// It accepts `any` because that is what falls out of decoding a recorded
// document, and it routes back through valueText so that a value read from state
// and a value read from a patch render identically. Two renderings of the same
// JSON in one column would read as two different values.
func valueOf(value any) string {
	if raw, ok := value.(json.RawMessage); ok {
		return valueText(raw)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		// Unreachable for anything encoding/json produced, and handled rather
		// than asserted: a value that will not marshal is still a value the row
		// changed, and %v is a worse rendering of it than no row at all is.
		return fmt.Sprintf("%v", value)
	}
	return valueText(encoded)
}

// valueText renders a JSON value as one line.
//
// A string loses its quotes, because "2Gi" reads worse than 2Gi in a column of
// Kubernetes field values and nothing here is being fed back to a parser.
// Everything else is compacted onto one line: a container spec spread over
// fifteen lines would take the table with it.
func valueText(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		// Not a failure worth reporting: the bytes came out of a patch this
		// process is rendering rather than validating, and showing them as they
		// stand is more informative than an error in a table cell.
		return collapseWhitespace(string(raw))
	}
	if text, ok := decoded.(string); ok {
		return collapseWhitespace(text)
	}
	compacted, err := json.Marshal(decoded)
	if err != nil {
		return collapseWhitespace(string(raw))
	}
	return collapseWhitespace(string(compacted))
}

// collapseWhitespace flattens a value onto one line.
//
// Tabs and newlines inside a recorded string — a ConfigMap's contents, an
// annotation holding YAML — would otherwise break the table's alignment for
// every row after them, which is a rendering failure that looks like data
// corruption.
func collapseWhitespace(text string) string {
	if !strings.ContainsAny(text, "\n\r\t") {
		return text
	}
	replacer := strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ", "\t", " ")
	return strings.Join(strings.Fields(replacer.Replace(text)), " ")
}

// multiOpSummary is how a patch with more than one operation is summarized.
//
// The count and not the operations, because a row is a row: five operations
// rendered into one cell would either elide all five into uselessness or push
// the table past any terminal. --full is the way to see them, and the count is
// what tells a reader the flag is worth typing.
func multiOpSummary(count int) string {
	return glyphReplace + strconv.Itoa(count) + " ops"
}
