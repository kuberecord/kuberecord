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

	"github.com/kuberecord/kuberecord/internal/query"
)

// The watch-scope listing: what was being recorded, and when.
//
// This is the compliance view, and it is also the table every other command's
// empty result points a reader at. Its rows are periods, not events, and the two
// edges of a period mean different things: the start says the recorder began
// watching a scope, and the end says it stopped watching — emphatically not that
// the objects in it were deleted. Nothing in this rendering may blur that, which
// is why a still-open interval says so in a word rather than by leaving a cell
// blank.

// Column headings for the scope table. They are constants for the reason the
// timeline's are: the layout measures them, the golden files assert them, and a
// heading that drifted would silently change something people `awk` against.
const (
	columnScopeKind      = "KIND"
	columnScopeNamespace = "NAMESPACE"
	columnScopeFrom      = "FROM"
	columnScopeTo        = "TO"
	columnScopeRule      = "RULE"
)

// OpenInterval is what the TO cell says for a scope that is still being watched.
//
// A blank would read as an interval with no end recorded, which is a different
// and much weaker statement: this one says the recorder is watching the scope
// now, so an absence of changes after it is genuine silence rather than a gap in
// observation. It is exported because a test asserting the acceptance criterion
// has to name the thing it is asserting.
const OpenInterval = "(open)"

// AllNamespaces is what the NAMESPACE cell says for a scope that is not pinned to
// one.
//
// An empty namespace in a scope log is the all-namespaces scope itself rather
// than a wildcard or a missing value (see query.ScopeInterval), and a blank cell
// would read as the third thing. A cluster-scoped kind lands here too, which is
// correct for the same reason: every object of that kind was covered.
const AllNamespaces = "(all)"

// unrecordedRule is what the RULE cell says when an interval carries no rule
// reference.
//
// That happens when a recovery pass closed a scope whose rule no longer exists.
// It is a real state, and reporting it blank would read as a rule named by the
// empty string.
const unrecordedRule = "(not recorded)"

// ScopesDocument is a rendered scope listing, ready to be written.
type ScopesDocument struct {
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// Scope describes what was asked for — which kind, which namespace — in the
	// words the command used, so that an empty table is visibly an empty answer
	// to a specific question.
	Scope string
	// Window is the pre-rendered window the question was asked over.
	Window string
	// Intervals are the periods to list, oldest first as query.CoverageOf
	// returns them.
	Intervals []query.ScopeInterval
	// Notices are written to standard error, in order.
	Notices []Notice
}

// WriteScopes writes the listing to out and its notices to errOut.
//
// The split is the one the whole CLI keeps: stdout is the data, stderr explains
// it. Both writes are checked, because a notice that did not arrive is a
// qualification the reader never saw.
func WriteScopes(out, errOut io.Writer, doc ScopesDocument, opts Options) error {
	if out != nil {
		if _, err := io.WriteString(out, renderScopes(doc, opts)); err != nil {
			return fmt.Errorf("writing the watch scopes: %w", err)
		}
	}
	if errOut == nil || len(doc.Notices) == 0 {
		return nil
	}
	if _, err := io.WriteString(errOut, renderNotices(doc.Notices, opts)); err != nil {
		return fmt.Errorf("writing the watch scopes' notices: %w", err)
	}
	return nil
}

// renderScopes builds the stdout half: the header, a blank line, and the table.
func renderScopes(doc ScopesDocument, opts Options) string {
	p := palette{enabled: opts.Color}

	var built strings.Builder
	built.WriteString(renderScopeHeader(doc, p))
	if len(doc.Intervals) == 0 {
		// No table, not an empty one — the same choice renderTimeline makes. A
		// heading row with nothing under it implies the question was answered and
		// the answer was none, and for this command that is the one reading which
		// must never be offered without the explanation on stderr.
		return built.String()
	}
	built.WriteString("\n")
	built.WriteString(renderScopeTable(doc, opts, p))
	return built.String()
}

// renderScopeHeader states which cluster, which question and over what window.
//
// The window is part of the identity of the answer rather than decoration: an
// interval is kept when it *overlaps* the window and is then reported whole, so a
// reader has to know what was asked in order to read what came back.
func renderScopeHeader(doc ScopesDocument, p palette) string {
	fields := [][2]string{
		{"Cluster", doc.Cluster},
		{"Scope", doc.Scope},
		{"Window", doc.Window},
	}

	labelWidth := 0
	for _, field := range fields {
		labelWidth = max(labelWidth, displayWidth(field[0]))
	}

	var built strings.Builder
	for _, field := range fields {
		// Painted first and padded second, so the escape sequences stay out of the
		// width arithmetic that lines the values up.
		label := p.dim(field[0]+":") + strings.Repeat(" ", labelWidth-displayWidth(field[0])) + " "
		built.WriteString(label + field[1] + "\n")
	}
	return built.String()
}

// renderScopeTable lays the intervals out, giving every column but the last the
// width its content needs.
//
// The layout is computed over unpainted text and the colour applied afterwards,
// for the reason renderTable does it: ANSI escapes carry no display width, and
// padding computed over painted cells is padding that includes the escape
// sequences.
func renderScopeTable(doc ScopesDocument, opts Options, p palette) string {
	headings := []string{
		columnScopeKind, columnScopeNamespace, columnScopeFrom, columnScopeTo, columnScopeRule,
	}

	rows := make([][]string, 0, len(doc.Intervals))
	widths := make([]int, len(headings)-1)
	for i := range widths {
		widths[i] = displayWidth(headings[i])
	}
	for _, interval := range doc.Intervals {
		cells := scopeCells(interval, opts.Wide)
		for i := range widths {
			widths[i] = max(widths[i], displayWidth(cells[i]))
		}
		rows = append(rows, cells)
	}

	var built strings.Builder
	built.WriteString(p.dim(strings.TrimRight(headerLine(headings, widths), " ")) + "\n")
	for _, cells := range rows {
		built.WriteString(strings.TrimRight(renderScopeRow(cells, widths, p), " ") + "\n")
	}
	return built.String()
}

// renderScopeRow lays one interval out over the measured widths.
//
// The open marker is the one painted cell. It is the fact a reader is scanning
// for — which of these scopes is being recorded *now* — and it is the only cell
// whose meaning is not already spelled out by its own content.
func renderScopeRow(cells []string, widths []int, p palette) string {
	var built strings.Builder
	for i, cell := range cells {
		if i > 0 {
			built.WriteString(gutter)
		}
		painted := cell
		if cell == OpenInterval {
			painted = p.paint(ansiGreen, cell)
		}
		if i < len(widths) {
			built.WriteString(painted + strings.Repeat(" ", max(widths[i]-displayWidth(cell), 0)))
			continue
		}
		built.WriteString(painted)
	}
	return built.String()
}

// scopeCells renders one interval's five columns, unpainted.
func scopeCells(interval query.ScopeInterval, wide bool) []string {
	to := OpenInterval
	if interval.To != nil {
		to = formatTimestamp(*interval.To, wide)
	}
	return []string{
		ScopeKind(interval),
		ScopeNamespace(interval),
		formatTimestamp(interval.From, wide),
		to,
		ScopeRule(interval),
	}
}

// ScopeKind renders a scope's group and kind the way every header in this package
// renders a kind: "apps/Deployment", or the bare kind for the core group.
//
// It is exported because the command names the same scope in its notices, and two
// spellings of one kind in one invocation's output is how a reader ends up
// wondering whether they are two kinds.
func ScopeKind(interval query.ScopeInterval) string {
	if interval.APIGroup == "" {
		return interval.Kind
	}
	return interval.APIGroup + "/" + interval.Kind
}

// ScopeNamespace renders a scope's namespace, or says it covers all of them.
func ScopeNamespace(interval query.ScopeInterval) string {
	if interval.Namespace == "" {
		return AllNamespaces
	}
	return interval.Namespace
}

// ScopeRule names the rule that opened a scope, or reports that none is recorded.
func ScopeRule(interval query.ScopeInterval) string {
	if interval.RuleRef == "" {
		return unrecordedRule
	}
	return interval.RuleRef
}
