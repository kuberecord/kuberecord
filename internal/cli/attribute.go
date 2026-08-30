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

package cli

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Which change last wrote each field.
//
// The procedure is the acceptance criterion's: replay the window's patches in ts
// order and record, per JSON Pointer, the most recent write. What that sentence
// leaves out is everything that is not a patch, and each of those cases is a
// place a per-field answer can quietly become wrong:
//
//   - A row carrying full state and no patch — a first sighting, a snapshot, a
//     modification whose diff could not be produced — moved fields without saying
//     which. Attributing all of them to it would claim it touched fields it left
//     alone; attributing none would leave the fields it *did* move credited to
//     whoever wrote them last, which is the same lie told about a different
//     person. So the leaves of the new document are compared against the state
//     before it, and only the ones that differ are attributed.
//   - A checkpoint carries both a diff and the state that diff produced. Its diff
//     is the attribution and its data is the state; applying both would produce a
//     document the object was never in, exactly as query.Replay warns.
//   - A write to an interior node writes everything beneath it. Replacing
//     spec.template.spec.containers[0].resources attributes every limit under it,
//     so a leaf takes the newest write at its own pointer *or at any ancestor of
//     it* — not only an exact match, which would leave those limits looking
//     untouched since whenever they were last named individually. See lastWrite,
//     which also handles the mirror case: a field holding an empty object was
//     emptied by writes to its children.
//
// # Why the state is carried through the replay
//
// The field list is the object's, not the window's. Most of a fat object's fields
// were last written before any bounded window, and they are the rows that render
// as "(before window)" — which requires knowing they exist, which requires the
// state. The replay therefore maintains the document as well as the attribution
// map, and a failure to maintain it degrades the field list rather than the
// attribution (Invariant 5): patch paths are still writes even when the document
// they applied to could not be kept in step.

// fieldWrite is one recorded write of one JSON Pointer.
//
// It is the change's own columns, kept per pointer rather than per row, because
// the answer this file computes is a map from field to change and holding the
// whole row for each of a hundred fields would be holding one row a hundred times.
type fieldWrite struct {
	ts              time.Time
	actors          []string
	uid             string
	resourceVersion string
	eventType       string

	// removed reports that the write deleted the path rather than setting it.
	removed bool
}

// fieldEntry is one field the table will have a row for, before the depth
// collapsing and the ordering are applied.
type fieldEntry struct {
	pointer string
	// removed marks a field the window's last word on was a deletion, so it is
	// not part of the object any more.
	removed bool
}

// attribution is the outcome of replaying a consecutive run of history forward.
type attribution struct {
	// writes is the last write of every pointer the run touched.
	writes map[string]fieldWrite

	// state is the document as it stood after the last row replayed, or nil when
	// no state could be established at all.
	state []byte

	// stale reports that state stopped being maintained part way through, so the
	// field list it yields describes an earlier instant than the attribution does.
	stale bool

	// notices are the qualifications the replay produced, in order.
	notices []render.Notice
}

// attributeRun replays rows over seed and records the last write of every field.
//
// rows must be in ascending timestamp order — the order history happened in —
// and must be one incarnation's consecutive run. Both are the caller's to
// guarantee for the reason priorValues states: a replay walked backwards, or over
// a filtered slice, produces plausible nonsense.
//
// seed is the state as it stood immediately before the first row, or nil when
// none could be established. A nil seed is not a failure: the fields the window's
// own patches name are still attributable, and the caller says what was lost.
func attributeRun(seed []byte, rows []render.TimelineRow) attribution {
	result := attribution{writes: map[string]fieldWrite{}, state: seed}

	for _, row := range rows {
		if row.Change.EventType == query.EventKubernetes {
			// Every field of such a row describes the Event object rather than the
			// object whose timeline it was merged into, so attributing anything from
			// it would be splicing two objects' histories together.
			continue
		}
		if row.Change.EventType == query.EventDeleted {
			result.notices = append(result.notices, render.Notice{
				Text: fmt.Sprintf("this incarnation was deleted at %s, so the fields below are what it "+
					"held immediately before the deletion rather than what it holds now",
					render.FormatInstant(row.Change.TS)),
				Warning: true,
			})
			return result
		}
		result.record(row)
	}
	return result
}

// record attributes one row and advances the state past it.
func (a *attribution) record(row render.TimelineRow) {
	write := fieldWrite{
		ts:              row.Change.TS,
		actors:          row.Change.Actors,
		uid:             row.Change.UID,
		resourceVersion: row.Change.ResourceVersion,
		eventType:       row.Change.EventType,
	}

	switch {
	case row.PatchErr != "":
		// The row is a change that happened and whose fields cannot be named. Saying
		// so is the whole of what can be done honestly: attributing nothing leaves
		// those fields credited to an earlier writer, and the notice is what stops a
		// reader believing that credit.
		a.notices = append(a.notices, render.Notice{
			Text: fmt.Sprintf("the patch recorded at %s could not be decoded (%s), so the fields it "+
				"moved are still attributed to whatever wrote them before it",
				render.FormatInstant(row.Change.TS), row.PatchErr),
			Warning: true,
		})
	case len(row.Ops) > 0:
		for _, op := range row.Ops {
			operation := write
			operation.removed = op.Type == render.OpRemove
			a.writes[op.Path] = operation
		}
	case row.Change.Data != "":
		// A full-state row with no patch to read: which fields it moved has to be
		// worked out by comparison. See the file's opening comment.
		a.recordFullState(row.Change.Data, write)
	}

	a.advance(row)
}

// recordFullState attributes the leaves a full-state row changed, added or
// dropped.
func (a *attribution) recordFullState(data string, write fieldWrite) {
	var next any
	if err := json.Unmarshal([]byte(data), &next); err != nil {
		// Recorded state is JSON by construction, so this is handled rather than
		// asserted; advance will report the same failure once, where a reader can
		// act on it.
		return
	}
	var previous any
	if len(a.state) > 0 {
		if err := json.Unmarshal(a.state, &previous); err != nil {
			previous = nil
		}
	}

	for _, pointer := range leafPointers(next) {
		before, ok := valueAtPointer(previous, pointer)
		if !ok || !reflect.DeepEqual(before, mustValueAt(next, pointer)) {
			a.writes[pointer] = write
		}
	}
	for _, pointer := range leafPointers(previous) {
		if _, ok := valueAtPointer(next, pointer); !ok {
			gone := write
			gone.removed = true
			a.writes[pointer] = gone
		}
	}
}

// advance moves the replayed document past one row.
//
// A row carrying full state replaces the document rather than patching it, which
// is both the checkpoint rule and the modification-that-fell-back-to-full-state
// case: treating either as a patch source would produce a state the object was
// never in.
//
// A patch that will not apply stops the document rather than the command. The
// attribution after it is still sound — a patch names the paths it writes whether
// or not it applies — so what is lost is the field list, and that is what the
// notice says.
func (a *attribution) advance(row render.TimelineRow) {
	if row.Change.Data != "" {
		a.state, a.stale = []byte(row.Change.Data), false
		return
	}
	if a.state == nil || a.stale || row.Change.Diff == "" {
		return
	}

	patch, err := jsonpatch.DecodePatch([]byte(row.Change.Diff))
	if err == nil {
		var next []byte
		if next, err = patch.Apply(a.state); err == nil {
			a.state = next
			return
		}
	}
	a.stale = true
	a.notices = append(a.notices, render.Notice{
		Text: fmt.Sprintf("the fields listed below are the ones the object held at %s: the patch "+
			"recorded there did not apply to the reconstructed state (%v), so anything added or "+
			"removed after it is missing from the list. The attribution of the fields that are "+
			"listed is unaffected", render.FormatInstant(row.Change.TS), err),
		Warning: true,
	})
}

// blameRows turns the attribution into the rows a table or an envelope renders.
//
// fields narrows to the paths asked for and depth collapses what is left, in that
// order: collapsing first would decide which rows exist from paths the reader
// never asked about, and a --field under the collapse depth would then select
// nothing.
func (a attribution) blameRows(fields []string, depth int) []render.BlameRow {
	entries := a.entries()
	rows := make([]render.BlameRow, 0, len(entries))
	index := map[string]int{}

	for _, entry := range entries {
		if !query.MatchesFieldPath(entry.pointer, fields) {
			continue
		}
		key := collapseTo(entry.pointer, depth)
		at, seen := index[key]
		if !seen {
			index[key] = len(rows)
			rows = append(rows, render.BlameRow{
				Path: render.DisplayPath(key), Pointer: key, Removed: entry.removed, Fields: 1,
			})
			at = len(rows) - 1
		} else {
			rows[at].Fields++
			// A collapsed row claims a removal only when everything under it is
			// gone. Anything else would report a subtree as deleted because one
			// field of it was.
			rows[at].Removed = rows[at].Removed && entry.removed
		}
		a.applyWrite(&rows[at], entry.pointer)
	}

	slices.SortFunc(rows, compareBlameRows)
	return rows
}

// applyWrite folds one field's last write into the row that stands for it.
//
// The newest wins, which for a collapsed row is what makes its timestamp mean
// "the last time anything under here moved". A tie is broken towards the write
// already held, so that two operations of one change — which share a timestamp to
// the nanosecond — cannot make the row depend on map iteration order.
func (a attribution) applyWrite(row *render.BlameRow, pointer string) {
	write, found := a.lastWrite(pointer)
	if !found || (row.Attributed && !write.ts.After(row.TS)) {
		return
	}
	row.Attributed = true
	row.TS = write.ts
	row.Actors = write.actors
	row.UID = write.uid
	row.ResourceVersion = write.resourceVersion
	row.EventType = write.eventType
}

// lastWrite is the newest write at a pointer, at any of its ancestors, or at
// anything beneath it.
//
// The ancestors are consulted because writing a subtree writes everything in it: a
// replace of an object's whole `resources` block is the change that last set the
// memory limit inside it, and a lookup matching only the exact pointer would report
// that limit as untouched since whenever it was last named on its own.
//
// The descendants are consulted for the mirror case, which is rarer and reads far
// worse when it is wrong. A field holding an empty object or an empty array is a
// leaf and therefore a row, and it can have become empty inside the window — the
// writes that emptied it are its children. Without this it would render as
// "(before window)" beside the removals that produced it, which is the table
// contradicting itself.
//
// A tie is broken towards the shallower pointer, so that two operations of one
// change — which share a timestamp to the nanosecond — cannot make the row depend
// on map iteration order.
func (a attribution) lastWrite(pointer string) (fieldWrite, bool) {
	var (
		newest fieldWrite
		at     string
		found  bool
	)
	better := func(candidate fieldWrite, candidateAt string) bool {
		switch {
		case !found, candidate.ts.After(newest.ts):
			return true
		case candidate.ts.Equal(newest.ts):
			return comparePointers(candidateAt, at) < 0
		}
		return false
	}

	for ancestor := pointer; ; ancestor = parentPointer(ancestor) {
		if write, ok := a.writes[ancestor]; ok && better(write, ancestor) {
			newest, at, found = write, ancestor, true
		}
		if ancestor == "" {
			break
		}
	}

	prefix := pointer + "/"
	for descendant, write := range a.writes {
		if strings.HasPrefix(descendant, prefix) && better(write, descendant) {
			newest, at, found = write, descendant, true
		}
	}
	return newest, found
}

// entries is every field the table will speak about: the leaves of the object as
// it stood at the end of the run, plus every path the run wrote that is not one of
// them.
//
// The second half is two cases at once. A path the run *removed* is not in the end
// state and is exactly what somebody asking who deleted a field came for. And when
// no state could be established at all, the paths the run's own patches name are
// the whole of what can be answered — the object's other fields are what was lost,
// and answering for the ones that survive is the degradation Invariant 5 asks for
// rather than an empty page.
//
// A path is skipped when it still resolves in the end state, because the leaves
// beneath it are already rows and listing the interior node as well would report
// one change twice. It is skipped when an ancestor of it was removed, because that
// ancestor's row already says the whole subtree went.
func (a attribution) entries() []fieldEntry {
	var document any
	known := false
	if len(a.state) > 0 {
		known = json.Unmarshal(a.state, &document) == nil
	}
	if !known {
		document = nil
	}

	present := leafPointers(document)
	entries := make([]fieldEntry, 0, len(present)+len(a.writes))
	for _, pointer := range present {
		entries = append(entries, fieldEntry{pointer: pointer})
	}

	written := make([]string, 0, len(a.writes))
	for pointer := range a.writes {
		if a.removedAncestor(pointer) {
			continue
		}
		if _, stillThere := valueAtPointer(document, pointer); stillThere {
			continue
		}
		written = append(written, pointer)
	}
	// Sorted because a map's iteration order is not one: two runs of the same
	// question must produce the same table, and the ordering below only breaks
	// ties that this list would otherwise decide at random.
	slices.Sort(written)
	for _, pointer := range written {
		// With a state in hand, a path that does not resolve in it is gone whatever
		// the operation that last wrote it was called: an ancestor replaced by a
		// value without that subtree in it removes the field just as a remove does.
		// With no state, the operation's own name is all there is to go on.
		entries = append(entries, fieldEntry{pointer: pointer, removed: known || a.writes[pointer].removed})
	}
	return entries
}

// removedAncestor reports whether a strict ancestor of pointer was itself removed.
func (a attribution) removedAncestor(pointer string) bool {
	for at := parentPointer(pointer); at != ""; at = parentPointer(at) {
		if write, ok := a.writes[at]; ok && write.removed {
			return true
		}
	}
	write, ok := a.writes[""]
	return ok && write.removed
}

// compareBlameRows is the table's order: most recently written first.
//
// Recency leads because the question that brings somebody to this command is
// which field moved last and who moved it, and because it puts the fields whose
// last write predates the window — the ones there is least to say about — at the
// bottom rather than interleaved through the answer.
//
// The tie-break is the path, so that the several fields one change wrote land in a
// readable order rather than in whatever order the object happened to serialize.
func compareBlameRows(a, b render.BlameRow) int {
	switch {
	case a.Attributed && b.Attributed && !a.TS.Equal(b.TS):
		return b.TS.Compare(a.TS)
	case a.Attributed != b.Attributed:
		if a.Attributed {
			return -1
		}
		return 1
	}
	return comparePointers(a.Pointer, b.Pointer)
}

// comparePointers orders two pointers segment by segment, comparing array indices
// as numbers.
//
// Lexicographic ordering of the whole string would put containers[10] between
// containers[1] and containers[2], which reads as data in no order at all. The
// numeric comparison is applied only where both segments are numeric, so a member
// name that happens to be digits still sorts as text against a name that is not.
func comparePointers(a, b string) int {
	left, right := pointerTokens(a), pointerTokens(b)
	for i := range min(len(left), len(right)) {
		if left[i] == right[i] {
			continue
		}
		leftIndex, leftErr := strconv.Atoi(left[i])
		rightIndex, rightErr := strconv.Atoi(right[i])
		if leftErr == nil && rightErr == nil && leftIndex != rightIndex {
			return leftIndex - rightIndex
		}
		return strings.Compare(left[i], right[i])
	}
	return len(left) - len(right)
}

// collapseTo truncates a pointer to at most depth tokens, which is what --depth
// does to a fat object.
//
// Depth is counted in JSON Pointer tokens, so an array index is a level of its
// own: --depth 4 collapses a container array into one row and --depth 5 gives a
// row per container. That is the object's real structure rather than the display
// grammar's, and it is the only counting that does not mis-measure a key
// containing dots — an annotation named deployment.kubernetes.io/revision is one
// level, not three.
func collapseTo(pointer string, depth int) string {
	if depth <= 0 {
		return pointer
	}
	tokens := pointerTokens(pointer)
	if len(tokens) <= depth {
		return pointer
	}
	return "/" + strings.Join(tokens[:depth], "/")
}

// pointerTokens splits an RFC 6901 pointer into its tokens, still escaped.
//
// Still escaped because the tokens are rejoined into a pointer that render.
// DisplayPath then unescapes: unescaping here and rejoining would turn a member
// named "a/b" into two levels, which is the same silent segment split the escape
// order exists to prevent.
func pointerTokens(pointer string) []string {
	if pointer == "" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(pointer, "/"), "/")
}

// parentPointer is the pointer to the node one level up, or the empty pointer at
// the root.
func parentPointer(pointer string) string {
	if cut := strings.LastIndex(pointer, "/"); cut > 0 {
		return pointer[:cut]
	}
	return ""
}

// leafPointers is the pointer of every leaf of a decoded document.
//
// A leaf is anything that is not a populated object or array, so an empty object
// and an empty array are leaves: they are values a field holds, and a field
// holding one is a field somebody set. Recursing past them would produce no
// pointer at all and the field would vanish from the table.
func leafPointers(document any) []string {
	var (
		pointers []string
		walk     func(node any, prefix string)
	)
	walk = func(node any, prefix string) {
		switch value := node.(type) {
		case map[string]any:
			if len(value) == 0 {
				break
			}
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			// Sorted so that the table is the same table on every run: a map's
			// iteration order is deliberately not one, and the ordering applied
			// later only breaks ties this walk would otherwise decide at random.
			slices.Sort(keys)
			for _, key := range keys {
				walk(value[key], prefix+"/"+escapeToken(key))
			}
			return
		case []any:
			if len(value) == 0 {
				break
			}
			for i, element := range value {
				walk(element, prefix+"/"+strconv.Itoa(i))
			}
			return
		}
		pointers = append(pointers, prefix)
	}

	if document == nil {
		return nil
	}
	walk(document, "")
	return pointers
}

// escapeToken writes one member name as an RFC 6901 token.
//
// The two escapes are applied in the order the RFC mandates — ~ first, then / —
// which is the reverse of the order they are undone in. Escaping the slash first
// would then re-escape the tilde it had just written, and a member named "a/b"
// would round-trip into one named "a~1b".
func escapeToken(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
}

// mustValueAt reads a pointer that was produced by walking the document it is
// being read from.
//
// The lookup cannot fail — the pointer came from leafPointers over this exact
// document — and a nil for a genuinely absent field would compare equal to a
// recorded JSON null. Returning the value alone keeps the comparison above
// readable; the pointer's provenance is what makes that safe.
func mustValueAt(document any, pointer string) any {
	value, _ := valueAtPointer(document, pointer)
	return value
}
