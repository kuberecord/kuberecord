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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Where "2Gi → 512Mi" comes from.
//
// The value on the right is in the row: an RFC 6902 operation carries the value
// it wrote. The value on the left is not, and cannot be — internal/pipeline
// records what wI2L/jsondiff emits, and that library's OldValue field is tagged
// `json:"-"`. So the past has to be reconstructed, and this file is where.
//
// # One reconstruction, not one per row
//
// The rows a timeline renders are a consecutive run of one incarnation's
// history: the window and the limit both cut it at the ends, never out of the
// middle. So a single StateAt just before the oldest row shown, followed by
// replaying the patches forward in memory, establishes the value every operation
// replaced — at one round trip rather than one per row. A hundred rows of a
// hundred reconstructions is what makes an interactive command feel broken.
//
// The replay is skipped entirely when the rows *are* not consecutive, which is
// what an actor or field-path predicate makes them. Applying only the surviving
// patches to a real base state would produce a document the object was never in,
// and reading old values out of it would put confident, wrong numbers in an audit
// timeline — far worse than the arrow being absent. When that happens the arrow
// is dropped and a notice says why (Invariants 4 and 5).
//
// # What is not replayed
//
// Merged Kubernetes Event rows are skipped. Every field of such a row describes
// the Event object rather than the object whose timeline it was merged into (see
// query.EventKubernetes), so applying its patch to the target's state would be
// splicing two objects' histories together.
//
// A checkpoint's own patch is skipped too, exactly as query.Replay skips it: a
// checkpoint carries both a diff and the state that diff produced, and applying
// both yields a document that never existed. The failure is silent for a replace
// operation — a value set twice looks like a value set once — which is why the
// rule is restated here rather than left to be inferred.

// priorValues fills Op.Old on every operation whose replaced value a replay can
// establish, and reports the notices that filling it produced.
//
// rows must be in ascending timestamp order — the order history happened in,
// which is not necessarily the order it is displayed in. The caller re-sorts a
// copy rather than this function taking a display-ordered slice and guessing,
// because a replay walked backwards is a replay that produces plausible nonsense.
func priorValues(
	ctx context.Context, engine query.QueryEngine, ref query.ObjectRef, rows []render.TimelineRow,
) []render.Notice {
	var notices []render.Notice
	for _, group := range byIncarnation(rows) {
		if notice, ok := replayGroup(ctx, engine, ref, group); ok {
			notices = append(notices, notice)
		}
	}
	return notices
}

// byIncarnation splits the rows into one consecutive run per UID.
//
// Per UID because a replay across a delete-and-recreate is exactly the splice
// Invariant 7 forbids: the new object's first patch does not apply to the old
// object's last state, and if it happened to apply, the value it reported as
// "before" would belong to a different object with the same name.
//
// Kubernetes Event rows carry the Event's own UID and are dropped here rather
// than grouped, so they never seed or advance anybody's state.
func byIncarnation(rows []render.TimelineRow) [][]render.TimelineRow {
	order := make([]string, 0, 2)
	groups := map[string][]render.TimelineRow{}
	for _, row := range rows {
		if row.Change.EventType == query.EventKubernetes {
			continue
		}
		uid := row.Change.UID
		if _, seen := groups[uid]; !seen {
			order = append(order, uid)
		}
		groups[uid] = append(groups[uid], row)
	}

	grouped := make([][]render.TimelineRow, 0, len(order))
	for _, uid := range order {
		grouped = append(grouped, groups[uid])
	}
	return grouped
}

// replayGroup establishes the prior values for one incarnation's run of rows,
// reporting a notice when it could not.
//
// The boolean says whether the notice is worth printing, rather than the notice
// being compared against a zero value: "no prior values were needed" and "prior
// values could not be established" are different outcomes and only the second is
// news.
func replayGroup(
	ctx context.Context, engine query.QueryEngine, ref query.ObjectRef, rows []render.TimelineRow,
) (render.Notice, bool) {
	first := firstNeedingPriorValue(rows)
	if first < 0 {
		return render.Notice{}, false
	}

	state, start, err := seedState(ctx, engine, ref, rows, first)
	if err != nil {
		return render.Notice{
			Text: fmt.Sprintf("prior values are not shown: %s. The new value of each change "+
				"is still exact; only the value it replaced is missing", err),
			Warning: true,
		}, true
	}

	for _, row := range rows[start:] {
		if state != nil {
			fillOldValues(state, row.Ops)
		}
		next, advanceErr := advanceState(state, row.Change)
		if advanceErr != nil {
			return render.Notice{
				Text: fmt.Sprintf("prior values stop at %s: %s. Rows after it show the new value only",
					render.FormatInstant(row.Change.TS), advanceErr),
				Warning: true,
			}, true
		}
		state = next
	}
	return render.Notice{}, false
}

// firstNeedingPriorValue returns the index of the earliest row holding an
// operation whose replaced value is worth recovering, or -1 for none.
//
// An add writes a value that was not there, so it needs nothing. A replace and a
// remove both destroy one, and both are worth the round trip: knowing a limit
// went from 2Gi to 512Mi, or that a container's env entry held DEBUG=1 before it
// was removed, is the whole point of asking.
func firstNeedingPriorValue(rows []render.TimelineRow) int {
	for i, row := range rows {
		if slices.ContainsFunc(row.Ops, needsPriorValue) {
			return i
		}
	}
	return -1
}

// needsPriorValue reports whether an operation destroyed a value.
func needsPriorValue(op render.Op) bool {
	return op.Type == render.OpReplace || op.Type == render.OpRemove
}

// seedState finds the document to start replaying from, and the row to start at.
//
// It prefers a full-state row already in the result over a round trip, which is
// the ordinary case for a timeline reaching back to a first sighting or a
// checkpoint: the object's whole state is sitting in the rows the command has
// already paid for, and asking the backend to reconstruct what it just handed
// over would be spending a query on arithmetic.
func seedState(
	ctx context.Context, engine query.QueryEngine, ref query.ObjectRef, rows []render.TimelineRow, first int,
) ([]byte, int, error) {
	for i := first - 1; i >= 0; i-- {
		if data := rows[i].Change.Data; data != "" {
			// Start after the seeding row: its own prior values are not needed
			// (nothing before `first` needs any) and its state is the document
			// every later row is replayed over.
			return []byte(data), i + 1, nil
		}
	}

	// The instant is one nanosecond before the oldest row so that the
	// reconstruction is the state that row acted on, rather than the state it
	// produced. The schema records nanoseconds, so this is the smallest
	// representable step and not an approximation.
	at := rows[0].Change.TS.Add(-time.Nanosecond)
	reconstruction, err := engine.StateAt(ctx, ref, at, rows[0].Change.UID)
	if err != nil {
		return nil, 0, describeStateFailure(err)
	}
	encoded, err := json.Marshal(reconstruction.Object)
	if err != nil {
		return nil, 0, fmt.Errorf("the reconstructed state could not be re-encoded: %w", err)
	}
	return encoded, 0, nil
}

// describeStateFailure turns a reconstruction failure into the half-sentence a
// notice reads best with.
//
// The three cases lead a reader somewhere different, which is why they are not
// collapsed: a backend that cannot reconstruct at all is a property of the tool,
// a base that aged out is a property of retention, and anything else is a failure
// worth seeing in full.
func describeStateFailure(err error) error {
	switch {
	case errors.Is(err, query.ErrCapabilityUnsupported):
		return errors.New("this backend cannot reconstruct state")
	case errors.Is(err, query.ErrObjectNotFound):
		return errors.New("no full-state row survives before the oldest change shown, " +
			"so there is nothing to replay from")
	}
	return err
}

// advanceState moves the replay past one row.
//
// A row carrying full state replaces the document rather than patching it, which
// is both the checkpoint rule and the Modified-that-fell-back-to-full-state case.
// Treating either as a patch source would produce a state the object was never
// in.
func advanceState(state []byte, change query.Change) ([]byte, error) {
	if change.Data != "" {
		return []byte(change.Data), nil
	}
	if state == nil || change.Diff == "" {
		return state, nil
	}
	patch, err := jsonpatch.DecodePatch([]byte(change.Diff))
	if err != nil {
		return nil, fmt.Errorf("the patch recorded there could not be decoded: %w", err)
	}
	next, err := patch.Apply(state)
	if err != nil {
		return nil, fmt.Errorf("the patch recorded there did not apply to the reconstructed state: %w", err)
	}
	return next, nil
}

// fillOldValues reads the value each operation is about to destroy out of the
// state as it stands before the operation runs.
//
// A pointer that does not resolve leaves OldKnown false rather than recording a
// nil: an operation whose path is absent from the state means the replay and the
// history disagree, and inventing a null for it would put a value in the output
// that the object never held.
func fillOldValues(state []byte, ops []render.Op) {
	if !slices.ContainsFunc(ops, needsPriorValue) {
		return
	}
	var document any
	if err := json.Unmarshal(state, &document); err != nil {
		// The state came from a reconstruction or from a recorded data column,
		// both of which are JSON by construction. Handled rather than asserted,
		// and handled by leaving the arrows off this row.
		return
	}
	for i := range ops {
		if !needsPriorValue(ops[i]) {
			continue
		}
		if value, ok := valueAtPointer(document, ops[i].Path); ok {
			ops[i].Old, ops[i].OldKnown = value, true
		}
	}
}

// valueAtPointer resolves an RFC 6901 pointer against a decoded document.
//
// The two escapes are undone in the order the RFC mandates — ~1 then ~0 —
// because reversing them turns the encoding of a literal "~1" into a slash and
// silently splits one segment into two, which would resolve to nothing and be
// reported as a value the state did not hold.
func valueAtPointer(document any, pointer string) (any, bool) {
	if pointer == "" {
		return document, true
	}
	current := document
	for segment := range strings.SplitSeq(strings.TrimPrefix(pointer, "/"), "/") {
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")

		switch node := current.(type) {
		case map[string]any:
			value, ok := node[segment]
			if !ok {
				return nil, false
			}
			current = value
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				// Includes RFC 6901's "-", which addresses the position past the
				// end of an array: a place a value can be written to and never a
				// place one can be read from.
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}
