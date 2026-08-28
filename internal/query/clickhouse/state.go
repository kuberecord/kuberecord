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

package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"

	"github.com/kuberecord/kuberecord/internal/query"
)

// replayRow is one row of an incarnation's history, as the reconstruction reads
// it.
type replayRow struct {
	ts        time.Time
	eventType string
	data      string
	diff      string
	sha256    string
}

// StateAt reconstructs what an object looked like at an instant.
//
// It is the recipe published in docs/SCHEMA.md, executed step for step, and the
// correspondence is deliberate: the document is what a person reaches for when
// they want to check this answer by hand against clickhouse-client, and an
// implementation that took a cleverer route would make the two accounts of the
// same procedure impossible to compare.
//
//  1. Read the incarnation's history up to the instant, FINAL.
//  2. Take the last row with non-empty data as the base.
//  3. Replay the diff of every row after it, in ts order.
//  4. Stop at a deletion: it is terminal for its uid.
//
// Step 4 is applied first here, before the base is even looked for, because it is
// terminal whatever precedes it — an object deleted before the instant did not
// exist at the instant, and finding a base for it would be answering a question
// about a thing that was gone.
func (e *Engine) StateAt(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	if uid == "" {
		// An empty uid means the newest incarnation alive at or before the instant
		// — never a blend of two. A (namespace, name) pair may span several UIDs,
		// and splicing them would reconstruct an object that never existed
		// (Invariant 7).
		resolved, err := e.scanOneString(ctx, newestIncarnationAtStatement(ref, at))
		if err != nil {
			return nil, fmt.Errorf("finding the incarnation of %s alive at %s: %w",
				describeRef(ref), formatInstant(at), err)
		}
		if resolved == "" {
			return nil, fmt.Errorf("%s has no recorded rows at or before %s: %w",
				describeRef(ref), formatInstant(at), query.ErrObjectNotFound)
		}
		uid = resolved
	}

	history, err := e.replayHistory(ctx, ref, at, uid)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		return nil, fmt.Errorf("incarnation %s of %s has no recorded rows at or before %s: %w",
			uid, describeRef(ref), formatInstant(at), query.ErrObjectNotFound)
	}

	for _, row := range history {
		if row.eventType == query.EventDeleted {
			return nil, fmt.Errorf("incarnation %s of %s was deleted at %s, so it did not exist at %s: %w",
				uid, describeRef(ref), formatInstant(row.ts), formatInstant(at), query.ErrObjectNotFound)
		}
	}

	base := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].data != "" {
			base = i
			break
		}
	}
	if base < 0 {
		// A different fact from absence, and the message has to say which. History
		// holds rows for this object but nothing to start a replay from, which means
		// the base has aged out of the retention window — not that the object was
		// never there. The sentinel is shared because what a caller does about it is
		// the same: report that no state can be produced, and never substitute a
		// neighbouring instant's.
		return nil, fmt.Errorf(
			"history for incarnation %s of %s holds no full-state row at or before %s, so its base "+
				"predates the retention window rather than the object being absent: %w",
			uid, describeRef(ref), formatInstant(at), query.ErrObjectNotFound)
	}

	return replay(history, base)
}

// replayHistory runs step 1: one incarnation's rows up to the instant, oldest
// first.
func (e *Engine) replayHistory(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) (history []replayRow, err error) {
	stmt := replayStatement(ref, at, uid)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the history of %s: %w", describeRef(ref), err)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		var row replayRow
		if scanErr := rows.Scan(&row.ts, &row.eventType, &row.data, &row.diff, &row.sha256); scanErr != nil {
			return nil, fmt.Errorf("decoding a history row of %s: %w", describeRef(ref), scanErr)
		}
		history = append(history, row)
	}
	// A mid-stream failure is returned rather than treated as a short-but-complete
	// history. A replay over a truncated history does not fail — it produces a
	// state the object was in some time ago, presented as the state it was in at
	// the instant asked about.
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("streaming the history of %s: %w", describeRef(ref), rowsErr)
	}
	return history, nil
}

// replay runs steps 2 and 3: the base document, then every subsequent patch.
//
// The base row's own diff is skipped, and that is the trap docs/SCHEMA.md names
// explicitly. A Checkpoint carries both a diff and the state that diff produced;
// applying it on top of that state would produce a document the object was never
// in. The failure is quiet for a replace op — a value set twice looks like a value
// set once — which is why it is called out here rather than left to be inferred
// from the loop starting at base+1.
//
// A later full-state row *replaces* the document rather than patching it. That is
// the Modified-that-fell-back-to-full-state case, and treating it as a patch
// source would skip the row's content entirely.
func replay(history []replayRow, base int) (*query.Reconstruction, error) {
	state := []byte(history[base].data)
	applied := 0
	last := history[base]

	for _, row := range history[base+1:] {
		switch {
		case row.data != "":
			state = []byte(row.data)
		case row.diff != "":
			patch, err := jsonpatch.DecodePatch([]byte(row.diff))
			if err != nil {
				return nil, fmt.Errorf("decoding the patch recorded at %s: %w", formatInstant(row.ts), err)
			}
			next, err := patch.Apply(state)
			if err != nil {
				return nil, fmt.Errorf("applying the patch recorded at %s: %w", formatInstant(row.ts), err)
			}
			state = next
			applied++
		}
		last = row
	}

	var object map[string]any
	if err := json.Unmarshal(state, &object); err != nil {
		return nil, fmt.Errorf("decoding the state reconstructed for %s: %w", formatInstant(last.ts), err)
	}

	return &query.Reconstruction{
		Object:         object,
		BaseTS:         history[base].ts,
		BaseEvent:      history[base].eventType,
		PatchesApplied: applied,
		// The digest of the row the replay finished on, not of the base. It is what
		// turns "the replay ran without errors" into "the replay produced the state
		// that was recorded": canonicalize the object, hash it, and the two must
		// match. A mismatch is a chain-of-custody finding, not a rounding error.
		SHA256: last.sha256,
	}, nil
}

// formatInstant renders a timestamp for an error message at the precision the
// schema records it.
func formatInstant(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
