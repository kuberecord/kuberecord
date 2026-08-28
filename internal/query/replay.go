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

package query

import (
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
)

// ReplayRow is one row of an incarnation's history, as a reconstruction reads
// it.
//
// It is the shape [Replay] consumes and nothing more: the columns a replay
// touches, named the way the schema names them. A backend materializes its own
// history into this and does not otherwise share a row type with the
// reconstruction, so the procedure below is written against one row shape rather
// than against whatever each backend happens to scan into.
type ReplayRow struct {
	// TS is when the change was recorded.
	TS time.Time
	// EventType is the row's event_type.
	EventType string
	// Data is the full recorded state, or empty on a diff-only row.
	Data string
	// Diff is the RFC 6902 patch from the previous state, or empty on a row that
	// carries no diff.
	Diff string
	// SHA256 is the digest recorded for the row.
	SHA256 string
}

// BaseRow returns the index of the row a replay starts from: the last row
// carrying full state. It returns -1 when the history holds none.
//
// The rule is separate from [Replay] because the two failures it distinguishes
// are a backend's to phrase. A history with no full-state row is not an absent
// object — it is a base that has aged out of the retention window — and the
// message that says so has to name the object and the instant in the vocabulary
// the backend answers questions in.
func BaseRow(history []ReplayRow) int {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Data != "" {
			return i
		}
	}
	return -1
}

// Replay runs steps 2 and 3 of the reconstruction: the base document, then every
// subsequent patch.
//
// This is the single definition of the reconstruction procedure specified in
// docs/SCHEMA.md. A backend implementing StateAt must use it rather than
// reimplement it: the two traps below are both quiet, and a second
// implementation is a second chance to get either of them wrong in a way no
// error surfaces and no reader of the output could detect.
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
func Replay(history []ReplayRow, base int) (*Reconstruction, error) {
	state := []byte(history[base].Data)
	applied := 0
	last := history[base]

	for _, row := range history[base+1:] {
		switch {
		case row.Data != "":
			state = []byte(row.Data)
		case row.Diff != "":
			patch, err := jsonpatch.DecodePatch([]byte(row.Diff))
			if err != nil {
				return nil, fmt.Errorf("decoding the patch recorded at %s: %w", formatInstant(row.TS), err)
			}
			next, err := patch.Apply(state)
			if err != nil {
				return nil, fmt.Errorf("applying the patch recorded at %s: %w", formatInstant(row.TS), err)
			}
			state = next
			applied++
		}
		last = row
	}

	var object map[string]any
	if err := json.Unmarshal(state, &object); err != nil {
		return nil, fmt.Errorf("decoding the state reconstructed for %s: %w", formatInstant(last.TS), err)
	}

	return &Reconstruction{
		Object:         object,
		BaseTS:         history[base].TS,
		BaseEvent:      history[base].EventType,
		PatchesApplied: applied,
		// The digest of the row the replay finished on, not of the base. It is what
		// turns "the replay ran without errors" into "the replay produced the state
		// that was recorded": canonicalize the object, hash it, and the two must
		// match. A mismatch is a chain-of-custody finding, not a rounding error.
		SHA256: last.SHA256,
	}, nil
}

// formatInstant renders a timestamp for an error message at the precision the
// schema records it.
func formatInstant(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
