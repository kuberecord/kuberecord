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

package objectsource

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// StateAt reconstructs what an object looked like at an instant.
//
// The procedure is the contract's, executed through the contract's own helpers:
// query.BaseRow finds the row to start from and query.Replay walks the patches after
// it. Neither is reimplemented here, and that is deliberate rather than tidy — the
// checkpoint trap is quiet. A checkpoint carries both a diff and the state that diff
// produced, and applying both yields a document the object was never in; for a
// replace operation the mistake is invisible, because a value set twice looks like a
// value set once. A second implementation of that rule is a second chance to get it
// wrong in a way no error surfaces and no reader of the output could detect.
//
// # Why it walks backwards a day at a time
//
// There is no index, and unlike a timeline the caller names an instant rather than a
// window — so the range to read has to be *discovered*. The walk goes back partition
// by partition from the instant until it holds a full-state row to replay from, which
// is usually the first day it looks in: the write path checkpoints, and a restart
// records a snapshot, so a base is rarely far behind. It walks by day rather than by
// hour because an hour-by-hour walk over a month would be seven hundred listings to
// answer one question.
//
// It continues one object span past the base it found, because an object's partition
// comes from its first record: a row between the base and the instant, or a *newer*
// full-state row, can sit in an earlier partition than the base does. Stopping the
// moment a base appeared would replay a history missing its own last patch.
//
// # Why the walk is bounded
//
// The instant carries no lower bound, so without one the answer to "this object was
// never recorded" would be a scan to the beginning of the archive. Options.
// StateLookback bounds it, and exhausting the bound is reported as exhausting the
// bound — never as the object being absent, which is a different fact and would end
// an investigation with the wrong answer.
func (e *Engine) StateAt(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if at.IsZero() {
		return nil, fmt.Errorf("reconstructing %s: the instant to reconstruct at is required; a zero "+
			"instant is not \"now\", it is the year 1", describeRef(ref))
	}

	collected, resolved, err := e.walkBack(ctx, ref, at, uid)
	if err != nil {
		return nil, fmt.Errorf("reconstructing %s at %s: %w", describeRef(ref), formatInstant(at), err)
	}

	history := replayRows(collected, resolved)
	if len(history) == 0 {
		// Either nothing at all was recorded for the object at or before the instant,
		// or nothing was recorded for the incarnation the caller pinned. Both are the
		// same answer — no state can be produced — and the message says which, because
		// a caller that pinned the wrong incarnation should not go looking for a
		// retention problem.
		return nil, fmt.Errorf("%s: %w", e.describeNothingFound(ref, at, uid), query.ErrObjectNotFound)
	}

	for _, row := range history {
		if row.EventType == query.EventDeleted {
			// This format never receives a deletion, so this is unreachable against an
			// archive this project wrote. It is here because the rule is terminal and
			// belongs to the procedure rather than to a backend's luck: an object
			// deleted before the instant did not exist at the instant, and finding a
			// base for it would answer a question about a thing that was gone.
			return nil, fmt.Errorf(
				"incarnation %s of %s was deleted at %s, so it did not exist at %s: %w",
				resolved, describeRef(ref), formatInstant(row.TS), formatInstant(at),
				query.ErrObjectNotFound)
		}
	}

	base := query.BaseRow(history)
	if base < 0 {
		// A different fact from absence, and the message has to say which. The archive
		// holds changes for this object but nothing to start a replay from, which means
		// the full-state row is older than the walk reached — not that the object was
		// never there. The sentinel is shared because what a caller does about it is
		// the same: report that no state can be produced, and never substitute a
		// neighbouring instant's.
		return nil, fmt.Errorf(
			"the %d recorded change(s) for incarnation %s of %s at or before %s hold no full-state row "+
				"to replay from, so its base is older than the %s this engine walks back rather than the "+
				"object being absent: %w",
			len(history), resolved, describeRef(ref), formatInstant(at), e.stateLookback,
			query.ErrObjectNotFound)
	}
	return query.Replay(history, base)
}

// walkBack reads partitions backwards from at until the collected history holds a
// settled base, or the lookback is exhausted.
//
// It returns everything it collected for the object at or before the instant, and
// the incarnation the reconstruction is about: the one the caller pinned, or the one
// owning the newest change — never a blend of two, since a (namespace, name) pair may
// span several UIDs and splicing them would reconstruct an object that never existed
// (Invariant 7).
func (e *Engine) walkBack(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) ([]query.Change, string, error) {
	root := recordsRoot(e.prefix, ref.ClusterID)
	earliest := dayStart(at.Add(-e.stateLookback))
	// The window is one-sided on purpose: the day being read supplies the lower
	// bound, and the instant supplies the upper one.
	scan := recordScan{ref: ref, to: at, retain: true}

	var collected []query.Change
	resolved := uid
	for day := dayStart(at); !day.Before(earliest); day = day.AddDate(0, 0, -1) {
		var changes []query.Change
		err := scanPartitions(ctx, e, dayPrefixes(root, day, at), scan.decode,
			func(acc *timelineAccumulator) { changes = append(changes, acc.changes...) })
		if err != nil {
			// A reconstruction cannot be partial. Replaying a history that is short by
			// an unread object does not fail: it produces the state the object was in
			// some time ago, presented as the state it was in at the instant asked
			// about.
			return nil, "", err
		}

		if len(changes) > 0 {
			collected = append(collected, changes...)
			slices.SortStableFunc(collected, byChangeTS)
			if uid == "" {
				// Recomputed on every pass rather than fixed the first time a change
				// appears: an object's partition can be earlier than its records, so a
				// day read later in the walk may still hold the newest change.
				resolved = collected[len(collected)-1].UID
			}
		}
		if e.baseIsSettled(day, collected, resolved) {
			break
		}
	}
	return collected, resolved, nil
}

// baseIsSettled reports whether the walk may stop after reading day.
//
// It may stop once it holds a full-state row *and* has read one object span below
// that row's own partition — because until then a newer full-state row, or a patch
// belonging between the base and the instant, could still be sitting in an earlier
// partition than the base is.
func (e *Engine) baseIsSettled(day time.Time, collected []query.Change, uid string) bool {
	history := replayRows(collected, uid)
	base := query.BaseRow(history)
	if base < 0 {
		return false
	}
	return day.Before(dayStart(history[base].TS.Add(-e.objectSpan)))
}

// replayRows narrows collected changes to one incarnation and renders them as the
// rows a replay consumes, oldest first.
func replayRows(changes []query.Change, uid string) []query.ReplayRow {
	rows := make([]query.ReplayRow, 0, len(changes))
	for _, c := range changes {
		if c.UID != uid {
			continue
		}
		rows = append(rows, query.ReplayRow{
			TS:        c.TS,
			EventType: c.EventType,
			Data:      c.Data,
			Diff:      c.Diff,
			SHA256:    c.SHA256,
		})
	}
	return rows
}

// describeNothingFound phrases the two ways a reconstruction can find no history,
// because the two send a reader in different directions.
func (e *Engine) describeNothingFound(ref query.ObjectRef, at time.Time, uid string) string {
	if uid != "" {
		return fmt.Sprintf(
			"the archive holds no change for incarnation %s of %s at or before %s, within the %s this "+
				"engine walks back", uid, describeRef(ref), formatInstant(at), e.stateLookback)
	}
	return fmt.Sprintf(
		"the archive holds no change for %s at or before %s, within the %s this engine walks back",
		describeRef(ref), formatInstant(at), e.stateLookback)
}
