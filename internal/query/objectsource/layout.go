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

// This file is the archive's key layout as the *reader* understands it, and the
// partition pruning built on top of it.
//
// The layout is spelled out here rather than imported from the writer that
// produces it, and that is a boundary rather than a duplication. The read plane
// may not depend on the operator's runtime (D20, and deps_test.go over the whole
// import closure), so the format is what the two halves share — a versioned public
// contract, documented once in docs/SCHEMA.md, whose reader and writer are
// deliberately independent accounts of it. A reader that asked the writer what the
// writer emits would agree with it by construction and would stop being able to
// detect that the format had changed.
//
//	<prefix>/format=jsonl-v1/cluster_id=<id>/date=<YYYY-MM-DD>/hour=<HH>/<hash>.jsonl.zst
//	<prefix>/format=jsonl-v1/scopes/date=<YYYY-MM-DD>/<hash>.jsonl.zst

package objectsource

import (
	"strings"
	"time"
)

// The layout's segments. Every one of them is part of the public contract of the
// object format and none may change meaning under jsonl-v1.
const (
	// formatPartition names the version of the contract these keys follow. It is
	// matched rather than assumed: an archive holding a future sibling format is
	// read by whichever engine understands that one, and this engine must not
	// wander into it.
	formatPartition = "format=jsonl-v1"

	// scopesPartition separates the scope log from the records under one prefix.
	// It sits inside format=jsonl-v1 because it is the same versioned contract, and
	// outside cluster_id= because it is partitioned by date alone.
	scopesPartition = "scopes"

	// clusterSegment, dateSegment and hourSegment are the Hive-style partition
	// keys. They are what make a time-window query a prefix listing rather than a
	// scan of the whole archive.
	clusterSegment = "cluster_id="
	dateSegment    = "date="
	hourSegment    = "hour="

	// objectSuffix is what every object in the format carries. Keys that do not
	// end in it are skipped rather than decoded: a bucket may hold a health-probe
	// object, a lifecycle marker or somebody's notes, and none of them is a frame
	// of records.
	objectSuffix = ".jsonl.zst"

	// dateLayout and hourLayout render the two time partitions. The hour is
	// zero-padded ("09", never "9"), which is what makes a lexicographic listing
	// also a chronological one.
	dateLayout = "2006-01-02"
	hourLayout = "15"
)

// hoursPerDay is spelled out because the whole-day shortcut below is a claim about
// how many hour partitions a date partition covers, and 24 appearing bare in an
// arithmetic expression is where that claim would go unnoticed.
const hoursPerDay = 24

// recordsRoot is the prefix every record object of one cluster hangs under.
func recordsRoot(prefix, clusterID string) string {
	return joinSegments(prefix, formatPartition, clusterSegment+clusterID) + "/"
}

// scopesRoot is the prefix the whole scope log hangs under.
//
// There is deliberately no cluster in it: the scope log is partitioned by date
// alone, and the cluster a transition belongs to is a field of the line rather
// than a segment of the key. A reader filters, it does not prune.
func scopesRoot(prefix string) string {
	return joinSegments(prefix, formatPartition, scopesPartition) + "/"
}

// joinSegments joins key segments with "/", dropping empty ones.
//
// The empty prefix is an ordinary configuration — a bucket dedicated to one
// archive — and contributes no segment. Joining it blindly would open every key
// with a "/", which an object store accepts and every reader then has to deal
// with forever.
func joinSegments(segments ...string) string {
	kept := make([]string, 0, len(segments))
	for _, s := range segments {
		if s != "" {
			kept = append(kept, strings.Trim(s, "/"))
		}
	}
	return strings.Join(kept, "/")
}

// isObjectKey reports whether a listed key names an object of this format.
func isObjectKey(key string) bool { return strings.HasSuffix(key, objectSuffix) }

// partitionPrefixes returns the prefixes to list in order to read [from, to],
// oldest first.
//
// # Why the range is widened downward and not upward
//
// An object's partition comes from its *first* record's timestamp and it keeps
// accepting records until it rotates, so a record stamped 10:00 can live in the
// hour=09 object. docs/SCHEMA.md states the consequence for every reader: a
// time-window scan must widen its partition range by the sink's maxObjectAge.
// span is that widening, and the caller supplies it (see Options.ObjectSpan).
//
// The upper bound needs none, because an object never holds a record from before
// its own first one — so a record's partition is never *later* than the record.
// Widening upward as well would be a symmetry that costs objects and buys nothing.
//
// # Why whole days collapse to a date prefix
//
// A day that is covered from hour 00 to hour 23 is listed as one date= prefix
// rather than as twenty-four hour= prefixes. The keys under it are exactly the
// same set, and it is the difference between 90 listings and 2,160 for a 90-day
// window — which against a remote store is 90 round trips instead of 2,160 before
// the first object is fetched. A partial day at either edge is still listed hour
// by hour, so a one-hour query touches one hour's objects and not a day's.
//
// The prefixes are pairwise disjoint by construction: a day contributes either its
// date prefix or a set of hour prefixes, never both. Overlapping prefixes would
// list an object twice and render a change twice, which for an audit timeline is a
// claim the cluster did something twice.
func partitionPrefixes(root string, from, to time.Time, span time.Duration) []string {
	start := hourStart(from.Add(-span))
	end := hourStart(to)

	var prefixes []string
	for cur := start; !cur.After(end); {
		day := dayStart(cur)
		lastHourOfDay := day.Add((hoursPerDay - 1) * time.Hour)
		if cur.Equal(day) && !lastHourOfDay.After(end) {
			prefixes = append(prefixes, root+dateSegment+day.Format(dateLayout)+"/")
			cur = day.AddDate(0, 0, 1)
			continue
		}
		prefixes = append(prefixes,
			root+dateSegment+cur.Format(dateLayout)+"/"+hourSegment+cur.Format(hourLayout)+"/")
		cur = cur.Add(time.Hour)
	}
	return prefixes
}

// hourStart and dayStart truncate an instant to its UTC hour and UTC day.
//
// They build the instant explicitly rather than using time.Truncate, which
// operates on the duration since the zero time and therefore only happens to
// align with midnight UTC. "Happens to" is not a property a partition boundary
// should rest on, and the explicit form is the one a reader can check against the
// key layout above.
func hourStart(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), u.Hour(), 0, 0, 0, time.UTC)
}

func dayStart(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// dayPrefixes returns the prefixes covering one UTC day, clipped to the hours at
// or before through.
//
// It exists for the backward walk a state reconstruction performs, which asks
// about a day at a time rather than about a window: the day the instant falls in
// is only wanted up to that instant's own hour, and every earlier day is wanted
// whole. Listing the later hours of the instant's day would fetch objects that
// cannot hold a record at or before it, since a record's partition is never later
// than the record.
func dayPrefixes(root string, day, through time.Time) []string {
	day = dayStart(day)
	last := hourStart(through)
	if last.Before(day) {
		return nil
	}
	if !last.Before(day.Add((hoursPerDay - 1) * time.Hour)) {
		return []string{root + dateSegment + day.Format(dateLayout) + "/"}
	}

	var prefixes []string
	for cur := day; !cur.After(last); cur = cur.Add(time.Hour) {
		prefixes = append(prefixes,
			root+dateSegment+cur.Format(dateLayout)+"/"+hourSegment+cur.Format(hourLayout)+"/")
	}
	return prefixes
}
