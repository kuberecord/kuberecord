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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The prefix every layout fixture is written under, and the cluster it belongs to.
const (
	layoutPrefix  = "audit"
	layoutCluster = "prod-eu-1"
)

// layoutRoot is the records root those two produce.
func layoutRoot() string { return recordsRoot(layoutPrefix, layoutCluster) }

// hourPrefix and datePrefix spell an expectation the way the archive spells it, so a
// failure message can be pasted into a listing.
func hourPrefix(day string, hour int) string {
	return fmt.Sprintf("%s%s%s/%s%02d/", layoutRoot(), dateSegment, day, hourSegment, hour)
}

func datePrefix(day string) string {
	return fmt.Sprintf("%s%s%s/", layoutRoot(), dateSegment, day)
}

// TestPartitionPrefixesPrunesToTheWindow is the assertion that decides whether this
// backend is usable.
//
// A one-hour question against a ninety-day archive must list the partitions the hour
// falls in and *not one more*. The failure it guards against is not a wrong answer —
// an engine that listed everything would return the same changes — it is a query
// that takes four minutes instead of four seconds, which in evaluation mode is
// indistinguishable from the feature not working.
//
// The widening is asserted in both configurations, because both are real. With the
// default span the range is widened downward by an hour, since an object's partition
// comes from its first record and a record stamped 08:05 can live in the hour=07
// object (docs/SCHEMA.md). With the widening disabled — which a caller may do when it
// knows how the archive was written — the range is exactly the hours the window
// touches.
func TestPartitionPrefixesPrunesToTheWindow(t *testing.T) {
	t.Parallel()

	const day = "2026-03-14"
	instant := func(hour, minute int) time.Time {
		return time.Date(2026, 3, 14, hour, minute, 0, 0, time.UTC)
	}

	tests := []struct {
		name     string
		from, to time.Time
		span     time.Duration
		want     []string
	}{
		{
			name: "one hour, widening disabled: exactly the hours the window touches",
			from: instant(7, 15), to: instant(8, 15), span: 0,
			want: []string{hourPrefix(day, 7), hourPrefix(day, 8)},
		},
		{
			name: "one hour, default widening: one hour further down and no more",
			from: instant(7, 15), to: instant(8, 15), span: DefaultObjectSpan,
			want: []string{hourPrefix(day, 6), hourPrefix(day, 7), hourPrefix(day, 8)},
		},
		{
			name: "an instant: the hour it lands in",
			from: instant(7, 15), to: instant(7, 15), span: 0,
			want: []string{hourPrefix(day, 7)},
		},
		{
			name: "a whole day collapses to its date partition",
			from: time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2026, 3, 14, 23, 59, 59, 0, time.UTC),
			span: 0,
			want: []string{datePrefix(day)},
		},
		{
			name: "partial edges, whole middle",
			from: instant(22, 30),
			to:   time.Date(2026, 3, 16, 1, 30, 0, 0, time.UTC),
			span: 0,
			want: []string{
				hourPrefix("2026-03-14", 22), hourPrefix("2026-03-14", 23),
				datePrefix("2026-03-15"),
				hourPrefix("2026-03-16", 0), hourPrefix("2026-03-16", 1),
			},
		},
		{
			name: "the widening can cross a date boundary",
			from: time.Date(2026, 3, 14, 0, 10, 0, 0, time.UTC),
			to:   time.Date(2026, 3, 14, 0, 50, 0, 0, time.UTC),
			span: DefaultObjectSpan,
			want: []string{hourPrefix("2026-03-13", 23), hourPrefix("2026-03-14", 0)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := partitionPrefixes(layoutRoot(), tc.from, tc.to, tc.span)
			if !slices.Equal(got, tc.want) {
				t.Errorf("partitions:\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// TestPartitionPrefixesOverNinetyDaysStaysOnePerDay pins the other half of the
// pruning trade: a wide window must not become a listing per hour.
//
// Ninety days is 2,160 hour partitions and 90 date partitions holding exactly the
// same objects. Against a remote store the difference is 2,070 round trips in front
// of the first byte, which is why the whole-day collapse exists and why it is worth
// a test of its own rather than being left to the case above.
func TestPartitionPrefixesOverNinetyDaysStaysOnePerDay(t *testing.T) {
	t.Parallel()

	const days = 90
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, days).Add(-time.Second)

	got := partitionPrefixes(layoutRoot(), from, to, 0)
	if len(got) != days {
		t.Fatalf("a %d-day window resolved to %d prefixes, want one per day", days, len(got))
	}
	for _, prefix := range got {
		if strings.Contains(prefix, hourSegment) {
			t.Errorf("a fully covered day was listed hour by hour: %q", prefix)
		}
	}
	if got[0] != datePrefix("2026-01-01") || got[days-1] != datePrefix("2026-03-31") {
		t.Errorf("the range runs from %q to %q, want 2026-01-01 to 2026-03-31", got[0], got[days-1])
	}
}

// TestPartitionPrefixesAreDisjointAndOrdered guards the property that makes the
// prefixes safe to list independently.
//
// Two overlapping prefixes would list the same object twice, and a change rendered
// twice is a claim the cluster did something twice — which for an audit timeline is
// not a cosmetic defect. The ordering matters for a different reason: the prefixes
// are the order the archive is walked in, and a caller reporting progress reports it
// in that order.
func TestPartitionPrefixesAreDisjointAndOrdered(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 3, 14, 22, 30, 0, 0, time.UTC)
	to := time.Date(2026, 3, 18, 3, 15, 0, 0, time.UTC)

	got := partitionPrefixes(layoutRoot(), from, to, DefaultObjectSpan)
	if !slices.IsSorted(got) {
		t.Errorf("the prefixes are not in ascending order: %v", got)
	}
	for i, outer := range got {
		for j, inner := range got {
			if i != j && strings.HasPrefix(inner, outer) {
				t.Errorf("prefix %q contains %q, so their objects would be listed twice", outer, inner)
			}
		}
	}
}

// TestTimelineListsOnlyTheIntersectingPartitions asserts the pruning where it counts
// — through the engine, over an archive that spans ninety days.
//
// The case above pins the arithmetic; this one pins that the arithmetic is what the
// engine actually asks the archive. They are different failures: a correct range
// computed and then ignored looks exactly like no range at all.
func TestTimelineListsOnlyTheIntersectingPartitions(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 3, 14, 7, 15, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	tests := []struct {
		name string
		span time.Duration
		want []string
	}{
		{
			name: "widening disabled",
			span: NoObjectSpan,
			want: []string{hourPrefix("2026-03-14", 7), hourPrefix("2026-03-14", 8)},
		},
		{
			name: "default widening",
			span: 0,
			want: []string{
				hourPrefix("2026-03-14", 6), hourPrefix("2026-03-14", 7), hourPrefix("2026-03-14", 8),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// A ninety-day archive of empty partitions: the objects are beside the point,
			// the question is which prefixes are asked about.
			dir := t.TempDir()
			engine, spy := engineOverDir(t, dir, Options{Prefix: layoutPrefix, ObjectSpan: tc.span})

			ref := testRef()
			ref.ClusterID = layoutCluster
			drain(t, engine, query.TimelineQuery{Ref: ref, From: from, To: to})

			want := slices.Clone(tc.want)
			slices.Sort(want)
			if got := spy.listed(); !slices.Equal(got, want) {
				t.Errorf("a one-hour query against a ninety-day archive listed:\n got: %v\nwant: %v",
					got, want)
			}
			if opened := spy.opened(); len(opened) != 0 {
				t.Errorf("empty partitions were fetched from anyway: %v", opened)
			}
		})
	}
}

// TestDayPrefixesClipsToTheInstant covers the range a state reconstruction walks,
// which is a day at a time rather than a window.
//
// The instant's own day is wanted only up to the instant's hour: a record's partition
// is never later than the record, so the hours after it cannot hold anything at or
// before it. Every earlier day is wanted whole, and asking for it as one prefix
// rather than twenty-four is what keeps a thirty-day walk to thirty listings.
func TestDayPrefixesClipsToTheInstant(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 3, 14, 2, 30, 0, 0, time.UTC)

	tests := []struct {
		name string
		day  time.Time
		want []string
	}{
		{
			name: "the instant's own day stops at its hour",
			day:  time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC),
			want: []string{
				hourPrefix("2026-03-14", 0), hourPrefix("2026-03-14", 1), hourPrefix("2026-03-14", 2),
			},
		},
		{
			name: "an earlier day is asked for whole",
			day:  time.Date(2026, 3, 13, 0, 0, 0, 0, time.UTC),
			want: []string{datePrefix("2026-03-13")},
		},
		{
			name: "a day after the instant is not asked for at all",
			day:  time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := dayPrefixes(layoutRoot(), tc.day, at); !slices.Equal(got, tc.want) {
				t.Errorf("prefixes:\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// TestKeyRootsAndSuffix pins the two roots and the suffix filter, which are the whole
// of what separates a records query from a scope query in a store that holds both
// under one prefix.
func TestKeyRootsAndSuffix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"records root", recordsRoot("audit", "prod-eu-1"),
			"audit/format=jsonl-v1/cluster_id=prod-eu-1/"},
		{"records root, empty prefix", recordsRoot("", "prod-eu-1"),
			"format=jsonl-v1/cluster_id=prod-eu-1/"},
		{"scopes root", scopesRoot("audit"), "audit/format=jsonl-v1/scopes/"},
		{"scopes root, empty prefix", scopesRoot(""), "format=jsonl-v1/scopes/"},
		{"a prefix given with slashes is not doubled", recordsRoot("/audit/", "c"),
			"audit/format=jsonl-v1/cluster_id=c/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("got %q, want %q", tc.got, tc.want)
			}
		})
	}

	// The suffix filter is what keeps a health-probe object, or anything else that
	// shares the archive's prefix, from being handed to a frame decoder.
	for key, want := range map[string]bool{
		"audit/format=jsonl-v1/cluster_id=c/date=2026-03-14/hour=07/aaaa.jsonl.zst": true,
		"audit/.kuberecord-probe": false,
		"audit/format=jsonl-v1/cluster_id=c/date=2026-03-14/hour=07/aaaa.jsonl": false,
		"audit/notes.txt": false,
	} {
		if got := isObjectKey(key); got != want {
			t.Errorf("isObjectKey(%q) = %t, want %t", key, got, want)
		}
	}
}
