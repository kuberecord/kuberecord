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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// archiveOfKeys builds an archive holding exactly these keys and nothing else.
//
// The objects are empty, and that is the assertion as much as the fixture: the
// cluster partitions are in the *keys*, so an implementation that had to open an
// object to answer this question would fail here on a decode rather than pass
// slowly. Nothing else in this package can be tested with empty objects.
func archiveOfKeys(t *testing.T, keys ...string) (*Engine, *spySource) {
	t.Helper()

	dir := t.TempDir()
	for _, key := range keys {
		path := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("creating the parents of %q: %v", key, err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("writing %q: %v", key, err)
		}
	}
	return engineOverDir(t, dir, Options{})
}

// TestClusterIDsReadsThePartitionsAndNothingElse.
//
// The cases are the shapes a real bucket holds, and each one is a way the answer
// could be wrong rather than merely absent:
//
//   - several partitions per cluster, which is every archive older than an hour:
//     the answer is a set, not a list of what was listed.
//   - the scope log, which lives under the same format prefix but is partitioned
//     by date alone: reading its date= segment as a cluster would invent one.
//   - a foreign object in a shared bucket: not every key under a prefix is this
//     format's, and a bucket browser's marker object must not become a cluster.
//   - a sibling format version: an archive holding format=jsonl-v2 one day must be
//     read by whichever engine understands it, and this one must not wander in.
func TestClusterIDsReadsThePartitionsAndNothingElse(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want []string
	}{
		{
			name: "one cluster across several partitions",
			keys: []string{
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=10/b.jsonl.zst",
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-02/hour=00/c.jsonl.zst",
			},
			want: []string{"prod-eu-1"},
		},
		{
			name: "several clusters come back sorted and distinct",
			keys: []string{
				"format=jsonl-v1/cluster_id=prod-us-1/date=2026-03-01/hour=09/a.jsonl.zst",
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/b.jsonl.zst",
				"format=jsonl-v1/cluster_id=prod-us-1/date=2026-03-01/hour=10/c.jsonl.zst",
				"format=jsonl-v1/cluster_id=staging/date=2026-03-01/hour=10/d.jsonl.zst",
			},
			want: []string{"prod-eu-1", "prod-us-1", "staging"},
		},
		{
			name: "the scope log is not a cluster",
			keys: []string{
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
				"format=jsonl-v1/scopes/date=2026-03-01/s.jsonl.zst",
			},
			want: []string{"prod-eu-1"},
		},
		{
			name: "somebody else's object in a shared bucket is skipped",
			keys: []string{
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
				"format=jsonl-v1/NOTES.md",
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/browser-marker",
			},
			want: []string{"prod-eu-1"},
		},
		{
			name: "a sibling format version is left to the engine that understands it",
			keys: []string{
				"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
				"format=jsonl-v2/cluster_id=from-the-future/date=2026-03-01/hour=09/b.jsonl.zst",
			},
			want: []string{"prod-eu-1"},
		},
		{
			name: "an empty archive holds no clusters and no error",
			keys: nil,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine, _ := archiveOfKeys(t, tc.keys...)

			ids, err := engine.ClusterIDs(t.Context())
			if err != nil {
				t.Fatalf("ClusterIDs: %v", err)
			}
			if !slices.Equal(ids, tc.want) {
				t.Errorf("ClusterIDs = %v, want %v", ids, tc.want)
			}
		})
	}
}

// TestClusterIDsIsScopedToTheArchivesOwnPrefix.
//
// A prefix is chosen by whoever owns the bucket and may contain anything at all,
// including — quite legally — a directory called cluster_id=something. An
// implementation that searched the whole key for the segment rather than reading
// the one position the layout defines would report that as a recorded cluster, and
// the user would be offered a value that answers nothing.
func TestClusterIDsIsScopedToTheArchivesOwnPrefix(t *testing.T) {
	dir := t.TempDir()
	for _, key := range []string{
		"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
		// Outside the archive's format partition, and a decoy by name.
		"cluster_id=not-this-archive/date=2026-03-01/hour=09/b.jsonl.zst",
		"audit/cluster_id=not-this-archive-either/c.jsonl.zst",
	} {
		path := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("creating the parents of %q: %v", key, err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("writing %q: %v", key, err)
		}
	}

	engine, spy := engineOverDir(t, dir, Options{Prefix: "audit"})

	ids, err := engine.ClusterIDs(t.Context())
	if err != nil {
		t.Fatalf("ClusterIDs: %v", err)
	}
	if want := []string{"prod-eu-1"}; !slices.Equal(ids, want) {
		t.Errorf("ClusterIDs = %v, want %v", ids, want)
	}

	listed := spy.listed()
	if len(listed) != 1 || listed[0] != "audit/format=jsonl-v1/" {
		t.Errorf("listed %v, want exactly the archive's own format root: a walk that started higher "+
			"would read another archive's keys as this one's", listed)
	}
}

// TestClusterIDsOpensNothing: the answer is in the keys, and paying to decompress
// objects for it would make the fallback in a resolution chain cost more than the
// query it precedes.
func TestClusterIDsOpensNothing(t *testing.T) {
	engine, spy := archiveOfKeys(t,
		"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst",
		"format=jsonl-v1/cluster_id=prod-us-1/date=2026-03-01/hour=09/b.jsonl.zst")

	if _, err := engine.ClusterIDs(t.Context()); err != nil {
		t.Fatalf("ClusterIDs: %v", err)
	}
	if opened := spy.opened(); len(opened) != 0 {
		t.Errorf("ClusterIDs opened %v; the cluster partitions are in the keys", opened)
	}
}

// TestClusterIDsReportsAFailedListing.
//
// A listing that failed on its third page and was reported as a short answer would
// present a subset of the clusters as the set — and this list is rendered as the
// values a user may choose from, so an omission reads as proof that the cluster
// they are looking for is not here (Invariant 4).
func TestClusterIDsReportsAFailedListing(t *testing.T) {
	engine, spy := archiveOfKeys(t,
		"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst")
	spy.listErr = errors.New("the store said no")

	ids, err := engine.ClusterIDs(t.Context())
	if err == nil {
		t.Fatalf("a failed listing returned %v and no error", ids)
	}
	if !strings.Contains(err.Error(), "the store said no") {
		t.Errorf("the failure does not carry what went wrong: %v", err)
	}
	if !strings.Contains(err.Error(), formatPartition) {
		t.Errorf("the failure does not name what was being listed: %v", err)
	}
}

// TestClusterIDsRefusesAfterClose keeps a use-after-close a stated error rather
// than whatever the source happens to do with one.
func TestClusterIDsRefusesAfterClose(t *testing.T) {
	engine, _ := archiveOfKeys(t,
		"format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-01/hour=09/a.jsonl.zst")
	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := engine.ClusterIDs(t.Context()); err == nil {
		t.Error("ClusterIDs after Close returned no error")
	}
}
