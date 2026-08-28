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
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// archiveKeys is a fixture in the layout the writer actually produces, because the
// properties under test are about that layout: partitions pruned by prefix, keys
// that sort chronologically because they sort lexicographically, and a scopes/
// tree beside the records rather than under a cluster.
//
// Two dates, two hours on one of them, and a second cluster — which is what makes
// "list one hour" a question with a wrong answer available.
var archiveKeys = []string{
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/aaaa.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/bbbb.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=08/cccc.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-15/hour=00/dddd.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=staging/date=2026-03-14/hour=07/eeee.jsonl.zst",
	"audit/format=jsonl-v1/scopes/date=2026-03-14/ffff.jsonl.zst",
}

// TestLocalListOrderIsWholeKeyByteOrder is the assertion the local source exists to
// satisfy: it must hand back keys in the order an object store would.
//
// The fixture is chosen for the case a filename sort gets wrong. A directory "a"
// and a sibling file "a-x" sort a, a-x by name — but the keys they produce sort
// a-x before a/b, because '-' is 0x2D and '/' is 0x2F. Every engine built on this
// seam walks a window in time order without collecting it first, so an order that
// is nearly right is an engine that emits changes nearly in order.
func TestLocalListOrderIsWholeKeyByteOrder(t *testing.T) {
	t.Parallel()

	keys := []string{
		"a-x", "ab", "a/b", "a/c", "a/b-1/x", "a0", "a/.hidden", "z", "b/c/d/e",
	}
	dir := writeArchive(t, keys)
	src := openLocal(t, dir)

	got := listKeys(t, src, "")

	want := slices.Clone(keys)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("listing order is not whole-key byte order:\n got: %v\nwant: %v", got, want)
	}
}

// TestLocalListPrefixIsABytedPrefixNotAPath pins the other half of the fidelity
// claim. An object store's prefix is a byte prefix of the key; a local source that
// treated it as a directory path would answer a different question from the one the
// engine's partition pruning asks, and every test written against this source would
// then prove nothing about a bucket.
func TestLocalListPrefixIsABytedPrefixNotAPath(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	src := openLocal(t, dir)

	cases := []struct {
		name   string
		prefix string
		want   []string
	}{
		{
			name:   "empty lists the whole archive",
			prefix: "",
			want:   archiveKeys,
		},
		{
			name:   "a directory prefix with its separator",
			prefix: "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/",
			want:   archiveKeys[0:3],
		},
		{
			name:   "a directory prefix without its separator",
			prefix: "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14",
			want:   archiveKeys[0:3],
		},
		{
			// The case that separates the two readings: a partial partition name is a
			// legal prefix and matches two days, which no directory walk would find.
			name:   "a partial partition name",
			prefix: "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-1",
			want:   archiveKeys[0:4],
		},
		{
			name:   "a partial name inside a partition",
			prefix: "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=0",
			want:   archiveKeys[0:3],
		},
		{
			name:   "a partial object name",
			prefix: "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/aa",
			want:   archiveKeys[0:1],
		},
		{
			name:   "a whole key",
			prefix: archiveKeys[3],
			want:   archiveKeys[3:4],
		},
		{
			name:   "the scopes tree, which sits beside the clusters",
			prefix: "audit/format=jsonl-v1/scopes/",
			want:   archiveKeys[5:6],
		},
		{
			// Not an error. "There is nothing here" and "the archive could not be read"
			// are different answers and a caller has to be able to tell them apart.
			name:   "a prefix nothing matches",
			prefix: "audit/format=jsonl-v1/cluster_id=nowhere/",
			want:   nil,
		},
		{
			// Keys never carry a leading slash, so nothing can match one. It is an empty
			// listing rather than a refusal, which is what a bucket would answer too.
			name:   "an absolute-looking prefix",
			prefix: "/audit/",
			want:   nil,
		},
		{
			name:   "a prefix that climbs out of the archive",
			prefix: "../",
			want:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := listKeys(t, src, tc.prefix)
			if !slices.Equal(got, tc.want) {
				t.Errorf("List(%q):\n got: %v\nwant: %v", tc.prefix, got, tc.want)
			}
		})
	}
}

// TestLocalListReportsSizes covers the half of Object that a scan estimate is built
// from. An estimate that is presented to a human before a ninety-day query has to be
// derived from the listing alone — opening the objects to find out how big they are
// is the cost the estimate exists to warn about.
func TestLocalListReportsSizes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "a/one", strings.Repeat("x", 11))
	writeFile(t, dir, "a/two", "")
	src := openLocal(t, dir)

	objects := listObjects(t, src, "")
	want := []Object{{Key: "a/one", Size: 11}, {Key: "a/two", Size: 0}}
	if !slices.Equal(objects, want) {
		t.Errorf("listing:\n got: %v\nwant: %v", objects, want)
	}
}

// TestLocalListPrunesUnrelatedDirectories is the difference between a usable
// backend and an unusable one: a one-hour query against a ninety-day archive must
// not read eighty-nine other days.
//
// It is asserted without instrumenting the walk, by making the directories that must
// not be read impossible to read. If the walk descends into one, the listing fails;
// if it prunes them, the listing succeeds — and the control case at the end shows the
// trap is armed rather than the assertion being vacuous.
func TestLocalListPrunesUnrelatedDirectories(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	unreadable := filepath.Join(dir,
		"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-15")
	makeUnreadable(t, unreadable)
	src := openLocal(t, dir)

	wanted := "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/"
	if got := listKeys(t, src, wanted); !slices.Equal(got, archiveKeys[0:2]) {
		t.Errorf("listing one hour read a partition it did not need:\n got: %v\nwant: %v",
			got, archiveKeys[0:2])
	}

	// The control. Without it, a walk that pruned everything would pass the
	// assertion above for the wrong reason.
	it := src.List(t.Context(), "")
	defer func() { _ = it.Close() }()
	for it.Next() { //nolint:revive // draining is the point; the error is the assertion
	}
	if it.Err() == nil {
		t.Error("listing the whole archive did not fail, so the unreadable partition " +
			"was not actually unreadable and the pruning assertion proves nothing")
	}
}

// TestLocalListSkipsWhatAnObjectStoreCannotHold keeps the walk to regular files in
// real directories.
//
// A symlink has no analogue in a bucket: following one to a directory would make a
// listing depend on link topology and need not terminate, and emitting one as an
// object would hand out a key Open then has to refuse. Skipping is the documented
// behaviour, and it is documented rather than silent precisely because it is a gap.
func TestLocalListSkipsWhatAnObjectStoreCannotHold(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "real/object", "payload")
	if err := os.Symlink(filepath.Join(dir, "real/object"), filepath.Join(dir, "link-to-file")); err != nil {
		t.Skipf("this filesystem does not support symlinks: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "link-to-dir")); err != nil {
		t.Fatalf("symlink to a directory: %v", err)
	}
	src := openLocal(t, dir)

	if got := listKeys(t, src, ""); !slices.Equal(got, []string{"real/object"}) {
		t.Errorf("listing followed something an object store cannot hold: %v", got)
	}
}

// TestLocalListSkipsAnObjectThatChangesMidScan covers the ordinary case in an
// archive that is doing its job: an object aged out by a lifecycle rule between the
// moment its directory was read and the moment the walk reached it.
//
// An object store's listing would simply not have contained it, so failing a scan
// over it would make a correctly-configured retention policy look like a broken
// archive. The second case is the same window used differently — the entry is still
// there but is no longer a regular file — and it is why the walk checks what it is
// twice rather than trusting the directory read.
func TestLocalListSkipsAnObjectThatChangesMidScan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		disturb func(t *testing.T, path string)
	}{
		{
			name: "the object is removed",
			disturb: func(t *testing.T, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove the object mid-scan: %v", err)
				}
			},
		},
		{
			name: "the object becomes a symlink",
			disturb: func(t *testing.T, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove the object mid-scan: %v", err)
				}
				if err := os.Symlink(path+"-elsewhere", path); err != nil {
					t.Skipf("this filesystem does not support symlinks: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir, "p/a", "one")
			writeFile(t, dir, "p/b", "two")
			writeFile(t, dir, "p/c", "three")
			src := openLocal(t, dir)

			it := src.List(t.Context(), "")
			defer func() { _ = it.Close() }()

			if !it.Next() || it.Object().Key != "p/a" {
				t.Fatalf("first object: %v (err %v)", it.Object(), it.Err())
			}
			// The directory has been read by now, so "b" is already on the walk's stack
			// and disturbing it is exactly the race the skip exists for.
			tc.disturb(t, filepath.Join(dir, "p/b"))

			var rest []string
			for it.Next() {
				rest = append(rest, it.Object().Key)
			}
			if err := it.Err(); err != nil {
				t.Fatalf("an object changing mid-scan failed the listing: %v", err)
			}
			if !slices.Equal(rest, []string{"p/c"}) {
				t.Errorf("after the disturbance, got %v, want [p/c]", rest)
			}
		})
	}
}

// TestLocalListStopsOnContextCancellation covers the interruptible half of a bounded
// scan, and covers it at both moments a caller can cancel: before the walk starts,
// and part way through it.
//
// The cancellation must arrive through Err. A listing that ended early with a nil
// error would be a scan that reported "nothing changed" for a window it abandoned.
func TestLocalListStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	src := openLocal(t, dir)

	t.Run("cancelled before the first step", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		it := src.List(ctx, "")
		defer func() { _ = it.Close() }()
		if it.Next() {
			t.Errorf("a cancelled listing yielded %v", it.Object())
		}
		if !errors.Is(it.Err(), context.Canceled) {
			t.Errorf("Err = %v, want context.Canceled", it.Err())
		}
	})

	t.Run("cancelled part way through", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		it := src.List(ctx, "")
		defer func() { _ = it.Close() }()
		if !it.Next() {
			t.Fatalf("the listing was empty: %v", it.Err())
		}
		cancel()

		for it.Next() { //nolint:revive // the error is the assertion, not the objects
		}
		if !errors.Is(it.Err(), context.Canceled) {
			t.Errorf("Err = %v, want context.Canceled", it.Err())
		}
	})
}

// TestLocalListCloseIsSafeAtAnyPoint covers the path every limited query takes.
// Breaking out early is normal here, not exceptional, so it must be free and it must
// not turn an exhausted listing into a failed one.
func TestLocalListCloseIsSafeAtAnyPoint(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	src := openLocal(t, dir)

	it := src.List(t.Context(), "")
	if !it.Next() {
		t.Fatalf("the listing was empty: %v", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if it.Next() {
		t.Errorf("a closed iterator yielded %v", it.Object())
	}
	if err := it.Err(); err != nil {
		t.Errorf("Err after an early Close = %v, want nil: abandoning a scan is not a failure", err)
	}
	if err := it.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestLocalOpen covers the fetching half, including the two failures the contract
// gives names to and the one it deliberately does not.
func TestLocalOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "p/object", "payload")
	outside := filepath.Join(filepath.Dir(dir), "outside-the-archive")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write the file outside the archive: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(outside) })
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Skipf("this filesystem does not support symlinks: %v", err)
	}
	src := openLocal(t, dir)

	t.Run("an object reads back byte for byte", func(t *testing.T) {
		body, err := src.Open(t.Context(), "p/object")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = body.Close() }()

		got, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("read the object: %v", err)
		}
		if string(got) != "payload" {
			t.Errorf("object body = %q, want %q", got, "payload")
		}
	})

	notFound := []string{
		"p/missing",
		"missing/object",
		// A shared prefix is not an object. What a bucket browser draws as a folder
		// has no key of its own, and a source that opened a directory here would hand
		// the decoder something no object store could ever produce.
		"p",
	}
	for _, key := range notFound {
		t.Run("not found: "+key, func(t *testing.T) {
			body, err := src.Open(t.Context(), key)
			if err == nil {
				_ = body.Close()
				t.Fatalf("Open(%q) succeeded", key)
			}
			if !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("Open(%q) = %v, want ErrKeyNotFound", key, err)
			}
		})
	}

	// Traversal is refused, and refused as *itself*. Reporting it as ErrKeyNotFound
	// would file an attempt to read outside the archive under the same heading as an
	// object that aged out last night, and the caller's correct response to those two
	// is not the same.
	refused := []string{"../outside-the-archive", "p/../../outside-the-archive", "/etc/passwd", "escape", "", "."}
	for _, key := range refused {
		t.Run("refused: "+key, func(t *testing.T) {
			body, err := src.Open(t.Context(), key)
			if err == nil {
				content, _ := io.ReadAll(body)
				_ = body.Close()
				t.Fatalf("Open(%q) succeeded and returned %q", key, content)
			}
			if errors.Is(err, ErrKeyNotFound) {
				t.Errorf("Open(%q) = %v, which reports a refused traversal as an absent object", key, err)
			}
		})
	}

	t.Run("a cancelled context is honoured", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := src.Open(ctx, "p/object"); !errors.Is(err, context.Canceled) {
			t.Errorf("Open with a cancelled context = %v, want context.Canceled", err)
		}
	})
}

// TestNewLocalRefusesAMissingDirectory keeps a mistyped path from presenting as an
// empty archive. "Nothing was recorded" and "nothing was there to read" are the two
// answers Invariant 9 exists to keep apart, and a CLI cannot report the difference if
// the source does not.
func TestNewLocalRefusesAMissingDirectory(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "no-such-archive")
	if src, err := NewLocal(missing); err == nil {
		_ = src.Close()
		t.Fatal("NewLocal accepted a directory that does not exist")
	}
}

// TestLocalCloseIsIdempotentAndStatesItself covers the shutdown path a CLI takes on
// every invocation, and the use-after-close a cancelled parallel fetch can produce.
func TestLocalCloseIsIdempotentAndStatesItself(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	src, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}

	if _, err := src.Open(t.Context(), archiveKeys[0]); err == nil {
		t.Error("Open on a closed source succeeded")
	}
	it := src.List(t.Context(), "")
	defer func() { _ = it.Close() }()
	if it.Next() {
		t.Error("List on a closed source yielded an object")
	}
	if it.Err() == nil {
		t.Error("List on a closed source reported an empty archive rather than a closed one")
	}
}

// TestLocalIsSafeForConcurrentUse is the -race half of the contract's promise. A
// query over a wide window lists while it fetches, under a concurrency cap, so every
// method here is reached from several goroutines at once.
func TestLocalIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	dir := writeArchive(t, archiveKeys)
	src := openLocal(t, dir)

	const goroutines = 8
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			it := src.List(t.Context(), "audit/format=jsonl-v1/")
			defer func() { _ = it.Close() }()

			var seen int
			for it.Next() {
				seen++
				body, err := src.Open(t.Context(), it.Object().Key)
				if err != nil {
					t.Errorf("goroutine %d: Open(%q): %v", i, it.Object().Key, err)
					return
				}
				if _, err := io.Copy(io.Discard, body); err != nil {
					t.Errorf("goroutine %d: read %q: %v", i, it.Object().Key, err)
				}
				_ = body.Close()
			}
			if err := it.Err(); err != nil {
				t.Errorf("goroutine %d: %v", i, err)
			}
			if seen != len(archiveKeys) {
				t.Errorf("goroutine %d saw %d objects, want %d", i, seen, len(archiveKeys))
			}
		})
	}
	wg.Wait()
}

// TestDirMayMatch pins the pruning predicate on its own, because the end-to-end
// assertion above can only show that pruning happened somewhere.
//
// The predicate has to be symmetric: a prefix reaches down into a directory on its
// way to a partition, and a directory sits wholly inside a prefix once the walk is
// past the last partition the prefix names. Getting either direction wrong is a
// listing that is silently short rather than one that fails.
func TestDirMayMatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dirKey string
		prefix string
		want   bool
	}{
		{name: "everything matches an empty prefix", dirKey: "a/b", prefix: "", want: true},
		{name: "the prefix reaches into the directory", dirKey: "a", prefix: "a/b/c", want: true},
		{name: "the directory lies inside the prefix", dirKey: "a/b/c", prefix: "a/b", want: true},
		{name: "the prefix stops exactly at the separator", dirKey: "a/b", prefix: "a/b/", want: true},
		{name: "a sibling partition", dirKey: "date=2026-03-13", prefix: "date=2026-03-14/", want: false},
		{name: "a name that merely starts the same", dirKey: "ab", prefix: "a/b", want: false},
		{name: "a partial name the prefix is still spelling", dirKey: "hour=07", prefix: "hour=0", want: true},
		{name: "an unrelated tree", dirKey: "scopes", prefix: "cluster_id=x/", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dirMayMatch(tc.dirKey, tc.prefix); got != tc.want {
				t.Errorf("dirMayMatch(%q, %q) = %v, want %v", tc.dirKey, tc.prefix, got, tc.want)
			}
		})
	}
}

// writeArchive materialises a set of keys as a directory tree and returns its root.
// The content of each object is its own key, so a test that reads one back can say
// which object it got rather than only that it got something.
func writeArchive(t *testing.T, keys []string) string {
	t.Helper()

	dir := t.TempDir()
	for _, key := range keys {
		writeFile(t, dir, key, key)
	}
	return dir
}

// writeFile creates one object, parent directories included.
func writeFile(t *testing.T, dir, key, content string) {
	t.Helper()

	path := filepath.Join(dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create the parents of %q: %v", key, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %q: %v", key, err)
	}
}

// makeUnreadable takes away a directory's read permission and skips the test if that
// had no effect — which is what happens when the suite runs as root, where the
// permission bits a test like this depends on are advisory.
func makeUnreadable(t *testing.T, dir string) {
	t.Helper()

	if err := os.Chmod(dir, 0o000); err != nil {
		t.Skipf("cannot take away read permission on %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("this process can read a directory with no permission bits set (running as root), " +
			"so the pruning assertion would pass whether or not the directory was pruned")
	}
}

// openLocal opens a source over dir and closes it when the test ends.
func openLocal(t *testing.T, dir string) *Local {
	t.Helper()

	src, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal(%q): %v", dir, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// listObjects drains a listing, failing the test if it ended in an error. The error
// check is not optional for a caller and it is not optional here either.
func listObjects(t *testing.T, src ObjectSource, prefix string) []Object {
	t.Helper()

	it := src.List(t.Context(), prefix)
	defer func() { _ = it.Close() }()

	var objects []Object
	for it.Next() {
		objects = append(objects, it.Object())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	return objects
}

// listKeys is listObjects reduced to what most assertions are about.
func listKeys(t *testing.T, src ObjectSource, prefix string) []string {
	t.Helper()

	objects := listObjects(t, src, prefix)
	if objects == nil {
		// A nil listing stays nil, so an assertion against a nil expectation reads as
		// "nothing matched" rather than as "an empty slice, which is a different value".
		return nil
	}
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	return keys
}
