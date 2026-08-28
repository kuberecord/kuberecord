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
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync/atomic"
)

// Local is an ObjectSource over a directory tree: an archive synced to a laptop,
// a mounted volume, or a bucket copied down for an investigation.
//
// It is the primary test vehicle for everything built on this seam, which is a
// deliberate choice rather than a convenience. Every property the query engine
// depends on — ordering, prefix semantics, a missing object, an interrupted scan —
// is asserted here, with no credential and no server, and the object store is then
// one more implementation of a contract rather than the thing the tests were
// written around. That is only sound if this source is faithful to how an object
// store answers the same questions, so the two places it could plausibly differ,
// listing order and what a prefix means, are matched exactly and tested for it.
//
// A Local is safe for concurrent use. It holds a directory handle rather than a
// path, so a directory swapped or moved underneath it does not silently redirect
// reads to a different tree.
type Local struct {
	root *os.Root

	// closed makes Close idempotent and gives a use-after-close a stated error
	// instead of whatever the runtime would have said. It is atomic because the
	// source is documented as safe for concurrent use, and a query fetching objects
	// in parallel is exactly where a close would race.
	closed atomic.Bool
}

// NewLocal opens a source rooted at dir.
//
// It uses os.Root, so containment is enforced by the operating system for every
// subsequent operation rather than by string checks here: an absolute key, a key
// climbing out with "..", or a symlink pointing outside the tree is refused at the
// syscall, and it stays refused if the tree is rearranged while a scan is running.
// The alternative — cleaning paths and comparing prefixes — is the classic way to
// be almost right, and this source is pointed at whatever directory a CLI user
// names.
//
// A directory that does not exist is an error and not an empty archive. The two
// are different answers to different questions, and a CLI that reported "no
// changes recorded" for a mistyped path would be the exact failure Invariant 9
// exists to prevent.
func NewLocal(dir string) (*Local, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("objectsource: open the local archive at %q: %w", dir, err)
	}
	return &Local{root: root}, nil
}

// List streams every object under prefix, in ascending byte order of the whole
// key. See ObjectSource.List for the semantics; what follows is how they are met
// on a filesystem, since neither is free here.
//
// A directory is descended only when a key beneath it could match, so a one-hour
// query against a ninety-day archive reads the handful of directories on the path
// and none of the others. The test for "could match" is symmetric — the prefix
// reaches into the directory, or the directory lies inside the prefix — because
// both happen: "…/date=2026-03-14/hour=07" reaches into date=2026-03-14, and
// hour=07 lies inside "…/date=2026-03-14/".
//
// Entries are re-sorted rather than taken in the order a directory read hands them
// back, because that order is by bare filename and the guarantee is about whole
// keys. Given a directory "a" holding "b" and a sibling file "a-x", a filename sort
// visits a before a-x while the keys sort a-x before a/b — '-' is 0x2D and '/' is
// 0x2F. Sorting each directory's entries on the name a directory *contributes to a
// key*, which includes its separator, restores the object store's order exactly:
// sibling names never contain a separator themselves, so the comparison can never
// be decided past the point where the two tokens differ.
//
// One directory's entries are held per level of depth, which for this archive
// layout is the path down to an hour partition plus that partition's own objects.
// Reading a whole directory is what sorting requires; the listing above it remains
// streamed, so the cost does not grow with the size of the archive.
func (l *Local) List(ctx context.Context, prefix string) ObjectIterator {
	if l.closed.Load() {
		return &staticIterator{err: errClosed(l.root.Name())}
	}

	it := &localIterator{ctx: ctx, root: l.root, prefix: prefix}
	// The root itself is always on the path to any key, so it is pushed without a
	// match test; every level below it earns its place.
	if err := it.push("", "."); err != nil {
		it.err = err
	}
	return it
}

// Open returns the file at key. See ObjectSource.Open.
//
// A key naming something that is not a regular file — a directory, a socket — is
// reported as ErrKeyNotFound, because an object store has no such key either: what
// looks like a folder in a bucket browser is a shared prefix, never an object. A
// key that is not a valid path is refused by name instead, and deliberately not as
// "not found": traversal refused and object absent are different facts, and
// folding the first into the second would file an attempt to read outside the
// archive under the same heading as an object aged out last night.
func (l *Local) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if l.closed.Load() {
		return nil, errClosed(l.root.Name())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !fs.ValidPath(key) || key == "." {
		return nil, fmt.Errorf(
			"objectsource: %q is not a usable object key for a local archive: it must be a "+
				"relative, slash-separated path inside %s", key, l.root.Name())
	}

	f, err := l.root.Open(key)
	if err != nil {
		return nil, l.classify(key, err)
	}
	// Stat the open file rather than the path, so what is checked is what was
	// opened: a key replaced between the two would otherwise be reported on by its
	// successor.
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(l.classify(key, err), closeAfterFailure(f))
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(
			fmt.Errorf("%w: %q under %s is not a regular file", ErrKeyNotFound, key, l.root.Name()),
			closeAfterFailure(f))
	}
	return f, nil
}

// closeAfterFailure releases a file Open has decided not to hand back, reporting a
// failure to release it rather than dropping it.
//
// It is joined to the error that caused the abandonment rather than replacing it,
// because the caller needs the first one — that is what says whether the object was
// absent or the key refused — and a descriptor that could not be closed is the kind
// of thing that shows up only on the hundred-thousandth object of a scan.
func closeAfterFailure(f *os.File) error {
	if err := f.Close(); err != nil {
		return fmt.Errorf("objectsource: release %s after an abandoned open: %w", f.Name(), err)
	}
	return nil
}

// Close releases the directory handle. It is idempotent, and a source in use after
// it reports so rather than failing at the syscall.
func (l *Local) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := l.root.Close(); err != nil {
		return fmt.Errorf("objectsource: close the local archive at %s: %w", l.root.Name(), err)
	}
	return nil
}

// classify turns a filesystem failure into the vocabulary the seam speaks, so that
// a caller's handling of a vanished object or a refused credential is the same code
// whichever source it is talking to.
func (l *Local) classify(key string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: %q under %s: %w", ErrKeyNotFound, key, l.root.Name(), err)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("%w: %q under %s: %w", ErrAccessDenied, key, l.root.Name(), err)
	default:
		return fmt.Errorf("objectsource: read %q under %s: %w", key, l.root.Name(), err)
	}
}

// errClosed is what every method answers once the source has been closed. It is
// spelled once so the three of them cannot describe the same state differently.
func errClosed(name string) error {
	return fmt.Errorf("objectsource: the local archive at %s is closed", name)
}

// localIterator walks the tree depth-first, holding one directory's remaining
// entries per level.
//
// Depth-first with the per-directory ordering above is what produces whole-key
// order, and it is why this is an explicit stack rather than fs.WalkDir: WalkDir
// visits in filename order, which is a different order, and it cannot be
// interrupted without a sentinel error threaded through the callback. A caller
// abandoning a scan is the normal path here, not the exception.
type localIterator struct {
	ctx    context.Context
	root   *os.Root
	prefix string

	stack  []localLevel
	cur    Object
	err    error
	closed bool
}

// localLevel is one open directory: the key prefix its entries hang under, the
// path to read it by, and how far through them the walk has got.
type localLevel struct {
	keyPrefix string // "" at the root, otherwise "a/b/"
	path      string // "." at the root, otherwise "a/b"
	entries   []localEntry
	next      int
}

// localEntry is one directory entry reduced to what the walk needs: its name, and
// whether it is a directory to descend or a candidate object.
type localEntry struct {
	name  string
	isDir bool
}

// Next advances to the next object. See ObjectIterator.
//
// The context is checked once per entry rather than once per directory, so an
// interrupted scan stops within one filesystem operation of the cancellation
// instead of at the end of whatever partition it happened to be in.
func (it *localIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for {
		if err := it.ctx.Err(); err != nil {
			it.err = err
			return false
		}
		if len(it.stack) == 0 {
			return false
		}

		level := &it.stack[len(it.stack)-1]
		if level.next >= len(level.entries) {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		entry := level.entries[level.next]
		level.next++
		key := level.keyPrefix + entry.name

		if entry.isDir {
			if !dirMayMatch(key, it.prefix) {
				continue
			}
			if err := it.push(key+"/", pathOf(level.path, entry.name)); err != nil {
				it.err = err
				return false
			}
			continue
		}

		if !strings.HasPrefix(key, it.prefix) {
			continue
		}
		// Lstat rather than Stat, so what is measured is the entry itself: an entry
		// that has become a symlink since the directory was read is skipped below
		// rather than followed, and a symlink out of the archive cannot fail a
		// listing by being refused here.
		info, err := it.root.Lstat(pathOf(level.path, entry.name))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// The file went away between the directory read and this stat. An
				// object store's listing would simply not have contained it — a bucket
				// under a lifecycle rule ages objects out mid-scan as a matter of course
				// — so the faithful answer is to carry on rather than to fail a listing
				// over an object that no longer exists.
				continue
			}
			it.err = fmt.Errorf("objectsource: stat %q under %s: %w", key, it.root.Name(), err)
			return false
		}
		if !info.Mode().IsRegular() {
			// It was a regular file when the directory was read and is not one now. The
			// same reasoning as the skip in push applies, and the window between the two
			// is why this is checked twice rather than once.
			continue
		}

		it.cur = Object{Key: key, Size: info.Size()}
		return true
	}
}

// Object returns the object Next advanced to. See ObjectIterator.
func (it *localIterator) Object() Object { return it.cur }

// Err returns what ended the listing, or nil if it ran out. See ObjectIterator.
func (it *localIterator) Err() error { return it.err }

// Close abandons the walk. It holds no file handles between calls to Next — a
// directory is read and released in one operation — so this releases the pending
// entries and nothing else, which is what makes breaking out of a scan free.
func (it *localIterator) Close() error {
	it.closed = true
	it.stack = nil
	return nil
}

// push reads a directory and puts its entries on the stack in whole-key order.
//
// A directory that has disappeared since it was named is treated as empty for the
// same reason a vanished file is skipped: an archive being aged out underneath a
// scan is ordinary, and the alternative is a listing that fails because a partition
// expired while it was being read.
func (it *localIterator) push(keyPrefix, path string) error {
	entries, err := fs.ReadDir(it.root.FS(), path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("objectsource: list %q under %s: %w", keyPrefix, it.root.Name(), err)
	}

	level := localLevel{keyPrefix: keyPrefix, path: path, entries: make([]localEntry, 0, len(entries))}
	for _, entry := range entries {
		switch {
		case entry.IsDir():
			level.entries = append(level.entries, localEntry{name: entry.Name(), isDir: true})
		case entry.Type().IsRegular():
			level.entries = append(level.entries, localEntry{name: entry.Name()})
		default:
			// Symlinks, sockets and devices are skipped rather than followed. An object
			// store has no analogue for any of them; following a directory symlink would
			// make a listing depend on link topology and need not terminate; and emitting
			// one as an object would hand out a key that Open then has to refuse. A local
			// archive is a tree of regular files, and this is the documented shape of that
			// claim rather than an omission.
		}
	}
	slices.SortFunc(level.entries, func(a, b localEntry) int {
		return strings.Compare(sortToken(a), sortToken(b))
	})

	it.stack = append(it.stack, level)
	return nil
}

// sortToken is what an entry contributes to every key beneath it: its name, plus
// the separator a directory adds. Sorting on it makes a depth-first walk emit keys
// in the byte order an object store lists them in — see List for why the bare
// filename does not.
func sortToken(e localEntry) string {
	if e.isDir {
		return e.name + "/"
	}
	return e.name
}

// pathOf joins a directory's path with an entry name, keeping "." from leaking
// into the paths handed to the filesystem.
func pathOf(dir, name string) string {
	if dir == "." {
		return name
	}
	return dir + "/" + name
}

// dirMayMatch reports whether any key under the directory at dirKey can begin with
// prefix.
//
// Every such key begins with dirKey+"/", so the two of them have to be on the same
// line of descent: either the prefix reaches into this directory, or this directory
// lies wholly inside the prefix. Anything else — a sibling partition, an unrelated
// tree — is pruned before it is ever read, which is what keeps a one-hour query
// from touching eighty-nine other days.
func dirMayMatch(dirKey, prefix string) bool {
	within := dirKey + "/"
	return strings.HasPrefix(prefix, within) || strings.HasPrefix(within, prefix)
}

// staticIterator is a listing that is over before it starts, carrying the reason.
// It exists so List can report a failure through the iterator's own error channel
// rather than by growing a second return value that every caller would have to
// handle alongside Err (see ObjectSource.List).
type staticIterator struct{ err error }

func (it *staticIterator) Next() bool     { return false }
func (it *staticIterator) Object() Object { return Object{} }
func (it *staticIterator) Err() error     { return it.err }
func (it *staticIterator) Close() error   { return nil }

// Compile-time proof that both shapes in this file satisfy the contract, asserted
// here rather than discovered at a call site where a signature drift would surface
// in a file that has nothing to do with either.
var (
	_ ObjectSource   = (*Local)(nil)
	_ ObjectIterator = (*localIterator)(nil)
	_ ObjectIterator = (*staticIterator)(nil)
)
