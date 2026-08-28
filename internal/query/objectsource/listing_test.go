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

// The listing's own footprint, measured rather than asserted about.
//
// # What is being ruled out
//
// A listing phase that materializes its result twice: one slice per prefix, and then a
// merged slice holding every key again, both live at the same moment. It is invisible
// in every other test in this package, because a fixture of a few hundred objects makes
// the difference a few kilobytes. A ninety-day window of a busy cluster is a different
// figure: at the writer's ceiling rotation that is on the order of 10^5 keys of 110–130
// bytes each, so the listing is tens of megabytes and the doubling is a slice of
// headers nobody needed.
//
// Note that "pre-size the destination and append into it" does *not* fix this. The
// destination's backing array is allocated in full before the first element is copied
// into it, so the parts and the merged array are live together from the first append
// regardless. What fixes it is not building the merged array at all — see
// listObjectKeys.
//
// # Why the measurement is a slope over a source that allocates nothing
//
// A real listing allocates a key string per object, which dwarfs the term under test
// and varies with the fixture. The source below hands back keys built before the
// measurement started, so the only heap traffic during listObjectKeys is the slices
// themselves. What is then measured is total allocated bytes — which, unlike live heap,
// does not depend on when the collector happened to run — for two archive sizes, and
// the assertion is on the difference divided by the difference in keys. Constants
// cancel; the per-key term is what the two implementations differ in.

package objectsource

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"testing"
)

// preListedSource is a source whose listings allocate nothing.
//
// Every key it yields was built by newPreListed before the measurement began, and the
// iterator hands them out by index. That is what makes a byte count over listObjectKeys
// a measurement of listObjectKeys rather than of a fixture.
type preListedSource struct {
	prefixes []string
	keys     map[string][]string
}

// newPreListed builds an archive listing of prefixes × perPrefix keys, shaped like the
// real thing: the same partition layout, and keys long enough that a header is a
// visible fraction of one.
func newPreListed(prefixes, perPrefix int) *preListedSource {
	src := &preListedSource{
		prefixes: make([]string, 0, prefixes),
		keys:     make(map[string][]string, prefixes),
	}
	root := recordsRoot("audit", "prod-eu-1")
	for p := range prefixes {
		prefix := fmt.Sprintf("%sdate=2026-03-%02d/hour=%02d/", root, 1+p/hoursPerDay, p%hoursPerDay)
		under := make([]string, 0, perPrefix)
		for k := range perPrefix {
			under = append(under, fmt.Sprintf("%s%064x-%08d%s", prefix, p, k, objectSuffix))
		}
		src.prefixes = append(src.prefixes, prefix)
		src.keys[prefix] = under
	}
	return src
}

func (s *preListedSource) List(_ context.Context, prefix string) ObjectIterator {
	return &preListedIterator{keys: s.keys[prefix], at: -1}
}

// Open is unreachable: this source exists to measure a listing, and a test that
// fetched through it would be measuring something else.
func (s *preListedSource) Open(_ context.Context, key string) (io.ReadCloser, error) {
	panic("preListedSource: Open is not part of what this source is for, and " + key +
		" was asked for anyway")
}

func (s *preListedSource) Close() error { return nil }

type preListedIterator struct {
	keys []string
	at   int
}

func (it *preListedIterator) Next() bool {
	it.at++
	return it.at < len(it.keys)
}

func (it *preListedIterator) Object() Object { return Object{Key: it.keys[it.at], Size: 1} }
func (it *preListedIterator) Err() error     { return nil }
func (it *preListedIterator) Close() error   { return nil }

var (
	_ ObjectSource   = (*preListedSource)(nil)
	_ ObjectIterator = (*preListedIterator)(nil)
)

// listingBytes reports how many bytes listObjectKeys allocated over an archive of
// prefixes × perPrefix keys.
//
// TotalAlloc rather than a live-heap reading, because the question is what the phase
// *builds* and the answer must not depend on when the collector ran. The result is kept
// alive across the second reading so that nothing under measurement can be reclaimed
// inside the window.
func listingBytes(t *testing.T, prefixes, perPrefix int) uint64 {
	t.Helper()

	src := newPreListed(prefixes, perPrefix)
	engine, err := NewEngine(src, Options{Prefix: "audit"})
	if err != nil {
		t.Fatalf("building an engine: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	parts, err := listObjectKeys(context.Background(), engine, src.prefixes)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("listing the fixture: %v", err)
	}

	counted := 0
	for _, part := range parts {
		counted += len(part)
	}
	if want := prefixes * perPrefix; counted != want {
		t.Fatalf("the listing returned %d keys, want %d; the measurement below is meaningless if the "+
			"listing is not the whole listing", counted, want)
	}
	runtime.KeepAlive(parts)
	return after.TotalAlloc - before.TotalAlloc
}

// TestTheListingIsNotMaterializedTwice: the keys of a window are built once.
//
// The threshold is arithmetic rather than a guess. A []string element is 16 bytes on a
// 64-bit build, and a per-prefix slice grown by append from nil costs about twice its
// final size in total allocations — the geometric sum of the doublings — so roughly 32
// bytes per key, plus a small constant per prefix: 36 as measured here. A merged array
// on top of that is one more element per key, which takes it to 52. The budget sits
// between the two figures rather than beside either, so that neither ordinary variation
// nor a modest change elsewhere in the listing decides the outcome.
//
// It is deliberately not t.Parallel: TotalAlloc is a process-wide counter, and a test
// allocating beside this one would be measured as part of it.
func TestTheListingIsNotMaterializedTwice(t *testing.T) {
	const (
		perPrefix = 128
		fewer     = 32
		more      = 256
		// Per key, between the one-array figure (~36 B) and the two-array one (~52 B).
		budget = 44
	)

	// Warm first, so that neither reading pays for a one-off the other does not: the
	// slope is a difference and a constant charged to only one side would survive it.
	listingBytes(t, fewer, perPrefix)

	small := listingBytes(t, fewer, perPrefix)
	large := listingBytes(t, more, perPrefix)
	if large <= small {
		t.Fatalf("listing %d keys allocated %d bytes and listing %d allocated %d; the measurement is "+
			"too noisy to conclude anything from", fewer*perPrefix, small, more*perPrefix, large)
	}

	perKey := (large - small) / uint64((more-fewer)*perPrefix)
	t.Logf("listing allocated %d bytes for %d keys and %d for %d — %d bytes per additional key, "+
		"against a %d-byte slice element", small, fewer*perPrefix, large, more*perPrefix, perKey,
		stringHeaderBytes)

	if perKey > budget {
		t.Errorf("each additional key listed allocated %d bytes, over a budget of %d. A slice element "+
			"is %d bytes and one grown by append costs about twice that in total, so a figure this far "+
			"above %d means the listing is being materialized a second time — one slice per prefix and "+
			"a merged one beside it, both live at once. At the 10^5 keys a ninety-day window of a busy "+
			"cluster holds, that second array is megabytes held for nothing but concatenation. "+
			"Pre-sizing the destination does not fix it: its backing array is allocated in full before "+
			"the first element is copied in", perKey, budget, stringHeaderBytes, 2*stringHeaderBytes)
	}
}

// stringHeaderBytes is the size of a []string element on the builds this project
// targets, spelled out because the budget above is reasoned in terms of it.
const stringHeaderBytes = 16
