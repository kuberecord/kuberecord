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

// The boundary, before anything below is trusted.
//
// This run proves the engine against a *directory*, and that is nearly all of it:
// the layout is the layout, the lines are the lines, the frame is a real zstd
// frame, and every property of the read plane is asserted over objects a writer
// could have produced. What it does not prove is that a real object store answers
// the listing questions the way a directory does — the ordering of whole keys, the
// meaning of a prefix, the story for an object that has gone. Task 10.1's own
// integration suite pins that seam directly, and the same conformance run is
// executed against a real store beside it (see awssource), from this same fixture.
//
// So "this backend passes conformance" means the format and the semantics are
// right. It does not mean any particular deployment's credentials or endpoint are.

package objectsource

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// archivePrefix is the prefix every fixture archive is written under.
//
// Non-empty deliberately. An empty prefix is the simpler configuration and the one
// a bug in prefix handling would survive, so the suite runs against the harder of
// the two and the local unit tests cover the empty case.
const archivePrefix = "audit"

// declaredCapabilities is what this backend declares, named by hand rather than
// copied off Capabilities().
//
// The suite checks the declaration against the engine in both directions, and that
// check is only worth anything if the two were written independently: a harness
// that handed over the engine's own report would be comparing a value with itself.
//
// TimeBoundRequired is the only one, and the three absences are each a statement.
// Deletions: this archive never receives one (D12), so a timeline that simply stops
// must be rendered with a notice rather than read as "still there".
// ServerSideFilter: there is nothing behind the seam to push a predicate into.
// PointQuery: there is no index, so one object's history costs the partitions its
// window lands in.
func declaredCapabilities() conformance.CapabilitySet {
	return conformance.DeclareCapabilities(conformance.CapTimeBoundRequired)
}

// harness seeds an archive on disk and hands the suite an engine over it.
type harness struct {
	dir    string
	source *faultingSource
	layout *archivetest.Layout
}

// seed writes the history the property asserts against.
func (h *harness) seed(history conformance.History) error {
	layout, err := archivetest.WriteDir(h.dir, archivePrefix, history)
	if err != nil {
		return err
	}
	h.layout = layout
	return nil
}

// setFault installs the failure the suite uses to break a stream part-way through,
// by refusing the objects holding every change after the nth.
//
// This is how a mid-stream failure is injected into a backend whose stream is a set
// of objects rather than a cursor over rows. The fixture writes one change per
// object, oldest first (see archivetest), so the objects beyond a given change
// are addressable — and because a scan does not cancel its siblings when one object
// fails, the ones before it are read in full. The result is exactly n changes
// delivered and then the failure, deterministically rather than by winning a race.
func (h *harness) setFault(fault *conformance.StreamFault) {
	if fault == nil {
		h.source.refuse(nil, nil)
		return
	}
	keys := h.layout.RecordKeys
	if fault.AfterChanges < len(keys) {
		keys = keys[fault.AfterChanges:]
	} else {
		keys = nil
	}
	h.source.refuse(keys, fault.Err)
}

// faultingSource is a source that refuses named objects.
//
// It wraps the shipped local source rather than replacing it, so that everything a
// property exercises — the listing order, the prefix semantics, the real frames on
// disk — is the shipped code, and only the one failure is injected.
type faultingSource struct {
	inner ObjectSource

	mu      sync.Mutex
	refused map[string]bool
	err     error
}

// refuse installs the set of keys that will fail to open, and the failure they
// report. A nil key list clears it.
func (s *faultingSource) refuse(keys []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refused = make(map[string]bool, len(keys))
	for _, key := range keys {
		s.refused[key] = true
	}
	s.err = err
}

// fault returns the failure installed for a key, or nil.
func (s *faultingSource) fault(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refused[key] {
		return s.err
	}
	return nil
}

func (s *faultingSource) List(ctx context.Context, prefix string) ObjectIterator {
	return s.inner.List(ctx, prefix)
}

func (s *faultingSource) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.fault(key); err != nil {
		return nil, err
	}
	return s.inner.Open(ctx, key)
}

func (s *faultingSource) Close() error { return s.inner.Close() }

// newHarness builds one archive and one engine over it, per property.
func newHarness(t *testing.T) conformance.Harness {
	t.Helper()

	dir := t.TempDir()
	local, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("opening the fixture archive at %q: %v", dir, err)
	}
	source := &faultingSource{inner: local}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("closing the fixture source: %v", err)
		}
	})

	engine, err := NewEngine(source, Options{Prefix: archivePrefix})
	if err != nil {
		t.Fatalf("building an engine over the fixture archive: %v", err)
	}

	h := &harness{dir: dir, source: source}
	return conformance.Harness{
		Engine:         engine,
		Seed:           h.seed,
		SetStreamFault: h.setFault,
		Capabilities:   declaredCapabilities(),
	}
}

// TestQueryConformance runs the read-plane contract against this backend.
func TestQueryConformance(t *testing.T) {
	conformance.RunQuerySuite(t, newHarness)
}

// Compile-time proof that the faulting wrapper is a source like any other, so a
// property exercises the seam rather than a special case.
var _ ObjectSource = (*faultingSource)(nil)
