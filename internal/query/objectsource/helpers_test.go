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

// The plumbing this package's own engine tests share: a source that records what
// was asked of it, and the shortest path from a history to an engine over an
// archive holding it.
//
// These are separate from the conformance harness on purpose. The suite's fixtures
// belong to the contract and are shared by every backend; the questions here are
// the ones only this backend raises — which prefixes a window resolves to, how many
// objects were open at once, what happens when one of them has gone — and none of
// them is something the contract has an opinion about.

package objectsource

import (
	"context"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// spySource wraps a source and records what was asked of it, so a test can assert
// on the *questions* rather than only on the answers.
//
// Pruning is the reason it exists. "This query returned the right changes" is
// satisfied by an engine that read the whole archive, and the difference between
// reading two hour partitions and reading two thousand is the difference between
// evaluation mode being usable and being a demo.
type spySource struct {
	inner ObjectSource

	mu sync.Mutex
	// prefixes and keys are every List and Open the engine performed, in the order
	// they were performed.
	prefixes []string
	keys     []string
	// open is how many objects are open right now, and peak is the most that were
	// ever open at once — the concurrency cap, observed rather than assumed.
	open, peak int
	// gateAt makes every open object wait until that many are open at once, which
	// is how a parallelism assertion becomes deterministic instead of hopeful: if
	// the engine really fetches n at a time they all arrive and proceed, and if it
	// does not the wait times out and the peak says so.
	gateAt int
	// refuse names keys whose Open fails, and the failure each reports. Per key
	// rather than one failure for all of them, because *which* failure a scan
	// surfaces when several objects are unreadable is itself a property worth
	// pinning.
	refuse map[string]error
	// listErr fails every listing, for the case where a partition cannot be read.
	listErr error
	// probe, when set, is called on every Open with the number of objects opened so
	// far. It is how the memory assertions sample live heap *during* a scan, which is
	// the only moment a retained-everything implementation is distinguishable from a
	// streaming one.
	probe func(opened int)
}

func newSpy(inner ObjectSource) *spySource {
	return &spySource{inner: inner, refuse: map[string]error{}}
}

// refuseKey makes one object's Open fail with err.
func (s *spySource) refuseKey(key string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuse[key] = err
}

// onOpen installs a probe called on every Open with the number of opens so far.
func (s *spySource) onOpen(probe func(opened int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probe = probe
}

// gateUntil holds every opened object until n of them are open at once.
func (s *spySource) gateUntil(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gateAt = n
}

// awaitPeers blocks until want objects are open at once, or the deadline passes.
//
// Polling rather than a condition variable because the deadline is the point: a
// test that deadlocked when the engine turned out to be serial would report
// "timeout" from the test framework instead of "the peak was 1, want 4".
func (s *spySource) awaitPeers(want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		reached := s.open >= want
		s.mu.Unlock()
		if reached || time.Now().After(deadline) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *spySource) List(ctx context.Context, prefix string) ObjectIterator {
	s.mu.Lock()
	s.prefixes = append(s.prefixes, prefix)
	listErr := s.listErr
	s.mu.Unlock()

	if listErr != nil {
		return &staticIterator{err: listErr}
	}
	return s.inner.List(ctx, prefix)
}

func (s *spySource) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	refusal, refused := s.refuse[key]
	gate, probe, opened := s.gateAt, s.probe, len(s.keys)
	if !refused {
		s.open++
		if s.open > s.peak {
			s.peak = s.open
		}
	}
	s.mu.Unlock()

	if refused {
		return nil, refusal
	}
	if gate > 0 {
		s.awaitPeers(gate, 2*time.Second)
	}
	if probe != nil {
		probe(opened)
	}
	body, err := s.inner.Open(ctx, key)
	if err != nil {
		s.closed()
		return nil, err
	}
	return &spyBody{ReadCloser: body, source: s}, nil
}

func (s *spySource) Close() error { return s.inner.Close() }

// closed records that one open object has been released.
func (s *spySource) closed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open--
}

// listed returns the prefixes the engine listed, sorted, so an assertion is about
// the set rather than about the order two parallel listings happened to start in.
func (s *spySource) listed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(s.prefixes)
	slices.Sort(out)
	return out
}

// opened returns the keys the engine fetched.
func (s *spySource) opened() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

// peakOpen returns the most objects that were open at the same moment.
func (s *spySource) peakOpen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// spyBody counts an object as open until it is closed, which is how the peak
// concurrency assertion measures what it claims to.
type spyBody struct {
	io.ReadCloser
	source *spySource
	once   sync.Once
}

func (b *spyBody) Close() error {
	b.once.Do(b.source.closed)
	return b.ReadCloser.Close()
}

// engineOver builds an engine over a directory holding history, and hands back the
// spy in front of it.
func engineOver(t *testing.T, history conformance.History, opts Options) (*Engine, *spySource) {
	t.Helper()

	dir := t.TempDir()
	if _, err := archivetest.WriteDir(dir, opts.Prefix, history); err != nil {
		t.Fatalf("seeding the fixture archive: %v", err)
	}
	return engineOverDir(t, dir, opts)
}

// engineOverDir builds an engine over a directory that is already as the test wants
// it — including empty, which is the archive a pruning assertion needs.
func engineOverDir(t *testing.T, dir string, opts Options) (*Engine, *spySource) {
	t.Helper()

	local, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("opening the fixture archive at %q: %v", dir, err)
	}
	spy := newSpy(local)
	t.Cleanup(func() {
		if err := spy.Close(); err != nil {
			t.Errorf("closing the fixture source: %v", err)
		}
	})

	engine, err := NewEngine(spy, opts)
	if err != nil {
		t.Fatalf("building an engine: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})
	return engine, spy
}

// drain runs a timeline query the way the contract documents: drain, close on every
// path, and check Err after the loop.
func drain(t *testing.T, engine *Engine, q query.TimelineQuery) []query.Change {
	t.Helper()

	changes, err := drainWithErr(t, engine, q)
	if err != nil {
		t.Fatalf("the iterator failed mid-stream: %v", err)
	}
	return changes
}

// drainWithErr is drain for the tests that are about the failure rather than about
// the changes: it returns both, because this engine delivers what it read *and*
// reports what cut it short.
func drainWithErr(t *testing.T, engine *Engine, q query.TimelineQuery) ([]query.Change, error) {
	t.Helper()

	return drainCtx(t, context.Background(), engine, q)
}

// drainCtx is drainWithErr under a context the test controls, for the assertions about
// a scan its caller stopped waiting for.
func drainCtx(
	t *testing.T, ctx context.Context, engine *Engine, q query.TimelineQuery,
) ([]query.Change, error) {
	t.Helper()

	it, err := engine.Timeline(ctx, q)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			t.Errorf("closing the iterator: %v", closeErr)
		}
	}()

	var changes []query.Change
	for it.Next() {
		changes = append(changes, it.Change())
	}
	return changes, it.Err()
}

// seedDir writes a history into dir and returns where each change landed, for the
// tests that need to name the object holding the nth change.
func seedDir(t *testing.T, dir, prefix string, history conformance.History) *archivetest.Layout {
	t.Helper()

	layout, err := archivetest.WriteDir(dir, prefix, history)
	if err != nil {
		t.Fatalf("seeding the fixture archive: %v", err)
	}
	return layout
}

// containsAll reports whether text contains every fragment.
//
// It is how a test asserts on a message without pinning its wording: what matters is
// that a reader is told which of two situations they are in, not the sentence it is
// told in.
func containsAll(text string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			return false
		}
	}
	return true
}

// testEpoch is the instant this package's own fixtures are dated from — fixed, so a
// failure message names the same partitions today as in a log pasted last week.
func testEpoch() time.Time { return time.Date(2026, 3, 14, 7, 15, 0, 0, time.UTC) }

// testRef is the object those fixtures record history for.
func testRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: "prod-eu-1",
		APIGroup:  "apps",
		Kind:      "Deployment",
		Namespace: "payments",
		Name:      "checkout",
	}
}
