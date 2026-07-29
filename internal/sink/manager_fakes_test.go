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

package sink

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The fakes in this file exist so the sink runtime can be tested without a
// backend: every property Task 1.8 has to prove — a job committed exactly once on
// the instance that accepted it, a drain that finishes before its connection
// closes, a probe that fails and recovers — is a property of the *manager's*
// ordering, not of ClickHouse. A fake writer that reproduces CHWriter's shutdown
// contract (drain the queue, settle every job, then close) is therefore a
// complete stand-in.

// fakeWriter is a sink.Writer with an observable lifecycle. It reproduces the
// contract every real Writer implements (see sink.Writer.Start): jobs accepted by
// Enqueue are held until the context is cancelled, at which point they are all
// settled — exactly once each — and only then is the connection "closed".
//
// Every event is stamped from a clock shared across the fakes of one test, so an
// assertion can be about *ordering* (commits before close, close before the
// pipeline eviction) rather than about wall-clock timing.
type fakeWriter struct {
	// label identifies this instance in failure messages: several instances of one
	// sink exist during a recycle, and "which one got the job?" is the question the
	// rotation test asks.
	label string
	clock *atomic.Int64

	// holdDrain, when non-nil, blocks the drain until it is closed, so a test can
	// act while an instance is mid-drain (the delete-then-recreate race).
	holdDrain chan struct{}

	// startErr is returned by Start after the drain, mirroring a backend that fails
	// to close its connection cleanly.
	startErr error
	// enqueueErr, when non-nil, is returned by every Enqueue.
	enqueueErr error

	starts atomic.Int64

	mu sync.Mutex
	// queued holds accepted-but-unsettled jobs, exactly like a real writer's
	// bounded hand-off queue.
	queued []Job
	// commits counts settled outcomes per record name, which is what makes
	// "exactly once" assertable rather than merely plausible.
	commits map[string]int
	trues   int
	falses  int
	// lastCommitAt / closedAt are clock stamps; closed reports whether the
	// instance finished its shutdown.
	lastCommitAt int64
	closedAt     int64
	closed       bool
}

func newFakeWriter(label string, clock *atomic.Int64) *fakeWriter {
	return &fakeWriter{label: label, clock: clock, commits: make(map[string]int)}
}

// Enqueue accepts a job without settling it, exactly as a bounded hand-off does.
func (f *fakeWriter) Enqueue(_ context.Context, job Job) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("fakeWriter %s: closed, refusing new write", f.label)
	}
	f.queued = append(f.queued, job)
	return nil
}

// Start holds until ctx is cancelled, then drains: every queued job is settled
// once, and the instance is marked closed only afterwards. That ordering is the
// contract the manager's drain depends on, so the fake must not shortcut it.
func (f *fakeWriter) Start(ctx context.Context) error {
	f.starts.Add(1)
	<-ctx.Done()

	if f.holdDrain != nil {
		<-f.holdDrain
	}

	f.mu.Lock()
	queued := f.queued
	f.queued = nil
	f.mu.Unlock()

	for _, job := range queued {
		// The callback runs outside the lock, exactly as a real worker's does; it
		// is the test's own closure, which stamps the commit via record.
		job.Commit(true)
	}

	f.mu.Lock()
	f.closed = true
	f.closedAt = f.clock.Add(1)
	f.mu.Unlock()
	return f.startErr
}

// record notes one settled outcome. Tests call it from the Commit callbacks they
// hand to Enqueue, so the count is of *callback invocations* — the thing the
// exactly-once contract is about — not of the fake's own bookkeeping.
func (f *fakeWriter) record(name string, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits[name]++
	if ok {
		f.trues++
	} else {
		f.falses++
	}
	f.lastCommitAt = f.clock.Add(1)
}

// counts returns the settled totals and the highest per-name commit count, which
// is what proves no job was settled twice.
func (f *fakeWriter) counts() (total, trues, maxPerName int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.commits {
		total += n
		maxPerName = max(maxPerName, n)
	}
	return total, f.trues, maxPerName
}

// committed reports how many times the named record settled on this instance.
func (f *fakeWriter) committed(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits[name]
}

// isClosed reports whether the instance finished its shutdown, plus the clock
// stamps of its last commit and of the close itself.
func (f *fakeWriter) isClosed() (closed bool, lastCommitAt, closedAt int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed, f.lastCommitAt, f.closedAt
}

// enqueue submits one job named name to w, settling into w's own commit log.
func enqueue(t *testing.T, w Writer, target *fakeWriter, name string) {
	t.Helper()
	err := w.Enqueue(context.Background(), Job{
		Record: Record{Name: name},
		Commit: func(ok bool) { target.record(name, ok) },
	})
	if err != nil {
		t.Fatalf("Enqueue(%q) on %s: %v", name, target.label, err)
	}
}

// fakeInstance is a fakeWriter that also implements every optional half of the
// sink contract (StateReader, ScopeEventWriter, Prober), so the routing tests can
// distinguish a fully-featured backend from a write-only one.
//
// It is a separate type rather than more methods on fakeWriter precisely because
// that distinction has to be testable: a backend without a StateReader must route
// writes and yet report no reader.
type fakeInstance struct {
	*fakeWriter
	// probe is the scripted probe outcome; attempts start at 1.
	probe func(attempt int) error
	// attempts counts Probe calls, so a test can assert the loop kept retrying.
	attempts atomic.Int64
	// probeAt records when every attempt ran, which is how the backoff test proves
	// the loop waited between them. It has its own mutex rather than reusing the
	// embedded writer's, so a probe can never contend with a drain.
	probeMu sync.Mutex
	probeAt []time.Time
}

func newFakeInstance(label string, clock *atomic.Int64) *fakeInstance {
	return &fakeInstance{fakeWriter: newFakeWriter(label, clock)}
}

func (f *fakeInstance) Probe(ctx context.Context) error {
	attempt := int(f.attempts.Add(1))
	f.probeMu.Lock()
	f.probeAt = append(f.probeAt, time.Now())
	f.probeMu.Unlock()
	if f.probe == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.probe(attempt)
}

// probeTimes returns when each probe attempt ran.
func (f *fakeInstance) probeTimes() []time.Time {
	f.probeMu.Lock()
	defer f.probeMu.Unlock()
	return append([]time.Time(nil), f.probeAt...)
}

// LastKnownStates answers with no history at all. The manager only ever routes
// this call, never interprets it, so the per-incarnation semantics of a
// KnownState (see sink.KnownState) are exercised where they are consumed —
// internal/pipeline's warm-up — rather than here.
func (f *fakeInstance) LastKnownStates(context.Context, ScopeFilter) ([]KnownState, error) {
	return nil, nil
}

func (f *fakeInstance) ScopeWasActive(context.Context, ScopeFilter, time.Time) (bool, error) {
	return false, nil
}

func (f *fakeInstance) ActiveScopes(context.Context, string) ([]ScopeFilter, error) {
	return nil, nil
}

func (f *fakeInstance) EnqueueScopeEvent(context.Context, ScopeEvent) error { return nil }

// fakeConfig is an InstanceConfig whose fingerprint is whatever the test says it
// is, so a "rotated credential" is expressed directly as a changed fingerprint
// without the test having to model a backend's settings.
type fakeConfig struct {
	fingerprint string
}

func (c fakeConfig) Fingerprint() string { return c.fingerprint }

// fakePipeline records RemoveSink calls and when they happened, which is how the
// deletion test proves the eviction followed the drain rather than racing it.
type fakePipeline struct {
	clock *atomic.Int64

	mu       sync.Mutex
	removed  []string
	removeAt map[string]int64
}

func newFakePipeline(clock *atomic.Int64) *fakePipeline {
	return &fakePipeline{clock: clock, removeAt: make(map[string]int64)}
}

func (f *fakePipeline) RemoveSink(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, name)
	f.removeAt[name] = f.clock.Add(1)
}

func (f *fakePipeline) removals() ([]string, map[string]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.removed...), maps.Clone(f.removeAt)
}

// fakeWarmHooks records ForgetSink calls and when they happened, so the deletion
// test can prove the coordinator's bookkeeping is cleared alongside — and not
// before — the pipeline state it describes.
type fakeWarmHooks struct {
	clock *atomic.Int64

	mu        sync.Mutex
	forgotten []string
	forgetAt  map[string]int64
}

func newFakeWarmHooks(clock *atomic.Int64) *fakeWarmHooks {
	return &fakeWarmHooks{clock: clock, forgetAt: make(map[string]int64)}
}

func (f *fakeWarmHooks) ForgetSink(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgotten = append(f.forgotten, name)
	f.forgetAt[name] = f.clock.Add(1)
}

func (f *fakeWarmHooks) forgets() ([]string, map[string]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.forgotten...), maps.Clone(f.forgetAt)
}

// fakeDependents is the rule index the parking callback consults.
type fakeDependents struct {
	rules map[string][]string
}

func (f fakeDependents) RulesForSink(sinkName string) []string { return f.rules[sinkName] }

// parkLog is the fake consumer of the parking callback: it records which rules
// were parked for which sink, and when.
type parkLog struct {
	clock *atomic.Int64

	mu     sync.Mutex
	parked map[string][]string
	at     map[string]int64
}

func newParkLog(clock *atomic.Int64) *parkLog {
	return &parkLog{clock: clock, parked: make(map[string][]string), at: make(map[string]int64)}
}

func (p *parkLog) park(sinkName string, ruleKeys []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parked[sinkName] = ruleKeys
	p.at[sinkName] = p.clock.Add(1)
}

// snapshot returns what has been parked so far: the rule keys per sink, and the
// clock stamp of each parking. Both maps are copies, so a test may read them
// without holding the log's lock — and an unexpected sink showing up in them is
// itself a failure worth seeing.
func (p *parkLog) snapshot() (rules map[string][]string, at map[string]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return maps.Clone(p.parked), maps.Clone(p.at)
}

// errFactory is the sentinel a factory returns when a test wants the build to
// fail (a bad address, an unresolvable credential).
var errFactory = errors.New("cannot build this sink")

// waitFor polls cond until it holds or the deadline passes, so a test asserts on
// a settled state rather than on a sleep. The manager does its draining and
// probing on its own goroutines, so every post-condition here is eventual.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
