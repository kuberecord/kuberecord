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

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The promise `jsonl` makes: one item per line, written as it arrives.
//
// It is asserted two ways, because the two failures look nothing alike. A path
// that collected the result and then wrote it would still produce byte-identical
// output, so the interleaving test watches *when* each line is written — it is the
// only way to tell a stream from a buffer by looking at the result. And a path
// that streamed but held a reference to every change it had seen would interleave
// perfectly while still growing without bound, so the second test measures the
// live heap at the end of two runs of very different sizes.
//
// Both use a generated history rather than a fixture slice, because a fake that
// materialized a hundred thousand changes up front would be measuring the test's
// own memory.

// itemBytes is roughly how large one generated change is on the wire.
//
// It is large enough that a retained result would be unmistakable in the heap
// measurement — a hundred thousand of them is on the order of a hundred megabytes
// — and it is the shape of a real row rather than an inflated one: the data column
// of a small object.
const itemBytes = 1024

// TestJSONLWritesEachItemBeforeReadingTheNext is the streaming property, asserted
// deterministically.
//
// The engine checks, before it produces change number n, that n lines have already
// reached the writer: the head line plus the n-1 items before it. A path that
// gathered the result first would have written exactly one line — the head — by
// the time the last change was read, and this fails on the second item rather than
// at some size that depends on the machine.
func TestJSONLWritesEachItemBeforeReadingTheNext(t *testing.T) {
	const total = 500

	written := &lineCounter{}
	engine := &generatedEngine{
		caps:  clickHouseCapabilities(),
		total: total,
		observe: func(index int) {
			if got, want := written.lines(), index+1; got != want {
				t.Errorf("change %d was read with %d lines written, want %d: the answer is being "+
					"gathered rather than streamed", index, got, want)
			}
		},
	}

	if err := runStream(t, engine, streamRequest(0, false), written); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if got := written.lines(); got != total+1 {
		t.Errorf("%d lines were written, want %d (a head line and %d items)", got, total+1, total)
	}
}

// TestJSONLIsValidUnderALargeResult is the acceptance criterion's first half.
//
// Every line is parsed as it arrives and then dropped, so the check itself holds
// nothing: a test that collected the output into one buffer would pass while
// proving that the *test* can hold a hundred thousand rows.
func TestJSONLIsValidUnderALargeResult(t *testing.T) {
	const total = 100_000

	checker := &lineParser{t: t}
	engine := &generatedEngine{caps: clickHouseCapabilities(), total: total}
	if err := runStream(t, engine, streamRequest(0, false), checker); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	if checker.lines != total+1 {
		t.Fatalf("%d lines were written, want %d (a head line and %d items)",
			checker.lines, total+1, total)
	}
	if checker.head["kind"] != render.KindTimeline {
		t.Errorf("the head line's kind is %#v, want %q", checker.head["kind"], render.KindTimeline)
	}
	if checker.items != total {
		t.Errorf("%d item lines carried a ts field, want %d; every line after the head is one change",
			checker.items, total)
	}
}

// TestJSONLMemoryDoesNotScaleWithTheResult is the acceptance criterion's second
// half.
//
// The live heap is measured at the last change of each run, which is the only
// moment the question has an answer: afterwards everything is garbage whether it
// was retained or not. Two runs differ by a factor of fifty, and a path holding
// every change would differ by about a hundred megabytes — so the allowance below
// is generous enough to absorb GC scheduling and still nowhere near able to hide
// the failure it exists to catch.
func TestJSONLMemoryDoesNotScaleWithTheResult(t *testing.T) {
	if testing.Short() {
		t.Skip("the measurement runs a hundred thousand changes through the writer")
	}

	small := liveHeapAtLastItem(t, 2_000)
	large := liveHeapAtLastItem(t, 100_000)

	// Signed, because the two runs are independent and the large one can
	// legitimately measure lower.
	growth := int64(large) - int64(small)
	const allowance = 8 << 20
	if growth > allowance {
		t.Errorf("the live heap grew by %d bytes between a 2,000-change answer and a 100,000-change "+
			"one, which is more than the %d-byte allowance: the result is being retained rather than "+
			"streamed", growth, allowance)
	}
}

// liveHeapAtLastItem runs a timeline of total changes and reports the live heap at
// the moment the last one was read.
func liveHeapAtLastItem(t *testing.T, total int) uint64 {
	t.Helper()

	var live uint64
	engine := &generatedEngine{
		caps:  clickHouseCapabilities(),
		total: total,
		observe: func(index int) {
			if index != total-1 {
				return
			}
			var stats runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&stats)
			live = stats.HeapAlloc
		},
	}
	if err := runStream(t, engine, streamRequest(0, false), io.Discard); err != nil {
		t.Fatalf("RunTimeline over %d changes: %v", total, err)
	}
	return live
}

// TestJSONLHoldsAtMostTheLimitWhenReversed pins the one case that cannot stream.
//
// --reverse with --limit has to read the newest N before it can write the oldest of
// them, and the buffer is bounded by the limit rather than by the result. The
// engine below returns far more changes than the limit, and the assertion is that
// the number *read* is the limit — which is what makes the buffer's size a number
// the user typed.
func TestJSONLHoldsAtMostTheLimitWhenReversed(t *testing.T) {
	const (
		total = 10_000
		limit = 25
	)

	engine := &generatedEngine{caps: clickHouseCapabilities(), total: total, limitAware: true}
	var out bytes.Buffer
	if err := runStream(t, engine, streamRequest(limit, true), &out); err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	if engine.read > limit {
		t.Errorf("%d changes were read for a --limit of %d: the whole result was pulled through "+
			"to be reversed", engine.read, limit)
	}

	_, items := decodeJSONL(t, out.String())
	if len(items) != limit {
		t.Fatalf("%d items were written, want %d", len(items), limit)
	}
	// Oldest first, which is what --reverse asked for, over the newest `limit`
	// changes, which is what --limit selects. Getting the other end of history
	// here would mean a cheap query had been turned into a different question.
	if got := timestampOf(t, items[0]); !strings.HasPrefix(got, generatedStamp(total-limit)) {
		t.Errorf("the first item is %s, want the oldest of the newest %d changes (%s)",
			got, limit, generatedStamp(total-limit))
	}
}

// streamRequest is a `timeline … -o jsonl` for the generated fixture.
func streamRequest(limit int, reverse bool) cli.TimelineRequest {
	request := defaultRequest()
	request.Structured = render.StructuredJSONL
	request.Limit = limit
	request.Reverse = reverse
	return request
}

// runStream drives the command against a generated history, writing to out.
func runStream(t *testing.T, engine *generatedEngine, request cli.TimelineRequest, out io.Writer) error {
	t.Helper()

	backend := &cli.Backend{Engine: engine, ClusterID: fixtureCluster}
	return cli.RunTimeline(context.Background(), backend, request,
		ioStreams(out, io.Discard), render.Options{Width: goldenWidth})
}

// generatedStamp is the second a generated change at index falls on.
//
// The generated history is one change a second, oldest at index zero, which makes
// a timestamp readable back as a position — the property the ordering assertions
// above rely on.
func generatedStamp(index int) string {
	return generatedBase.Add(time.Duration(index) * time.Second).UTC().Format("2006-01-02T15:04:05")
}

// generatedBase is where the generated history starts.
var generatedBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// generatedEngine answers a timeline from a generator rather than from a slice.
//
// Nothing here holds a change once it has been handed over, which is what lets the
// tests above measure the *command's* memory rather than the fixture's.
type generatedEngine struct {
	caps  query.Capabilities
	total int

	// observe is called with each change's index just before it is produced, so a
	// test can assert what the writer has already received.
	observe func(index int)

	// limitAware makes the iterator stop at the query's limit, as a real backend
	// does. It is opt-in because the streaming tests want every change read.
	limitAware bool

	// read counts the changes actually produced.
	read int
}

func (e *generatedEngine) Capabilities() query.Capabilities { return e.caps }

func (e *generatedEngine) Close() error { return nil }

func (e *generatedEngine) Timeline(_ context.Context, q query.TimelineQuery) (query.ChangeIterator, error) {
	total := e.total
	if e.limitAware && q.Limit > 0 && q.Limit < total {
		total = q.Limit
	}
	return &generatedIterator{engine: e, total: total, reverse: q.Reverse}, nil
}

func (e *generatedEngine) StateAt(
	_ context.Context, _ query.ObjectRef, _ time.Time, _ string,
) (*query.Reconstruction, error) {
	return nil, query.ErrObjectNotFound
}

func (e *generatedEngine) Coverage(_ context.Context, _ query.ScopeQuery) ([]query.ScopeInterval, error) {
	return watchedSince("2025-12-01T00:00:00Z", "ClusterStreamRule/all-workloads"), nil
}

func (e *generatedEngine) Incarnations(
	_ context.Context, _ query.ObjectRef, _, _ time.Time,
) ([]query.Incarnation, error) {
	return []query.Incarnation{{
		UID:       fixtureUID,
		FirstSeen: generatedBase,
		LastSeen:  generatedBase.Add(time.Duration(e.total) * time.Second),
	}}, nil
}

// generatedIterator produces changes one at a time, newest first when asked to.
type generatedIterator struct {
	engine  *generatedEngine
	total   int
	reverse bool
	at      int
	closed  bool
}

func (i *generatedIterator) Next() bool {
	if i.at >= i.total {
		return false
	}
	if i.engine.observe != nil {
		i.engine.observe(i.at)
	}
	i.at++
	i.engine.read++
	return true
}

// Change builds the change at the cursor, fresh each time.
//
// A fresh value rather than a slot in a slice is the whole point of this fake: it
// is also what the contract requires, since ChangeIterator.Change promises the
// caller may keep what it is handed.
func (i *generatedIterator) Change() query.Change {
	index := i.at - 1
	if i.reverse {
		index = i.engine.total - i.at
	}
	return query.Change{
		TS:              generatedBase.Add(time.Duration(index) * time.Second),
		EventType:       query.EventModified,
		Actors:          []string{"kube-controller-manager"},
		UID:             fixtureUID,
		ResourceVersion: fmt.Sprint(1000 + index),
		APIVersion:      "apps/v1",
		Diff: fmt.Sprintf(`[{"op":"replace","path":"/spec/replicas","value":%d},`+
			`{"op":"replace","path":"/metadata/annotations/padding","value":%q}]`,
			index%9+1, strings.Repeat("x", itemBytes)),
	}
}

func (i *generatedIterator) Err() error { return nil }

func (i *generatedIterator) Close() error {
	i.closed = true
	return nil
}

// lineCounter counts newline-terminated writes without keeping any of them.
type lineCounter struct{ seen int }

func (w *lineCounter) Write(p []byte) (int, error) {
	w.seen += bytes.Count(p, []byte("\n"))
	return len(p), nil
}

func (w *lineCounter) lines() int { return w.seen }

// lineParser parses each line as it is written and keeps nothing but counters.
//
// It reassembles across writes rather than assuming one write per line: the
// assertion is about the *format* being line-delimited JSON, and a test that only
// worked because the writer happened to flush whole lines would be asserting the
// implementation instead.
type lineParser struct {
	t       *testing.T
	partial bytes.Buffer

	lines int
	items int
	head  map[string]any
}

func (w *lineParser) Write(p []byte) (int, error) {
	written := len(p)
	for {
		cut := bytes.IndexByte(p, '\n')
		if cut < 0 {
			w.partial.Write(p)
			return written, nil
		}
		w.partial.Write(p[:cut])
		w.consume(w.partial.Bytes())
		w.partial.Reset()
		p = p[cut+1:]
	}
}

// consume parses one line and forgets it.
func (w *lineParser) consume(line []byte) {
	w.t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		w.t.Fatalf("line %d is not valid JSON: %v\n%s", w.lines+1, err, line)
	}
	w.lines++
	if w.lines == 1 {
		w.head = decoded
		return
	}
	if _, present := decoded["ts"]; present {
		w.items++
	}
}
