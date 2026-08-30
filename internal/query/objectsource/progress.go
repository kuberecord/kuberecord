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
	"io"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Reporting a scan's progress: the other half of admitting there is no index.
//
// EstimateScan says what a question will cost before it is asked. This says how
// much of that cost has been paid while it is being answered, and the two are
// counted the same way — stored objects and stored bytes — because a caller
// renders one against the other. A percentage assembled from two different
// measures would be a number that looks precise and is not.

// progressSink holds the installed callback.
//
// A struct pointer rather than an atomic.Pointer over the function type directly:
// a func is not comparable and cannot be stored in an atomic.Pointer without a
// level of indirection anyway, and naming that indirection is cheaper to read than
// a pointer-to-func.
type progressSink struct {
	report func(query.ScanProgress)
}

// SetScanProgress installs the callback the read-plane contract's optional
// progress half defines. See query.ScanProgressReporter for the rules it carries.
//
// It is safe to call while a scan is running — the sink is read atomically on
// every object — though the ordinary shape is a caller installing one before its
// query and clearing it afterwards.
func (e *Engine) SetScanProgress(report func(query.ScanProgress)) {
	if report == nil {
		e.progress.Store(nil)
		return
	}
	e.progress.Store(&progressSink{report: report})
}

// beginScan resets the counters at the start of one scan.
//
// Per scan rather than per engine, which is the contract's own rule and is the
// half that makes the figures usable: one timeline costs this engine two passes
// over the same partitions — the incarnations, then the changes — and a caller
// rendering a running total against a one-window estimate would watch its own
// progress line run to two hundred per cent.
//
// It is deliberately not synchronized against a scan in flight. The engine's
// contract is one caller at a time, so a reset racing a scan is a caller doing
// something the engine never promised to support, and a lock here would be a
// lock on the hot side of every fetch for a case that cannot legitimately occur.
func (e *Engine) beginScan() {
	e.scanObjects.Store(0)
	e.scanBytes.Store(0)
}

// recordScanned counts one fetched object and the bytes it cost.
//
// It is called once per object from the goroutine that fetched it, on every path
// including a fetch that failed. Counting a failure is the deliberate half: an
// archive under a lifecycle rule can list an object that has already gone, and a
// progress line that stalled on it would report a hang where there was only a
// gap the scan is already recording (see readObject).
//
// # Why the lock
//
// The counting and the reporting happen together, under one lock, which is what
// makes the sequence a caller sees monotonic. Counting them atomically and
// reporting outside the lock is cheaper and produces a progress line that goes
// backwards: two fetches increment to five and six, and whichever calls back
// second is the one the terminal shows. A number that decreases while a tool
// insists it is making progress reads as a bug in the tool, which is the opposite
// of what narrating a scan is for.
//
// It costs a single uncontended mutex per fetched object, against an object that
// has just been fetched over a network and decompressed. It is not measurable, and
// it is only paid when somebody is watching: with no callback installed this is one
// atomic load and a branch.
func (e *Engine) recordScanned(bytes int64) {
	if e.progress.Load() == nil {
		return
	}

	e.progressMu.Lock()
	defer e.progressMu.Unlock()

	// Re-read under the lock: a caller is allowed to remove the callback while a
	// scan is running, and the removal must actually stop the reporting rather than
	// race with the check above.
	sink := e.progress.Load()
	if sink == nil {
		return
	}
	objects := e.scanObjects.Add(1)
	total := e.scanBytes.Add(bytes)
	sink.report(query.ScanProgress{Objects: objects, Bytes: total})
}

// countingReader tallies what was read through it.
//
// It counts *stored* bytes, because it wraps the body before the decompressor
// rather than after it: that is the figure an estimate can be built from a
// listing, and therefore the only one the two halves can agree on.
//
// It is not safe for concurrent use and does not need to be: one object is read
// by one goroutine, and the count is read only after that goroutine's decode has
// returned.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
