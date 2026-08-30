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

package query

// ScanProgress is how far a scan has got, reported while it is still running.
//
// It is the companion of [ScanEstimate] and is measured in the same units for a
// reason: the estimate is the denominator a caller renders the progress against,
// and two figures counted differently would produce a bar that finished at 60%
// or ran past 100%. Both count stored units and stored bytes — what the listing
// counted, and what came off the wire.
//
// A scan that reports nothing is indistinguishable from a hang, and the tool that
// appears to hang is the one people stop trusting with wide windows. That is the
// whole of why this exists: [Capabilities.PointQuery] being false means a
// single-object question costs the window around it, and the honest response to
// an expensive answer is to show it being earned rather than to go quiet.
type ScanProgress struct {
	// Objects is how many stored units this scan has fetched so far.
	Objects int64 `json:"objects"`

	// Bytes is how many bytes those fetches have read, as stored — so for a
	// compressed archive, bytes off the wire rather than bytes decoded. It is the
	// same measure [ScanEstimate.Bytes] carries, which is what makes the two
	// comparable.
	Bytes int64 `json:"bytes"`
}

// ScanProgressReporter is the optional half of the read plane for engines whose
// answers take long enough to need narrating.
//
// It is capability-detected exactly as [ScanEstimator] is, and for the same
// reason: an engine that answers from an index has nothing to report, and a
// progress callback that fired once with a final total would be a spinner
// pretending to be a measurement.
//
//	if reporter, ok := engine.(query.ScanProgressReporter); ok {
//	        reporter.SetScanProgress(paint)
//	        defer reporter.SetScanProgress(nil)
//	}
//
// A caller that installs one is buying two things with it. The first is the line
// on the terminal. The second is a circuit breaker: the only place a scan's size
// is known while it can still be stopped is here, so a caller enforcing a bound
// on the work — rather than on the answer — cancels its own context from this
// callback (see the CLI's --max-objects).
type ScanProgressReporter interface {
	// SetScanProgress installs report, replacing whatever was installed before. A
	// nil report disables reporting, and a caller that installed one must remove
	// it when it is finished — a closure that outlives the terminal line it paints
	// keeps painting.
	//
	// report is called from the goroutines performing the scan, and an engine must
	// serialize those calls: within one scan the figures never go backwards and the
	// two of them always describe the same instant. That is a promise to the caller
	// rather than an implementation detail, because the alternative is a progress
	// line that counts down while the tool insists it is making progress, which
	// reads as a defect in the tool. It costs one uncontended lock per stored unit,
	// against a unit that has just been fetched over a network.
	//
	// A caller still needs its own synchronization for anything the callback shares
	// with the goroutine that installed it — a terminal line that is also erased
	// when the scan ends, for instance — because those two are genuinely concurrent.
	//
	// The figures are *per scan*, reset each time the engine begins one, rather
	// than cumulative for the engine's lifetime. That is what keeps them comparable
	// with an estimate of the same window: one question often costs an engine
	// several passes over the same partitions — the incarnations, then the changes
	// — and a running total across all of them would exceed the estimate of any one
	// of them while describing none.
	SetScanProgress(report func(ScanProgress))
}
