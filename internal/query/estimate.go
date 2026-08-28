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

import (
	"context"
	"time"
)

// ScanEstimate is what a question will cost to answer, measured before it is
// asked.
//
// It exists because [Capabilities.PointQuery] being false has a consequence a
// caller has to be able to *quantify*, not merely mention. An engine that cannot
// seek to one object's history reads the whole window that history lands in, and
// a window a person picked casually can be three months of a busy cluster. Told
// nothing, they wait; told "1,240 units, 3.1 GiB", they narrow the window or say
// yes on purpose. That is the difference between a trade being honest and a
// command appearing to hang.
//
// Every field is a *pre-scan* figure, derived from what a listing reports. It is
// therefore an estimate in one specific sense: it says how much storage the scan
// must fetch, not how many changes will come back, and not how long it will take.
type ScanEstimate struct {
	// Objects is how many stored units the scan must fetch — whatever the
	// backend's unit of retrieval is, counted as the listing counts them.
	Objects int64 `json:"objects"`

	// Bytes is their total size *as stored*, which for a compressed archive is
	// bytes off the wire rather than bytes decoded. It is the figure a caller
	// renders, because it is the one that predicts the wait and the egress bill,
	// and the only one a listing can supply.
	Bytes int64 `json:"bytes"`

	// Partitions is how many storage partitions the window resolved to after
	// pruning. It is reported so that a caller can show *why* an estimate is large
	// — a wide window rather than a busy cluster — and so that a pruning
	// regression is visible in output rather than only in a timing.
	Partitions int `json:"partitions"`
}

// ScanEstimator is the optional half of the read plane for engines whose answers
// cost a scan.
//
// It is a separate interface rather than a method on [QueryEngine] because most
// engines have nothing useful to say here: an engine that seeks to one object's
// rows would have to invent a figure or return zero, and a zero a caller cannot
// distinguish from "cheap" is worse than an absent estimate. So the capability is
// detected, exactly as the write side detects its own optional halves:
//
//	if estimator, ok := engine.(query.ScanEstimator); ok {
//	        ...
//	}
//
// An engine implementing it must not open any stored unit to answer: the estimate
// exists to be shown *before* the scan, and one that paid a fraction of the scan
// to produce would be charging for the warning.
type ScanEstimator interface {
	// EstimateScan reports what scanning [from, to] of one cluster's history will
	// cost.
	//
	// The window is the caller's own, unwidened: an engine that widens a range to
	// stay correct applies its own widening here too, so that the figure describes
	// the scan that will actually run rather than the one that was asked for.
	//
	// Errors: whatever prevented the listing. An empty archive is not an error —
	// it is a zero estimate, and the distinction between "nothing to read" and
	// "could not be read" is one the caller must keep (Invariant 4).
	EstimateScan(ctx context.Context, clusterID string, from, to time.Time) (ScanEstimate, error)
}
