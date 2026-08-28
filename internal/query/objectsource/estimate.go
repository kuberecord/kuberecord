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
	"fmt"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// EstimateScan reports what scanning a window of one cluster's history will cost.
//
// It is the honest half of declaring PointQuery false. This engine has no index, so
// a question about one object costs the partitions its window lands in — and a person
// who picked "the last 90 days" without thinking about it deserves to be told that
// before the wait rather than during it.
//
// # It opens nothing
//
// Every figure comes from the listing: a key count and the sizes the listing already
// reports. That is a requirement rather than an optimisation — an estimate that
// fetched a sample would charge a fraction of the scan for the warning about the
// scan, which is the sort of cost people learn to skip.
//
// The consequence is what Bytes means: stored bytes, so compressed. It is the figure
// that predicts the wait and the egress, and the only one a listing can supply; the
// decoded volume is several times larger and is not knowable without decoding.
//
// # It measures the scan that will run
//
// The window is widened here exactly as a timeline widens it, so the count describes
// the partitions that will really be listed rather than the ones that were asked
// for. An estimate that quietly described a smaller scan than the one about to
// happen would be worse than none, because it would be believed.
//
// It is an upper bound in one direction only, and deliberately so: a reverse-limited
// timeline stops as soon as its answer is settled and may read a fraction of these
// partitions (see Engine.scanNewestFirst). Erring on the high side is the safe half of
// that — a caller warned about a scan that then finished early has lost nothing, while
// one told a 90-day scan was cheap because a *different* query would have short-
// circuited it has been told something false about the query it actually asked.
func (e *Engine) EstimateScan(
	ctx context.Context, clusterID string, from, to time.Time,
) (query.ScanEstimate, error) {
	if err := e.ensureOpen(); err != nil {
		return query.ScanEstimate{}, err
	}
	if err := requireWindow(from, to); err != nil {
		return query.ScanEstimate{}, fmt.Errorf("estimating a scan of cluster %q: %w", clusterID, err)
	}

	prefixes := e.recordPrefixes(clusterID, from, to)
	measured := make([]partitionSize, len(prefixes))
	errs := make([]error, len(prefixes))

	cancelled := waitAll(ctx, e.concurrency, len(prefixes), func(i int) {
		measured[i], errs[i] = e.measure(ctx, prefixes[i])
	})
	if cancelled != nil {
		// An interrupted estimate must not come back as a small one. Every prefix whose
		// listing never ran left a zero in its slot with no error beside it, so a
		// cancellation reported as success would be an estimate of *nothing* — handed to
		// a caller in the act of deciding whether the scan is affordable (Invariant 4).
		//
		// It is reported ahead of the per-prefix failures for the reason listObjectKeys
		// gives: under a cancellation those are whichever listings were in flight.
		return query.ScanEstimate{}, fmt.Errorf("estimating a scan of cluster %q: %w",
			clusterID, abandoned(cancelled))
	}
	for _, err := range errs {
		if err != nil {
			// An unreadable listing is reported rather than estimated around. A zero
			// estimate would be read as "this is cheap" and the scan behind it would
			// then fail anyway, having promised otherwise.
			return query.ScanEstimate{}, fmt.Errorf("estimating a scan of cluster %q: %w", clusterID, err)
		}
	}

	estimate := query.ScanEstimate{Partitions: len(prefixes)}
	for _, part := range measured {
		estimate.Objects += part.objects
		estimate.Bytes += part.bytes
	}
	return estimate, nil
}

// partitionSize is one partition's contribution to an estimate.
type partitionSize struct {
	objects int64
	bytes   int64
}

// measure counts the objects under one prefix and adds up their stored sizes.
func (e *Engine) measure(ctx context.Context, prefix string) (size partitionSize, err error) {
	it := e.src.List(ctx, prefix)
	defer closeListing(it, &err)

	for it.Next() {
		object := it.Object()
		if !isObjectKey(object.Key) {
			continue
		}
		size.objects++
		size.bytes += object.Size
	}
	if listErr := it.Err(); listErr != nil {
		return partitionSize{}, fmt.Errorf("listing %q: %w", prefix, listErr)
	}
	return size, nil
}
