//go:build integration

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

// The newest-first walk, measured where a listing costs a round trip.
//
// # Why this runs here as well as over a directory
//
// The engine's own package proves the short circuit four ways, and every one of
// those runs against the local filesystem. Correctness is source-independent, so
// nothing there needs repeating: an equivalence that holds over a directory holds
// over a bucket, because the seam between them is pinned by its own suite.
//
// The *claim* is not source-independent. "Ninety days read as three partitions
// instead of two thousand" is a claim about round trips against an object store, and
// demonstrating it where a listing is a readdir demonstrates the arithmetic rather
// than the saving. This is the same measurement against a real store: the same
// question asked with and without a limit, over a bucket holding enough hour
// partitions for the step schedule to engage, counting the objects each one opened.
//
// The straddling case runs here too, and for a reason of its own. It is the case
// where an object's partition and its records disagree, the ceiling that handles it
// is the walk's only defence against returning a plausible timeline missing its
// newest rows, and every fixture that has exercised it so far was a directory laid
// out by hand. Here it is a real bucket, seeded through the shared archive fixture.
//
// # What it costs
//
// A little over two hundred small PUTs and about as many GETs against a container on
// the loopback, plus one bucket created and emptied: about a second measured, against
// a budget of roughly a minute for this case. The seeding is what a future author
// sizing a fixture should watch — the object count is the round-trip count, both on
// the way in and on the way out, and the bucket teardown pays it a third time — so
// the partition and per-partition counts below are named constants and the ratio they
// produce is stated beside them.

package awssource

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// The fixture's shape, and the ratio it exists to produce.
//
// Thirty hour partitions of five objects each is a hundred and fifty objects. A
// reverse-limited walk over it settles on the newest three partitions — the first
// step is one partition, the second is two, and the default object span puts the
// ceiling inside the second step — so it opens fifteen. Ten to one, which is the
// order of magnitude this case is here to show; a handful would prove nothing that
// arithmetic on a laptop had not already.
const (
	itSpreadHours    = 30
	itSpreadPerHour  = 5
	itSpreadObjects  = itSpreadHours * itSpreadPerHour
	itSpreadMinRatio = 10
)

// itSpreadEpoch is where the spread fixture starts: midday, so that thirty hours
// straddle two dates without either of them being covered end to end.
//
// That matters to what is being measured. A day covered from hour 00 to hour 23 is
// listed as one date= prefix rather than twenty-four hour= ones, which is the right
// thing for a scan and would collapse the very partitions whose count this case is
// about.
func itSpreadEpoch() time.Time { return time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC) }

// itSpreadLast is the instant of the fixture's newest record.
//
// The window ends a minute after it rather than on a round hour, so the newest
// partition in range is the one holding records. A window ending on an empty hour
// would spend the first step on nothing and make the object count say less than it
// looks like it says.
func itSpreadLast() time.Time {
	return itSpreadEpoch().Add((itSpreadHours-1)*time.Hour + itSpreadPerHour*10*time.Minute)
}

// itRef is the object every fixture here records history for.
func itRef() query.ObjectRef {
	return query.ObjectRef{
		ClusterID: "prod-eu-1",
		APIGroup:  "apps",
		Kind:      "Deployment",
		Namespace: "payments",
		Name:      "checkout",
	}
}

const itUID = "bbbbbbbb-0000-0000-0000-000000000002"

// TestIntegrationAReverseLimitedTimelineReadsFarFewerObjectsFromABucket measures the
// short circuit against a real object store, and checks it changed no answer.
func TestIntegrationAReverseLimitedTimelineReadsFarFewerObjectsFromABucket(t *testing.T) {
	ctx := t.Context()
	bucket := newITBucket(ctx, t)
	client := itClient(ctx, t, itSecretKey())

	t.Run("the walk opens an order of magnitude fewer objects", func(t *testing.T) {
		const prefix = "audit-spread"
		seedHistory(ctx, t, client, bucket, prefix, itSpreadHistory())

		q := query.TimelineQuery{
			Ref:     itRef(),
			From:    itSpreadEpoch(),
			To:      itSpreadLast().Add(time.Minute),
			Reverse: true,
			Limit:   3,
		}

		limited, limitedCount := itEngine(ctx, t, bucket, prefix, objectsource.Options{Prefix: prefix})
		short := drainTimeline(ctx, t, limited, q)

		// The reference is a real unlimited reverse query rather than a test-only
		// bypass, for the reason shortcircuit_test.go states: the property is that the
		// two paths agree, so one of them has to be the path a caller takes.
		full, fullCount := itEngine(ctx, t, bucket, prefix, objectsource.Options{Prefix: prefix})
		unlimited := q
		unlimited.Limit = 0
		want := drainTimeline(ctx, t, full, unlimited)
		if len(want) > q.Limit {
			want = want[:q.Limit]
		}
		assertSameITChanges(t, short, want)

		opened, wholeScan := limitedCount.opens(), fullCount.opens()
		if wholeScan != itSpreadObjects {
			t.Fatalf("the full scan opened %d objects, want the whole fixture's %d; the comparison "+
				"below means nothing if the baseline is not a whole scan", wholeScan, itSpreadObjects)
		}
		t.Logf("against %s the reverse-limited walk opened %d objects and listed %d partitions, "+
			"where the full scan opened %d and listed %d",
			itEndpoint(), opened, limitedCount.lists(), wholeScan, fullCount.lists())

		if opened*itSpreadMinRatio > wholeScan {
			t.Errorf("the reverse-limited walk opened %d of the window's %d objects against a real "+
				"bucket, short of the %dx this fixture is sized for. Every one of those is a round "+
				"trip rather than a readdir, and this ratio is the whole of what evaluation mode "+
				"promises: the flagship command's default is exactly this query",
				opened, wholeScan, itSpreadMinRatio)
		}
	})

	t.Run("a record stamped into the next hour is not lost", func(t *testing.T) {
		// An object's partition comes from its first record and it keeps accepting
		// records until it rotates, so an object filed under hour=17 legitimately
		// carries a record stamped 18:30. The ceiling the walk stops on is widened by
		// one object span for exactly this, and until now that has only ever been
		// exercised against a directory laid out by hand.
		const prefix = "audit-straddle"
		hourSeventeen := itSpreadEpoch().Add(5 * time.Hour)
		hourEighteen := hourSeventeen.Add(time.Hour)

		// Three ordinary objects in hour=18, placed by the fixture's own derivation…
		ordinary := conformance.History{}
		for i, at := range []time.Duration{5 * time.Minute, 10 * time.Minute, 15 * time.Minute} {
			ordinary.Rows = append(ordinary.Rows, itRow(hourEighteen.Add(at), i+1))
		}
		seedHistory(ctx, t, client, bucket, prefix, ordinary)

		// …and one object filed in hour=17 that was still accepting records at 18:30,
		// inside the writer's one-hour rotation ceiling. This is an archive an operator
		// really produces, and the derivation cannot express it.
		if err := archivetest.WriteObjectAt(itPut(ctx, t, client, bucket), prefix, "straddler",
			hourSeventeen, []conformance.Row{
				itRow(hourSeventeen.Add(45*time.Minute), 4),
				itRow(hourEighteen.Add(30*time.Minute), 5),
			}); err != nil {
			t.Fatalf("seeding the straddling object: %v", err)
		}

		q := query.TimelineQuery{
			Ref:     itRef(),
			From:    hourSeventeen.Add(-time.Hour),
			To:      hourEighteen.Add(59 * time.Minute),
			Reverse: true,
			Limit:   2,
		}

		limited, _ := itEngine(ctx, t, bucket, prefix, objectsource.Options{Prefix: prefix})
		got := drainTimeline(ctx, t, limited, q)

		full, _ := itEngine(ctx, t, bucket, prefix, objectsource.Options{Prefix: prefix})
		unlimited := q
		unlimited.Limit = 0
		want := drainTimeline(ctx, t, full, unlimited)
		if len(want) > q.Limit {
			want = want[:q.Limit]
		}
		assertSameITChanges(t, got, want)

		newest := hourEighteen.Add(30 * time.Minute)
		if len(got) == 0 || !got[0].TS.Equal(newest) {
			t.Fatalf("the newest change came back as %v, want %s — the record in the hour=%s object "+
				"that belongs to the hour after it. A walk that stopped as soon as its limit was "+
				"filled from the partitions it had read would return the two rows before it, in "+
				"order, plausibly, and with the actual newest change missing",
				itInstants(got), newest.Format(time.RFC3339), hourSeventeen.UTC().Format("15"))
		}
	})
}

// itSpreadHistory is one object's history across itSpreadHours consecutive hour
// partitions, itSpreadPerHour changes in each.
//
// One change per object is the fixture's own rule (see archivetest), so the partition
// count and the object count are both what they say, and "how many objects did the
// walk open" is a number with a meaning.
func itSpreadHistory() conformance.History {
	history := conformance.History{Rows: make([]conformance.Row, 0, itSpreadObjects)}
	n := 0
	for h := range itSpreadHours {
		for j := range itSpreadPerHour {
			at := itSpreadEpoch().Add(time.Duration(h)*time.Hour + time.Duration(j+1)*10*time.Minute)
			history.Rows = append(history.Rows, itRow(at, n))
			n++
		}
	}
	return history
}

// itRow is one recorded change of the fixture object at an instant.
func itRow(at time.Time, n int) conformance.Row {
	return conformance.Row{
		Ref: itRef(),
		Change: query.Change{
			TS:              at,
			EventType:       query.EventAdded,
			UID:             itUID,
			ResourceVersion: fmt.Sprintf("%d", 1000+n),
			APIVersion:      "apps/v1",
			Actors:          []string{"kubectl"},
			Data:            fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, n),
			SHA256:          fmt.Sprintf("%064d", n),
		},
	}
}

// itPut writes one archive object into the bucket, which is the whole of what seeding
// an archive needs.
func itPut(ctx context.Context, t *testing.T, client *awss3.Client, bucket string) archivetest.Put {
	t.Helper()

	return func(key string, body []byte) error {
		putObject(ctx, t, client, bucket, key, string(body))
		return nil
	}
}

// seedHistory writes a history into the bucket under prefix.
func seedHistory(
	ctx context.Context, t *testing.T, client *awss3.Client, bucket, prefix string,
	history conformance.History,
) {
	t.Helper()

	if _, err := archivetest.Write(itPut(ctx, t, client, bucket), prefix, history); err != nil {
		t.Fatalf("seeding %d rows under %q: %v", len(history.Rows), prefix, err)
	}
}

// itEngine builds an engine over the bucket, behind a source that counts what it was
// asked for.
//
// The counting lives in a wrapper at the ObjectSource seam rather than inside the
// shipped source, so everything under measurement is the code that ships — the same
// arrangement the conformance run's injected failure uses, for the same reason.
func itEngine(
	ctx context.Context, t *testing.T, bucket, prefix string, opts objectsource.Options,
) (*objectsource.Engine, *countingSource) {
	t.Helper()

	source, err := New(ctx, itConfig(bucket, itSecretKey()))
	if err != nil {
		t.Fatalf("building a source for bucket %q: %v", bucket, err)
	}
	counting := &countingSource{inner: source}
	t.Cleanup(func() {
		if closeErr := counting.Close(); closeErr != nil {
			t.Errorf("closing the source: %v", closeErr)
		}
	})

	engine, err := objectsource.NewEngine(counting, opts)
	if err != nil {
		t.Fatalf("building an engine over %q/%q: %v", bucket, prefix, err)
	}
	t.Cleanup(func() {
		if closeErr := engine.Close(); closeErr != nil {
			t.Errorf("closing the engine: %v", closeErr)
		}
	})
	return engine, counting
}

// countingSource counts the listings and fetches a query performed, wrapping the
// shipped source so that everything but the counters is the code that ships.
type countingSource struct {
	inner objectsource.ObjectSource

	listCount atomic.Int64
	openCount atomic.Int64
}

func (s *countingSource) List(ctx context.Context, prefix string) objectsource.ObjectIterator {
	s.listCount.Add(1)
	return s.inner.List(ctx, prefix)
}

func (s *countingSource) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.openCount.Add(1)
	return s.inner.Open(ctx, key)
}

func (s *countingSource) Close() error { return s.inner.Close() }

func (s *countingSource) lists() int { return int(s.listCount.Load()) }
func (s *countingSource) opens() int { return int(s.openCount.Load()) }

// Compile-time proof that the wrapper is a source like any other.
var _ objectsource.ObjectSource = (*countingSource)(nil)

// drainTimeline runs a timeline query the way the contract documents: drain, close on
// every path, and check Err after the loop.
func drainTimeline(
	ctx context.Context, t *testing.T, engine *objectsource.Engine, q query.TimelineQuery,
) []query.Change {
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
	if err := it.Err(); err != nil {
		t.Fatalf("the iterator failed mid-stream: %v", err)
	}
	return changes
}

// assertSameITChanges compares two answers row by row, on content and not only on
// instants.
//
// The distinction earns its keep for the same reason it does over a directory: the
// short circuit reads a different set of objects from the full scan, so a row
// attributed to the wrong incarnation or carrying another observation's state would
// still be at the right instant and in the right position.
func assertSameITChanges(t *testing.T, got, want []query.Change) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("the reverse-limited timeline returned %d changes, want the %d the full scan "+
			"returned:\n got: %v\nwant: %v", len(got), len(want), itInstants(got), itInstants(want))
	}
	for i := range want {
		a, b := got[i], want[i]
		if a.TS.Equal(b.TS) && a.UID == b.UID && a.ResourceVersion == b.ResourceVersion &&
			a.EventType == b.EventType && a.SHA256 == b.SHA256 && a.Data == b.Data &&
			a.Diff == b.Diff && slices.Equal(a.Actors, b.Actors) {
			continue
		}
		t.Fatalf("the reverse-limited timeline differs from the full scan at row %d:\n"+
			" got: %s rv=%s uid=%s\nwant: %s rv=%s uid=%s", i,
			a.TS.Format(time.RFC3339Nano), a.ResourceVersion, a.UID,
			b.TS.Format(time.RFC3339Nano), b.ResourceVersion, b.UID)
	}
}

// itInstants renders an answer as its timestamps, which is what a failure message
// about ordering or completeness is actually about.
func itInstants(changes []query.Change) []string {
	out := make([]string, 0, len(changes))
	for _, c := range changes {
		out = append(out, c.TS.UTC().Format(time.RFC3339Nano))
	}
	return out
}
