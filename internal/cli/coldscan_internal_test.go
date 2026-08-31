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

// The units behind the cold-scan guard, tested from inside the package.
//
// Everything else in this package is tested through its commands, which is the
// right level for behaviour. These four are not behaviour: they are the arithmetic
// a person reads in the second before answering a yes/no question about a scan
// measured in gigabytes, plus the monitor's own bookkeeping, and driving them
// through a command would assert one case each.

package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// TestFormatCount: the separators are why the figure is legible at a glance.
func TestFormatCount(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{7, "7"},
		{999, "999"},
		{1000, "1,000"},
		{1240, "1,240"},
		{12400, "12,400"},
		{1234567, "1,234,567"},
		{1000000000, "1,000,000,000"},
		{-1240, "-1,240"},
	} {
		if got := formatCount(testCase.n); got != testCase.want {
			t.Errorf("formatCount(%d) = %q, want %q", testCase.n, got, testCase.want)
		}
	}
}

// TestFormatBytes: binary units, because that is what an object store reports and
// what the reader will compare this against in their bucket's console.
func TestFormatBytes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1 << 10, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{2 << 20, "2.0 MiB"},
		{3328599655, "3.1 GiB"},
		{5 << 40, "5.0 TiB"},
		{-1, "0 B"},
	} {
		if got := formatBytes(testCase.n); got != testCase.want {
			t.Errorf("formatBytes(%d) = %q, want %q", testCase.n, got, testCase.want)
		}
	}
}

// TestDescribeSpan: a window is rendered in the units --since accepts, so a
// sentence about one can be pasted back in as a flag.
func TestDescribeSpan(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		d    time.Duration
		want string
	}{
		{DefaultWindow, "1d"},
		{ConfirmWindow, "1w"},
		{90 * 24 * time.Hour, "90d"},
		{14 * 24 * time.Hour, "2w"},
		{6 * time.Hour, "6h"},
		{90 * time.Minute, "1h30m0s"},
		{0, "0s"},
		{-time.Hour, "0s"},
	} {
		if got := DescribeSpan(testCase.d); got != testCase.want {
			t.Errorf("DescribeSpan(%s) = %q, want %q", testCase.d, got, testCase.want)
		}
	}
}

// TestNeedsConfirmation pins where the line is drawn, including the two edges that
// decide whether a bare invocation prompts.
//
// The unmeasured rows are the inversion this guard used to have: a scan whose size
// could not be determined is a decision at any width, because the window is only a
// proxy for cost while the listing that measures it works.
func TestNeedsConfirmation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)
	archive := query.Capabilities{Backend: "objectsource", TimeBoundRequired: true}
	indexed := query.Capabilities{Backend: "clickhouse", PointQuery: true}

	measured := scanEstimate{figures: query.ScanEstimate{Objects: 1240}, known: true}
	unmeasured := scanEstimate{unmeasured: true}
	// An engine that never offered an estimator: neither known nor unmeasured, and
	// deliberately still governed by the width alone.
	silent := scanEstimate{}

	for name, testCase := range map[string]struct {
		capabilities query.Capabilities
		size         scanEstimate
		from, to     time.Time
		want         bool
	}{
		"the default window":    {archive, measured, now.Add(-DefaultWindow), now, false},
		"exactly the threshold": {archive, measured, now.Add(-ConfirmWindow), now, false},
		"a minute past it":      {archive, measured, now.Add(-ConfirmWindow - time.Minute), now, true},
		"ninety days":           {archive, measured, now.Add(-90 * 24 * time.Hour), now, true},
		"an unbounded end":      {archive, measured, now.Add(-time.Hour), time.Time{}, true},
		"an indexed backend":    {indexed, measured, now.Add(-90 * 24 * time.Hour), now, false},

		"an hour, unmeasured":     {archive, unmeasured, now.Add(-time.Hour), now, true},
		"a day, unmeasured":       {archive, unmeasured, now.Add(-DefaultWindow), now, true},
		"ninety days, unmeasured": {archive, unmeasured, now.Add(-90 * 24 * time.Hour), now, true},
		// Nothing to estimate and nothing to confirm: the guard stays keyed on the
		// declared capability, so a failed listing cannot invent work for an index.
		"an indexed backend, unmeasured": {indexed, unmeasured, now.Add(-90 * 24 * time.Hour), now, false},

		"a day, no estimator offered":       {archive, silent, now.Add(-DefaultWindow), now, false},
		"ninety days, no estimator offered": {archive, silent, now.Add(-90 * 24 * time.Hour), now, true},
	} {
		got := needsConfirmation(testCase.capabilities, testCase.size, testCase.from, testCase.to)
		if got != testCase.want {
			t.Errorf("%s: needsConfirmation = %t, want %t", name, got, testCase.want)
		}
	}
}

// TestConfirmPromptNamesAnUnknownSize is the half of the new gate a person reads.
//
// The measured prompt is asserted byte-for-byte because it is the one that has
// shipped: a reader has learned to skim the figures and a question mark, and the
// unmeasured case must not be paid for by rewording it.
func TestConfirmPromptNamesAnUnknownSize(t *testing.T) {
	t.Parallel()

	measured := scanEstimate{
		figures: query.ScanEstimate{Objects: 1240, Bytes: 3328599655}, known: true,
	}
	if got, want := confirmPrompt(measured), "~1,240 objects, ~3.1 GiB — continue? [y/N] "; got != want {
		t.Errorf("the measured prompt changed:\ngot  %q\nwant %q", got, want)
	}

	unmeasured := confirmPrompt(scanEstimate{unmeasured: true})
	if !strings.Contains(unmeasured, "could not be determined") {
		t.Errorf("the prompt does not say the size is unknown, so the user is being asked to decide "+
			"on the basis of a figure they were never given: %q", unmeasured)
	}
	if !strings.HasSuffix(unmeasured, "continue? [y/N] ") {
		t.Errorf("the unmeasured prompt does not end in the question: %q", unmeasured)
	}
}

// TestDescribeScanWidth: the sentence explaining an assumed confirmation has to
// name a width even when there is not one to compute.
func TestDescribeScanWidth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)

	for name, testCase := range map[string]struct {
		from, to time.Time
		want     string
		wantSpan string
	}{
		"a week":         {now.Add(-ConfirmWindow), now, "1w", " for 1w"},
		"no lower bound": {time.Time{}, now, "an unbounded window", ""},
		"no upper bound": {now.Add(-time.Hour), time.Time{}, "an unbounded window", ""},
		"inverted":       {now, now.Add(-time.Hour), "an unbounded window", ""},
	} {
		if got := describeScanWidth(testCase.from, testCase.to); got != testCase.want {
			t.Errorf("%s: describeScanWidth = %q, want %q", name, got, testCase.want)
		}
		if got := describeScanSpan(testCase.from, testCase.to); got != testCase.wantSpan {
			t.Errorf("%s: describeScanSpan = %q, want %q", name, got, testCase.wantSpan)
		}
	}
}

// TestScanMonitorThrottlesAndErases covers the two things that keep the progress
// line from becoming the output.
//
// It repaints at most ten times a second — a scan of small objects would otherwise
// spend its time on the terminal — and it blanks what it wrote, so that the notices
// after it start on a clean line.
func TestScanMonitorThrottlesAndErases(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	monitor := &scanMonitor{
		out:       &out,
		paint:     true,
		total:     query.ScanEstimate{Objects: 1240},
		known:     true,
		lastAt:    time.Now(),
		unpainted: true,
	}

	monitor.report(query.ScanProgress{Objects: 1, Bytes: 1 << 20})
	monitor.report(query.ScanProgress{Objects: 2, Bytes: 2 << 20})

	painted := out.String()
	if !strings.Contains(painted, "scanning 1/1,240 objects, 1.0 MiB read") {
		t.Errorf("the first report was not painted:\n%q", painted)
	}
	if strings.Contains(painted, "scanning 2/1,240") {
		t.Errorf("a second report within %s was painted; the throttle is what keeps a scan of small "+
			"objects off the terminal:\n%q", progressInterval, painted)
	}

	out.Reset()
	monitor.clear()
	if cleared := out.String(); !strings.HasPrefix(cleared, "\r") || strings.TrimSpace(cleared) != "" {
		t.Errorf("clear left something on the line: %q", cleared)
	}

	out.Reset()
	monitor.clear()
	if second := out.String(); second != "" {
		t.Errorf("clearing twice wrote %q the second time; it has to be idempotent, because stop "+
			"clears a line a tripped breaker may already have erased", second)
	}
}

// TestScanMonitorTripsOnceWithACauseThatNamesTheFlag is the breaker's contract
// with the error phrasing: the cancellation carries the reason, or the failure
// surfaces as "context canceled" and names nothing the user can act on.
func TestScanMonitorTripsOnceWithACauseThatNamesTheFlag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	trips := 0
	monitor := &scanMonitor{
		out: &bytes.Buffer{},
		cancel: func(cause error) {
			trips++
			cancel(cause)
		},
		limit:  2,
		lastAt: time.Now(),
	}

	monitor.report(query.ScanProgress{Objects: 2, Bytes: 10})
	if ctx.Err() != nil {
		t.Fatal("the breaker tripped at exactly its limit; --max-objects 2 permits two objects")
	}
	monitor.report(query.ScanProgress{Objects: 3, Bytes: 20})
	monitor.report(query.ScanProgress{Objects: 4, Bytes: 30})

	if trips != 1 {
		t.Errorf("the breaker fired %d times; a scan already told to stop must not be told again "+
			"once per object still in flight", trips)
	}

	stopped := scanStopped(ctx)
	if stopped == nil {
		t.Fatal("scanStopped reported nothing after a trip, so the failure would read as a bare " +
			"cancellation")
	}
	var limit *scanLimitError
	if !errors.As(stopped, &limit) {
		t.Fatalf("the cause is %T, want *scanLimitError", stopped)
	}
	for _, want := range []string{"--" + FlagMaxObjects + "=2", "3 objects", "--since"} {
		if !strings.Contains(limit.Error(), want) {
			t.Errorf("the breaker's message does not mention %q: %s", want, limit)
		}
	}
}

// TestScanStoppedIgnoresAnOrdinaryCancellation keeps the two reasons a scan can
// end apart.
//
// Ctrl-C and a tripped breaker both arrive as a cancelled context, and only one of
// them should produce a sentence about a flag the user may never have passed.
func TestScanStoppedIgnoresAnOrdinaryCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if stopped := scanStopped(ctx); stopped != nil {
		t.Errorf("an interrupted scan was attributed to the circuit breaker: %v", stopped)
	}
}
