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

// This file is why the suite can be trusted. A conformance suite is a claim about
// backends nobody has written yet, and the failure mode of such a claim is
// silence: a property that asserts nothing passes every backend, and the badge it
// hands out is worse than no badge at all, because it retires the scrutiny that
// would have caught the bug.
//
// So the suite is tested in both directions. TestWriterSuitePassesCompliantWriter
// shows the properties can be satisfied; TestWriterSuiteIsNonVacuous shows each
// one rejects a Writer built to violate it. Neither test alone means anything —
// a suite that always failed would pass the second and a suite that always
// passed would pass the first.
package conformance

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// propertyTimeout bounds one property run against a broken fixture. A property
// that hangs has not rejected anything, so the runner reports the hang separately
// rather than letting it count as a rejection.
const propertyTimeout = 90 * time.Second

// recordingT is a conformanceT that captures failures instead of reporting them,
// so a test can assert that a property failed.
//
// Fatalf must abandon the property the way testing does, or the code after a
// fatal assertion would run against state the assertion just declared broken.
// runtime.Goexit is how testing itself does it, and it still runs deferred calls —
// which is what lets a property's `defer r.stop()` shut the Writer down even when
// it gives up early.
type recordingT struct {
	mu       sync.Mutex
	failures []string
	logs     []string
}

func (r *recordingT) Helper() {}

func (r *recordingT) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recordingT) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}

func (r *recordingT) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures) > 0
}

// first is the first recorded failure, for the log line that shows *why* the
// property rejected the fixture — a property failing for an unrelated reason
// would prove nothing about the obligation it is supposed to enforce.
func (r *recordingT) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.failures) == 0 {
		return ""
	}
	return r.failures[0]
}

// runPropertyIsolated runs one property against one harness on its own goroutine
// (Fatalf needs to be able to abandon it) and reports what it recorded, plus
// whether it terminated at all.
func runPropertyIsolated(p property, h Harness) (*recordingT, bool) {
	rec := &recordingT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProperty(rec, p, h)
	}()
	select {
	case <-done:
		return rec, true
	case <-time.After(propertyTimeout):
		return rec, false
	}
}

// TestWriterSuitePassesCompliantWriter is the other half of the non-vacuity
// argument: the properties are satisfiable. fakeWriter with no fixture switches
// set is an ordinary, correct Writer, and the suite must have nothing to say
// about it.
func TestWriterSuitePassesCompliantWriter(t *testing.T) {
	RunWriterSuite(t, func(*testing.T) Harness { return newFakeHarness(fakeOpts{}) })
}

// TestWriterSuiteIsNonVacuous runs each property against a Writer that violates
// it and asserts the property fails.
//
// Every property in the table is covered by at least one fixture. A fixture may
// well fail properties beyond the ones listed — a Writer that lies about durability
// breaks several at once — so the list is what each fixture must *at minimum* be
// caught by, not an exhaustive account of its damage.
func TestWriterSuiteIsNonVacuous(t *testing.T) {
	fixtures := []struct {
		name    string
		opts    fakeOpts
		what    string
		catches []string
	}{
		{
			name: "doubleCommit",
			opts: fakeOpts{doubleCommit: true},
			what: "settles every job twice",
			catches: []string{
				propExactlyOnceSuccess,
				propExactlyOnceFailure,
				propExactlyOnceCancelled,
				propExactlyOnceDrain,
				propNoLostJobs,
				propStorm,
			},
		},
		{
			name:    "dropOnDrain",
			opts:    fakeOpts{dropOnDrain: true},
			what:    "abandons queued work at shutdown instead of flushing it",
			catches: []string{propExactlyOnceDrain, propDrainOrdering},
		},
		{
			name:    "lyingCommit",
			opts:    fakeOpts{lyingCommit: true},
			what:    "reports a refused write as durably written",
			catches: []string{propExactlyOnceFailure, propNoLostJobs},
		},
		{
			name:    "unboundedEnqueue",
			opts:    fakeOpts{unboundedEnqueue: true},
			what:    "blocks on a full queue past its own timeout",
			catches: []string{propEnqueueBounded},
		},
		{
			name:    "nonIdempotent",
			opts:    fakeOpts{nonIdempotent: true},
			what:    "re-stamps each record as it stores it, so a replay never collapses",
			catches: []string{propIdempotency},
		},
	}

	covered := map[string]bool{}
	for _, f := range fixtures {
		for _, name := range f.catches {
			covered[name] = true
		}
		t.Run(f.name, func(t *testing.T) {
			for _, name := range f.catches {
				t.Run(name, func(t *testing.T) {
					p, ok := propertyByName(name)
					if !ok {
						t.Fatalf("no property named %q; the fixture table names one the suite does not run", name)
					}
					rec, terminated := runPropertyIsolated(p, newFakeHarness(f.opts))
					if !terminated {
						t.Fatalf("%s did not terminate within %s against a writer that %s: a property that hangs "+
							"rejects nothing", name, propertyTimeout, f.what)
					}
					if !rec.failed() {
						t.Fatalf("%s passed against a writer that %s: the property asserts nothing about the "+
							"obligation it is named for", name, f.what)
					}
					t.Logf("%s rejected it: %s", name, truncate(rec.first(), 220))
				})
			}
		})
	}

	// A property with no fixture behind it is untested machinery: it could be
	// asserting nothing and this file would never notice.
	for _, p := range writerProperties() {
		if !covered[p.name] {
			t.Errorf("property %s has no fixture proving it can fail; add one to the table above", p.name)
		}
	}
}

// TestHarnessValidationRejectsIncompleteHarness covers the other way a backend
// could be certified without being tested: a harness that omits what the suite
// needs. Each omission must be fatal and must name the field.
func TestHarnessValidationRejectsIncompleteHarness(t *testing.T) {
	full := newFakeHarness(fakeOpts{})
	cases := []struct {
		name  string
		mutBy func(h *Harness)
		want  string
	}{
		{"noWriter", func(h *Harness) { h.Writer = nil }, "Harness.Writer"},
		{"noEvents", func(h *Harness) { h.Events = nil }, "Harness.Events"},
		{"noFault", func(h *Harness) { h.SetFault = nil }, "Harness.SetFault"},
		{"noLogicalKey", func(h *Harness) { h.LogicalKey = nil }, "Harness.LogicalKey"},
		{"noDedup", func(h *Harness) { h.Dedup = "" }, "Harness.Dedup"},
		{"noCapacity", func(h *Harness) { h.QueueCapacity = 0 }, "Harness.QueueCapacity"},
		{"shortTimeout", func(h *Harness) { h.EnqueueTimeout = time.Millisecond }, "Harness.EnqueueTimeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := full
			tc.mutBy(&h)
			rec := &recordingT{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.withDefaults().validate(rec)
			}()
			<-done
			if !rec.failed() {
				t.Fatalf("validate accepted a harness with no %s", tc.want)
			}
			if !strings.Contains(rec.first(), tc.want) {
				t.Fatalf("validate failed with %q, want it to name %s", rec.first(), tc.want)
			}
		})
	}
}

// TestDefaultSettleWithinApplies pins the one field a harness may leave zero.
func TestDefaultSettleWithinApplies(t *testing.T) {
	h := Harness{}.withDefaults()
	if h.SettleWithin != defaultSettleWithin {
		t.Fatalf("SettleWithin defaulted to %s, want %s", h.SettleWithin, defaultSettleWithin)
	}
	h = Harness{SettleWithin: time.Minute}.withDefaults()
	if h.SettleWithin != time.Minute {
		t.Fatalf("SettleWithin was overwritten to %s, want the harness's own minute", h.SettleWithin)
	}
}

// truncate keeps a captured failure message readable in the log line that reports
// which obligation caught a fixture.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
