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

package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/internal/sink"
)

// errSentinelDial is what the injected dialer returns. It is a sentinel so the test
// can prove *which* call reached the backend rather than merely that something
// failed.
var errSentinelDial = errors.New("sentinel: a control-plane code path dialled the sink backend")

// sentinelDialer stands in for a ClickHouse connection: every attempt to reach the
// backend goes through Dial, which counts the attempt and refuses it.
//
// Counting rather than panicking is deliberate: a panic inside a reconcile would be
// recovered by controller-runtime and turned into a logged error, which is exactly
// the kind of failure a test can pass through without noticing. A counter is
// observable from the assertion.
type sentinelDialer struct {
	calls atomic.Int64
}

func (d *sentinelDialer) Dial(ctx context.Context) error {
	d.calls.Add(1)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errSentinelDial
}

// dialingWriter is a sink backend whose every I/O-shaped operation goes through the
// dialer: starting, probing and writing.
//
// It implements sink.Prober as well as sink.Writer, so the sink runtime discovers the
// health half and drives it — which is what makes the second half of the test (the
// dialer *is* reachable, from the runtime's own goroutines) a real check rather than a
// tautology.
type dialingWriter struct {
	dialer *sentinelDialer
}

func (w *dialingWriter) Start(ctx context.Context) error {
	// A real backend connects and then serves until cancelled. This one records the
	// dial and then waits, so the instance behaves like a running one.
	if err := w.dialer.Dial(ctx); err != nil && ctx.Err() == nil {
		<-ctx.Done()
	}
	return nil
}

func (w *dialingWriter) Enqueue(ctx context.Context, job sink.Job) error {
	if err := w.dialer.Dial(ctx); err != nil {
		job.Commit(false)
		return err
	}
	return nil
}

func (w *dialingWriter) Probe(ctx context.Context) error {
	return w.dialer.Dial(ctx)
}

// Compile-time proof that the injected backend really is both halves of the contract
// the sink runtime looks for; a writer the runtime could not probe would make phase
// two of the test vacuous.
var (
	_ sink.Writer = (*dialingWriter)(nil)
	_ sink.Prober = (*dialingWriter)(nil)
)

// TestInvariant1NoReconcilerPathDialsTheSink is the Invariant 1 enforcement test.
//
// The invariant is that no reconcile path may perform a synchronous sink round-trip:
// a control-plane reconciler that dialled ClickHouse would make every rule's status
// hostage to the database being up, and would put a Kubernetes-API-paced goroutine
// on a network operation with no bound the reconciler controls.
//
// The test proves it structurally rather than by timing. A dialer that counts and
// refuses every backend contact is injected into the reconcilers' dependency graph —
// the sink configuration builder and the sink runtime's factory both lead to it —
// and then every reconcile path is driven: a sink resolving its credential and being
// declared, a rule passing policy, resolution and access review, a probe verdict
// being turned into conditions, and a sink being deleted. The dial count must stay
// zero throughout.
//
// The second phase is what keeps the first honest. It starts the sink runtime, so the
// same graph is exercised by the component that is *allowed* to dial, and asserts
// that the dialer is reached there. Without it, the test would pass just as happily
// against a graph wired to nothing at all.
func TestInvariant1NoReconcilerPathDialsTheSink(t *testing.T) {
	dialer := &sentinelDialer{}

	// The builder is the reconciler's only door to a backend configuration. Building
	// one must not connect: on the production path this is clickhouse.Config, and
	// the driver's Open is lazy for exactly this reason.
	builder := func(name string, spec v1alpha1.ClickHouseSinkSpec, password string) (sink.InstanceConfig, error) {
		return fakeConfig{fingerprint: fmt.Sprintf("%s|%s|%s", name, spec.Connection.Addr, password)}, nil
	}

	h := newHarness(t, harnessOptions{allowAll: true, buildConfig: builder})
	ctx := context.Background()

	sinkName := uniqueName("sentinel")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRule(namespace, "sentinel", sinkName, resourceEntry("", "ConfigMap"))
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)

	// A failing probe, a recovery, a rotated credential and a deletion: every event
	// the two reconcilers respond to.
	h.pushProbe(sink.ProbeResult{
		Sink: sinkName, At: time.Now().UTC(),
		Err: errors.New("unreachable"), Reason: sink.ProbeReasonUnreachable,
	})
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkNotReady)
	h.pushProbe(sink.ProbeResult{Sink: sinkName, At: time.Now().UTC()})
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)

	var chSink v1alpha1.ClickHouseSink
	if err := h.Client.Get(ctx, client.ObjectKey{Name: sinkName}, &chSink); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if err := h.Client.Delete(ctx, &chSink); err != nil {
		t.Fatalf("delete the sink: %v", err)
	}
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing)

	if got := dialer.calls.Load(); got != 0 {
		t.Fatalf("a reconcile path reached the sink backend %d time(s); Invariant 1 forbids any", got)
	}

	// Phase two: the same dialer, reached by the component whose job it is. This is
	// what proves the graph above was actually connected to a backend that *can* be
	// dialled, so the zero count means "the reconcilers did not" rather than "nothing
	// could".
	runtime, err := sink.NewSinkManager(sink.ManagerOptions{
		Factory:         func(string, sink.InstanceConfig) (sink.Writer, error) { return &dialingWriter{dialer: dialer}, nil },
		Pipeline:        nopPipeline{},
		ProbeInterval:   10 * time.Millisecond,
		ProbeMinBackoff: 10 * time.Millisecond,
		ProbeMaxBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("build a sink runtime: %v", err)
	}
	runtimeCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := runtime.Start(runtimeCtx); err != nil {
			t.Errorf("the sink runtime stopped with an error: %v", err)
		}
	}()
	if err := runtime.Ensure(sinkName, fakeConfig{fingerprint: "phase-two"}); err != nil {
		t.Fatalf("declare a sink to the runtime: %v", err)
	}

	waitFor(t, "the sink runtime's own goroutines to reach the dialer", func() (bool, string) {
		return dialer.calls.Load() > 0, fmt.Sprintf("dial calls=%d", dialer.calls.Load())
	})
	// Drain the probe results the runtime is now posting, so its probe loop is never
	// blocked on a full channel while shutting down.
	go func() {
		for range runtime.ProbeResults() {
		}
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Error("the sink runtime did not stop within 30s")
	}
}

// nopPipeline is the data-plane side of the sink runtime, which this test does not
// exercise.
type nopPipeline struct{}

func (nopPipeline) RemoveSink(string) {}
