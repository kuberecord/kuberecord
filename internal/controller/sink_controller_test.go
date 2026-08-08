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
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// TestSinkCredentialResolution drives the CredentialsResolved condition through every
// shape of a missing or malformed credential, and proves the sink is never handed to
// the runtime until the credential is real — an instance built with an empty password
// would report itself unreachable for a reason that has nothing to do with the
// address it was given.
func TestSinkCredentialResolution(t *testing.T) {
	tests := []struct {
		name string
		// secret, when non-nil, is created in the operator namespace before the sink.
		secret func(namespace, name string) *corev1.Secret
		// refName overrides the Secret name the sink points at. Empty means the
		// sink's own name.
		refName    string
		wantStatus metav1.ConditionStatus
		wantReason string
		// wantDeclared is whether the sink runtime should have been given a
		// configuration.
		wantDeclared bool
	}{
		{
			name: "a Secret carrying the password resolves",
			secret: func(namespace, name string) *corev1.Secret {
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Data:       map[string][]byte{DefaultCredentialsSecretKey: []byte("hunter2")},
				}
			},
			wantStatus:   metav1.ConditionTrue,
			wantReason:   ReasonSecretResolved,
			wantDeclared: true,
		},
		{
			name:       "a missing Secret degrades the sink",
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretNotFound,
		},
		{
			name: "a Secret without the password key degrades the sink",
			secret: func(namespace, name string) *corev1.Secret {
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Data:       map[string][]byte{"user": []byte("kuberecord")},
				}
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretKeyMissing,
		},
		{
			name: "a Secret whose password key is empty degrades the sink",
			secret: func(namespace, name string) *corev1.Secret {
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Data:       map[string][]byte{DefaultCredentialsSecretKey: {}},
				}
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretKeyMissing,
		},
		{
			name:       "a Secret named in another namespace is not read",
			refName:    "elsewhere",
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{allowAll: true})
			ctx := context.Background()
			name := uniqueName("sink")

			refName := tc.refName
			if refName == "" {
				refName = name
			}
			if tc.secret != nil {
				if err := h.Client.Create(ctx, tc.secret(h.OperatorNamespace, refName)); err != nil {
					t.Fatalf("create the credentials Secret: %v", err)
				}
			}

			chSink := &v1alpha1.ClickHouseSink{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1alpha1.ClickHouseSinkSpec{
					Connection: v1alpha1.ConnectionSpec{
						Addr:                 "clickhouse.invalid:9000",
						CredentialsSecretRef: v1alpha1.SecretReference{Name: refName},
					},
				},
			}
			if err := h.Client.Create(ctx, chSink); err != nil {
				t.Fatalf("create the sink: %v", err)
			}
			t.Cleanup(func() { h.deleteIfExists(chSink) })

			h.waitForSinkCondition(name, v1alpha1.ConditionCredentialsResolved, tc.wantStatus, tc.wantReason)

			// A sink that cannot authenticate must not be probed, so its schema
			// verdict is Unknown rather than a False nobody could have reached.
			if tc.wantStatus != metav1.ConditionTrue {
				h.waitForSinkCondition(name, v1alpha1.ConditionSchemaValid,
					metav1.ConditionUnknown, ReasonCredentialsUnavailable)
				h.waitForSinkCondition(name, v1alpha1.ConditionReady,
					metav1.ConditionFalse, tc.wantReason)
			}

			declared := len(h.Runtime.fingerprints(name)) > 0
			if declared != tc.wantDeclared {
				t.Errorf("declared to the sink runtime = %t, want %t", declared, tc.wantDeclared)
			}
		})
	}
}

// TestSinkObservedGeneration checks that a spec edit is reflected in
// status.observedGeneration, without which a client cannot tell "Ready, and up to
// date" from "Ready, but that verdict predates your edit".
func TestSinkObservedGeneration(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	name := uniqueName("gen")
	h.createSecret(name, "p1")

	chSink := &v1alpha1.ClickHouseSink{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ClickHouseSinkSpec{
			Connection: v1alpha1.ConnectionSpec{
				Addr:                 "clickhouse.invalid:9000",
				CredentialsSecretRef: v1alpha1.SecretReference{Name: name},
			},
		},
	}
	if err := h.Client.Create(ctx, chSink); err != nil {
		t.Fatalf("create the sink: %v", err)
	}
	t.Cleanup(func() { h.deleteIfExists(chSink) })
	h.waitForSinkCondition(name, v1alpha1.ConditionCredentialsResolved, metav1.ConditionTrue, ReasonSecretResolved)

	var live v1alpha1.ClickHouseSink
	if err := h.Client.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	live.Spec.Connection.Addr = "clickhouse-2.invalid:9000"
	if err := h.Client.Update(ctx, &live); err != nil {
		t.Fatalf("edit the sink: %v", err)
	}
	wantGeneration := live.Generation

	waitFor(t, "observedGeneration to catch up with the edit", func() (bool, string) {
		var fresh v1alpha1.ClickHouseSink
		if err := h.Client.Get(ctx, client.ObjectKey{Name: name}, &fresh); err != nil {
			return false, err.Error()
		}
		return fresh.Status.ObservedGeneration == wantGeneration,
			fmt.Sprintf("observedGeneration=%d want %d", fresh.Status.ObservedGeneration, wantGeneration)
	})
}

// TestSinkProbeFailureAndRecovery is acceptance criterion (f) for the sink half: an
// unreachable backend surfaced over the sink runtime's result channel must flip the
// sink to Ready=False, and a later success must flip it back — all without the
// reconciler ever dialling anything itself.
func TestSinkProbeFailureAndRecovery(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	name := uniqueName("probe")
	h.createSecret(name, "p1")

	chSink := &v1alpha1.ClickHouseSink{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ClickHouseSinkSpec{
			Connection: v1alpha1.ConnectionSpec{
				Addr:                 "clickhouse.invalid:9000",
				CredentialsSecretRef: v1alpha1.SecretReference{Name: name},
			},
		},
	}
	if err := h.Client.Create(ctx, chSink); err != nil {
		t.Fatalf("create the sink: %v", err)
	}
	t.Cleanup(func() { h.deleteIfExists(chSink) })

	// Before any probe settles: Unknown, not False. Nothing is known to be wrong.
	h.waitForSinkCondition(name, v1alpha1.ConditionSchemaValid, metav1.ConditionUnknown, ReasonProbePending)
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionUnknown, ReasonProbePending)

	// Unreachable: the schema is unknowable, but the sink is definitely not usable,
	// so the roll-up is False while the specific condition stays Unknown.
	h.pushProbe(sink.ProbeResult{
		Sink:   name,
		At:     time.Now().UTC(),
		Err:    errors.New("dial tcp: lookup clickhouse.invalid: no such host"),
		Reason: sink.ProbeReasonUnreachable,
	})
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionFalse, sink.ProbeReasonUnreachable)
	h.waitForSinkCondition(name, v1alpha1.ConditionSchemaValid, metav1.ConditionUnknown, sink.ProbeReasonUnreachable)

	// A backend that answered but disagrees about its columns is the one probe
	// failure that is a real verdict about the schema.
	h.pushProbe(sink.ProbeResult{
		Sink:   name,
		At:     time.Now().UTC(),
		Err:    fmt.Errorf("%w: resource_states is missing column sha256", sink.ErrSchemaInvalid),
		Reason: sink.ProbeReasonSchemaInvalid,
	})
	h.waitForSinkCondition(name, v1alpha1.ConditionSchemaValid, metav1.ConditionFalse, sink.ProbeReasonSchemaInvalid)
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionFalse, sink.ProbeReasonSchemaInvalid)

	// Recovery flips both back with no restart and no re-created CR.
	h.pushProbe(sink.ProbeResult{Sink: name, At: time.Now().UTC()})
	h.waitForSinkCondition(name, v1alpha1.ConditionSchemaValid, metav1.ConditionTrue, ReasonSchemaMatches)
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonConnected)
}

// TestSinkCredentialRotationRecycles proves the rotation loop the Secret field index
// exists for: updating the Secret re-reconciles the sink and produces a *different*
// configuration fingerprint, which is what makes the sink runtime recycle the
// instance rather than keep authenticating with the old password.
func TestSinkCredentialRotationRecycles(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	name := uniqueName("rotate")
	h.createSecret(name, "old-password")

	chSink := &v1alpha1.ClickHouseSink{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ClickHouseSinkSpec{
			Connection: v1alpha1.ConnectionSpec{
				Addr:                 "clickhouse.invalid:9000",
				CredentialsSecretRef: v1alpha1.SecretReference{Name: name},
			},
		},
	}
	if err := h.Client.Create(ctx, chSink); err != nil {
		t.Fatalf("create the sink: %v", err)
	}
	t.Cleanup(func() { h.deleteIfExists(chSink) })

	waitFor(t, "the sink to be declared to the runtime", func() (bool, string) {
		return len(h.Runtime.fingerprints(name)) == 1, fmt.Sprintf("%v", h.Runtime.fingerprints(name))
	})
	first := h.Runtime.fingerprints(name)[0]

	var secret corev1.Secret
	if err := h.Client.Get(ctx, client.ObjectKey{Namespace: h.OperatorNamespace, Name: name}, &secret); err != nil {
		t.Fatalf("get the credentials Secret: %v", err)
	}
	secret.Data[DefaultCredentialsSecretKey] = []byte("new-password")
	if err := h.Client.Update(ctx, &secret); err != nil {
		t.Fatalf("rotate the credentials Secret: %v", err)
	}

	waitFor(t, "the rotated credential to produce a new configuration", func() (bool, string) {
		got := h.Runtime.fingerprints(name)
		return len(got) == 2, fmt.Sprintf("%v", got)
	})
	if second := h.Runtime.fingerprints(name)[1]; second == first {
		t.Error("the rotated password produced the same fingerprint, so no recycle would happen")
	}
}

// TestSinkDeletionWithdrawsFromRuntime checks the deletion half of the sink
// lifecycle: no finalizer, and the runtime is told exactly once so it can drain the
// instance and evict the pipeline state on its own goroutines.
func TestSinkDeletionWithdrawsFromRuntime(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	name := uniqueName("delsink")
	h.createReadySink(name, v1alpha1.SinkPolicy{})

	var live v1alpha1.ClickHouseSink
	if err := h.Client.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if len(live.Finalizers) != 0 {
		t.Errorf("the sink carries finalizers %v; the design is deliberately finalizer-free", live.Finalizers)
	}

	if err := h.Client.Delete(ctx, &live); err != nil {
		t.Fatalf("delete the sink: %v", err)
	}
	waitFor(t, "the sink runtime to be told the sink is gone", func() (bool, string) {
		return slices.Contains(h.Runtime.deletions(), name), fmt.Sprintf("%v", h.Runtime.deletions())
	})

	// The probe verdict is forgotten with the sink, so a sink recreated under the
	// same name starts from ProbePending rather than inheriting its predecessor's
	// health.
	if _, ok := h.Probes.latest(name); ok {
		t.Error("the deleted sink's probe verdict is still stored")
	}
}

// TestProbeWatcherShutdown is the goleak check for the one long-lived goroutine this
// package owns. It also pins the two properties that make the watcher safe to put on
// the sink runtime's health path: a verdict is recorded before it is announced, and a
// blocked wake-up channel never outlives the context.
func TestProbeWatcherShutdown(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	t.Run("drains until cancelled", func(t *testing.T) {
		probes := newProbeStore()
		events := make(chan event.GenericEvent, 4)
		results := make(chan sink.ProbeResult, 4)
		watcher := &ProbeWatcher{Results: results, Probes: probes, Events: events}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- watcher.Start(ctx) }()

		results <- sink.ProbeResult{Sink: "one", At: time.Now().UTC()}
		select {
		case ev := <-events:
			if ev.Object.GetName() != "one" {
				t.Errorf("woke the reconciler for %q, want %q", ev.Object.GetName(), "one")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the watcher never announced a verdict")
		}
		// The store is written before the wake-up is sent, so a reconcile triggered
		// by that wake-up can never read a verdict older than the one it was woken
		// for.
		if _, recorded := probes.latest("one"); !recorded {
			t.Error("the verdict was announced before it was recorded")
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Start returned %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after its context was cancelled")
		}
	})

	t.Run("a full wake-up channel does not outlive the context", func(t *testing.T) {
		// Capacity zero with no reader: the send blocks, and only ctx.Done may
		// release it. A watcher that could not be cancelled here would hold the sink
		// runtime's probe loop open through shutdown.
		results := make(chan sink.ProbeResult, 1)
		watcher := &ProbeWatcher{Results: results, Probes: newProbeStore(), Events: make(chan event.GenericEvent)}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- watcher.Start(ctx) }()
		results <- sink.ProbeResult{Sink: "blocked", At: time.Now().UTC()}

		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Start stayed blocked on a full wake-up channel after cancellation")
		}
	})
}
