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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// TestS3SinkCredentialResolution is Task 6.4's first acceptance criterion, plus the
// state that makes this backend different from ClickHouse: a sink with *no*
// credentials at all is valid and must be declared to the runtime, because on a
// cloud provider that is the recommended shape (IRSA, workload identity, an
// instance role) and no Secret exists to read.
//
// The negative half matters as much: a sink whose Secret is missing, or which
// carries half a key, must never be handed to the runtime. An instance built with
// an empty access key would fail for a reason that has nothing to do with the
// configuration, and would be reported as an unreachable bucket rather than as the
// missing Secret it is.
func TestS3SinkCredentialResolution(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()

	t.Run("a complete Secret resolves and is declared", func(t *testing.T) {
		name := uniqueName("s3creds")
		h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
		h.createS3Sink(name, nil)

		resolved := h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
			metav1.ConditionTrue, ReasonSecretResolved)
		if !strings.Contains(resolved.Message, name) {
			t.Errorf("message %q does not name the Secret it read", resolved.Message)
		}
		waitFor(t, "the sink to be declared to the runtime", func() (bool, string) {
			return len(h.Runtime.s3Fingerprints(name)) == 1,
				fmt.Sprintf("%v", h.Runtime.s3Fingerprints(name))
		})
	})

	t.Run("a missing Secret reports CredentialsResolved=False", func(t *testing.T) {
		name := uniqueName("s3nosecret")
		h.createS3Sink(name, nil)

		h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
			metav1.ConditionFalse, ReasonSecretNotFound)
		// The bucket is not reported unreachable: nothing ever tried to reach it,
		// and an operator sent to check the network for a missing Secret is an
		// operator sent to the wrong place.
		h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
			metav1.ConditionUnknown, ReasonCredentialsUnavailable)
		h.waitForS3SinkCondition(name, v1alpha1.ConditionReady,
			metav1.ConditionFalse, ReasonSecretNotFound)

		if got := h.Runtime.s3Fingerprints(name); len(got) != 0 {
			t.Errorf("a sink with no credentials was declared to the runtime: %v", got)
		}
	})

	t.Run("half an access key is a content error, not a missing Secret", func(t *testing.T) {
		name := uniqueName("s3halfkey")
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.OperatorNamespace},
			Data:       map[string][]byte{DefaultAccessKeyIDSecretKey: []byte("AKIAEXAMPLE")},
		}
		if err := h.Client.Create(ctx, secret); err != nil {
			t.Fatalf("create the half-filled Secret: %v", err)
		}
		h.createS3Sink(name, nil)

		// SecretKeyMissing rather than SecretNotFound, because the fix is different:
		// the Secret is there, its content is wrong. And it must not fall back to the
		// ambient chain — that would silently authenticate as the pod's own identity
		// instead of the one the author wrote down.
		reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
			metav1.ConditionFalse, ReasonSecretKeyMissing)
		for _, want := range []string{DefaultAccessKeyIDSecretKey, DefaultSecretAccessKeySecretKey} {
			if !strings.Contains(reported.Message, want) {
				t.Errorf("message %q does not name the required key %q", reported.Message, want)
			}
		}
		if got := h.Runtime.s3Fingerprints(name); len(got) != 0 {
			t.Errorf("a sink with half a key was declared to the runtime: %v", got)
		}
	})

	t.Run("an omitted credentials block is the ambient chain", func(t *testing.T) {
		name := uniqueName("s3ambient")
		// No Secret is created at all, deliberately: the point of this shape is that
		// no long-lived key exists in the cluster.
		h.createS3Sink(name, func(s *v1alpha1.S3Sink) { s.Spec.Credentials = nil })

		reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
			metav1.ConditionTrue, ReasonAmbientCredentials)
		if !strings.Contains(reported.Message, "ambient") {
			t.Errorf("message %q does not say where the credential comes from", reported.Message)
		}
		waitFor(t, "the ambient sink to be declared to the runtime", func() (bool, string) {
			return len(h.Runtime.s3Fingerprints(name)) == 1,
				fmt.Sprintf("%v", h.Runtime.s3Fingerprints(name))
		})
	})
}

// TestS3SinkProbeFailureAndRecovery is the acceptance criterion in full: an
// unreachable bucket reports BucketReachable=False while Ready is False, the sink
// stays declared so the runtime keeps probing it, and recovery flips both back
// without anything else happening.
//
// "The manager keeps retrying" is asserted here as the property this tier is
// responsible for — the sink is never withdrawn from the runtime because its bucket
// is down, so the runtime's probe loop keeps running against it. The backoff
// schedule itself is the runtime's, asserted in internal/sink.
func TestS3SinkProbeFailureAndRecovery(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	name := uniqueName("s3probe")
	h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
	h.createS3Sink(name, nil)

	h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
		metav1.ConditionTrue, ReasonSecretResolved)
	// Before any probe settles, the bucket is Unknown rather than False: nothing is
	// known to be wrong yet, and a False here would degrade every freshly created
	// sink for as long as its first probe takes.
	h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
		metav1.ConditionUnknown, ReasonProbePending)

	h.pushProbe(sink.ProbeResult{
		Sink:   s3SinkID(name),
		At:     time.Now().UTC(),
		Err:    errors.New("dial tcp 10.0.0.1:9000: connect: connection refused"),
		Reason: sink.ProbeReasonUnreachable,
	})
	unreachable := h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
		metav1.ConditionFalse, ReasonBucketUnreachable)
	if !strings.Contains(unreachable.Message, "connection refused") {
		t.Errorf("message %q does not carry the backend's own words", unreachable.Message)
	}
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonBucketUnreachable)

	// Still declared, still exactly once: an unreachable bucket must not be a reason
	// to tear a sink down or to rebuild it, or the queued writes it holds would be
	// abandoned every probe interval.
	if got := h.Runtime.s3Fingerprints(name); len(got) != 1 {
		t.Errorf("configurations declared = %v, want exactly one across the outage", got)
	}
	if got := h.Runtime.deletions(); slices.Contains(got, s3SinkID(name)) {
		t.Errorf("an unreachable bucket withdrew the sink from the runtime: %v", got)
	}

	h.pushProbe(sink.ProbeResult{Sink: s3SinkID(name), At: time.Now().UTC()})
	h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
		metav1.ConditionTrue, ReasonBucketWritable)
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonArchiving)

	var s3Sink v1alpha1.S3Sink
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &s3Sink); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if s3Sink.Status.ObservedGeneration != s3Sink.Generation {
		t.Errorf("observedGeneration = %d, want %d", s3Sink.Status.ObservedGeneration, s3Sink.Generation)
	}
	// This sink's instance was never reported as running by the fake runtime, so its
	// capabilities have not been detected. Unknown is the only honest answer: on an
	// inverted condition, False is the *reassuring* value, so guessing it would
	// claim this archive records deletions when nothing has looked.
	history := h.waitForS3SinkCondition(name, v1alpha1.ConditionHistoryUnavailable,
		metav1.ConditionUnknown, ReasonCapabilitiesUnknown)
	if !strings.Contains(history.Message, "no running instance") {
		t.Errorf("message %q does not say why nothing is known", history.Message)
	}
	// And it did not drag the roll-up: Ready is decided by BucketReachable and
	// CredentialsResolved alone, which is what makes a capability limit reportable
	// without making it look like a fault.
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonArchiving)
}

// TestS3SinkBucketIncompatibleIsPermanent covers the one probe outcome that will
// never clear on its own: a bucket with no Object Lock configuration cannot accept
// the retained objects this sink is configured to write, and on S3 that cannot be
// enabled after the bucket exists.
//
// It gets its own reason precisely so `kubectl describe` distinguishes "wait" from
// "a human must change something" — the same distinction the ClickHouse backend
// draws with SchemaValid.
func TestS3SinkBucketIncompatibleIsPermanent(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	name := uniqueName("s3lock")
	h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
	h.createS3Sink(name, func(s *v1alpha1.S3Sink) {
		s.Spec.ObjectLock = &v1alpha1.S3ObjectLockSpec{
			Mode:       v1alpha1.ObjectLockModeCompliance,
			RetainDays: 30,
		}
	})

	h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
		metav1.ConditionTrue, ReasonSecretResolved)
	h.pushProbe(sink.ProbeResult{
		Sink:   s3SinkID(name),
		At:     time.Now().UTC(),
		Err:    errors.New("bucket kuberecord-audit is missing Object Lock Configuration"),
		Reason: sink.ProbeReasonSchemaInvalid,
	})

	reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
		metav1.ConditionFalse, ReasonBucketIncompatible)
	if !strings.Contains(reported.Message, "Object Lock") {
		t.Errorf("message %q does not say what the bucket refused", reported.Message)
	}
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonBucketIncompatible)
}

// TestS3SinkAmbientCredentialFailureLandsOnCredentials is why the sink contract grew
// sink.ProbeReasonCredentialsInvalid.
//
// A sink using the ambient chain reports CredentialsResolved=True on the strength of
// its configuration being complete — there is nothing in the cluster to read. If the
// chain then turns out to produce nothing, the only place that can be discovered is a
// request, and the verdict belongs on the *credential* condition rather than on the
// bucket's: an operator whose IRSA annotation is missing must not be sent to read
// firewall rules.
func TestS3SinkAmbientCredentialFailureLandsOnCredentials(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	name := uniqueName("s3chain")
	h.createS3Sink(name, func(s *v1alpha1.S3Sink) { s.Spec.Credentials = nil })

	h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
		metav1.ConditionTrue, ReasonAmbientCredentials)

	h.pushProbe(sink.ProbeResult{
		Sink:   s3SinkID(name),
		At:     time.Now().UTC(),
		Err:    errors.New("get identity: get credentials: no EC2 IMDS role found"),
		Reason: sink.ProbeReasonCredentialsInvalid,
	})

	reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
		metav1.ConditionFalse, ReasonCredentialsUnavailable)
	if !strings.Contains(reported.Message, "IMDS") {
		t.Errorf("message %q does not carry the chain's own words", reported.Message)
	}
	h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
		metav1.ConditionUnknown, ReasonCredentialsUnavailable)
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady,
		metav1.ConditionFalse, ReasonCredentialsUnavailable)
}

// TestS3SinkCredentialRotationRecycles is the control-plane half of the zero-loss
// rotation criterion: an updated Secret must reach this reconciler through the field
// index (not a resync), and must produce a *different* configuration — because a
// fingerprint that did not change is a recycle that never happens.
//
// The other half — that the swap itself loses no job — is asserted against the real
// writer and the real sink runtime in internal/sink/s3.
func TestS3SinkCredentialRotationRecycles(t *testing.T) {
	// A long resync, so what is observed can only be the Secret watch firing.
	h := newHarness(t, harnessOptions{allowAll: true, resyncPeriod: time.Hour})
	ctx := context.Background()
	name := uniqueName("s3rotate")
	h.createS3Secret(name, "AKIAOLD", "old-secret")
	h.createS3Sink(name, nil)

	waitFor(t, "the sink to be declared to the runtime", func() (bool, string) {
		return len(h.Runtime.s3Fingerprints(name)) == 1,
			fmt.Sprintf("%v", h.Runtime.s3Fingerprints(name))
	})
	first := h.Runtime.s3Fingerprints(name)[0]

	var secret corev1.Secret
	if err := h.Client.Get(ctx, client.ObjectKey{Namespace: h.OperatorNamespace, Name: name}, &secret); err != nil {
		t.Fatalf("get the credentials Secret: %v", err)
	}
	secret.Data[DefaultSecretAccessKeySecretKey] = []byte("new-secret")
	if err := h.Client.Update(ctx, &secret); err != nil {
		t.Fatalf("rotate the credentials Secret: %v", err)
	}

	waitFor(t, "the rotated credential to produce a new configuration", func() (bool, string) {
		got := h.Runtime.s3Fingerprints(name)
		return len(got) == 2, fmt.Sprintf("%v", got)
	})
	if second := h.Runtime.s3Fingerprints(name)[1]; second == first {
		t.Error("the rotated access key produced the same fingerprint, so no recycle would happen")
	}
}

// TestS3SinkDeletionWithdrawsFromRuntime checks the deletion half of the lifecycle:
// no finalizer, and the runtime is told — under the typed identity — so it can drain
// the instance, evict the pipeline state and park the dependent rules on its own
// goroutines.
//
// A finalizer would be actively wrong here rather than merely unnecessary: there is
// nothing to clean up outside the process, and the objects already in the bucket
// *are* the audit trail. Deleting a CR must never delete an archive.
func TestS3SinkDeletionWithdrawsFromRuntime(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	name := uniqueName("s3delete")
	h.createReadyS3Sink(name, v1alpha1.SinkPolicy{})

	var live v1alpha1.S3Sink
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
		return slices.Contains(h.Runtime.deletions(), s3SinkID(name)),
			fmt.Sprintf("%v", h.Runtime.deletions())
	})
	// Withdrawn under the S3Sink kind and no other: a Delete keyed on the bare name
	// would drain whichever backend happened to answer to it.
	if slices.Contains(h.Runtime.deletions(), clickHouseSinkID(name)) {
		t.Error("deleting an S3Sink withdrew a ClickHouseSink of the same name")
	}
	// The probe verdict is forgotten with the sink, so a sink recreated under the
	// same name starts from ProbePending rather than inheriting its predecessor's
	// health.
	if _, ok := h.Probes.latest(s3SinkID(name)); ok {
		t.Error("the deleted sink's probe verdict is still stored")
	}
}

// TestS3SinkDeletionParksDependentRules is the last clause of the acceptance
// criterion, and the one that only means something now that a rule can bind to an
// S3Sink at all: the rules that streamed to a deleted archive must be parked with
// Ready=False/SinkMissing and must stop watching.
//
// The park arrives through the sink runtime's callback rather than through the rule's
// own sink watch, which is the path that has to work when the sink was deleted while
// the operator was elsewhere. Both paths reach the same verdict; this asserts the
// callback carries a typed identity the registry recognises.
func TestS3SinkDeletionParksDependentRules(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	ctx := context.Background()
	sinkName := uniqueName("s3parksink")
	h.createReadyS3Sink(sinkName, v1alpha1.SinkPolicy{})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)
	rule := h.newStreamRuleWithSink(namespace, "archived",
		v1alpha1.SinkReference{Kind: s3SinkKind, Name: sinkName}, resourceEntry("", "ConfigMap"))
	ruleKey := RuleKey(kindStreamRule, namespace, "archived")

	// A rule bound to an archive sink streams like any other: that is the whole
	// point of the sink contract, and it is what makes the park below non-vacuous.
	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonStreaming)
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})
	if got := h.Registry.RulesForSink(s3SinkID(sinkName)); !slicesEqual(got, []string{ruleKey}) {
		t.Fatalf("RulesForSink(%s) = %v, want [%s] — the runtime could not name this rule to park it",
			s3SinkID(sinkName), got, ruleKey)
	}

	var live v1alpha1.S3Sink
	if err := h.Client.Get(ctx, client.ObjectKey{Name: sinkName}, &live); err != nil {
		t.Fatalf("get the sink: %v", err)
	}
	if err := h.Client.Delete(ctx, &live); err != nil {
		t.Fatalf("delete the sink: %v", err)
	}
	h.Parker.SinkGone(s3SinkID(sinkName), []string{ruleKey})

	h.waitForRuleCondition(rule, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonSinkMissing)
	h.waitForTargets(ruleKey, nil)
}

// TestS3SinkReportsWriterOnlyDegradation is Task 6.5's first and last acceptance
// criteria together: a sink whose running instance implements no StateReader reports
// HistoryUnavailable=True, one whose instance does is unaffected, and neither
// answer touches Ready.
//
// The two halves belong in one test because the property is the *discrimination*,
// not either verdict. A reconciler that hard-coded "an S3Sink cannot read history"
// would pass the first subtest and be wrong in the way that matters: the condition
// would keep asserting a limit the running backend no longer had, which is the one
// thing a status condition must never do.
func TestS3SinkReportsWriterOnlyDegradation(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})

	t.Run("a writer with no StateReader is reported, and stays Ready", func(t *testing.T) {
		name := uniqueName("s3writeronly")
		h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
		// Declared before the CR exists, so the very first reconcile already has an
		// instance to ask about — the production ordering, where Ensure builds the
		// instance synchronously and CapabilitiesFor is answered from the routing
		// table it just swapped in.
		h.Runtime.setCapabilities(s3SinkID(name), sink.Capabilities{})
		h.createS3Sink(name, nil)

		reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionHistoryUnavailable,
			metav1.ConditionTrue, ReasonWriterOnlySink)
		// All three disabled behaviours, by the names the operator will search for.
		for _, want := range []string{
			"cache warm-up", "zombie garbage collection", "boot reconciliation of scope epochs",
		} {
			if !strings.Contains(reported.Message, want) {
				t.Errorf("message does not name the disabled behaviour %q:\n%s", want, reported.Message)
			}
		}
		// And both consequences. The second is the one that cannot be recovered
		// from the archive itself, which is why it has to be said here.
		for _, want := range []string{"permanent Snapshot", "while the operator is down are never recorded"} {
			if !strings.Contains(reported.Message, want) {
				t.Errorf("message does not state the consequence %q:\n%s", want, reported.Message)
			}
		}

		// The whole point: a declared capability limit is not a fault. The sink
		// authenticated and its bucket answered, so it is Ready — and an operator
		// must be able to tell this archive from a broken one.
		h.pushProbe(sink.ProbeResult{Sink: s3SinkID(name), At: time.Now().UTC()})
		h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonArchiving)

		// Still True after the health verdict landed: the two are independent axes,
		// and a resync must not talk itself out of the limit.
		var s3Sink v1alpha1.S3Sink
		if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &s3Sink); err != nil {
			t.Fatalf("get the sink: %v", err)
		}
		if c := findCondition(s3Sink.Status.Conditions, v1alpha1.ConditionHistoryUnavailable); c == nil ||
			c.Status != metav1.ConditionTrue {
			t.Errorf("HistoryUnavailable = %v after the sink became Ready, want True", c)
		}
	})

	t.Run("an unreachable bucket does not change the capability verdict", func(t *testing.T) {
		// Health and capability are orthogonal, and this is the pairing that proves
		// it: the sink is degraded *and* Writer-only, and each condition reports its
		// own thing. A reconciler that derived one from the other would collapse
		// here — the bucket outage would either hide the limit or masquerade as it.
		name := uniqueName("s3writeronlydown")
		h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
		h.Runtime.setCapabilities(s3SinkID(name), sink.Capabilities{})
		h.createS3Sink(name, nil)
		// Waited on before the probe is pushed, and not for tidiness: a probe
		// wake-up that arrives before the CR is in the reconciler's cache reconciles
		// a NotFound, which withdraws the sink and drops the verdict with it.
		h.waitForS3SinkCondition(name, v1alpha1.ConditionBucketReachable,
			metav1.ConditionUnknown, ReasonProbePending)

		h.pushProbe(sink.ProbeResult{
			Sink:   s3SinkID(name),
			At:     time.Now().UTC(),
			Err:    errors.New("dial tcp 10.0.0.1:9000: connect: connection refused"),
			Reason: sink.ProbeReasonUnreachable,
		})
		h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionFalse, ReasonBucketUnreachable)
		h.waitForS3SinkCondition(name, v1alpha1.ConditionHistoryUnavailable,
			metav1.ConditionTrue, ReasonWriterOnlySink)
	})

	t.Run("a writer that can read its history is unaffected", func(t *testing.T) {
		name := uniqueName("s3readable")
		h.createS3Secret(name, "AKIAEXAMPLE", "secret-access-key")
		h.Runtime.setCapabilities(s3SinkID(name), sink.Capabilities{HistoryReadable: true})
		h.createS3Sink(name, nil)

		reported := h.waitForS3SinkCondition(name, v1alpha1.ConditionHistoryUnavailable,
			metav1.ConditionFalse, ReasonHistoryReadable)
		if !strings.Contains(reported.Message, "can read its own history back") {
			t.Errorf("message %q does not say the sink is unaffected", reported.Message)
		}
		h.pushProbe(sink.ProbeResult{Sink: s3SinkID(name), At: time.Now().UTC()})
		h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonArchiving)
	})
}
