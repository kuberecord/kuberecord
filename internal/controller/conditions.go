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

// Package controller holds kubestream's control plane: the reconcilers that turn
// ClickHouseSink, StreamRule and ClusterStreamRule CRs into two things, and only
// two things — entries in the desired-state registry (internal/plan) and
// configurations in the sink runtime (internal/sink) — plus the status conditions
// and events that tell an operator why a rule is or is not streaming.
//
// The reconcilers are deliberately thin translators. Everything expensive lives
// behind an interface they call and do not wait on: watches are started by the
// WatchManager off the registry, and sinks are dialled and probed by the
// SinkManager on its own goroutines. No reconcile path in this package performs a
// sink round-trip (Invariant 1), which is why this package does not import
// internal/sink/clickhouse at all — the one place a backend configuration is
// built is a function injected at wiring time (SinkConfigBuilder).
//
// This file holds what both reconcilers share: the condition reasons, the rule
// key format that is the registry's and the sink runtime's common vocabulary, and
// the conflict-retrying status writer.
package controller

import (
	"context"
	"fmt"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Condition reasons for ClickHouseSink.
//
// Reasons are CamelCase per the metav1.Condition convention, and each one is a
// distinct *operator action*: an unreachable backend means "check the network or
// the database", a schema mismatch means "run the migration", a missing Secret
// means "create the Secret". Collapsing any two of them into one reason would
// leave a `kubectl describe` reader unable to tell which of those to do.
const (
	// ReasonSecretResolved marks a sink whose credentials Secret was found and
	// carried the expected key.
	ReasonSecretResolved = "SecretResolved"

	// ReasonSecretNotFound marks a sink whose credentialsSecretRef names a Secret
	// that does not exist (or that the operator may not read — its RBAC grants
	// Secret access only in its own namespace, see SecretReference.Namespace).
	ReasonSecretNotFound = "SecretNotFound"

	// ReasonSecretKeyMissing marks a sink whose Secret exists but carries no
	// password key. It is distinct from ReasonSecretNotFound because the fix is
	// different: the Secret is there, its content is wrong.
	ReasonSecretKeyMissing = "SecretKeyMissing"

	// ReasonSchemaMatches marks a sink whose live schema is the one this operator
	// build writes against, as reported by an asynchronous probe.
	ReasonSchemaMatches = "SchemaMatches"

	// ReasonProbePending marks a sink no probe has settled for yet — the instance
	// was only just handed to the sink runtime. It is an Unknown status, never a
	// False one: nothing is known to be wrong.
	ReasonProbePending = "ProbePending"

	// ReasonConnected marks a sink that is reachable, authenticated and
	// schema-valid: the roll-up Ready=True.
	ReasonConnected = "Connected"

	// ReasonCredentialsUnavailable marks Ready=False caused by the credential
	// rather than by the backend, so the roll-up condition points at the specific
	// condition an operator should read next.
	ReasonCredentialsUnavailable = "CredentialsUnavailable"
)

// Condition reasons for StreamRule and ClusterStreamRule.
const (
	// ReasonSecretsDenied marks a rule naming v1/Secret. The deny is hard-coded
	// (D8): no sink policy can re-enable it in v1alpha1.
	ReasonSecretsDenied = "SecretsDenied"

	// ReasonNotInAllowList marks a rule naming a GVK the target sink's
	// spec.policy.allowedGVKs does not admit.
	ReasonNotInAllowList = "NotInAllowList"

	// ReasonAllResourcesPermitted marks a rule every one of whose resources is
	// admitted by the sink's policy and is not on the hard deny-list.
	ReasonAllResourcesPermitted = "AllResourcesPermitted"

	// ReasonAllVerbsGranted marks a rule for which every expanded target passed
	// its SelfSubjectAccessReview for get, list and watch.
	ReasonAllVerbsGranted = "AllVerbsGranted"

	// ReasonMissingPermissions marks a rule at least one of whose targets the
	// operator's own ServiceAccount may not read. The condition message names the
	// resource and the verbs, because the operator can never self-escalate (D7):
	// the only fix is an administrator adding the grant, and they need to know
	// exactly which one.
	ReasonMissingPermissions = "MissingPermissions"

	// ReasonAccessReviewFailed marks a rule whose access review could not be
	// completed at all — the API server refused or failed the review. It is
	// distinct from ReasonMissingPermissions because it is not a verdict about the
	// rule: nothing about the rule is known to be wrong, so it retries.
	ReasonAccessReviewFailed = "AccessReviewFailed"

	// ReasonAllKindsResolved marks a rule every one of whose named kinds resolved
	// to a resource of a scope the rule is allowed to watch.
	ReasonAllKindsResolved = "AllKindsResolved"

	// ReasonInvalidSelector marks a rule carrying a label selector that cannot be
	// converted to a selector string. It sits on ResourceResolved because it is
	// the same class of problem — this resource entry cannot be turned into a
	// watch target — and because CRD validation cannot catch it (a
	// LabelSelector's operator/values combinations are only checkable in code).
	ReasonInvalidSelector = "InvalidSelector"

	// ReasonNamespaceSelectorInvalid marks a ClusterStreamRule whose
	// namespaceSelector cannot be converted to a selector at all. It sits on
	// ResourceResolved for the same reason as ReasonInvalidSelector: no target can
	// be derived from it.
	ReasonNamespaceSelectorInvalid = "NamespaceSelectorInvalid"

	// ReasonNamespacesUnavailable marks a ClusterStreamRule whose namespace listing
	// failed. It is Unknown rather than False, and is deliberately a different
	// reason from ReasonNamespaceSelectorInvalid: the selector is fine, the cluster
	// did not answer, so the rule's existing targets are kept and the pass retries.
	// Conflating the two would let one cache hiccup stop every watch a wide
	// ClusterStreamRule owns.
	ReasonNamespacesUnavailable = "NamespacesUnavailable"

	// ReasonSinkMissing marks a rule whose spec.sinkRef names a ClickHouseSink
	// that does not exist. Its targets are withdrawn: a target without a sink is
	// not a watch anybody could write the result of.
	ReasonSinkMissing = "SinkMissing"

	// ReasonSinkNotReady marks a rule whose sink exists but is not currently
	// Ready — unreachable, unauthenticated, or schema-invalid.
	//
	// The rule's targets deliberately stay installed (see RuleReconciler's
	// doc comment): an unreachable database is the failure the pipeline's
	// requeue path exists to absorb, and tearing every watch down over it would
	// cost a false scope epoch per scope plus a full re-emission on recovery.
	ReasonSinkNotReady = "SinkNotReady"

	// ReasonKindsUnresolved marks ResourceResolved=False. The per-kind verdicts —
	// a kind that is not installed yet, a cluster-scoped kind under a namespaced
	// rule — are in the message, quoting the resolver's own words (see
	// watch.ReasonKindNotFound and watch.ReasonClusterScopedKind, that package's
	// names for the same two cases).
	ReasonKindsUnresolved = "KindsUnresolved"

	// ReasonNotEvaluated marks a condition whose gate never ran because an earlier
	// gate already refused the rule. It is Unknown, never False: claiming a verdict
	// nobody reached would send an operator chasing a second problem that may not
	// exist.
	ReasonNotEvaluated = "NotEvaluated"

	// ReasonStreaming marks a fully realised rule: policy-admitted, RBAC-granted,
	// every kind resolved, and its targets installed in the desired-state
	// registry.
	ReasonStreaming = "Streaming"
)

// Event reasons. They are separate constants from the condition reasons above
// because an event is a different statement — "this just happened" rather than
// "this is the current state" — and because `kubectl get events` groups on them.
const (
	// EventReasonDegraded is emitted as a Warning whenever a CR's roll-up Ready
	// condition goes to False. Every degrade path funnels through degradedEvent,
	// so a new failure mode cannot be added without an event.
	EventReasonDegraded = "Degraded"

	// EventReasonReady is emitted as a Normal event when a CR reaches Ready=True.
	EventReasonReady = "Ready"
)

// Rule kind discriminators used in a registry rule key. They are lowercase
// singular resource-ish names rather than the Go type names so a key reads like
// the `kubectl` command an operator would type to find the rule it names.
const (
	kindStreamRule        = "streamrule"
	kindClusterStreamRule = "clusterstreamrule"
)

// RuleKey renders the desired-state registry's key for one rule.
//
// The registry treats rule keys as opaque strings, but two other components have
// to agree with this function exactly: the sink runtime reports dependent rules
// by these keys when a sink disappears (sink.Dependents), and the Parker below
// turns them back into reconcile requests. So the format is a *parseable*
// "<kind>/<namespace>/<name>" — cluster-scoped rules render an empty namespace
// segment — and this is the only function that builds one.
//
// The kind is part of the key because a StreamRule and a ClusterStreamRule may
// legitimately share a name; without it, one would silently overwrite the other's
// targets in the registry.
func RuleKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// parseRuleKey is RuleKey's inverse: it recovers the rule kind and object
// reference from a key, reporting false for anything it did not produce.
//
// It exists for the sink runtime's parking callback, which hands back the very
// keys the reconcilers wrote and needs them turned into reconcile requests. A key
// this function cannot parse is a wiring bug rather than a user error, so callers
// log it at Error rather than degrading anything (Invariant 4).
func parseRuleKey(key string) (kind string, ref types.NamespacedName, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return "", types.NamespacedName{}, false
	}
	switch parts[0] {
	case kindStreamRule, kindClusterStreamRule:
	default:
		return "", types.NamespacedName{}, false
	}
	return parts[0], types.NamespacedName{Namespace: parts[1], Name: parts[2]}, true
}

// condition builds a metav1.Condition carrying the generation it was decided
// against.
//
// observedGeneration on the condition (not only on status) is what lets a client
// distinguish "Ready, and up to date" from "Ready, but that verdict predates your
// last edit" per condition rather than per object — which matters here because a
// rule's conditions are decided from several inputs and a resync can refresh one
// of them without the others changing.
func condition(condType string, status metav1.ConditionStatus, reason, message string, generation int64) metav1.Condition {
	return metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}
}

// findCondition looks a condition up by type. It wraps apimeta's helper so the
// conditions a pass has accumulated but not yet written and the ones already
// persisted on an object are read through one call, which is what lets the
// roll-up be computed before the status write rather than after it.
func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	return apimeta.FindStatusCondition(conditions, condType)
}

// statusWriter accumulates the conditions one reconcile pass decided, so they are
// written to the API server exactly once — as a single status update — rather
// than one round-trip per condition.
//
// It also owns the Warning events: emitting them from here (rather than at each
// decision site) is what guarantees Invariant 5's "every degrade is visible"
// cannot be forgotten by a future failure mode, since a False roll-up condition
// and its event are set by the same call.
type statusWriter struct {
	conditions []metav1.Condition
	generation int64
}

// set records one condition's verdict for this pass.
func (w *statusWriter) set(condType string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&w.conditions, condition(condType, status, reason, message, w.generation))
}

// apply merges the accumulated conditions into an existing condition slice,
// preserving lastTransitionTime for conditions whose status did not change.
func (w *statusWriter) apply(existing *[]metav1.Condition) {
	for _, c := range w.conditions {
		apimeta.SetStatusCondition(existing, c)
	}
}

// updateStatus writes obj's status with optimistic-conflict retry.
//
// mutate is re-invoked on every attempt against a freshly read object, which is
// the only correct shape here: a conflict means somebody else wrote the object
// between the read and the update, so replaying the mutation against the stale
// copy would resurrect whatever they wrote. The read goes through the manager's
// client (and therefore its cache) on the first attempt; RetryOnConflict's later
// attempts re-read, and the cache is refreshed by the same watch event that
// caused the conflict.
//
// A conflict is expected traffic, not an anomaly: both rule reconcilers, the
// probe watcher and the periodic resync can all decide to write the same object's
// status within milliseconds of each other.
func updateStatus[T client.Object](ctx context.Context, c client.Client, obj T, mutate func(T)) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh, ok := obj.DeepCopyObject().(T)
		if !ok {
			// Unreachable: DeepCopyObject on a T always returns a T. Guarded
			// because a silent type failure here would look like a status that
			// simply never updates.
			return fmt.Errorf("deep copy of %T is not a %T", obj, obj)
		}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		return c.Status().Update(ctx, fresh)
	})
}

// emitReadyEvent records a *change* in the roll-up verdict as a Kubernetes event:
// a Warning for a degrade, a Normal for a recovery.
//
// Only the roll-up is evented, not every specific condition. An operator watching
// events wants "this rule stopped working, here is why", and one event per
// contributing condition would bury that in noise — the conditions themselves are
// where the detail lives.
//
// The comparison against the previous condition is what makes the event a record
// of an edge rather than of a level. Reconcile runs on a periodic resync (so RBAC
// grants applied later self-heal), and an unconditional event would turn every
// degraded rule into a permanent, repeating stream of identical Warnings that
// crowds out everything else in the namespace's event log.
func emitReadyEvent(recorder record.EventRecorder, obj client.Object, previous *metav1.Condition, ready metav1.Condition) {
	if recorder == nil {
		return
	}
	if previous != nil && previous.Status == ready.Status && previous.Reason == ready.Reason {
		return
	}
	switch ready.Status {
	case metav1.ConditionTrue:
		recorder.Event(obj, "Normal", EventReasonReady, ready.Message)
	case metav1.ConditionFalse:
		recorder.Eventf(obj, "Warning", EventReasonDegraded, "%s: %s", ready.Reason, ready.Message)
	case metav1.ConditionUnknown:
		// Not evented. Unknown is "nobody has decided yet" — the state every CR
		// passes through on its way to a verdict — and a Warning for it would tell
		// an operator something is wrong at the exact moment nothing is known to
		// be. The condition itself still reports it.
	}
}
