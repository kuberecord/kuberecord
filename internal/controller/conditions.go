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

// Package controller holds kuberecord's control plane: the reconcilers that turn
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

// Condition reasons specific to S3Sink.
//
// They exist as their own set rather than reusing the ClickHouse ones because the
// operator action each names is genuinely different: a bucket that will not accept
// a retained object is not a schema to migrate, and a credential resolved from the
// ambient chain is not a Secret to create.
const (
	// ReasonAmbientCredentials marks a sink that names no Secret and authenticates
	// from the ambient chain — IRSA, workload identity, or an instance role.
	//
	// It reports CredentialsResolved=True on the strength of the *configuration*
	// being complete, not of a credential having been produced: the chain is lazy,
	// and only a request can exercise it. A chain that turns out to produce nothing
	// is reported by the health probe and lands on this same condition as
	// ReasonCredentialsUnavailable, which is why omitting spec.credentials is a
	// supported state rather than an unverifiable one.
	ReasonAmbientCredentials = "AmbientCredentials"

	// ReasonBucketWritable marks a sink whose bucket answered a write. It is a
	// write and not a HEAD deliberately: a read-only credential passes a HEAD and
	// then fails every PUT (see v1alpha1.ConditionBucketReachable).
	ReasonBucketWritable = "BucketWritable"

	// ReasonBucketIncompatible marks a bucket that answered and refused the *shape*
	// of object this sink is configured to write — today, a spec.objectLock against
	// a bucket with no Object Lock configuration, which only a human on the account
	// can give it (docs/RETENTION.md).
	//
	// It is distinct from ReasonBucketUnreachable because it will never clear on
	// its own: every write this sink attempts will fail identically until a human
	// changes the bucket or the spec.
	ReasonBucketIncompatible = "BucketIncompatible"

	// ReasonBucketUnreachable marks a bucket that did not answer at all — DNS, a
	// refused connection, a 5xx, a rejected credential. Unlike
	// ReasonBucketIncompatible it usually clears with time, so the sink runtime
	// keeps probing and the condition flips back on its own.
	ReasonBucketUnreachable = "BucketUnreachable"

	// ReasonArchiving marks an S3Sink that is fully healthy: its credential
	// resolved and its bucket accepted a write. It is the S3 counterpart of
	// ReasonConnected, named for what the sink is *doing* rather than for a
	// connection it does not hold — an object store is reached per request, so
	// there is no connection to be "connected" over.
	ReasonArchiving = "Archiving"
)

// Condition reasons for HistoryUnavailable, the one condition whose True is the
// abnormal-sounding value on a perfectly healthy sink (see
// v1alpha1.ConditionHistoryUnavailable).
//
// They are named for the *capability*, not for a failure, because that is what
// they report: a backend either can read its own history back or it cannot, and
// the one that cannot is doing exactly what it was designed to do (D12). None of
// them ever reaches the Ready roll-up — HistoryUnavailable is deliberately absent
// from every readyOrder list, so a declared limit can never be mistaken for a
// fault.
const (
	// ReasonWriterOnlySink marks a sink whose running instance implements the
	// write half of the sink contract and not the read half. It is the reason
	// HistoryUnavailable is True, and the reason a rule bound to such a sink says
	// so too.
	ReasonWriterOnlySink = "WriterOnlySink"

	// ReasonHistoryReadable marks a sink that *can* read its own history back, and
	// therefore runs with warm-up, zombie GC and boot reconciliation all enabled.
	//
	// It is reported rather than left absent on purpose. An absent condition is
	// indistinguishable from an operator build that never decided one, so a reader
	// could not tell "this archive records deletions" from "nobody checked" —
	// which is the precise ambiguity this whole condition exists to remove.
	ReasonHistoryReadable = "HistoryReadable"

	// ReasonCapabilitiesUnknown marks a sink with no running instance yet, so what
	// it can do has not been detected. It is Unknown, never False: claiming a sink
	// can reconstruct history because nothing has looked would be the most
	// misleading of the three answers.
	//
	// It is transient by construction — the instance is built synchronously by the
	// declaration this reconciler just made, or on the sink runtime's own start —
	// and the first health probe wakes this reconciler again within a second or so.
	// A sink whose credentials never resolve stays here, correctly: it was never
	// handed to the runtime at all.
	ReasonCapabilitiesUnknown = "CapabilitiesUnknown"
)

// writerOnlySinkMessage is what a Writer-only sink's HistoryUnavailable condition
// says on the sink itself.
//
// It is one shared string rather than a sentence written at each site because the
// sink's condition and the rules' mirrored conditions must not drift: an operator
// comparing a rule against its sink has to see the same claim twice, not two
// paraphrases they then have to reconcile. It names all three disabled behaviours
// and both consequences, because the consequences are the part that is invisible
// in the archive itself — an object store with no Deleted records in it looks
// exactly like an archive of a cluster where nothing was deleted.
const writerOnlySinkMessage = "This sink cannot read its own history back, so three behaviours are disabled " +
	"for it: dedup cache warm-up, zombie garbage collection, and boot reconciliation of scope epochs. " +
	"An object first seen by this operator process is therefore always a permanent Snapshot and never an " +
	"Added, every object is re-snapshotted in full after each operator restart, and deletions that occur " +
	"while the operator is down are never recorded. " +
	"kuberecord_safe_mode stays at 1 for every scope on this sink, which is where the same fact is " +
	"observable in metrics. This is a declared capability limit of this backend (D12) and not a fault, so " +
	"Ready stays True; pair it with a ClickHouseSink over the same resources for a queryable timeline."

// historyReadableMessage is the complement, for a sink that can read its history.
const historyReadableMessage = "This sink can read its own history back, so dedup cache warm-up, zombie " +
	"garbage collection and boot reconciliation of scope epochs all run for it."

// writerOnlySinkRuleMessage is the same statement as writerOnlySinkMessage, said
// to the author of a *rule* bound to such a sink.
//
// The rule needs its own wording for one reason: the author of a StreamRule may
// well not own the sink it names, and may never look at it. A rule that reported
// only Ready=True would tell them their rule is streaming — which is true — while
// leaving them to discover from row counts, months later, that the stream has no
// deletions in it. So the rule names the sink, states the limit, and points at
// the sink's own condition for the detail.
func writerOnlySinkRuleMessage(sinkID string) string {
	return fmt.Sprintf("Sink %s cannot reconstruct history: an object this rule reports for the first time "+
		"is always a permanent Snapshot and never an Added, every object is re-snapshotted in full after "+
		"each operator restart, and a deletion that happens while the operator is down is never recorded. "+
		"This is a declared limit of that sink's backend (D12), not a fault of this rule — see the sink's "+
		"own HistoryUnavailable condition.", sinkID)
}

// historyReadableRuleMessage is the complement for a rule whose sink is fine.
func historyReadableRuleMessage(sinkID string) string {
	return fmt.Sprintf("Sink %s can reconstruct history, so this rule's records carry accurate event types "+
		"and a deletion that happens while the operator is down is recovered on the next boot.", sinkID)
}

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

	// ReasonSinkMissing marks a rule whose spec.sink names a sink that does not
	// exist. Its targets are withdrawn: a target without a sink is not a watch
	// anybody could write the result of.
	//
	// The verdict is reached by comparing the whole typed identity, so it also
	// covers a rule naming a *kind* this build does not serve — an S3Sink while
	// only a ClickHouseSink of that name exists. That is the same answer for the
	// same reason: the sink the rule asked for is not here. Resolving it to the
	// same-named sink of another kind would be far worse than parking, since the
	// rule would stream to a backend carrying another sink's dedup baseline.
	ReasonSinkMissing = "SinkMissing"

	// ReasonSinkNotReady marks a rule whose sink exists but is not currently
	// Ready — unreachable, unauthenticated, or schema-invalid.
	//
	// The rule's targets deliberately stay installed (see RuleReconciler's
	// doc comment): an unreachable database is the failure the pipeline's
	// requeue path exists to absorb, and tearing every watch down over it would
	// cost a false scope epoch per scope plus a full re-emission on recovery.
	ReasonSinkNotReady = "SinkNotReady"

	// ReasonLegacySinkRef marks a rule that names no sink at all, because it was
	// authored against v0.1.0 where the sink was a bare string field
	// (spec.sinkRef). That field was renamed rather than retyped (D10), so the old
	// spelling is pruned as an unknown field and the new one decodes as the zero
	// value — which is the only trace such a rule leaves, and therefore the only
	// thing a reconciler can detect.
	//
	// It is a reconciler verdict rather than a CRD validation rule because it has
	// to be: validation runs on write, and these objects are already stored, so
	// the API server will never re-examine them. Admission does reject a *new*
	// rule with no sink (Task 4.3) — this is the other half, for the ones an
	// upgrade inherited.
	//
	// It never self-heals and the rule contributes nothing meanwhile. spec.sink is
	// immutable, so there is no edit that repairs such a rule; the message names
	// the rename and says to delete and recreate it, which is the whole fix.
	ReasonLegacySinkRef = "LegacySinkRef"

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

	// EventReasonHistoryUnavailable is emitted as a Warning the first time a sink
	// is found to be Writer-only.
	//
	// It is a Warning even though the sink is healthy, and it is *not*
	// EventReasonDegraded. A Warning because an operator who did not read the
	// backend's documentation must be told once, unprompted, that this archive
	// will contain no deletions; a separate reason because `kubectl get events`
	// groups on it, and filing a declared capability limit under "Degraded" would
	// have somebody looking for the outage that never happened.
	EventReasonHistoryUnavailable = "HistoryUnavailable"
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
// probe hub and the periodic resync can all decide to write the same object's
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

// emitCapabilityLimitEvent records a sink's declared capability limit as a
// Warning, once, at the moment it is first established.
//
// It is deliberately not folded into emitReadyEvent. That function reports a
// *change in health*, and this is the opposite: a sink whose Ready is True and
// stays True, which nonetheless has something an operator must be told once. The
// two also compare against different conditions, so one function taking both
// would need to be told which it was reporting anyway.
//
// Only the edge is evented, and only in the True direction. A level-triggered
// version would re-emit on every resync — a permanent limit producing a permanent
// stream of identical Warnings, which is how an event log stops being read at all
// — and the False direction is not news: a sink that gained a read half is a sink
// that stopped having a limitation worth warning about.
//
// One consequence worth naming: because the comparison is against the *persisted*
// condition, an operator restart does not re-emit this. The event is a nudge with
// an expiry; the condition is the durable statement, and the condition is where a
// reader is meant to end up.
func emitCapabilityLimitEvent(recorder record.EventRecorder, obj client.Object,
	previous *metav1.Condition, current metav1.Condition) {
	if recorder == nil || current.Status != metav1.ConditionTrue {
		return
	}
	if previous != nil && previous.Status == current.Status && previous.Reason == current.Reason {
		return
	}
	recorder.Eventf(obj, "Warning", EventReasonHistoryUnavailable, "%s: %s", current.Reason, current.Message)
}
