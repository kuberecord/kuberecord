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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// The keys an S3Sink's credentials Secret must carry.
//
// They are fixed rather than configurable per sink, for the reason
// DefaultCredentialsSecretKey states: the Secret exists for exactly one consumer,
// and `key` fields on the CR would only add a way to spell the same thing twice
// (and a second failure mode when the two disagree). They are spelled in the
// camelCase the rest of the API uses rather than as AWS_* environment variables,
// so a Secret written for kuberecord reads like the rest of kuberecord.
const (
	// DefaultAccessKeyIDSecretKey holds the access key ID.
	DefaultAccessKeyIDSecretKey = "accessKeyId"

	// DefaultSecretAccessKeySecretKey holds the secret access key.
	DefaultSecretAccessKeySecretKey = "secretAccessKey"

	// DefaultSessionTokenSecretKey holds the session token, and is *optional*: it
	// is present only for temporary credentials (an assumed role's key placed in a
	// Secret). Its absence is the ordinary long-lived-key case, so a missing value
	// here is never an error — unlike the two above.
	DefaultSessionTokenSecretKey = "sessionToken"
)

// s3SecretRefIndexKey is the field-index name under which every S3Sink is indexed
// by the "namespace/name" of the Secret it reads its access key from. The index is
// what makes credential rotation a watch rather than a poll: a Secret update maps
// straight back to the sinks that read it.
//
// It is a *separate* index from the ClickHouse one rather than a shared
// "sinks by secret" index, because a field index is registered per object type:
// the two indexes name different fields on different kinds and can only be built
// by different extractors. A sink using ambient credentials contributes no entry
// at all, which is correct — no Secret event concerns it.
const s3SecretRefIndexKey = ".spec.credentials.secretRef"

// s3SinkKind is the kind an S3Sink CR is reconciled under, spelled as the API
// server spells it.
//
// Like clickHouseSinkKind it is named separately from anything it happens to equal:
// this reconciler reconciles exactly one kind and therefore *knows* its own, which
// is the only condition under which naming a kind outright is legitimate (see
// sink.DefaultSinkKind, which this deliberately is not).
const s3SinkKind = "S3Sink"

// s3SinkID is the runtime identity of one S3Sink CR.
//
// Keeping the construction in one function is what makes "which kind does the
// S3Sink reconciler register under?" a single, greppable answer rather than a
// literal repeated at every call site — and it is what the deletion path, the
// probe lookup and the Ensure call are all guaranteed to agree on.
func s3SinkID(name string) sink.ID {
	return sink.ID{Kind: s3SinkKind, Name: name}
}

// S3Credentials is one S3Sink's resolved static access key, on its way from the
// Secret to the client constructor.
//
// Its zero value means "authenticate from the ambient credential chain" — IRSA,
// workload identity, or an instance role — which is the supported and, on a cloud
// provider, preferred shape (see v1alpha1.S3CredentialsSpec). That is why this is a
// struct with a meaningful zero value rather than a pointer: the absence of a key
// is a state the configuration carries, not a nil to be guarded at every use.
//
// It is declared here, in the control plane, rather than reusing the S3 backend's
// own credential type, so that internal/controller depends on no backend package
// and therefore cannot reach an SDK even by accident (Invariant 1). The wiring
// translates it, exactly as it translates the rest of the spec.
type S3Credentials struct {
	// AccessKeyID and SecretAccessKey are the two halves of a static key. Both
	// empty means the ambient chain.
	AccessKeyID     string
	SecretAccessKey string

	// SessionToken is set only for temporary credentials.
	SessionToken string
}

// IsAmbient reports whether these credentials say "resolve me from the
// environment".
func (c S3Credentials) IsAmbient() bool {
	return c.AccessKeyID == "" && c.SecretAccessKey == "" && c.SessionToken == ""
}

// S3SinkConfigBuilder turns a resolved S3Sink plus its credentials into the backend
// configuration the sink runtime builds an instance from.
//
// It is injected for the same reason SinkConfigBuilder is: a backend configuration
// is backend knowledge, and keeping the mapping in the wiring means this package
// imports no object-store client and the reconciler's inability to reach S3 is a
// property of its imports rather than of its control flow (Invariant 1).
//
// Implementations must not perform I/O. They translate a struct; the client is
// built later, by the sink runtime, from that struct.
type S3SinkConfigBuilder func(name string, spec v1alpha1.S3SinkSpec, creds S3Credentials) (sink.InstanceConfig, error)

// S3SinkReconciler reconciles S3Sink: it resolves the sink's credentials, hands the
// resulting configuration to the sink runtime, and reports the runtime's
// asynchronous health verdicts as conditions.
//
// It never talks to S3 (Invariant 1). Everything it knows about a bucket's health
// arrives through the shared probe hub from the sink runtime's own goroutines, so a
// sink pointed at an unreachable bucket costs this reconciler nothing but a
// condition — no reconcile is ever slowed by a store that does not answer.
//
// It is a separate type from SinkReconciler rather than one reconciler generic over
// both sink kinds, because the two share almost nothing beyond the shape: a
// ClickHouseSink resolves one password and reports a schema, an S3Sink resolves a
// key pair *or nothing at all* and reports a bucket. What they do share — the
// verdict store, the status writer, the roll-up derivation, the event on transition
// — they share as functions, which is where the duplication that matters was
// removed.
//
// HistoryUnavailable is the one condition here that is not a health verdict. It is
// this backend's declared capability limit, and it is decided from what the
// *running instance* turned out to implement rather than from the CR — so it is
// read from the sink runtime (SinkRuntime.CapabilitiesFor) rather than derived from
// the kind. That indirection is deliberate: a reconciler that hard-coded "an S3Sink
// cannot read history" would keep reporting it even if this backend ever grew a
// StateReader, which is precisely the kind of stale claim a status condition must
// never make.
type S3SinkReconciler struct {
	// Client reads sinks and Secrets and writes sink status.
	Client client.Client

	// Recorder emits the Warning events that accompany a degrade.
	Recorder record.EventRecorder

	// Sinks is the runtime this reconciler declares configurations to.
	Sinks SinkRuntime

	// BuildConfig maps a sink spec plus its credentials onto a backend
	// configuration. Required.
	BuildConfig S3SinkConfigBuilder

	// OperatorNamespace is the namespace an omitted secretRef.namespace defaults
	// to. That default is a security boundary (see v1alpha1.SecretReference.
	// Namespace), so it is explicit configuration rather than something guessed
	// from the environment at read time.
	OperatorNamespace string

	// Probes is the shared health-verdict hub this reconciler reads its sink's
	// latest probe result from, and is woken by. Required.
	Probes *SinkProbeHub

	// events is the generic-event channel the probe hub pushes onto so a probe
	// result wakes this reconciler for the sink it concerns. Claimed from the hub
	// by SetupWithManager.
	events chan event.GenericEvent

	// ResyncPeriod overrides defaultSinkResyncPeriod. Tests shorten it.
	ResyncPeriod time.Duration
}

// The S3Sink grants are the operator reading its own CRD and writing its status,
// and nothing else: an S3Sink's credentials come from the *existing* namespaced
// Secret Role declared on the ClickHouseSink reconciler, so this backend adds no
// cluster-wide Secret reach. That is the property worth stating, because a
// cluster-scoped sink CR is editable by anyone with cluster-level write access to
// the CRD — if the operator could read Secrets anywhere, creating an S3Sink would
// become a way to make it read any Secret in the cluster and ship it to a bucket.
//
// +kubebuilder:rbac:groups=kuberecord.io,resources=s3sinks,verbs=get;list;watch
// +kubebuilder:rbac:groups=kuberecord.io,resources=s3sinks/status,verbs=get;update;patch

// Reconcile brings one S3Sink's runtime state and status in line with its spec.
//
// The order is credentials first, then runtime, then health — the same order the
// ClickHouseSink reconciler uses and for the same reason: a sink whose Secret is
// missing is never handed to the runtime at all, because an instance built with an
// empty key would fail for a reason that has nothing to do with the actual
// configuration and would be reported as an unreachable bucket rather than as the
// missing Secret it is.
//
// A sink using ambient credentials *is* handed to the runtime immediately: there is
// nothing to resolve from the cluster, and whether the chain produces a credential
// can only be learned by attempting a request — which is the probe's job, not this
// reconciler's.
func (r *S3SinkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var s3Sink v1alpha1.S3Sink
	if err := r.Client.Get(ctx, req.NamespacedName, &s3Sink); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted. Withdraw it from the runtime, which drains the instance,
			// evicts the pipeline state on its own goroutines and then parks the
			// rules that streamed to it (see sink.ParkFunc).
			//
			// No finalizer, deliberately: there is nothing to clean up outside this
			// process (Invariant 6) — the objects already in the bucket are the
			// archive, and deleting a CR must never delete an audit trail — and a
			// sink deleted while the operator was down is picked up by the
			// level-triggered boot pass.
			gone := s3SinkID(req.Name)
			log.Info("S3Sink is gone; withdrawing it from the sink runtime", "sink", gone.String())
			r.Probes.forget(gone)
			r.Sinks.Delete(gone)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get S3Sink %q: %w", req.Name, err)
	}

	status := &statusWriter{generation: s3Sink.Generation}
	creds, credErr := r.resolveCredentials(ctx, &s3Sink)
	if credErr == nil {
		if err := r.declare(&s3Sink, creds); err != nil {
			// A configuration the runtime refused leaves any previous instance
			// running (sink.SinkManager.Ensure is documented to), so this degrades
			// the CR and retries rather than tearing a working sink down.
			log.Error(err, "Failed to declare a sink to the sink runtime", "sink", s3Sink.Name)
			return ctrl.Result{}, err
		}
	}
	r.setHealthConditions(status, &s3Sink, creds, credErr)
	r.setHistoryUnavailable(status, &s3Sink)

	previousReady := findCondition(s3Sink.Status.Conditions, v1alpha1.ConditionReady)
	previousHistory := findCondition(s3Sink.Status.Conditions, v1alpha1.ConditionHistoryUnavailable)
	// s3SinkReadyOrder does not contain HistoryUnavailable, which is what keeps a
	// declared capability limit from degrading a healthy sink (Invariant 5 read the
	// other way round: degradation must be visible, and *only* degradation).
	ready := readyFor(status, s3SinkReadyOrder, ReasonArchiving,
		fmt.Sprintf("Bucket %s accepted a write from this sink's credentials", s3Sink.Spec.Bucket))
	status.set(v1alpha1.ConditionReady, ready.Status, ready.Reason, ready.Message)

	if err := updateStatus(ctx, r.Client, &s3Sink, func(fresh *v1alpha1.S3Sink) {
		status.apply(&fresh.Status.Conditions)
		fresh.Status.ObservedGeneration = status.generation
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("update S3Sink %q status: %w", s3Sink.Name, err)
	}
	emitReadyEvent(r.Recorder, &s3Sink, previousReady, ready)
	// Read back rather than threaded out of the setter: the events are emitted from
	// one place, after the write that made the conditions real, so a status update
	// that failed cannot leave an event claiming something no reader can confirm.
	if history := findCondition(status.conditions, v1alpha1.ConditionHistoryUnavailable); history != nil {
		emitCapabilityLimitEvent(r.Recorder, &s3Sink, previousHistory, *history)
	}

	return ctrl.Result{RequeueAfter: r.resyncPeriod()}, nil
}

// setHistoryUnavailable reports whether this sink's running instance can read its
// own history back.
//
// The answer comes from the sink runtime, not from the kind: the runtime resolved
// it once, by type assertion, when it built the instance (see
// sink.Capabilities). Three states, and the third is the one worth spelling out —
// a sink with no running instance has not been *found* to be capable of anything,
// and reporting False there would be the one genuinely misleading answer available,
// since False is the reassuring value on this inverted condition.
//
// A sink whose credentials never resolved stays in that third state permanently,
// and correctly: it was never handed to the runtime, so nothing about its backend
// has been established. Its CredentialsResolved condition is where an operator is
// meant to be looking.
func (r *S3SinkReconciler) setHistoryUnavailable(status *statusWriter, s3Sink *v1alpha1.S3Sink) {
	caps, live := r.Sinks.CapabilitiesFor(s3SinkID(s3Sink.Name))
	switch {
	case !live:
		status.set(v1alpha1.ConditionHistoryUnavailable, metav1.ConditionUnknown, ReasonCapabilitiesUnknown,
			"This sink has no running instance yet, so what its backend can do has not been detected")
	case caps.HistoryReadable:
		status.set(v1alpha1.ConditionHistoryUnavailable, metav1.ConditionFalse, ReasonHistoryReadable,
			historyReadableMessage)
	default:
		status.set(v1alpha1.ConditionHistoryUnavailable, metav1.ConditionTrue, ReasonWriterOnlySink,
			writerOnlySinkMessage)
	}
}

// resolveCredentials reads the sink's access key out of the Secret its
// spec.credentials.secretRef names, or reports that it uses the ambient chain.
//
// An omitted spec.credentials is not a failure and not a default being applied: it
// is the recommended shape on a cloud provider, where no long-lived key should
// exist to leak. A *present* block with an unreadable Secret, or one carrying only
// half a key, is a failure — and the half-a-key case is called out separately
// because it is the one an author is most likely to have created by hand, and
// because falling back to the ambient chain for it would silently authenticate as
// the pod's own identity instead of the one they wrote down.
func (r *S3SinkReconciler) resolveCredentials(ctx context.Context, s3Sink *v1alpha1.S3Sink) (S3Credentials, *credentialError) {
	if !hasSecretRef(s3Sink) {
		return S3Credentials{}, nil
	}
	ref := r.secretRef(s3Sink)

	var secret corev1.Secret
	if err := r.Client.Get(ctx, ref, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return S3Credentials{}, &credentialError{
				reason:  ReasonSecretNotFound,
				message: fmt.Sprintf("Secret %s does not exist", ref),
			}
		}
		// Forbidden lands here too, and deliberately reads as "not resolved" rather
		// than as an operator crash: the operator's Secret access is
		// namespace-scoped by design (D7), so a sink naming a Secret elsewhere is a
		// rejected request, not a fault.
		return S3Credentials{}, &credentialError{
			reason:  ReasonSecretNotFound,
			message: fmt.Sprintf("Secret %s could not be read: %v", ref, err),
		}
	}

	creds := S3Credentials{
		AccessKeyID:     string(secret.Data[DefaultAccessKeyIDSecretKey]),
		SecretAccessKey: string(secret.Data[DefaultSecretAccessKeySecretKey]),
		SessionToken:    string(secret.Data[DefaultSessionTokenSecretKey]),
	}
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return S3Credentials{}, &credentialError{
			reason: ReasonSecretKeyMissing,
			message: fmt.Sprintf("Secret %s must carry non-empty %q and %q keys (it carries %s)",
				ref, DefaultAccessKeyIDSecretKey, DefaultSecretAccessKeySecretKey, presentKeys(secret)),
		}
	}
	return creds, nil
}

// presentKeys renders the keys a Secret does carry, so the condition message tells
// an author what they actually created rather than only what is missing. The values
// are never rendered — only the key names, which are not secret.
func presentKeys(secret corev1.Secret) string {
	if len(secret.Data) == 0 {
		return "no keys"
	}
	names := make([]string, 0, len(secret.Data))
	for name := range secret.Data {
		names = append(names, fmt.Sprintf("%q", name))
	}
	// Sorted so one Secret always produces one message: map order would otherwise
	// make the condition's text change on every reconcile, and every change is a
	// status write.
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// hasSecretRef reports whether the sink names a Secret at all.
//
// Both levels are checked because both are optional in the schema: the CEL rule on
// S3CredentialsSpec rejects an empty `credentials: {}`, so a present block with no
// secretRef cannot be stored — but a reconciler that assumed that would panic on
// the one object stored before the rule existed.
func hasSecretRef(s3Sink *v1alpha1.S3Sink) bool {
	return s3Sink.Spec.Credentials != nil && s3Sink.Spec.Credentials.SecretRef != nil
}

// secretRef resolves the sink's secretRef to a concrete object key, applying the
// operator-namespace default. It must only be called when hasSecretRef holds.
func (r *S3SinkReconciler) secretRef(s3Sink *v1alpha1.S3Sink) types.NamespacedName {
	ref := s3Sink.Spec.Credentials.SecretRef
	namespace := ref.Namespace
	if namespace == "" {
		namespace = r.OperatorNamespace
	}
	return types.NamespacedName{Namespace: namespace, Name: ref.Name}
}

// declare builds the backend configuration and hands it to the sink runtime.
//
// This is the whole of the reconciler's interaction with a backend, and it is a
// pure struct translation plus a non-blocking hand-off: the runtime diffs the
// configuration's fingerprint, and only if it changed does it build and start a new
// instance — on its own goroutines. A rotated access key changes the fingerprint,
// which is what makes rotation a lossless recycle rather than something this
// reconciler has to orchestrate.
func (r *S3SinkReconciler) declare(s3Sink *v1alpha1.S3Sink, creds S3Credentials) error {
	cfg, err := r.BuildConfig(s3Sink.Name, s3Sink.Spec, creds)
	if err != nil {
		return fmt.Errorf("build configuration for sink %q: %w", s3Sink.Name, err)
	}
	if cfg == nil {
		return fmt.Errorf("build configuration for sink %q: %w", s3Sink.Name, errNoLiveConfig)
	}
	return r.Sinks.Ensure(s3SinkID(s3Sink.Name), cfg)
}

// setHealthConditions writes CredentialsResolved and BucketReachable from the
// cluster-side credential resolution and the latest probe verdict.
//
// The two conditions are decided together because one probe outcome speaks to the
// credential rather than to the bucket. A sink using the ambient chain has a
// complete configuration and nothing to fetch, so it reports
// CredentialsResolved=True on that basis — but if the chain then turns out to
// produce nothing, the probe says so (sink.ProbeReasonCredentialsInvalid) and that
// verdict belongs on *this* condition, not on the bucket's. Reporting a broken IRSA
// binding as an unreachable bucket would send an operator to read firewall rules
// about a role.
//
// BucketReachable has three states, and Unknown is the honest answer both before
// the first probe settles and while the credential is the thing that is wrong: a
// bucket nobody could authenticate to has told us nothing about itself. Only a
// bucket that answered gets a definite verdict.
func (r *S3SinkReconciler) setHealthConditions(status *statusWriter, s3Sink *v1alpha1.S3Sink,
	creds S3Credentials, credErr *credentialError) {
	if credErr != nil {
		status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionFalse, credErr.reason, credErr.message)
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionUnknown, ReasonCredentialsUnavailable,
			"The bucket cannot be checked until the sink's credentials resolve")
		return
	}

	result, probed := r.Probes.latest(s3SinkID(s3Sink.Name))
	if probed && result.Reason == sink.ProbeReasonCredentialsInvalid {
		status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionFalse, ReasonCredentialsUnavailable,
			fmt.Sprintf("No credential could be obtained for this sink: %v", result.Err))
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionUnknown, ReasonCredentialsUnavailable,
			"The bucket cannot be checked until the sink's credentials resolve")
		return
	}
	r.setCredentialsResolved(status, s3Sink, creds)

	switch {
	case !probed:
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionUnknown, ReasonProbePending,
			"Waiting for the first health probe to settle")
	case result.Err == nil:
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionTrue, ReasonBucketWritable,
			fmt.Sprintf("Bucket %s accepted a probe object", s3Sink.Spec.Bucket))
	case result.Reason == sink.ProbeReasonSchemaInvalid:
		// The backend's word for "the shape is wrong" is this sink's word for "the
		// bucket will not accept the objects it is configured to write" — today,
		// Object Lock retention against a bucket that has none. It will not clear
		// with time, which is why it is a False with its own reason rather than the
		// Unknown an unreachable bucket earns.
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionFalse, ReasonBucketIncompatible,
			fmt.Sprintf("Bucket %s will not accept the objects this sink writes: %v",
				s3Sink.Spec.Bucket, result.Err))
	default:
		status.set(v1alpha1.ConditionBucketReachable, metav1.ConditionFalse, ReasonBucketUnreachable,
			fmt.Sprintf("Bucket %s did not answer: %v", s3Sink.Spec.Bucket, result.Err))
	}
}

// setCredentialsResolved reports the credential this sink will authenticate with,
// naming its source: an operator reading the condition must be able to tell "I read
// your Secret" from "I am using whatever identity this pod has", because the two
// fail in completely different places.
func (r *S3SinkReconciler) setCredentialsResolved(status *statusWriter, s3Sink *v1alpha1.S3Sink,
	creds S3Credentials) {
	if creds.IsAmbient() {
		status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionTrue, ReasonAmbientCredentials,
			"This sink names no Secret and authenticates from the ambient credential chain "+
				"(IRSA, workload identity or an instance role)")
		return
	}
	status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionTrue, ReasonSecretResolved,
		fmt.Sprintf("Read the access key from Secret %s", r.secretRef(s3Sink)))
}

// s3SinkReadyOrder is the order the sink's roll-up condition consults its specific
// conditions in, most fundamental first: an operator whose Secret is missing needs
// to hear about the Secret, not about a bucket that could never have been reached
// without it.
var s3SinkReadyOrder = []string{v1alpha1.ConditionCredentialsResolved, v1alpha1.ConditionBucketReachable}

// resyncPeriod is the configured resync or the package default.
func (r *S3SinkReconciler) resyncPeriod() time.Duration {
	if r.ResyncPeriod > 0 {
		return r.ResyncPeriod
	}
	return defaultSinkResyncPeriod
}

// SetupWithManager registers the S3Sink reconciler, claims its wake-up channel from
// the shared probe hub, and installs the Secret field index that makes credential
// rotation a watch.
//
// The Secret watch closes the rotation loop end-to-end: an updated Secret maps back
// through the index to every sink reading it, those sinks re-reconcile, their
// configuration fingerprints change, and the sink runtime recycles each instance
// without losing a queued write. Without the index this would be a poll, and a
// rotated key would take up to a resync period to take effect.
func (r *S3SinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.BuildConfig == nil {
		return errors.New("controller: S3SinkReconciler.BuildConfig is required")
	}
	if r.Sinks == nil {
		return errors.New("controller: S3SinkReconciler.Sinks is required")
	}
	if r.OperatorNamespace == "" {
		return errors.New("controller: S3SinkReconciler.OperatorNamespace is required")
	}
	if r.Probes == nil {
		return errors.New("controller: S3SinkReconciler.Probes is required")
	}
	events, err := r.Probes.register(s3SinkKind, func(name string) client.Object {
		return &v1alpha1.S3Sink{ObjectMeta: metav1.ObjectMeta{Name: name}}
	})
	if err != nil {
		return err
	}
	r.events = events

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.S3Sink{}, s3SecretRefIndexKey,
		func(obj client.Object) []string {
			s3Sink, ok := obj.(*v1alpha1.S3Sink)
			if !ok || !hasSecretRef(s3Sink) {
				return nil
			}
			return []string{r.secretRef(s3Sink).String()}
		}); err != nil {
		return fmt.Errorf("index S3Sink by credentials Secret: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.S3Sink{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.sinksForSecret)).
		WatchesRawSource(source.Channel(r.events, &handler.EnqueueRequestForObject{})).
		Named("s3sink").
		Complete(r)
}

// sinksForSecret maps a Secret event onto the S3Sinks that read their access key
// from it, via the field index.
func (r *S3SinkReconciler) sinksForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var sinks v1alpha1.S3SinkList
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}.String()
	if err := r.Client.List(ctx, &sinks, client.MatchingFields{s3SecretRefIndexKey: key}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list the S3 sinks referencing a Secret", "secret", key)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sinks.Items))
	for i := range sinks.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: sinks.Items[i].Name},
		})
	}
	return requests
}

// Compile-time proof that this type is a reconciler, asserted here rather than
// discovered at wiring time.
var _ reconcile.Reconciler = (*S3SinkReconciler)(nil)
