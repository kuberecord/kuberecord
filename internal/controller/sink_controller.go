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
	"sync"
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
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// DefaultCredentialsSecretKey is the key a sink's credentials Secret must carry
// its password under.
//
// It is fixed rather than configurable per sink: the Secret exists for exactly one
// consumer, and a `key` field on the CR would only add a way to spell the same
// thing two ways (and a second failure mode when the two disagree). It matches the
// key the shipped deployment manifests create.
const DefaultCredentialsSecretKey = "password"

// defaultSinkResyncPeriod is how often a ClickHouseSink is reconciled even when
// nothing about it changed.
//
// The sink's health is push-driven (the probe watcher wakes the reconciler on
// every probe result), so this is only the backstop that re-asserts the sink
// runtime's configuration — it is what brings a sink back up if an Ensure ever
// failed transiently. Two minutes matches the rule reconciler's resync so the two
// tiers converge on the same cadence.
const defaultSinkResyncPeriod = 2 * time.Minute

// secretRefIndexKey is the field-index name under which every ClickHouseSink is
// indexed by the "namespace/name" of the Secret it reads its password from. The
// index is what makes credential rotation a watch rather than a poll: a Secret
// update maps straight back to the sinks that read it.
const secretRefIndexKey = ".spec.connection.credentialsSecretRef"

// probeEventCapacity is how many probe wake-ups may be in flight to the sink
// reconciler. A probe result is never dropped (the watcher's send blocks against the
// manager's context instead), so this only decides how far the sink runtime's probe
// loops may run ahead of a busy reconciler before they wait for it.
const probeEventCapacity = 128

// errNoLiveConfig reports that a sink's configuration could not be assembled, so
// the sink runtime was deliberately left untouched. It backs a log line; nothing
// branches on it.
var errNoLiveConfig = errors.New("sink configuration is incomplete; not handing it to the sink runtime")

// SinkRuntime is the sink runtime as the reconciler needs it: somewhere to declare
// a sink's desired configuration, and somewhere to withdraw it.
//
// It is an interface owned by this package so the reconciler's dependency graph
// contains no ClickHouse driver at all — which is how the Invariant 1 test can
// prove no reconcile path reaches a dialer. sink.SinkManager is the production
// implementation, asserted at the bottom of this file.
type SinkRuntime interface {
	// Ensure declares that the named sink must be running with cfg. It must not
	// block on a backend round-trip: the reconciler calls it inline.
	Ensure(name string, cfg sink.InstanceConfig) error

	// Delete withdraws a sink for good. Draining happens on the runtime's own
	// goroutines, so this returns immediately.
	Delete(name string)
}

// SinkConfigBuilder turns a resolved ClickHouseSink plus its credential into the
// backend configuration the sink runtime builds an instance from.
//
// It is injected rather than implemented here because a backend configuration is
// backend knowledge: the ClickHouse mapping lives in the wiring (Task 1.10), and
// D6's future sinks add a branch there rather than a dependency here. The reason
// that matters beyond tidiness is Invariant 1 — with the mapping injected, this
// package cannot import a driver even by accident, and the reconciler's inability
// to dial ClickHouse is a property of its imports rather than of its control flow.
//
// Implementations must not perform I/O. They translate a struct; the connection is
// established later, by the sink runtime, on its own goroutine.
type SinkConfigBuilder func(name string, spec v1alpha1.ClickHouseSinkSpec, password string) (sink.InstanceConfig, error)

// probeStore holds the most recent probe verdict per sink.
//
// It is the buffer between two independently paced things: probes settle on the
// sink runtime's goroutines whenever a backend answers, and conditions are written
// on the reconciler's. Keeping only the latest verdict per sink is deliberate — a
// condition describes the present, so an older result is not merely redundant but
// wrong to write.
type probeStore struct {
	mu      sync.RWMutex
	results map[string]sink.ProbeResult
}

func newProbeStore() *probeStore {
	return &probeStore{results: make(map[string]sink.ProbeResult)}
}

// record stores a verdict, overwriting any older one for the same sink.
func (s *probeStore) record(result sink.ProbeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result.Sink] = result
}

// latest returns the newest verdict for name, if any has settled yet.
func (s *probeStore) latest(name string) (sink.ProbeResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.results[name]
	return result, ok
}

// forget drops a deleted sink's verdict, so a sink recreated under the same name
// starts from ProbePending rather than inheriting its predecessor's health.
func (s *probeStore) forget(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.results, name)
}

// SinkReconciler reconciles ClickHouseSink: it resolves the credential, hands the
// resulting configuration to the sink runtime, and reports the runtime's
// asynchronous health verdicts as conditions.
//
// It never dials ClickHouse (Invariant 1). Everything it knows about a backend's
// health arrives over the probe channel from the sink runtime's own goroutines, so
// a sink pointed at an unreachable address costs this reconciler nothing but a
// condition — no reconcile is ever slowed by a backend that does not answer.
type SinkReconciler struct {
	// Client reads sinks and Secrets and writes sink status.
	Client client.Client

	// Recorder emits the Warning events that accompany a degrade.
	Recorder record.EventRecorder

	// Sinks is the runtime this reconciler declares configurations to.
	Sinks SinkRuntime

	// BuildConfig maps a sink spec plus its password onto a backend
	// configuration. Required.
	BuildConfig SinkConfigBuilder

	// OperatorNamespace is the namespace an omitted credentialsSecretRef.namespace
	// defaults to. That default is a security boundary (see
	// v1alpha1.SecretReference.Namespace), so it is explicit configuration rather
	// than something guessed from the environment at read time.
	OperatorNamespace string

	// Probes holds the latest probe verdict per sink, filled by the ProbeWatcher.
	Probes *probeStore

	// events is the generic-event channel the ProbeWatcher pushes onto so a probe
	// result wakes this reconciler for the sink it concerns. Wired in through
	// WatchesRawSource(source.Channel(...)) by SetupWithManager.
	events chan event.GenericEvent

	// ResyncPeriod overrides defaultSinkResyncPeriod. Tests shorten it.
	ResyncPeriod time.Duration
}

// The Secret grant is deliberately namespaced (controller-gen emits a Role rather
// than a ClusterRole for a marker carrying `namespace=`), which is what makes
// SecretReference.Namespace's default a security boundary rather than a convenience:
// a cluster-scoped ClickHouseSink is editable by anyone with cluster-level write
// access to the CRD, so a cluster-wide Secret read would turn creating a sink into a
// way to make the operator read any Secret in the cluster. The namespace is spelled
// `system` because kustomize's namespace transformer rewrites it to the deployment's
// real namespace, exactly as it does for the shipped credentials Secret. The matching
// RoleBinding is part of the RBAC task (1.9), which owns config/rbac.
//
// +kubebuilder:rbac:groups=kuberecord.io,resources=clickhousesinks,verbs=get;list;watch
// +kubebuilder:rbac:groups=kuberecord.io,resources=clickhousesinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=system,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile brings one ClickHouseSink's runtime state and status in line with its
// spec.
//
// The order is credential first, then runtime, then health: a sink whose Secret is
// missing is never handed to the runtime at all, because an instance built with an
// empty password would connect (or fail to) for a reason that has nothing to do
// with the actual configuration and would be reported as "unreachable" rather than
// as the missing Secret it is.
func (r *SinkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var chSink v1alpha1.ClickHouseSink
	if err := r.Client.Get(ctx, req.NamespacedName, &chSink); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted. Withdraw it from the runtime, which drains the instance and
			// evicts the pipeline state on its own goroutines and then parks the
			// rules that streamed to it (see Parker).
			//
			// No finalizer is used, deliberately: there is nothing to clean up
			// outside this process (Invariant 6), and a sink deleted while the
			// operator was down is picked up by the level-triggered boot pass
			// rather than by a finalizer that would block the deletion of a CR the
			// operator might never come back to release.
			log.Info("ClickHouseSink is gone; withdrawing it from the sink runtime", "sink", req.Name)
			r.Probes.forget(req.Name)
			r.Sinks.Delete(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get ClickHouseSink %q: %w", req.Name, err)
	}

	status := &statusWriter{generation: chSink.Generation}
	password, credErr := r.resolveCredential(ctx, &chSink)
	if credErr != nil {
		status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionFalse, credErr.reason, credErr.message)
	} else {
		status.set(v1alpha1.ConditionCredentialsResolved, metav1.ConditionTrue, ReasonSecretResolved,
			fmt.Sprintf("Read the password from Secret %s", r.secretRef(&chSink)))
		if err := r.declare(&chSink, password); err != nil {
			// A configuration the runtime refused leaves any previous instance
			// running (sink.SinkManager.Ensure is documented to), so this degrades
			// the CR and retries rather than tearing a working sink down.
			log.Error(err, "Failed to declare a sink to the sink runtime", "sink", chSink.Name)
			return ctrl.Result{}, err
		}
	}

	r.setHealthConditions(status, &chSink, credErr)

	previousReady := findCondition(chSink.Status.Conditions, v1alpha1.ConditionReady)
	ready := readyFor(status, v1alpha1.ConditionReady, sinkReadyOrder, ReasonConnected,
		"ClickHouse is reachable, authenticated and carries the expected schema")
	status.set(v1alpha1.ConditionReady, ready.Status, ready.Reason, ready.Message)

	if err := updateStatus(ctx, r.Client, &chSink, func(fresh *v1alpha1.ClickHouseSink) {
		status.apply(&fresh.Status.Conditions)
		fresh.Status.ObservedGeneration = status.generation
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("update ClickHouseSink %q status: %w", chSink.Name, err)
	}
	emitReadyEvent(r.Recorder, &chSink, previousReady, ready)

	return ctrl.Result{RequeueAfter: r.resyncPeriod()}, nil
}

// credentialError is a credential-resolution failure already classified into the
// condition it produces, so the caller does not re-derive the reason from an error
// string.
type credentialError struct {
	reason  string
	message string
}

func (e *credentialError) Error() string { return e.reason + ": " + e.message }

// resolveCredential reads the sink's password out of the Secret its
// credentialsSecretRef names.
//
// An unreadable Secret and an empty value are both failures. An empty password is
// legal in ClickHouse, but a Secret whose password key is missing or blank is
// overwhelmingly a deployment mistake (a placeholder never substituted, a key
// typo), and reporting it as such is far kinder than a sink that reports itself
// unreachable because it is authenticating as nobody.
func (r *SinkReconciler) resolveCredential(ctx context.Context, chSink *v1alpha1.ClickHouseSink) (string, *credentialError) {
	ref := r.secretRef(chSink)

	var secret corev1.Secret
	if err := r.Client.Get(ctx, ref, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", &credentialError{
				reason:  ReasonSecretNotFound,
				message: fmt.Sprintf("Secret %s does not exist", ref),
			}
		}
		// Forbidden lands here too, and deliberately reads as "not resolved"
		// rather than as an operator crash: the operator's Secret access is
		// namespace-scoped by design (D7), so a sink naming a Secret elsewhere is
		// a rejected request, not a fault.
		return "", &credentialError{
			reason:  ReasonSecretNotFound,
			message: fmt.Sprintf("Secret %s could not be read: %v", ref, err),
		}
	}

	password, ok := secret.Data[DefaultCredentialsSecretKey]
	if !ok || len(password) == 0 {
		return "", &credentialError{
			reason: ReasonSecretKeyMissing,
			message: fmt.Sprintf("Secret %s carries no non-empty %q key",
				ref, DefaultCredentialsSecretKey),
		}
	}
	return string(password), nil
}

// secretRef resolves the sink's credentialsSecretRef to a concrete object key,
// applying the operator-namespace default.
func (r *SinkReconciler) secretRef(chSink *v1alpha1.ClickHouseSink) types.NamespacedName {
	ref := chSink.Spec.Connection.CredentialsSecretRef
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
// configuration's fingerprint, and only if it changed does it build and start a
// new instance — on its own goroutines. A rotated password changes the
// fingerprint, which is what makes rotation a lossless recycle rather than
// something this reconciler has to orchestrate.
func (r *SinkReconciler) declare(chSink *v1alpha1.ClickHouseSink, password string) error {
	cfg, err := r.BuildConfig(chSink.Name, chSink.Spec, password)
	if err != nil {
		return fmt.Errorf("build configuration for sink %q: %w", chSink.Name, err)
	}
	if cfg == nil {
		return fmt.Errorf("build configuration for sink %q: %w", chSink.Name, errNoLiveConfig)
	}
	return r.Sinks.Ensure(chSink.Name, cfg)
}

// setHealthConditions turns the latest probe verdict into SchemaValid.
//
// Three states, not two. Unknown is the honest answer both before the first probe
// settles and while the backend is unreachable: a host that does not answer has
// told us nothing about its schema, and reporting SchemaValid=False for it would
// send an operator to write a migration for a database they cannot reach. Only a
// backend that answered and disagreed about its columns gets a False.
//
// A sink whose credential did not resolve is never probed at all (no instance was
// declared), so its SchemaValid stays Unknown with the credential as the reason.
func (r *SinkReconciler) setHealthConditions(status *statusWriter, chSink *v1alpha1.ClickHouseSink, credErr *credentialError) {
	if credErr != nil {
		status.set(v1alpha1.ConditionSchemaValid, metav1.ConditionUnknown, ReasonCredentialsUnavailable,
			"The schema cannot be checked until the sink's credentials resolve")
		return
	}

	result, probed := r.Probes.latest(chSink.Name)
	switch {
	case !probed:
		status.set(v1alpha1.ConditionSchemaValid, metav1.ConditionUnknown, ReasonProbePending,
			"Waiting for the first health probe to settle")
	case result.Err == nil:
		status.set(v1alpha1.ConditionSchemaValid, metav1.ConditionTrue, ReasonSchemaMatches,
			"The live ClickHouse schema matches the schema this operator writes")
	case result.Reason == sink.ProbeReasonSchemaInvalid:
		status.set(v1alpha1.ConditionSchemaValid, metav1.ConditionFalse, sink.ProbeReasonSchemaInvalid,
			fmt.Sprintf("The live ClickHouse schema does not match the schema this operator writes: %v", result.Err))
	default:
		status.set(v1alpha1.ConditionSchemaValid, metav1.ConditionUnknown, sink.ProbeReasonUnreachable,
			fmt.Sprintf("ClickHouse did not answer, so its schema is unknown: %v", result.Err))
	}
}

// sinkReadyOrder is the order the sink's roll-up condition consults its specific
// conditions in, most fundamental first: an operator whose Secret is missing needs
// to hear about the Secret, not about the schema they cannot check yet.
var sinkReadyOrder = []string{v1alpha1.ConditionCredentialsResolved, v1alpha1.ConditionSchemaValid}

// readyFor computes a roll-up condition from the specific conditions decided this
// pass.
//
// The roll-up is derived rather than set by hand at each decision site, which is
// what keeps the Kubernetes convention that True means healthy mechanically true:
// a future condition type added to the order list degrades Ready automatically,
// and no code path can set Ready=True while something it does not know about is
// False.
//
// A non-True specific condition — False *or* Unknown — makes the roll-up non-True,
// carrying that condition's own status and reason forward: an unreachable sink is
// Ready=False (it is definitely not usable), while a sink no probe has settled for
// is Ready=Unknown (nothing is known to be wrong yet).
func readyFor(status *statusWriter, readyType string, order []string, trueReason, trueMessage string) metav1.Condition {
	for _, condType := range order {
		c := findCondition(status.conditions, condType)
		if c == nil || c.Status == metav1.ConditionTrue {
			continue
		}
		readyStatus := c.Status
		if readyStatus == metav1.ConditionUnknown && c.Reason == sink.ProbeReasonUnreachable {
			// Unreachable is the one Unknown that is a definite verdict about
			// usability: the schema is unknown, but the sink certainly cannot be
			// written to.
			readyStatus = metav1.ConditionFalse
		}
		return condition(readyType, readyStatus, c.Reason, c.Message, status.generation)
	}
	return condition(readyType, metav1.ConditionTrue, trueReason, trueMessage, status.generation)
}

// resyncPeriod is the configured resync or the package default.
func (r *SinkReconciler) resyncPeriod() time.Duration {
	if r.ResyncPeriod > 0 {
		return r.ResyncPeriod
	}
	return defaultSinkResyncPeriod
}

// ProbeWatcher drains the sink runtime's probe results into the reconciler's store
// and wakes the reconciler for each sink a result concerns.
//
// It exists as its own runnable because the two ends of the probe path have
// different owners: the sink runtime posts results whenever a backend answers, and
// only a reconcile may write a CR's status (so it retries conflicts, respects
// observedGeneration, and emits one event per transition). Writing status directly
// from this goroutine would put a Kubernetes client on the sink runtime's health
// path and lose all three of those properties.
type ProbeWatcher struct {
	// Results is the sink runtime's probe-result channel.
	Results <-chan sink.ProbeResult

	// Probes is the store each result is recorded in.
	Probes *probeStore

	// Events is the reconciler's generic-event channel. The send is blocking
	// (bounded only by the channel's capacity and the reconciler draining it),
	// because a dropped result would leave a CR claiming a health it no longer has
	// (Invariant 4) — the source of truth for the verdict is the store, but
	// nothing would ever read it again without the wake-up.
	Events chan<- event.GenericEvent
}

// Start drains probe results until ctx is cancelled. It satisfies
// manager.Runnable.
func (w *ProbeWatcher) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("sink-probes")
	log.Info("Watching sink health probe results")
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopped watching sink health probe results")
			return nil
		case result := <-w.Results:
			w.Probes.record(result)
			obj := &v1alpha1.ClickHouseSink{ObjectMeta: metav1.ObjectMeta{Name: result.Sink}}
			select {
			case w.Events <- event.GenericEvent{Object: obj}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// NeedLeaderElection gates the watcher on leadership, matching the sink runtime
// that feeds it: a non-leader runs no sink instances, so it has no probe results
// to consume and no business writing another replica's verdicts into CR status.
func (w *ProbeWatcher) NeedLeaderElection() bool { return true }

// SetupWithManager registers the sink reconciler, its probe watcher, and the
// Secret field index that makes credential rotation a watch.
//
// The Secret watch is what closes the rotation loop end-to-end: an updated Secret
// maps back through the index to every sink reading it, those sinks re-reconcile,
// their configuration fingerprints change, and the sink runtime recycles each
// instance without losing a queued write. Without the index this would be a poll,
// and a rotated credential would take up to a resync period to take effect.
func (r *SinkReconciler) SetupWithManager(mgr ctrl.Manager, results <-chan sink.ProbeResult) error {
	if r.BuildConfig == nil {
		return errors.New("controller: SinkReconciler.BuildConfig is required")
	}
	if r.Sinks == nil {
		return errors.New("controller: SinkReconciler.Sinks is required")
	}
	if r.OperatorNamespace == "" {
		return errors.New("controller: SinkReconciler.OperatorNamespace is required")
	}
	if r.Probes == nil {
		r.Probes = newProbeStore()
	}
	// Capacity absorbs a burst of results from many sinks while a reconcile is in
	// flight. It is a wake-up channel, not a queue of verdicts — the store already
	// holds the latest one per sink — so a modest buffer is enough.
	r.events = make(chan event.GenericEvent, probeEventCapacity)

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.ClickHouseSink{}, secretRefIndexKey,
		func(obj client.Object) []string {
			chSink, ok := obj.(*v1alpha1.ClickHouseSink)
			if !ok {
				return nil
			}
			return []string{r.secretRef(chSink).String()}
		}); err != nil {
		return fmt.Errorf("index ClickHouseSink by credentials Secret: %w", err)
	}

	if results != nil {
		if err := mgr.Add(&ProbeWatcher{Results: results, Probes: r.Probes, Events: r.events}); err != nil {
			return fmt.Errorf("add the sink probe watcher: %w", err)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ClickHouseSink{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.sinksForSecret)).
		WatchesRawSource(source.Channel(r.events, &handler.EnqueueRequestForObject{})).
		Named("clickhousesink").
		Complete(r)
}

// sinksForSecret maps a Secret event onto the sinks that read their password from
// it, via the field index.
func (r *SinkReconciler) sinksForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var sinks v1alpha1.ClickHouseSinkList
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}.String()
	if err := r.Client.List(ctx, &sinks, client.MatchingFields{secretRefIndexKey: key}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list the sinks referencing a Secret", "secret", key)
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

// Compile-time proof that the probe watcher is a leader-gated manager.Runnable and
// that the production sink runtime satisfies the narrow interface this reconciler
// declares. Both are asserted here rather than discovered at wiring time, where a
// signature drift would surface in a file that has nothing to do with either.
var (
	_ manager.Runnable               = (*ProbeWatcher)(nil)
	_ manager.LeaderElectionRunnable = (*ProbeWatcher)(nil)
	_ SinkRuntime                    = (*sink.SinkManager)(nil)
	_ reconcile.Reconciler           = (*SinkReconciler)(nil)
)
