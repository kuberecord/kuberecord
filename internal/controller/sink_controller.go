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
// The sink's health is push-driven (the probe hub wakes the reconciler on
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
	// Ensure declares that the identified sink must be running with cfg. It must
	// not block on a backend round-trip: the reconciler calls it inline.
	Ensure(id sink.ID, cfg sink.InstanceConfig) error

	// Delete withdraws a sink for good. Draining happens on the runtime's own
	// goroutines, so this returns immediately.
	Delete(id sink.ID)

	// CapabilitiesFor reports what the running instance for a sink can do, or
	// ok=false when none is running yet. It is a read of the runtime's routing
	// snapshot, not a backend round-trip, so it is safe to call inline
	// (Invariant 1) — see sink.SinkManager.CapabilitiesFor.
	CapabilitiesFor(id sink.ID) (sink.Capabilities, bool)
}

// clickHouseSinkKind is the one sink CR kind this build serves.
//
// It equals sink.DefaultSinkKind because ClickHouse is the first backend and, so
// far, the only one — this is that constant's "a ClickHouseSink-specific
// component naming its own kind" use, not a fallback being applied to a reference
// that failed to resolve. It is named separately from the constant it equals
// because the two answer different questions: what an unqualified legacy name
// means, and which kinds this binary can actually resolve. D6's next backend adds
// a kind beside this one, which is when the rule reconciler's single comparison
// becomes a lookup.
const clickHouseSinkKind = sink.DefaultSinkKind

// clickHouseSinkID is the runtime identity of one ClickHouseSink CR.
//
// It is not a default being applied: this reconciler reconciles exactly one kind
// and therefore *knows* its own, which is the only condition under which naming
// the kind outright is legitimate. Keeping the construction in one function is
// what makes "which kind does the ClickHouseSink reconciler register under?" a
// single, greppable answer rather than a literal repeated at every call site.
func clickHouseSinkID(name string) sink.ID {
	return sink.ID{Kind: clickHouseSinkKind, Name: name}
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
	results map[sink.ID]sink.ProbeResult
}

func newProbeStore() *probeStore {
	return &probeStore{results: make(map[sink.ID]sink.ProbeResult)}
}

// record stores a verdict, overwriting any older one for the same sink.
func (s *probeStore) record(result sink.ProbeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result.Sink] = result
}

// latest returns the newest verdict for id, if any has settled yet.
//
// Keying on the whole identity is what keeps two same-named sinks of different
// kinds from reading each other's health: a verdict is about one backend, and an
// S3Sink that cannot be reached says nothing about the ClickHouseSink that
// happens to share its name.
func (s *probeStore) latest(id sink.ID) (sink.ProbeResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.results[id]
	return result, ok
}

// forget drops a deleted sink's verdict, so a sink recreated under the same ID
// starts from ProbePending rather than inheriting its predecessor's health.
func (s *probeStore) forget(id sink.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.results, id)
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

	// Probes is the shared health-verdict hub this reconciler reads its sink's
	// latest probe result from, and is woken by. Required.
	Probes *SinkProbeHub

	// events is the generic-event channel the probe hub pushes onto so a probe
	// result wakes this reconciler for the sink it concerns. Claimed from the hub
	// and wired in through WatchesRawSource(source.Channel(...)) by
	// SetupWithManager.
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
			gone := clickHouseSinkID(req.Name)
			log.Info("ClickHouseSink is gone; withdrawing it from the sink runtime", "sink", gone.String())
			r.Probes.forget(gone)
			r.Sinks.Delete(gone)
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
	ready := readyFor(status, sinkReadyOrder, ReasonConnected,
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
	return r.Sinks.Ensure(clickHouseSinkID(chSink.Name), cfg)
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

	result, probed := r.Probes.latest(clickHouseSinkID(chSink.Name))
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

// readyFor computes the Ready roll-up condition from the specific conditions
// decided this pass.
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
func readyFor(status *statusWriter, order []string, trueReason, trueMessage string) metav1.Condition {
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
		return condition(v1alpha1.ConditionReady, readyStatus, c.Reason, c.Message, status.generation)
	}
	return condition(v1alpha1.ConditionReady, metav1.ConditionTrue, trueReason, trueMessage, status.generation)
}

// resyncPeriod is the configured resync or the package default.
func (r *SinkReconciler) resyncPeriod() time.Duration {
	if r.ResyncPeriod > 0 {
		return r.ResyncPeriod
	}
	return defaultSinkResyncPeriod
}

// SinkProbeHub drains the sink runtime's probe results into a verdict store shared
// by every sink reconciler, and wakes the right reconciler for each result.
//
// It exists as its own runnable because the two ends of the probe path have
// different owners: the sink runtime posts results whenever a backend answers, and
// only a reconcile may write a CR's status (so it retries conflicts, respects
// observedGeneration, and emits one event per transition). Writing status directly
// from this goroutine would put a Kubernetes client on the sink runtime's health
// path and lose all three of those properties.
//
// It is *one* hub for every kind rather than one per reconciler because the sink
// runtime has one result channel, carrying verdicts for every kind of sink it runs
// (sink.SinkManager.ProbeResults). Two drainers over that channel would steal each
// other's results — a ClickHouseSink's verdict consumed by the S3Sink reconciler is
// a verdict nothing ever writes — so the fan-out has to happen after the receive,
// keyed on the verdict's own kind. Each reconciler registers its kind and takes the
// wake-up channel it will be watched through.
type SinkProbeHub struct {
	// results is the sink runtime's probe-result channel. A nil channel is legal
	// and means no runtime feeds this hub (a test, or a deployment assembled
	// without one): Start then simply waits for its context.
	results <-chan sink.ProbeResult

	// store holds the latest verdict per sink. It is shared across kinds, which is
	// safe and correct because it is keyed on the whole sink.ID — an S3Sink and a
	// ClickHouseSink sharing a name have separate entries (see probeStore.latest).
	store *probeStore

	// targets maps a sink kind onto the reconciler serving it. It is written only
	// by register, which runs during setup — before Start — and read only by
	// Start's goroutine, so the mutex guards the handover rather than concurrent
	// traffic.
	mu      sync.Mutex
	targets map[string]probeTarget

	// added records that this hub is already a manager runnable, so a second
	// registration cannot start a second drainer over the same channel.
	added bool
}

// probeTarget is one reconciler's end of the hub: where to send a wake-up, and how
// to name the object it concerns.
type probeTarget struct {
	// events is the generic-event channel the reconciler is watched through.
	events chan event.GenericEvent

	// newObject builds the wake-up object for one sink name. Only the name reaches
	// it: the kind is already fixed by which target was selected, and the object
	// exists solely to tell controller-runtime which CR to reconcile.
	newObject func(name string) client.Object
}

// NewSinkProbeHub builds a hub over the sink runtime's probe results.
//
// results may be nil, which is how a deployment (or a test) with no sink runtime
// still gets a working verdict store and a reconciler that reports ProbePending
// rather than one that cannot be constructed.
func NewSinkProbeHub(results <-chan sink.ProbeResult) *SinkProbeHub {
	return &SinkProbeHub{
		results: results,
		store:   newProbeStore(),
		targets: make(map[string]probeTarget),
	}
}

// SetupWithManager adds the hub to mgr. It must be called exactly once, and it is
// the caller's job rather than a reconciler's precisely because the hub is shared:
// a hub added once per registering reconciler would run one drainer per kind, and
// they would compete for the same results.
func (h *SinkProbeHub) SetupWithManager(mgr ctrl.Manager) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.added {
		return errors.New("controller: the sink probe hub is already registered with a manager")
	}
	if err := mgr.Add(h); err != nil {
		return fmt.Errorf("add the sink probe hub: %w", err)
	}
	h.added = true
	return nil
}

// register claims the wake-up channel for one sink kind and returns it, for the
// caller to watch as a raw source.
//
// Registering the same kind twice is a wiring bug — two reconcilers for one kind
// would each write the other's conditions — so it is refused rather than
// overwritten.
func (h *SinkProbeHub) register(kind string, newObject func(name string) client.Object) (chan event.GenericEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.targets[kind]; exists {
		return nil, fmt.Errorf("controller: sink kind %q is already registered with the probe hub", kind)
	}
	// Capacity absorbs a burst of results from many sinks while a reconcile is in
	// flight. It is a wake-up channel, not a queue of verdicts — the store already
	// holds the latest one per sink — so a modest buffer is enough.
	events := make(chan event.GenericEvent, probeEventCapacity)
	h.targets[kind] = probeTarget{events: events, newObject: newObject}
	return events, nil
}

// latest returns the newest verdict for id, if any has settled yet.
func (h *SinkProbeHub) latest(id sink.ID) (sink.ProbeResult, bool) { return h.store.latest(id) }

// forget drops a deleted sink's verdict, so a sink recreated under the same ID
// starts from ProbePending rather than inheriting its predecessor's health.
func (h *SinkProbeHub) forget(id sink.ID) { h.store.forget(id) }

// target resolves the reconciler serving one kind.
func (h *SinkProbeHub) target(kind string) (probeTarget, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.targets[kind]
	return t, ok
}

// errUnroutableProbe reports a verdict for a kind no reconciler registered. It
// gives the log line a non-nil error value; nothing branches on it.
var errUnroutableProbe = errors.New("no reconciler is registered for this sink's kind, so its verdict cannot be reported")

// Start drains probe results until ctx is cancelled. It satisfies
// manager.Runnable.
//
// Every result is recorded before it is announced, so a wake-up can never arrive
// at a reconciler that would then read a staler verdict than the one that woke it.
func (h *SinkProbeHub) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("sink-probes")
	log.Info("Watching sink health probe results")
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopped watching sink health probe results")
			return nil
		case result := <-h.results:
			h.store.record(result)
			target, ok := h.target(result.Sink.Kind)
			if !ok {
				// The verdict is stored regardless, so a reconciler registered later
				// in this process's life would still read it. Logged at Error because
				// in a running operator it can only mean the wiring built a sink of a
				// kind nothing reconciles (Invariant 4).
				log.Error(errUnroutableProbe, "Dropping a sink health verdict's wake-up",
					"sink", result.Sink.String())
				continue
			}
			// The send is blocking (bounded only by the channel's capacity and the
			// reconciler draining it), because a dropped wake-up would leave a CR
			// claiming a health it no longer has: the store holds the verdict, but
			// nothing would ever read it again.
			select {
			case target.events <- event.GenericEvent{Object: target.newObject(result.Sink.Name)}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// NeedLeaderElection gates the hub on leadership, matching the sink runtime that
// feeds it: a non-leader runs no sink instances, so it has no probe results to
// consume and no business writing another replica's verdicts into CR status.
func (h *SinkProbeHub) NeedLeaderElection() bool { return true }

// SetupWithManager registers the sink reconciler, claims its wake-up channel from
// the shared probe hub, and installs the Secret field index that makes credential
// rotation a watch.
//
// The Secret watch is what closes the rotation loop end-to-end: an updated Secret
// maps back through the index to every sink reading it, those sinks re-reconcile,
// their configuration fingerprints change, and the sink runtime recycles each
// instance without losing a queued write. Without the index this would be a poll,
// and a rotated credential would take up to a resync period to take effect.
func (r *SinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
		return errors.New("controller: SinkReconciler.Probes is required")
	}
	events, err := r.Probes.register(clickHouseSinkKind, func(name string) client.Object {
		return &v1alpha1.ClickHouseSink{ObjectMeta: metav1.ObjectMeta{Name: name}}
	})
	if err != nil {
		return err
	}
	r.events = events

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

// Compile-time proof that the probe hub is a leader-gated manager.Runnable and
// that the production sink runtime satisfies the narrow interface this reconciler
// declares. Both are asserted here rather than discovered at wiring time, where a
// signature drift would surface in a file that has nothing to do with either.
var (
	_ manager.Runnable               = (*SinkProbeHub)(nil)
	_ manager.LeaderElectionRunnable = (*SinkProbeHub)(nil)
	_ SinkRuntime                    = (*sink.SinkManager)(nil)
	_ reconcile.Reconciler           = (*SinkReconciler)(nil)
)
