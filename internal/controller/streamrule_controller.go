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

	authzv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/internal/plan"
	"github.com/yelzhy/kubestream/internal/sink"
	"github.com/yelzhy/kubestream/internal/watch"
)

// defaultRuleResyncPeriod is how often a rule is reconciled even when nothing
// about it, its sink, or the namespaces it selects has changed.
//
// It exists for one specific self-heal: a rule parked on RBACGranted=False must
// come back on its own once an administrator adds the missing grant. Nothing
// watchable happens when a ClusterRole changes — the operator has no business
// watching RBAC objects, and a SelfSubjectAccessReview is the only honest way to
// ask the question anyway — so the answer is re-asked on a timer. Two minutes is
// short enough that an administrator who just applied a grant sees the rule
// activate while they are still looking at it, and long enough that the review
// traffic is negligible.
const defaultRuleResyncPeriod = 2 * time.Minute

// sinkRefIndexKey is the field-index name under which both rule kinds are indexed
// by spec.sinkRef, so a sink event maps straight to the rules that stream to it.
const sinkRefIndexKey = ".spec.sinkRef"

// accessVerbs are the verbs a watch actually needs.
//
// All three are required, not merely `watch`: client-go's informers List before
// they Watch, and the pipeline reads objects back out of the resulting cache, so a
// rule granted `watch` alone would start an informer that fails its initial List
// forever. Reviewing all three up front turns that into one legible condition
// instead of a stream of informer errors.
var accessVerbs = []string{"get", "list", "watch"}

// deniedGroup and deniedKind spell the hard deny-list of D8: v1/Secret is never
// watchable in v1alpha1, whatever any sink's policy says.
//
// The version is deliberately not part of the check. The deny is about the
// *resource* — an object whose whole purpose is to hold credentials — so a future
// v2 Secret, or the same Secret reached through another version, is exactly as
// sensitive. Matching on group and kind alone means a new version cannot become a
// way around the deny.
const (
	deniedGroup = ""
	deniedKind  = "Secret"
)

// maxConditionMessage bounds a condition message. The API server's own limit is
// 32768 bytes; staying well inside it means a status update can never be rejected
// for a message length that is itself a symptom of a degraded rule.
const maxConditionMessage = 4096

// SelfAccessReviewer creates SelfSubjectAccessReviews, i.e. asks the API server
// "may I do this?" about the operator's own identity.
//
// It is a one-method interface rather than a whole kubernetes.Interface so tests
// can answer the question deterministically — envtest RBAC would work too, but a
// fake turns "the grant appears between two resyncs" into an assertion instead of a
// race. The production client satisfies it, asserted at the bottom of this file.
type SelfAccessReviewer interface {
	Create(ctx context.Context, review *authzv1.SelfSubjectAccessReview,
		opts metav1.CreateOptions) (*authzv1.SelfSubjectAccessReview, error)
}

// ruleKind is everything the one reconcile implementation needs to know about
// which of the two rule CRDs it is currently serving.
//
// The two CRDs share their whole spec (ClusterStreamRuleSpec embeds
// StreamRuleSpec) and their entire status shape, so there is exactly one reconcile
// algorithm; what differs is the object type, the scope authority the resolver is
// asked under, and how a rule's target namespaces are derived. Capturing that
// difference in a small descriptor — rather than in two reconcilers, or in a type
// switch inside every step — is what guarantees the two cannot drift: a new
// condition or a new check is written once and applies to both.
type ruleKind struct {
	// kind is the discriminator in a registry rule key (see RuleKey).
	kind string

	// controllerName names the controller for logs and metrics.
	controllerName string

	// scope is the authority the resolver vets a named kind under: a namespaced
	// StreamRule may only watch namespaced resources.
	scope watch.RuleScope

	// newObject and newList build empty typed objects for Get, List and the
	// controller builder's For().
	newObject func() client.Object
	newList   func() client.ObjectList

	// items projects a list into client.Objects, for the map functions that
	// re-enqueue rules.
	items func(client.ObjectList) []client.Object

	// spec and status expose the shared spec and status of either type. Both
	// return zero values for an object of the wrong type, which cannot happen —
	// the descriptor and the object always come from the same registration — but
	// is guarded rather than left to panic (Invariant 5).
	spec   func(client.Object) v1alpha1.StreamRuleSpec
	status func(client.Object) *v1alpha1.StreamRuleStatus

	// namespaceSelector returns the rule's namespace selector, or nil. A
	// StreamRule has none: it is pinned to its own namespace, so the function
	// always returns nil for it and namespace expansion short-circuits.
	namespaceSelector func(client.Object) *metav1.LabelSelector
}

// streamRuleKind is the descriptor for the namespaced StreamRule.
var streamRuleKind = ruleKind{
	kind:           kindStreamRule,
	controllerName: "streamrule",
	scope:          watch.NamespacedRule,
	newObject:      func() client.Object { return &v1alpha1.StreamRule{} },
	newList:        func() client.ObjectList { return &v1alpha1.StreamRuleList{} },
	items: func(list client.ObjectList) []client.Object {
		typed, ok := list.(*v1alpha1.StreamRuleList)
		if !ok {
			return nil
		}
		out := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			out = append(out, &typed.Items[i])
		}
		return out
	},
	spec: func(obj client.Object) v1alpha1.StreamRuleSpec {
		typed, ok := obj.(*v1alpha1.StreamRule)
		if !ok {
			return v1alpha1.StreamRuleSpec{}
		}
		return typed.Spec
	},
	status: func(obj client.Object) *v1alpha1.StreamRuleStatus {
		typed, ok := obj.(*v1alpha1.StreamRule)
		if !ok {
			return nil
		}
		return &typed.Status
	},
	namespaceSelector: func(client.Object) *metav1.LabelSelector { return nil },
}

// clusterStreamRuleKind is the descriptor for the cluster-scoped
// ClusterStreamRule.
var clusterStreamRuleKind = ruleKind{
	kind:           kindClusterStreamRule,
	controllerName: "clusterstreamrule",
	scope:          watch.ClusterRule,
	newObject:      func() client.Object { return &v1alpha1.ClusterStreamRule{} },
	newList:        func() client.ObjectList { return &v1alpha1.ClusterStreamRuleList{} },
	items: func(list client.ObjectList) []client.Object {
		typed, ok := list.(*v1alpha1.ClusterStreamRuleList)
		if !ok {
			return nil
		}
		out := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			out = append(out, &typed.Items[i])
		}
		return out
	},
	spec: func(obj client.Object) v1alpha1.StreamRuleSpec {
		typed, ok := obj.(*v1alpha1.ClusterStreamRule)
		if !ok {
			return v1alpha1.StreamRuleSpec{}
		}
		return typed.Spec.StreamRuleSpec
	},
	status: func(obj client.Object) *v1alpha1.StreamRuleStatus {
		typed, ok := obj.(*v1alpha1.ClusterStreamRule)
		if !ok {
			return nil
		}
		return &typed.Status
	},
	namespaceSelector: func(obj client.Object) *metav1.LabelSelector {
		typed, ok := obj.(*v1alpha1.ClusterStreamRule)
		if !ok {
			return nil
		}
		return typed.Spec.NamespaceSelector
	},
}

// RuleReconciler reconciles StreamRule and ClusterStreamRule: it validates a
// rule's intent and projects the part of it that survives validation into the
// desired-state registry.
//
// It is one implementation with two registrations (see NewStreamRuleReconciler and
// NewClusterStreamRuleReconciler). Reconcile is a pipeline of gates, and each gate
// owns exactly one condition:
//
//   - the sink must exist (Ready / SinkMissing) and be healthy (Ready /
//     SinkNotReady);
//   - every named resource must be admitted by the sink's policy and must not be
//     v1/Secret (PolicyAllowed);
//   - every named kind must resolve to a resource of a scope this rule may watch
//     (ResourceResolved);
//   - the operator's own ServiceAccount must be allowed to get, list and watch
//     each expanded target (RBACGranted).
//
// Two properties of that pipeline are load-bearing. First, a failure is per-target
// wherever it can be: a rule naming five kinds, one of which is not installed,
// streams the other four and says so in ResourceResolved. Only the sink-level and
// policy-level gates are all-or-nothing, because they are verdicts about the rule
// as a whole. Second, no gate touches the data plane directly — the registry is
// the single seam, and everything downstream of it is level-triggered, so a rule
// that fails a gate simply stops contributing targets.
//
// A sink that exists but is not currently Ready is the one deliberate exception to
// "degraded means withdrawn". Its targets stay installed and its watches keep
// running, because an unreachable database is precisely the failure the pipeline's
// requeue path absorbs: withdrawing the targets would evict every dedup baseline
// the sink serves (forcing a full re-emission once it recovers) and would write a
// pair of false scope epochs per scope — and the `Stopped` row could not even be
// written, since the sink is the thing that is down. The rule still reports
// Ready=False/SinkNotReady, which is what an operator needs to see.
type RuleReconciler struct {
	// Client reads rules, sinks and namespaces, and writes rule status.
	Client client.Client

	// Recorder emits the Warning events that accompany a degrade.
	Recorder record.EventRecorder

	// Registry is the desired-state registry this reconciler projects into. It is
	// the only channel by which a rule reaches the data plane.
	Registry *plan.Registry

	// Resolver maps the GVKs a rule is authored in onto GVRs, and is the authority
	// on whether a rule of this scope may watch the resulting resource. It is
	// shared with the WatchManager so a kind that is not installed yet is retried
	// on one backoff gate rather than two.
	Resolver *watch.Resolver

	// Access answers the SelfSubjectAccessReviews that decide RBACGranted.
	Access SelfAccessReviewer

	// ResyncPeriod overrides defaultRuleResyncPeriod. Tests shorten it.
	ResyncPeriod time.Duration

	// Metrics is the shared kubestream_rules gauge both rule kinds count into. It
	// must be the same instance for both reconcilers, since the gauge is a count
	// over the union of their rules; the constructors below default it to the
	// process-wide instance when the caller leaves it nil.
	Metrics *RuleMetrics

	// kind is which of the two CRDs this instance serves.
	kind ruleKind

	// events is the generic-event channel the Parker pushes onto when a sink the
	// rules depend on has gone for good. Created by SetupWithManager.
	events chan event.GenericEvent
}

// NewStreamRuleReconciler returns a RuleReconciler serving the namespaced
// StreamRule. base carries the shared dependencies; the rule kind is not settable
// from outside, so a reconciler can never be built for a kind it has no descriptor
// for.
func NewStreamRuleReconciler(base RuleReconciler) *RuleReconciler {
	base.kind = streamRuleKind
	base.defaultMetrics()
	return &base
}

// NewClusterStreamRuleReconciler returns a RuleReconciler serving the
// cluster-scoped ClusterStreamRule.
func NewClusterStreamRuleReconciler(base RuleReconciler) *RuleReconciler {
	base.kind = clusterStreamRuleKind
	base.defaultMetrics()
	return &base
}

// defaultMetrics points an unset Metrics at the process-wide instance.
//
// Defaulting rather than requiring the field keeps every existing construction
// site working and, more importantly, makes the wiring impossible to get subtly
// wrong: a caller that builds the two reconcilers from one base value shares one
// gauge whether it set the field or not, and a caller that sets it deliberately
// (a test with its own registry) is left alone.
func (r *RuleReconciler) defaultMetrics() {
	if r.Metrics == nil {
		r.Metrics = RuleMetricsInstance()
	}
}

// +kubebuilder:rbac:groups=kubestream.io,resources=streamrules;clusterstreamrules,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubestream.io,resources=streamrules/status;clusterstreamrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=selfsubjectaccessreviews,verbs=create

// Reconcile brings one rule's registry contribution and status in line with its
// spec.
func (r *RuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	ruleKey := RuleKey(r.kind.kind, req.Namespace, req.Name)

	obj := r.kind.newObject()
	if err := r.Client.Get(ctx, req.NamespacedName, obj); err != nil {
		if apierrors.IsNotFound(err) {
			// The rule is gone: withdraw its targets. The WatchManager turns the
			// targets nobody wants any more into stopped informers and the scope
			// recorder turns those into `Stopped` rows — never into `Deleted` rows
			// for the objects that were in scope.
			//
			// There is deliberately no finalizer. A finalizer would exist to run
			// this line before the object disappears, but it buys nothing here:
			// the registry is in-memory state that dies with the process
			// (Invariant 6), and a rule deleted while the operator was down leaves
			// nothing behind for a finalizer to clean up — the level-triggered
			// boot reconciliation (Task 1.6) closes the scope epochs its absence
			// orphaned. What a finalizer *would* add is a failure mode: a rule
			// stuck Terminating because the operator that must release it is not
			// running.
			log.Info("Rule is gone; withdrawing its watch targets", "rule", ruleKey)
			r.Registry.Remove(ruleKey)
			r.Metrics.Forget(ruleKey)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get %s %s: %w", r.kind.kind, req.NamespacedName, err)
	}

	if !obj.GetDeletionTimestamp().IsZero() {
		// Being deleted, with no finalizer of ours to block it. Withdrawing here
		// rather than waiting for the NotFound keeps the data plane from serving a
		// scope for a rule that is already on its way out.
		log.Info("Rule is being deleted; withdrawing its watch targets", "rule", ruleKey)
		r.Registry.Remove(ruleKey)
		r.Metrics.Forget(ruleKey)
		return ctrl.Result{}, nil
	}

	status := &statusWriter{generation: obj.GetGeneration()}
	outcome := r.plan(ctx, obj, status)

	if outcome.install {
		// Upsert rather than Remove-then-Upsert even when some targets were
		// dropped: it replaces the rule's whole contribution atomically, so a rule
		// that lost one of five targets never briefly contributes none of them —
		// which the data plane would see as five scopes stopping and four
		// restarting. An empty target set is Upsert's documented equivalent of
		// Remove, so a fully refused rule needs no special case here.
		if err := r.Registry.Upsert(ruleKey, outcome.targets); err != nil {
			// A malformed selector is the only way this fails, and the registry is
			// documented to be left exactly as it was, so the rule degrades alone
			// (Invariant 5).
			log.Error(err, "Failed to install a rule's watch targets", "rule", ruleKey)
			status.set(v1alpha1.ConditionResourceResolved, metav1.ConditionFalse, ReasonInvalidSelector, err.Error())
		}
	}

	previousReady := findCondition(r.kind.status(obj).Conditions, v1alpha1.ConditionReady)
	ready := r.readyCondition(status, outcome.verdict)
	status.set(v1alpha1.ConditionReady, ready.Status, ready.Reason, ready.Message)

	// Read the count back out of the registry rather than reporting the length of
	// what was just passed in: status.activeWatches is a statement about the
	// desired state that is actually installed, which after a failed Upsert is not
	// the same thing.
	activeWatches := int32(r.Registry.TargetCountForRule(ruleKey)) //nolint:gosec // bounded by the CRD's MaxItems
	// written is the merged condition set the last update attempt actually sent.
	// It is captured inside the mutation rather than read off `status`, because
	// `status` holds only the conditions *this* pass decided: a pass that returned
	// early leaves the rest of the rule's conditions untouched on the object, and
	// the gauge is meant to describe the rule, not the pass.
	var written []metav1.Condition
	if err := updateStatus(ctx, r.Client, obj, func(fresh client.Object) {
		freshStatus := r.kind.status(fresh)
		status.apply(&freshStatus.Conditions)
		freshStatus.ActiveWatches = activeWatches
		freshStatus.ObservedGeneration = status.generation
		written = slices.Clone(freshStatus.Conditions)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("update %s %s status: %w", r.kind.kind, req.NamespacedName, err)
	}
	r.Metrics.Observe(ruleKey, written)
	emitReadyEvent(r.Recorder, obj, previousReady, ready)

	if outcome.err != nil {
		// The failure is already reflected in the conditions written above; the
		// error is returned so the controller's rate limiter — not the resync
		// period — decides when to try again.
		return ctrl.Result{}, outcome.err
	}
	return ctrl.Result{RequeueAfter: r.resyncPeriod()}, nil
}

// sinkVerdict is what the sink gate concluded, carried forward because the roll-up
// condition needs it and it is not expressible as one of the rule's own specific
// conditions — a rule's sink being unreachable says nothing about the rule.
type sinkVerdict struct {
	// reason is empty when the sink is present and Ready.
	reason  string
	message string
}

// planOutcome is one pass's verdict: what to install, what to report, and whether
// to retry.
type planOutcome struct {
	// targets are the watch targets that survived every gate.
	targets []plan.WatchTarget

	// verdict is the sink gate's conclusion.
	verdict sinkVerdict

	// install is false when a transient failure means the rule's existing
	// contribution must be left alone rather than replaced by a set that is
	// short by however far the pass got. Withdrawing working targets because the
	// API server hiccuped would stop watches (and write scope epochs) for a rule
	// nothing is actually wrong with.
	install bool

	// err is a transient failure whose classification is already in the
	// conditions. Non-nil means "retry on the rate limiter".
	err error
}

// plan runs every gate and reports what survived.
//
// It returns targets even when a gate failed, because most gates are per-target: a
// rule naming a not-yet-installed CRD alongside four built-in kinds streams those
// four. The two exceptions are a missing sink and a policy denial, which yield no
// targets at all — nothing about such a rule may legitimately run.
func (r *RuleReconciler) plan(ctx context.Context, obj client.Object, status *statusWriter) planOutcome {
	log := logf.FromContext(ctx)
	spec := r.kind.spec(obj)

	chSink, verdict, err := r.resolveSink(ctx, spec.SinkRef)
	if err != nil {
		return planOutcome{err: err}
	}
	if chSink == nil {
		// No sink, no targets: a watch whose records have nowhere to go is not a
		// watch worth running, and its scope rows would name a sink that does not
		// exist.
		log.Info("Rule references a sink that does not exist; withdrawing its watch targets",
			"rule", RuleKey(r.kind.kind, obj.GetNamespace(), obj.GetName()), "sink", spec.SinkRef)
		r.setUnevaluated(status, verdict.message)
		return planOutcome{verdict: verdict, install: true}
	}

	if denied := checkPolicy(spec.Resources, chSink.Spec.Policy); denied != nil {
		log.Info("Rule is not admitted by its sink's policy; withdrawing its watch targets",
			"rule", RuleKey(r.kind.kind, obj.GetNamespace(), obj.GetName()),
			"sink", chSink.Name, "reason", denied.reason)
		status.set(v1alpha1.ConditionPolicyAllowed, metav1.ConditionFalse, denied.reason, denied.message)
		// The two later gates never ran, and saying nothing about them is more
		// honest than implying they passed.
		status.set(v1alpha1.ConditionResourceResolved, metav1.ConditionUnknown, ReasonNotEvaluated,
			"Resources are not resolved while the sink's policy denies this rule")
		status.set(v1alpha1.ConditionRBACGranted, metav1.ConditionUnknown, ReasonNotEvaluated,
			"Access is not reviewed while the sink's policy denies this rule")
		return planOutcome{verdict: verdict, install: true}
	}
	status.set(v1alpha1.ConditionPolicyAllowed, metav1.ConditionTrue, ReasonAllResourcesPermitted,
		fmt.Sprintf("Every resource this rule names is admitted by sink %q", chSink.Name))

	namespaces, err := r.targetNamespaces(ctx, obj)
	if err != nil {
		var invalid *errInvalidNamespaceSelector
		if errors.As(err, &invalid) {
			// The selector cannot be honoured as written. That is a verdict about
			// the rule, and only its author can fix it, so the rule degrades and
			// stops contributing rather than retrying forever.
			status.set(v1alpha1.ConditionResourceResolved, metav1.ConditionFalse,
				ReasonNamespaceSelectorInvalid, err.Error())
			return planOutcome{verdict: verdict, install: true}
		}
		// The listing failed. Nothing about the rule's target set is known this
		// pass, so the existing one is left alone: withdrawing it would stop
		// watches — and write scope epochs — because a cache read hiccuped.
		status.set(v1alpha1.ConditionResourceResolved, metav1.ConditionUnknown,
			ReasonNamespacesUnavailable, err.Error())
		return planOutcome{verdict: verdict, err: err}
	}

	resolved, resolveFailures := r.resolveResources(namespaces, spec.Resources)
	setGateCondition(status, v1alpha1.ConditionResourceResolved, resolveFailures,
		ReasonAllKindsResolved, "Every kind this rule names resolved to a watchable resource",
		ReasonKindsUnresolved)

	// The rule's additions are merged with the sink's floor once per pass, here,
	// rather than per target: every target of one rule streams to one sink, so
	// the answer cannot differ between them.
	redaction := canonicalRedaction(chSink.Spec.Policy.Redaction, spec.ExtraRedaction)

	targets, denials, err := r.reviewTargets(ctx, chSink.Name, resolved, redaction)
	if err != nil {
		status.set(v1alpha1.ConditionRBACGranted, metav1.ConditionUnknown, ReasonAccessReviewFailed, err.Error())
		return planOutcome{verdict: verdict, err: err}
	}
	setGateCondition(status, v1alpha1.ConditionRBACGranted, denials,
		ReasonAllVerbsGranted, "The operator may get, list and watch every resource this rule names",
		ReasonMissingPermissions)

	return planOutcome{targets: targets, verdict: verdict, install: true}
}

// resolveSink loads the rule's sink and judges its health.
//
// A missing sink returns a nil sink with the SinkMissing verdict. A present but
// unhealthy sink returns the sink *and* a SinkNotReady verdict, because the rule's
// targets stay installed in that case — see RuleReconciler's doc comment for why.
func (r *RuleReconciler) resolveSink(ctx context.Context, name string) (*v1alpha1.ClickHouseSink, sinkVerdict, error) {
	var chSink v1alpha1.ClickHouseSink
	if err := r.Client.Get(ctx, types.NamespacedName{Name: name}, &chSink); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, sinkVerdict{
				reason:  ReasonSinkMissing,
				message: fmt.Sprintf("ClickHouseSink %q does not exist", name),
			}, nil
		}
		return nil, sinkVerdict{}, fmt.Errorf("get ClickHouseSink %q: %w", name, err)
	}

	ready := findCondition(chSink.Status.Conditions, v1alpha1.ConditionReady)
	switch {
	case ready == nil:
		return &chSink, sinkVerdict{
			reason:  ReasonSinkNotReady,
			message: fmt.Sprintf("ClickHouseSink %q has not reported its health yet", name),
		}, nil
	case ready.Status != metav1.ConditionTrue:
		return &chSink, sinkVerdict{
			reason:  ReasonSinkNotReady,
			message: fmt.Sprintf("ClickHouseSink %q is not ready (%s: %s)", name, ready.Reason, ready.Message),
		}, nil
	default:
		return &chSink, sinkVerdict{}, nil
	}
}

// setUnevaluated marks the rule's own three conditions Unknown because no gate
// could run without a sink: the sink's policy decides PolicyAllowed and the sink's
// name is part of every target, so none of the three is answerable.
func (r *RuleReconciler) setUnevaluated(status *statusWriter, message string) {
	for _, condType := range ruleReadyOrder {
		status.set(condType, metav1.ConditionUnknown, ReasonNotEvaluated, message)
	}
}

// policyDenial is a policy refusal already classified into the condition it
// produces.
type policyDenial struct {
	reason  string
	message string
}

// checkPolicy applies the hard deny-list and then the sink's allow-list to every
// resource a rule names, returning the first refusal.
//
// The deny-list is checked first and is not overridable: v1/Secret is never
// watchable in v1alpha1 (D8), and the check lives in code precisely so that the
// permissive default of an empty allowedGVKs — which admits everything else — can
// never become a way to stream credentials. A sink policy that explicitly lists
// Secrets is refused too, with the same reason.
//
// The refusal is all-or-nothing for the rule rather than per-resource. Policy is
// the sink owner's admission decision (see v1alpha1.SinkPolicy), and quietly
// streaming the four resources of a five-resource rule they refused would honour
// the letter of the policy while defeating its point: the rule's author must see
// their rule refused, not silently narrowed.
func checkPolicy(resources []v1alpha1.WatchedResource, policy v1alpha1.SinkPolicy) *policyDenial {
	for _, res := range resources {
		if res.Group == deniedGroup && res.Kind == deniedKind {
			return &policyDenial{
				reason: ReasonSecretsDenied,
				message: fmt.Sprintf(
					"%s is never watchable in v1alpha1: Secrets are denied in code and no sink policy can admit them",
					gvkString(res.Group, res.Version, res.Kind)),
			}
		}
	}

	if len(policy.AllowedGVKs) == 0 {
		// An empty allow-list admits everything except the deny-list above.
		return nil
	}
	for _, res := range resources {
		if !policyAdmits(policy.AllowedGVKs, res) {
			return &policyDenial{
				reason: ReasonNotInAllowList,
				message: fmt.Sprintf("%s is not in the sink's spec.policy.allowedGVKs",
					gvkString(res.Group, res.Version, res.Kind)),
			}
		}
	}
	return nil
}

// policyAdmits reports whether any selector in the allow-list covers res.
func policyAdmits(allowed []v1alpha1.GVKSelector, res v1alpha1.WatchedResource) bool {
	for _, sel := range allowed {
		if sel.Group != res.Group || sel.Version != res.Version {
			continue
		}
		if slices.Contains(sel.Kinds, "*") || slices.Contains(sel.Kinds, res.Kind) {
			return true
		}
	}
	return false
}

// resolvedResource is one of a rule's resources after the resolver has vetted it:
// the GVK to watch, the canonical selector to filter with, and the namespaces the
// rule wants it in.
type resolvedResource struct {
	gvk        schema.GroupVersionKind
	gvr        schema.GroupVersionResource
	selector   string
	namespaces []string
}

// resolveResources vets every named resource against the rule's already-expanded
// namespace set, collecting per-resource failures rather than stopping at the first.
//
// Collecting is the point: a rule naming a not-yet-installed CRD alongside four
// built-in kinds must stream those four. The failures become one ResourceResolved
// message listing everything that did not resolve.
func (r *RuleReconciler) resolveResources(namespaces []string,
	resources []v1alpha1.WatchedResource) ([]resolvedResource, []string) {
	resolved := make([]resolvedResource, 0, len(resources))
	var failures []string
	for _, res := range resources {
		gvk := schema.GroupVersionKind{Group: res.Group, Version: res.Version, Kind: res.Kind}
		gvr, namespaced, err := r.Resolver.ResolveForScope(gvk, r.kind.scope)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", gvk, err))
			continue
		}
		selector, err := plan.CanonicalSelector(res.LabelSelector)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", gvk, err))
			continue
		}

		resolved = append(resolved, resolvedResource{
			gvk:        gvk,
			gvr:        gvr,
			selector:   selector,
			namespaces: namespacesFor(namespaced, namespaces),
		})
	}
	return resolved, failures
}

// errInvalidNamespaceSelector marks a namespace selector that cannot be converted
// to a selector at all, as distinct from a listing that merely failed.
//
// The distinction decides whether the rule's targets are withdrawn. An unconvertible
// selector is permanent until the rule is edited, so the rule degrades and stops
// contributing; a failed List is transient, so the rule keeps whatever it already
// had and the pass retries. Conflating the two would let one cache hiccup stop every
// watch a wide ClusterStreamRule owns.
type errInvalidNamespaceSelector struct{ err error }

func (e *errInvalidNamespaceSelector) Error() string {
	return fmt.Sprintf("%s: %v", ReasonNamespaceSelectorInvalid, e.err)
}

func (e *errInvalidNamespaceSelector) Unwrap() error { return e.err }

// namespacesFor picks the namespaces one resource is watched in.
//
// A cluster-scoped resource ignores the rule's namespace expansion entirely: it
// has no namespace to select on, so it yields the single all-namespaces target the
// dynamic client needs. Only a ClusterStreamRule reaches this branch — the
// resolver refuses a cluster-scoped kind under a namespaced rule.
func namespacesFor(namespaced bool, expanded []string) []string {
	if !namespaced {
		return []string{""}
	}
	return expanded
}

// targetNamespaces expands the rule into the namespaces its targets live in.
//
// Three cases, and the difference between the last two is the whole reason
// ClusterStreamRule has a selector:
//
//   - a StreamRule is pinned to its own namespace;
//   - a ClusterStreamRule with no selector is a single all-namespaces target
//     (Namespace=""), i.e. one informer for the whole cluster;
//   - a ClusterStreamRule with a selector is one target *per matching namespace*,
//     re-derived on every reconcile — and the Namespace watch registered in
//     SetupWithManager is what makes a namespace gaining or losing a label
//     re-project those targets live.
//
// The per-namespace expansion is deliberately not collapsed into an
// all-namespaces watch plus a filter. The selector is the rule author's statement
// about which namespaces this rule may read at all, and honouring it as a set of
// narrow watches means a namespace outside the selector is never listed, never
// cached and never streamed — not even transiently.
func (r *RuleReconciler) targetNamespaces(ctx context.Context, obj client.Object) ([]string, error) {
	if r.kind.scope == watch.NamespacedRule {
		return []string{obj.GetNamespace()}, nil
	}

	selector := r.kind.namespaceSelector(obj)
	if selector == nil {
		return []string{""}, nil
	}

	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, &errInvalidNamespaceSelector{err: err}
	}

	var namespaces corev1.NamespaceList
	if err := r.Client.List(ctx, &namespaces, client.MatchingLabelsSelector{Selector: labelSelector}); err != nil {
		return nil, fmt.Errorf("list namespaces matching %q: %w", labelSelector.String(), err)
	}

	out := make([]string, 0, len(namespaces.Items))
	for i := range namespaces.Items {
		// A namespace on its way out is skipped: its objects are being deleted by
		// the namespace controller, and starting a watch on it would record that
		// teardown as ordinary deletions in a scope that is about to stop anyway.
		if !namespaces.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		out = append(out, namespaces.Items[i].Name)
	}
	slices.Sort(out)
	return out, nil
}

// reviewTargets asks the API server whether the operator may watch each expanded
// target, and returns the targets that were allowed.
//
// A denial drops that target and is collected into the RBACGranted message; it
// never fails the rule, so a ClusterStreamRule allowed in nineteen of twenty
// namespaces streams nineteen.
//
// Verdicts are cached per (GVR, namespace) within the pass, because two resources
// of one rule can expand onto the same namespace set and the answer cannot differ
// between them.
//
// redaction is the rule's canonical merged redaction policy (see
// canonicalRedaction), stamped identically onto every target it produces.
func (r *RuleReconciler) reviewTargets(ctx context.Context, sinkName string,
	resolved []resolvedResource, redaction string) ([]plan.WatchTarget, []string, error) {
	targets := make([]plan.WatchTarget, 0, len(resolved))
	var denials []string
	seen := make(map[string]string)

	for _, res := range resolved {
		for _, namespace := range res.namespaces {
			cacheKey := res.gvr.String() + "|" + namespace
			denial, cached := seen[cacheKey]
			if !cached {
				var err error
				denial, err = r.reviewTarget(ctx, res.gvr, namespace)
				if err != nil {
					// The review itself failed, as opposed to answering "no",
					// which is not a verdict about the rule at all: the caller
					// retries rather than parking it on a conclusion nobody
					// reached.
					return nil, nil, err
				}
				seen[cacheKey] = denial
			}
			if denial != "" {
				if !slices.Contains(denials, denial) {
					denials = append(denials, denial)
				}
				continue
			}

			targets = append(targets, plan.WatchTarget{
				Sink:      sinkName,
				GVK:       res.gvk,
				Namespace: namespace,
				Selector:  res.selector,
				Redaction: redaction,
			})
		}
	}
	return targets, denials, nil
}

// reviewTarget runs the access reviews for one (GVR, namespace) target, returning
// an empty string when every verb is allowed and a message naming the resource and
// the missing verbs otherwise.
//
// A namespaced target is checked at cluster scope first and only then in its own
// namespace. The cluster-wide question is asked first because it is the common
// answer — the operator's watch grants come from an aggregated ClusterRole (D7) —
// and one allowed cluster-wide review makes every namespace's answer known without
// asking. Without that short-circuit a ClusterStreamRule expanding onto a hundred
// namespaces would cost three hundred reviews per resync, per resource.
func (r *RuleReconciler) reviewTarget(ctx context.Context,
	gvr schema.GroupVersionResource, namespace string) (string, error) {
	var missing []string
	for _, verb := range accessVerbs {
		allowed, err := r.review(ctx, gvr, "", verb)
		if err != nil {
			return "", err
		}
		if !allowed && namespace != "" {
			allowed, err = r.review(ctx, gvr, namespace, verb)
			if err != nil {
				return "", err
			}
		}
		if !allowed {
			missing = append(missing, verb)
		}
	}
	if len(missing) == 0 {
		return "", nil
	}

	scope := "at cluster scope"
	if namespace != "" {
		scope = fmt.Sprintf("in namespace %q", namespace)
	}
	return fmt.Sprintf("%s: missing %s %s", gvr.GroupResource(), strings.Join(missing, ","), scope), nil
}

// review runs one SelfSubjectAccessReview.
//
// SelfSubjectAccessReview — rather than reading the operator's own RoleBindings —
// is the only correct way to ask this: it is the API server's own authorizer
// answering about the operator's effective identity, so it accounts for aggregated
// ClusterRoles, group membership and any webhook authorizer in the chain, none of
// which are derivable from objects an operator could read.
func (r *RuleReconciler) review(ctx context.Context,
	gvr schema.GroupVersionResource, namespace, verb string) (bool, error) {
	review := &authzv1.SelfSubjectAccessReview{
		Spec: authzv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authzv1.ResourceAttributes{
				Group:     gvr.Group,
				Version:   gvr.Version,
				Resource:  gvr.Resource,
				Namespace: namespace,
				Verb:      verb,
			},
		},
	}
	result, err := r.Access.Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		return false, fmt.Errorf("review access to %s (verb %q, namespace %q): %w",
			gvr.GroupResource(), verb, namespace, err)
	}
	return result.Status.Allowed, nil
}

// setGateCondition renders one gate's collected failures into one condition.
//
// Failures are joined into a single message rather than reported one condition per
// failure, because they are all the same *kind* of problem and an operator reading
// `kubectl describe` wants one place to look. The message is capped so a rule
// naming 128 unresolvable kinds cannot produce a status object the API server
// refuses to store.
func setGateCondition(status *statusWriter, condType string, failures []string,
	trueReason, trueMessage, falseReason string) {
	if len(failures) == 0 {
		status.set(condType, metav1.ConditionTrue, trueReason, trueMessage)
		return
	}
	status.set(condType, metav1.ConditionFalse, falseReason, truncateMessage(strings.Join(failures, "; ")))
}

// truncateMessage caps a condition message, saying so rather than silently dropping
// the tail.
func truncateMessage(message string) string {
	if len(message) <= maxConditionMessage {
		return message
	}
	const suffix = " […truncated]"
	return message[:maxConditionMessage-len(suffix)] + suffix
}

// readyCondition rolls the rule's specific conditions plus the sink verdict up into
// Ready.
//
// The sink verdict is consulted first: a rule whose sink is missing or unhealthy
// has a problem that is not about the rule, and reporting the rule's own
// (unevaluated) conditions ahead of it would point an operator at the wrong object.
func (r *RuleReconciler) readyCondition(status *statusWriter, verdict sinkVerdict) metav1.Condition {
	if verdict.reason != "" {
		return condition(v1alpha1.ConditionReady, metav1.ConditionFalse,
			verdict.reason, verdict.message, status.generation)
	}
	return readyFor(status, v1alpha1.ConditionReady, ruleReadyOrder, ReasonStreaming,
		"Every resource this rule names is admitted, resolved, permitted and installed in the watch plan")
}

// ruleReadyOrder is the order the rule's roll-up consults its specific conditions
// in: policy (the sink owner's refusal) before resolution (the cluster's state)
// before RBAC (the administrator's grant), which is the order in which an operator
// can act on them. It doubles as the list of conditions that go Unknown when no
// gate could run at all.
var ruleReadyOrder = []string{
	v1alpha1.ConditionPolicyAllowed,
	v1alpha1.ConditionResourceResolved,
	v1alpha1.ConditionRBACGranted,
}

// resyncPeriod is the configured resync or the package default.
func (r *RuleReconciler) resyncPeriod() time.Duration {
	if r.ResyncPeriod > 0 {
		return r.ResyncPeriod
	}
	return defaultRuleResyncPeriod
}

// gvkString renders a group/version/kind the way a rule's author wrote it
// ("v1/Secret", "apps/v1/Deployment"), so a condition message is greppable against
// the CR that caused it.
func gvkString(group, version, kind string) string {
	if group == "" {
		return version + "/" + kind
	}
	return group + "/" + version + "/" + kind
}

// SetupWithManager registers this reconciler along with the three things that must
// re-enqueue a rule besides its own edits.
//
// Sinks, because a sink becoming Ready (or unreachable, or deleted) changes every
// dependent rule's verdict, and a rule that re-derived that only on its resync
// would stay parked for up to two minutes after its sink recovered. Namespaces,
// because a ClusterStreamRule's selector is evaluated against namespace labels, so
// labelling a namespace in or out must re-project its targets immediately — that is
// the whole point of the selector being dynamic. And the Parker's channel, because
// a sink that has finished draining is reported by the sink runtime, not by the API
// server.
func (r *RuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Registry == nil {
		return errors.New("controller: RuleReconciler.Registry is required")
	}
	if r.Resolver == nil {
		return errors.New("controller: RuleReconciler.Resolver is required")
	}
	if r.Access == nil {
		return errors.New("controller: RuleReconciler.Access is required")
	}
	if r.kind.newObject == nil {
		return errors.New("controller: RuleReconciler must be built by NewStreamRuleReconciler " +
			"or NewClusterStreamRuleReconciler")
	}
	if r.events == nil {
		r.events = make(chan event.GenericEvent, parkChannelCapacity)
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), r.kind.newObject(), sinkRefIndexKey,
		func(obj client.Object) []string { return []string{r.kind.spec(obj).SinkRef} }); err != nil {
		return fmt.Errorf("index %s by spec.sinkRef: %w", r.kind.kind, err)
	}

	builder := ctrl.NewControllerManagedBy(mgr).
		For(r.kind.newObject()).
		Watches(&v1alpha1.ClickHouseSink{}, handler.EnqueueRequestsFromMapFunc(r.rulesForSink)).
		WatchesRawSource(source.Channel(r.events, &handler.EnqueueRequestForObject{})).
		Named(r.kind.controllerName)

	if r.kind.scope == watch.ClusterRule {
		builder = builder.Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.rulesForNamespace))
	}

	return builder.Complete(r)
}

// parkChannelCapacity is how many pending wake-ups a rule reconciler's park channel
// holds. It absorbs one sink deletion's worth of dependent rules without blocking
// the sink runtime's drain goroutine; beyond that, a dropped wake-up costs at most
// one resync period of staleness (see Parker.SinkGone).
const parkChannelCapacity = 128

// rulesForSink maps a ClickHouseSink event onto the rules that stream to it, via
// the spec.sinkRef field index.
func (r *RuleReconciler) rulesForSink(ctx context.Context, obj client.Object) []reconcile.Request {
	list := r.kind.newList()
	if err := r.Client.List(ctx, list, client.MatchingFields{sinkRefIndexKey: obj.GetName()}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list the rules referencing a sink",
			"sink", obj.GetName(), "kind", r.kind.kind)
		return nil
	}
	return requestsFor(r.kind.items(list))
}

// rulesForNamespace re-enqueues every ClusterStreamRule carrying a namespace
// selector when a namespace changes.
//
// Only rules with a non-nil selector are enqueued: a rule watching all namespaces
// derives nothing from a namespace's labels, so waking it would be pure churn — and
// on a cluster where namespaces come and go constantly, churn proportional to the
// namespace creation rate times the rule count.
//
// It cannot narrow further by *which* labels changed, because a selector may match
// on a label's absence as well as its presence; every selector-carrying rule has to
// re-evaluate.
func (r *RuleReconciler) rulesForNamespace(ctx context.Context, _ client.Object) []reconcile.Request {
	var rules v1alpha1.ClusterStreamRuleList
	if err := r.Client.List(ctx, &rules); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list the cluster rules to re-project after a namespace change")
		return nil
	}
	var selecting []client.Object
	for i := range rules.Items {
		if rules.Items[i].Spec.NamespaceSelector != nil {
			selecting = append(selecting, &rules.Items[i])
		}
	}
	return requestsFor(selecting)
}

// requestsFor turns objects into reconcile requests.
func requestsFor(objs []client.Object) []reconcile.Request {
	requests := make([]reconcile.Request, 0, len(objs))
	for _, obj := range objs {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()},
		})
	}
	return requests
}

// Parker bridges the sink runtime's "this sink is gone for good" callback back onto
// the rule reconcilers.
//
// The sink runtime reports dependent rules by registry key, from the goroutine that
// just finished draining the sink, and is documented to require a callback that
// does not block. So this translates keys into generic events and hands them over
// with a non-blocking send: the reconcile that follows re-reads the world and
// reaches the SinkMissing verdict on its own. No verdict is decided here, which is
// what keeps the callback cheap and the decision in one place.
type Parker struct {
	// channels maps a rule kind (see RuleKey) to that reconciler's event channel.
	channels map[string]chan event.GenericEvent
}

// NewParker builds a Parker feeding the given reconcilers. Each must already have
// been registered with a manager, since SetupWithManager is what creates its
// channel; a reconciler without one is skipped rather than silently accepted, so a
// wiring mistake surfaces as a logged park failure instead of a nil-channel
// deadlock.
func NewParker(reconcilers ...*RuleReconciler) *Parker {
	p := &Parker{channels: make(map[string]chan event.GenericEvent, len(reconcilers))}
	for _, r := range reconcilers {
		if r == nil || r.events == nil {
			continue
		}
		p.channels[r.kind.kind] = r.events
	}
	return p
}

// SinkGone implements sink.ParkFunc: it wakes every rule that streamed to a sink
// that has gone away.
//
// A full channel drops the wake-up rather than blocking the sink runtime's drain
// goroutine, and says so at Error (Invariant 4). Dropping is safe but not free: the
// rule keeps its stale Ready=True until its own resync, at most one resync period
// later. Blocking the drain would be worse — it would hold up every other sink's
// lifecycle behind one busy reconciler.
func (p *Parker) SinkGone(sinkName string, ruleKeys []string) {
	log := logf.Log.WithName("sink-park")
	for _, key := range ruleKeys {
		kind, ref, ok := parseRuleKey(key)
		if !ok {
			log.Error(errUnparseableRuleKey, "Cannot park a rule whose registry key is unrecognised",
				"sink", sinkName, "rule", key)
			continue
		}
		events, known := p.channels[kind]
		if !known {
			log.Error(errUnparseableRuleKey, "No reconciler is registered for a rule kind that must be parked",
				"sink", sinkName, "rule", key, "kind", kind)
			continue
		}

		select {
		case events <- event.GenericEvent{Object: newRuleStub(kind, ref)}:
		default:
			log.Error(errParkChannelFull, "Dropping a park trigger; the rule re-reconciles on its next resync",
				"sink", sinkName, "rule", key)
		}
	}
}

// newRuleStub builds the minimal object a generic event needs: the handler reads
// only its namespace and name to form a reconcile request.
func newRuleStub(kind string, ref types.NamespacedName) client.Object {
	meta := metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name}
	if kind == kindClusterStreamRule {
		return &v1alpha1.ClusterStreamRule{ObjectMeta: meta}
	}
	return &v1alpha1.StreamRule{ObjectMeta: meta}
}

// errUnparseableRuleKey and errParkChannelFull give the log lines above a non-nil
// error value. Neither is branched on: the first is a wiring bug and the second a
// bounded degradation the next resync repairs.
var (
	errUnparseableRuleKey = errors.New("registry rule key does not name a known rule kind")
	errParkChannelFull    = errors.New("rule park channel is full")
)

// Compile-time proof of the contracts this file exists to satisfy: the production
// authorization client answers the access reviews, the desired-state registry is
// the sink runtime's dependent-rule oracle, and the Parker is a sink.ParkFunc.
// Asserted here rather than at wiring time (Task 1.10), where a signature drift
// would surface in a file that has nothing to do with any of them.
var (
	_ SelfAccessReviewer   = (authorizationv1client.SelfSubjectAccessReviewInterface)(nil)
	_ reconcile.Reconciler = (*RuleReconciler)(nil)
	_ sink.Dependents      = (*plan.Registry)(nil)
	_ sink.ParkFunc        = (&Parker{}).SinkGone
)
