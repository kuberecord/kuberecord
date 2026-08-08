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

// Package watch holds kuberecord's data plane: the machinery that turns the
// desired-state registry's watch targets (internal/plan) into live Kubernetes
// informers.
//
// It has three parts, each in its own file:
//
//   - The WatchManager (manager.go) is a manager.Runnable that level-triggers a
//     pool of informers towards the registry's snapshot, and is also the
//     pipeline's ListerRegistry — the only view of cluster state the data plane
//     has.
//   - The pool (pool.go) owns one self-managed informer per (GVR, namespace)
//     target, each with its own context and goroutine, because rules come and go
//     at runtime and controller-runtime's cache cannot be re-scoped or partially
//     retired once a manager is running.
//   - The interest map (interest.go) is what makes those informers shareable: it
//     records which sinks (and under which selectors) care about each informer's
//     stream, and it is the authority on whether a scope is still being watched.
//
// The resolver below is the seam between the two vocabularies involved.
// Everything downstream — the dynamic client, the ListWatch, the informer pool —
// is expressed in terms of *resources* (GVRs) and needs to know whether a
// resource is namespaced, while rules are authored in terms of *kinds* (GVKs).
// The resolver is the single place that crosses that gap, and therefore the
// single place that has to cope with a kind that does not exist yet.
package watch

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Condition reasons a rule reconciler (Task 1.7) puts on `ResourceResolved`
// when the corresponding typed error comes back from this package.
//
// They live next to the errors that cause them so the two can never drift: a
// reason string invented at the reconciler would be a second source of truth
// for a verdict this package alone reaches.
const (
	// ReasonKindNotFound accompanies ErrKindNotFound. It is a *transient*
	// verdict — the CRD backing the kind may still be installed later, at which
	// point the rule self-heals with no operator restart.
	ReasonKindNotFound = "KindNotFound"

	// ReasonClusterScopedKind accompanies ErrClusterScopedKind. Unlike
	// ReasonKindNotFound this is permanent until the rule itself is edited: no
	// cluster-side change can make a namespaced rule legal for a cluster-scoped
	// kind.
	ReasonClusterScopedKind = "ClusterScopedKind"
)

// ErrKindNotFound reports that a GVK a rule named has no counterpart in the
// cluster's discovery data.
//
// It is a distinct type rather than a sentinel because "this kind does not
// exist *yet*" is a legitimate, self-healing state — a rule may be applied
// before the CRD it refers to (GitOps applies whole directories in arbitrary
// order) — and the reconciler must be able to tell it apart from "discovery is
// broken", which is an operational problem. Callers classify with errors.As.
type ErrKindNotFound struct {
	// GVK is the group/version/kind that failed to resolve, carried so the
	// reconciler can name it in the rule's condition message without having to
	// remember which of a rule's resources it was asking about.
	GVK schema.GroupVersionKind

	// Err is the REST mapper's own verdict, kept for Unwrap so the precise
	// discovery failure survives into logs. It is one of the several shapes the
	// mapper uses for "no match" — see classify.
	Err error
}

func (e *ErrKindNotFound) Error() string {
	return fmt.Sprintf("kind %s is not installed in this cluster: %v", e.GVK, e.Err)
}

// Unwrap exposes the mapper's error so an operator debugging a stuck rule can
// see whether the group, the version, or only the kind was missing.
func (e *ErrKindNotFound) Unwrap() error { return e.Err }

// ErrClusterScopedKind reports that a namespaced rule named a kind that
// resolves to a cluster-scoped resource.
//
// This is refused rather than silently widened because a StreamRule is
// namespace-delegated authority: its author is trusted with their own
// namespace, and honouring a cluster-scoped kind would let them stream objects
// that are not theirs (D7 — the operator can never be used to escalate). It is
// refused rather than silently narrowed because there is no namespaced subset
// of a cluster-scoped resource to narrow to; the rule as written cannot be
// satisfied at all.
type ErrClusterScopedKind struct {
	// GVK is the kind the rule named.
	GVK schema.GroupVersionKind

	// Resource is what it resolved to. It is included because the plural
	// resource name is what an operator will use to check the scope themselves
	// (`kubectl api-resources`), and because it proves resolution itself
	// succeeded — this error never means "unknown kind".
	Resource schema.GroupVersionResource
}

func (e *ErrClusterScopedKind) Error() string {
	return fmt.Sprintf(
		"kind %s resolves to cluster-scoped resource %q: a namespaced StreamRule may only watch namespaced resources, use a ClusterStreamRule instead",
		e.GVK, e.Resource.Resource,
	)
}

// RuleScope is the authority a resolution request is made under: which of the
// two rule CRDs is asking.
//
// It is an explicit argument rather than something inferred from the target
// namespace because "" (cluster-wide) is a legal namespace for a
// ClusterStreamRule, so the namespace alone cannot distinguish "a cluster rule
// watching everywhere" from "a namespaced rule that forgot its namespace".
type RuleScope uint8

const (
	// NamespacedRule is a StreamRule: confined to its own namespace, and
	// therefore only ever allowed to watch namespaced resources.
	NamespacedRule RuleScope = iota

	// ClusterRule is a ClusterStreamRule: allowed to watch resources of either
	// scope, since its author already holds cluster-level authority.
	ClusterRule
)

// Retry pacing for kinds that failed to resolve. Exponential from baseRetryDelay
// to maxRetryDelay, doubling on each attempt that reaches the API server.
//
// No jitter: the gate is per-GVK and each kind's schedule is anchored to its own
// first failure, so parked kinds do not synchronise into a herd, and the callers
// that drive re-resolution (the reconciler's requeue, the WatchManager's tick)
// are already staggered relative to each other. Determinism buys testability
// here at no cost.
const (
	// baseRetryDelay is how long a freshly parked kind waits before another
	// resolution attempt is allowed through to discovery.
	baseRetryDelay = time.Second

	// maxRetryDelay caps the growth. A CRD installation should start streaming
	// within a window an operator perceives as immediate, so the ceiling is
	// tuned for responsiveness rather than for minimising discovery traffic.
	maxRetryDelay = 30 * time.Second
)

// parkedKind is the retry-gate state for one GVK that failed to resolve.
type parkedKind struct {
	// err is the classified failure, replayed verbatim to callers that arrive
	// while the gate is shut so a rule's condition message stays stable instead
	// of flapping between a real verdict and a generic "still waiting".
	err error

	// delay is the interval that produced retryAt, kept so the next failure can
	// double it. Storing the interval rather than recomputing it from an attempt
	// counter keeps the cap a simple clamp.
	delay time.Duration

	// retryAt is the instant from which a resolution attempt may again reach the
	// REST mapper.
	retryAt time.Time
}

// Resolver turns the GVKs rules are authored in into the GVRs and scopes the
// data plane needs, and absorbs the fact that a named kind may not exist yet.
//
// It is safe for concurrent use: one instance is shared by both rule
// reconcilers and, later, by the WatchManager.
type Resolver struct {
	// mapper is the manager's REST mapper. On the pinned controller-runtime
	// (v0.23.3) this is apiutil's lazy dynamic mapper, which reacts to any
	// no-match by re-running discovery and rebuilding itself before answering —
	// see the note on Resolve. It is therefore *the* rediscovery mechanism; this
	// type deliberately does not hand-roll a second one.
	mapper meta.RESTMapper

	// mu guards parked. It is never held across a call into mapper: discovery is
	// network I/O, and blocking every other rule's resolution behind one slow
	// API server round-trip would turn a degraded cluster into a stalled
	// operator.
	mu sync.Mutex

	// parked holds the retry gate for every GVK whose last resolution failed.
	// Entries are removed on success, so the map is bounded by the number of
	// distinct kinds currently named by rules but not resolvable — in practice a
	// handful, and each entry is a few dozen bytes.
	parked map[schema.GroupVersionKind]*parkedKind

	// now, baseDelay and maxDelay are injectable so the gate's timing can be
	// asserted exactly rather than by sleeping. Production code never sets them;
	// NewResolver fills in the real clock and the constants above.
	now       func() time.Time
	baseDelay time.Duration
	maxDelay  time.Duration
}

// NewResolver returns a Resolver over mapper, which in production is
// mgr.GetRESTMapper().
func NewResolver(mapper meta.RESTMapper) *Resolver {
	return &Resolver{
		mapper:    mapper,
		parked:    make(map[schema.GroupVersionKind]*parkedKind),
		now:       time.Now,
		baseDelay: baseRetryDelay,
		maxDelay:  maxRetryDelay,
	}
}

// Resolve maps gvk onto the resource the dynamic client needs, reporting
// whether that resource is namespaced.
//
// # Self-healing, and why there is no rediscovery hook here
//
// The pinned controller-runtime (v0.23.3) builds mgr.GetRESTMapper() from
// apiutil.NewDynamicRESTMapper, whose mapper answers any lookup that misses by
// running live discovery for the group, rebuilding its inner mapper, and
// retrying once (pkg/client/apiutil/restmapper.go). A kind installed after the
// operator started is therefore picked up by the very next attempt that reaches
// the mapper — no restart, and no reset hook to call. That mapper exposes no
// Reset()/Invalidate() at all, so hand-rolling rediscovery would mean building a
// second discovery client and a second cache that could disagree with the one
// controller-runtime already uses. The retry gate below is what this package
// adds instead.
//
// # The gate
//
// Because every miss costs at least one discovery round-trip, an ungated
// Resolve would let a single rule naming a not-yet-installed kind hammer the API
// server on every reconcile — multiplied by up to 128 resources per rule and by
// every rule in the cluster. So a failed GVK is parked: attempts arriving before
// its retryAt get the previous verdict replayed without touching the mapper, and
// the interval doubles up to maxRetryDelay. Any caller that simply retries
// therefore gets self-healing at a safe rate, which is why this package owns no
// ticker goroutine of its own — the reconciler's requeue and the WatchManager's
// resync tick are already the drivers.
//
// Resolve performs network I/O (discovery) on a cache miss and meta.RESTMapper
// takes no context, so it must only ever be called from a reconciler or the
// WatchManager's own loop — never from an informer handler or a pipeline worker
// (Invariant 1).
func (r *Resolver) Resolve(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
	if err := r.gatedError(gvk); err != nil {
		return schema.GroupVersionResource{}, false, err
	}

	mapping, err := r.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, r.park(gvk, classify(gvk, err))
	}

	r.unpark(gvk)
	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// ResolveForScope is Resolve plus the check that the resolved resource is one a
// rule of this scope is allowed to watch.
//
// It is the entry point reconcilers use; plain Resolve exists for the data
// plane, which acts on targets the control plane has already vetted and must
// not re-derive that verdict.
func (r *Resolver) ResolveForScope(gvk schema.GroupVersionKind, rule RuleScope) (schema.GroupVersionResource, bool, error) {
	gvr, namespaced, err := r.Resolve(gvk)
	if err != nil {
		return schema.GroupVersionResource{}, false, err
	}
	if err := checkScope(gvk, gvr, namespaced, rule); err != nil {
		return schema.GroupVersionResource{}, false, err
	}
	return gvr, namespaced, nil
}

// checkScope is the scope classifier: it decides whether a rule of the given
// scope may watch a resource of the resolved scope.
//
// A violation is deliberately *not* parked by the caller. Parking exists for
// verdicts the cluster can overturn on its own; this one can only be fixed by
// editing the rule, which produces a new generation and a fresh reconcile
// anyway.
func checkScope(gvk schema.GroupVersionKind, gvr schema.GroupVersionResource, namespaced bool, rule RuleScope) error {
	if rule == NamespacedRule && !namespaced {
		return &ErrClusterScopedKind{GVK: gvk, Resource: gvr}
	}
	return nil
}

// classify turns a REST mapper failure into ErrKindNotFound when — and only
// when — it means "this kind is not served by this cluster".
//
// It routes through meta.IsNoMatchError rather than asserting on
// *meta.NoKindMatchError because the pinned mapper reports the same condition in
// more than one shape: an unknown kind inside a known group surfaces as
// *meta.NoKindMatchError, while an entirely unknown *group* surfaces as
// *apiutil.ErrResourceDiscoveryFailed, a multi-error whose Unwrap() []error
// yields *meta.NoResourceMatchError. IsNoMatchError is errors.Is-based in the
// pinned apimachinery (v0.35.0) and so sees through both; a type assertion would
// silently misclassify the missing-group case — by far the common one for a rule
// that precedes its CRD — as a discovery outage.
//
// Anything else (an unreachable or erroring API server) is passed through
// wrapped: it is not the rule's fault and must not be reported as an unknown
// kind.
func classify(gvk schema.GroupVersionKind, err error) error {
	if meta.IsNoMatchError(err) {
		return &ErrKindNotFound{GVK: gvk, Err: err}
	}
	return fmt.Errorf("resolve %s: %w", gvk, err)
}

// gatedError returns the parked verdict for gvk if its retry interval has not
// elapsed, and nil if an attempt may proceed.
func (r *Resolver) gatedError(gvk schema.GroupVersionKind) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	parked, ok := r.parked[gvk]
	if !ok || !r.now().Before(parked.retryAt) {
		return nil
	}
	return parked.err
}

// park records err as gvk's current verdict and shuts the gate for the next
// interval, doubling it up to maxDelay. It returns err so callers can park and
// return in one expression.
func (r *Resolver) park(gvk schema.GroupVersionKind, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	parked, ok := r.parked[gvk]
	if !ok {
		parked = &parkedKind{delay: r.baseDelay}
		r.parked[gvk] = parked
	} else {
		parked.delay = min(parked.delay*2, r.maxDelay)
	}
	parked.err = err
	parked.retryAt = r.now().Add(parked.delay)
	return err
}

// unpark clears gvk's gate after a successful resolution, so a kind that is
// uninstalled and reinstalled later starts again from baseRetryDelay instead of
// inheriting a stale 30-second penalty.
func (r *Resolver) unpark(gvk schema.GroupVersionKind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.parked, gvk)
}
