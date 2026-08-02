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

package watch

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/plan"
)

// informerKey identifies one running informer: a resource plus the namespace it
// lists from (the empty string meaning cluster-wide).
//
// It deliberately excludes the sink. Informers are the expensive, shared
// resource — one List of every Pod in a namespace costs the same whether one
// sink or five want it — so the pool is keyed by what actually talks to the API
// server, and per-sink fan-out happens afterwards through the interest map. It
// also excludes the selector: selectors are applied handler-side (see
// scopeInterest.matches), so a rule editing its selector never invalidates an
// informer.
//
// The GVR carries a concrete version because that is what the dynamic client
// needs; object *identity* stays version-agnostic (Invariant 7) and lives in
// identityKey instead.
type informerKey struct {
	GVR       schema.GroupVersionResource
	Namespace string
}

// String renders an informerKey for logs and error messages, with "cluster-wide"
// spelled out rather than rendered as an empty field an operator has to guess at.
func (k informerKey) String() string {
	if k.Namespace == "" {
		return k.GVR.String() + " (cluster-wide)"
	}
	return k.GVR.String() + " in namespace " + k.Namespace
}

// interestID is the identity of one entry in the interest map: "this sink wants
// what this informer sees".
//
// It is exactly plan.TargetKey re-expressed in data-plane terms (GVK resolved to
// GVR), and it is the granularity at which scopes start and stop: a target
// appearing here is a `Started` transition, its disappearance a `Stopped` one,
// regardless of how many rules contributed it.
type interestID struct {
	informer informerKey
	sink     string
}

// identityKey is the shape a pipeline.Key arrives in when the pipeline asks the
// watch cache what it holds: version-agnostic identity minus the object name,
// plus the sink.
//
// It exists because the pipeline's world is version-agnostic (Invariant 7:
// apps/v1 and apps/v2 Deployments are one object) while the pool's world is
// GVR-keyed. This is the index that crosses that gap without the pipeline ever
// learning what a GVR is.
type identityKey struct {
	Group     string
	Kind      string
	Namespace string
	Sink      string
}

// scopeInterest is one sink's interest in one informer's stream: which rules
// asked for it, which selectors narrow it, and which scope it settles into.
//
// Instances are immutable once installed in the table. A selector edit produces
// a *new* scopeInterest that replaces the old one under the same interestID —
// which is how a selector change takes effect on the next event without the
// informer noticing anything happened.
type scopeInterest struct {
	// informer is which pool entry serves this interest.
	informer informerKey

	// gvk is the kind rules named. It is the authority for the Kind on every
	// work key produced from this interest — never the object's own apiVersion —
	// so a tombstone with sparse metadata still keys identically to the live
	// object it replaced.
	gvk schema.GroupVersionKind

	// sink is the ClickHouseSink name every work key from this interest carries.
	sink string

	// scope is the (group, kind, namespace) triple the pipeline evicts and warms
	// by. It is derived from gvk and the informer's namespace, so it is
	// version-agnostic like the identity keys it prefixes.
	scope pipeline.ScopeKey

	// ruleKeys are the rules currently contributing this interest, sorted (the
	// registry guarantees the ordering). They are carried so a scope transition
	// is always attributable in logs and in Task 1.6's watch_scopes rows.
	ruleKeys []string

	// selectors are the parsed label selectors, a *union*: an object is in scope
	// if it matches any of them (see plan.TargetState.Selectors). Parsing happens
	// once, at pool-diff time, because the alternative is parsing a selector
	// string per object per event on the informer's notification path.
	selectors []labels.Selector

	// matchAll short-circuits the union when any contributing rule asked for
	// everything, which makes the overwhelmingly common case a single bool read.
	matchAll bool

	// redaction is the compiled union of every contributing rule's redaction
	// paths (Task 3.3), compiled once at pool-diff time for the same reason the
	// selectors are parsed here: the alternative is re-parsing a policy per
	// object per event on the pipeline's hot path. nil means no rule configured
	// anything, which still leaves the data plane's built-in scrubs in force.
	redaction *pipeline.RedactionPolicy
}

// newScopeInterest builds the interest for one snapshot target, parsing its
// merged selector set and compiling its merged redaction policy.
//
// A selector or redaction path that fails to parse is an anomaly rather than a
// user error — the registry canonicalized every selector through the same parser
// before storing it (see plan.Upsert), and the CRD's own validation rejects a
// malformed redaction path at admission — so it is reported rather than silently
// dropped, and the caller degrades that one target instead of the whole pass
// (Invariant 5). Degrading means the target streams nothing, which for a
// redaction failure is the only safe direction: the alternative would be
// streaming objects whose author asked for parts of them to be scrubbed.
func newScopeInterest(key plan.TargetKey, informer informerKey,
	selectors, redactions, ruleKeys []string) (*scopeInterest, error) {
	in := &scopeInterest{
		informer: informer,
		gvk:      key.GVK,
		sink:     key.Sink,
		scope: pipeline.ScopeKey{
			Group:     key.GVK.Group,
			Kind:      key.GVK.Kind,
			Namespace: key.Namespace,
		},
		ruleKeys: ruleKeys,
	}

	for _, raw := range selectors {
		if raw == "" {
			// "Select everything" makes every other selector in the union
			// redundant, so the parsed set is left empty on purpose.
			in.matchAll = true
			continue
		}
		sel, err := labels.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse label selector %q: %w", raw, err)
		}
		in.selectors = append(in.selectors, sel)
	}
	if len(in.selectors) == 0 {
		// Either a rule asked for everything, or the merged set was empty
		// (which the registry only produces for a target nobody selects on).
		in.matchAll = true
	}

	paths := redactionPaths(redactions)
	if len(paths) > 0 {
		policy, err := pipeline.CompileRedaction(paths)
		if err != nil {
			return nil, fmt.Errorf("compile redaction policy: %w", err)
		}
		in.redaction = policy
	}
	return in, nil
}

// redactionPaths flattens the per-rule redaction sets a target carries into one
// deduplicated, sorted path list — the union every contributing rule's policy
// adds up to (see plan.TargetState.Redactions).
//
// Empty entries are dropped rather than compiled: a rule that configured no
// redaction contributes the empty string, and it means "I add nothing", not "an
// empty path".
func redactionPaths(redactions []string) []string {
	var paths []string
	seen := make(map[string]struct{})
	for _, set := range redactions {
		for path := range strings.SplitSeq(set, "\n") {
			if path == "" {
				continue
			}
			if _, done := seen[path]; done {
				continue
			}
			seen[path] = struct{}{}
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return paths
}

// id returns this interest's identity in the table.
func (i *scopeInterest) id() interestID {
	return interestID{informer: i.informer, sink: i.sink}
}

// transition renders this interest as the scope edge the recorder consumes,
// stamped with the reconcile pass's instant.
func (i *scopeInterest) transition(at time.Time) ScopeTransition {
	return ScopeTransition{
		Sink:  i.sink,
		Scope: i.scope,
		// The informer identity is what makes two same-scope interests
		// distinguishable; it already renders uniquely per (GVR, namespace).
		Target:     i.informer.String(),
		APIVersion: i.gvk.Version,
		RuleKeys:   i.ruleKeys,
		At:         at,
	}
}

// identity returns the index key this interest answers pipeline lookups under.
func (i *scopeInterest) identity() identityKey {
	return identityKey{
		Group:     i.gvk.Group,
		Kind:      i.gvk.Kind,
		Namespace: i.informer.Namespace,
		Sink:      i.sink,
	}
}

// keyFor builds the pipeline work key for one object under this interest.
//
// It is the only place the watch package constructs a pipeline.Key, and it takes
// Group and Kind from the interest's GVK rather than from the object, so every
// event for an identity — live object, tombstone, or a delete reconstructed from
// a bare cache key — produces byte-identical keys and therefore lands on the
// same hashCache entry (Invariant 7).
func (i *scopeInterest) keyFor(namespace, name string) pipeline.Key {
	return pipeline.Key{
		Sink:      i.sink,
		Group:     i.gvk.Group,
		Kind:      i.gvk.Kind,
		Namespace: namespace,
		Name:      name,
	}
}

// matches reports whether an object carrying these labels is in this interest's
// scope.
func (i *scopeInterest) matches(objLabels map[string]string) bool {
	if i.matchAll {
		return true
	}
	set := labels.Set(objLabels)
	for _, sel := range i.selectors {
		if sel.Matches(set) {
			return true
		}
	}
	return false
}

// matchesEither reports whether an object is in scope either as it is now or as
// it was before an update.
//
// Both sides are consulted so that an object *leaving* a selector's scope (a
// label removed, a rule's selector narrowed) still produces one final work item.
// Without it the sink's last-known state for that object would freeze at its
// last matching value and silently drift from reality — the stale-state failure
// mode is worse than one extra row, and the row itself is truthful: it records
// the very change that took the object out of scope.
//
// previous is nil for Add and Delete, where there is only one side to consider.
func (i *scopeInterest) matchesEither(current, previous map[string]string) bool {
	if i.matches(current) {
		return true
	}
	return previous != nil && i.matches(previous)
}

// interestTable is the thread-safe interest map: the single structure both the
// informers' notification path and the pipeline's lookup path read, and the only
// place a watch scope is registered or deregistered.
//
// It holds one authoritative map (by interestID) plus two derived indexes, all
// rebuilt wholesale on every pool diff. Rebuilding rather than patching is
// deliberate: the manager level-triggers towards a snapshot, so "install exactly
// this state" is the operation it actually has, and computing the diff inside the
// same critical section as the swap is what makes "deregistered" and "these are
// the scopes to evict" impossible to disagree about.
type interestTable struct {
	// mu is an RWMutex because the write side runs at most once per pool diff
	// (seconds apart) while the read side runs on every informer event and every
	// pipeline work item.
	mu sync.RWMutex

	// current is the installed interest set, the basis the next replace diffs
	// against.
	current map[interestID]*scopeInterest

	// byInformer indexes current by the informer serving it, for event fan-out.
	// Each slice is sorted by sink so one event always enqueues its keys in the
	// same order — irrelevant to correctness (the workqueue is a set), but it
	// makes fan-out assertions in tests deterministic.
	byInformer map[informerKey][]*scopeInterest

	// byIdentity indexes current by the identity shape pipeline keys arrive in.
	// The value is a slice because two informers can legitimately serve the same
	// version-agnostic identity for one sink — apps/v1 and apps/v2 of the same
	// resource are two GVRs but one object (Invariant 7).
	byIdentity map[identityKey][]*scopeInterest
}

// newInterestTable returns an empty table. Nothing is watched until the first
// replace, which is the correct boot state: with no rules the operator streams
// nothing.
func newInterestTable() *interestTable {
	return &interestTable{
		current:    make(map[interestID]*scopeInterest),
		byInformer: make(map[informerKey][]*scopeInterest),
		byIdentity: make(map[identityKey][]*scopeInterest),
	}
}

// replace installs desired as the table's contents and returns the interests
// that vanished, sorted for deterministic logging.
//
// The swap and the diff happen under one write lock, so the moment replace
// returns, every returned interest is already invisible to both readers: an
// in-flight work item for a stopped scope sees scopeActive=false and drops
// rather than re-populating a cache entry the caller is about to evict, and no
// further events can fan out to it. That ordering — deregister, *then* evict — is
// what keeps "we stopped watching" from ever being recorded as "it was deleted".
//
// Interests present on both sides are simply overwritten, which is the selector-
// change path: same identity, new selectors, no transition and no informer churn.
func (t *interestTable) replace(desired map[interestID]*scopeInterest) (removed []*scopeInterest) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, in := range t.current {
		if _, wanted := desired[id]; !wanted {
			removed = append(removed, in)
		}
	}

	t.current = desired
	t.byInformer = make(map[informerKey][]*scopeInterest, len(desired))
	t.byIdentity = make(map[identityKey][]*scopeInterest, len(desired))
	for _, in := range desired {
		t.byInformer[in.informer] = append(t.byInformer[in.informer], in)
		t.byIdentity[in.identity()] = append(t.byIdentity[in.identity()], in)
	}
	for _, interests := range t.byInformer {
		slices.SortFunc(interests, func(a, b *scopeInterest) int { return cmp.Compare(a.sink, b.sink) })
	}

	sortInterests(removed)
	return removed
}

// interestsFor returns the interests an event from key must fan out to.
//
// The returned slice is the table's own backing array and must not be mutated;
// it is safe to read after the lock is dropped because replace never mutates a
// published slice, it installs freshly built ones. That is what lets the
// notification path — the hottest path in the process — take the read lock for
// the length of a map lookup only.
func (t *interestTable) interestsFor(key informerKey) []*scopeInterest {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byInformer[key]
}

// lookupIdentity returns the interests that could answer for ref's identity,
// most specific first: the interest watching ref's exact namespace, then any
// interest watching the kind cluster-wide.
//
// Both are consulted because a scope's namespace is a property of the *rule*, not
// of the object: a ClusterStreamRule with no namespace selector watches every
// namespace, and the objects arriving under it carry concrete namespaces that
// would never match an exact-namespace lookup. It mirrors the same two-step
// lookup the pipeline's warm-scope check performs, for the same reason.
//
// An empty result is the authoritative "this scope is not active for this sink"
// answer the pipeline drops on.
func (t *interestTable) lookupIdentity(ref pipeline.Key) []*scopeInterest {
	exact := identityKey{Group: ref.Group, Kind: ref.Kind, Namespace: ref.Namespace, Sink: ref.Sink}
	clusterWide := exact
	clusterWide.Namespace = ""

	t.mu.RLock()
	defer t.mu.RUnlock()

	interests := t.byIdentity[exact]
	if exact == clusterWide {
		// A cluster-scoped object's namespace is already "", so the two lookups
		// are the same one; returning it twice would make a caller iterating
		// candidates do redundant work.
		return interests
	}

	// Only a genuine two-sided answer needs a combined slice. One side is almost
	// always empty — a rule watches a named namespace or every namespace, rarely
	// both for the same kind and sink — and this runs once per work item, so
	// cloning unconditionally allocated on every lookup for a result identical to
	// one of the table's own slices (measured in Task 2.3). Handing that slice
	// back is safe for exactly the reason interestsFor's is: replace installs
	// freshly built slices and never mutates a published one, and callers must not
	// mutate what they receive.
	clusterWideInterests := t.byIdentity[clusterWide]
	if len(interests) == 0 {
		return clusterWideInterests
	}
	if len(clusterWideInterests) == 0 {
		return interests
	}
	return append(slices.Clone(interests), clusterWideInterests...)
}

// interestsForScope returns the interests installed for exactly one (sink, scope)
// triple.
//
// Unlike lookupIdentity there is no cluster-wide fallback, and that is the whole
// point: this answers questions *about a scope* (is it desired, have its informers
// synced) rather than about an object, and a scope pinned to one namespace is a
// different scope — with its own epoch — from the cluster-wide scope over the same
// kind. The slice usually holds one interest; it holds more when two rules name
// two versions of the same resource, which is two informers but one
// version-agnostic scope (Invariant 7).
//
// The returned slice is the table's own backing array and must not be mutated.
func (t *interestTable) interestsForScope(sinkName string, scope pipeline.ScopeKey) []*scopeInterest {
	key := identityKey{Group: scope.Group, Kind: scope.Kind, Namespace: scope.Namespace, Sink: sinkName}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byIdentity[key]
}

// size reports how many interests are installed. It exists for tests and for the
// manager's log lines; nothing about behaviour depends on it.
func (t *interestTable) size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.current)
}

// sortInterests orders interests by sink, then by scope, so log output and test
// expectations are stable across map-iteration orders.
func sortInterests(interests []*scopeInterest) {
	slices.SortFunc(interests, func(a, b *scopeInterest) int {
		return cmp.Or(
			cmp.Compare(a.sink, b.sink),
			cmp.Compare(a.scope.Group, b.scope.Group),
			cmp.Compare(a.scope.Kind, b.scope.Kind),
			cmp.Compare(a.scope.Namespace, b.scope.Namespace),
		)
	})
}
