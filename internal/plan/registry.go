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

// Package plan holds the desired-state registry — the single seam between
// kubestream's control plane and its data plane.
//
// Reconcilers (Task 1.7) translate each validated StreamRule / ClusterStreamRule
// into a set of WatchTargets and write them here under the rule's key; the
// WatchManager (Task 1.4) reads Snapshot() and level-triggers reality towards
// it. Nothing else connects the two tiers, which is what keeps the data plane
// free of any notion of CRs and the control plane free of any notion of
// informers.
//
// The package is deliberately dependency-free beyond the standard library and
// k8s.io/apimachinery: it holds *the* shared mutable state of the operator, so
// it must stay trivially reviewable for correctness under concurrency and must
// never be able to reach a Kubernetes client, a sink, or a clock. It is pure
// bookkeeping — no goroutines, no I/O, no blocking.
package plan

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// WatchTarget is one rule's request for a single watch scope: "stream this
// Kind, from this namespace, matching this selector, into this sink".
//
// Every field is a string or a struct of strings so the whole value is
// comparable and can be used directly as a map key. That is what lets Upsert
// diff a rule's previous contribution against its new one with plain set
// arithmetic instead of an ordering-sensitive slice comparison — a rule that
// merely reorders its `resources` list must produce no churn in the data plane.
type WatchTarget struct {
	// Sink is the name of the ClickHouseSink this target's records are written
	// to (cluster-scoped, hence no namespace). It participates in target
	// identity because the same Kind in the same namespace streamed to two
	// different sinks is two independent streams with independent dedup
	// baselines.
	Sink string

	// GVK is the resource type to watch. The stored object identity is
	// version-agnostic (Invariant 7), but the version is retained here because
	// it is what the REST mapper and the dynamic client need to build a
	// concrete GVR, and it is what decides how the payload is rendered.
	GVK schema.GroupVersionKind

	// Namespace is the namespace to watch. The empty string means cluster-wide
	// — which only a ClusterStreamRule can ask for; a StreamRule always pins
	// this to its own namespace.
	Namespace string

	// Selector is a label selector in canonical form, i.e. the output of
	// labels.Selector.String(). The empty string means "every object in
	// scope". Callers may supply any parseable selector string; Upsert
	// canonicalizes it before storing, so `a=1,b=2` and `b=2,a=1` are one and
	// the same target and never look like a change to the data plane.
	Selector string
}

// TargetKey is the identity of a watch target as the data plane sees it.
//
// The selector is deliberately *not* part of the key. Informers are shared per
// (GVR, namespace) and selectors are applied by the event handler rather than
// the ListWatch (see WatchedResource.LabelSelector), so two rules that want the
// same Kind in the same namespace for the same sink must collapse onto one
// target whose selectors are merged — otherwise a selector edit would tear down
// and re-list a watch that did not actually need to change.
type TargetKey struct {
	Sink      string
	GVK       schema.GroupVersionKind
	Namespace string
}

// Key projects a WatchTarget onto the identity the data plane keys on, dropping
// the selector (which is merged into TargetState instead).
func (t WatchTarget) Key() TargetKey {
	return TargetKey{Sink: t.Sink, GVK: t.GVK, Namespace: t.Namespace}
}

// TargetState is the merged desire of every rule that wants a given target.
//
// It carries the contributing rule keys rather than a bare reference count so
// that a target's disappearance is always attributable: the WatchManager can
// log *which* rules stopped wanting a scope when it stops an informer, and
// Task 1.6's scope-epoch rows can name them. Both slices are sorted so
// snapshots are byte-comparable across calls, making "did the desired state
// actually change?" answerable without a set diff.
type TargetState struct {
	// Key repeats the map key so a TargetState remains self-describing once it
	// has been ranged out of the snapshot map.
	Key TargetKey

	// RuleKeys are the rules currently contributing this target, sorted. A
	// target lives exactly as long as this list is non-empty.
	RuleKeys []string

	// Selectors are the distinct canonical label selectors the contributing
	// rules asked for, sorted, deduplicated. They are a *union*: an object
	// belongs in this target's stream if it matches any of them, so the
	// presence of the empty selector ("match everything") makes the rest
	// redundant. Merging rather than intersecting is the only choice that
	// preserves per-rule intent when rules overlap.
	Selectors []string
}

// CanonicalSelector renders a metav1.LabelSelector as the canonical string form
// used in WatchTarget.Selector.
//
// It exists so reconcilers canonicalize with exactly the same code path the
// registry does, and so a malformed selector surfaces where it can be turned
// into a Degraded condition on the owning CR (Invariant 5) instead of deep in
// the data plane.
//
// A nil selector maps to the empty string — "select everything" — which matches
// WatchedResource.LabelSelector's documented semantics. Note this deliberately
// differs from metav1.LabelSelectorAsSelector, which maps nil to
// labels.Nothing(); an omitted optional field in a rule means the author did not
// want to narrow the scope, not that they wanted to silence it.
func CanonicalSelector(ls *metav1.LabelSelector) (string, error) {
	if ls == nil {
		return "", nil
	}
	sel, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return "", fmt.Errorf("canonicalize label selector: %w", err)
	}
	return sel.String(), nil
}

// canonicalizeSelectorString normalizes an already-stringified selector by
// round-tripping it through the parser, which sorts requirements by key (and
// set values within a requirement). Without this, two rules spelling the same
// intent in different orders would inflate a target's merged selector set and
// make every reorder look like a change.
func canonicalizeSelectorString(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	sel, err := labels.Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse label selector %q: %w", s, err)
	}
	return sel.String(), nil
}

// targetEntry is the registry's private ref-counting bookkeeping for one
// TargetKey.
//
// Both maps count rather than merely record membership because a single rule
// can contribute several WatchTargets to the same key — the same Kind and
// namespace under two different selectors. Plain sets would let the second
// contribution's removal drop a reference the first one still holds.
type targetEntry struct {
	// rules counts how many of a rule's targets land on this key, per rule.
	rules map[string]int
	// selectors counts how many contributions ask for each canonical selector,
	// across all rules.
	selectors map[string]int
}

// Registry is the thread-safe desired-state registry: rule keys in, merged
// watch targets out.
//
// A plain mutex guards both indexes because every mutation is a
// read-diff-write sequence that must be atomic as a whole; a sync.Map's
// per-key atomicity would not compose into that. Reads take the read side of
// an RWMutex: Snapshot is on the WatchManager's resync path and must never
// serialize behind other readers.
type Registry struct {
	mu sync.RWMutex

	// rules is each rule's canonicalized target set — the basis every Upsert
	// diffs against, so an Upsert only ever applies the delta.
	rules map[string]map[WatchTarget]struct{}

	// targets is the merged, ref-counted view the data plane consumes.
	targets map[TargetKey]*targetEntry

	// changes is the capacity-1 coalescing notification channel handed out by
	// Changes.
	changes chan struct{}
}

// New returns an empty Registry. The zero value is not usable: the maps and the
// notification channel must exist before the first concurrent reader appears.
func New() *Registry {
	return &Registry{
		rules:   make(map[string]map[WatchTarget]struct{}),
		targets: make(map[TargetKey]*targetEntry),
		changes: make(chan struct{}, 1),
	}
}

// Changes returns the registry's change notification channel.
//
// It is a capacity-1 channel written to with a non-blocking send, so
// notifications coalesce: a burst of a thousand Upserts leaves at most one
// pending wake-up, and a slow WatchManager can never exert back-pressure on a
// reconciler (Invariant 1). The channel therefore carries no information beyond
// "something changed since you last looked" — the reader must always respond by
// taking a fresh Snapshot and level-triggering towards it, never by trying to
// apply an edge.
//
// It is named for what it returns rather than for an action (`Watch`,
// `Subscribe`) because there is nothing to subscribe to: every caller shares
// this one channel, and a second consumer would steal the first's wake-ups.
// One WatchManager owns it.
func (r *Registry) Changes() <-chan struct{} {
	return r.changes
}

// Upsert records ruleKey's complete set of desired targets, replacing whatever
// that rule asked for previously. It is the only way intent enters the data
// plane.
//
// The call is all-or-nothing: every selector is canonicalized *before* the lock
// is taken, so a rule carrying one malformed selector leaves the registry
// exactly as it was and degrades only itself (Invariant 5) rather than landing
// half its targets. Passing an empty or nil slice is equivalent to Remove.
//
// Duplicate targets within one call collapse — the argument is a set, not a
// list — and a change notification fires only if the rule's canonical target
// set actually differs from its previous one, so a no-op reconcile (the common
// case, since controller-runtime resyncs periodically) does not wake the
// WatchManager.
func (r *Registry) Upsert(ruleKey string, targets []WatchTarget) error {
	desired := make(map[WatchTarget]struct{}, len(targets))
	for _, t := range targets {
		sel, err := canonicalizeSelectorString(t.Selector)
		if err != nil {
			return fmt.Errorf("rule %q: target %s in namespace %q: %w", ruleKey, t.GVK, t.Namespace, err)
		}
		t.Selector = sel
		desired[t] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.rules[ruleKey]
	changed := false
	for t := range desired {
		if _, held := current[t]; !held {
			r.addRefLocked(ruleKey, t)
			changed = true
		}
	}
	for t := range current {
		if _, wanted := desired[t]; !wanted {
			r.dropRefLocked(ruleKey, t)
			changed = true
		}
	}

	if len(desired) == 0 {
		delete(r.rules, ruleKey)
	} else {
		r.rules[ruleKey] = desired
	}

	if changed {
		r.notifyLocked()
	}
	return nil
}

// Remove withdraws every target ruleKey contributed, as happens when a rule is
// deleted or degrades. Targets other rules still want survive with a shorter
// RuleKeys list; targets nobody wants any more disappear, which is what the
// WatchManager turns into an informer shutdown and Task 1.6 turns into a
// `Stopped` scope row rather than a flood of `Deleted` rows.
//
// Removing a rule the registry never knew is a no-op and fires no notification,
// so a reconciler may call it unconditionally on a delete event.
func (r *Registry) Remove(ruleKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	current, known := r.rules[ruleKey]
	if !known {
		return
	}
	for t := range current {
		r.dropRefLocked(ruleKey, t)
	}
	delete(r.rules, ruleKey)
	r.notifyLocked()
}

// Snapshot returns the merged desired state as an independent deep copy.
//
// Deep copying is not a nicety: the WatchManager holds a snapshot for the whole
// duration of a reconcile pass while reconcilers keep writing, and any shared
// backing array would be a data race disguised as a value. The returned map,
// its TargetStates, and both of their slices are freshly allocated, so callers
// may sort, filter, or mutate them freely.
func (r *Registry) Snapshot() map[TargetKey]TargetState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[TargetKey]TargetState, len(r.targets))
	for key, entry := range r.targets {
		out[key] = TargetState{
			Key:       key,
			RuleKeys:  slices.Sorted(maps.Keys(entry.rules)),
			Selectors: slices.Sorted(maps.Keys(entry.selectors)),
		}
	}
	return out
}

// addRefLocked records one contribution of t by ruleKey. The caller must hold
// the write lock.
func (r *Registry) addRefLocked(ruleKey string, t WatchTarget) {
	key := t.Key()
	entry, ok := r.targets[key]
	if !ok {
		entry = &targetEntry{
			rules:     make(map[string]int),
			selectors: make(map[string]int),
		}
		r.targets[key] = entry
	}
	entry.rules[ruleKey]++
	entry.selectors[t.Selector]++
}

// dropRefLocked withdraws one contribution of t by ruleKey, deleting the target
// once no rule references it. The caller must hold the write lock.
func (r *Registry) dropRefLocked(ruleKey string, t WatchTarget) {
	key := t.Key()
	entry, ok := r.targets[key]
	if !ok {
		// Unreachable: a target is only ever dropped from the same per-rule set
		// that added it. Guarded rather than left to panic so that a future
		// refactor introducing an imbalance degrades into a stale target the
		// next Snapshot exposes, instead of taking the operator down
		// (Invariant 5).
		return
	}

	if entry.rules[ruleKey]--; entry.rules[ruleKey] <= 0 {
		delete(entry.rules, ruleKey)
	}
	if entry.selectors[t.Selector]--; entry.selectors[t.Selector] <= 0 {
		delete(entry.selectors, t.Selector)
	}
	if len(entry.rules) == 0 {
		delete(r.targets, key)
	}
}

// notifyLocked posts a coalesced wake-up. The send is non-blocking so a
// reconciler is never delayed by a busy or absent WatchManager; a full channel
// already means "you have unread news", which is all the signal conveys.
func (r *Registry) notifyLocked() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
}
