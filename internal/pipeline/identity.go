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

package pipeline

import "github.com/kuberecord/kuberecord/internal/sink"

// Key is one unit of work in the pipeline: "settle this object identity, for
// this sink." It is the workqueue's item type, so it must stay a comparable
// value type with no pointers or slices — the workqueue deduplicates pending
// items by equality, which is what collapses a burst of informer events for
// the same object into a single Process call. sink.ID is two plain strings, so
// embedding it by value keeps that property intact.
//
// The identity fields are exactly Invariant 7's in-process identity
// (api_group, kind, namespace, name) — deliberately **version-agnostic**, so
// apps/v1 and a hypothetical apps/v2 Deployment are one object and share one
// cache entry. Sink is added on top because the same object can stream to two
// sinks with wholly independent dedup/version state (see sinkState); it is
// part of the queue key so a per-sink retry never disturbs the other sink's
// delivery. cluster_id is deliberately absent: one operator process serves one
// cluster, so it is stamped onto the Record at write time, never threaded
// through in-memory keys.
type Key struct {
	// Sink is the typed identity of the sink this record is destined for — which
	// kind of backend and which CR of that kind — resolved to a live sink.Writer
	// at Process time via SinkRouter.
	//
	// The kind is carried rather than assumed because a name is only unique
	// within a kind: an S3Sink and a ClickHouseSink may both be named "default",
	// and a name-keyed work item would route to whichever of them the registry
	// happened to hold, settling its write against the *other* one's hashCache.
	Sink sink.ID
	// Group is the object's API group ("" for the core group).
	Group string
	// Kind is the object's kind.
	Kind string
	// Namespace is empty for cluster-scoped objects.
	Namespace string
	// Name is the object's name.
	Name string
}

// cacheKey builds the hashCache key for this identity. It is the single
// canonical identity-key builder in the codebase (Invariant 7); every consumer
// — Process, emitDelete, the close-out retry queue, and (from Task 1.6) the
// per-scope warm-up — routes through it, so a cache entry written under one
// code path is always found by the others, and no other call site concatenates
// a key by hand.
//
// Invariant 7 (verbatim): An object's identity is
// (cluster_id, api_group, kind, namespace, name) — version-agnostic (apps/v1
// and a hypothetical apps/v2 Deployment are the same object). Exactly one
// function in the codebase constructs this key. cluster_id is explicit in the
// schema but implicit in-process (one operator instance serves one cluster):
// in-memory cache/queue keys are (api_group, kind, namespace, name) — do not
// thread cluster_id through them.
//
// Hence the key embeds Group and Kind but never Version: batch/v1 Job and a
// CRD example.com/v1 Job are distinct resources and must not share a cache
// entry (the GVK-collision bug this builder fixes), while apps/v1 and apps/v2
// of the same resource must share one. Sink is *not* part of the key either:
// each sink owns its own hashCache instance (see sinkStateRegistry), so
// per-sink separation is structural rather than encoded in the string.
//
// The "|" delimiters keep Group, Kind, and the namespace/name pair
// unambiguous, and the namespace/name half always renders with its "/" even
// when Namespace is empty, so namespaced and cluster-scoped objects key
// identically ("|Node|/n1"). That shape is also what lets EvictScope match a
// whole watch scope by prefix (see scopeKeyPrefix).
func (k Key) cacheKey() string {
	return k.Group + "|" + k.Kind + "|" + k.Namespace + "/" + k.Name
}

// Scope returns the watch scope this key belongs to. It is the granularity at
// which watches start and stop (Task 1.4) and at which cache warm-up /
// Snapshot-tagging readiness is tracked (Task 1.6), so it drops Name while
// keeping the version-agnostic (group, kind) pair plus the namespace.
func (k Key) Scope() ScopeKey {
	return ScopeKey{Group: k.Group, Kind: k.Kind, Namespace: k.Namespace}
}

// String makes a Key readable in error messages and test failures. The sink
// renders as "<Kind>/<Name>" (see sink.ID.String), so a failure naming one of two
// same-named sinks says which backend it meant.
func (k Key) String() string {
	return k.Sink.String() + "/" + k.cacheKey()
}

// logValues returns this key's fields as logr key/value pairs, so every log
// line in the pipeline carries the same kind/namespace/name context
// (Invariant 4) without each call site re-listing them.
func (k Key) logValues() []any {
	return []any{"sink", k.Sink.String(), "group", k.Group, "kind", k.Kind,
		"namespace", k.Namespace, "name", k.Name}
}

// ScopeKey identifies a watch scope: one (version-agnostic) resource type,
// optionally narrowed to a single namespace. An empty Namespace means the
// scope spans every namespace, which is why warm-up lookups consult both the
// exact-namespace scope and the all-namespaces scope for a kind (see
// sinkState.scopeWarm).
//
// It intentionally mirrors — rather than reuses — plan.TargetKey: the plan
// registry keys targets by full GVK (a watch needs a concrete version to talk
// to the apiserver), while identity and cache state are version-agnostic.
// Task 1.4 translates between the two.
type ScopeKey struct {
	Group     string
	Kind      string
	Namespace string
}

// scopeKeyPrefix returns the cacheKey prefix shared by every object in this
// scope, so EvictScope can drop a stopped target's entries without keeping a
// second index. An all-namespaces scope yields "group|Kind|", which prefixes
// every namespace's entries for that kind; a namespaced scope yields
// "group|Kind|ns/", whose trailing "/" is what stops namespace "foo" from
// matching "foobar".
func (s ScopeKey) scopeKeyPrefix() string {
	prefix := s.Group + "|" + s.Kind + "|"
	if s.Namespace == "" {
		return prefix
	}
	return prefix + s.Namespace + "/"
}
