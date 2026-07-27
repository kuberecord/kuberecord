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

package sink

import (
	"context"
	"time"
)

// ScopeAction is what happened to a watch scope: it started being observed, or
// it stopped. It is a two-value enum rather than a free string because it is the
// discriminator an audit consumer keys off to tell "we stopped watching X" apart
// from "X was deleted" — the whole point of the scope-epoch design — and a typo
// in a string literal would silently corrupt that distinction.
type ScopeAction string

const (
	// ScopeActionStarted marks the instant a (sink, scope) pair gained its first
	// interested rule and an informer for it came up.
	ScopeActionStarted ScopeAction = "Started"
	// ScopeActionStopped marks the instant a (sink, scope) pair lost its last
	// interested rule. It is emphatically *not* a statement that the objects in
	// that scope were deleted; that is why it exists.
	ScopeActionStopped ScopeAction = "Stopped"
)

// ScopeEvent is one watch-scope epoch transition, destined for the sink's scope
// log (the watch_scopes table in ClickHouse).
//
// It is a first-class record type rather than a Record with a special
// event_type: a scope transition has no object identity, no content hash and no
// diff, and conflating it with resource_states rows is exactly the ambiguity
// that would let "we stopped watching" be read as "it was deleted".
//
// Exactly one event is written per transition of a (sink, scope) pair — never
// one per contributing rule and never one per informer. Multi-rule attribution
// lives in the owning CR's status; RuleRef here names only the rule that
// happened to trigger the edge.
type ScopeEvent struct {
	// Action is Started or Stopped.
	Action ScopeAction

	// Scope identifies the watch scope this transition is about.
	//
	// Note the *identity* reading of ScopeFilter applies here (see that type's
	// doc comment): an empty Namespace is the all-namespaces scope itself, not a
	// wildcard over namespaces.
	Scope ScopeFilter

	// APIVersion is the version of the resource the triggering watch target
	// named. It is provenance only — scope identity, like object identity, is
	// version-agnostic (Invariant 7), and one scope can be served by informers
	// on two versions of the same resource at once. It is carried because the
	// frozen schema has an api_version column for watch_scopes; a consumer must
	// not treat it as part of the scope's key.
	APIVersion string

	// RuleRef is the rule that triggered this transition, as the desired-state
	// registry's rule key: "<kind>/<namespace>/<name>", where kind is
	// "streamrule" or "clusterstreamrule" and a cluster-scoped rule renders an
	// empty namespace segment ("clusterstreamrule//platform-baseline"). The kind
	// is part of it because a StreamRule and a ClusterStreamRule may legitimately
	// share a name; see controller.RuleKey, the only function that builds one.
	//
	// It is empty for a Stopped event written by boot reconciliation, where the
	// scope's last epoch was left open by a previous process and the rule that
	// had held it is gone — there is genuinely no rule left to name, and
	// inventing one would be worse than an empty column.
	RuleRef string

	// TS is when the transition was observed, stamped once at that moment and
	// never re-stamped on retry. That immutability is what makes a delayed or
	// retried write still tell the truth about *when* the watch started or
	// stopped, rather than about when the sink happened to become reachable.
	TS time.Time
}

// ScopeEventWriter is the scope-log half of a sink: a bounded, asynchronous
// hand-off for watch-scope epoch transitions.
//
// It is deliberately separate from Writer. Writer's jobs are resource_states
// rows — high-volume, batched in the thousands, settled through the
// version-gated commit contract. Scope transitions are rare (one per rule
// lifecycle edge), have no cache state to settle, and must never queue behind a
// backlog of object rows, so the ClickHouse implementation gives them their own
// small batcher and retry queue rather than overloading the record path.
//
// Like Writer, it is optional for a future backend: a sink that cannot record
// scope epochs simply never receives them, and the operator's audit trail loses
// the epoch distinction for that sink alone.
type ScopeEventWriter interface {
	// EnqueueScopeEvent submits one transition without blocking the caller on
	// the sink round-trip. The returned error means the event was not accepted
	// (the sink is shutting down, or its queue stayed full) and the caller must
	// retry it — a scope epoch that is dropped silently is an audit hole, so no
	// implementation may swallow one.
	//
	// Callers must not invoke this from a watch-lifecycle path: it may block for
	// a bounded time waiting for queue room (Invariant 1). The scope recorder
	// therefore hands events to its own retry queue and calls this from its own
	// goroutine.
	EnqueueScopeEvent(ctx context.Context, event ScopeEvent) error
}
