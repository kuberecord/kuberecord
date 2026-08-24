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

// Job is a single record submitted to a Writer, together with the callback that
// settles its outcome.
//
// commit is invoked by one of the Writer's own workers exactly once, only after
// the write has been durably confirmed or definitively abandoned after retries —
// it is the sole place cache mutation for that job's object is allowed to happen,
// so a failed write can never be mistaken for a persisted one.
//
// (The guarantee above is the original writeJob contract, generalised only in
// whose worker fires it: every Writer implementation must uphold it, whatever its
// backend.)
type Job struct {
	// Record is the row to persist.
	Record Record
	// Commit reports this job's settled outcome (true = durably written,
	// false = abandoned after retries). See the exactly-once contract above.
	Commit func(ok bool)
}

// Writer is the write half of a sink, and the only half a backend is obliged to
// implement: a bounded, asynchronous hand-off that decouples record persistence
// from the caller's hot path. StateReader, ScopeEventWriter and Prober are all
// optional, discovered by type assertion at registration (see Factory).
//
// A compliant implementation need not be a database. Two ship today: ClickHouse
// (internal/sink/clickhouse), which inserts batched rows, and S3
// (internal/sink/s3), which accumulates records into a compressed object and PUTs
// it on rotation. What makes both of them Writers is this interface's contract —
// never blocking the caller, and settling every job exactly once — rather than
// anything they share about storage or a query language.
type Writer interface {
	// Start runs the Writer's worker pool until ctx is cancelled, then shuts
	// down in a strict order so no write is ever stranded or raced against
	// connection closure:
	//  1. Stop accepting new Enqueue calls (under mu, so this can't race a send).
	//  2. Swap in a fresh, shutdownDrainTimeout-bounded drainCtx for any job
	//     processed from here on — see attemptContext for why the original ctx
	//     (already cancelled by this point) can't be reused for these attempts.
	//  3. Wait for any Enqueue call already past the closing check to finish
	//     sending (or bail via its own ctx/timeout) — after this, jobs can
	//     receive no further sends from anyone.
	//  4. Close jobs. Workers range over it, so they drain every already-queued
	//     job and then exit cleanly once it's both empty and closed — no worker
	//     can exit "too early" and leave a job stranded.
	//  5. Wait for otherUsers (if set) — other goroutines sharing conn.
	//  6. Close conn — guaranteed safe now, since nothing can still be using it.
	//
	// It is manager.Runnable-compatible so the manager owns its lifecycle.
	Start(ctx context.Context) error

	// Enqueue submits a write job without blocking the caller on the actual
	// sink round-trip. It is a bounded, metered hand-off: if the queue is full
	// it waits a bounded time for room and then returns an error rather than
	// dropping the job silently or blocking the hot path indefinitely. The
	// returned error should be propagated so the caller's own backpressure
	// (e.g. controller-runtime's requeue/backoff) takes over.
	Enqueue(ctx context.Context, job Job) error
}

// ScopeFilter names a single watch scope. ClusterID is explicit here because the
// sink is a multi-cluster store even though a single operator process only ever
// serves one cluster (Invariant 7).
//
// Namespace carries two readings, and every consumer documents which one it
// uses:
//
//   - As a *record query* (LastKnownStates), an empty Namespace matches every
//     namespace: the caller wants the objects a GVK-wide scope covers, which
//     live under concrete namespaces.
//   - As a *scope identity* (ScopeEvent.Scope, ScopeWasActive, ActiveScopes), an
//     empty Namespace is the all-namespaces scope itself and is matched exactly.
//     A cluster-wide rule and a rule pinned to one namespace are two different
//     scopes with two independent epochs, so a wildcard reading here would let
//     one scope's history answer for another's.
//
// The two readings coexist on one type because the caller always knows which
// question it is asking, and splitting them would mean converting between two
// structurally identical structs at every call site.
type ScopeFilter struct {
	ClusterID string
	APIGroup  string
	Kind      string
	Namespace string
}

// KnownState is the last-known persisted state of **one incarnation** — one
// (identity, UID) pair — as returned by StateReader. It is deliberately not "one
// object": an identity whose death went unrecorded (a delete-and-recreate that
// happened while the operator was down) yields one KnownState per incarnation,
// and that multiplicity is the only evidence that the older one was never closed
// out. See LastKnownStates.
//
// TS and APIVersion exist so a close-out record for an unclosed incarnation is
// fully derivable from history: dating it from TS rather than from now keeps a
// reconstruction in the order events actually happened, and makes a re-emitted
// close-out byte-identical to the first attempt (and therefore collapsible by
// resource_states' ReplacingMergeTree).
type KnownState struct {
	Namespace string
	Name      string
	UID       string
	SHA256    string

	// APIVersion is the api_version last recorded for this incarnation. Identity
	// is version-agnostic (Invariant 7), so this is provenance carried forward
	// rather than part of the key.
	APIVersion string

	// TS is the most recent timestamp recorded for this incarnation. Within one
	// identity, the incarnation with the greatest TS is the current one; every
	// other is a prior whose death nobody recorded.
	TS time.Time
}

// StateReader is the read half of a sink: it reports, per scope, the last-known
// state of every object not currently tombstoned, so a restarting operator can
// warm its dedup cache from durable history rather than re-emitting every live
// object as a duplicate.
//
// It also answers the two scope-epoch questions the warm/GC coordinator asks of
// history rather than of an object: was this scope watched in a previous epoch,
// and which scopes did a previous process leave open?
//
// StateReader is optional: a Writer that cannot read its own history back omits
// it, which is what the S3 archive tier does (D12). Such a Writer-only sink runs
// with cache warm-up, zombie garbage-collection *and* boot reconciliation of
// scope epochs disabled, and tags every record as a permanent Snapshot (it can
// never prove an object is genuinely new versus merely un-warmed).
//
// That omission is detected once, at registration, by SinkManager.newLiveSink,
// and is reported rather than inferred — which is the whole point, because a
// Writer-only sink's degradation is invisible in its own output. An archive with
// no deletions in it looks exactly like an archive of a cluster where nothing was
// deleted. Three places state it instead:
//
//   - CapabilitiesFor reports it to the control plane, which is how an S3Sink CR
//     comes to carry HistoryUnavailable=True (with Ready still True — a declared
//     capability limit is not a fault) and how the rules bound to that sink come
//     to carry the same condition.
//   - The warm/GC coordinator logs it once per sink at Info and then skips all
//     three behaviours silently, because for such a sink they are expected rather
//     than anomalous.
//   - kuberecord_safe_mode stays pinned at 1 for every scope on the sink, since
//     no scope is ever marked warm. That gauge is the observable signal; there is
//     deliberately no second metric saying the same thing.
type StateReader interface {
	// LastKnownStates returns the last-known state of every *incarnation*
	// matching filter whose own most recent event is not a deletion. A transient
	// backend error is returned as-is so the caller can retry; a partial read
	// must be reported as an error, never as a short-but-successful result.
	//
	// An ordinary object yields exactly one KnownState. Two or more for the same
	// (Namespace, Name) mean an incarnation died without a Deleted row ever being
	// written for it — the operator was down across a delete-and-recreate — and
	// the warm-up closes those priors out from history (see KnownState).
	//
	// filter.Namespace has the *record query* reading: empty matches every
	// namespace (see ScopeFilter).
	LastKnownStates(ctx context.Context, filter ScopeFilter) ([]KnownState, error)

	// ScopeWasActive reports whether filter's most recent recorded scope action
	// strictly before asOf is Started — i.e. whether a *previous* epoch of this
	// scope was left open.
	//
	// It is the guard that keeps zombie GC honest. An object present in
	// LastKnownStates but absent from the live watch cache is only a genuine
	// deletion if this scope was already being watched before the current epoch
	// began: then the object disappeared while the operator was down, and one
	// Deleted row is the truth. If the scope's last action was Stopped, the trail
	// already says "we stopped watching here" and the objects' last-known states
	// are correctly dated to that Stopped row — re-dating their disappearance to
	// now would be a lie. If the scope has no history at all, those states came
	// from some other scope's epoch and are simply pre-history.
	//
	// asOf is what makes the answer race-free: the caller passes the instant its
	// own epoch began, so the current epoch's own Started row (which may land at
	// any moment, since scope events are written asynchronously) can never be
	// mistaken for a previous one. A zero asOf means "as of now".
	//
	// filter.Namespace has the *scope identity* reading: it is matched exactly,
	// including the empty (all-namespaces) scope.
	ScopeWasActive(ctx context.Context, filter ScopeFilter, asOf time.Time) (bool, error)

	// ActiveScopes returns every scope in clusterID whose most recent recorded
	// action is Started — the scopes some process left open.
	//
	// Boot reconciliation needs the enumeration, not a per-scope probe: a rule
	// deleted while the operator was down leaves an open scope that nothing in
	// the desired state mentions any more, so there is no list of candidates to
	// probe. Each returned filter carries the scope-identity reading of
	// Namespace and is directly usable as a ScopeEvent.Scope.
	ActiveScopes(ctx context.Context, clusterID string) ([]ScopeFilter, error)
}
