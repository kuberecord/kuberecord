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

package query

import "time"

// Event types recorded in the event_type column, plus the one synthetic value
// this package adds.
//
// The enum is open: a reader branches on the values it knows and passes anything
// else through unchanged, so a value added to the schema later is rendered rather
// than dropped or coerced into a neighbour. They live here because the contract,
// the conformance suite and every backend need the identical spellings, and a
// literal repeated across three packages is a typo waiting to become a silently
// empty timeline.
const (
	// EventAdded is a first sighting of an object, or a reincarnation superseding
	// a prior one. Carries full data.
	EventAdded = "Added"
	// EventModified is a later observation whose content differs from the last
	// recorded state. Carries a patch, or full data when a patch could not be
	// produced.
	EventModified = "Modified"
	// EventDeleted is the object being gone. Carries no data, no patch and no
	// hash, which is why an engine that cannot record these rows leaves a silence
	// a reader must be warned about rather than a gap a reader can see.
	EventDeleted = "Deleted"
	// EventSnapshot is a first sighting recorded before the recorder's cache had
	// been warmed from history, so it cannot claim the object is genuinely new.
	// Carries full data and serves as a reconstruction base exactly as
	// EventAdded does.
	EventSnapshot = "Snapshot"
	// EventCheckpoint is a modification that also carries the full state, so a
	// replay never has to walk back past it. Its own patch describes the
	// transition its data already reflects and must not be applied on top of it.
	EventCheckpoint = "Checkpoint"

	// EventKubernetes marks a Kubernetes Event correlated into another object's
	// timeline by TimelineQuery.IncludeEvents.
	//
	// It is synthetic: it never appears in the event_type column, because an
	// ingested Event object is recorded as an ordinary object in its own right.
	// The engine stamps it on merge so that a caller can tell a row about the
	// object apart from a row about something that happened to it — a distinction
	// Change cannot otherwise carry, since every other field of a merged row
	// describes the Event object rather than the target.
	EventKubernetes = "Event"
)

// ObjectRef names one object by its canonical identity.
//
// The identity is (cluster_id, api_group, kind, namespace, name) and it is
// deliberately **version-agnostic** (Invariant 7): a resource observed at two API
// versions is one object with one history, so no version appears here. The
// version each individual observation was made at is provenance and travels on
// Change.APIVersion instead.
//
// Two consequences follow, and both matter to a caller. APIGroup carries the core
// group as the empty string, which is a value and not a wildcard — a query for
// the core group must not match every group. And an ObjectRef is not unique over
// time: a (namespace, name) pair may be reused by several incarnations with
// different UIDs, which is why UID is not a field here and why anything rendering
// a single history has to say which incarnation it chose.
type ObjectRef struct {
	ClusterID string `json:"cluster_id"`
	APIGroup  string `json:"api_group"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// Change is one recorded state transition of one object, as read back.
//
// Its JSON field names mirror the frozen schema's column names exactly, which is
// the point of the type rather than a detail of it: people script against
// structured output, and mirroring the columns means a recipe written against a
// SQL result transfers to command-line output without being rewritten. Renaming a
// field here is a breaking change to a public contract even though nothing in Go
// would complain.
//
// Populated fields depend on EventType — a deletion carries no data, patch, hash
// or actors; a modification carries a patch or, having failed to produce one, full
// data. A reader must therefore branch on EventType rather than infer meaning from
// emptiness.
type Change struct {
	// TS is when the transition was observed: event time, not ingestion time, at
	// nanosecond precision. Two changes a microsecond apart are two changes and
	// must not be reordered or collapsed.
	TS time.Time `json:"ts"`
	// EventType is one of the values above. Treat it as an open enum.
	EventType string `json:"event_type"`
	// Actors are the distinct, sorted field managers seen on the object — the
	// cheapest available answer to "who did this". Always empty on a deletion:
	// there is no live object left to attribute one to, and an empty list is the
	// honest answer rather than a missing one.
	Actors []string `json:"actors"`
	// UID identifies the incarnation this change belongs to. It is what separates
	// an update from a delete-and-recreate under the same name.
	UID string `json:"uid"`
	// ResourceVersion is the object's resourceVersion at observation.
	ResourceVersion string `json:"resource_version"`
	// APIVersion is the version this observation was made at. Provenance only —
	// see ObjectRef on why it is not part of identity.
	APIVersion string `json:"api_version"`
	// Data is the full normalized JSON of the object, present on full-state rows.
	Data string `json:"data"`
	// Diff is an RFC 6902 JSON Patch against the previous state, present on
	// modifications and on checkpoints. On a checkpoint it describes the
	// transition Data already reflects and must not be re-applied over it.
	Diff string `json:"diff"`
	// SHA256 is the hex digest of the normalized JSON, and the means of verifying
	// that a reconstruction produced the state that was actually recorded.
	SHA256 string `json:"sha256"`
	// Labels are the object's labels at observation.
	Labels map[string]string `json:"labels"`
}

// ChangeIterator is a streaming cursor over changes.
//
// It is a cursor rather than a slice because the result set is unbounded in
// principle: an object caught in a reconcile loop can produce a hundred thousand
// changes in a day, and a contract that returned a slice would require every
// backend to hold all of them in memory to answer a question about the last
// twenty of them.
//
// The usage shape is the one the standard library established, and the error check
// after the loop is not optional:
//
//	it, err := engine.Timeline(ctx, q)
//	if err != nil {
//	        return err
//	}
//	defer func() { _ = it.Close() }()
//	for it.Next() {
//	        render(it.Change())
//	}
//	return it.Err()
//
// Skipping Err turns a backend that failed halfway into a result that looks
// complete and merely short, which for an audit timeline is the worst available
// outcome (Invariant 4).
//
// An iterator is not safe for concurrent use.
type ChangeIterator interface {
	// Next advances to the next change and reports whether one is available. It
	// returns false at the end of the result set and also on failure; Err
	// distinguishes them.
	Next() bool

	// Change returns the change Next just advanced to. It is only valid after
	// Next has returned true.
	//
	// The returned value and everything reachable from it — Actors, Labels — are
	// the caller's to keep. An implementation must not hand out backing arrays or
	// maps it intends to overwrite on the next Next: a caller that appends rows to
	// a slice is doing an ordinary thing, and a recycled buffer would turn that
	// into a result where every row holds the last row's labels.
	Change() Change

	// Err returns the error that ended the iteration, or nil if the result set was
	// exhausted normally. It is valid only once Next has returned false.
	//
	// A backend failure mid-stream must surface here. Truncating silently would
	// present a partial history as a whole one.
	Err() error

	// Close releases the iterator's resources — driver rows, open readers, the
	// goroutines feeding a merge. It is safe to call at any point, including
	// before the result set is exhausted, which is the normal path whenever a
	// limit is satisfied or a caller breaks out early. Calling it more than once
	// is safe.
	Close() error
}

// TimelineQuery asks for the recorded changes of one object.
type TimelineQuery struct {
	// Ref is the object whose history is wanted.
	Ref ObjectRef

	// From and To bound the window, inclusive. A zero value means unbounded on
	// that side — which an engine declaring Capabilities.TimeBoundRequired
	// refuses with ErrTimeBoundRequired rather than accepting and then scanning
	// everything it has.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	// UID restricts the result to one incarnation. Empty means the newest
	// incarnation in the window, which is the default because it is the one an
	// engineer investigating right now almost always means.
	UID string `json:"uid"`

	// AllIncarnations returns every incarnation in the window instead of only the
	// newest. The changes remain in ts order across incarnations, so a reader must
	// still key on Change.UID to tell them apart: interleaving is a rendering
	// decision and this flag does not make it safe to ignore identity
	// (Invariant 7). It is ignored when UID is set.
	AllIncarnations bool `json:"all_incarnations"`

	// Actors keeps only changes with at least one actor in this list; empty means
	// no actor restriction.
	//
	// Note what this necessarily does to deletions: a deletion records no actors,
	// so any non-empty Actors filter excludes every deletion. That is arithmetic
	// rather than policy, but it is surprising enough that a caller applying an
	// actor filter should say so in its output rather than let the deletion vanish
	// unremarked.
	Actors []string `json:"actors"`

	// ExcludeActors drops changes with at least one actor in this list. It is
	// applied after Actors and wins on conflict, so an actor named in both is
	// excluded — the narrower, safer reading when a caller has contradicted
	// itself.
	ExcludeActors []string `json:"exclude_actors"`

	// FieldPaths keeps only changes touching at least one of these paths; empty
	// means no field restriction.
	//
	// The syntax is a dotted prefix — "spec.replicas",
	// "spec.template.spec.containers" — matched against each patch operation's
	// path after conversion from JSON Pointer, so "spec.replicas" also matches
	// "spec.replicas[0]" and anything else beneath it. Dotted rather than raw
	// JSON Pointer because that is the grammar redaction policies already use and
	// the form the renderer already displays; asking a reader to hold two path
	// languages in mind for one object is a cost with no return.
	//
	// Rows carrying no patch — a first sighting, a snapshot, a deletion — are kept
	// regardless. They are the boundaries of the object's existence, and a
	// filtered timeline that dropped them would show a history with no beginning
	// and no end and imply the object had neither.
	FieldPaths []string `json:"field_paths"`

	// Limit caps the number of changes emitted; zero means no cap.
	//
	// It takes the *first* Limit changes in the emission order, which is the order
	// Reverse selects. So Reverse false with Limit 100 yields the hundred oldest
	// changes in the window, and a caller wanting the hundred newest sets Reverse.
	// This is stated bluntly because the other reading is tempting and wrong: the
	// backend's own ordering is what makes a limited query cheap, and taking from
	// the far end would mean reading the whole window and sorting it in memory —
	// the exact cost a limit exists to avoid.
	Limit int `json:"limit"`

	// Reverse emits newest first instead of oldest first.
	Reverse bool `json:"reverse"`

	// IncludeEvents merges Kubernetes Events naming this object into the stream in
	// ts order. Merged changes carry EventKubernetes as their EventType and the
	// Event object's own data; every other field describes the Event, not the
	// target.
	//
	// Both group spellings of Event are correlated. Handling only one of them
	// would drop whichever half of the cluster's events happens to be reported the
	// other way, which is a silent hole rather than a visible gap.
	IncludeEvents bool `json:"include_events"`
}

// ScopeQuery asks which watch scopes were active, and when.
//
// It is the input to the answer that makes an empty timeline explicable, so its
// matching rules are written for the question "was anything watching this?"
// rather than for the shape of the underlying scope log.
type ScopeQuery struct {
	// ClusterID is the cluster whose scope log to read.
	ClusterID string `json:"cluster_id"`

	// APIGroup restricts to one group; empty means every group.
	//
	// This is the one place the core group cannot be spelled, since it is itself
	// the empty string. A caller needing the core group alone filters the result,
	// and the ambiguity is accepted here because a scope query is a small,
	// human-facing listing rather than a hot path.
	APIGroup string `json:"api_group"`

	// Kind restricts to one kind; empty means every kind.
	Kind string `json:"kind"`

	// Namespace has the *covering* reading, which is not the reading the scope log
	// itself uses. A non-empty namespace matches both that namespace's own scope
	// and the all-namespaces scope, because a cluster-wide rule genuinely was
	// watching an object in that namespace and reporting otherwise would answer
	// "never observed" about an object that was observed the whole time. An empty
	// namespace matches every scope for the kind.
	//
	// The consequence is that Coverage may return an interval whose Namespace is
	// empty in reply to a query for a specific one. That is not a bug to be
	// normalized away: the interval is reporting the scope that actually covered
	// the object, and a renderer should show it as such.
	Namespace string `json:"namespace"`

	// From and To bound the window, inclusive. An interval that merely overlaps
	// the window is returned, clipped to nothing — the caller is told when the
	// scope really opened and closed, since a coverage answer trimmed to the
	// question would make a scope opened last year look as though it opened when
	// the window did. A zero value means unbounded on that side.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// ScopeInterval is one period during which a scope was being watched.
//
// A scope's history is a sequence of these, and their edges are the difference
// between "this object was not modified" and "nobody was looking". The end of an
// interval says the recorder stopped watching; it emphatically does not say the
// objects in it were deleted, and anything rendering one has to keep those apart.
type ScopeInterval struct {
	// APIGroup is the group watched; empty is the core group.
	APIGroup string `json:"api_group"`
	// Kind is the kind watched.
	Kind string `json:"kind"`
	// Namespace is the namespace watched, with the scope log's own reading rather
	// than ScopeQuery's: empty is the all-namespaces scope itself, not a wildcard.
	Namespace string `json:"namespace"`
	// RuleRef names the rule that opened or closed this interval. It can be empty
	// when the interval was closed by a recovery pass whose rule no longer exists,
	// which is a real state and better reported as blank than invented.
	RuleRef string `json:"rule_ref"`
	// From is when the scope started being watched.
	From time.Time `json:"from"`
	// To is when it stopped. A nil To means the interval is still open: the scope
	// is being watched now. It is a pointer precisely so that "still open" cannot
	// be confused with a zero timestamp, which is what a plain time.Time would
	// have forced a reader to guess at.
	To *time.Time `json:"to"`
}

// Incarnation is one UID's span under a single (namespace, name).
//
// It exists so a caller can find out how many objects have worn a name before
// rendering the history of one of them. Kubernetes reuses names freely, and a
// timeline that spliced a deleted Deployment's history onto its replacement's
// would be a coherent-looking account of something that never happened
// (Invariant 7).
type Incarnation struct {
	// UID is the incarnation's Kubernetes UID.
	UID string `json:"uid"`
	// FirstSeen is the timestamp of its earliest recorded change.
	FirstSeen time.Time `json:"first_seen"`
	// LastSeen is the timestamp of its most recent recorded change.
	LastSeen time.Time `json:"last_seen"`
	// Deleted reports whether a deletion was recorded for this incarnation.
	//
	// False does not mean the object still exists. It means no deletion is in the
	// history — which, on a backend that cannot record deletions at all
	// (Capabilities.Deletions), it never will be. A renderer must qualify this
	// field by that capability rather than present it as a fact about the cluster.
	Deleted bool `json:"deleted"`
}

// Reconstruction is an object's state at an instant, rebuilt from history,
// together with the evidence for how it was rebuilt.
//
// The provenance fields are not diagnostics. A reconstruction is an assertion
// about the past that somebody may act on, and BaseTS, BaseEvent and
// PatchesApplied are what let a reader judge it: a state assembled from a base an
// hour old and two patches invites more confidence than one assembled from a base
// three months old and four hundred.
type Reconstruction struct {
	// Object is the reconstructed state, decoded. It is the recorded state, which
	// is not the same as the object the API server held: volatile metadata was
	// stripped before recording and any redaction policy in force replaced values
	// with its sentinel. Anything presenting this to a user must say so — it is
	// not a manifest.
	Object map[string]any `json:"object"`
	// BaseTS is the timestamp of the full-state row the replay started from.
	BaseTS time.Time `json:"base_ts"`
	// BaseEvent is that row's event type — one of EventAdded, EventSnapshot,
	// EventCheckpoint, or a modification that fell back to full state.
	BaseEvent string `json:"base_event"`
	// PatchesApplied is how many patches were replayed over the base. Zero means
	// the base row was itself the answer.
	PatchesApplied int `json:"patches_applied"`
	// SHA256 is the digest recorded for the last row consumed. Hashing the
	// canonicalized Object must reproduce it; a mismatch means the history and the
	// replay disagree, which is a chain-of-custody finding and not a rounding
	// error.
	SHA256 string `json:"sha256"`
}
