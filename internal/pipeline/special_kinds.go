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

// This file is the pipeline's whole built-in special-casing surface: the kinds
// whose semantics differ from an ordinary watched resource, and the predicate
// every differing branch consults. Today that is exactly one family, Kubernetes
// Events, and the file exists so the branches elsewhere read as one named
// concept rather than as three unrelated `if kind == "Event"` checks.
//
// The behaviour is intrinsic to the kind and is deliberately **not** a CRD
// field. An Event is an Event whoever names it, so a knob would only ever let an
// author configure kuberecord into recording something untrue about Events —
// TTL expiries as deletions, most obviously. The rule's `resources` field
// comment (api/v1alpha1/shared_types.go) documents the behaviour for authors;
// docs/SCHEMA.md documents it for readers of the data.

const (
	// eventKind is the Kind both Kubernetes Event APIs use. It is matched
	// exactly: identity is case-sensitive everywhere else in the pipeline, and
	// the CRD's Kind pattern already rejects anything but upper-camel.
	eventKind = "Event"

	// coreGroup is the empty API group of the legacy `v1/Event`. Spelled as a
	// constant rather than a bare "" so the two accepted groups read as a pair at
	// the one place they are compared.
	coreGroup = ""

	// eventsGroup is the modern `events.k8s.io/v1/Event` API group. Both groups
	// are backed by the same storage, so a rule may name either and gets the same
	// stream — rendered under whichever api_group it asked for.
	eventsGroup = "events.k8s.io"
)

// ephemeralKind reports whether (group, kind) names a Kubernetes Event — the one
// kind kuberecord treats as append-only ephemera rather than as durable cluster
// state.
//
// Three pipeline behaviours hang off it, and each exists because the ordinary
// behaviour would record something false about an Event:
//
//   - No diffs. The API server *updates* an Event to bump `count`, so the
//     interesting row is the Event as it stood at that moment. A patch against a
//     baseline that will be TTL'd within the hour costs a reader a replay step to
//     recover something the row could simply have carried. Hash dedup still runs,
//     so a no-op resync writes nothing.
//   - No Deleted rows. An Event's disappearance is its ~1h TTL expiring, not a
//     change to the cluster. Recording it as a deletion would be the same class of
//     audit lie scope epochs exist to prevent, at roughly the volume of the Event
//     stream itself.
//   - No Snapshot tagging, and no zombie GC. Warm-up still seeds hashes (so a
//     restart does not re-emit every live Event), but nothing is reconciled *away*
//     from history: an Event that history knows and reality does not is an expired
//     Event, never a missed deletion. With no deletions to be ambiguous about, a
//     cache miss on an Event is unambiguously a new Event and is tagged Added.
//
// The version is deliberately not part of the check, exactly as for the Secret
// deny-list (D8, see internal/controller): in-process identity is version-agnostic
// (Invariant 7), so a future `v2` Event is the same ephemera as today's and must
// not slip into the durable-object path by virtue of its version alone.
func ephemeralKind(group, kind string) bool {
	if kind != eventKind {
		return false
	}
	return group == coreGroup || group == eventsGroup
}

// ephemeral reports whether this work item is for a Kubernetes Event. See
// ephemeralKind for what that changes.
func (k Key) ephemeral() bool {
	return ephemeralKind(k.Group, k.Kind)
}

// ephemeral reports whether this watch scope covers Kubernetes Events. It is the
// scope-level counterpart of Key.ephemeral, consulted by the warm coordinator,
// which reasons about whole scopes rather than about single objects.
func (s ScopeKey) ephemeral() bool {
	return ephemeralKind(s.Group, s.Kind)
}
