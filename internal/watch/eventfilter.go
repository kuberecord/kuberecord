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

// This file is the handler-side half of Event filtering (Task 19.2): the one
// place that knows how a rule's `eventFilter` is spelled on the wire, how it is
// canonicalized, and how it is evaluated against an Event.
//
// Handler-side for the reason label selectors are (see
// v1alpha1.WatchedResource.LabelSelector and scopeInterest.matches): one informer
// per (GVR, namespace) is shared by every rule that wants that stream, so a
// filter belongs to an *interest* rather than to the informer's ListWatch.
//
// It is also the *server-side* half (Task 19.3): deriveEventFieldSelector renders
// what a Kubernetes field selector can express of a compiled filter, and the pool
// hands that to the List and the Watch. The two halves live in one file because
// they must not drift: the derived selector is only ever a narrowing the matcher
// below re-checks, so push-down stays a performance decision and never becomes a
// content decision (D49). Nothing downstream may skip the handler-side evaluation
// on the strength of a selector having been pushed.

import (
	"encoding/json"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// EventFilterSpec is one rule's Event filter in the shape the data plane stores
// and evaluates: six lists of plain strings, and nothing that knows a custom
// resource exists.
//
// It mirrors v1alpha1.EventFilter field for field and JSON tag for JSON tag, and
// is deliberately a second declaration rather than that type re-used.
// internal/watch, internal/plan and internal/pipeline import no API types at all
// — the data plane level-triggers towards a registry of strings and has no notion
// of a CR — and the one conversion lives in the control plane, beside the
// redaction policy's (controller.canonicalEventFilter, whose companion test
// fails if the two declarations ever stop agreeing).
//
// The semantics are v1alpha1.EventFilter's, restated because this is the
// declaration the matcher below is written against: within a list OR, across
// fields AND, an absent or empty list no constraint at all.
type EventFilterSpec struct {
	// Types are the Event types to keep — `Normal`, `Warning`.
	Types []string `json:"types,omitempty"`

	// Reasons are the Event reasons to keep. Mutually exclusive with
	// ExcludeReasons, which the CRD enforces at admission; nothing here depends
	// on that, and a filter carrying both would simply apply both.
	Reasons []string `json:"reasons,omitempty"`

	// ExcludeReasons are the Event reasons to drop, keeping everything else.
	ExcludeReasons []string `json:"excludeReasons,omitempty"`

	// SourceComponents are the emitting components to keep, matched against
	// every spelling the two Event APIs give that fact (see componentMatches).
	SourceComponents []string `json:"sourceComponents,omitempty"`

	// SubjectKinds are the Kinds of the object an Event is about.
	SubjectKinds []string `json:"subjectKinds,omitempty"`

	// SubjectNames are the exact `metadata.name`s of the object an Event is
	// about.
	SubjectNames []string `json:"subjectNames,omitempty"`
}

// CanonicalEventFilter renders spec as the canonical string a plan.WatchTarget
// carries: compact JSON with every list sorted, deduplicated and stripped of
// empty entries — or the empty string when the filter constrains nothing.
//
// Canonicalizing in the control plane, rather than in the registry or at
// evaluation time, is what makes the registry's ref-counting collapse two rules
// that express the same filter onto one entry: the bytes are identical, so the
// target counts one filter rather than two, and a rule that merely reorders or
// repeats its `reasons` list produces no churn in the data plane at all. It is
// the discipline CanonicalSelector and canonicalRedaction already apply to the
// other two opaque fields a WatchTarget carries.
//
// The empty filter renders as the empty string rather than as `{}`, so it is the
// same "no constraint" sentinel WatchTarget.Selector already uses — and one
// contributing rule without an eventFilter therefore widens the merged set to
// every Event, exactly as one rule without a label selector widens the merged
// selector set to every object.
func CanonicalEventFilter(spec EventFilterSpec) (string, error) {
	canonical := EventFilterSpec{
		Types:            canonicalEventFilterList(spec.Types),
		Reasons:          canonicalEventFilterList(spec.Reasons),
		ExcludeReasons:   canonicalEventFilterList(spec.ExcludeReasons),
		SourceComponents: canonicalEventFilterList(spec.SourceComponents),
		SubjectKinds:     canonicalEventFilterList(spec.SubjectKinds),
		SubjectNames:     canonicalEventFilterList(spec.SubjectNames),
	}
	if canonical.constrainsNothing() {
		return "", nil
	}

	encoded, err := json.Marshal(canonical)
	if err != nil {
		// Unreachable: every field is a []string. Reported rather than swallowed
		// because the alternative — falling back to the empty string — would
		// record the whole stream its author asked to narrow, silently
		// (Invariant 4).
		return "", fmt.Errorf("encode event filter: %w", err)
	}
	return string(encoded), nil
}

// constrainsNothing reports whether this filter narrows the Event stream at all.
// Every axis empty is what an author who set no eventFilter asked for, and it is
// also what one who set an eventFilter containing only empty lists asked for.
func (s EventFilterSpec) constrainsNothing() bool {
	return len(s.Types) == 0 && len(s.Reasons) == 0 && len(s.ExcludeReasons) == 0 &&
		len(s.SourceComponents) == 0 && len(s.SubjectKinds) == 0 && len(s.SubjectNames) == 0
}

// canonicalEventFilterList sorts, deduplicates and drops the empty entries from
// one axis of a filter, returning nil for an axis that ends up constraining
// nothing.
//
// It never mutates its argument: the slice it is handed belongs to the CR the
// reconciler is holding, and sorting that in place would reorder the object a
// later status write patches.
//
// An empty entry is dropped rather than compiled. CRD validation rejects one at
// admission, and an entry that reached here anyway must not become a matcher for
// "the Event does not carry this field" — the one thing an include list must
// never mean (see inEventFilterSet).
func canonicalEventFilterList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

// eventFieldSet is one axis of a compiled filter: the values that axis accepts.
// A nil set is an axis the filter did not name, which len reports as zero and the
// matcher reads as "this axis constrains nothing".
type eventFieldSet map[string]struct{}

// compiledEventFilter is one rule's filter, ready to evaluate. It is immutable
// once built, which is what lets every informer goroutine in the process read the
// same instance concurrently without a lock.
type compiledEventFilter struct {
	types            eventFieldSet
	reasons          eventFieldSet
	excludeReasons   eventFieldSet
	sourceComponents eventFieldSet
	subjectKinds     eventFieldSet
	subjectNames     eventFieldSet
}

// eventMatcher is a target's merged Event filter, compiled once when the interest
// is built.
//
// The merged set is a *union*, like the selector set beside it, and that is
// forced rather than chosen: there is one stored payload and one dedup baseline
// per (sink, identity), so two rules streaming the same scope to the same sink
// cannot be served two different subsets of it. Honouring only their
// intersection would let one rule's existence silence another's — the same
// reasoning plan.TargetState.Selectors records.
//
// Independence between rules that *can* be served separately lives one level up:
// interests are per (informer, sink), so an Event matching rule A's filter and
// not rule B's is enqueued for A's sink alone (see pool.fanOut).
type eventMatcher struct {
	// matchAll short-circuits the union when some contributing rule asked for
	// every Event. That is what every rule naming no eventFilter asks for, and
	// what every target for any kind other than Event carries, so it is the
	// overwhelmingly common case and is kept to a single bool read.
	matchAll bool

	// filters are the distinct compiled filters the contributing rules asked
	// for. Empty whenever matchAll is set: one contributor wanting everything
	// makes the rest redundant.
	filters []compiledEventFilter

	// fieldSelector is what a Kubernetes field selector can express of this
	// matcher, rendered once at compile time (Task 19.3). The empty string means
	// "nothing could be pushed", which is the only answer for a matcher that
	// accepts everything, for a union of two rules' filters, and for any kind
	// that is not an Event.
	//
	// It is a *narrowing the matcher still re-checks*, never a replacement for
	// it: see deriveEventFieldSelector for why that is the property the whole
	// design rests on, and note that whether it is actually used is not this
	// type's decision — an informer is shared, so the pool only pushes a
	// selector every interest on it agrees about (see WatchManager.translate).
	fieldSelector string
}

// compileEventFilters compiles a target's merged canonical filter set into the
// matcher every Event from its informer is evaluated against.
//
// Compilation happens once per pool diff, never per Event. Events are the
// highest-volume kind in a cluster, and decoding a filter inside an informer's
// notification goroutine would put JSON parsing on the one path Invariant 1 says
// must stay cheap — the same reason the selectors beside it are parsed here and
// the redaction policy is compiled here.
//
// A filter that fails to decode is an anomaly rather than an authoring error: the
// control plane produced these bytes with CanonicalEventFilter, and the CRD
// validated the values before that. It is therefore reported so the caller can
// degrade that one target (Invariant 5) instead of falling back to recording a
// stream its author asked to narrow.
//
// gr is the resource the interest's informer lists, and it is taken here rather
// than at the push-down site because the field labels an Event API registers
// depend on which of the two Event APIs it is (see eventSelectableFields). For
// every other kind the lookup simply misses, which is what makes "a Pod informer
// never acquires a field selector" true by construction rather than by a check.
func compileEventFilters(gr schema.GroupResource, canonical []string) (*eventMatcher, error) {
	m := &eventMatcher{}
	// sole is the one contributing filter, kept for the push-down derivation
	// below. It is meaningful only when exactly one survives, which is the only
	// case that can be pushed at all.
	var sole EventFilterSpec
	for _, raw := range canonical {
		if raw == "" {
			// A contributing rule with no eventFilter wants every Event, which
			// makes every other filter in the union redundant. The compiled set
			// is left empty on purpose, exactly as the selector union's is.
			m.matchAll = true
			continue
		}
		var spec EventFilterSpec
		if err := json.Unmarshal([]byte(raw), &spec); err != nil {
			return nil, fmt.Errorf("decode event filter %q: %w", raw, err)
		}
		sole = spec
		m.filters = append(m.filters, compiledEventFilter{
			types:            newEventFieldSet(spec.Types),
			reasons:          newEventFieldSet(spec.Reasons),
			excludeReasons:   newEventFieldSet(spec.ExcludeReasons),
			sourceComponents: newEventFieldSet(spec.SourceComponents),
			subjectKinds:     newEventFieldSet(spec.SubjectKinds),
			subjectNames:     newEventFieldSet(spec.SubjectNames),
		})
	}
	if len(m.filters) == 0 {
		// Either a rule asked for everything, or the merged set was empty —
		// which is what a target assembled by hand (a test, a future caller)
		// carries. Both mean the same thing and neither may mean "record
		// nothing".
		m.matchAll = true
	}
	if !m.matchAll && len(m.filters) == 1 {
		// Exactly one filter survives, so the matcher is a plain conjunction and
		// a conjunctive selector can stand for part of it. Two or more is a
		// union — an OR — which no field selector can express, and one
		// contributing rule that filtered nothing has already widened this to
		// everything.
		m.fieldSelector = deriveEventFieldSelector(gr, sole)
	}
	return m, nil
}

// eventFieldLabels are the field-selector labels one Event API registers for the
// four axes a filter can push down. An empty label is an axis that API does not
// offer, which the derivation skips.
type eventFieldLabels struct {
	eventType   string
	reason      string
	subjectKind string
	subjectName string
}

// eventSelectableFields is what each Event API actually accepts as a field
// selector, **measured against the pinned Kubernetes (envtest 1.35.0, matching
// k8s.io/api v0.35.0) rather than assumed** — which the Task 19.3 acceptance
// criteria require, because the two APIs are not symmetric and the next reader
// will otherwise expect them to be.
//
// What the probe found, in full, so nobody has to run it again:
//
//   - core `v1/events` accepts `type`, `reason`, `source`, `reportingComponent`,
//     `involvedObject.{kind,namespace,name,uid,apiVersion,resourceVersion,fieldPath}`,
//     `metadata.{name,namespace}`. It rejects every `regarding.*` label and
//     rejects `reportingController`.
//   - `events.k8s.io/v1/events` accepts `type`, `reason`,
//     `regarding.{kind,namespace,name,uid,apiVersion,fieldPath}`,
//     `reportingController`, `metadata.{name,namespace}`. It rejects every
//     `involvedObject.*` label, and it rejects `source` outright.
//   - Neither accepts `action`, `note`, `reportingInstance` or `metadata.uid`.
//
// So the modern API is not selector-less: it registers *renamed* equivalents,
// which is why both rows below are populated and why the subject labels differ
// between them. A label sent to the wrong API is rejected with `field label not
// supported`, which the reflector would retry forever — so the rename is a
// correctness matter, not a nicety.
//
// `sourceComponents` is absent from both rows on purpose, and the reason is
// measured rather than theoretical. Core's `source` is a *fallback chain* —
// `source.component`, else `reportingController` — while componentMatches is an
// **OR over four spellings**. An Event carrying `source.component=kubelet` and
// `reportingComponent=my-controller` is returned by `source=kubelet` and *not* by
// `source=my-controller`, while the matcher accepts it for either. Pushing it
// down would therefore change which rows are recorded, which is precisely what
// D49 forbids. The modern API settles the question a second time by registering
// no equivalent label at all.
//
// A GroupResource that is not here derives nothing, which is the whole of the
// "some other kind" case and also the whole of the "a future Event API this code
// has not been verified against" case. Falling back to handler-side evaluation
// costs bandwidth; guessing at a label costs correctness.
var eventSelectableFields = map[schema.GroupResource]eventFieldLabels{
	{Group: "", Resource: "events"}: {
		eventType:   "type",
		reason:      "reason",
		subjectKind: "involvedObject.kind",
		subjectName: "involvedObject.name",
	},
	{Group: "events.k8s.io", Resource: "events"}: {
		eventType:   "type",
		reason:      "reason",
		subjectKind: "regarding.kind",
		subjectName: "regarding.name",
	},
}

// deriveEventFieldSelector renders what a Kubernetes field selector can express
// of one rule's filter, for the Event API named by gr. The empty string means
// nothing could be pushed.
//
// **The derivation rule, which is not obvious and is the whole design.** Field
// selectors AND their terms and have no OR. That asymmetry decides every case:
//
//   - A **single-valued include** pushes down: `types: [Warning]` is exactly
//     `type=Warning`.
//   - An **exclusion pushes down at any length**, because an AND of `!=` terms
//     *is* "none of these": `excludeReasons: [Pulled, Created, Started]` becomes
//     `reason!=Pulled,reason!=Created,reason!=Started`. This is the happy
//     accident of the design — "drop the startup chatter, keep the rest" is both
//     the filter people most want and the one that pushes down completely.
//   - A **multi-valued include stays handler-side**, because `reason=BackOff OR
//     reason=Killing` has no spelling. It is left out of the selector entirely
//     rather than approximated.
//   - `sourceComponents` **never** pushes down (see eventSelectableFields).
//
// Every term this emits is *implied by* the filter, so the selector is always a
// superset of what the matcher will accept — which is what makes a partial
// push-down safe. The handler re-evaluates the complete filter afterwards
// regardless, so the rows recorded are identical whether a selector was pushed or
// not (D49); push-down changes only how much traffic was paid for to reach them.
// Nothing may ever be added here that the matcher does not also check.
func deriveEventFieldSelector(gr schema.GroupResource, spec EventFilterSpec) string {
	labels, selectable := eventSelectableFields[gr]
	if !selectable {
		return ""
	}

	// Terms are appended in a fixed field order and every list reaching here is
	// already canonical — sorted, deduplicated (see CanonicalEventFilter) — so
	// two interests expressing the same filter render byte-identical strings.
	// That is load-bearing rather than tidy: "do these interests agree?" is
	// decided by comparing these strings, and an order that varied would make
	// two identical filters look like a disagreement and silently give up the
	// push-down.
	var terms []fields.Selector
	if len(spec.Types) == 1 {
		terms = append(terms, fields.OneTermEqualSelector(labels.eventType, spec.Types[0]))
	}
	switch {
	case len(spec.Reasons) == 1 && len(spec.ExcludeReasons) == 0:
		terms = append(terms, fields.OneTermEqualSelector(labels.reason, spec.Reasons[0]))
	case len(spec.Reasons) == 0 && len(spec.ExcludeReasons) > 0:
		for _, reason := range spec.ExcludeReasons {
			terms = append(terms, fields.OneTermNotEqualSelector(labels.reason, reason))
		}
	}
	// A filter carrying both lists is a shape the CRD rejects at admission, and
	// the two branches above therefore leave the reason axis wholly to the
	// handler when one arrives anyway. Two operators on one key is a selector
	// shape this code has not verified any server against, and an unverified
	// selector is the one thing worth less than no selector.
	if len(spec.SubjectKinds) == 1 {
		terms = append(terms, fields.OneTermEqualSelector(labels.subjectKind, spec.SubjectKinds[0]))
	}
	if len(spec.SubjectNames) == 1 {
		terms = append(terms, fields.OneTermEqualSelector(labels.subjectName, spec.SubjectNames[0]))
	}

	if len(terms) == 0 {
		return ""
	}
	return fields.AndSelectors(terms...).String()
}

// newEventFieldSet compiles one axis into the set the matcher probes.
func newEventFieldSet(values []string) eventFieldSet {
	if len(values) == 0 {
		return nil
	}
	set := make(eventFieldSet, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

// matchesAll reports whether this matcher accepts every Event.
//
// It is asked separately from matches so the common path — no rule filtered
// anything — is one bool read at the call site rather than a call into the union
// loop. A nil matcher is match-everything for the same reason an interest with no
// selectors is: a target nobody narrowed is a target that streams whole.
func (m *eventMatcher) matchesAll() bool { return m == nil || m.matchAll }

// matches reports whether obj — the unstructured content of a Kubernetes Event —
// satisfies any contributing rule's filter.
//
// It reads only fields the Event itself carries and looks nothing up: no informer
// read, no API call, no cache access. That is Invariant 1, and it is also D47 — a
// matcher that needed the *subject* would match against a cache of things that
// exist, dropping precisely the FailedScheduling and FailedCreate Events an
// incident most needs, and it would be collectEvents rediscovered.
func (m *eventMatcher) matches(obj map[string]any) bool {
	if m.matchesAll() {
		return true
	}
	for i := range m.filters {
		if m.filters[i].matches(obj) {
			return true
		}
	}
	return false
}

// matches evaluates one rule's filter: AND across the axes it names, OR within
// each axis, and no constraint at all from an axis it left empty.
func (f compiledEventFilter) matches(obj map[string]any) bool {
	if len(f.types) > 0 && !inEventFilterSet(f.types, plainString(obj, "type")) {
		return false
	}
	if len(f.reasons) > 0 || len(f.excludeReasons) > 0 {
		// Read once and test both ways round. The CRD makes the two lists
		// mutually exclusive, so in practice only one branch is live; sharing
		// the read costs nothing and keeps that a property of the CRD rather
		// than an assumption of the matcher.
		reason := plainString(obj, "reason")
		if len(f.reasons) > 0 && !inEventFilterSet(f.reasons, reason) {
			return false
		}
		if len(f.excludeReasons) > 0 && inEventFilterSet(f.excludeReasons, reason) {
			return false
		}
	}
	if len(f.sourceComponents) > 0 && !componentMatches(f.sourceComponents, obj) {
		return false
	}
	if len(f.subjectKinds) > 0 && !inEventFilterSet(f.subjectKinds, subjectField(obj, "kind")) {
		return false
	}
	if len(f.subjectNames) > 0 && !inEventFilterSet(f.subjectNames, subjectField(obj, "name")) {
		return false
	}
	return true
}

// componentMatches reports whether any spelling of "who emitted this Event" is
// one of the values this axis accepts.
//
// There are four of them, because the two Event APIs are one storage behind two
// shapes and each shape carries the fact twice. Verified against the pinned
// k8s.io/api v0.35.0: core `v1.Event` has `source.component` and
// `reportingComponent` — whose Go field is named ReportingController, the JSON
// tag deliberately differing — while `events.k8s.io/v1.Event` has
// `deprecatedSource.component` and `reportingController`. Which one is populated
// depends on the recorder: client-go's legacy tools/record recorder sets the
// source and leaves the reporting fields empty, and tools/events sets the
// reporting fields and leaves `source` an empty object.
//
// Any-of rather than a fallback chain, which is what v1alpha1.EventFilter's
// SourceComponents field already promises its author — "an entry here is matched
// against whichever spelling the stored Event carries rather than against one of
// them". A fallback down the core pair alone would record nothing for a rule
// naming events.k8s.io/v1, where the legacy source lives under
// `deprecatedSource`: a filter narrowed to silence on a rule that stays Ready,
// which is this field's characteristic failure and precisely the one it must not
// have.
//
// The consequence for Task 19.3 is deliberate and recorded here because this is
// where it is decided: a disjunction over four fields is not expressible as a
// Kubernetes field selector, so `sourceComponents` cannot be pushed to the API
// server without changing which rows are recorded, and it stays handler-side
// (D49 — push-down is a performance decision and never a content decision). That
// prediction was then measured against the pinned API server rather than left as
// an argument, and it held twice over; eventSelectableFields carries what the
// probe found.
func componentMatches(set eventFieldSet, obj map[string]any) bool {
	return inEventFilterSet(set, nestedString(obj, "source", "component")) ||
		inEventFilterSet(set, plainString(obj, "reportingComponent")) ||
		inEventFilterSet(set, plainString(obj, "reportingController")) ||
		inEventFilterSet(set, nestedString(obj, "deprecatedSource", "component"))
}

// subjectField reads one field of the object an Event is about, under whichever
// of the two spellings the stored Event carries.
//
// core `v1` names the subject `involvedObject`; `events.k8s.io/v1` names it
// `regarding`. The read plane already coalesces the pair this way (see the query
// package's subjectMatch), and the capture side has to agree with it: a filter
// that narrowed by one spelling would silently record nothing for rules naming
// the other group, and the recorded stream would then disagree with the CLI that
// reads it back.
func subjectField(obj map[string]any, field string) string {
	if value := nestedString(obj, "involvedObject", field); value != "" {
		return value
	}
	return nestedString(obj, "regarding", field)
}

// inEventFilterSet reports whether value is one of the entries on this axis.
//
// The empty string is never a member, which is what makes "an Event missing a
// field the filter names does not match" true by construction rather than by a
// check at every call site: CRD validation rejects an empty entry and
// canonicalEventFilterList drops one that arrived anyway, so an absent field can
// only ever fail an include test and can never satisfy one.
//
// Read through ExcludeReasons the same rule says the right thing from the other
// side: an Event carrying no reason at all is not dropped by a list of reasons,
// because it is not one of them.
func inEventFilterSet(set eventFieldSet, value string) bool {
	if value == "" {
		return false
	}
	_, ok := set[value]
	return ok
}

// plainString reads a top-level string field of an Event, or "" when it is absent
// or is not a string.
//
// It walks the unstructured map directly rather than going through
// unstructured.NestedString, and the reason is the error that helper returns for
// a field of the wrong type. This matcher has to treat that exactly as it treats
// absence: an Event whose `reason` is somehow a number is not an Event the filter
// names, there is nothing an informer handler could do about it, and reporting it
// per event on the highest-volume kind in the cluster would be a log flood rather
// than a diagnosis. Delegating would therefore mean discarding an error on every
// field read, six reads per Event per interest. Reading the map directly has no
// error to discard because it has no failure to distinguish.
//
// It is also a little cheaper — measured at roughly 16ns against 20ns per read —
// but that is the smaller reason and not the one worth preserving on edit.
// Neither form allocates: BenchmarkFanOutEventFilter is what holds the whole path
// at zero (Invariant 1).
func plainString(obj map[string]any, field string) string {
	value, _ := obj[field].(string)
	return value
}

// nestedString reads a string one level down, or "" when either level is absent
// or is not what it should be. See plainString for why it is spelled out rather
// than delegated.
func nestedString(obj map[string]any, outer, field string) string {
	inner, ok := obj[outer].(map[string]any)
	if !ok {
		return ""
	}
	value, _ := inner[field].(string)
	return value
}
