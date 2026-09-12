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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types shared by every kuberecord CRD.
//
// They are string constants rather than inline literals because the reconcilers
// and the e2e suite both assert on them: a typo in one place would
// otherwise silently produce a condition nobody is watching for. Every type
// listed here follows the Kubernetes convention that `True` is the healthy
// state, so a `False` value always means "this specific thing is wrong" and
// never "this thing is fine" — with exactly one deliberate exception,
// ConditionHistoryUnavailable, which documents its own inversion.
const (
	// ConditionReady is the roll-up condition present on all four CRDs: the
	// object is fully realised (a sink is connected and schema-checked, a rule
	// has all its watches running). It is False whenever any of the more
	// specific conditions below is False, so a single `kubectl get` column can
	// summarise health — one bad rule degrades only itself.
	//
	// ConditionHistoryUnavailable is the one condition it does not roll up. A
	// Writer-only sink reports that condition True forever and is
	// nonetheless entirely healthy: a declared capability limit is not a fault,
	// and folding it into Ready would leave an operator permanently unable to
	// tell a working archive from a broken one.
	ConditionReady = "Ready"
)

// Condition types shared by every sink CRD.
const (
	// ConditionCredentialsResolved reports whether the credential this sink
	// names was found and was usable — the Secret at
	// spec.connection.credentialsSecretRef for a ClickHouseSink, the one at
	// spec.credentials.secretRef for an S3Sink. It is a condition of its own so
	// an operator can tell "I cannot authenticate" apart from every other way a
	// backend can be unreachable or wrong.
	//
	// A sink that names no Secret at all — an S3Sink using ambient credentials —
	// still reports it: there is nothing to resolve from the cluster, but whether
	// the ambient chain produced a credential is exactly as worth knowing.
	ConditionCredentialsResolved = "CredentialsResolved"

	// ConditionHistoryUnavailable reports that this sink cannot read its own
	// history back, so the behaviours that depend on reading it — dedup cache
	// warm-up, zombie garbage collection, and boot reconciliation of scope
	// epochs — are disabled for it, and every record it receives is a permanent
	// Snapshot.
	//
	// It inverts this file's convention on purpose: `True` is the abnormal-sounding
	// value and yet the sink is healthy, in the same shape as a Node's
	// NetworkUnavailable. The inversion is the point. A Writer-only sink's
	// degradation is invisible in its output — an archive with no deletions in it
	// and a full re-snapshot after every restart looks exactly like an archive of
	// a cluster where nothing was deleted — so the only honest place to state it is
	// a condition that is present, positive and permanent, rather than a False
	// reading of some capability that a reader might never think to look for.
	//
	// It never drags Ready False; see ConditionReady.
	ConditionHistoryUnavailable = "HistoryUnavailable"
)

// Condition types specific to ClickHouseSink.
const (
	// ConditionSchemaValid reports whether the sink's live ClickHouse schema
	// matches the DDL this operator build expects. It is a *separate* condition
	// from Ready because a schema mismatch is operator-actionable in a
	// completely different way from an unreachable host, and because the probe
	// that sets it is asynchronous — control-plane reconcilers never dial
	// ClickHouse on the reconcile path.
	ConditionSchemaValid = "SchemaValid"
)

// Condition types specific to S3Sink.
const (
	// ConditionBucketReachable reports whether the bucket named by spec.bucket
	// answered, and answered to a *write*. It is deliberately not a HEAD: a
	// read-only credential passes a HEAD and then fails every PUT, which would
	// produce a sink reporting Ready while archiving nothing — the exact silent
	// degradation this backend must not ship with.
	ConditionBucketReachable = "BucketReachable"
)

// Condition types specific to StreamRule and ClusterStreamRule.
const (
	// ConditionRBACGranted reports whether the operator's own ServiceAccount is
	// actually permitted to list/watch every resource this rule names, as
	// answered by SelfSubjectAccessReview. It exists because the operator can
	// never self-escalate: a rule asking for a resource
	// outside the aggregated ClusterRole must degrade visibly on the rule
	// rather than crash the process or silently stream nothing.
	ConditionRBACGranted = "RBACGranted"

	// ConditionPolicyAllowed reports whether every resource this rule names is
	// permitted by the target sink's spec.policy.allowedGVKs and is not on the
	// hard deny-list — v1/Secret is never watchable in v1alpha1. Sink
	// policy is enforced rule-side so the rule's owner — not the sink's — sees
	// why their rule is inert.
	ConditionPolicyAllowed = "PolicyAllowed"

	// ConditionResourceResolved reports whether every named GVK resolved to a
	// GVR via the REST mapper with a compatible scope. It is False (and
	// self-heals) while a rule names a CRD-backed kind whose CRD is not
	// installed yet, and False permanently while a namespaced StreamRule names
	// a cluster-scoped kind — only ClusterStreamRule may do that.
	ConditionResourceResolved = "ResourceResolved"
)

// The patterns every `+kubebuilder:validation:Pattern` marker in this package
// spells out. They are the single source of truth for the shapes kuberecord
// accepts — a Kubernetes Kind, a redaction path, an object-key prefix — and they
// are duplicated verbatim into the markers below (markers cannot reference Go
// constants) and re-asserted against the generated CRD YAML in
// crdmanifests_test.go, so a drift between the two is a test failure rather
// than a silently weaker schema.
const (
	// KindPattern matches a Kubernetes Kind: an upper-camel identifier of at
	// most 63 characters. Rejecting a lowercase leading character is what
	// catches the single most common authoring mistake — writing the plural
	// *resource* ("pods") where a *Kind* ("Pod") belongs, which would otherwise
	// fail much later and much less legibly at REST-mapper resolution time.
	KindPattern = `^[A-Z][A-Za-z0-9]{0,62}$`

	// GroupPattern matches an API group: either empty (the core group, as in
	// `v1/Pod`) or a DNS-1123 subdomain. Empty must be spelled as the empty
	// string rather than "core"; the API machinery uses "" everywhere and
	// accepting a second spelling would make two distinct rules resolve to the
	// same watch target.
	GroupPattern = `^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

	// VersionPattern matches a Kubernetes API version: `v1`, `v2beta1`,
	// `v1alpha1`. The identity key is version-agnostic, but the
	// version is still required here because it is what the REST mapper and the
	// dynamic client need to build a concrete GVR.
	VersionPattern = `^v[0-9]+((alpha|beta)[0-9]+)?$`

	// KindsEntryPattern matches one entry of a GVKSelector's `kinds` list: a
	// Kind, or the literal `*` meaning "every kind in this group/version".
	KindsEntryPattern = `^(\*|[A-Z][A-Za-z0-9]{0,62})$`

	// RedactionFieldPathPattern matches a RedactionRule.FieldPath: dot-separated
	// field names, each optionally followed by the `[*]` array wildcard, as in
	// `spec.template.spec.containers[*].env[*].value`.
	//
	// It is the admission-time half of a grammar whose other half is the data
	// plane's parser (see pipeline.CompileRedaction); a controller test asserts
	// the two accept the same strings, so a path the API server admits can never
	// fail to compile in the pipeline and silently degrade a rule to streaming
	// nothing. It deliberately admits no JSONPath construct — no filters, no
	// recursive descent, no index — because a policy whose match set depends on
	// an object's contents is one whose effect cannot be read off the policy.
	RedactionFieldPathPattern = `^[a-zA-Z_][a-zA-Z0-9_-]*(\[\*\])?(\.[a-zA-Z_][a-zA-Z0-9_-]*(\[\*\])?)*$`

	// RedactionAnnotationPattern matches a RedactionRule.Annotation: a
	// Kubernetes annotation key, i.e. an optional DNS-subdomain prefix and a
	// `/`, then the name itself.
	//
	// Quotation marks and backslashes are excluded by construction, which is
	// load-bearing rather than incidental: the key is rendered into a quoted path
	// segment when it crosses into the data plane (see
	// pipeline.AnnotationRedactionPath), and a key able to close that quote could
	// express a path its author did not write.
	RedactionAnnotationPattern = `^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`

	// S3PrefixPattern matches an S3Sink's optional object-key prefix: slash-joined
	// segments of an unreserved character set, or nothing at all.
	//
	// What it forbids is as load-bearing as what it admits. A leading slash, a
	// trailing slash and an empty segment all produce a `//` in every key the sink
	// writes, because the key is built by joining this prefix to a fixed layout
	// (`<prefix>/format=jsonl-v1/…`) — and that layout is a public contract
	// whose readers glob on it. The character set is the conservative subset that
	// needs no escaping in a key, a URL or a query engine's glob; it can be widened
	// later without invalidating a single object already written, which is not true
	// of narrowing it.
	S3PrefixPattern = `^([A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*)?$`

	// EventReasonPattern matches one entry of an EventFilter's `reasons` or
	// `excludeReasons`: an Event's `reason`, as every emitter in practice spells
	// it — `BackOff`, `FailedScheduling`, `SuccessfulCreate`.
	//
	// It is deliberately stricter than Kubernetes is. The API server constrains
	// `reason` by length alone (128 characters; see events/v1.Event.Reason in
	// k8s.io/api), so any byte sequence is a legal reason and a pattern that
	// admitted all of them would admit `Failed Scheduling` typed with a space —
	// which matches no Event ever written, on a rule that stays Ready. A filter
	// narrowed to silence rather than to relevance is the one authoring mistake
	// this field can make with no feedback at all, and rejecting whitespace at
	// admission is where it is cheapest to learn.
	//
	// Excluding `,` and `=` is the second reason: an entry is rendered into a
	// field-selector term when the filter pushes down, and those two are that
	// grammar's separators.
	EventReasonPattern = `^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`

	// EventSourceComponentPattern matches one entry of an EventFilter's
	// `sourceComponents`: the component that emitted an Event, spelled either
	// bare (`kubelet`, `default-scheduler`) or qualified (`kubernetes.io/kubelet`,
	// which is the example events/v1 itself gives for `reportingController`).
	//
	// It is a Kubernetes qualified name because that is what the API server
	// validates `reportingController` as. The legacy `source.component` carries no
	// validation, but both spellings are written by the same controllers, so
	// holding the field to the stricter of the two shapes rejects nothing a real
	// cluster emits.
	//
	// It is byte-identical to RedactionAnnotationPattern and is deliberately a
	// separate constant. An annotation key and an Event's reporting component are
	// unrelated things that happen to share Kubernetes' qualified-name grammar,
	// and that pattern's exclusion of quotes and backslashes is load-bearing for a
	// reason — path rendering in the data plane — which has nothing to do with
	// this field. Folding one into the other would make a future edit to either
	// silently an edit to both.
	EventSourceComponentPattern = `^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`

	// EventSubjectNamePattern matches one entry of an EventFilter's
	// `subjectNames`: the `metadata.name` of the object an Event is about.
	//
	// It is a DNS-1123 subdomain widened by exactly one character, `:`, which no
	// workload's name may contain but which is the ordinary shape of an RBAC
	// object's — `system:controller:deployment-controller` — and an Event about a
	// ClusterRole is rare rather than impossible.
	//
	// ObjectReference.Name has no validation of its own in Kubernetes, so this
	// bound is kuberecord's rather than the API server's. It exists to reject the
	// uppercase and whitespace typos that would otherwise yield a filter matching
	// nothing, which is this field's characteristic failure (see the field
	// comment). Widening it later invalidates no rule already authored; narrowing
	// it would.
	EventSubjectNamePattern = `^[a-z0-9]([-a-z0-9.:]*[a-z0-9])?$`
)

// RedactionRule names one value to scrub out of every streamed object before it
// is hashed, diffed and written.
//
// Exactly one of the two fields is set. They are separate fields rather than one
// string with two grammars because an annotation key routinely contains dots and
// slashes — `kubectl.kubernetes.io/last-applied-configuration` — which the
// dot-segment path syntax cannot spell unambiguously: written as a `fieldPath`
// it would mean six nested maps that do not exist. The shorthand is therefore
// the only way to name such a key, not sugar over a longer form.
//
// Redaction is *additive* everywhere it appears. A sink's policy is the floor,
// a rule's `extraRedaction` adds to it, and rules that overlap on one target
// contribute a union — nothing anywhere can remove a path another party asked
// for. That is what lets a platform team hand out a sink whose redaction floor
// they own without reviewing every rule written against it.
//
// +kubebuilder:validation:XValidation:rule="has(self.fieldPath) != has(self.annotation)",message="exactly one of fieldPath or annotation must be set"
type RedactionRule struct {
	// FieldPath is the value to scrub, as dot-separated field names with an
	// optional `[*]` wildcard over arrays — `data.password`,
	// `spec.template.spec.containers[*].env[*].value`.
	//
	// A path that matches nothing in a given object is a silent no-op, which is
	// what makes one policy usable across a whole kind. A path that matches a
	// map or an array rather than a scalar replaces that whole subtree with the
	// sentinel.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_-]*(\[\*\])?(\.[a-zA-Z_][a-zA-Z0-9_-]*(\[\*\])?)*$`
	FieldPath string `json:"fieldPath,omitempty"`

	// Annotation is the shorthand for one annotation key, equivalent to a
	// fieldPath of `metadata.annotations` indexed by this exact key.
	//
	// `kubectl.kubernetes.io/last-applied-configuration` never needs listing: it
	// is scrubbed on every object under every policy, including an empty one,
	// because kubectl copies the entire submitted object into it and it would
	// otherwise re-leak every value the rest of the policy removes.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	Annotation string `json:"annotation,omitempty"`
}

// EventType is the `type` axis of a Kubernetes Event: the two-valued severity
// every emitter sets.
//
// It is a named type carrying its own enum rather than a `[]string` field with
// an items-level marker, so the closed set lives with the thing it closes and
// the evaluator that compiles a filter compares against named constants rather
// than bare literals. The values mirror corev1.EventTypeNormal and
// corev1.EventTypeWarning exactly; they are restated here because a CRD field's
// enum must be a marker on a type in this package, not a reference to one in
// another module.
//
// There is no `excludeTypes` counterpart. With two members, excluding one is
// spelling the other, and a second field that can only ever be a synonym for the
// first is one more thing to keep consistent for no expressive gain.
//
// +kubebuilder:validation:Enum=Normal;Warning
type EventType string

const (
	// EventTypeNormal is an Event reporting that something happened as intended
	// — `Scheduled`, `Pulled`, `Created`, `Started`. These are numerous, and
	// each fires roughly once.
	EventTypeNormal EventType = "Normal"

	// EventTypeWarning is an Event reporting that something did not —
	// `BackOff`, `FailedScheduling`, `Unhealthy`. These are fewer, and they
	// recur: the API server bumps an Event's `count` in place for as long as the
	// fault persists, and every bump is another full row.
	//
	// The two distributions run opposite to intuition, which is worth knowing
	// before sizing anything on them. `types: [Warning]` drops most rows in a
	// healthy cluster and almost none in an unhealthy one — so it narrows a
	// stream to what an operator wants to read, and does not bound what the
	// stream costs when the cluster is on fire. Relevance and volume are
	// different axes; see EventFilter.
	EventTypeWarning EventType = "Warning"
)

// EventFilter narrows which Kubernetes Events a rule records, using fields the
// Event itself carries.
//
// **Semantics.** Within a list, OR: `reasons: [BackOff, FailedScheduling]`
// matches an Event whose reason is either. Across fields, AND: a filter naming
// both `types` and `reasons` records only the Events matching both. An absent or
// empty list is no constraint at all, and an absent `eventFilter` is what every
// rule did before this field existed — every Event in the rule's scope is
// recorded.
//
// **Every axis is a column of the Event**, which is the design rule rather than
// a coincidence. A filter is evaluated per Event inside an informer handler, so
// it may read only what is already in hand and may look nothing up
// (Invariant 1). The *subject's* labels are absent for a second and sharper
// reason: matching them would mean consulting a cache of objects that exist, and
// the Events worth most during an incident — `FailedScheduling`, `FailedCreate`,
// `Killing` — are about objects that failed to exist or are ceasing to. A filter
// on the subject's labels would drop precisely those.
//
// `involvedObject.uid` is absent for a third reason: a UID does not exist until
// its object does and changes on every recreation, so no rule could be authored
// against one in advance. It is a query predicate, not a capture predicate, and
// it is available when querying the recorded stream.
//
// **This narrows relevance, not volume.** Filtering chooses which Event streams
// are kept; it does not change how deep each one goes. An Event that recurs is
// updated in place to bump its `count`, so its content genuinely changes, hash
// dedup cannot suppress it, and every recurrence writes another full row —
// under every filter here, including one that keeps a single reason. See the
// `resources` field comment for what that costs and docs/SCHEMA.md
// ("Event volume") for how to size it.
//
// The rule below states the answer to "what if both `reasons` and
// `excludeReasons`?", because two fields doing inverse jobs need one. It
// compares sizes rather than mere presence so that it agrees with the semantics
// above: a list that is present and empty constrains nothing, and something that
// constrains nothing cannot be in conflict with anything.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.reasons) && size(self.reasons) > 0 && has(self.excludeReasons) && size(self.excludeReasons) > 0)",message="reasons and excludeReasons are mutually exclusive: name the reasons you want, or the ones you do not"
type EventFilter struct {
	// Types restricts capture to Events of these types — `Normal`, `Warning`, or
	// both, which is the same as naming neither.
	//
	// The bound is the enum's own size, so it can reject nothing a set of these
	// values could hold; it is spelled anyway because every list on this type is
	// bounded, and a member added to EventType later would otherwise silently
	// widen this one.
	// +optional
	// +kubebuilder:validation:MaxItems=2
	// +listType=set
	Types []EventType `json:"types,omitempty"`

	// Reasons restricts capture to Events whose `reason` is one of these —
	// `BackOff`, `FailedScheduling`, `Unhealthy`.
	//
	// A reason is emitter-defined, so the set worth naming depends on which
	// controllers run in the cluster; `kubectl get events -o custom-columns=:.reason`
	// over a representative window is the honest way to find it. Mutually
	// exclusive with ExcludeReasons.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`
	// +listType=set
	Reasons []string `json:"reasons,omitempty"`

	// ExcludeReasons drops Events whose `reason` is one of these, recording
	// everything else — `Pulling`, `Pulled`, `Created`, `Started`: the startup
	// chatter a rollout produces once per container and nobody reads twice.
	//
	// It is the inverse of Reasons and the two may not both be used. Prefer this
	// one where either would do. An exclusion keeps the reasons nobody has
	// thought of yet, which for a stream written by every controller in the
	// cluster is most of them — and an include list silently stops recording the
	// day a new operator starts emitting something worth seeing.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`
	// +listType=set
	ExcludeReasons []string `json:"excludeReasons,omitempty"`

	// SourceComponents restricts capture to Events emitted by these components —
	// `default-scheduler`, `kubelet`, `deployment-controller`.
	//
	// The two Event APIs spell this field differently — `source.component` in
	// `v1`, `reportingController` in `events.k8s.io/v1` — and they are one
	// storage behind two APIs, so an entry here is matched against whichever
	// spelling the stored Event carries rather than against one of them. A
	// component may be bare or qualified (`kubernetes.io/kubelet`); name it as
	// `kubectl get events -o custom-columns=:.source.component` prints it.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:items:Pattern=`^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	// +listType=set
	SourceComponents []string `json:"sourceComponents,omitempty"`

	// SubjectKinds restricts capture to Events about objects of these Kinds —
	// `Pod`, `ReplicaSet`. It reads the Event's `involvedObject.kind`, which is
	// the Kind as the emitter spelled it, not a plural resource name.
	//
	// Note what this does *not* do: it selects Events by the kind of their
	// subject, and has no relationship to the `resources` list this rule also
	// names. Event capture is scope-wide, so `subjectKinds: [Pod]` records
	// Events about every Pod in scope, including Pods no other entry in this
	// rule watches.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[A-Z][A-Za-z0-9]{0,62}$`
	// +listType=set
	SubjectKinds []string `json:"subjectKinds,omitempty"`

	// SubjectNames restricts capture to Events about objects with these exact
	// names, read from the Event's `involvedObject.name`.
	//
	// ⚠️ **Exact match only, and its usefulness is inverted from what you would
	// expect.** It works for objects whose names a human chose and which survive
	// a rollout — a Deployment, a Service, a StatefulSet's Pods (`postgres-0`,
	// `postgres-1`, which are ordinal and stable). It is **useless for the Pods
	// of a Deployment**: those names are generated
	// (`checkout-api-69dfc5f67d-ldw5j`), they change on every rollout, and a name
	// that no longer exists matches nothing while the rule stays Ready — so the
	// stream goes quiet with nothing anywhere saying why. To follow an ownership
	// tree, ask at read time, where the names exist; a capture-time filter cannot
	// express one.
	//
	// There is no prefix or glob form, deliberately. A prefix cannot be expressed
	// as a Kubernetes field selector, so it could not be pushed to the API server
	// and would instead pull the entire Event stream of the namespace over the
	// network, cache it, transform it and discard most of it locally — spending
	// the exact cost this type exists to avoid, on the highest-volume kind in the
	// cluster.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=253
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9.:]*[a-z0-9])?$`
	// +listType=set
	SubjectNames []string `json:"subjectNames,omitempty"`
}

// WatchedResource names one resource type a rule wants streamed.
//
// It is a (group, version, kind) triple plus an optional label selector rather
// than a plural resource name because the CRD is authored by humans against
// the same vocabulary they read in `kubectl explain` and YAML `apiVersion` /
// `kind` fields; the plural GVR is derived by the REST mapper, which
// is also where an unknown kind is detected and parked.
//
// The rule below is written at *type* level rather than on `eventFilter`, which
// is not a style choice: a field-level rule's `self` is the field itself, and
// this one has to read two of the field's siblings. It lands on the `items`
// schema of `resources` and is therefore carried into ClusterStreamRule by the
// same inlining that carries every other rule on this spec.
//
// It rejects an `eventFilter` on anything but an Event because such a rule
// cannot do what it says — none of the filter's axes exists on a Deployment, so
// the filter would match nothing and the entry would record an empty stream
// while reporting Ready. Admission is where that is cheapest to learn.
//
// The core group is spelled `size(self.group) == 0` rather than as a comparison
// against an empty string literal: gofmt rewrites a doubled quote inside a doc
// comment into a typographic one, which would silently corrupt the rule this
// marker generates.
//
// +kubebuilder:validation:XValidation:rule="!has(self.eventFilter) || (self.kind == 'Event' && (!has(self.group) || size(self.group) == 0 || self.group == 'events.k8s.io'))",message="eventFilter is only valid on a Kubernetes Event: name kind Event in the core or the events.k8s.io group"
type WatchedResource struct {
	// Group is the API group, e.g. "apps" or "networking.k8s.io". Empty means
	// the core group (`v1/Pod`). Must be empty or a DNS-1123 subdomain.
	// +optional
	// +kubebuilder:default=""
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Group string `json:"group"`

	// Version is the API version to watch, e.g. "v1" or "v1beta1".
	//
	// The recorded object identity is version-agnostic, so
	// naming `apps/v1` and `apps/v2` for the same Kind describes the *same*
	// objects seen through two different lenses — do not do that; pick the
	// version you want the stored payload rendered in.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^v[0-9]+((alpha|beta)[0-9]+)?$`
	Version string `json:"version"`

	// Kind is the resource Kind in upper camel case, e.g. "Deployment" — not
	// the plural resource name ("deployments").
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Z][A-Za-z0-9]{0,62}$`
	Kind string `json:"kind"`

	// LabelSelector optionally narrows the rule to objects carrying matching
	// labels. Nil selects every object of this kind in scope.
	//
	// Selectors are applied by the event handler, not by the informer's
	// ListWatch: one informer per (GVR, namespace) is shared by every rule and
	// sink interested in that target, so changing a selector re-filters events
	// without tearing down and re-listing a watch. The trade-off is
	// deliberate — informer bandwidth in exchange for a pool that never
	// thrashes on a selector edit.
	//
	// It matches the watched object's *own* labels, which makes it useless on an
	// Event entry and quietly so: it would be matched against the Event's labels
	// rather than against those of whatever the Event is about, and Events —
	// written by kubelet, the scheduler and the controllers — carry essentially
	// none. The result is an empty scope, not a narrower one, on a rule that
	// stays Ready. Narrow Events by namespace, or by `eventFilter` below, which
	// reads fields the Event itself carries; see docs/SCHEMA.md
	// ("Event volume").
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// EventFilter optionally narrows which Kubernetes Events this entry records,
	// by fields the Event carries — its type, its reason, the component that
	// emitted it, and the kind and name of the object it is about.
	//
	// It is valid only on an Event entry (`v1/Event` or `events.k8s.io/v1/Event`)
	// and is rejected at admission anywhere else; see the rule on this type. Nil
	// records every Event in scope, which is what a rule naming Event did before
	// this field existed.
	//
	// It is the Event-shaped counterpart of LabelSelector above, and exists
	// because that field cannot do this job: a selector matches the *Event's*
	// labels, and Events carry essentially none.
	// +optional
	EventFilter *EventFilter `json:"eventFilter,omitempty"`
}

// GVKSelector matches a set of resource types for sink admission policy.
//
// It is a group/version plus a *list* of kinds (rather than a flat GVK list)
// because the common policy statement is "everything in this group" — spelled
// `kinds: ["*"]` — and enumerating every Kind of a large group by hand would
// both bloat the CR and go stale the moment a new Kind ships.
type GVKSelector struct {
	// Group is the API group this selector admits. Empty means the core group.
	// +optional
	// +kubebuilder:default=""
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Group string `json:"group"`

	// Version is the API version this selector admits.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^v[0-9]+((alpha|beta)[0-9]+)?$`
	Version string `json:"version"`

	// Kinds are the admitted Kinds within group/version. The single entry `*`
	// admits every Kind in that group/version.
	//
	// The list is a set: the API server rejects duplicate entries outright
	// (`x-kubernetes-list-type: set`), which is both cheaper and stricter than
	// an equivalent CEL uniqueness rule would be.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^(\*|[A-Z][A-Za-z0-9]{0,62})$`
	// +listType=set
	Kinds []string `json:"kinds"`
}

// SinkReference names one sink instance: which kind of backend, and which CR of
// that kind. It is the authored spelling of the runtime's own sink identity, and
// the two are deliberately the same shape.
//
// The kind is part of the reference because a name is only unique *within* a
// kind: a ClickHouseSink named "default" and an S3Sink named "default" are both
// legal in etcd and are two entirely unrelated backends. Keyed on the name
// alone, whichever reconciled second would displace the first, and rules would
// then stream to a backend carrying another one's dedup cache and warm state —
// re-emitting every object, or suppressing genuine changes, with nothing in the
// logs to say so. Naming the kind makes that unrepresentable rather than merely
// unlikely.
//
// Sink CRs are cluster-scoped, so a kind and a name are a complete
// reference: there is no namespace to carry, and a rule names its sink the same
// way from any namespace.
type SinkReference struct {
	// Kind is the sink CR's kind, spelled as the API server spells it —
	// "ClickHouseSink", "S3Sink". Omitting it means ClickHouseSink, which is the
	// default only because ClickHouse was the first backend and is what every rule
	// written before the kind existed meant; nothing in the runtime treats that
	// kind specially, and no path falls back to it when another kind fails to
	// resolve. In particular a rule meaning to archive to an S3Sink must say so:
	// the default is inherited history, not a preference.
	//
	// The enum lists only the kinds this release ships a backend for, which is the
	// point of having one: a rule naming a kind no reconciler implements would
	// otherwise be admitted and then park forever with nothing to bind to, and
	// its author's only clue would be a condition on an object they may not think
	// to read. Rejecting the spelling at admission puts the error where they
	// typed it. Each new backend adds itself here as it lands.
	//
	// Widening it is a release decision rather than a refactor, so it is asserted
	// literally, with its list items, in crdmanifests_test.go.
	// +optional
	// +kubebuilder:default="ClickHouseSink"
	// +kubebuilder:validation:Enum=ClickHouseSink;S3Sink
	Kind string `json:"kind,omitempty"`

	// Name is the sink CR's name.
	//
	// Unlike the kind it has no default. An author writing a sink reference is
	// naming one specific backend out of however many a cluster runs, and
	// guessing which one they meant is the kind of convenience that quietly
	// streams a cluster's audit trail somewhere nobody chose.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// StreamRuleSpec is the intent shared by StreamRule and ClusterStreamRule:
// which sink to write to, and which resources to stream there.
//
// ClusterStreamRuleSpec embeds this inline and adds only a namespace selector,
// so the two CRDs cannot drift apart field-by-field — and every validation rule
// below is written at *field* level precisely so that inlining preserves it.
type StreamRuleSpec struct {
	// Sink names the sink this rule's records are written to: which kind of
	// backend, and which CR of that kind (sinks are cluster-scoped, so no
	// namespace).
	//
	// It is immutable. Re-pointing a live rule at a different sink would strand
	// the dedup/diff baseline the pipeline has built for every object in scope:
	// the new sink has no history for them, so either every object re-emits as
	// a duplicate or, worse, diffs get written against a baseline the target
	// sink never received. Rather than build a cross-sink cache migration for a
	// rare operation, moving a rule is delete + recreate — which re-warms the
	// cache from the new sink's own history, correctly and by construction.
	//
	// The rule guards the whole reference rather than each field, because the
	// identity is the pair: changing the name and changing the kind are the same
	// mistake with the same consequence, and one rule says so once.
	//
	// A rule targets exactly one sink, permanently. To stream one resource
	// set to two backends — a queryable timeline and a cheap immutable archive,
	// say — author two rules naming the same resources and different sinks. That
	// is the supported shape rather than a workaround: each rule then carries its
	// own dedup state, its own conditions and its own watch accounting, so one
	// unreachable backend degrades one rule.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sink is immutable: delete and recreate this rule to point it at a different sink"
	Sink SinkReference `json:"sink"`

	// Resources are the resource types this rule streams. At least one is
	// required — an empty rule is always an authoring mistake, and rejecting it
	// at admission is far kinder than a rule that reconciles green while
	// streaming nothing.
	//
	// Kubernetes Events (`v1/Event` or `events.k8s.io/v1/Event`, whichever you
	// name) are streamed in a built-in Events mode, because an Event is
	// append-only ephemera rather than durable cluster state. Naming one is the
	// only thing you do — there is no switch, and no way to opt out, since every
	// difference exists to stop kuberecord recording something untrue:
	//
	//   - every row carries the full Event, never a diff, so a count bump is
	//     readable on its own;
	//   - an Event's ~1h TTL expiry is recorded as nothing at all, never as a
	//     Deleted row;
	//   - watch scopes still open and close normally, and a restart still
	//     deduplicates against already-recorded Events.
	//
	// Naming Event is a sizing decision, not a checkbox. Capture is scope-wide —
	// every Event in the selected namespaces, not only those about the other
	// kinds listed here — and the API server bumps an Event's `count` in place,
	// so every recurrence changes the content, escapes hash dedup and writes
	// another full row: a crash-looping namespace, not a busy one, is what
	// dominates write volume. Prefer a namespaced StreamRule or a
	// namespaceSelector; a labelSelector does not narrow this (see the field
	// below). An `eventFilter` narrows *which* Events are recorded, by fields the
	// Event carries — but it does not change how many rows a recurring one
	// produces, so it is a relevance control and not a volume bound.
	//
	// See docs/EVENTS.md for the subsystem on one page — this paragraph in full,
	// the filter's semantics, which filters reach the API server, how to size a
	// rule, and what --with-events and --events-only do at read time. The
	// row-level detail stays in docs/SCHEMA.md ("Kubernetes Events" for what the
	// rows mean, "Event volume" for what they cost), and the SQL in
	// docs/QUERIES.md.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	Resources []WatchedResource `json:"resources"`

	// ExtraRedaction adds value paths to scrub, on top of whatever the target
	// sink's `spec.policy.redaction` already scrubs.
	//
	// It is strictly additive: a rule can add paths, never remove one the sink's
	// owner configured. Values are scrubbed after normalization and *before*
	// hashing, so a redacted value never reaches ClickHouse — not in `data`, not
	// in a `diff` delta, and not as a hash an attacker could grind. Two objects
	// differing only in a redacted value are indistinguishable to the pipeline
	// and deduplicate away.
	//
	// The paths apply to every resource this rule names. A path matching nothing
	// in a given object is a no-op, so one rule can redact `data.password`
	// across a mixed resource list without splitting into two rules.
	//
	// Redaction is not a way to stream something otherwise forbidden: `v1/Secret`
	// remains denied in code whether or not a policy would scrub it.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	ExtraRedaction []RedactionRule `json:"extraRedaction,omitempty"`
}

// StreamRuleStatus is the observed state shared by StreamRule and
// ClusterStreamRule.
type StreamRuleStatus struct {
	// Conditions carries Ready, RBACGranted, PolicyAllowed,
	// ResourceResolved and HistoryUnavailable (see the constants above). A
	// rule that cannot run degrades here and only here: the process never
	// exits and every other rule keeps streaming.
	//
	// HistoryUnavailable is the odd one out, and is mirrored from the sink
	// the rule names rather than decided about the rule: a rule bound to a
	// Writer-only sink reports it True, permanently, while staying
	// entirely Ready. It is here because the two objects usually have
	// different owners — a rule's author may never read the cluster-scoped
	// sink they named — and an author who sees only Ready=True would have no
	// way to learn that their stream will contain no deletions. See
	// ConditionHistoryUnavailable.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ActiveWatches is how many (GVR, namespace) watch targets this rule
	// currently contributes to the data plane. It is the field that makes
	// "is this rule actually doing anything?" answerable from `kubectl get`
	// alone — a rule can be Ready with zero active watches if, for example,
	// its namespaceSelector currently matches no namespace.
	//
	// Informers are shared across rules, so this counts the rule's *targets*,
	// not a number of goroutines it exclusively owns.
	// +optional
	ActiveWatches int32 `json:"activeWatches,omitempty"`

	// ObservedGeneration is the metadata.generation this status reflects.
	// Without it a client cannot distinguish "Ready, and up to date" from
	// "Ready, but that verdict predates your last edit".
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// SinkPolicy is the sink owner's admission policy over what may be written to
// it. Every sink CRD carries one, in this one shape.
//
// It exists because sink ownership and rule ownership are different roles: the
// platform team owning a backend needs a say in what lands in it that does not
// depend on reviewing every StreamRule anyone writes.
//
// That the shape is shared rather than per-backend is a property, not tidiness.
// Redaction is a per-sink floor, so a backend whose policy block were weaker or
// absent would make *choosing that backend* a way around the floor — and the
// temptation is worst exactly where the risk is: an archive nobody queries, whose
// objects outlive every rule and reviewer that produced them.
type SinkPolicy struct {
	// AllowedGVKs restricts which resource types may be streamed to this sink.
	//
	// An empty (or omitted) list allows everything *except* the hard deny-list
	// — v1/Secret is never watchable in v1alpha1 and no policy here can
	// re-enable it. Deny is enforced in code, not merely in config, so the
	// permissive default can never become a way to exfiltrate Secrets.
	// +optional
	// +kubebuilder:validation:MaxItems=128
	AllowedGVKs []GVKSelector `json:"allowedGVKs,omitempty"`

	// Redaction is this sink's redaction floor: value paths scrubbed out of
	// every object any rule streams here, before hashing.
	//
	// It lives on the sink for the same reason AllowedGVKs does. Whoever owns
	// the backend owns what may land in it, and that authority has to hold
	// without reviewing every StreamRule anyone writes — so a rule may add paths
	// through its own `spec.extraRedaction`, but nothing a rule declares can
	// remove one listed here.
	//
	// An empty list is not "no redaction": the data plane always scrubs
	// `kubectl.kubernetes.io/last-applied-configuration`, which embeds whole
	// prior copies of the objects it annotates.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	Redaction []RedactionRule `json:"redaction,omitempty"`
}

// SecretReference points at a Secret holding a sink's credentials.
//
// It is a local type rather than corev1.SecretReference so the namespace can
// carry kuberecord's own defaulting semantics (see the field comment) and so
// the API surface stays limited to the two fields kuberecord actually honours.
type SecretReference struct {
	// Name is the Secret's name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace is the Secret's namespace. Empty means the namespace the
	// operator itself runs in.
	//
	// That default is a security property, not a convenience: the operator's
	// aggregated ClusterRole grants Secret read access *only* in its own
	// namespace. Sink CRs are cluster-scoped and therefore
	// editable by anyone with cluster-level write access to the CRD, so if this
	// field could freely name any namespace, creating a sink would become a way
	// to make the operator read a Secret its RBAC never intended to expose —
	// and, with a sink that ships the value straight to a bucket, to read it
	// back out again. Left empty, a sink can only ever reach credentials an
	// administrator has deliberately placed alongside the operator.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace,omitempty"`
}
