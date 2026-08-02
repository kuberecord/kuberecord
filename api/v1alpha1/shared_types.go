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

// Condition types shared by every kubestream CRD.
//
// They are string constants rather than inline literals because reconcilers
// (Task 1.7) and the e2e suite both assert on them: a typo in one place would
// otherwise silently produce a condition nobody is watching for. Every type
// listed here follows the Kubernetes convention that `True` is the healthy
// state, so a `False` value always means "this specific thing is wrong" and
// never "this thing is fine".
const (
	// ConditionReady is the roll-up condition present on all three CRDs: the
	// object is fully realised (a sink is connected and schema-checked, a rule
	// has all its watches running). It is False whenever any of the more
	// specific conditions below is False, so a single `kubectl get` column can
	// summarise health (Invariant 5: one bad rule degrades only itself).
	ConditionReady = "Ready"
)

// Condition types specific to ClickHouseSink.
const (
	// ConditionSchemaValid reports whether the sink's live ClickHouse schema
	// matches the DDL this operator build expects. It is a *separate* condition
	// from Ready because a schema mismatch is operator-actionable in a
	// completely different way from an unreachable host, and because the probe
	// that sets it is asynchronous — control-plane reconcilers never dial
	// ClickHouse on the reconcile path (Invariant 1).
	ConditionSchemaValid = "SchemaValid"

	// ConditionCredentialsResolved reports whether the Secret named by
	// spec.connection.credentialsSecretRef was found and contained the expected
	// key. It is distinct from SchemaValid so an operator can tell "I cannot
	// authenticate" apart from "I authenticated but the tables are wrong".
	ConditionCredentialsResolved = "CredentialsResolved"
)

// Condition types specific to StreamRule and ClusterStreamRule.
const (
	// ConditionRBACGranted reports whether the operator's own ServiceAccount is
	// actually permitted to list/watch every resource this rule names, as
	// answered by SelfSubjectAccessReview (Task 1.9). It exists because the
	// operator can never self-escalate (D7): a rule asking for a resource
	// outside the aggregated ClusterRole must degrade visibly on the rule
	// rather than crash the process or silently stream nothing.
	ConditionRBACGranted = "RBACGranted"

	// ConditionPolicyAllowed reports whether every resource this rule names is
	// permitted by the target sink's spec.policy.allowedGVKs and is not on the
	// hard deny-list (D8: v1/Secret is never watchable in v1alpha1). Sink
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

// kindPattern and its `kinds`-entry variant are the single source of truth for
// what kubestream accepts as a Kubernetes Kind. They are duplicated verbatim
// into the `+kubebuilder:validation:Pattern` markers below (markers cannot
// reference Go constants) and re-asserted against the generated CRD YAML in
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
	// `v1alpha1`. The identity key is version-agnostic (Invariant 7), but the
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
)

// RedactionRule names one value to scrub out of every streamed object before it
// is hashed, diffed and written (Task 3.3).
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

// WatchedResource names one resource type a rule wants streamed.
//
// It is a (group, version, kind) triple plus an optional label selector rather
// than a plural resource name because the CRD is authored by humans against
// the same vocabulary they read in `kubectl explain` and YAML `apiVersion` /
// `kind` fields; the plural GVR is derived by the REST mapper (Task 1.3), which
// is also where an unknown kind is detected and parked.
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
	// The recorded object identity is version-agnostic (Invariant 7), so
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
	// without tearing down and re-listing a watch (Task 1.4). The trade-off is
	// deliberate — informer bandwidth in exchange for a pool that never
	// thrashes on a selector edit.
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
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

// StreamRuleSpec is the intent shared by StreamRule and ClusterStreamRule:
// which sink to write to, and which resources to stream there.
//
// ClusterStreamRuleSpec embeds this inline and adds only a namespace selector,
// so the two CRDs cannot drift apart field-by-field — and every validation rule
// below is written at *field* level precisely so that inlining preserves it.
type StreamRuleSpec struct {
	// SinkRef names the ClickHouseSink (cluster-scoped, so no namespace) that
	// this rule's records are written to.
	//
	// It is immutable. Re-pointing a live rule at a different sink would strand
	// the dedup/diff baseline the pipeline has built for every object in scope:
	// the new sink has no history for them, so either every object re-emits as
	// a duplicate or, worse, diffs get written against a baseline the target
	// sink never received. Rather than build a cross-sink cache migration for a
	// rare operation, moving a rule is delete + recreate — which re-warms the
	// cache from the new sink's own history, correctly and by construction.
	//
	// Omitting the field defaults it to "default", so rules in a single-sink
	// cluster carry no boilerplate. Spelling it as an explicit empty string is
	// rejected rather than defaulted: an author who typed the key meant to
	// name a sink.
	// +kubebuilder:default="default"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sinkRef is immutable: delete and recreate this rule to point it at a different sink"
	SinkRef string `json:"sinkRef,omitempty"`

	// Resources are the resource types this rule streams. At least one is
	// required — an empty rule is always an authoring mistake, and rejecting it
	// at admission is far kinder than a rule that reconciles green while
	// streaming nothing.
	//
	// Kubernetes Events (`v1/Event` or `events.k8s.io/v1/Event`, whichever you
	// name) are streamed in a built-in Events mode, because an Event is
	// append-only ephemera rather than durable cluster state. Naming one is the
	// only thing you do — there is no switch, and no way to opt out, since every
	// difference exists to stop kubestream recording something untrue:
	//
	//   - every row carries the full Event, never a diff, so a count bump is
	//     readable on its own;
	//   - an Event's ~1h TTL expiry is recorded as nothing at all, never as a
	//     Deleted row;
	//   - watch scopes still open and close normally, and a restart still
	//     deduplicates against already-recorded Events.
	//
	// See docs/SCHEMA.md ("Kubernetes Events") and docs/QUERIES.md.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	Resources []WatchedResource `json:"resources"`

	// ExtraRedaction adds value paths to scrub, on top of whatever the target
	// sink's `spec.policy.redaction` already scrubs (Task 3.3).
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
	// remains denied in code (D8) whether or not a policy would scrub it.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	ExtraRedaction []RedactionRule `json:"extraRedaction,omitempty"`
}

// StreamRuleStatus is the observed state shared by StreamRule and
// ClusterStreamRule.
type StreamRuleStatus struct {
	// Conditions carries Ready, RBACGranted, PolicyAllowed and
	// ResourceResolved (see the constants above). A rule that cannot run
	// degrades here and only here: the process never exits and every other
	// rule keeps streaming (Invariant 5).
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

// SecretReference points at a Secret holding a sink's credentials.
//
// It is a local type rather than corev1.SecretReference so the namespace can
// carry kubestream's own defaulting semantics (see the field comment) and so
// the API surface stays limited to the two fields kubestream actually honours.
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
	// namespace (Task 1.9, D7). A cluster-scoped ClickHouseSink is editable by
	// anyone with cluster-level write access to the CRD, so if this field
	// could freely name any namespace, creating a sink would become a way to
	// make the operator read a Secret its RBAC never intended to expose. Left
	// empty, a sink can only ever reach credentials an administrator has
	// deliberately placed alongside the operator.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace,omitempty"`
}
