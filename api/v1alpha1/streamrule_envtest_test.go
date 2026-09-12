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
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// deploymentResource is the canonical valid WatchedResource the rule tables
// start from. Cases testing a GVK rule swap in a deliberately broken variant so
// a failure names the rule that fired, not "something in this object is wrong".
func deploymentResource() WatchedResource {
	return WatchedResource{Group: "apps", Version: "v1", Kind: "Deployment"}
}

// serviceResource is a second valid resource, used to prove that the resources
// list stays mutable even though the sink reference does not.
func serviceResource() WatchedResource {
	return WatchedResource{Group: "", Version: "v1", Kind: "Service"}
}

// ruleSpec returns a StreamRuleSpec the apiserver must accept. Called with no
// arguments it yields an explicitly *empty* (not nil) resources list, so the
// emitted JSON is `[]` and the MinItems rule is what rejects it — a nil slice
// would serialise to `null` and trip the weaker "Required value" check instead,
// leaving MinItems untested.
func ruleSpec(resources ...WatchedResource) StreamRuleSpec {
	if resources == nil {
		resources = []WatchedResource{}
	}
	return StreamRuleSpec{Sink: SinkReference{Name: defaultSinkName}, Resources: resources}
}

// unstructuredRule builds a rule of the given kind directly as unstructured
// JSON, with sink as its `spec.sink` — or with no `sink` key at all when sink is
// nil.
//
// It exists for the cases the typed client cannot express. SinkReference marshals
// both of its fields, so a typed object always submits a `sink` object carrying a
// name, which is exactly what the required-field and MinLength rules are there to
// reject. Only a document that omits the key, or spells `name: ""`, reaches them —
// and those are the documents a YAML author actually writes: an omitted `sink` is
// what a rule migrated from v0.1.0's `sinkRef` looks like on the way in.
func unstructuredRule(kind, namespace string, sink map[string]any) clientObject {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind(kind))
	if namespace != "" {
		u.SetNamespace(namespace)
	}
	spec := map[string]any{
		"resources": []any{map[string]any{
			"group": "apps", "version": "v1", "kind": "Deployment",
		}},
	}
	if sink != nil {
		spec["sink"] = sink
	}
	if err := unstructured.SetNestedMap(u.Object, spec, "spec"); err != nil {
		panic("building unstructured rule: " + err.Error())
	}
	return u
}

// The sink names and kinds the table below references.
//
// defaultSinkKind is the kind an unqualified reference defaults to; otherSinkKind
// is the second kind this build serves, which is what makes a *kind*
// change a legal edit to attempt and therefore what finally exercises the sink
// reference's immutability rule rather than its enum. unknownSinkKind is a kind
// no release serves yet, used here to prove that this one refuses to admit it.
const (
	defaultSinkName = "default"
	otherSinkName   = "other-sink"
	defaultSinkKind = "ClickHouseSink"
	otherSinkKind   = "S3Sink"
	unknownSinkKind = "PostgresSink"
)

// ruleEditor bundles the two CRD-specific operations the shared rule table
// needs: how to build the concrete CRD around a StreamRuleSpec, and how to
// perform each in-place edit on it.
//
// The table below is shared between StreamRule and ClusterStreamRule rather
// than duplicated because "a ClusterStreamRule validates its inlined spec
// exactly like a StreamRule does" is precisely the property that inlining
// StreamRuleSpec is supposed to guarantee. Asserting it from one table makes a
// regression in either CRD impossible to miss.
type ruleEditor struct {
	// kind and namespace let the table build the same rule as unstructured
	// JSON where the typed client cannot express the case under test.
	kind           string
	namespace      string
	build          func(StreamRuleSpec) clientObject
	setSinkName    func(clientObject)
	setSinkKind    func(clientObject)
	appendResource func(clientObject)
}

// ruleValidationCases builds the admission expectations both rule CRDs must
// satisfy identically.
func ruleValidationCases(e ruleEditor) []apiCase {
	withResource := func(r WatchedResource) clientObject { return e.build(ruleSpec(r)) }
	return append([]apiCase{
		{
			name: "minimal-valid-rule-is-accepted",
			obj:  e.build(ruleSpec(deploymentResource())),
		},
		{
			name: "core-group-resource-is-accepted",
			obj:  e.build(ruleSpec(WatchedResource{Group: "", Version: "v1", Kind: "ConfigMap"})),
		},
		{
			name: "alpha-version-is-accepted",
			obj:  withResource(WatchedResource{Group: "kuberecord.io", Version: "v1alpha1", Kind: "StreamRule"}),
		},
		{
			name:    "empty-resources-is-rejected",
			obj:     e.build(ruleSpec()),
			wantErr: "should have at least 1 items",
		},
		{
			name:    "lowercase-kind-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "v1", Kind: "deployment"}),
			wantErr: "should match",
		},
		{
			name:    "plural-resource-name-as-kind-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "v1", Kind: "deployments"}),
			wantErr: "should match",
		},
		{
			name:    "bad-version-string-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "apps/v1", Kind: "Deployment"}),
			wantErr: "should match",
		},
		{
			name:    "non-dns-group-is-rejected",
			obj:     withResource(WatchedResource{Group: "Apps", Version: "v1", Kind: "Deployment"}),
			wantErr: "should match",
		},
		// The sink reference. A rule with no sink at all is what a
		// v0.1.0 rule looks like once `sinkRef` is an unknown field, so rejecting
		// it on write is half of the migration story — the other half is the
		// reconciler guard for the ones already in etcd, which admission cannot
		// reach.
		{
			name:    "missing-sink-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, nil),
			wantErr: "spec.sink: Required value",
		},
		{
			name:    "sink-without-a-name-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, map[string]any{}),
			wantErr: "spec.sink.name: Required value",
		},
		{
			name:    "explicitly-empty-sink-name-is-rejected",
			obj:     unstructuredRule(e.kind, e.namespace, map[string]any{"name": ""}),
			wantErr: "should be at least 1 chars long",
		},
		{
			// The enum is what keeps a kind this build does not serve from being
			// admitted and then parking forever with no backend behind it.
			name: "unknown-sink-kind-is-rejected",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": unknownSinkKind, "name": defaultSinkName,
			}),
			wantErr: "Unsupported value",
		},
		{
			// The accepting direction of the same rule, which is what stops the
			// enum from being widened in the Go marker and nowhere else: every kind
			// this build serves must be spellable.
			name: "the-second-served-sink-kind-is-accepted",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": otherSinkKind, "name": defaultSinkName,
			}),
		},
		{
			name: "fully-spelled-sink-is-accepted",
			obj: unstructuredRule(e.kind, e.namespace, map[string]any{
				"kind": defaultSinkKind, "name": otherSinkName,
			}),
		},
		{
			// The kind may be omitted; the name may not. What the omission is
			// defaulted *to* is asserted by TestStreamRuleLabelSelectorAndDefaults.
			name: "sink-name-alone-is-accepted",
			obj:  unstructuredRule(e.kind, e.namespace, map[string]any{"name": otherSinkName}),
		},
		{
			name:    "sink-name-mutation-is-rejected",
			obj:     e.build(ruleSpec(deploymentResource())),
			mutate:  e.setSinkName,
			wantErr: "sink is immutable",
		},
		{
			// Now that a second sink kind is served, this edit is
			// structurally valid and the *immutability* rule is what refuses it —
			// which is the expectation that was unreachable while the enum held one
			// value and the apiserver rejected the spelling before ever evaluating a
			// transition rule.
			//
			// It is also the edit with the worst consequences if it were allowed:
			// re-pointing a live rule from a ClickHouseSink to an S3Sink would strand
			// the dedup baseline the pipeline built for every object in scope, and
			// silently move that rule onto a backend that can never reconstruct
			// history.
			name:    "sink-kind-mutation-is-rejected",
			obj:     e.build(ruleSpec(deploymentResource())),
			mutate:  e.setSinkKind,
			wantErr: "sink is immutable",
		},
		{
			name:   "resources-remain-mutable",
			obj:    e.build(ruleSpec(deploymentResource())),
			mutate: e.appendResource,
		},
		// Redaction path syntax. The rejections are what stops a
		// malformed policy from reaching the data plane, where the only remaining
		// options would be to degrade the rule silently or to stream unredacted.
		{
			name: "redaction-field-path-is-accepted",
			obj:  e.build(redactingSpec(RedactionRule{FieldPath: "data.password"})),
		},
		{
			name: "redaction-array-wildcard-is-accepted",
			obj: e.build(redactingSpec(RedactionRule{
				FieldPath: "spec.template.spec.containers[*].env[*].value",
			})),
		},
		{
			name: "redaction-annotation-shorthand-is-accepted",
			obj:  e.build(redactingSpec(RedactionRule{Annotation: "my.company.io/api-token"})),
		},
		{
			name: "redaction-annotation-shorthand-accepts-a-dotted-key",
			obj: e.build(redactingSpec(RedactionRule{
				Annotation: "kubectl.kubernetes.io/last-applied-configuration",
			})),
		},
		{
			name:    "redaction-with-both-fields-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "data.password", Annotation: "token"})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "redaction-with-neither-field-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "redaction-indexed-path-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "spec.containers[0].name"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-leading-dot-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: ".data.password"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-trailing-dot-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "data.password."})),
			wantErr: "should match",
		},
		{
			name:    "redaction-jsonpath-syntax-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: "$.data.password"})),
			wantErr: "should match",
		},
		{
			name:    "redaction-quoted-segment-is-rejected-in-a-field-path",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: `metadata.annotations["token"]`})),
			wantErr: "should match",
		},
		{
			// A key that could close the quote the data plane renders it into
			// (see pipeline.AnnotationRedactionPath) would let an author express
			// a path they did not write.
			name:    "redaction-annotation-with-a-quote-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{Annotation: `to"ken`})),
			wantErr: "should match",
		},
		{
			name:    "redaction-empty-field-path-is-rejected",
			obj:     e.build(redactingSpec(RedactionRule{FieldPath: ""})),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
	}, eventFilterCases(e)...)
}

// eventResource is a `v1/Event` entry carrying filter, which may be nil. Every
// eventFilter case is built from this or from its events.k8s.io twin, because
// Event in one of those two groups is the only place the filter is valid.
func eventResource(filter *EventFilter) WatchedResource {
	return WatchedResource{Group: "", Version: "v1", Kind: "Event", EventFilter: filter}
}

// modernEventResource is the same entry named through `events.k8s.io/v1`. Both
// groups are one storage behind two APIs, so the CEL rule must admit either —
// and a test that exercised only the core spelling would pass against a rule
// that had quietly lost half its disjunction.
func modernEventResource(filter *EventFilter) WatchedResource {
	return WatchedResource{Group: "events.k8s.io", Version: "v1", Kind: "Event", EventFilter: filter}
}

// unstructuredEventRule builds a rule whose single resource is a `v1/Event`
// carrying exactly the given eventFilter, as raw JSON.
//
// It exists for the one property the typed client cannot express: `reasons: []`.
// Every list on EventFilter is `omitempty`, so an explicitly-empty slice
// marshals to nothing at all and the case it is meant to drive — a present but
// empty list alongside a populated excludeReasons — never reaches the API
// server. That case is what pins the mutual-exclusion rule to comparing *sizes*
// rather than presence, which is in turn what keeps it consistent with the
// documented semantics: a list that is present and empty constrains nothing, and
// something that constrains nothing cannot conflict with anything.
func unstructuredEventRule(kind, namespace string, filter map[string]any) clientObject {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind(kind))
	if namespace != "" {
		u.SetNamespace(namespace)
	}
	spec := map[string]any{
		"sink": map[string]any{"name": defaultSinkName},
		"resources": []any{map[string]any{
			"group": "", "version": "v1", "kind": "Event",
			"eventFilter": filter,
		}},
	}
	if err := unstructured.SetNestedMap(u.Object, spec, "spec"); err != nil {
		panic("building unstructured event rule: " + err.Error())
	}
	return u
}

// reasonList builds n distinct, individually valid reasons, so a MaxItems
// rejection is unambiguously about the bound rather than about an entry.
func reasonList(n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, fmt.Sprintf("Reason%d", i))
	}
	return out
}

// eventFilterCases are the admission expectations for `eventFilter`, appended to
// the shared rule table so both CRDs run them.
//
// They are a separate function only for readability; running them through
// ruleEditor is the point, since the two CEL rules under test live on
// WatchedResource and on EventFilter, and reach ClusterStreamRule solely by way
// of StreamRuleSpec being inlined. A suite that checked one CRD would report
// half a regression.
func eventFilterCases(e ruleEditor) []apiCase {
	withResource := func(r WatchedResource) clientObject { return e.build(ruleSpec(r)) }
	filtered := func(f *EventFilter) clientObject { return withResource(eventResource(f)) }

	return []apiCase{
		// The accepting direction, field by field. A rule that rejected every
		// filter would satisfy the rejections below and nothing else.
		{
			name: "event-rule-without-a-filter-is-accepted",
			obj:  filtered(nil),
		},
		{
			name: "empty-event-filter-is-accepted",
			obj:  filtered(&EventFilter{}),
		},
		{
			name: "event-filter-types-alone-is-accepted",
			obj:  filtered(&EventFilter{Types: []EventType{EventTypeWarning}}),
		},
		{
			// Naming both members is legal and means what naming neither means.
			// It is also the only value MaxItems=2 admits at full length.
			name: "event-filter-with-both-types-is-accepted",
			obj:  filtered(&EventFilter{Types: []EventType{EventTypeNormal, EventTypeWarning}}),
		},
		{
			name: "event-filter-reasons-alone-is-accepted",
			obj:  filtered(&EventFilter{Reasons: []string{"BackOff", "FailedScheduling"}}),
		},
		{
			name: "event-filter-exclude-reasons-alone-is-accepted",
			obj:  filtered(&EventFilter{ExcludeReasons: []string{"Pulling", "Pulled", "Created"}}),
		},
		{
			// Bare and qualified spellings both, because events/v1 documents
			// `kubernetes.io/kubelet` as the shape of a reportingController while
			// every legacy source.component in a cluster is bare.
			name: "event-filter-source-components-alone-is-accepted",
			obj: filtered(&EventFilter{
				SourceComponents: []string{"default-scheduler", "kubernetes.io/kubelet"},
			}),
		},
		{
			name: "event-filter-subject-kinds-alone-is-accepted",
			obj:  filtered(&EventFilter{SubjectKinds: []string{"Pod", "ReplicaSet"}}),
		},
		{
			name: "event-filter-subject-names-alone-is-accepted",
			obj:  filtered(&EventFilter{SubjectNames: []string{"postgres-0", "postgres-1"}}),
		},
		{
			// The one character EventSubjectNamePattern adds to a DNS-1123
			// subdomain. An Event about a ClusterRole is rare, not impossible,
			// and its subject's name is the only common one shaped like this.
			name: "event-filter-subject-name-with-rbac-colons-is-accepted",
			obj: filtered(&EventFilter{
				SubjectNames: []string{"system:controller:deployment-controller"},
			}),
		},
		{
			// Every field that can coexist, at once: the AND-across-fields shape
			// the type comment describes.
			name: "event-filter-combining-every-compatible-field-is-accepted",
			obj: filtered(&EventFilter{
				Types:            []EventType{EventTypeWarning},
				ExcludeReasons:   []string{"Pulling", "Pulled"},
				SourceComponents: []string{"default-scheduler"},
				SubjectKinds:     []string{"Pod", "ReplicaSet"},
				SubjectNames:     []string{"postgres-0"},
			}),
		},
		{
			name: "event-filter-on-the-events-group-is-accepted",
			obj:  withResource(modernEventResource(&EventFilter{Types: []EventType{EventTypeWarning}})),
		},
		{
			// The size-based half of the mutual-exclusion rule: a present but
			// empty reasons list constrains nothing, so it conflicts with
			// nothing. Rewriting that rule as `has(x) && has(y)` fails here.
			name: "event-filter-empty-reasons-beside-exclude-reasons-is-accepted",
			obj: unstructuredEventRule(e.kind, e.namespace, map[string]any{
				"reasons":        []any{},
				"excludeReasons": []any{"Pulled"},
			}),
		},

		// The rejections. The first two are the whole reason the rule lives on
		// WatchedResource rather than on the filter: a filter on a kind that
		// carries none of its axes would match nothing, and the entry would
		// record an empty stream while the rule reported Ready.
		{
			name:    "event-filter-on-a-non-event-kind-is-rejected",
			obj:     withResource(WatchedResource{Group: "apps", Version: "v1", Kind: "Deployment", EventFilter: &EventFilter{Types: []EventType{EventTypeWarning}}}),
			wantErr: "eventFilter is only valid on a Kubernetes Event",
		},
		{
			// The group half of the same rule. A CRD-backed kind that happens to
			// be called Event is not a Kubernetes Event and carries none of the
			// fields a filter reads.
			name:    "event-filter-on-an-event-kind-in-another-group-is-rejected",
			obj:     withResource(WatchedResource{Group: "kuberecord.io", Version: "v1alpha1", Kind: "Event", EventFilter: &EventFilter{Types: []EventType{EventTypeWarning}}}),
			wantErr: "eventFilter is only valid on a Kubernetes Event",
		},
		{
			name: "event-filter-reasons-with-exclude-reasons-is-rejected",
			obj: filtered(&EventFilter{
				Reasons:        []string{"BackOff"},
				ExcludeReasons: []string{"Pulled"},
			}),
			wantErr: "reasons and excludeReasons are mutually exclusive",
		},
		{
			name:    "event-filter-invalid-type-is-rejected",
			obj:     filtered(&EventFilter{Types: []EventType{"warning"}}),
			wantErr: "Unsupported value",
		},
		{
			// The same mistake KindPattern exists to catch, one level deeper:
			// involvedObject.kind is a Kind, never a plural resource name.
			name:    "event-filter-lowercase-subject-kind-is-rejected",
			obj:     filtered(&EventFilter{SubjectKinds: []string{"pod"}}),
			wantErr: "should match",
		},
		{
			// A reason no emitter can ever produce. Admitted, it would narrow the
			// rule to silence with nothing to say why.
			name:    "event-filter-reason-with-a-space-is-rejected",
			obj:     filtered(&EventFilter{Reasons: []string{"Failed Scheduling"}}),
			wantErr: "should match",
		},
		{
			name:    "event-filter-source-component-with-a-space-is-rejected",
			obj:     filtered(&EventFilter{SourceComponents: []string{"default scheduler"}}),
			wantErr: "should match",
		},
		{
			name:    "event-filter-uppercase-subject-name-is-rejected",
			obj:     filtered(&EventFilter{SubjectNames: []string{"Postgres-0"}}),
			wantErr: "should match",
		},
		{
			// listType=set, which is what makes a duplicate an apiserver
			// rejection rather than a CEL uniqueness rule nobody wrote.
			name:    "event-filter-duplicate-reason-is-rejected",
			obj:     filtered(&EventFilter{Reasons: []string{"BackOff", "BackOff"}}),
			wantErr: "Duplicate value",
		},
		{
			name:    "event-filter-too-many-reasons-is-rejected",
			obj:     filtered(&EventFilter{Reasons: reasonList(65)}),
			wantErr: "must have at most 64 items",
		},
	}
}

// redactingSpec is a valid rule spec whose extraRedaction is exactly rule, so a
// rejection names the redaction rule under test and nothing else.
func redactingSpec(rule RedactionRule) StreamRuleSpec {
	spec := ruleSpec(deploymentResource())
	spec.ExtraRedaction = []RedactionRule{rule}
	return spec
}

func TestStreamRuleValidation(t *testing.T) {
	runAPICases(t, ruleValidationCases(ruleEditor{
		kind:      "StreamRule",
		namespace: testNamespace,
		build: func(spec StreamRuleSpec) clientObject {
			return &StreamRule{ObjectMeta: objectMeta(testNamespace), Spec: spec}
		},
		setSinkName: func(o clientObject) {
			o.(*StreamRule).Spec.Sink.Name = otherSinkName
		},
		setSinkKind: func(o clientObject) {
			o.(*StreamRule).Spec.Sink.Kind = otherSinkKind
		},
		appendResource: func(o clientObject) {
			r := o.(*StreamRule)
			r.Spec.Resources = append(r.Spec.Resources, serviceResource())
		},
	}))
}

// TestStreamRuleLabelSelectorAndDefaults proves the two things a pattern check
// cannot: that the optional per-resource label selector is actually wired
// through to storage, and that sink.kind defaults to ClickHouseSink when a rule
// names only a sink name — which is what keeps rules in a ClickHouse-only
// cluster free of a kind they have no alternative for.
func TestStreamRuleLabelSelectorAndDefaults(t *testing.T) {
	ctx := context.Background()
	rule := &StreamRule{
		ObjectMeta: objectMeta(testNamespace),
		Spec: StreamRuleSpec{
			// sink.kind deliberately omitted: the apiserver must default it.
			Sink: SinkReference{Name: defaultSinkName},
			Resources: []WatchedResource{{
				Group: "", Version: "v1", Kind: "ConfigMap",
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"kuberecord.io/audit": "true"},
				},
			}},
		},
	}
	rule.SetName("selector-and-defaults-rule")
	if err := k8sClient.Create(ctx, rule); err != nil {
		t.Fatalf("creating rule: %v", err)
	}
	defer deleteObject(ctx, t, rule)

	got := &StreamRule{}
	key := types.NamespacedName{Name: rule.Name, Namespace: rule.Namespace}
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatalf("reading rule back: %v", err)
	}
	if got.Spec.Sink.Kind != defaultSinkKind {
		t.Errorf("sink.kind defaulted to %q, want %q", got.Spec.Sink.Kind, defaultSinkKind)
	}
	if got.Spec.Sink.Name != defaultSinkName {
		t.Errorf("sink.name round-tripped as %q, want %q", got.Spec.Sink.Name, defaultSinkName)
	}
	sel := got.Spec.Resources[0].LabelSelector
	if sel == nil || sel.MatchLabels["kuberecord.io/audit"] != "true" {
		t.Errorf("labelSelector did not round-trip: %+v", sel)
	}
}
