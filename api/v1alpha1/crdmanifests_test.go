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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// crdBasesDir is where `make manifests` writes the generated CRDs.
const crdBasesDir = "../../config/crd/bases"

// TestGeneratedCRDsContainValidationRules asserts that the CRDs checked into
// config/crd/bases actually carry the schema this package's markers describe.
//
// The envtest suite in this package proves the *behavior* (bad objects are
// rejected) against whatever CRDs are on disk; this test proves the CRDs on
// disk are the ones the Go markers currently generate. Without it, a marker
// could be edited and `make manifests` forgotten, and the envtest suite would
// happily keep passing against the stale, previously-generated schema.
//
// The assertions are deliberately literal substring matches on the YAML rather
// than a parsed-schema walk: the whole point is to catch a *silent weakening*
// of a rule, and a literal match fails loudly the moment a pattern, bound, or
// printer column changes for any reason at all.
func TestGeneratedCRDsContainValidationRules(t *testing.T) {
	tests := []struct {
		name         string
		file         string
		mustContain  []string
		mustNotExist []string
	}{
		{
			name: "clickhousesink",
			file: "kuberecord.io_clickhousesinks.yaml",
			mustContain: []string{
				// Printer columns: READY, ADDR (see the type comment on
				// ClickHouseSink for why ADDR is host:port, not host).
				`jsonPath: .status.conditions[?(@.type=="Ready")].status`,
				"name: READY",
				"jsonPath: .spec.connection.addr",
				"name: ADDR",
				"name: AGE",
				"scope: Cluster",
				// addr non-empty.
				"minLength: 1",
				// batchMaxRows in [1, 100000] and workers in [1, 64].
				"maximum: 100000",
				"maximum: 64",
				// checkpointEvery in [0, 10000] — the floor is 0 (the off switch)
				// rather than 1, which is what makes it worth pinning here.
				"maximum: 10000",
				"minimum: 0",
				// kinds: "*" or a valid Kind, and no duplicates.
				`pattern: ^(\*|[A-Z][A-Za-z0-9]{0,62})$`,
				"x-kubernetes-list-type: set",
				// Writer defaults must stay pinned to the shipped
				// clickhouse.Default* values.
				"default: 5000",
				"default: 1000",
				"default: 15s",
				"default: 50",
				// The sink's redaction floor validates exactly like a rule's
				// additions do.
				"pattern: " + RedactionFieldPathPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
			},
		},
		{
			name: "s3sink",
			file: "kuberecord.io_s3sinks.yaml",
			mustContain: []string{
				// Printer columns: READY, BUCKET (see the type comment on S3Sink for
				// why the prefix is not in it).
				`jsonPath: .status.conditions[?(@.type=="Ready")].status`,
				"name: READY",
				"jsonPath: .spec.bucket",
				"name: BUCKET",
				"name: AGE",
				"scope: Cluster",
				// A bucket is the one required field, and the whole minimal spec.
				"\n            - bucket\n",
				"minLength: 1",
				// The object-key prefix: no leading slash, no trailing slash, no
				// empty segment. It is the authored half of a public contract (D15).
				"pattern: " + S3PrefixPattern,
				// The format enum is deliberately one member wide; widening it is an
				// additive release decision, so it is matched with its list item.
				"enum:\n                - jsonl-v1-zstd\n",
				"default: jsonl-v1-zstd",
				// Rotation: 1Mi..1Gi, and a duration range no Minimum/Maximum can
				// express, so a CEL rule that leads with its own shape guard.
				"minimum: 1048576",
				"maximum: 1073741824",
				"default: 67108864",
				"default: 5m",
				"rule: self.matches('^([0-9]+(ns|us|ms|s|m|h))+$') && duration(self)",
				// The cross-field memory bound. Matched here only far enough to prove
				// a rule is present on `spec` at all — controller-gen line-folds a
				// long CEL expression, so no substring can carry the whole of it.
				// TestS3SinkWorkerMemoryRuleIsGenerated asserts the rule itself
				// against the parsed document.
				"rule: '(has(self.writer) && has(self.writer.workers) ? self.writer.workers",
				// The credential union: a Secret, or nothing at all. The message is
				// asserted because it is what tells an author which state they meant.
				"rule: has(self.secretRef)",
				"spec.credentials must name a secretRef",
				// Object Lock, matched with its list items: adding a mode is an S3 API
				// fact, not a refactor.
				"enum:\n                    - GOVERNANCE\n                    - COMPLIANCE\n",
				"maximum: 36500",
				// The shared policy shape reaches this CRD too — redaction is a floor,
				// and choosing a backend must not be a way around it.
				"pattern: " + RedactionFieldPathPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
				"x-kubernetes-list-type: set",
			},
			mustNotExist: []string{
				// The three writer knobs this backend does not have. Their absence is
				// a design statement (the object is the batch; a Writer-only sink
				// writes no diffs), so it is pinned rather than left to review — see
				// TestSharedWriterKnobsAgreeAcrossSinks for the other half.
				"batchMaxRows:",
				"batchMaxWait:",
				"checkpointEvery:",
			},
		},
		{
			name: "streamrule",
			file: "kuberecord.io_streamrules.yaml",
			mustContain: []string{
				`jsonPath: .status.conditions[?(@.type=="Ready")].status`,
				"name: READY",
				"jsonPath: .spec.sink.name",
				// Matched with the trailing newline so the SINK-KIND column below
				// cannot satisfy this assertion on its own.
				"name: SINK\n",
				"jsonPath: .spec.sink.kind",
				"name: SINK-KIND",
				"jsonPath: .status.activeWatches",
				"name: WATCHES",
				"name: AGE",
				"scope: Namespaced",
				// resources non-empty.
				"minItems: 1",
				// GVK shape rules.
				"pattern: ^[A-Z][A-Za-z0-9]{0,62}$",
				`pattern: ^v[0-9]+((alpha|beta)[0-9]+)?$`,
				`pattern: ^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`,
				// The sink reference (Task 4.3): required as a whole, its kind
				// defaulted and restricted to the kinds this build serves, its name
				// non-empty, and the pair immutable. The enum is matched with its
				// list item so that widening it — which is a release decision, not a
				// refactor — has to be made here too.
				"\n            - sink\n",
				"default: ClickHouseSink",
				"enum:\n                    - ClickHouseSink\n                    - S3Sink\n",
				"minLength: 1",
				"rule: self == oldSelf",
				// Redaction path syntax, and the one rule a pattern cannot
				// express: exactly one of the two spellings.
				"pattern: " + RedactionFieldPathPattern,
				"pattern: " + RedactionAnnotationPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
			},
			mustNotExist: []string{
				// The retired v0.1.0 field. It is not merely absent from the Go
				// struct: it must be absent from the *schema*, because a pruned
				// unknown field is what makes a legacy rule decode with a zero sink
				// instead of silently keeping a name nothing reads.
				"sinkRef:",
				// A namespaced rule must not gain a namespaceSelector *field*:
				// it can only ever see its own namespace. Matched with the
				// trailing colon so a passing mention inside a description
				// (StreamRuleStatus.ActiveWatches has one) is not a false
				// positive — only a schema property key looks like this.
				"namespaceSelector:",
			},
		},
		{
			name: "clusterstreamrule",
			file: "kuberecord.io_clusterstreamrules.yaml",
			mustContain: []string{
				`jsonPath: .status.conditions[?(@.type=="Ready")].status`,
				"jsonPath: .spec.sink.name",
				"name: SINK\n",
				"name: SINK-KIND",
				"name: WATCHES",
				"scope: Cluster",
				"minItems: 1",
				"pattern: ^[A-Z][A-Za-z0-9]{0,62}$",
				`pattern: ^v[0-9]+((alpha|beta)[0-9]+)?$`,
				// Inlining StreamRuleSpec must carry its field rules across —
				// this is the assertion that catches controller-gen dropping
				// an inherited rule. The sink reference is the whole point of the
				// check now: its default, its enum and its transition rule all live
				// on the embedded spec.
				"\n            - sink\n",
				"default: ClickHouseSink",
				"enum:\n                    - ClickHouseSink\n                    - S3Sink\n",
				"rule: self == oldSelf",
				// Inlining must carry the redaction rules across too.
				"pattern: " + RedactionFieldPathPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
				"namespaceSelector:",
			},
			mustNotExist: []string{"sinkRef:"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(crdBasesDir, tt.file))
			if err != nil {
				t.Fatalf("reading generated CRD (did you run `make manifests`?): %v", err)
			}
			// Named `manifest` rather than `yaml`: this file now imports
			// sigs.k8s.io/yaml for the parsed assertions below, and a local of
			// that name would shadow the package.
			manifest := string(raw)
			for _, want := range tt.mustContain {
				if !strings.Contains(manifest, want) {
					t.Errorf("generated CRD %s is missing %q", tt.file, want)
				}
			}
			for _, unwanted := range tt.mustNotExist {
				if strings.Contains(manifest, unwanted) {
					t.Errorf("generated CRD %s unexpectedly contains %q", tt.file, unwanted)
				}
			}
		})
	}
}

// TestCRDPatternConstantsMatchMarkers guards the one thing the marker syntax
// cannot express: the exported *Pattern constants in shared_types.go document
// the accepted shapes for other packages (and for humans), but kubebuilder
// markers must repeat the regex literally because they are comments, not code.
// This test fails if the two ever drift.
func TestCRDPatternConstantsMatchMarkers(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		pattern string
	}{
		{name: "kind", file: "kuberecord.io_streamrules.yaml", pattern: KindPattern},
		{name: "group", file: "kuberecord.io_streamrules.yaml", pattern: GroupPattern},
		{name: "version", file: "kuberecord.io_streamrules.yaml", pattern: VersionPattern},
		{name: "kindsEntry", file: "kuberecord.io_clickhousesinks.yaml", pattern: KindsEntryPattern},
		{name: "redactionFieldPath", file: "kuberecord.io_streamrules.yaml", pattern: RedactionFieldPathPattern},
		{name: "redactionAnnotation", file: "kuberecord.io_streamrules.yaml", pattern: RedactionAnnotationPattern},
		{name: "redactionFieldPathOnSink", file: "kuberecord.io_clickhousesinks.yaml",
			pattern: RedactionFieldPathPattern},
		{name: "s3Prefix", file: "kuberecord.io_s3sinks.yaml", pattern: S3PrefixPattern},
		{name: "redactionFieldPathOnS3Sink", file: "kuberecord.io_s3sinks.yaml",
			pattern: RedactionFieldPathPattern},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(crdBasesDir, tt.file))
			if err != nil {
				t.Fatalf("reading generated CRD (did you run `make manifests`?): %v", err)
			}
			if !strings.Contains(string(raw), "pattern: "+tt.pattern) {
				t.Errorf("constant %q is not the pattern generated into %s", tt.pattern, tt.file)
			}
		})
	}
}

// ruleCRDFiles are the two generated CRDs that serve a rule.
//
// Both are always asserted, never one as a representative: ClusterStreamRuleSpec
// embeds StreamRuleSpec inline, so a single marker edit lands in both files, and a
// test that checked only one would report half a regression.
var ruleCRDFiles = []string{
	"kuberecord.io_streamrules.yaml",
	"kuberecord.io_clusterstreamrules.yaml",
}

// TestSinkReferenceHasNoMaterializingDefault pins the *absence* of a schema default
// on `spec.sink` and on `spec.sink.name`, which is the sole reason the
// ReasonLegacySinkRef guard in RuleReconciler.plan is reachable at all.
//
// The mechanism, in full, because nothing else in the tree writes it down:
//
//   - Structural-schema defaulting is applied by the API server on every *read* from
//     etcd, not only at admission. An object stored years ago is defaulted afresh
//     each time it is served, against whatever schema is installed today.
//   - Defaulting descends into a field only when that field's parent is present. A
//     default declared on a child never fires while the parent is absent.
//
// Put together: a rule stored under v0.1.0's `spec.sinkRef` is served with its
// unknown field pruned and no `sink` key at all, so it decodes into a true zero
// `SinkReference` — and `spec.Sink == (v1alpha1.SinkReference{})` catches it. Add an
// object-level default to `sink` (`+kubebuilder:default={kind:"ClickHouseSink",name:"default"}`
// is the obvious-looking convenience) and the API server materializes that reference
// on read instead. Every legacy rule then binds to a backend nobody chose, inherits
// that sink's dedup and warm state, reports Ready=True, and the guard becomes dead
// code that nothing exercises. Silent rebinding to the wrong backend is precisely the
// failure D10 exists to prevent, and it is invisible from the rule's status.
//
// `spec.sink.name` is held to the same rule for the compounding case rather than the
// immediate one. A default on `name` alone cannot fire for a legacy rule — its parent
// is absent, exactly as with `kind` — but it removes the property that makes the pair
// safe: with it in place, any path that materializes an empty `sink` yields a
// complete, plausible-looking reference instead of one the required/MinLength rules
// or this guard would still catch. It also contradicts what SinkReference.Name's own
// comment promises, which is the more durable reason not to have one.
//
// `spec.sink.kind`'s default is correct and is asserted positively here, so this test
// pins the whole arrangement rather than half of it: removing the kind default is a
// separate regression (rules in a ClickHouse-only cluster would have to spell a kind
// they have no alternative for), and a vacuity test that only forbids things would
// pass just as happily after someone deleted it.
//
// The assertions run against the generated YAML rather than the Go markers because
// the marker-to-schema translation is exactly what could change: controller-gen
// deciding to hoist a child default onto its parent would satisfy any marker-level
// check and still break the guard. And unlike this file's other tests they parse the
// document instead of matching substrings, because the property is the absence of a
// key at one specific node — `default: ClickHouseSink` legitimately appears in both
// files, so no negative substring can express it.
func TestSinkReferenceHasNoMaterializingDefault(t *testing.T) {
	for _, file := range ruleCRDFiles {
		t.Run(strings.TrimSuffix(file, ".yaml"), func(t *testing.T) {
			specSchema := crdSpecSchema(t, file)

			if !specRequires(t, specSchema, "sink") {
				t.Errorf("generated CRD %s no longer lists `sink` in spec.required.\n"+
					"A rule that names no sink must be rejected at admission; the LegacySinkRef guard in "+
					"RuleReconciler.plan exists only for the rules already in etcd that admission never saw.",
					file)
			}

			sinkSchema := schemaNode(t, file, specSchema, "properties", "sink")
			if got, defaulted := sinkSchema["default"]; defaulted {
				t.Errorf("generated CRD %s gives spec.sink a schema default (%v) — remove it.\n"+
					"Defaulting is applied on every read from etcd, not only at admission, so this default "+
					"materializes a sink reference for rules that never named one: every rule inherited from "+
					"v0.1.0's spec.sinkRef silently binds to that backend, carrying another sink's dedup and "+
					"warm state, and reports Ready=True while doing it.\n"+
					"It also makes RuleReconciler.plan's `spec.Sink == (v1alpha1.SinkReference{})` guard "+
					"(ReasonLegacySinkRef) unreachable, so nothing anywhere would report the rebinding. "+
					"This is the failure D10 exists to prevent.",
					file, got)
			}

			nameSchema := schemaNode(t, file, sinkSchema, "properties", "name")
			if got, defaulted := nameSchema["default"]; defaulted {
				t.Errorf("generated CRD %s gives spec.sink.name a schema default (%v) — remove it.\n"+
					"Guessing which of a cluster's sinks an author meant is how an audit trail quietly ends "+
					"up somewhere nobody chose. It cannot fire on its own while spec.sink is absent, but it "+
					"removes the property that keeps the pair safe: with a name default in place, anything "+
					"that materializes an empty spec.sink produces a complete, plausible reference rather "+
					"than one the required/MinLength rules or the LegacySinkRef guard would still catch.",
					file, got)
			}

			kindSchema := schemaNode(t, file, sinkSchema, "properties", "kind")
			if got := kindSchema["default"]; got != defaultSinkKind {
				t.Errorf("generated CRD %s defaults spec.sink.kind to %v, want %q.\n"+
					"This half of the arrangement is deliberate and must stay: a child default never fires "+
					"while its parent is absent, so it cannot rebind a legacy rule, and without it every rule "+
					"in a ClickHouse-only cluster would have to spell a kind it has no alternative for.",
					file, got, defaultSinkKind)
			}
		})
	}
}

// crdSpecSchema decodes one generated CRD and returns the schema of its `spec` —
// the node the parsed assertions in this file hang off, for a rule CRD and a sink
// CRD alike.
func crdSpecSchema(t *testing.T, file string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(crdBasesDir, file))
	if err != nil {
		t.Fatalf("reading generated CRD (did you run `make manifests`?): %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decoding generated CRD %s: %v", file, err)
	}

	versions, ok := schemaNode(t, file, doc, "spec")["versions"].([]any)
	if !ok || len(versions) != 1 {
		// One served version is not incidental: v1alpha1 is the only version these
		// CRDs have, and a second one would mean this helper had to pick which one
		// the guard's reachability depends on.
		t.Fatalf("generated CRD %s does not serve exactly one version", file)
	}
	version, ok := versions[0].(map[string]any)
	if !ok {
		t.Fatalf("generated CRD %s version 0 is not an object", file)
	}
	return schemaNode(t, file, version, "schema", "openAPIV3Schema", "properties", "spec")
}

// schemaNode descends a decoded CRD by map key.
//
// It fails rather than returning an empty map, because every key on the paths above
// is one controller-gen has always emitted: a missing one means the document's shape
// changed, and an absence assertion against a node that does not exist is the exact
// shape of a test that passes while proving nothing.
func schemaNode(t *testing.T, file string, node map[string]any, path ...string) map[string]any {
	t.Helper()

	for i, key := range path {
		next, found := node[key]
		if !found {
			t.Fatalf("generated CRD %s has no %s", file, strings.Join(path[:i+1], "."))
		}
		typed, ok := next.(map[string]any)
		if !ok {
			t.Fatalf("generated CRD %s node %s is not an object", file, strings.Join(path[:i+1], "."))
		}
		node = typed
	}
	return node
}

// specRequires reports whether a decoded `spec` schema lists field as required.
func specRequires(t *testing.T, specSchema map[string]any, field string) bool {
	t.Helper()

	required, ok := specSchema["required"].([]any)
	if !ok {
		t.Fatalf("the generated spec schema has no required list")
	}
	return slices.Contains(required, any(field))
}

// The two sink CRDs, named here because the writer-shape assertions below read
// both of them in one breath.
const (
	clickHouseSinkCRDFile = "kuberecord.io_clickhousesinks.yaml"
	s3SinkCRDFile         = "kuberecord.io_s3sinks.yaml"
)

// sharedWriterKnobs are the writer knobs every sink CRD carries, and
// clickHouseOnlyWriterKnobs are the ones only ClickHouse's does.
//
// The split is the API promise Task 6.1 makes: an author who has tuned one sink's
// write path has tuned the other's, and the fields that are missing are missing
// for a stated reason rather than by oversight.
var (
	sharedWriterKnobs         = []string{"queueSize", "workers", "enqueueTimeout", "drainTimeout"}
	clickHouseOnlyWriterKnobs = []string{"batchMaxRows", "batchMaxWait", "checkpointEvery"}
)

// TestS3SinkWorkerMemoryRuleIsGenerated asserts that the cross-field memory bound
// declared on S3SinkSpec actually reached the CRD on disk, in full.
//
// The envtest table proves the *behaviour* against whatever CRDs are installed;
// this proves the installed CRDs are the ones today's markers generate. That
// separation is what catches the specific accident this rule is most exposed to: a
// marker edited — the bound raised, a fallback dropped — and `make manifests`
// forgotten, leaving the admission suite passing against the previously-generated
// schema.
//
// It reads the parsed document rather than matching substrings because
// controller-gen line-folds a CEL expression this long, so a literal match can only
// ever cover the first fold. Parsing gives the expression back whole, which lets
// each load-bearing piece be named: both operands, both fallback literals, and the
// bound — the last one taken from S3WriterMemoryBudgetBytes, so the Go constant and
// the schema cannot drift the way the *Pattern constants could before
// TestCRDPatternConstantsMatchMarkers existed.
func TestS3SinkWorkerMemoryRuleIsGenerated(t *testing.T) {
	specSchema := crdSpecSchema(t, s3SinkCRDFile)

	validations, ok := specSchema["x-kubernetes-validations"].([]any)
	if !ok {
		t.Fatalf("generated CRD %s has no x-kubernetes-validations on spec", s3SinkCRDFile)
	}

	var rule, message string
	for _, entry := range validations {
		validation, isMap := entry.(map[string]any)
		if !isMap {
			t.Fatalf("generated CRD %s has a non-object spec validation: %v", s3SinkCRDFile, entry)
		}
		text, _ := validation["rule"].(string)
		if strings.Contains(text, "workers") && strings.Contains(text, "maxObjectBytes") {
			rule = text
			message, _ = validation["message"].(string)
			break
		}
	}
	if rule == "" {
		t.Fatalf("generated CRD %s carries no spec-level rule over workers and maxObjectBytes "+
			"(did you run `make manifests`?). Its absence means the 64 × 1Gi = 64Gi pairing is "+
			"admitted again, with nothing but a field comment against it.\nspec validations: %v",
			s3SinkCRDFile, validations)
	}

	// The fallbacks are named as whole ternary arms rather than as bare numbers: a
	// spec that omits `rotation` or `writer` entirely is never defaulted (structural
	// defaulting does not descend into an absent parent — see
	// TestS3SinkOmittedRotationAndWriterStayAbsent), so dropping one of these arms
	// would silently exempt exactly the hand-written YAML the rule exists for.
	for _, want := range []string{
		"? self.writer.workers : 4",
		"? self.rotation.maxObjectBytes : 67108864",
		strconv.Itoa(S3WriterMemoryBudgetBytes),
	} {
		if !strings.Contains(rule, want) {
			t.Errorf("the generated worker-memory rule does not contain %q.\nrule: %s", want, rule)
		}
	}

	// The message has to be enough to fix the spec without reading the source. It
	// names its operands by field path rather than by value because a
	// messageExpression interpolating the numbers is refused at CRD installation on
	// cost grounds (see S3SinkSpec) — so what it does say is all an author gets.
	for _, want := range []string{
		"spec.writer.workers",
		"spec.rotation.maxObjectBytes",
		"4Gi",
		strconv.Itoa(S3WriterMemoryBudgetBytes),
		"each worker accumulates its own object",
	} {
		if !strings.Contains(message, want) {
			t.Errorf("the generated worker-memory rejection message does not name %q.\nmessage: %s",
				want, message)
		}
	}
}

// TestSharedWriterKnobsAgreeAcrossSinks pins the relationship between
// ClickHouseSink's WriterSpec and S3Sink's S3WriterSpec: the four shared knobs
// validate and default identically, and the three ClickHouse-only ones exist
// nowhere on the S3 side.
//
// The two are separate Go types on purpose. Their bounds coincide but their
// reasons do not — `workers` is capped at 64 because ClickHouse punishes insert
// concurrency, and at 64 on the S3 side because each worker holds a whole object
// in memory — and a comment true of neither backend would be worse than the
// duplication. What that choice costs is the compiler's guarantee that the two
// cannot drift, so this test buys it back: change a default or a bound on one
// side and this fails, naming the knob.
//
// The comparison is deliberately over the *machine-enforced* keys only. The
// descriptions are expected to differ; that is the whole point of not sharing the
// type.
//
// The absence half matters just as much. batchMaxRows and batchMaxWait would be a
// second set of controls over a decision spec.rotation already owns, and
// checkpointEvery would be a cadence over diffs a Writer-only sink never writes
// (D12) — a knob that silently does nothing is worse than no knob, so their
// absence is asserted rather than assumed.
func TestSharedWriterKnobsAgreeAcrossSinks(t *testing.T) {
	chWriter := schemaNode(t, clickHouseSinkCRDFile,
		crdSpecSchema(t, clickHouseSinkCRDFile), "properties", "writer", "properties")
	s3Writer := schemaNode(t, s3SinkCRDFile,
		crdSpecSchema(t, s3SinkCRDFile), "properties", "writer", "properties")

	// The keys the API server actually enforces. `description` is excluded by
	// omission, not by oversight.
	enforced := []string{"type", "format", "default", "minimum", "maximum"}

	for _, knob := range sharedWriterKnobs {
		t.Run(knob, func(t *testing.T) {
			ch := schemaNode(t, clickHouseSinkCRDFile, chWriter, knob)
			s3 := schemaNode(t, s3SinkCRDFile, s3Writer, knob)
			for _, key := range enforced {
				chValue, onCH := ch[key]
				s3Value, onS3 := s3[key]
				if onCH != onS3 || chValue != s3Value {
					t.Errorf("spec.writer.%s disagrees on %q: ClickHouseSink has %v (present=%v), "+
						"S3Sink has %v (present=%v).\n"+
						"The four shared writer knobs are an API promise that tuning one sink tunes "+
						"the other; they are separate Go types only so each can explain itself in its "+
						"own backend's terms.",
						knob, key, chValue, onCH, s3Value, onS3)
				}
			}
		})
	}

	for _, knob := range clickHouseOnlyWriterKnobs {
		t.Run("no-"+knob+"-on-s3", func(t *testing.T) {
			if _, found := chWriter[knob]; !found {
				t.Fatalf("spec.writer.%s is gone from %s, so its absence from the S3Sink proves "+
					"nothing — this test is now vacuous", knob, clickHouseSinkCRDFile)
			}
			if got, found := s3Writer[knob]; found {
				t.Errorf("spec.writer.%s appeared on the S3Sink (%v).\n"+
					"batchMaxRows and batchMaxWait would be a second set of controls over what "+
					"spec.rotation already decides, and checkpointEvery a cadence over diffs a "+
					"Writer-only sink never writes (D12). A knob that does nothing is worse than "+
					"no knob.", knob, got)
			}
		})
	}
}
