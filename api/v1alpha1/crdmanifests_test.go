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
	"strings"
	"testing"
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
			name: "streamrule",
			file: "kuberecord.io_streamrules.yaml",
			mustContain: []string{
				`jsonPath: .status.conditions[?(@.type=="Ready")].status`,
				"name: READY",
				"jsonPath: .spec.sinkRef",
				"name: SINK",
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
				// sinkRef defaulted, non-empty and immutable.
				"default: default",
				"rule: self == oldSelf",
				// Redaction path syntax, and the one rule a pattern cannot
				// express: exactly one of the two spellings.
				"pattern: " + RedactionFieldPathPattern,
				"pattern: " + RedactionAnnotationPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
			},
			mustNotExist: []string{
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
				"name: SINK",
				"name: WATCHES",
				"scope: Cluster",
				"minItems: 1",
				"pattern: ^[A-Z][A-Za-z0-9]{0,62}$",
				`pattern: ^v[0-9]+((alpha|beta)[0-9]+)?$`,
				// Inlining StreamRuleSpec must carry its field rules across —
				// this is the assertion that catches controller-gen dropping
				// an inherited rule.
				"rule: self == oldSelf",
				// Inlining must carry the redaction rules across too.
				"pattern: " + RedactionFieldPathPattern,
				"rule: has(self.fieldPath) != has(self.annotation)",
				"namespaceSelector:",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(crdBasesDir, tt.file))
			if err != nil {
				t.Fatalf("reading generated CRD (did you run `make manifests`?): %v", err)
			}
			yaml := string(raw)
			for _, want := range tt.mustContain {
				if !strings.Contains(yaml, want) {
					t.Errorf("generated CRD %s is missing %q", tt.file, want)
				}
			}
			for _, unwanted := range tt.mustNotExist {
				if strings.Contains(yaml, unwanted) {
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
