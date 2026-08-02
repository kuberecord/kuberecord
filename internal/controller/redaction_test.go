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

package controller

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/internal/pipeline"
)

// TestCanonicalRedaction covers the control plane's half of Task 3.3: a sink's
// floor and a rule's additions become one canonical string, and the canonical
// form is stable under reordering and repetition — which is what stops a
// cosmetic policy edit from churning every compiled policy in the data plane.
func TestCanonicalRedaction(t *testing.T) {
	fieldPath := func(path string) v1alpha1.RedactionRule {
		return v1alpha1.RedactionRule{FieldPath: path}
	}
	annotation := func(key string) v1alpha1.RedactionRule {
		return v1alpha1.RedactionRule{Annotation: key}
	}

	tests := []struct {
		name  string
		floor []v1alpha1.RedactionRule
		extra []v1alpha1.RedactionRule
		want  string
	}{
		{
			name: "no policy at all",
			want: "",
		},
		{
			name:  "the sink's floor alone",
			floor: []v1alpha1.RedactionRule{fieldPath("data.password")},
			want:  "data.password",
		},
		{
			name:  "a rule's additions alone",
			extra: []v1alpha1.RedactionRule{fieldPath("spec.containers[*].env[*].value")},
			want:  "spec.containers[*].env[*].value",
		},
		{
			name:  "the union of both, sorted",
			floor: []v1alpha1.RedactionRule{fieldPath("data.password")},
			extra: []v1alpha1.RedactionRule{fieldPath("spec.containers[*].env[*].value")},
			want:  "data.password\nspec.containers[*].env[*].value",
		},
		{
			name:  "a rule cannot remove the sink's floor by repeating it differently",
			floor: []v1alpha1.RedactionRule{fieldPath("data.password"), fieldPath("data.token")},
			extra: []v1alpha1.RedactionRule{fieldPath("data.password")},
			want:  "data.password\ndata.token",
		},
		{
			name:  "the annotation shorthand expands to a quoted path",
			extra: []v1alpha1.RedactionRule{annotation("my.company.io/api-token")},
			want:  `metadata.annotations["my.company.io/api-token"]`,
		},
		{
			name: "order and duplicates do not matter",
			floor: []v1alpha1.RedactionRule{
				fieldPath("spec.b"), fieldPath("spec.a"), fieldPath("spec.b"),
			},
			extra: []v1alpha1.RedactionRule{fieldPath("spec.a")},
			want:  "spec.a\nspec.b",
		},
		{
			name:  "a rule with neither field set contributes nothing",
			floor: []v1alpha1.RedactionRule{{}},
			extra: []v1alpha1.RedactionRule{fieldPath("data.password")},
			want:  "data.password",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := canonicalRedaction(tc.floor, tc.extra)
			if got != tc.want {
				t.Errorf("canonicalRedaction() = %q, want %q", got, tc.want)
			}
			// Whatever it produced must compile in the data plane: a canonical
			// form the pipeline cannot parse would degrade the rule's targets to
			// streaming nothing, with the failure visible only in a log line.
			if _, err := pipeline.CompileRedaction(splitPaths(got)); err != nil {
				t.Errorf("the canonical form %q does not compile: %v", got, err)
			}
		})
	}
}

// splitPaths is how the data plane splits a canonical redaction string (see
// watch.redactionPaths), restated here so this package's tests exercise the
// round trip rather than assuming it.
func splitPaths(canonical string) []string {
	if canonical == "" {
		return nil
	}
	return strings.Split(canonical, "\n")
}

// TestRedactionPatternsMatchTheDataPlaneParser is the cross-tier contract: the
// CRD's admission patterns and the pipeline's parser must accept exactly the same
// paths.
//
// The two are written in different languages in different packages — a
// kubebuilder marker and a hand-rolled parser — so nothing but this test stops
// them from drifting. Drift in one direction admits a path the data plane cannot
// compile, which silently degrades a rule the API server said was fine; drift in
// the other rejects a path that would have worked.
func TestRedactionPatternsMatchTheDataPlaneParser(t *testing.T) {
	fieldPaths := regexp.MustCompile(v1alpha1.RedactionFieldPathPattern)
	annotations := regexp.MustCompile(v1alpha1.RedactionAnnotationPattern)

	// The contract is one-directional by design: everything the CRD admits must
	// compile, while the reverse is deliberately false for exactly one shape —
	// the bracket-quoted segment, which the data plane understands (it is how the
	// `annotation:` shorthand is transported) but no author may write into a
	// fieldPath, since that would bypass the annotation key's own validation.
	// Both facts are stated per candidate rather than derived, so a change to
	// either side has to be made deliberately here too.
	t.Run("fieldPath", func(t *testing.T) {
		candidates := []struct {
			path         string
			wantAdmitted bool
			wantCompiles bool
		}{
			{path: "data", wantAdmitted: true, wantCompiles: true},
			{path: "data.password", wantAdmitted: true, wantCompiles: true},
			{path: "spec.template.spec.containers[*].env[*].value", wantAdmitted: true, wantCompiles: true},
			{path: "spec.args[*]", wantAdmitted: true, wantCompiles: true},
			{path: "_private.field-name_2", wantAdmitted: true, wantCompiles: true},
			{path: ""},
			{path: ".data"},
			{path: "data."},
			{path: "data..password"},
			{path: "9data"},
			{path: "spec.containers[0].name"},
			{path: "spec.containers[*"},
			{path: "spec.containers[*]x"},
			{path: "spec.my field"},
			{path: "spec.*"},
			{path: "$.spec.name"},
			// Understood by the data plane, never writable by an author.
			{path: `metadata.annotations["a"]`, wantCompiles: true},
		}
		for _, tc := range candidates {
			admitted := tc.path != "" && fieldPaths.MatchString(tc.path)
			if admitted != tc.wantAdmitted {
				t.Errorf("%q: the CRD pattern admits = %t, want %t", tc.path, admitted, tc.wantAdmitted)
			}
			_, err := pipeline.CompileRedaction([]string{tc.path})
			if compiles := err == nil; compiles != tc.wantCompiles {
				t.Errorf("%q: the data plane compiles = %t, want %t", tc.path, compiles, tc.wantCompiles)
			}
			if admitted && err != nil {
				t.Errorf("%q: admitted by the CRD but does not compile: %v", tc.path, err)
			}
		}
	})

	t.Run("annotation", func(t *testing.T) {
		candidates := []string{
			pipeline.LastAppliedConfigAnnotation,
			"my.company.io/api-token",
			"simple",
			"with_underscores.and-dashes",
			`quote"injected`,
			`back\slash`,
			"",
		}
		for _, candidate := range candidates {
			admitted := candidate != "" && annotations.MatchString(candidate)
			_, err := pipeline.CompileRedaction([]string{pipeline.AnnotationRedactionPath(candidate)})
			compiles := err == nil
			if !admitted && compiles {
				// An annotation key the CRD rejects may still render into a
				// *parseable* path (quoting handles almost anything); what must
				// never happen is the reverse — an admitted key that cannot be
				// compiled.
				continue
			}
			if admitted && !compiles {
				t.Errorf("%q: the CRD admits it but the rendered path does not compile: %v", candidate, err)
			}
		}
	})
}

// TestRedactionDoesNotUnlockSecrets pins D8 against the one reading Task 3.3
// invites: "the values are scrubbed now, so surely Secrets can be streamed."
// They cannot. The deny is in code, ahead of every policy, and a sink that
// redacts every field of a Secret still cannot admit one.
func TestRedactionDoesNotUnlockSecrets(t *testing.T) {
	policy := v1alpha1.SinkPolicy{
		AllowedGVKs: []v1alpha1.GVKSelector{{Group: "", Version: "v1", Kinds: []string{"Secret"}}},
		Redaction:   []v1alpha1.RedactionRule{{FieldPath: "data"}, {FieldPath: "stringData"}},
	}
	denial := checkPolicy([]v1alpha1.WatchedResource{resourceEntry("", "Secret")}, policy)
	if denial == nil {
		t.Fatal("a fully-redacted Secret was admitted; v1/Secret is denied in code (D8)")
	}
	if denial.reason != ReasonSecretsDenied {
		t.Errorf("denial reason = %q, want %q", denial.reason, ReasonSecretsDenied)
	}
}

// TestRuleRedactionReachesTheRegistry is the end of the control-plane chain: a
// sink's floor and a rule's additions arrive in the desired-state registry as one
// merged, canonical policy on every target the rule contributes — which is the
// only thing that makes the CRD fields more than documentation.
func TestRuleRedactionReachesTheRegistry(t *testing.T) {
	h := newHarness(t, harnessOptions{allowAll: true})
	sinkName := uniqueName("redactsink")
	h.createReadySink(sinkName, v1alpha1.SinkPolicy{
		Redaction: []v1alpha1.RedactionRule{{FieldPath: "data.password"}},
	})

	namespace := uniqueName("ns")
	h.createNamespace(namespace, nil)

	rule := &v1alpha1.StreamRule{
		ObjectMeta: metav1.ObjectMeta{Name: "redacting", Namespace: namespace},
		Spec: v1alpha1.StreamRuleSpec{
			SinkRef:   sinkName,
			Resources: []v1alpha1.WatchedResource{resourceEntry("", "ConfigMap")},
			ExtraRedaction: []v1alpha1.RedactionRule{
				{Annotation: "my.company.io/api-token"},
			},
		},
	}
	if err := h.Client.Create(context.Background(), rule); err != nil {
		t.Fatalf("create StreamRule: %v", err)
	}
	t.Cleanup(func() { h.deleteIfExists(rule) })

	ruleKey := RuleKey(kindStreamRule, namespace, "redacting")
	h.waitForTargets(ruleKey, []string{fmt.Sprintf("%s@%s", coreGVK("ConfigMap"), namespace)})

	// The sink's floor and the rule's addition, merged and canonical.
	want := "data.password\n" + `metadata.annotations["my.company.io/api-token"]`
	waitFor(t, fmt.Sprintf("rule %q redaction %q", ruleKey, want), func() (bool, string) {
		got := redactionsFor(h, ruleKey)
		return len(got) == 1 && got[0] == want, fmt.Sprintf("%q", got)
	})

	// Removing the rule's addition leaves the sink's floor in force: a rule can
	// add to the floor and take its own addition back, never the floor itself.
	if err := h.Client.Get(context.Background(),
		client.ObjectKeyFromObject(rule), rule); err != nil {
		t.Fatalf("re-read StreamRule: %v", err)
	}
	rule.Spec.ExtraRedaction = nil
	if err := h.Client.Update(context.Background(), rule); err != nil {
		t.Fatalf("update StreamRule: %v", err)
	}
	waitFor(t, fmt.Sprintf("rule %q redaction %q", ruleKey, "data.password"), func() (bool, string) {
		got := redactionsFor(h, ruleKey)
		return len(got) == 1 && got[0] == "data.password", fmt.Sprintf("%q", got)
	})
}

// redactionsFor returns the redaction sets the registry currently holds for every
// target one rule contributes.
func redactionsFor(h *harness, ruleKey string) []string {
	var got []string
	for _, state := range h.Registry.Snapshot() {
		for _, rule := range state.RuleKeys {
			if rule == ruleKey {
				got = append(got, state.Redactions...)
			}
		}
	}
	return got
}
