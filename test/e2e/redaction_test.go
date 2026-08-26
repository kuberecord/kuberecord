//go:build e2e

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

package e2e

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/sink"
)

// The Task 3.3 gate: a value redaction was configured for must not appear
// anywhere in ClickHouse, for an object applied the way a human applies one.
//
// The scenario exercises both halves of the additive design at once — one value
// scrubbed by the *sink's* floor (test/e2e/manifests/sink.yaml) and another by
// the *rule's* own extraRedaction — because "extraRedaction adds to the floor
// rather than replacing it" is the property a real cluster can disprove and a
// unit test cannot.
//
// "The way a human applies one" is the other half of the scenario. `kubectl
// apply` copies the entire submitted object into the last-applied-configuration
// annotation, so an implementation that redacts only the fields it was told
// about still ships a verbatim second copy of both values inside that
// annotation. Each planted value is therefore present twice in the live object,
// by two different routes, and the assertion is that *none* of it survives.

const (
	redactNamespace = "redaction-demo"
	redactRule      = "redaction-configmaps"
	redactConfigMap = "app-config"

	// floorKey is scrubbed by the sink's own policy and ruleKey by the rule's
	// extraRedaction; keptKey is a third key nothing says anything about, which
	// is what proves a scrub is a scrub and not a dropped stream.
	floorKey = "password"
	ruleKey  = "token"
	keptKey  = "log-level"

	// The values that must never reach the sink. They are fixed, distinctive
	// literals so a substring search over a whole row cannot match one by
	// accident.
	plantedBySink = "kuberecord-e2e-planted-sink-secret-do-not-store"
	plantedByRule = "kuberecord-e2e-planted-rule-secret-do-not-store"
	keptValue     = "debug"
)

// rulePath is what the rule adds to the sink's floor, and the reason every
// assertion is about a *value* rather than about the whole `data` map: the key
// survives, its value does not.
const rulePath = "data." + ruleKey

// plantedValues are the two values the scenario searches every row for.
var plantedValues = []string{plantedBySink, plantedByRule}

var _ = Describe("Phase 3 — redaction", Ordered, Serial, func() {
	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		By("dumping the controller-manager log")
		logs, err := kubectl("logs", "-l", operatorPodSelector, "-n", operatorNamespace, "--tail=200")
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to fetch controller logs: %v\n", err)
			return
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
	})

	It("scrubs a configured value, and the copy kubectl leaves in last-applied, from every row", func() {
		By("creating a rule that adds one ConfigMap key to the sink's redaction floor")
		createNamespace(redactNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", redactRule, redactNamespace)
			deleteResourceQuietly("namespace", redactNamespace, "")
		})
		applyYAML(redactingStreamRuleYAML(redactNamespace, redactRule,
			[]ruleResource{{Group: groupCore, Version: "v1", Kind: kindConfigMap}},
			[]redactionEntry{{FieldPath: rulePath}}))
		expectRuleStreaming("streamrule", redactRule, redactNamespace)

		By("asserting the scope opened")
		// A visible Started row means the scope's warm round-trip completed, which
		// is what makes the row assertions below sound rather than lucky.
		eventuallyScopeRows(scopeQuery{
			Group: groupCore, Kind: kindConfigMap, Namespace: redactNamespace,
			Action: string(sink.ScopeActionStarted),
		})

		By("applying a ConfigMap carrying both planted secrets, client-side")
		// Client-side apply on purpose: it is what writes the last-applied
		// annotation, and the annotation is half of what this scenario is about.
		// Every other scenario server-side-applies (see harness.ApplyYAML).
		DeferCleanup(func() { deleteResourceQuietly("configmap", redactConfigMap, redactNamespace) })
		clientSideApplyYAML(configMapYAML(redactNamespace, redactConfigMap, map[string]string{
			floorKey: plantedBySink,
			ruleKey:  plantedByRule,
			keptKey:  keptValue,
		}))

		configMap := objectFilter{
			Group: groupCore, Kind: kindConfigMap,
			Namespace: redactNamespace, Name: redactConfigMap,
		}

		By("asserting the object was streamed with both values replaced by the sentinel")
		Eventually(func(g Gomega) {
			rows, err := resourceRows(withEvent(configMap, creationEvents...))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Data).To(SatisfyAll(
				// Structure preserved: both keys are still there, and so is
				// everything neither policy mentioned.
				ContainSubstring(floorKey),
				ContainSubstring(ruleKey),
				ContainSubstring(pipeline.RedactionSentinel),
				ContainSubstring(keptValue),
			), "the ConfigMap was not recorded with its values scrubbed in place")
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("rotating both secrets and changing an unrelated key")
		clientSideApplyYAML(configMapYAML(redactNamespace, redactConfigMap, map[string]string{
			floorKey: plantedBySink + "-rotated",
			ruleKey:  plantedByRule + "-rotated",
			keptKey:  "info",
		}))

		By("asserting the unrelated change was streamed")
		// Without a row that the pipeline had to diff, the absence assertion below
		// would be trivially satisfied by an operator that recorded nothing at all.
		Eventually(func(g Gomega) {
			rows, err := resourceRows(withEvent(configMap, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).NotTo(BeEmpty())
			g.Expect(rows).To(ContainElement(
				HaveField("Diff", ContainSubstring("info")),
			), "no Modified row describes the unrelated change")
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting no row anywhere contains either planted secret")
		// Every row for the object, every column that can carry object content —
		// including the diff, which is where a redact-on-write-only design leaks,
		// and including the last-applied annotation kubectl embedded in `data`.
		rows, err := resourceRows(configMap)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).NotTo(BeEmpty())
		for _, row := range rows {
			for column, value := range map[string]string{"data": row.Data, "diff": row.Diff} {
				for _, planted := range plantedValues {
					Expect(value).NotTo(ContainSubstring(planted),
						"the planted secret %q leaked into the %s column of a %s row",
						planted, column, row.EventType)
				}
			}
			// The annotation is scrubbed rather than merely absent: an object
			// applied with kubectl always carries it, so its total disappearance
			// would mean the fixture, not the redactor, is doing the work.
			if row.Data != "" {
				Expect(row.Data).To(ContainSubstring(pipeline.LastAppliedConfigAnnotation),
					"the last-applied annotation is missing entirely, so this row proves nothing")
				Expect(lastAppliedValue(row.Data)).To(Equal(pipeline.RedactionSentinel),
					"the last-applied annotation was not scrubbed")
			}
		}
	})
})

// lastAppliedValue extracts the recorded value of the last-applied annotation
// from a row's data column.
//
// It reads the raw JSON rather than unmarshalling because the point is what the
// stored bytes say: the annotation's value is itself JSON, so a scrubbed one is
// the sentinel *string* where a whole embedded object used to be, and comparing
// the extracted scalar is the most direct way to say that.
func lastAppliedValue(data string) string {
	marker := `"` + pipeline.LastAppliedConfigAnnotation + `":`
	_, rest, found := strings.Cut(data, marker)
	if !found || !strings.HasPrefix(rest, `"`) {
		return ""
	}
	value, _, closed := strings.Cut(rest[1:], `"`)
	if !closed {
		return ""
	}
	return value
}
