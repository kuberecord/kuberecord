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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The Task 3.1 gate: Kubernetes Events streamed by naming them in a rule, with
// the two behaviours that make Events different from every other kind proved
// against a real cluster rather than against a fixture.
//
// Both are cases a naive exporter gets wrong, in opposite directions: the
// count-bump update is the one it *drops* (it treats an Event as write-once), and
// the TTL expiry is the one it *invents* (it records the expiry as a deletion).

const (
	eventsNamespace = "events-demo"
	eventsRule      = "events-stream"
	// crasherPod exits immediately and is restarted forever, which is what makes
	// the kubelet emit a BackOff Event and then keep bumping its count in place.
	crasherPod = "crasher"
	// eventsPresetRole is the ClusterRole config/rbac/presets/events.yaml declares.
	// Applied straight from the file it keeps the name in the file, exactly as the
	// networking preset does in the Phase 1 RBAC scenario.
	eventsPresetRole = "watcher-events"
	// backOffReason is the Event the kubelet re-emits — i.e. updates in place —
	// for a container it is backing off from restarting.
	backOffReason = "BackOff"
)

// eventsPreset is the preset that grants `events` in both API groups.
const eventsPreset = "config/rbac/presets/events.yaml"

// eventChurnTimeout bounds the wait for a crash-looping container to produce a
// *second* count on its BackOff Event. The kubelet's restart backoff is
// exponential (10s, 20s, 40s, …), and each step is one bump, so two distinct
// counts arrive in well under a minute on an idle node — this is that with room
// for a loaded CI machine and a cold image cache.
const eventChurnTimeout = 5 * time.Minute

var _ = Describe("Phase 3 — Kubernetes Events ingestion", Ordered, Serial, func() {
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

	It("persists a crash-looping pod's BackOff Event under both Event GVKs, and never records its expiry", func() {
		By("granting Events watch rights and creating a rule naming both Event GVKs")
		applyFile(eventsPreset)
		createNamespace(eventsNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", eventsRule, eventsNamespace)
			deleteResourceQuietly("namespace", eventsNamespace, "")
			deleteResourceQuietly("clusterrole", eventsPresetRole, "")
		})
		// One rule, both spellings. They are two resources over one storage, so
		// every assertion below runs twice against the same underlying Event — which
		// is the only honest way to claim "support whichever the rule names".
		applyYAML(streamRuleYAML(eventsNamespace, eventsRule, []ruleResource{
			{Group: groupCore, Version: "v1", Kind: kindEvent},
			{Group: groupEvents, Version: "v1", Kind: kindEvent},
		}))
		expectRuleStreaming("streamrule", eventsRule, eventsNamespace)

		By("asserting both Event scopes opened")
		// Scope epochs are recorded for an Events scope exactly as for any other
		// kind, and this is also the barrier that makes the row assertions below
		// sound rather than lucky: a visible Started row means the scope's warm
		// round-trip has completed.
		for _, group := range []string{groupCore, groupEvents} {
			eventuallyScopeRows(scopeQuery{
				Group: group, Kind: kindEvent, Namespace: eventsNamespace, Action: string(sink.ScopeActionStarted),
			})
		}

		By("creating a pod that crash-loops")
		applyYAML(crashLoopPodYAML(eventsNamespace, crasherPod))
		DeferCleanup(func() { deleteResourceQuietly("pod", crasherPod, eventsNamespace) })

		By("waiting for the kubelet's BackOff Event")
		eventName := eventuallyBackOffEventName(eventsNamespace, crasherPod)
		eventUID := eventuallyUID("event", eventName, eventsNamespace)
		_, _ = fmt.Fprintf(GinkgoWriter, "BackOff event: %s (uid %s)\n", eventName, eventUID)

		filters := map[string]objectFilter{}
		for _, group := range []string{groupCore, groupEvents} {
			filters[group] = objectFilter{
				Group: group, Kind: kindEvent,
				Namespace: eventsNamespace, Name: eventName, UID: eventUID,
			}
		}

		By("asserting the count bump lands as rows with a rising count and no diffs")
		for group, filter := range filters {
			// Two rows carrying two different counts is the whole claim: an exporter
			// that treats an Event as write-once records the first and drops every
			// bump after it.
			var counts []int64
			Eventually(func(g Gomega) {
				rows, err := resourceRows(filter)
				g.Expect(err).NotTo(HaveOccurred())
				counts = eventCounts(g, rows)
				g.Expect(counts).To(HaveLen(len(rows)))
				g.Expect(len(distinct(counts))).To(BeNumerically(">=", 2),
					"the Event's count never changed across %d rows: %v", len(rows), counts)
			}, eventChurnTimeout, pollInterval).Should(Succeed(),
				"no rising count for api_group=%q", group)

			rows, err := resourceRows(filter)
			Expect(err).NotTo(HaveOccurred())

			// Monotonic, not merely different: a count that ever went *down* would
			// mean rows are being written out of order, which would make every
			// "how often did this happen" query wrong.
			for i := 1; i < len(counts); i++ {
				Expect(counts[i]).To(BeNumerically(">=", counts[i-1]),
					"count went backwards between rows %d and %d: %v", i-1, i, counts)
			}
			Expect(counts[len(counts)-1]).To(BeNumerically(">", counts[0]))

			// Every Event row is self-contained: full state, never a diff. That is
			// what lets docs/QUERIES.md read a count or a message straight off a row
			// instead of replaying a patch chain.
			for i, row := range rows {
				Expect(row.Data).NotTo(BeEmpty(), "row %d of api_group=%q carries no data", i, group)
				Expect(row.Diff).To(BeEmpty(), "row %d of api_group=%q carries a diff; Events are never diffed", i, group)
				Expect(row.SHA256).NotTo(BeEmpty())
			}
			// The first row is the Event's first sighting; the rest are ordinary
			// Modifieds. No Checkpoint can appear, because there is no diff run to
			// interrupt.
			Expect(rows[0].EventType).To(BeElementOf(creationEvents))
			for _, row := range rows[1:] {
				Expect(row.EventType).To(Equal(eventModified))
			}
		}

		By("deleting the pod so the Event stops changing")
		// The "unchanged row count" claim below needs a count that has stopped
		// moving, and a crash-looping pod bumps its BackOff Event indefinitely.
		// Removing the pod is what closes that window; the Event itself outlives it
		// (Events are not owned by their subject — only their TTL removes them).
		deleteResource("pod", crasherPod, eventsNamespace)
		rowsPerGroup := map[string]int{}
		for group, filter := range filters {
			rowsPerGroup[group] = stableRowCount(filter)
			_, _ = fmt.Fprintf(GinkgoWriter, "api_group=%q settled at %d rows\n", group, rowsPerGroup[group])
		}

		By("deleting the Event, standing in for its TTL expiring")
		// A forced delete delivers the operator the same watch event a TTL expiry
		// does — the API server does not distinguish them — and it is the only way
		// to make the ~1h expiry testable inside a suite.
		deleteResource("event", eventName, eventsNamespace)
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "event", eventName, "-n", eventsNamespace, "--ignore-not-found")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "the Event is still in the API server")
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting the expiry produced no Deleted row and no new rows at all")
		for group, filter := range filters {
			consistentlyRowCount(withEvent(filter, eventDeleted), 0)
			// Not merely "no Deleted": the row count for this UID must be *unchanged*,
			// so the expiry did not produce a row of any other kind either.
			consistentlyRowCount(filter, rowsPerGroup[group])
		}
	})
})

// stableRowCount returns an object's row count once it has stopped moving, and
// is what makes the "unchanged after the expiry" assertion a real one: comparing
// against a count read while rows were still arriving would fail for the ordinary
// reason (more rows) rather than the interesting one (a fabricated deletion).
//
// "Stopped moving" is two consecutive polls agreeing, which given a poll interval
// comfortably longer than one batch flush is enough once the thing producing the
// rows has been removed.
func stableRowCount(filter objectFilter) int {
	GinkgoHelper()
	var count int
	Eventually(func(g Gomega) {
		rows, err := resourceRows(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).NotTo(BeEmpty())
		previous := count
		count = len(rows)
		g.Expect(count).To(Equal(previous), "the row count is still climbing (%d -> %d)", previous, count)
	}).Should(Succeed())
	return count
}

// eventuallyBackOffEventName waits for the kubelet to emit a BackOff Event for
// pod and returns its name.
//
// The field selector is what makes the choice unambiguous: a crash-looping pod
// also produces Scheduled, Pulled, Created, Started and Failed Events, and only
// BackOff is the one the kubelet keeps updating in place.
func eventuallyBackOffEventName(namespace, pod string) string {
	GinkgoHelper()
	var name string
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "events", "-n", namespace,
			"--field-selector", "involvedObject.name="+pod+",reason="+backOffReason,
			"-o", "jsonpath={.items[0].metadata.name}")
		g.Expect(err).NotTo(HaveOccurred())
		name = strings.TrimSpace(out)
		g.Expect(name).NotTo(BeEmpty(), "no %s Event for pod %s yet", backOffReason, pod)
	}, eventChurnTimeout, pollInterval).Should(Succeed())
	return name
}

// eventCounts reads the occurrence count out of each row's stored Event, in row
// order.
//
// The field is looked up by preference rather than by group, because the two
// Event APIs spell the same number differently and the mapping is the API
// server's business, not this suite's: core `v1` carries `count`, while
// `events.k8s.io/v1` renders the same legacy Event's count as `deprecatedCount`
// and only populates `series.count` for an Event that was authored with a series.
// Trying each in turn keeps the assertion about the number rather than about
// which conversion the cluster under test happens to apply.
func eventCounts(g Gomega, rows []resourceRow) []int64 {
	counts := make([]int64, 0, len(rows))
	for i, row := range rows {
		var event struct {
			Count           int64 `json:"count"`
			DeprecatedCount int64 `json:"deprecatedCount"`
			Series          *struct {
				Count int64 `json:"count"`
			} `json:"series"`
		}
		g.Expect(json.Unmarshal([]byte(row.Data), &event)).To(Succeed(),
			"row %d does not hold a JSON Event: %s", i, row.Data)

		switch {
		case event.Count > 0:
			counts = append(counts, event.Count)
		case event.DeprecatedCount > 0:
			counts = append(counts, event.DeprecatedCount)
		case event.Series != nil:
			counts = append(counts, event.Series.Count)
		default:
			// An Event that has fired exactly once may carry no count at all in the
			// events.k8s.io rendering; treat that as one occurrence rather than
			// failing, so the first row never breaks the monotonicity check.
			counts = append(counts, 1)
		}
	}
	return counts
}

// distinct returns the unique values of counts, preserving first-seen order. It
// exists so "the count actually changed" is a claim about values rather than
// about how many rows happened to be written.
func distinct(counts []int64) []int64 {
	seen := make(map[int64]struct{}, len(counts))
	out := make([]int64, 0, len(counts))
	for _, c := range counts {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}
