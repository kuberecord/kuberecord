//go:build chaos

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

package chaos

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/controller"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/test/harness"
)

// The Phase 2 failure-mode gate: one scenario per failure Task 2.1 enumerates,
// each against the real operator, a real kind cluster and a real ClickHouse the
// suite stops and starts, with every assertion made by querying the sink and the
// operator's own metrics.
//
// They run Ordered and Serial because they share one cluster, one sink and one
// backend, and every one of them takes that backend away from the others while it
// runs. The first must run first: it is the only one that can observe a boot, and
// it is what hands the rest of the suite a running ClickHouse. Each keeps to its
// own namespace and its own object names, and undoes what it created through
// DeferCleanup, so a failure in one leaves the next with a clean cluster rather
// than a half-finished one.

// Kinds as kubectl spells them on the command line.
const (
	resourceConfigMap  = "configmap"
	resourceStreamRule = "streamrule"
)

// Per-scenario fixtures. Namespaces are distinct so a query filtered on one can
// never see another scenario's rows, which is what lets exact row counts — the
// strongest assertions here — be exact.
const (
	bootNamespace = "chaos-boot"
	bootRule      = "boot-configmaps"
	bootObjects   = 3

	outageNamespace = "chaos-outage"
	outageRule      = "outage-configmaps"
	outageObjects   = 4

	saturationNamespace = "chaos-saturation"
	saturationRule      = "saturation-configmaps"
	// saturationObjects is three times the sink's 50-job hand-off queue, so the
	// queue is certain to fill even if the single writer drains a batch or two on
	// the way. It is applied in one call; see applyConfigMaps.
	saturationObjects = 150

	poisonNamespace  = "chaos-poison"
	poisonRule       = "poison-configmaps"
	poisonObject     = "oversized"
	poisonNeighbours = 10
	// poisonGuard is a CHECK constraint installed for the duration of one
	// scenario. It is how a row is made *individually* un-insertable while its
	// batch-mates stay perfectly valid — the exact shape poison isolation exists
	// for, and one no ordinary Kubernetes object can produce on its own, since
	// every schema-v1 column is an unbounded String.
	poisonGuard = "chaos_poison_guard"
	// poisonLimit is the constraint's threshold and poisonPayload is comfortably
	// past it, while an ordinary fixture object is three orders of magnitude
	// under.
	poisonLimit   = 65536
	poisonPayload = 262144

	restartNamespace = "chaos-restart"
	restartRule      = "restart-configmaps"
	// goneObject is deleted while the operator is down and never comes back;
	// rebornObject is deleted and re-created while the operator is down, so it
	// comes back as a different object under the same name.
	goneObject   = "gone"
	rebornObject = "reborn"

	// The orphan fixture is the watch_scopes half of the restart scenario: a rule
	// deleted while the operator is down, whose scope nothing will ever close
	// unless boot reconciliation does it.
	orphanNamespace = "chaos-orphan"
	orphanRule      = "orphan-configmaps"
	orphanObject    = "bystander"
)

// ordinaryPayload is what a fixture ConfigMap carries at a given revision. Small
// enough that a hundred of them are nothing to ClickHouse, and containing a
// revision marker so successive applies genuinely change the object's hash rather
// than dedup away.
func ordinaryPayload(rev int) map[string]string {
	return map[string]string{"payload": filler(256), "rev": fmt.Sprintf("%d", rev)}
}

var _ = Describe("Phase 2 chaos scenarios", Ordered, Serial, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			By("dumping the controller-manager log")
			dumpOperatorLog()
		}
		if !clickHouseUp {
			// The backend is the subject of these scenarios, not a dependency of
			// them; if a spec left it down there is nothing to query and saying so
			// is more useful than a connection error stacked on top of the real
			// failure.
			_, _ = fmt.Fprintf(GinkgoWriter,
				"skipping the duplicate-Deleted invariant: ClickHouse is stopped\n")
			return
		}
		// Task 2.1's standing invariant, asserted after every scenario rather than
		// inside one. See harness.ExpectNoDuplicateDeletes.
		By("asserting no object's deletion was recorded twice")
		ch.ExpectNoDuplicateDeletes()
	})

	It("boots against a dead ClickHouse and recovers without duplicating anything", func() {
		By("creating objects that exist before anything watches them")
		harness.CreateNamespace(bootNamespace)
		DeferCleanup(func() {
			harness.DeleteResourceQuietly(resourceStreamRule, bootRule, bootNamespace)
			harness.DeleteResourceQuietly("namespace", bootNamespace, "")
		})
		names := configMapNames("pre", bootObjects)
		applyConfigMaps(bootNamespace, names, ordinaryPayload(0))

		By("installing a rule while the sink's backend is still down")
		// Not applyConfigMapRule: that waits for Ready and for a Started row, and
		// neither can happen yet — the sink is unreachable and the scope log lives
		// in the very database that is down.
		harness.ApplyYAML(fieldManager, harness.StreamRuleYAML(bootNamespace, bootRule, sinkName,
			[]ruleResource{
				{Group: groupCore, Version: "v1", Kind: kindConfigMap},
			}))

		By("asserting the rule went active anyway, and reports only the sink as unhealthy")
		// Invariant 5 and the rule reconciler's one deliberate exception to
		// "degraded means withdrawn": an unreachable sink must not withdraw a rule's
		// targets, because doing so would evict every dedup baseline and write a
		// pair of false scope epochs — and the Stopped row could not even be
		// written, since the sink is the thing that is down. Everything the operator
		// can decide without the backend must still be decided.
		Eventually(func(g Gomega) {
			harness.ExpectCondition(g, resourceStreamRule, bootRule, bootNamespace,
				v1alpha1.ConditionPolicyAllowed, statusTrue)
			harness.ExpectCondition(g, resourceStreamRule, bootRule, bootNamespace,
				v1alpha1.ConditionResourceResolved, statusTrue)
			harness.ExpectCondition(g, resourceStreamRule, bootRule, bootNamespace,
				v1alpha1.ConditionRBACGranted, statusTrue)
			notReady := harness.ExpectCondition(g, resourceStreamRule, bootRule, bootNamespace,
				v1alpha1.ConditionReady, statusFalse)
			g.Expect(notReady.Reason).To(Equal(controller.ReasonSinkNotReady))
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting the scope is watching in Snapshot mode")
		// safe_mode is published from the Snapshot-tagging branch itself, so seeing
		// it at 1 is not a statement about configuration — it is proof that the data
		// plane observed these objects, found no baseline, and refused to call them
		// Added while it could not check the sink's history. That refusal is the
		// whole reason a cold start against an empty cache does not re-announce
		// every object in the cluster as new.
		Eventually(func(g Gomega) {
			value, ok := safeMode(g, bootNamespace)
			g.Expect(ok).To(BeTrue(), "the scope has no safe_mode series yet")
			g.Expect(value).To(Equal(float64(1)))
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("starting ClickHouse")
		startClickHouse()
		expectSinkReady()
		expectRuleStreaming(bootRule, bootNamespace)

		By("asserting the warm-up completed and the scope left Snapshot mode")
		Eventually(func(g Gomega) {
			value, ok := safeMode(g, bootNamespace)
			g.Expect(ok).To(BeTrue())
			g.Expect(value).To(Equal(float64(0)), "the scope is still warming")
		}, recoveryTimeout, pollInterval).Should(Succeed())

		By("asserting each pre-existing object was recorded exactly once, as a Snapshot")
		// The tag is stamped when the event is processed and is never re-stamped, so
		// these rows carry the verdict the pipeline reached while the backend was
		// down even though they only landed once it came back — first from the
		// batch's own retry, and failing that from the per-row isolation phase that
		// follows it, both of which re-send the identical record.
		//
		// Zero Added rows is the criterion in its sharpest form: an Added here would
		// mean the operator had decided these objects were new without ever having
		// been able to check, which at cluster scale is a full re-announcement of
		// every watched object on every cold start.
		for _, name := range names {
			uid := harness.EventuallyUID(resourceConfigMap, name, bootNamespace, ruleReadyTimeout)
			object := configMapFilter(bootNamespace, name, uid)
			row := ch.EventuallyExactlyOneRow(withEvent(object, creationEvents...), recoveryTimeout)
			Expect(row.EventType).To(Equal(eventSnapshot),
				"%s was announced as new by an operator that could not read the sink's history", name)
			Expect(row.Data).NotTo(BeEmpty(), "a Snapshot row must carry the full normalized object")
			Expect(row.SHA256).NotTo(BeEmpty())
		}
		Expect(creationRowCounts(bootNamespace)).To(HaveLen(bootObjects))

		By("asserting a subsequent change is a Modified row carrying a diff")
		// The other half of a completed warm: the baseline the Snapshot row seeded is
		// now trusted, so the next change is expressed as a patch rather than as
		// another full state.
		applyConfigMaps(bootNamespace, names, ordinaryPayload(1))
		for _, name := range names {
			object := configMapFilter(bootNamespace, name, "")
			Eventually(func(g Gomega) {
				rows, err := ch.ResourceRows(withEvent(object, eventModified))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rows).To(ContainElement(SatisfyAll(
					HaveField("Diff", ContainSubstring("/data/rev")),
					HaveField("Data", BeEmpty()),
				)), "no Modified row describes the change to %s", name)
			}, recoveryTimeout, pollInterval).Should(Succeed())
			expectConverged(bootNamespace, name, recoveryTimeout)
		}
	})

	It("survives a mid-stream outage longer than the writer's retry budget", func() {
		By("streaming a set of objects to a healthy sink")
		harness.CreateNamespace(outageNamespace)
		DeferCleanup(func() {
			harness.DeleteResourceQuietly(resourceStreamRule, outageRule, outageNamespace)
			harness.DeleteResourceQuietly("namespace", outageNamespace, "")
		})
		applyConfigMapRule(outageNamespace, outageRule)

		names := configMapNames("churned", outageObjects)
		applyConfigMaps(outageNamespace, names, ordinaryPayload(0))
		for _, name := range names {
			uid := harness.EventuallyUID(resourceConfigMap, name, outageNamespace, ruleReadyTimeout)
			ch.EventuallyExactlyOneRow(withEvent(configMapFilter(outageNamespace, name, uid), eventAdded))
		}

		baseline := failedWrites(Default)

		By("taking ClickHouse away and changing every object")
		stopClickHouse()
		applyConfigMaps(outageNamespace, names, ordinaryPayload(1))

		By("asserting every affected write eventually fails terminally")
		// Terminally, not transiently: the writer retries a batch for its whole
		// budget and then re-attempts each row on its own before giving up, so the
		// first failed commit cannot arrive before that budget has elapsed. A
		// counter that rose sooner would mean the write path had stopped trying
		// early.
		expectCounterToRise(metricWrites, failedSeries(), baseline, outageObjects,
			fmt.Sprintf("no write failed terminally within %s of the outage (budget is %s)",
				outageTimeout, writerRetryBudget))

		By("asserting the failed writes are re-driven rather than abandoned")
		// A second full cycle's worth of failures can only happen if each failure
		// reverted its optimistic cache entry and re-queued its key on the rate
		// limiter — nothing else in the system would re-attempt these objects while
		// they are not changing. This is the "cache reverts + rate-limited re-adds"
		// half of the criterion, observed rather than inferred.
		expectCounterToRise(metricWrites, failedSeries(), baseline, 2*outageObjects,
			"the failed writes were never retried")

		By("asserting nothing was recorded while the sink was unreachable")
		// A failed write must never be mistaken for a persisted one (Invariant 3).
		// The pre-outage Added row is still the only row for each object.
		startClickHouse()
		expectSinkReady()

		By("asserting every affected object converges on exactly one correct latest row")
		for _, name := range names {
			// Modified, not Added: the revert restored the *previously confirmed*
			// baseline rather than clearing the entry, so the recovered write still
			// diffs against the state the sink already holds. An Added here would mean
			// the operator had forgotten what it had already recorded.
			object := configMapFilter(outageNamespace, name, "")
			Eventually(func(g Gomega) {
				rows, err := ch.ResourceRows(withEvent(object, eventModified))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rows).To(ContainElement(
					HaveField("Diff", ContainSubstring("/data/rev"))),
					"the recovered write for %s did not diff against its pre-outage baseline", name)
			}, recoveryTimeout, pollInterval).Should(Succeed())

			// And the "no gaps" criterion itself: what ClickHouse now believes about
			// this object is what the object actually is, recorded once.
			expectConverged(outageNamespace, name, recoveryTimeout)
			ch.ConsistentlyRowCount(withEvent(object, eventAdded), 1)
		}
	})

	It("saturates its hand-off queue under load without deadlocking", func() {
		By("streaming a namespace under a healthy sink")
		harness.CreateNamespace(saturationNamespace)
		DeferCleanup(func() {
			harness.DeleteResourceQuietly(resourceStreamRule, saturationRule, saturationNamespace)
			harness.DeleteResourceQuietly("namespace", saturationNamespace, "")
		})
		applyConfigMapRule(saturationNamespace, saturationRule)

		before := harness.SolePod(Default, operatorPodSelector, operatorNamespace)
		baselineTimeouts := metricSum(Default, metricEnqueueTimeouts, sinkSeries)

		By("stopping ClickHouse and applying sustained load")
		stopClickHouse()
		names := configMapNames("load", saturationObjects)
		applyConfigMaps(saturationNamespace, names, ordinaryPayload(0))
		applyConfigMaps(saturationNamespace, names, ordinaryPayload(1))

		By("asserting the hand-off queue backpressures rather than dropping work")
		// enqueue_timeouts_total rising is the bounded-queue contract doing its job:
		// the queue is full, the hot path waited for room, and the write was refused
		// *back to the caller* — which re-queues the key — rather than dropped
		// silently. Invariant 1 is what forbids the alternative of blocking a worker
		// on the backend indefinitely.
		expectCounterToRise(metricEnqueueTimeouts, sinkSeries, baselineTimeouts, 1,
			"the queue never reported backpressure under a load three times its capacity")

		By("asserting no enqueue blocked longer than the configured timeout")
		// "Behave per config": the sink is configured with enqueueTimeout 2s, so no
		// observation may fall outside the 2.5s histogram bucket. A sample beyond it
		// would mean the hot path had blocked past its own deadline — the failure
		// mode the timeout exists to make impossible.
		Eventually(func(g Gomega) {
			observed := metricSum(g, metricEnqueueBlockCnt, sinkSeries)
			bounded := metricSum(g, metricEnqueueBlockBkt, blockBucket("2.5"))
			g.Expect(observed).To(BeNumerically(">", 0))
			g.Expect(bounded).To(Equal(observed),
				"%v of %v enqueue waits exceeded the configured 2s timeout", observed-bounded, observed)
		}, rowTimeout, pollInterval).Should(Succeed())

		By("asserting the operator neither crashed nor was replaced")
		// A deadlocked worker pool shows up here first: the liveness probe fails, the
		// kubelet restarts the container, and the restart count is the only thing
		// that distinguishes "it recovered" from "it never stalled".
		after := harness.SolePod(Default, operatorPodSelector, operatorNamespace)
		Expect(after.Name).To(Equal(before.Name), "the operator pod was replaced")
		Expect(after.RestartCount).To(BeZero(), "the operator container restarted")
		Expect(after.Ready).To(BeTrue(), "the operator stopped reporting ready")

		By("asserting recovery drains the queue")
		startClickHouse()
		expectSinkReady()
		Eventually(func(g Gomega) {
			g.Expect(metricSum(g, metricQueueDepth, sinkSeries)).To(BeZero())
		}, recoveryTimeout, pollInterval).Should(Succeed())

		By("asserting every object under load was recorded exactly once")
		Eventually(func(g Gomega) {
			counts, err := creationRowCounts(saturationNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(counts).To(HaveLen(saturationObjects))
			for name, count := range counts {
				g.Expect(count).To(Equal(1), "%s was announced as new %d times", name, count)
			}
		}, recoveryTimeout, pollInterval).Should(Succeed())
	})

	It("isolates a poison row without losing its batch-mates or itself", func() {
		By("streaming a namespace under a healthy sink")
		harness.CreateNamespace(poisonNamespace)
		DeferCleanup(func() {
			harness.DeleteResourceQuietly(resourceStreamRule, poisonRule, poisonNamespace)
			harness.DeleteResourceQuietly("namespace", poisonNamespace, "")
		})
		applyConfigMapRule(poisonNamespace, poisonRule)

		By("making one specific record un-insertable")
		Expect(ch.Exec(fmt.Sprintf(
			"ALTER TABLE resource_states ADD CONSTRAINT %s CHECK length(data) < %d",
			poisonGuard, poisonLimit))).To(Succeed())
		constraintInstalled := true
		dropGuard := func() {
			if !constraintInstalled {
				return
			}
			constraintInstalled = false
			Expect(ch.Exec(fmt.Sprintf(
				"ALTER TABLE resource_states DROP CONSTRAINT %s", poisonGuard))).To(Succeed())
		}
		// Registered as well as called below, so a failed assertion in the middle of
		// the scenario cannot leave a constraint behind that would poison every
		// later scenario's writes.
		DeferCleanup(dropGuard)

		baseline := failedWrites(Default)

		By("creating the poison object and its blameless neighbours in one batch")
		// One apply, so the objects reach the informer together and the sink's single
		// writer coalesces them into one batch. That is the condition isolation
		// exists for: without it the poison row would simply fail alone and prove
		// nothing about its neighbours.
		neighbours := configMapNames("neighbour", poisonNeighbours)
		docs := make([]string, 0, poisonNeighbours+1)
		for _, name := range neighbours {
			docs = append(docs, harness.ConfigMapYAML(poisonNamespace, name, ordinaryPayload(0)))
		}
		docs = append(docs, harness.ConfigMapYAML(poisonNamespace, poisonObject,
			map[string]string{"payload": filler(poisonPayload)}))
		harness.ApplyYAML(fieldManager, strings.Join(docs, "---\n"))

		By("asserting the neighbours land despite sharing a doomed batch")
		// The batch fails as a whole first and is only then re-attempted row by row,
		// so this cannot resolve before the writer's retry budget has elapsed. What
		// it proves is the point of Task 0.6's isolation: one bad row does not
		// condemn the rows it happened to travel with.
		Eventually(func(g Gomega) {
			counts, err := creationRowCounts(poisonNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			for _, name := range neighbours {
				g.Expect(counts).To(HaveKeyWithValue(name, 1))
			}
		}, recoveryTimeout, pollInterval).Should(Succeed())

		By("asserting the poison record itself was never recorded")
		poison := configMapFilter(poisonNamespace, poisonObject, "")
		ch.ConsistentlyRowCount(poison, 0)

		By("asserting the poison record keeps being retried, visibly")
		// Exactly the distinction the criterion draws: a record that cannot be
		// written must not be quietly dropped. It commits false, its cache entry is
		// reverted, its key goes back on the rate limiter, and every failed attempt
		// is both counted and logged with the object's identity.
		reached := expectCounterToRise(metricWrites, failedSeries(), baseline, 1,
			"the poison row never settled as a failed write")
		expectCounterToRise(metricWrites, failedSeries(), reached, 1,
			"the poison row was abandoned after its first failure instead of being retried")
		Eventually(func(g Gomega) {
			logs, err := harness.Kubectl("logs", "-l", operatorPodSelector,
				"-n", operatorNamespace, "--tail=500")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring(poisonObject),
				"nothing in the operator's log names the object that cannot be written")
		}, rowTimeout, pollInterval).Should(Succeed())

		By("asserting the retry was live: removing the constraint lets the record land")
		dropGuard()
		ch.EventuallyExactlyOneRow(withEvent(poison, creationEvents...), recoveryTimeout)
		expectConverged(poisonNamespace, poisonObject, recoveryTimeout)
	})

	It("recovers offline deletions and orphaned scopes after being killed mid-flight", func() {
		By("streaming two namespaces, one of whose rules will not survive the outage")
		harness.CreateNamespace(restartNamespace)
		harness.CreateNamespace(orphanNamespace)
		DeferCleanup(func() {
			harness.DeleteResourceQuietly(resourceStreamRule, restartRule, restartNamespace)
			harness.DeleteResourceQuietly(resourceStreamRule, orphanRule, orphanNamespace)
			harness.DeleteResourceQuietly("namespace", restartNamespace, "")
			harness.DeleteResourceQuietly("namespace", orphanNamespace, "")
		})
		applyConfigMapRule(restartNamespace, restartRule)
		applyConfigMapRule(orphanNamespace, orphanRule)

		names := []string{goneObject, rebornObject}
		applyConfigMaps(restartNamespace, names, ordinaryPayload(0))
		applyConfigMaps(orphanNamespace, []string{orphanObject}, ordinaryPayload(0))

		goneUID := harness.EventuallyUID(resourceConfigMap, goneObject, restartNamespace, ruleReadyTimeout)
		oldRebornUID := harness.EventuallyUID(resourceConfigMap, rebornObject, restartNamespace, ruleReadyTimeout)
		orphanUID := harness.EventuallyUID(resourceConfigMap, orphanObject, orphanNamespace, ruleReadyTimeout)

		gone := configMapFilter(restartNamespace, goneObject, goneUID)
		oldReborn := configMapFilter(restartNamespace, rebornObject, oldRebornUID)
		bystander := configMapFilter(orphanNamespace, orphanObject, orphanUID)
		ch.EventuallyExactlyOneRow(withEvent(gone, creationEvents...))
		ch.EventuallyExactlyOneRow(withEvent(oldReborn, creationEvents...))
		ch.EventuallyExactlyOneRow(withEvent(bystander, creationEvents...))

		By("killing the operator with writes in flight")
		// The burst is applied and the kill follows immediately, so the process dies
		// with records between the workqueue and the sink — the state a graceful
		// shutdown drains and a kill does not.
		applyConfigMaps(restartNamespace, names, ordinaryPayload(1))
		killOperator()

		By("deleting an object, replacing another and deleting a rule while nothing watches")
		harness.DeleteResource(resourceConfigMap, goneObject, restartNamespace)
		harness.DeleteResource(resourceConfigMap, rebornObject, restartNamespace)
		applyConfigMaps(restartNamespace, []string{rebornObject}, ordinaryPayload(2))
		newRebornUID := harness.EventuallyUID(resourceConfigMap, rebornObject, restartNamespace, ruleReadyTimeout)
		Expect(newRebornUID).NotTo(Equal(oldRebornUID), "the replacement must be a different object")
		newReborn := configMapFilter(restartNamespace, rebornObject, newRebornUID)
		harness.DeleteResource(resourceStreamRule, orphanRule, orphanNamespace)

		By("bringing the operator back")
		scaleOperator(1)

		// From here the claims are Task 1.11's restart scenario, made verbatim
		// against a process that was killed rather than asked to stop. Nothing about
		// them is weakened by the kill, and that is the point: the recovery path
		// cannot depend on the previous process having had a chance to tidy up.
		By("asserting the offline deletion produced exactly one Deleted row")
		ch.EventuallyExactlyOneRow(withEvent(gone, eventDeleted), restartTimeout)
		ch.ConsistentlyRowCount(withEvent(gone, eventDeleted), 1)

		By("asserting the replacement was recorded under its own identity")
		ch.EventuallyExactlyOneRow(withEvent(newReborn, creationEvents...), restartTimeout)
		ch.ConsistentlyRowCount(withEvent(oldReborn, creationEvents...), 1)

		By("asserting the old incarnation's death was recorded exactly once")
		ch.EventuallyExactlyOneRow(withEvent(oldReborn, eventDeleted), restartTimeout)
		ch.ConsistentlyRowCount(withEvent(oldReborn, eventDeleted), 1)

		By("asserting the successor is still recorded as live after the close-out")
		ch.ConsistentlyRowCount(withEvent(newReborn, eventDeleted), 0)

		By("asserting the scope orphaned by the deleted rule was closed, not emptied")
		// The audit-integrity keystone: a rule that disappears while the operator is
		// down means "we stopped watching", and boot reconciliation says exactly that
		// — one Stopped row, and not a single Deleted row for the objects that were
		// in scope. Getting this wrong turns one rule deletion during a restart into
		// a mass deletion event.
		ch.EventuallyScopeRows(withAction(configMapScope(orphanNamespace),
			sink.ScopeActionStopped), restartTimeout)
		ch.ConsistentlyRowCount(withEvent(bystander, eventDeleted), 0)

		By("asserting the still-wanted scope stayed open across the kill")
		// Boot reconciliation deliberately leaves a scope open when a rule still
		// wants it: the killed process wrote no Stopped row, and inventing one would
		// claim a watch had ended that never did. So the correct state here is an
		// unmatched Started, and the invariant below is about scopes nobody wants.
		Eventually(func(g Gomega) {
			action, found := latestScopeAction(g, restartNamespace)
			g.Expect(found).To(BeTrue(), "the restart namespace has no watch scope recorded")
			g.Expect(action).To(Equal(string(sink.ScopeActionStarted)))
		}, restartTimeout, pollInterval).Should(Succeed())

		By("asserting no watch scope is left open once every rule is gone")
		// "No orphan Started", as a claim about the whole scope log rather than about
		// this scenario's two scopes. Every earlier scenario deleted its rule while
		// the operator was up and its scope should have been closed live; this one's
		// rule is deleted here. Anything still reading Started afterwards is an epoch
		// nobody will ever close, which makes every future zombie GC pass over that
		// scope believe it was being watched when it was not.
		harness.DeleteResource(resourceStreamRule, restartRule, restartNamespace)
		Eventually(func(g Gomega) {
			states, err := scopeStates()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(states).NotTo(BeEmpty())
			for _, state := range states {
				g.Expect(state.Action).To(Equal(string(sink.ScopeActionStopped)),
					"scope %s/%s in namespace %q is still open",
					state.Group, state.Kind, state.Namespace)
			}
		}, restartTimeout, pollInterval).Should(Succeed())
	})
})

// failedSeries pins the writes_total family to this sink's failure outcome.
func failedSeries() map[string]string {
	return map[string]string{"sink": sinkLabel, "outcome": "failed"}
}

// blockBucket pins the enqueue_block_seconds histogram to one cumulative bucket.
func blockBucket(le string) map[string]string {
	return map[string]string{"sink": sinkLabel, "le": le}
}

// latestScopeAction returns the most recent transition recorded for a
// namespace's ConfigMap scope, and whether the scope appears in the log at all.
func latestScopeAction(g Gomega, namespace string) (string, bool) {
	states, err := scopeStates()
	g.Expect(err).NotTo(HaveOccurred())
	for _, state := range states {
		if state.Group == groupCore && state.Kind == kindConfigMap && state.Namespace == namespace {
			return state.Action, true
		}
	}
	return "", false
}
