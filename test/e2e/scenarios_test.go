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

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/controller"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// The Phase 1 gate: the five scenarios of Task 1.11, each against the real
// operator, a real kind cluster and a real ClickHouse, with every assertion
// made by querying the sink rather than by reading the operator's own state.
//
// They run Ordered and Serial because they share one cluster and one sink, and
// two of them mutate cluster-wide state the others depend on — the RBAC
// scenario grants and revokes a preset, the restart scenario takes the operator
// down. Each keeps to its own namespace and its own object names, and undoes
// what it created through DeferCleanup, so a failure in one leaves the next
// with a clean cluster rather than a half-finished one.

// Per-scenario fixtures. Namespaces are distinct so a query filtered on one can
// never see another scenario's rows, which is what lets exact row counts — the
// strongest assertions here — be exact.
const (
	happyNamespace  = "demo"
	happyRule       = "demo-deployments"
	happyDeployment = "web"

	churnNamespace  = "churn"
	churnRule       = "churn-deployments"
	churnDeployment = "survivor"

	rbacNamespace = "rbac-demo"
	rbacRule      = "rbac-ingresses"
	rbacIngress   = "audited"
	// rbacHealthyRule and rbacHealthyDeployment are the control group: a second
	// rule in the same namespace, for a kind the default preset does grant. It is
	// what turns "the Ingress rule degraded" into the claim Invariant 5 actually
	// makes — that it degraded *alone*.
	rbacHealthyRule       = "rbac-deployments"
	rbacHealthyDeployment = "unaffected"
	// networkingPresetRole is the ClusterRole config/rbac/presets/networking.yaml
	// declares. Applied straight from the file with kubectl it keeps the name in
	// the file (no kustomize prefix), which is exactly the "grant a GVK in 30
	// seconds" path docs/RBAC.md documents.
	networkingPresetRole = "watcher-networking"

	restartNamespace = "restart"
	restartRule      = "restart-deployments"
	// goneDeployment is deleted while the operator is down and never comes back;
	// rebornDeployment is deleted and re-created while the operator is down, so
	// it comes back as a different object under the same name.
	goneDeployment   = "gone"
	rebornDeployment = "reborn"

	nodeRule            = "node-inventory"
	nodeWatcherRole     = "kubestream-e2e-watcher-nodes"
	nodeDeniedNamespace = "node-denied"
	nodeDeniedRule      = "namespaced-nodes"
	nodeLabelKey        = "kubestream.io/e2e"
	nodeLabelValue      = "cluster-scoped-scenario"
)

var _ = Describe("Phase 1 acceptance scenarios", Ordered, Serial, func() {
	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		// The operator's log is the only place a degraded rule explains itself in
		// full; the conditions carry a summary, the log carries the cause.
		By("dumping the controller-manager log")
		logs, err := kubectl("logs", "-l", operatorPodSelector, "-n", operatorNamespace, "--tail=200")
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to fetch controller logs: %v\n", err)
			return
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
	})

	It("streams a Deployment's create, scale and delete to ClickHouse", func() {
		By("creating the demo namespace and a StreamRule for apps/v1 Deployments")
		createNamespace(happyNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", happyRule, happyNamespace)
			deleteResourceQuietly("namespace", happyNamespace, "")
		})
		applyYAML(streamRuleYAML(happyNamespace, happyRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", happyRule, happyNamespace)

		By("asserting the watch scope opened with a Started row naming this rule")
		// This is also the barrier that makes the Added assertion below sound, and
		// not merely lucky. An object observed before its scope has warmed from the
		// sink's history is tagged Snapshot rather than Added (see creationEvents).
		// A visible Started row means the scope's ClickHouse round-trip has already
		// completed, so the warm — a single query issued at the same moment — has
		// too, and a cache miss from here on genuinely means "new".
		started := eventuallyScopeRows(scopeQuery{
			Group: groupApps, Kind: kindDeployment, Namespace: happyNamespace,
			Action: string(sink.ScopeActionStarted),
		})
		Expect(started[0].RuleRef).To(Equal(streamRuleKey(happyNamespace, happyRule)))

		By("creating the Deployment")
		applyYAML(deploymentYAML(happyNamespace, happyDeployment, 1))
		uid := eventuallyUID("deployment", happyDeployment, happyNamespace)
		object := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: happyNamespace, Name: happyDeployment, UID: uid,
		}

		By("asserting one Added row carrying the full object, its hash and its actors")
		added := eventuallyExactlyOneRow(withEvent(object, eventAdded))
		Expect(added.Data).NotTo(BeEmpty(), "an Added row must carry the full normalized object")
		Expect(added.SHA256).NotTo(BeEmpty())
		Expect(added.Diff).To(BeEmpty(), "an Added row has nothing to diff against")
		Expect(added.Actors).To(ContainElement(fieldManager))
		Expect(added.APIVersion).To(Equal("v1"))
		Expect(added.Labels).To(HaveKeyWithValue("app", happyDeployment))

		By("scaling the Deployment")
		applyYAML(deploymentYAML(happyNamespace, happyDeployment, 2))

		By("asserting a Modified row whose diff is the replica change")
		Eventually(func(g Gomega) {
			rows, err := resourceRows(withEvent(object, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(ContainElement(SatisfyAll(
				HaveField("Diff", ContainSubstring("/spec/replicas")),
				HaveField("Data", BeEmpty()),
				HaveField("Actors", ContainElement(fieldManager)),
			)), "no Modified row describes the scale")
		}).Should(Succeed())

		By("deleting the Deployment")
		deleteResource("deployment", happyDeployment, happyNamespace)

		By("asserting exactly one Deleted row, carrying no payload at all")
		deleted := eventuallyExactlyOneRow(withEvent(object, eventDeleted))
		Expect(deleted.Data).To(BeEmpty())
		Expect(deleted.Diff).To(BeEmpty())
		Expect(deleted.SHA256).To(BeEmpty(), "a deleted object has no content to hash")
		consistentlyRowCount(withEvent(object, eventDeleted), 1)
	})

	It("closes and reopens a watch scope without inventing deletions", func() {
		By("creating the churn namespace, a rule and a Deployment that outlives it")
		createNamespace(churnNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", churnRule, churnNamespace)
			deleteResourceQuietly("namespace", churnNamespace, "")
		})
		applyYAML(streamRuleYAML(churnNamespace, churnRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", churnRule, churnNamespace)
		churnScope := scopeQuery{Group: groupApps, Kind: kindDeployment, Namespace: churnNamespace}
		eventuallyScopeRows(withAction(churnScope, sink.ScopeActionStarted))

		applyYAML(deploymentYAML(churnNamespace, churnDeployment, 1))
		uid := eventuallyUID("deployment", churnDeployment, churnNamespace)
		object := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: churnNamespace, Name: churnDeployment, UID: uid,
		}
		// Added, not merely a creation row: the Started barrier above means the
		// scope was warm before this object existed, so its first sighting is
		// unambiguously new. That is what makes the count this scenario carries
		// across the churn a meaningful baseline.
		eventuallyExactlyOneRow(withEvent(object, eventAdded))

		By("deleting the rule while the Deployment stays alive")
		deleteResource("streamrule", churnRule, churnNamespace)

		By("asserting a Stopped row appears")
		stopped := eventuallyScopeRows(withAction(churnScope, sink.ScopeActionStopped))
		Expect(stopped[0].RuleRef).To(Equal(streamRuleKey(churnNamespace, churnRule)))

		By("asserting no Deleted row was written for the still-live Deployment")
		// The whole point of scope epochs: "we stopped watching" and "it was
		// deleted" are different truths, and only the first one happened here.
		consistentlyRowCount(withEvent(object, eventDeleted), 0)

		By("re-creating the same rule")
		applyYAML(streamRuleYAML(churnNamespace, churnRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", churnRule, churnNamespace)

		By("asserting the scope reopened with a second Started row")
		Eventually(func(g Gomega) {
			rows, err := scopeRows(withAction(churnScope, sink.ScopeActionStarted))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(len(rows)).To(BeNumerically(">=", 2), "the reopened scope wrote no new Started row")
		}).Should(Succeed())

		By("asserting the re-warm did not re-announce the object as newly Added")
		// This is the criterion the acceptance list actually states: no *duplicate
		// Added flood*, by either of the two mechanisms that prevent one. Where the
		// new epoch finishes seeding from the sink's history first, the object
		// dedups against its own baseline and no row is written at all; where the
		// informer's initial list wins that race, the sighting is written once as
		// Snapshot instead. Both are correct, and the operator picks between them
		// on timing alone — so pinning the outcome to one of them would make this
		// scenario flaky while testing nothing extra.
		//
		// A second Added, by contrast, is never correct: at cluster scale it is
		// every object in scope re-announced as new on every rule edit.
		consistentlyRowCount(withEvent(object, eventAdded), 1)
	})

	It("degrades only the rule that lacks RBAC, and recovers it without a restart", func() {
		By("recording which operator pod is serving")
		var before operatorPodInfo
		Eventually(func(g Gomega) { before = theOperatorPod(g) }).Should(Succeed())

		By("creating a healthy rule alongside one for a kind no installed preset grants")
		createNamespace(rbacNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("ingress", rbacIngress, rbacNamespace)
			deleteResourceQuietly("streamrule", rbacRule, rbacNamespace)
			deleteResourceQuietly("streamrule", rbacHealthyRule, rbacNamespace)
			deleteResourceQuietly("namespace", rbacNamespace, "")
			deleteResourceQuietly("clusterrole", networkingPresetRole, "")
		})
		applyYAML(streamRuleYAML(rbacNamespace, rbacHealthyRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", rbacHealthyRule, rbacNamespace)

		applyYAML(streamRuleYAML(rbacNamespace, rbacRule, []ruleResource{
			{Group: groupNetworking, Version: "v1", Kind: kindIngress},
		}))

		By("asserting the rule reports the missing grant, and only that")
		Eventually(func(g Gomega) {
			denied := expectCondition(g, "streamrule", rbacRule, rbacNamespace,
				v1alpha1.ConditionRBACGranted, statusFalse)
			g.Expect(denied.Reason).To(Equal(controller.ReasonMissingPermissions))
			g.Expect(denied.Message).To(ContainSubstring("ingresses"), "the message must name the resource")
			// The sink admits Ingress and the kind resolves, so the two gates on
			// either side of RBAC must still be green: a rule that degraded for a
			// reason nobody can act on is worse than no condition at all.
			expectCondition(g, "streamrule", rbacRule, rbacNamespace, v1alpha1.ConditionPolicyAllowed, statusTrue)
			expectCondition(g, "streamrule", rbacRule, rbacNamespace, v1alpha1.ConditionResourceResolved, statusTrue)
			expectCondition(g, "streamrule", rbacRule, rbacNamespace, v1alpha1.ConditionReady, statusFalse)
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting the other rule kept streaming right through the degrade")
		// Invariant 5: one bad rule degrades itself and nothing else. Proven by
		// behaviour, not just by a condition — an object created *while* the
		// Ingress rule is parked still reaches the sink.
		applyYAML(deploymentYAML(rbacNamespace, rbacHealthyDeployment, 1))
		healthyUID := eventuallyUID("deployment", rbacHealthyDeployment, rbacNamespace)
		eventuallyExactlyOneRow(withEvent(objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: rbacNamespace, Name: rbacHealthyDeployment, UID: healthyUID,
		}, creationEvents...))
		Eventually(func(g Gomega) {
			expectCondition(g, "streamrule", rbacHealthyRule, rbacNamespace, v1alpha1.ConditionReady, statusTrue)
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("applying the networking preset")
		applyFile(networkingPreset)

		By("asserting the rule heals on its own within one resync")
		Eventually(func(g Gomega) {
			granted := expectCondition(g, "streamrule", rbacRule, rbacNamespace,
				v1alpha1.ConditionRBACGranted, statusTrue)
			g.Expect(granted.Reason).To(Equal(controller.ReasonAllVerbsGranted))
			expectCondition(g, "streamrule", rbacRule, rbacNamespace, v1alpha1.ConditionReady, statusTrue)
		}, rbacRecoveryTimeout, pollInterval).Should(Succeed())

		By("asserting rows now flow for the newly granted kind")
		applyYAML(ingressYAML(rbacNamespace, rbacIngress))
		uid := eventuallyUID("ingress", rbacIngress, rbacNamespace)
		eventuallyExactlyOneRow(withEvent(objectFilter{
			Group: groupNetworking, Kind: kindIngress,
			Namespace: rbacNamespace, Name: rbacIngress, UID: uid,
		}, creationEvents...))

		By("asserting the operator neither restarted nor was replaced")
		after := theOperatorPod(Default)
		Expect(after.Name).To(Equal(before.Name), "the operator pod was replaced")
		Expect(after.RestartCount).To(BeZero(), "the operator container restarted")
	})

	It("recovers offline deletions and reincarnations after a restart", func() {
		By("creating two Deployments under a watching rule")
		createNamespace(restartNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", restartRule, restartNamespace)
			deleteResourceQuietly("namespace", restartNamespace, "")
		})
		applyYAML(streamRuleYAML(restartNamespace, restartRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", restartRule, restartNamespace)

		applyYAML(deploymentYAML(restartNamespace, goneDeployment, 1))
		applyYAML(deploymentYAML(restartNamespace, rebornDeployment, 1))
		goneUID := eventuallyUID("deployment", goneDeployment, restartNamespace)
		oldRebornUID := eventuallyUID("deployment", rebornDeployment, restartNamespace)

		gone := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: restartNamespace, Name: goneDeployment, UID: goneUID,
		}
		oldReborn := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: restartNamespace, Name: rebornDeployment, UID: oldRebornUID,
		}
		eventuallyExactlyOneRow(withEvent(gone, creationEvents...))
		eventuallyExactlyOneRow(withEvent(oldReborn, creationEvents...))

		By("taking the operator down")
		scaleOperator(0)
		DeferCleanup(func() {
			// If an assertion below fails mid-outage, the suite must not leave the
			// operator scaled to zero for every later spec.
			scaleOperator(1)
		})

		By("deleting one Deployment and replacing another while nothing is watching")
		deleteResource("deployment", goneDeployment, restartNamespace)
		deleteResource("deployment", rebornDeployment, restartNamespace)
		applyYAML(deploymentYAML(restartNamespace, rebornDeployment, 1))
		newRebornUID := eventuallyUID("deployment", rebornDeployment, restartNamespace)
		Expect(newRebornUID).NotTo(Equal(oldRebornUID), "the replacement must be a different object")
		newReborn := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: restartNamespace, Name: rebornDeployment, UID: newRebornUID,
		}

		By("bringing the operator back")
		scaleOperator(1)

		By("asserting the offline deletion produced exactly one Deleted row")
		eventuallyExactlyOneRow(withEvent(gone, eventDeleted), restartTimeout)
		consistentlyRowCount(withEvent(gone, eventDeleted), 1)

		By("asserting the replacement was recorded under its own identity")
		eventuallyExactlyOneRow(withEvent(newReborn, creationEvents...), restartTimeout)
		// The successor's rows attach to the successor's UID; the old incarnation's
		// history is not rewritten by the object that took its name.
		consistentlyRowCount(withEvent(oldReborn, creationEvents...), 1)

		By("asserting the old incarnation's death was recorded exactly once")
		// The reincarnation close-out. Nothing in the live pipeline can produce it
		// here: the successor is listed by the informer before the warm-up finishes
		// (warm has to dial ClickHouse, the informer only has to reach the API
		// server), so the dedup cache never holds the old UID and the zombie GC's
		// UID-gated claim is correctly refused — that refusal is what stops a live
		// object being deleted by name alone. The evidence survives only in the
		// sink's own history, and the warm-up recovers it from there (Task 1.12):
		// either as two incarnations of one name where the older has no Deleted
		// row, or — the ordering this scenario usually produces, where the warm's
		// history read beats the successor's own first row to ClickHouse — from the
		// refused claim, once history has caught up enough to date the row from.
		//
		// The row is dated from history, not from the recovery, so a reconstruction
		// reads the old incarnation's death *before* the successor's first row — and
		// so a re-emitted close-out is byte-identical and collapses on merge, which
		// is what the duplicate check below is really testing.
		eventuallyExactlyOneRow(withEvent(oldReborn, eventDeleted), restartTimeout)
		consistentlyRowCount(withEvent(oldReborn, eventDeleted), 1)

		By("asserting the successor is still recorded as live after the close-out")
		// The close-out must not bury its successor: dated from history it sorts
		// before the successor's rows, so the identity's most recent event is still
		// the successor's own — which is what keeps a later warm-up seeding it.
		consistentlyRowCount(withEvent(newReborn, eventDeleted), 0)
	})

	It("streams a cluster-scoped kind only for the rule type allowed to name it", func() {
		By("granting Node watch rights and creating a ClusterStreamRule")
		applyFile(nodeWatcherManifest)
		DeferCleanup(func() {
			deleteResourceQuietly("clusterstreamrule", nodeRule, "")
			deleteResourceQuietly("clusterrole", nodeWatcherRole, "")
		})
		applyYAML(clusterStreamRuleYAML(nodeRule, []ruleResource{
			{Group: groupCore, Version: "v1", Kind: kindNode},
		}))
		expectRuleStreaming("clusterstreamrule", nodeRule, "")

		nodeName := someNodeName()
		// A cluster-scoped object's rows carry an empty namespace, which is a real
		// value here and not a wildcard — see objectFilter.
		node := objectFilter{Group: groupCore, Kind: kindNode, Namespace: "", Name: nodeName}

		By("asserting the node reached the sink")
		// The event type of this first row is deliberately not asserted. The node
		// already existed when the rule was created, so whether the informer's
		// initial list is processed before or after the scope finishes warming
		// from the sink's history decides between Added and Snapshot — a race the
		// design resolves in favour of Snapshot precisely because over-reporting
		// Added at scale is the harmful direction (see Pipeline.MarkScopeWarm).
		// What the scenario is about is the update below, which is unambiguous.
		eventuallyAnyRows(node)

		By("labelling the node")
		DeferCleanup(func() {
			if out, err := kubectl("label", "node", nodeName, nodeLabelKey+"-"); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: unlabelling node: %v\n%s", err, out)
			}
		})
		out, err := kubectl("label", "node", nodeName, nodeLabelKey+"="+nodeLabelValue, "--overwrite")
		Expect(err).NotTo(HaveOccurred(), "failed to label the node: %s", out)

		By("asserting the node update was streamed")
		Eventually(func(g Gomega) {
			rows, err := resourceRows(withEvent(node, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(ContainElement(SatisfyAll(
				HaveField("Diff", ContainSubstring(nodeLabelValue)),
				HaveField("Labels", HaveKeyWithValue(nodeLabelKey, nodeLabelValue)),
			)), "no Modified row describes the new node label")
		}).Should(Succeed())

		By("asserting a namespaced rule may not name the same kind")
		createNamespace(nodeDeniedNamespace)
		DeferCleanup(func() {
			deleteResourceQuietly("streamrule", nodeDeniedRule, nodeDeniedNamespace)
			deleteResourceQuietly("namespace", nodeDeniedNamespace, "")
		})
		applyYAML(streamRuleYAML(nodeDeniedNamespace, nodeDeniedRule, []ruleResource{
			{Group: groupCore, Version: "v1", Kind: kindNode},
		}))
		Eventually(func(g Gomega) {
			unresolved := expectCondition(g, "streamrule", nodeDeniedRule, nodeDeniedNamespace,
				v1alpha1.ConditionResourceResolved, statusFalse)
			g.Expect(unresolved.Reason).To(Equal(controller.ReasonKindsUnresolved))
			// The message has to say what is wrong and what to do instead —
			// ResolveForScope's verdict is permanent until the rule is edited, so
			// it is the only thing the author has to go on.
			g.Expect(unresolved.Message).To(SatisfyAll(
				ContainSubstring("cluster-scoped"),
				ContainSubstring("ClusterStreamRule"),
			))
			expectCondition(g, "streamrule", nodeDeniedRule, nodeDeniedNamespace,
				v1alpha1.ConditionReady, statusFalse)
		}, ruleReadyTimeout, pollInterval).Should(Succeed())
	})
})

// streamRuleKey renders the rule_ref a watch_scopes row carries for a namespaced
// StreamRule.
//
// It goes through controller.RuleKey rather than spelling the format out, so the
// column's contract is asserted against the one function that produces it — the
// reference is "<kind>/<namespace>/<name>" (documented in docs/SCHEMA.md), and a
// change to that format has to break this test rather than silently rewrite what
// every existing audit query matches on.
func streamRuleKey(namespace, name string) string {
	return controller.RuleKey("streamrule", namespace, name)
}

// withEvent narrows a filter to the given event types. Filters are values, so
// this returns a copy and the caller's object filter stays reusable for the next
// question it asks.
func withEvent(filter objectFilter, eventTypes ...string) objectFilter {
	filter.EventTypes = eventTypes
	return filter
}

// withAction narrows a scope query to one transition. Like withEvent it copies,
// so one scopeQuery value describes a scope and is then asked about both edges.
func withAction(query scopeQuery, action sink.ScopeAction) scopeQuery {
	query.Action = string(action)
	return query
}

// expectRuleStreaming waits until a rule is fully realised: policy-admitted,
// kind-resolved, RBAC-granted and installed in the registry. Every scenario
// starts from this state, so asserting it here means a later failure is about
// the behaviour under test rather than about a rule that never started.
func expectRuleStreaming(kind, name, namespace string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		ready := expectCondition(g, kind, name, namespace, v1alpha1.ConditionReady, statusTrue)
		g.Expect(ready.Reason).To(Equal(controller.ReasonStreaming))
	}, ruleReadyTimeout, pollInterval).Should(Succeed())
}

// scaleOperator takes the manager Deployment to replicas and waits for reality
// to match: no pods left at zero, an available Deployment at one.
//
// Scaling rather than deleting the pod is what gives the restart scenario a
// window it controls. A deleted pod is replaced immediately, so "while the
// operator is down" would be a race against the ReplicaSet rather than a state
// the test can act in.
func scaleOperator(replicas int) {
	GinkgoHelper()
	out, err := kubectl("scale", "deployment/"+operatorDeployment, "-n", operatorNamespace,
		fmt.Sprintf("--replicas=%d", replicas))
	Expect(err).NotTo(HaveOccurred(), "failed to scale the operator: %s", out)

	if replicas == 0 {
		// Every pod, terminating ones included: a pod inside its termination grace
		// period is still watching, and proceeding then would delete the scenario's
		// objects in front of a live operator — turning the offline-deletion test
		// into an ordinary online one without failing.
		Eventually(func(g Gomega) {
			pods, err := operatorPods()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(BeEmpty(), "the operator is still running")
		}, restartTimeout, pollInterval).Should(Succeed())
		return
	}

	out, err = kubectl("wait", "deployment/"+operatorDeployment, "-n", operatorNamespace,
		"--for=condition=Available", "--timeout="+restartTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "the operator never came back: %s", out)
}

// someNodeName returns a node of the kind cluster to watch. Any node will do —
// the scenario is about a cluster-scoped kind reaching the sink, not about a
// particular machine.
func someNodeName() string {
	GinkgoHelper()
	out, err := kubectl("get", "nodes", "-o", "jsonpath={.items[0].metadata.name}")
	Expect(err).NotTo(HaveOccurred(), "failed to list nodes: %s", out)
	name := strings.TrimSpace(out)
	Expect(name).NotTo(BeEmpty(), "the cluster reports no nodes")
	return name
}
