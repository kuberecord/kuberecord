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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/controller"
	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/test/harness"
)

// The chaos verbs: the things this suite does to a running system that no other
// suite does. Everything that merely *reads* the system lives in test/harness and
// is shared with the Phase 1 gate.

// ---------------------------------------------------------------------------
// Breaking things
// ---------------------------------------------------------------------------

// stopClickHouse takes the backend down and waits until nothing of it is left
// running.
//
// Scaling the Deployment to zero is a genuine container stop, and that is the
// point: it severs every established connection the operator's driver pool
// holds. The obvious cheaper alternative — pointing the Service at nothing —
// would not, because conntrack keeps existing sockets flowing to the pod IP long
// after the endpoint disappears, so the operator would keep writing happily
// through an "outage" and every assertion downstream would be vacuous.
//
// Waiting for the pods to be *gone*, not merely terminating, matters for the same
// reason: a pod inside its grace period still answers queries.
func stopClickHouse() {
	GinkgoHelper()
	By("stopping ClickHouse")
	out, err := harness.Kubectl("scale", "deployment/"+clickHouseDeployment,
		"-n", clickHouseNamespace, "--replicas=0")
	Expect(err).NotTo(HaveOccurred(), "failed to stop ClickHouse: %s", out)

	Eventually(func(g Gomega) {
		pods, podErr := harness.Pods("app=clickhouse", clickHouseNamespace)
		g.Expect(podErr).NotTo(HaveOccurred())
		g.Expect(pods).To(BeEmpty(), "ClickHouse is still running")
	}, rolloutTimeout, pollInterval).Should(Succeed())
	clickHouseUp = false
}

// startClickHouse brings the backend back and waits until it answers queries.
//
// The rows written before the outage are still there: the fixture's storage is a
// PersistentVolumeClaim precisely so a recovery assertion is about the operator
// converging, not about a server that came back empty.
func startClickHouse() {
	GinkgoHelper()
	By("starting ClickHouse")
	out, err := harness.Kubectl("scale", "deployment/"+clickHouseDeployment,
		"-n", clickHouseNamespace, "--replicas=1")
	Expect(err).NotTo(HaveOccurred(), "failed to start ClickHouse: %s", out)

	out, err = harness.Kubectl("rollout", "status", "deployment/"+clickHouseDeployment,
		"-n", clickHouseNamespace, "--timeout="+rolloutTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "ClickHouse never became ready: %s", out)
	clickHouseUp = true
}

// scaleOperator takes the manager Deployment to replicas and waits for reality to
// match: no pods left at zero, an available Deployment at one.
func scaleOperator(replicas int) {
	GinkgoHelper()
	out, err := harness.Kubectl("scale", "deployment/"+operatorDeployment,
		"-n", operatorNamespace, fmt.Sprintf("--replicas=%d", replicas))
	Expect(err).NotTo(HaveOccurred(), "failed to scale the operator: %s", out)

	if replicas == 0 {
		awaitOperatorGone()
		return
	}
	out, err = harness.Kubectl("wait", "deployment/"+operatorDeployment, "-n", operatorNamespace,
		"--for=condition=Available", "--timeout="+restartTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "the operator never came back: %s", out)
}

// killOperator ends the operator the way a node failure or an OOM kill does:
// abruptly, mid-flight, with no drain.
//
// Both halves are necessary and the order is deliberate. Scaling to zero first is
// what stops the ReplicaSet from immediately replacing the pod, so the suite gets
// an outage window it controls rather than a race. Force-deleting with a zero
// grace period is what makes it a kill rather than a shutdown: the ordinary
// termination path would give the manager its full drain — queue drained, writer
// flushed, scope epochs closed — which is the *graceful* restart the Phase 1
// suite already covers. What Task 2.1 asks for is the other one: writes in flight
// when the process dies, a leader lease never released, and scope epochs left
// open with no Stopped row.
//
// The two commands are issued back to back, so the SIGTERM the scale triggers has
// no useful time to drain anything before the SIGKILL lands.
func killOperator() {
	GinkgoHelper()
	By("killing the operator mid-flight")
	out, err := harness.Kubectl("scale", "deployment/"+operatorDeployment,
		"-n", operatorNamespace, "--replicas=0")
	Expect(err).NotTo(HaveOccurred(), "failed to scale the operator down: %s", out)

	out, err = harness.Kubectl("delete", "pod", "-l", operatorPodSelector, "-n", operatorNamespace,
		"--force", "--grace-period=0", "--ignore-not-found", "--wait=false")
	Expect(err).NotTo(HaveOccurred(), "failed to kill the operator pod: %s", out)

	awaitOperatorStopped()
}

// leaderElectionLease is the Lease controller-runtime holds the manager's
// leadership in; the name is cmd/main.go's LeaderElectionID verbatim.
const leaderElectionLease = "885d930f.kuberecord.io"

// leaseStablePolls is how many consecutive polls must observe an unchanged
// renewal before the process is declared dead. A live leader renews on
// controller-runtime's RetryPeriod (2s by default) and the suite polls no faster
// than that, so four unchanged readings span comfortably more than one renewal
// interval.
const leaseStablePolls = 4

// awaitOperatorStopped waits until the operator process is genuinely no longer
// running — not merely no longer represented in the API.
//
// The distinction is the entire integrity of the offline scenario, and
// `--force --grace-period=0` is exactly where it bites: a forced delete removes
// the Pod object from etcd *immediately*, without waiting for the kubelet to
// confirm anything, so "no pods match the selector" becomes true while the
// container may still be running. Deleting this scenario's objects in that window
// would let a live operator observe them and quietly turn the offline-deletion
// test into an ordinary online one — passing, while testing nothing it claims to.
//
// The leader-election Lease is the barrier, because it is the one thing only a
// running process can produce. The manager renews it every couple of seconds and
// never releases it on shutdown (LeaderElectionReleaseOnCancel is deliberately
// off), so a frozen renewTime means the process is gone, whatever the API says
// about its Pod.
func awaitOperatorStopped() {
	GinkgoHelper()
	awaitOperatorGone()

	previous, err := leaseRenewTime()
	Expect(err).NotTo(HaveOccurred(), "failed to read the leader-election lease")
	Expect(previous).NotTo(BeEmpty(),
		"the manager holds no leader-election lease, so there is no way to tell a killed process from a live one")

	stable := 0
	Eventually(func(g Gomega) {
		current, readErr := leaseRenewTime()
		g.Expect(readErr).NotTo(HaveOccurred())
		if current == previous {
			stable++
		} else {
			previous = current
			stable = 0
		}
		g.Expect(stable).To(BeNumerically(">=", leaseStablePolls),
			"the operator is still renewing its leader-election lease, so it is still running")
	}, restartTimeout, pollInterval).Should(Succeed())
}

// leaseRenewTime reads the instant the manager last renewed its leadership, or
// the empty string if the Lease does not exist.
func leaseRenewTime() (string, error) {
	out, err := harness.Kubectl("get", "lease", leaderElectionLease, "-n", operatorNamespace,
		"--ignore-not-found", "-o", "jsonpath={.spec.renewTime}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// awaitOperatorGone blocks until no manager pod remains, terminating ones
// included.
//
// Terminating pods count. A pod inside its termination grace period is still
// watching, and proceeding then would delete a scenario's objects in front of a
// live operator — turning the offline-deletion test into an ordinary online one
// without failing.
func awaitOperatorGone() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := harness.Pods(operatorPodSelector, operatorNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(BeEmpty(), "the operator is still running")
	}, restartTimeout, pollInterval).Should(Succeed())
}

// ---------------------------------------------------------------------------
// Reading the operator's own metrics
// ---------------------------------------------------------------------------

// sinkSeries pins a metric to the one sink this suite installs. Every write-path
// collector carries the label (see pipeline.PipelineMetrics), so leaving it off
// would silently sum across sinks the day a scenario adds a second one.
var sinkSeries = map[string]string{"sink": sinkLabel}

// Metric names, fully qualified with the operator's "kuberecord" namespace. Named
// constants because a typo in a metric name reads exactly like a metric sitting
// at zero, which is the single most misleading way a chaos assertion can fail.
const (
	metricWrites          = "kuberecord_writes_total"
	metricEnqueueTimeouts = "kuberecord_enqueue_timeouts_total"
	metricEnqueueBlockCnt = "kuberecord_enqueue_block_seconds_count"
	metricEnqueueBlockBkt = "kuberecord_enqueue_block_seconds_bucket"
	metricQueueDepth      = "kuberecord_write_queue_depth"
	metricSafeMode        = "kuberecord_safe_mode"
)

// metricSum reads one counter or gauge family from the live endpoint, summed over
// every series matching labels. It takes a Gomega so it can be used inside an
// Eventually (where a scrape failure should retry) and outside one (with Default,
// where it should fail).
func metricSum(g Gomega, name string, labels map[string]string) float64 {
	samples, err := metrics.Scrape()
	g.Expect(err).NotTo(HaveOccurred(), "failed to scrape the operator's metrics")
	return harness.Sum(samples, name, labels)
}

// failedWrites is the counter Task 2.1 names directly: settled write jobs whose
// outcome was a failure, per sink.
func failedWrites(g Gomega) float64 {
	return metricSum(g, metricWrites, map[string]string{"sink": sinkLabel, "outcome": "failed"})
}

// safeMode reads one scope's Snapshot-mode gauge, distinguishing "this scope is
// warm" (0) from "this scope has never been seen" (absent) — a distinction Sum
// would flatten and the boot scenario depends on.
func safeMode(g Gomega, namespace string) (float64, bool) {
	samples, err := metrics.Scrape()
	g.Expect(err).NotTo(HaveOccurred(), "failed to scrape the operator's metrics")
	sample, ok := harness.Find(samples, metricSafeMode, map[string]string{
		"sink": sinkLabel, "group": groupCore, "kind": kindConfigMap, "namespace": namespace,
	})
	return sample.Value, ok
}

// expectCounterToRise waits until a counter has climbed by at least delta above
// baseline, and returns where it got to.
//
// "Rose by at least N" rather than "is nonzero" is what makes these assertions
// scenario-local: the suite runs Ordered and Serial against one operator, so
// every counter carries what earlier scenarios did to it, and an absolute
// threshold would start passing for the wrong reasons as the suite grows.
//
// It always waits out outageTimeout, because every counter these scenarios watch
// is one the write path only touches after exhausting its retry budget — there is
// no shorter honest deadline for "this write has definitively failed".
func expectCounterToRise(name string, labels map[string]string,
	baseline, delta float64, description string) float64 {
	GinkgoHelper()
	var reached float64
	Eventually(func(g Gomega) {
		reached = metricSum(g, name, labels)
		g.Expect(reached).To(BeNumerically(">=", baseline+delta), description)
	}, outageTimeout, pollInterval).Should(Succeed())
	return reached
}

// ---------------------------------------------------------------------------
// Fixtures the scenarios churn
// ---------------------------------------------------------------------------

// filler returns n bytes of deterministic, incompressible-enough padding. It is
// how a scenario dials a record's size: a few hundred bytes for an ordinary
// object, a few hundred kilobytes for the poison one.
func filler(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

// configMapNames returns count deterministic names with the given prefix, so a
// scenario can create a burst and then assert over exactly the set it created.
func configMapNames(prefix string, count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}
	return names
}

// applyConfigMaps creates or updates every named ConfigMap in a *single* kubectl
// apply.
//
// One apply, not one per object, and that is load-bearing rather than merely
// fast: the objects then reach the informer within milliseconds of each other, so
// the sink's single writer coalesces them into one batch. Both the poison-row
// scenario (which needs the poison record to share a batch with blameless
// neighbours) and the saturation scenario (which needs the hand-off queue to fill
// faster than one worker drains it) depend on that.
func applyConfigMaps(namespace string, names []string, data map[string]string) {
	GinkgoHelper()
	docs := make([]string, 0, len(names))
	for _, name := range names {
		docs = append(docs, harness.ConfigMapYAML(namespace, name, data))
	}
	harness.ApplyYAML(fieldManager, strings.Join(docs, "---\n"))
}

// configMapFilter is the row filter for one ConfigMap. UID is optional: a
// scenario that has not yet minted the object (or is asserting across a
// reincarnation) passes "".
func configMapFilter(namespace, name, uid string) objectFilter {
	return objectFilter{
		Group: groupCore, Kind: kindConfigMap,
		Namespace: namespace, Name: name, UID: uid,
	}
}

// autoInjectedConfigMap is the ConfigMap the API server publishes into every
// namespace on creation. It is streamed like any other ConfigMap — correctly, and
// the suite has no business hiding it — but no scenario created it, so counting
// it would turn "exactly the objects I made" into an off-by-one that has nothing
// to do with the operator.
const autoInjectedConfigMap = "kube-root-ca.crt"

// creationRowCounts returns how many creation rows (Added or Snapshot) each
// object a scenario created in a namespace carries.
//
// One query for the whole namespace, rather than one Eventually per object,
// because the scenarios that need it assert over a hundred and fifty objects at a
// time: per-object polling would spend more wall-clock spawning kubectl processes
// than the operator spends recovering, and would say nothing extra. "Exactly one
// creation row per object, and no object missing" is the same claim, made once.
func creationRowCounts(namespace string) (map[string]int, error) {
	rows, err := ch.ResourceRows(withEvent(objectFilter{
		Group: groupCore, Kind: kindConfigMap, Namespace: namespace,
	}, creationEvents...))
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		if row.Name == autoInjectedConfigMap {
			continue
		}
		counts[row.Name]++
	}
	return counts, nil
}

// configMapScope is the watch_scopes query for a namespace's ConfigMap scope.
func configMapScope(namespace string) scopeQuery {
	return scopeQuery{Group: groupCore, Kind: kindConfigMap, Namespace: namespace}
}

// withEvent narrows a filter to the given event types.
func withEvent(filter objectFilter, eventTypes ...string) objectFilter {
	return harness.WithEvent(filter, eventTypes...)
}

// withAction narrows a scope query to one transition.
func withAction(query scopeQuery, action sink.ScopeAction) scopeQuery {
	return harness.WithAction(query, string(action))
}

// streamRuleKey renders the rule_ref a watch_scopes row carries for a namespaced
// StreamRule, through the one function that produces it — so the column's
// contract is asserted rather than restated.
func streamRuleKey(namespace, name string) string {
	return controller.RuleKey("streamrule", namespace, name)
}

// expectRuleStreaming waits until a rule is fully realised: policy-admitted,
// kind-resolved, RBAC-granted and installed in the registry. A scenario that
// starts from this state fails later for the reason it is testing, rather than
// because a rule never started.
func expectRuleStreaming(name, namespace string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		ready := harness.ExpectCondition(g, "streamrule", name, namespace,
			v1alpha1.ConditionReady, statusTrue)
		g.Expect(ready.Reason).To(Equal(controller.ReasonStreaming))
	}, ruleReadyTimeout, pollInterval).Should(Succeed())
}

// applyConfigMapRule installs a StreamRule for v1/ConfigMap in namespace and
// waits for its watch scope to open.
//
// The Started row is a barrier, not decoration: an object observed before its
// scope has warmed from the sink's history is tagged Snapshot rather than Added
// (see harness.CreationEvents). A visible Started row means the scope's
// ClickHouse round-trip has already completed, so the warm — one query issued at
// the same moment — has too, and a cache miss from then on genuinely means "new".
// Scenarios that assert on Added specifically depend on having crossed it.
func applyConfigMapRule(namespace, name string) {
	GinkgoHelper()
	harness.ApplyYAML(fieldManager, harness.StreamRuleYAML(namespace, name, []ruleResource{
		{Group: groupCore, Version: "v1", Kind: kindConfigMap},
	}))
	expectRuleStreaming(name, namespace)
	started := ch.EventuallyScopeRows(withAction(configMapScope(namespace), sink.ScopeActionStarted))
	Expect(started[0].RuleRef).To(Equal(streamRuleKey(namespace, name)))
}

// ---------------------------------------------------------------------------
// Convergence
// ---------------------------------------------------------------------------

// liveHash recomputes an object's canonical content hash from the API server's
// current copy, through the very function the write path hashes with
// (pipeline.ObjectHash).
//
// This is what makes Task 2.1's "no gaps" criterion a real claim. After an
// outage, comparing the sink's newest sha256 for an object against this value
// asks the only question that matters — is what ClickHouse believes about this
// object what the object actually is? — rather than the much weaker "did some row
// eventually arrive". Reimplementing the normalization here instead would mean
// the comparison silently stopped testing anything the first time those rules
// changed, which is precisely the drift the criterion exists to catch.
func liveHash(kind, name, namespace string) (string, error) {
	raw, err := harness.ObjectJSON(kind, name, namespace)
	if err != nil {
		return "", err
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(raw, &obj.Object); err != nil {
		return "", fmt.Errorf("decode %s/%s: %w", kind, name, err)
	}
	return pipeline.ObjectHash(obj, nil)
}

// expectConverged asserts that the sink's newest row for an object describes the
// object as it now is: the row's sha256 equals a live recompute, and exactly one
// row carries that hash.
//
// The uniqueness half is not redundant. A write path that recovered from an
// outage by re-emitting the same state twice would satisfy "the latest row is
// correct" while doubling the audit trail — and at cluster scale that is the
// difference between a recovery and an incident.
func expectConverged(namespace, name string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		want, err := liveHash("configmap", name, namespace)
		g.Expect(err).NotTo(HaveOccurred())

		rows, err := ch.ResourceRows(configMapFilter(namespace, name, ""))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).NotTo(BeEmpty(), "no rows at all for %s/%s", namespace, name)

		newest := rows[len(rows)-1]
		g.Expect(newest.SHA256).To(Equal(want),
			"the newest row for %s/%s does not describe its current state", namespace, name)

		var carrying int
		for _, row := range rows {
			if row.SHA256 == want {
				carrying++
			}
		}
		g.Expect(carrying).To(Equal(1),
			"the converged state of %s/%s was recorded %d times", namespace, name, carrying)
	}, timeout, pollInterval).Should(Succeed())
}

// ---------------------------------------------------------------------------
// Standing invariants
// ---------------------------------------------------------------------------

// scopeState is one watch scope reduced to its latest transition — the shape the
// "no orphan Started" claim is made against.
type scopeState struct {
	Group     string `json:"api_group"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Action    string `json:"action"`
}

// scopeStates returns the most recent action recorded for every scope this
// cluster has ever watched.
//
// argMax over ts, rather than counting Started and Stopped rows, is what makes
// the answer meaningful across a kill: an ordinary restart legitimately leaves a
// scope with two Starteds and no Stopped (the killed process never wrote one, and
// boot reconciliation deliberately leaves a still-wanted scope open), so a count
// comparison would flag correct behaviour. What is never correct is a scope whose
// rule is gone and whose latest transition is still Started.
func scopeStates() ([]scopeState, error) {
	return harness.Select[scopeState](ch, `
		SELECT api_group, kind, namespace, argMax(action, ts) AS action
		FROM watch_scopes WHERE cluster_id = `+harness.Literal(clusterID)+`
		GROUP BY api_group, kind, namespace`)
}

// dumpOperatorLog writes the manager's recent log to the test output. The
// conditions carry a summary of a degrade; the log carries the cause.
func dumpOperatorLog() {
	logs, err := harness.Kubectl("logs", "-l", operatorPodSelector, "-n", operatorNamespace, "--tail=200")
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "failed to fetch controller logs: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
}
