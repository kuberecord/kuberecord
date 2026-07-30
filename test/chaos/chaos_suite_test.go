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

// Package chaos is kubestream's failure-mode suite (Task 2.1).
//
// The correctness machinery this project is built around — version-gated
// commits, UID-gated delete claims, scope epochs, batch poison isolation, the
// bounded hand-off queue — exists entirely for conditions that do not occur on a
// healthy system. Every one of those mechanisms is unit-tested and every one is
// exercised end to end by the Phase 1 gate (test/e2e), but only while nothing is
// broken. This suite is the other half: a real operator, a real kind cluster and
// a real ClickHouse the suite can stop and start at will, with each scenario
// asserting through direct ClickHouse queries *and* the operator's own /metrics
// endpoint — because the rows say what was ultimately recorded and the metrics
// say what the write path went through to record it, and a chaos claim needs
// both.
//
// It is a separate suite from test/e2e, with its own kind cluster and its own
// fixtures, for one structural reason: its first scenario requires the operator
// to boot against a backend that is not there, which is the opposite of the
// invariant every e2e scenario starts from. The two share their vocabulary
// (test/harness) so the restart scenario here can reuse the Phase 1 restart
// assertions literally rather than paraphrase them.
package chaos

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/test/harness"
	"github.com/yelzhy/kubestream/test/utils"
)

// Identities of everything the suite installs. The operator-side names are the
// ones `config/default` produces, so the suite breaks the shipped install rather
// than a parallel one that could drift from it.
const (
	operatorNamespace   = "kubestream-system"
	operatorDeployment  = "kubestream-controller-manager"
	operatorPodSelector = "control-plane=controller-manager"
	operatorAccount     = "kubestream-controller-manager"
	// credentialsSecret is the Secret `config/manager` ships. Both the sink and
	// the ClickHouse server take their password from it, so the two sides of the
	// connection cannot drift apart.
	credentialsSecret = "kubestream-clickhouse-credentials"
	// clusterID is the value the shipped Deployment sets CLUSTER_ID to. Every row
	// and scope event the operator writes is stamped with it, so every query in
	// the suite filters on it.
	clusterID = "local-kind-cluster"
	// sinkName is the ClickHouseSink the suite installs. "default" is the name a
	// rule's spec.sinkRef defaults to, which is why no rule here spells one.
	sinkName = "default"
)

// The stoppable ClickHouse fixture (test/chaos/manifests/clickhouse.yaml).
const (
	clickHouseNamespace  = "kubestream-chaos-clickhouse"
	clickHouseDeployment = "clickhouse"
	clickHouseSecret     = "clickhouse-credentials"
	clickHouseImage      = "clickhouse/clickhouse-server:24.8"
	clickHouseUser       = "kubestream"
	clickHouseDatabase   = "kubestream"
)

// The resident metrics-scraping pod. A chaos scenario reads the same counters
// every couple of seconds for minutes at a stretch, so the pod is created once
// and exec'd per scrape (see harness.MetricsEndpoint).
const (
	metricsService  = "kubestream-controller-manager-metrics-service"
	metricsReader   = "kubestream-metrics-reader"
	metricsBinding  = "kubestream-chaos-metrics-binding"
	metricsProbePod = "chaos-metrics-probe"
	metricsImage    = "curlimages/curl:latest"
)

// Manifest paths, relative to the project root — the directory utils.Run
// executes every command in.
const (
	clickHouseManifest = "test/chaos/manifests/clickhouse.yaml"
	sinkManifest       = "test/chaos/manifests/sink.yaml"
)

// Timeouts. Every wait is an Eventually or a `kubectl wait`; there are no sleeps
// except the ones a scenario uses to let a counter accumulate, so these bound how
// long a *stuck* system is tolerated rather than how long a healthy one takes.
const (
	// rolloutTimeout covers scheduling a pod and passing its readiness probe on a
	// cold kind node — including, for the very first ClickHouse start, binding a
	// PersistentVolume through kind's local-path provisioner.
	rolloutTimeout = 5 * time.Minute
	// sinkReadyTimeout covers the ClickHouseSink's first successful async probe
	// after the backend appears: dial, auto-create the schema, validate it, report
	// the condition. It is longer than the e2e suite's because the probe backs off
	// while the backend is down, so a recovery waits out the current backoff
	// interval (capped at 60s) before the first successful attempt.
	sinkReadyTimeout = 4 * time.Minute
	// ruleReadyTimeout covers a rule's reconcile: policy check, GVK resolution, a
	// SelfSubjectAccessReview per target, registry upsert, status write.
	ruleReadyTimeout = 2 * time.Minute
	// rowTimeout covers an object change reaching ClickHouse on a healthy system:
	// informer event, workqueue, pipeline worker, batched insert.
	rowTimeout = 2 * time.Minute
	// recoveryTimeout covers a change reaching ClickHouse after an outage, where
	// the key is waiting out the workqueue's rate limiter and the writer may still
	// be inside a retry cycle that began while the backend was down.
	recoveryTimeout = 6 * time.Minute
	// outageTimeout bounds a wait for writes to fail *terminally*. The writer's
	// retry budget is 60s per batch (clickhouse.defaultMaxRetryBackoff, not
	// operator-configurable) plus a bounded per-row isolation phase, so the first
	// failed commit cannot arrive sooner than that and a second cycle takes as long
	// again. This is that, doubled, with slack.
	outageTimeout = 8 * time.Minute
	// restartTimeout covers the operator coming back after being killed: pod
	// scheduling, then acquiring the leader-election lease the killed pod never
	// released (so it must expire first), then per-scope warm-up and zombie GC.
	restartTimeout = 5 * time.Minute
	// quietWindow is how long a "and no more rows appear" claim is held open. It is
	// comfortably longer than one batch flush plus one pipeline resync, which is the
	// timescale on which a wrong row would show up.
	quietWindow = 20 * time.Second
	// pollInterval paces every Eventually. Each poll spawns a kubectl process, so
	// polling faster would burn CPU the cluster under test needs.
	pollInterval = 2 * time.Second
)

// writerRetryBudget is the writer's per-batch retry budget
// (clickhouse.defaultMaxRetryBackoff). It is not a knob — neither the CRD nor a
// flag exposes it — so the scenarios that need an outage to outlast it, or to
// finish inside it, have to know the number. Spelled here once, with that
// dependency stated, rather than as a magic duration in three scenarios.
const writerRetryBudget = 60 * time.Second

var (
	// managerImage is the manager image built and side-loaded for this run. It
	// must match the image the chaos kustomize overlay pins.
	managerImage = "example.com/kubestream:v0.0.1"

	// ch is this suite's view of the backend. Password is filled in during
	// BeforeSuite, once the credentials Secret has been read.
	ch = &harness.ClickHouse{
		Namespace:  clickHouseNamespace,
		Deployment: clickHouseDeployment,
		User:       clickHouseUser,
		Database:   clickHouseDatabase,
		ClusterID:  clusterID,
	}

	// metrics scrapes the operator's own endpoint. Populated in BeforeSuite once
	// the probe pod is running and a token has been minted.
	metrics harness.MetricsEndpoint

	// clickHouseUp tracks whether the backend is currently running, so the
	// standing duplicate-Deleted invariant is only queried when there is something
	// to query — an outage is a scenario's subject, not a suite failure.
	clickHouseUp bool
)

// fieldManager is the field-manager name every object the suite applies is
// written under; see harness.ApplyYAML.
const fieldManager = "kubestream-chaos"

// TestChaos runs the Phase 2 failure-mode suite against a real kind cluster and a
// real, stoppable ClickHouse. It expects Kind to be installed and a cluster to
// exist — `make test-chaos` creates one and tears it down again.
func TestChaos(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(rowTimeout)
	SetDefaultEventuallyPollingInterval(pollInterval)
	SetDefaultConsistentlyDuration(quietWindow)
	SetDefaultConsistentlyPollingInterval(pollInterval)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting kubestream chaos test suite\n")
	RunSpecs(t, "chaos suite")
}

var _ = BeforeSuite(func() {
	configureKubectlKubeRC()

	By("building the manager image")
	out, err := utils.Run(exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage)))
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image: %s", out)

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed(),
		"Failed to load the manager image into Kind")

	By("loading the fixture images on Kind")
	harness.SideloadImage(clickHouseImage)
	harness.SideloadImage(metricsImage)

	// Ordering here is the suite's central setup decision, and each step is where
	// it is for a reason.
	//
	// The operator is installed first, with no backend and no sink at all, purely
	// so its shipped credentials Secret exists to configure the fixture from — the
	// same single source of truth the e2e suite uses, so the two sides of the
	// connection cannot drift apart.
	//
	// ClickHouse is then started once and stopped again. That start is not part of
	// any scenario: it provisions and binds the volume and lets the image's
	// entrypoint create the database and the user while nothing is being timed.
	// Paying for a local-path provisioner round-trip inside the first outage window
	// would put a StorageClass inside the writer's 60s retry budget and make a
	// recovery assertion fail for a reason that has nothing to do with the
	// operator.
	//
	// Finally the sink is created and the operator is restarted, so the process the
	// scenarios run against genuinely *boots* with a sink configured and its
	// backend absent — which is what the first scenario is about, and which could
	// not be claimed of the process that has been idling here since step one.
	deployOperator()
	deployClickHouse()
	stopClickHouse()

	By("creating the ClickHouseSink while its backend is down")
	// Deliberately not waited on: the sink cannot become Ready yet, and that is
	// the first scenario's subject rather than a setup failure.
	harness.ApplyFile(sinkManifest)

	By("restarting the operator so it boots against a backend that is not there")
	scaleOperator(0)
	scaleOperator(1)

	deployMetricsProbe()
})

var _ = AfterSuite(func() {
	// Ordered so each component still has what it needs while it shuts down: the
	// sink first, so the operator drains its queued writes against a ClickHouse
	// that is still up; then the operator; then the backend.
	if clickHouseUp {
		By("removing the ClickHouseSink")
		harness.DeleteResourceQuietly("clickhousesink", sinkName, "")
	}

	By("removing the metrics probe")
	harness.DeleteResourceQuietly("pod", metricsProbePod, operatorNamespace)
	harness.DeleteResourceQuietly("clusterrolebinding", metricsBinding, "")

	By("undeploying the operator")
	if out, err := utils.Run(exec.Command("make", "undeploy-chaos")); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: undeploy-chaos: %v\n%s", err, out)
	}

	By("removing the ClickHouse fixture")
	harness.DeleteResourceQuietly("namespace", clickHouseNamespace, "")
})

// configureKubectlKubeRC disables kubectl kuberc by default for test isolation,
// so a local kubectl configuration cannot change what the suite observes.
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		Expect(os.Setenv("KUBECTL_KUBERC", "false")).To(Succeed(), "Failed to disable kubectl kuberc")
	}
}

// deployClickHouse brings up the stoppable backend, giving it the same password
// the operator's credentials Secret holds.
//
// The namespace and the Secret are created before the manifest is applied so the
// server pod never starts in CreateContainerConfigError and burns kubelet backoff
// waiting for a Secret that was always going to exist.
func deployClickHouse() {
	By("reading the ClickHouse password from the operator's credentials Secret")
	encoded, err := harness.Kubectl("get", "secret", credentialsSecret, "-n", operatorNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read the credentials Secret")
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode the credentials Secret")
	chPassword := string(decoded)
	Expect(chPassword).NotTo(BeEmpty(), "the shipped credentials Secret carries an empty password")
	ch.Password = chPassword

	By("creating the ClickHouse namespace and credentials")
	harness.CreateNamespace(clickHouseNamespace)
	out, err := harness.Kubectl("create", "secret", "generic", clickHouseSecret,
		"-n", clickHouseNamespace, "--from-literal=password="+chPassword)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the ClickHouse Secret: %s", out)

	By("applying the ClickHouse fixture and starting it once to initialise its volume")
	harness.ApplyFile(clickHouseManifest)
	startClickHouse()
}

// deployOperator installs CRDs, RBAC, the credentials Secret and the manager from
// the chaos kustomize overlay, then waits for the Deployment to become available.
//
// Becoming available while ClickHouse is down is itself the first assertion of
// the suite: an operator that refused to start without its backend would violate
// Invariant 1 before any scenario ran.
func deployOperator() {
	By("deploying the operator with the chaos overlay, against a stopped backend")
	out, err := utils.Run(exec.Command("make", "deploy-chaos"))
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy the operator: %s", out)

	By("waiting for the controller-manager to become available")
	out, err = harness.Kubectl("wait", "deployment/"+operatorDeployment, "-n", operatorNamespace,
		"--for=condition=Available", "--timeout="+rolloutTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "controller-manager never became available: %s", out)
}

// deployMetricsProbe stands up the resident curl pod every metrics assertion is
// read through, and grants it the right to read the endpoint.
func deployMetricsProbe() {
	By("granting the operator's ServiceAccount access to its own metrics")
	out, err := harness.Kubectl("create", "clusterrolebinding", metricsBinding,
		"--clusterrole="+metricsReader,
		fmt.Sprintf("--serviceaccount=%s:%s", operatorNamespace, operatorAccount))
	Expect(err).NotTo(HaveOccurred(), "Failed to create the metrics ClusterRoleBinding: %s", out)

	By("starting the resident metrics probe pod")
	harness.ApplyYAML(fieldManager, metricsProbeYAML())
	out, err = harness.Kubectl("wait", "pod/"+metricsProbePod, "-n", operatorNamespace,
		"--for=condition=Ready", "--timeout="+rolloutTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "the metrics probe pod never became ready: %s", out)

	metrics = harness.MetricsEndpoint{
		Pod:       metricsProbePod,
		Namespace: operatorNamespace,
		URL: fmt.Sprintf("https://%s.%s.svc.cluster.local:8443/metrics",
			metricsService, operatorNamespace),
		Token: serviceAccountToken(),
	}

	By("verifying the metrics endpoint answers")
	Eventually(func(g Gomega) {
		samples, scrapeErr := metrics.Scrape()
		g.Expect(scrapeErr).NotTo(HaveOccurred())
		g.Expect(samples).NotTo(BeEmpty())
	}, rolloutTimeout, pollInterval).Should(Succeed())
}

// serviceAccountToken mints a long-lived token for the operator's ServiceAccount.
//
// The lifetime is deliberate: this suite waits out several multi-minute outages,
// and a token that expired mid-run would turn every later metrics assertion into
// an authentication failure that looks nothing like the thing under test.
func serviceAccountToken() string {
	GinkgoHelper()
	out, err := harness.Kubectl("create", "token", operatorAccount,
		"-n", operatorNamespace, "--duration=8h")
	Expect(err).NotTo(HaveOccurred(), "Failed to mint a ServiceAccount token: %s", out)

	// kubectl may warn (on stderr, which utils.Run merges in) that the API server
	// capped the requested duration; the token is the JWT-shaped line.
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ey") && strings.Count(line, ".") == 2 {
			return line
		}
	}
	Fail("kubectl create token returned no token: " + out)
	return ""
}

// metricsProbeYAML renders the resident scraping pod.
//
// The spec is spelled out rather than defaulted because the operator's namespace
// enforces the restricted Pod Security Standard, which this pod must satisfy too.
func metricsProbeYAML() string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/component: chaos-fixture
spec:
  restartPolicy: Never
  containers:
  - name: curl
    image: %s
    imagePullPolicy: IfNotPresent
    command: ["sleep", "infinity"]
    securityContext:
      readOnlyRootFilesystem: true
      allowPrivilegeEscalation: false
      capabilities:
        drop: ["ALL"]
      runAsNonRoot: true
      runAsUser: 1000
      seccompProfile:
        type: RuntimeDefault
`, metricsProbePod, operatorNamespace, metricsImage)
}

// The shared types and constants, spelled in this suite's own vocabulary. See
// test/e2e/vocabulary_test.go for the same binding on the other side; the two
// suites deliberately share the definitions and not the fixtures.
type (
	objectFilter = harness.ObjectFilter
	scopeQuery   = harness.ScopeQuery
	ruleResource = harness.RuleResource
)

const (
	eventAdded    = harness.EventAdded
	eventModified = harness.EventModified
	eventDeleted  = harness.EventDeleted
	eventSnapshot = harness.EventSnapshot

	statusTrue  = harness.StatusTrue
	statusFalse = harness.StatusFalse

	groupCore     = harness.GroupCore
	kindConfigMap = harness.KindConfigMap
)

// creationEvents are the two tags an object's first appearance can carry; see
// harness.CreationEvents.
var creationEvents = harness.CreationEvents

// sinkReady waits until the ClickHouseSink reports a healthy backend. It is a
// real assertion after every recovery, not just a barrier: it means the operator
// re-dialled, re-validated the schema it may have just auto-created, and reported
// it — all asynchronously, without a reconciler ever dialling the backend itself
// (Invariant 1).
func expectSinkReady() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		harness.ExpectCondition(g, "clickhousesink", sinkName, "", v1alpha1.ConditionSchemaValid, statusTrue)
		harness.ExpectCondition(g, "clickhousesink", sinkName, "", v1alpha1.ConditionReady, statusTrue)
	}, sinkReadyTimeout, pollInterval).Should(Succeed())
}
