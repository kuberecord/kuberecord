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
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/test/utils"
)

// Identities of everything the suite installs. The operator-side names are the
// ones `config/default` produces (namespace kubestream-system, `kubestream-`
// name prefix), so the suite exercises the shipped install rather than a
// parallel one that could drift from it.
const (
	// operatorNamespace is where the operator, its ServiceAccount and the
	// credentials Secret live. It is also the only namespace the operator holds
	// Secret read rights in (Task 1.9).
	operatorNamespace = "kubestream-system"
	// operatorDeployment and operatorPodSelector address the manager Deployment
	// and its pods, which the restart scenario scales and every scenario reads
	// logs from on failure.
	operatorDeployment  = "kubestream-controller-manager"
	operatorPodSelector = "control-plane=controller-manager"
	// credentialsSecret is the Secret `config/manager` ships. Both the sink and
	// the ClickHouse server take their password from it, so the two sides of the
	// connection cannot drift apart.
	credentialsSecret = "kubestream-clickhouse-credentials"
	// clusterID is the value the shipped Deployment sets CLUSTER_ID to. Every
	// row and scope event the operator writes is stamped with it, so every query
	// in the suite filters on it.
	clusterID = "local-kind-cluster"
	// sinkName is the ClickHouseSink the suite installs. "default" is the name a
	// rule's spec.sinkRef defaults to, which is why no rule here spells one.
	sinkName = "default"
)

// The in-cluster ClickHouse fixture (test/e2e/manifests/clickhouse.yaml).
const (
	clickHouseNamespace  = "kubestream-e2e-clickhouse"
	clickHouseDeployment = "clickhouse"
	clickHouseSecret     = "clickhouse-credentials"
	clickHouseImage      = "clickhouse/clickhouse-server:24.8"
	clickHouseUser       = "kubestream"
	clickHouseDatabase   = "kubestream"
)

// Manifest paths, relative to the project root — the directory utils.Run
// executes every command in.
const (
	clickHouseManifest  = "test/e2e/manifests/clickhouse.yaml"
	sinkManifest        = "test/e2e/manifests/sink.yaml"
	nodeWatcherManifest = "test/e2e/manifests/watcher-nodes.yaml"
	networkingPreset    = "config/rbac/presets/networking.yaml"
)

// Timeouts. Every wait in the suite is an Eventually or a `kubectl wait`; there
// are no sleeps, so these bound how long a *stuck* system is tolerated, not how
// long a healthy one takes. Each is sized from the slowest mechanism it waits
// on, with slack for a loaded CI machine.
const (
	// rolloutTimeout covers scheduling a pod and passing its readiness probe on
	// a cold kind node.
	rolloutTimeout = 5 * time.Minute
	// sinkReadyTimeout covers the ClickHouseSink's first successful async probe:
	// dial, auto-create the schema, validate it, report the condition.
	sinkReadyTimeout = 3 * time.Minute
	// ruleReadyTimeout covers a rule's reconcile: policy check, GVK resolution,
	// a SelfSubjectAccessReview per target, registry upsert, status write.
	ruleReadyTimeout = 2 * time.Minute
	// rowTimeout covers an object change reaching ClickHouse: informer event,
	// workqueue, pipeline worker, batched insert (batchMaxWait is 200ms for the
	// suite's sink) — plus room for a scope that is still warming.
	rowTimeout = 2 * time.Minute
	// rbacRecoveryTimeout covers a grant applied *after* a rule already degraded.
	// Applying a ClusterRole does not re-enqueue the rule, so recovery waits for
	// the rule reconciler's periodic resync, which is a hard-coded two minutes
	// (defaultRuleResyncPeriod); this is that plus slack.
	rbacRecoveryTimeout = 3 * time.Minute
	// restartTimeout covers the operator coming back after being scaled to zero:
	// pod scheduling, then acquiring the leader-election lease left behind by the
	// killed pod (which is not released on shutdown, so it must expire first),
	// then per-scope warm-up and zombie GC.
	restartTimeout = 5 * time.Minute
	// quietWindow is how long a "and no more rows appear" claim is held open. It
	// is comfortably longer than one batch flush plus one pipeline resync, which
	// is the timescale on which a wrong row would show up.
	quietWindow = 20 * time.Second
	// pollInterval paces every Eventually. Each poll spawns a kubectl process,
	// so polling faster would burn CPU the cluster under test needs.
	pollInterval = 2 * time.Second
)

var (
	// managerImage is the manager image built and side-loaded for this run. It
	// must match the image the e2e kustomize overlay pins.
	managerImage = "example.com/kubestream:v0.0.1"

	// chPassword is the ClickHouse password, read out of the operator's shipped
	// credentials Secret in BeforeSuite so the fixture server and the sink are
	// configured from one source.
	chPassword string
)

// TestE2E runs the Phase 1 acceptance suite against a real kind cluster and a
// real ClickHouse. It expects Kind to be installed and a cluster to exist —
// `make test-e2e` creates one and tears it down again.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(rowTimeout)
	SetDefaultEventuallyPollingInterval(pollInterval)
	SetDefaultConsistentlyDuration(quietWindow)
	SetDefaultConsistentlyPollingInterval(pollInterval)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting kubestream e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	configureKubectlKubeRC()

	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed(),
		"Failed to load the manager image into Kind")

	By("loading the ClickHouse image on Kind")
	loadClickHouseImage()

	deployOperator()
	deployClickHouse()
	deploySink()
})

var _ = AfterSuite(func() {
	// Ordered so each component still has what it needs while it shuts down: the
	// sink first, so the operator drains its queued writes against a ClickHouse
	// that is still up; then the operator; then the backend.
	By("removing the ClickHouseSink")
	deleteResourceQuietly("clickhousesink", sinkName, "")

	By("undeploying the operator")
	if out, err := utils.Run(exec.Command("make", "undeploy-e2e")); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: undeploy-e2e: %v\n%s", err, out)
	}

	By("removing the ClickHouse fixture")
	deleteResourceQuietly("namespace", clickHouseNamespace, "")
})

// Disable kubectl kuberc by default for test isolation. This prevents local
// kubectl configurations from affecting test behavior. To enable kuberc, set:
// KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		Expect(os.Setenv("KUBECTL_KUBERC", "false")).To(Succeed(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// loadClickHouseImage side-loads the ClickHouse image into the kind node,
// pulling it to the host first only if it is not already there.
//
// The alternative — letting the kubelet pull it — would put a several-hundred-
// megabyte registry download inside the suite's runtime budget on every cold
// run, and would make the suite fail on a machine with no registry access even
// though nothing about it needs one.
//
// It goes through a `docker save` archive rather than `kind load docker-image`
// because the published image is a multi-platform index: kind imports with
// --all-platforms, and containerd then fails looking for the manifests of the
// platforms a single-platform pull never fetched. Exporting this host's platform
// alone produces a plain single-platform archive kind imports without complaint.
// (`docker save --platform` needs Docker 25 or newer.)
func loadClickHouseImage() {
	if _, err := utils.Run(exec.Command("docker", "image", "inspect", clickHouseImage)); err != nil {
		By("pulling the ClickHouse image to the host")
		_, err := utils.Run(exec.Command("docker", "pull", clickHouseImage))
		Expect(err).NotTo(HaveOccurred(), "Failed to pull the ClickHouse image")
	}

	archiveDir, err := os.MkdirTemp("", "kubestream-e2e-images")
	Expect(err).NotTo(HaveOccurred(), "Failed to create a temporary directory for the image archive")
	DeferCleanup(func() {
		if err := os.RemoveAll(archiveDir); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: removing the image archive: %v\n", err)
		}
	})

	// The kind node runs on the host's Docker, so the host's architecture is the
	// node's architecture; the test binary is built for it too.
	archive := filepath.Join(archiveDir, "clickhouse.tar")
	out, err := utils.Run(exec.Command("docker", "save",
		"--platform", "linux/"+runtime.GOARCH, clickHouseImage, "-o", archive))
	Expect(err).NotTo(HaveOccurred(), "Failed to export the ClickHouse image: %s", out)

	Expect(utils.LoadImageArchiveToKindCluster(archive)).To(Succeed(),
		"Failed to load the ClickHouse image into Kind")
}

// deployOperator installs CRDs, RBAC, the credentials Secret and the manager
// from the e2e kustomize overlay (config/default plus a pinned image and
// --ch-auto-create-schema), then waits for the Deployment to become available.
func deployOperator() {
	By("deploying the operator with the e2e overlay")
	out, err := utils.Run(exec.Command("make", "deploy-e2e"))
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy the operator: %s", out)

	By("waiting for the controller-manager to become available")
	out, err = kubectl("wait", "deployment/"+operatorDeployment, "-n", operatorNamespace,
		"--for=condition=Available", "--timeout="+rolloutTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "controller-manager never became available: %s", out)
}

// deployClickHouse brings up the single-node backend, giving it the same
// password the operator's credentials Secret holds.
//
// The namespace and the Secret are created before the manifest is applied so
// the server pod never starts in CreateContainerConfigError and burn kubelet
// backoff waiting for a Secret that was always going to exist.
func deployClickHouse() {
	By("reading the ClickHouse password from the operator's credentials Secret")
	encoded, err := kubectl("get", "secret", credentialsSecret, "-n", operatorNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read the credentials Secret")
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode the credentials Secret")
	chPassword = string(decoded)
	Expect(chPassword).NotTo(BeEmpty(), "the credentials Secret carries an empty password")

	By("creating the ClickHouse namespace and credentials")
	createNamespace(clickHouseNamespace)
	out, err := kubectl("create", "secret", "generic", clickHouseSecret,
		"-n", clickHouseNamespace, "--from-literal=password="+chPassword)
	Expect(err).NotTo(HaveOccurred(), "Failed to create the ClickHouse Secret: %s", out)

	By("deploying ClickHouse")
	applyFile(clickHouseManifest)
	out, err = kubectl("rollout", "status", "deployment/"+clickHouseDeployment,
		"-n", clickHouseNamespace, "--timeout="+rolloutTimeout.String())
	Expect(err).NotTo(HaveOccurred(), "ClickHouse never became ready: %s", out)
}

// deploySink creates the ClickHouseSink and waits for it to report Ready.
//
// Reaching Ready is a real assertion, not just a barrier: it means the operator
// resolved the Secret, dialled ClickHouse, applied the shipped DDL under
// --ch-auto-create-schema and validated the resulting schema against the
// columns it writes — all of it asynchronously, without a reconciler ever
// dialling the backend itself (Invariant 1).
func deploySink() {
	By("creating the ClickHouseSink and waiting for it to connect")
	applyFile(sinkManifest)
	Eventually(func(g Gomega) {
		expectCondition(g, "clickhousesink", sinkName, "", v1alpha1.ConditionSchemaValid, statusTrue)
		expectCondition(g, "clickhousesink", sinkName, "", v1alpha1.ConditionReady, statusTrue)
	}, sinkReadyTimeout, pollInterval).Should(Succeed())
}
