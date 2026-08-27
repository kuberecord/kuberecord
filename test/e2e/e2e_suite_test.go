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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/test/harness"
	"github.com/kuberecord/kuberecord/test/utils"
)

// Identities of everything the suite installs. The operator-side names are the
// ones `config/default` produces (namespace kuberecord-system, `kuberecord-`
// name prefix), so the suite exercises the shipped install rather than a
// parallel one that could drift from it.
const (
	// operatorNamespace is where the operator, its ServiceAccount and the
	// credentials Secret live. It is also the only namespace the operator holds
	// Secret read rights in (Task 1.9).
	operatorNamespace = "kuberecord-system"
	// operatorDeployment and operatorPodSelector address the manager Deployment
	// and its pods, which the restart scenario scales and every scenario reads
	// logs from on failure.
	operatorDeployment  = "kuberecord-controller-manager"
	operatorPodSelector = "control-plane=controller-manager"
	// credentialsSecret is the Secret `config/manager` ships. Both the sink and
	// the ClickHouse server take their password from it, so the two sides of the
	// connection cannot drift apart.
	credentialsSecret = "kuberecord-clickhouse-credentials"
	// clusterID is the value the shipped Deployment sets CLUSTER_ID to. Every
	// row and scope event the operator writes is stamped with it, so every query
	// in the suite filters on it.
	clusterID = "local-kind-cluster"
	// sinkName is the ClickHouseSink the suite installs, and the name every rule it
	// renders puts in spec.sink.name. The kind half of that reference is left to the
	// CRD default (see harness.sinkYAML), so this is the whole of the suite's sink
	// identity.
	sinkName = "default"
)

// The in-cluster ClickHouse fixture (test/e2e/manifests/clickhouse.yaml).
const (
	clickHouseNamespace  = "kuberecord-e2e-clickhouse"
	clickHouseDeployment = "clickhouse"
	clickHouseSecret     = "clickhouse-credentials"
	clickHouseImage      = "clickhouse/clickhouse-server:24.8"
	clickHouseUser       = "kuberecord"
	clickHouseDatabase   = "kuberecord"
)

// The in-cluster MinIO fixture and the S3Sink over it (test/e2e/manifests/
// minio.yaml and s3sink.yaml, Task 6.6).
//
// Unlike ClickHouse, this fixture is brought up by the one scenario that needs it
// rather than in BeforeSuite: it is the whole of that scenario's runtime cost, and
// confining it there is what keeps the install-path smokes — which focus a single
// ClickHouse scenario — from paying for an object store they never touch.
const (
	minioNamespace  = "kuberecord-e2e-minio"
	minioDeployment = "minio"
	minioImage      = "minio/minio:RELEASE.2025-04-22T22-12-26Z"
	// minioSecret holds the server's root credentials, in the fixture's own
	// namespace; s3CredentialsSecret holds the same key pair beside the operator,
	// which is the only namespace it may read Secrets from (Task 1.9).
	minioSecret         = "minio-credentials"
	s3CredentialsSecret = "kuberecord-s3-credentials"
	// s3AccessKeyID and s3SecretAccessKey are that key pair. MinIO requires a root
	// password of at least eight characters, and the suite creates both Secrets
	// from these two constants so the two sides of the connection cannot drift.
	s3AccessKeyID     = "kuberecord"
	s3SecretAccessKey = "kuberecord-e2e-secret"
	// s3SinkName, s3Bucket and s3Prefix mirror test/e2e/manifests/s3sink.yaml. The
	// read path needs all three: the bucket and prefix to find the archive, the
	// name to assert on the sink's conditions.
	s3SinkName = "archive"
	s3Bucket   = "kuberecord-e2e"
	s3Prefix   = "audit"
)

// The published tee example and the fixtures it brings with it (examples/tee,
// Task 7.1).
//
// This scenario owns none of these manifests: they are the example a reader
// copies, applied through an overlay that patches one address (see
// test/e2e/manifests/tee). What the suite has to know is only the *names* the
// example uses, so it can address what it applied — everything with a value in it
// (the key pair, the bucket, the prefix, the image) is read back out of the
// cluster or out of the example file, so it cannot drift from what was applied.
//
// The fixture is brought up by the scenario rather than in BeforeSuite for the
// same reason the archive fixture is, and one more: it applies cluster-scoped
// sinks, and a suite-wide install would leave two extra sinks standing under
// every other scenario.
const (
	teeMinIONamespace  = "kuberecord-tee"
	teeMinIODeployment = "minio"
	// teeMinIOSecret holds the store's root credentials, in the fixture's own
	// namespace. The suite reads the key pair out of it rather than repeating it,
	// so the harness authenticates as exactly the identity the example created.
	teeMinIOSecret = "minio-credentials"
	// teeS3CredentialsSecret is the same key pair beside the operator, which the
	// example ships because that is the only namespace the operator may read
	// Secrets in (Task 1.9).
	teeS3CredentialsSecret = "kuberecord-tee-s3-credentials"
	// The two sinks the example authors, named for the halves of the pattern.
	// Neither may be called "default": sinks are cluster-scoped, and the suite
	// already runs a ClickHouseSink of that name.
	teeHotSinkName  = "hot"
	teeColdSinkName = "cold"
	// The namespace and the two rules over it. The rules differ in exactly one
	// field, spec.sink, which is the whole of the pattern (D14).
	teeNamespace = "tee-demo"
	teeHotRule   = "hot-timeline"
	teeColdRule  = "cold-archive"
	// teeDeployment is the workload examples/tee/workload.yaml carries, and the
	// object both backends are asserted on.
	teeDeployment = "checkout-api"
)

// Manifest paths, relative to the project root — the directory utils.Run
// executes every command in.
const (
	clickHouseManifest  = "test/e2e/manifests/clickhouse.yaml"
	sinkManifest        = "test/e2e/manifests/sink.yaml"
	minioManifest       = "test/e2e/manifests/minio.yaml"
	s3SinkManifest      = "test/e2e/manifests/s3sink.yaml"
	nodeWatcherManifest = "test/e2e/manifests/watcher-nodes.yaml"
	networkingPreset    = "config/rbac/presets/networking.yaml"
	// teeOverlay renders examples/tee with the suite's ClickHouse address patched
	// in; teeWorkload and teeMinIOExample are read from the example directly,
	// unpatched, because neither needs anything of this environment.
	teeOverlay      = "test/e2e/manifests/tee"
	teeWorkload     = "examples/tee/workload.yaml"
	teeMinIOExample = "examples/tee/minio.yaml"
	// teeBucketScript is the example's own bucket-creation step. The suite runs it
	// rather than creating the bucket through the harness, so the one command in
	// examples/tee/README.md that is not a kubectl apply is executed by CI too.
	teeBucketScript = "./examples/tee/bucket.sh"
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

// The install paths the suite can bring the operator up through. Which one runs
// is chosen by E2E_INSTALL; the scenarios never learn which it was.
//
// All of them produce the same object names — that is a property asserted
// independently by test/chart, and the reason the Phase 1 scenarios below are
// literally unmodified across them (Task 2.4). What each path proves is
// different: kustomize is the development install, helm is the chart a user
// installs from a checkout, installer is the single committed dist/install.yaml,
// and helm-oci is that same chart packaged, pushed to a registry and installed
// back out of it by reference — the distribution channel a release publishes
// (Task 8.1), which is the one thing rendering the chart locally cannot exercise.
const (
	installKustomize = "kustomize"
	installHelm      = "helm"
	installHelmOCI   = "helm-oci"
	installInstaller = "installer"
)

// installModes is the set E2E_INSTALL is checked against, and the order the
// failure message lists them in.
var installModes = []string{installKustomize, installHelm, installHelmOCI, installInstaller}

var (
	// managerImage is the manager image built and side-loaded for this run. It
	// must match the image the e2e kustomize overlay pins, the tag
	// test/e2e/manifests/helm-values.yaml sets, and E2E_INSTALLER_IMG.
	managerImage = "example.com/kuberecord:v0.0.1"

	// installMode is the install path under test, from E2E_INSTALL (default
	// kustomize). Read once in BeforeSuite so an unknown value fails the run
	// immediately rather than silently falling back to a path nobody asked for.
	installMode = installKustomize

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
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting kuberecord e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	configureKubectlKubeRC()
	resolveInstallMode()

	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed(),
		"Failed to load the manager image into Kind")

	By("loading the ClickHouse image on Kind")
	harness.SideloadImage(clickHouseImage)

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
	target := undeployTarget()
	if out, err := utils.Run(exec.Command("make", target)); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: %s: %v\n%s", target, err, out)
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

// resolveInstallMode reads E2E_INSTALL and records which install path this run
// exercises. An unrecognised value is a hard failure: silently defaulting would
// mean a CI job named after the chart quietly testing the kustomize install.
func resolveInstallMode() {
	requested := os.Getenv("E2E_INSTALL")
	if requested == "" {
		requested = installKustomize
	}
	Expect(requested).To(BeElementOf(installModes),
		"E2E_INSTALL must be one of %s", strings.Join(installModes, ", "))
	installMode = requested
	_, _ = fmt.Fprintf(GinkgoWriter, "installing the operator via %q\n", installMode)
}

// deployTarget and undeployTarget map the install mode onto the Makefile targets
// that own each path. The suite installs through make rather than by shelling out
// to kubectl or helm itself so that what CI runs, what a developer runs and what
// the README documents are one set of commands.
func deployTarget() string {
	switch installMode {
	case installHelm:
		return "deploy-e2e-helm"
	case installHelmOCI:
		return "deploy-e2e-helm-oci"
	case installInstaller:
		return "deploy-e2e-installer"
	default:
		return "deploy-e2e"
	}
}

func undeployTarget() string {
	switch installMode {
	case installHelm:
		return "undeploy-e2e-helm"
	case installHelmOCI:
		return "undeploy-e2e-helm-oci"
	case installInstaller:
		return "undeploy-e2e-installer"
	default:
		return "undeploy-e2e"
	}
}

// deployOperator installs the CRDs, the RBAC, the credentials Secret and the
// manager through the selected install path, then waits for the Deployment to
// become available.
//
// Every path lands on the same names — namespace kuberecord-system, Deployment
// kuberecord-controller-manager, Secret kuberecord-clickhouse-credentials — so
// nothing past this function knows or cares which one ran.
func deployOperator() {
	By(fmt.Sprintf("deploying the operator via %s", installMode))
	out, err := utils.Run(exec.Command("make", deployTarget()))
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
	// The shared query layer authenticates with the same password, so the suite
	// and the operator are reading and writing as the one user the fixture makes.
	ch.Password = chPassword

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
