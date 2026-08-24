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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
	"github.com/onsi/gomega/types"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/controller"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/test/harness"
	"github.com/yelzhy/kuberecord/test/utils"
)

// The Phase 7 gate for the tee pattern (Task 7.1): the published example,
// applied, with one object's change asserted in *both* backends at once.
//
// What makes this worth a scenario of its own is that the pattern is the one
// thing in this release that is entirely a claim about composition. Every part of
// it is already tested in isolation — informer sharing by pool size in
// internal/watch, per-sink dedup in internal/pipeline, the D12 limits by the
// archive scenario next door — and none of those say anything about the two
// halves running side by side over one informer. The claim documented in
// docs/TEE.md is that they compose with no interference and no special mode, and
// that is only observable from outside, in the two stores.
//
// Three properties, in the order they are asserted:
//
//   - The example applies. It is the manifest set a reader copies, rendered
//     through an overlay that patches one address (test/e2e/manifests/tee), so a
//     field renamed out from under examples/tee fails here rather than in
//     somebody's cluster.
//   - The two rules disagree about history, and say so. Same namespace, same
//     resources, same informer: HistoryReadable on the hot one, WriterOnlySink on
//     the cold one, both Ready. That asymmetry *is* the trade the pattern makes,
//     and it is the half a reader is most likely to disbelieve.
//   - One object's creation and one object's change reach both stores, typed for
//     each backend rather than uniformly. Added in ClickHouse and Snapshot in the
//     bucket, from the same event, at the same instant — which is possible only
//     because the dedup state that decides that tag is per sink.
//
// The fixture is this container's own, brought up and torn down here: the example
// ships its own MinIO, and Ginkgo randomises top-level containers, so borrowing
// the archive scenario's store would work or not depending on the seed.
//
// Runtime is about two and a half minutes with the images already side-loaded:
// the MinIO rollout, the S3Sink's first successful write probe once the bucket
// exists, and one rotation window (30s, the example's own) for each archive
// assertion.

// teeArchiveTimeout is how long an assertion about the *archive* side waits.
//
// A record is not visible until rotation closes the object holding it, and the
// example rotates every 30 seconds — so this is four rotation windows. That much
// slack is what makes a timeout here mean "the record was never written" rather
// than "the runner was busy": measured locally, each archive assertion settles in
// a little over one window, and anything short of two would be reporting latency
// as absence. The ClickHouse side of the same claim uses the suite default,
// because a batch there flushes in 200ms.
const teeArchiveTimeout = 2 * time.Minute

// teeImagePin matches the image an example's Deployment pins.
//
// The MinIO image has to be side-loaded into the kind node *before* the example
// is applied, or the kubelet spends the scenario's budget pulling it — and the
// suite must side-load the image the example actually names, not one it repeats
// in a constant. So it is read out of examples/tee/minio.yaml, which is the only
// place it is written down.
var teeImagePin = regexp.MustCompile(`(?m)^\s*image:\s*(\S+)\s*$`)

var _ = Describe("Phase 7 tee pattern scenario", Ordered, Serial, func() {
	BeforeAll(func() {
		By("side-loading the MinIO image the example pins")
		harness.SideloadImage(teeExampleImage())

		By("applying the tee example")
		// The overlay, not a hand-written manifest: examples/tee with one address
		// patched. Everything that carries meaning — both sinks, both rules, the
		// policies, the rotation window — is the example's own.
		applyKustomization(teeOverlay)
		DeferCleanup(teeTeardown)

		By("waiting for the example's MinIO to become ready")
		out, err := kubectl("rollout", "status", "deployment/"+teeMinIODeployment,
			"-n", teeMinIONamespace, "--timeout="+rolloutTimeout.String())
		Expect(err).NotTo(HaveOccurred(), "the example's MinIO never became ready: %s", out)

		By("reading the archive's coordinates back out of the cluster")
		// Out of the objects that were just applied rather than out of constants
		// here: the bucket, the prefix and the key pair are the example's values, and
		// reading them back makes it impossible for this suite to assert against a
		// store the example is not writing to.
		teeArchive.User = teeSecretValue(teeMinIOSecret, teeMinIONamespace,
			controller.DefaultAccessKeyIDSecretKey)
		teeArchive.Password = teeSecretValue(teeMinIOSecret, teeMinIONamespace,
			controller.DefaultSecretAccessKeySecretKey)
		teeArchive.Bucket = teeSinkField("bucket")
		teeArchive.Prefix = teeSinkField("prefix")

		By("creating the bucket, which kuberecord never does")
		// Through the example's own script rather than through the harness, which
		// could do it in one call: examples/tee/bucket.sh is the one step of that
		// README that is not a kubectl apply, and running it here is what stops it
		// from being the untested part of a tested example.
		//
		// It is also a real part of what is being asserted. The S3Sink's health probe
		// *writes*, so until the bucket exists the sink reports BucketReachable=False
		// and retries on its own schedule; the sink reaching Ready below therefore
		// proves the probe recovered once the bucket appeared, which is exactly the
		// sequence somebody standing this up for the first time will live through.
		out, err = utils.Run(exec.Command(teeBucketScript))
		Expect(err).NotTo(HaveOccurred(), "the example's bucket script failed: %s", out)

		By("waiting for both sinks to report themselves healthy")
		Eventually(func(g Gomega) {
			expectCondition(g, "clickhousesink", teeHotSinkName, "", v1alpha1.ConditionSchemaValid, statusTrue)
			expectCondition(g, "clickhousesink", teeHotSinkName, "", v1alpha1.ConditionReady, statusTrue)
			expectCondition(g, "s3sink", teeColdSinkName, "", v1alpha1.ConditionBucketReachable, statusTrue)
			expectCondition(g, "s3sink", teeColdSinkName, "", v1alpha1.ConditionReady, statusTrue)
		}, sinkReadyTimeout, pollInterval).Should(Succeed())

		By("waiting for both rules to report themselves streaming")
		expectRuleStreaming("streamrule", teeHotRule, teeNamespace)
		expectRuleStreaming("streamrule", teeColdRule, teeNamespace)
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		// Which half failed is the first question, and neither store answers it
		// alone: the log says what the write path did, and the bucket's key list says
		// what the cold half managed to write while the hot half was being asserted
		// on (or the other way round).
		By("dumping the controller-manager log")
		logs, err := kubectl("logs", "-l", operatorPodSelector, "-n", operatorNamespace, "--tail=200")
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to fetch controller logs: %v\n", err)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
		}

		By("listing the example's archive")
		keys, err := teeArchive.Keys()
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to list the example's bucket: %v\n", err)
			return
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "objects in %s/%s:\n%s\n",
			teeArchive.Bucket, teeArchive.Prefix, strings.Join(keys, "\n"))
	})

	It("reports one history verdict per sink on two rules watching one resource set", func() {
		// The pattern's central trade, asserted where a rule author actually reads
		// it. Both rules name the same kinds in the same namespace and are served by
		// the same informer; the only thing that differs is spec.sink, and this is
		// what that difference buys and costs.
		By("asserting the ClickHouse-bound rule reports its history as readable")
		Eventually(func(g Gomega) {
			history := expectCondition(g, "streamrule", teeHotRule, teeNamespace,
				v1alpha1.ConditionHistoryUnavailable, statusFalse)
			g.Expect(history.Reason).To(Equal(controller.ReasonHistoryReadable))
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting the S3-bound rule reports the Writer-only limit, naming its sink")
		Eventually(func(g Gomega) {
			history := expectCondition(g, "streamrule", teeColdRule, teeNamespace,
				v1alpha1.ConditionHistoryUnavailable, statusTrue)
			g.Expect(history.Reason).To(Equal(controller.ReasonWriterOnlySink))
			// Naming the sink matters more here than in a single-backend install: two
			// rules in this namespace mirror two different sinks' capabilities, and a
			// message that did not say which would be unreadable.
			g.Expect(history.Message).To(ContainSubstring(teeColdSinkName))
		}, ruleReadyTimeout, pollInterval).Should(Succeed())

		By("asserting neither rule is degraded by the other's verdict")
		// Ready deliberately ignores HistoryUnavailable, so a tee is two healthy
		// rules that disagree — not one healthy rule and one degraded one. A build
		// that rolled the limit into Ready would leave every archiving rule looking
		// broken forever, and this is where that would show up.
		Eventually(func(g Gomega) {
			expectCondition(g, "streamrule", teeHotRule, teeNamespace, v1alpha1.ConditionReady, statusTrue)
			expectCondition(g, "streamrule", teeColdRule, teeNamespace, v1alpha1.ConditionReady, statusTrue)
		}, ruleReadyTimeout, pollInterval).Should(Succeed())
	})

	It("records one object's life in both backends, typed for each", func() {
		By("waiting for the hot scope to open, which is also the warm barrier")
		// Two things at once. The Started row proves the ClickHouse-bound rule's scope
		// reached the sink — and because the warm is a query issued at the same
		// moment, it proves the dedup cache finished warming too. Only after that is
		// "Added" a sound claim rather than a lucky one: an object first seen by an
		// unwarmed scope is tagged Snapshot (see creationEvents), which is precisely
		// the tag the cold half is required to produce, and accepting either here
		// would make the whole comparison vacuous.
		started := eventuallyScopeRows(scopeQuery{
			Group: groupApps, Kind: kindDeployment, Namespace: teeNamespace,
			Action: string(sink.ScopeActionStarted),
		})
		Expect(started[0].RuleRef).To(Equal(streamRuleKey(teeNamespace, teeHotRule)),
			"the scope in ClickHouse was opened by some rule other than the hot one")

		By("applying the example's workload, with the tee already watching")
		// A separate step, from the example's own file, exactly as its README
		// instructs — and the ordering is the assertion's precondition, not tidiness.
		applyFileAs(teeWorkload)
		uid := eventuallyUID("deployment", teeDeployment, teeNamespace)
		object := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: teeNamespace, Name: teeDeployment, UID: uid,
		}

		By("asserting ClickHouse recorded the creation as Added, with full state")
		added := eventuallyExactlyOneRow(withEvent(object, eventAdded))
		Expect(added.Data).NotTo(BeEmpty(), "an Added row must carry the full normalized object")
		Expect(added.SHA256).NotTo(BeEmpty())
		Expect(added.Diff).To(BeEmpty(), "an Added row has nothing to diff against")
		Expect(added.Actors).To(ContainElement(fieldManager))
		Expect(added.Labels).To(HaveKeyWithValue("app", teeDeployment))

		By("asserting the archive recorded the same creation as a Snapshot, with full state")
		// The same object, the same UID, the same event, from the same informer — and
		// a different tag. Nothing went wrong: this backend cannot read its own
		// history, so "is this new, or merely new to me?" is unanswerable and Snapshot
		// is the safe answer. It is also the proof that the dedup state deciding the
		// tag is per sink: one shared cache could not have produced both.
		snapshot := teeArchive.EventuallyExactlyOneRecord(withEvent(object, eventSnapshot), teeArchiveTimeout)
		Expect(snapshot.Data).NotTo(BeEmpty(), "a Snapshot must carry the object's full state")
		Expect(snapshot.SHA256).NotTo(BeEmpty())
		Expect(snapshot.Diff).To(BeEmpty())
		Expect(snapshot.UID).To(Equal(uid), "the archive recorded a different object")
		Expect(snapshot.ClusterID).To(Equal(clusterID))

		By("asserting the archive recorded no Added for it, ever")
		// The other half of the same claim, and the one a passing Snapshot assertion
		// does not make: a sink that had somehow emitted both would satisfy the check
		// above.
		archived, err := teeArchive.Records(withEvent(object, eventAdded))
		Expect(err).NotTo(HaveOccurred())
		Expect(archived).To(BeEmpty(), "a Writer-only sink must never record an Added: %v", archived)

		By("scaling the Deployment, which is one event for two sinks")
		out, err := kubectl("scale", "deployment/"+teeDeployment, "-n", teeNamespace, "--replicas=2")
		Expect(err).NotTo(HaveOccurred(), "failed to scale the Deployment: %s", out)

		By("asserting both backends describe the scale as the same Modified diff")
		// Here the two agree, and that is the point of asserting it: only the *first*
		// sighting differs between the halves. Once an object is in a sink's dedup
		// cache, a change to it is diffed identically on both sides, because the
		// missing read half never mattered to a comparison against state this process
		// already holds.
		Eventually(func(g Gomega) {
			rows, err := resourceRows(withEvent(object, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(ContainElement(teeReplicaChange()), "no ClickHouse row describes the scale")
		}).Should(Succeed())

		Eventually(func(g Gomega) {
			records, err := teeArchive.Records(withEvent(object, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(records).To(ContainElement(teeReplicaChange()), "no archived record describes the scale")
		}, teeArchiveTimeout, pollInterval).Should(Succeed())
	})
})

// teeReplicaChange matches the diff-only record a scale produces, on either
// backend.
//
// One matcher for both because it is one claim: a resourceRow and a sink.Record
// are different types with the same two fields under these names, and writing the
// assertion twice would let the two halves of "both backends describe the same
// change" quietly stop meaning the same thing. HaveField reaches either by name.
func teeReplicaChange() types.GomegaMatcher {
	return SatisfyAll(
		HaveField("Diff", ContainSubstring("/spec/replicas")),
		HaveField("Data", BeEmpty()),
	)
}

// teeTeardown removes everything the example installed, in an order that lets
// each component finish what it is doing.
//
// The rules first, so nothing new is enqueued; then the workload's namespace;
// then the two sinks, so the operator drains what it is holding against backends
// that are still up; then the credentials and the store itself. It is registered
// with DeferCleanup rather than written as an AfterAll so that a failure *during*
// setup — a MinIO that never rolls out, a sink that never reaches Ready — still
// tears down what had been applied by then.
func teeTeardown() {
	By("removing the example's rules and demo namespace")
	deleteResourceQuietly("streamrule", teeHotRule, teeNamespace)
	deleteResourceQuietly("streamrule", teeColdRule, teeNamespace)
	deleteResourceQuietly("namespace", teeNamespace, "")

	By("removing the example's sinks")
	deleteResourceQuietly("clickhousesink", teeHotSinkName, "")
	deleteResourceQuietly("s3sink", teeColdSinkName, "")

	By("removing the example's operator-side credentials")
	deleteResourceQuietly("secret", teeS3CredentialsSecret, operatorNamespace)

	By("removing the example's object store")
	deleteResourceQuietly("namespace", teeMinIONamespace, "")
}

// teeExampleImage returns the container image examples/tee/minio.yaml pins.
//
// Reading it rather than repeating it is what makes "the suite side-loads the
// image the example uses" true by construction. A bumped image in the example
// with a stale constant here would not fail — it would simply make the kubelet
// pull, turning a bump into an intermittent timeout on machines with slow
// registry access.
func teeExampleImage() string {
	GinkgoHelper()
	root, err := utils.GetProjectDir()
	Expect(err).NotTo(HaveOccurred(), "failed to locate the project directory")
	raw, err := os.ReadFile(filepath.Join(root, teeMinIOExample))
	Expect(err).NotTo(HaveOccurred(), "failed to read %s", teeMinIOExample)

	match := teeImagePin.FindStringSubmatch(string(raw))
	Expect(match).NotTo(BeNil(), "%s pins no container image", teeMinIOExample)
	return match[1]
}

// teeSecretValue reads one key of a Secret the example created, failing the
// scenario if it is not there.
func teeSecretValue(name, namespace, key string) string {
	GinkgoHelper()
	value, err := secretValue(name, namespace, key)
	Expect(err).NotTo(HaveOccurred(), "failed to read %s/%s key %q", namespace, name, key)
	return value
}

// teeSinkField reads one spec field off the example's S3Sink, so the suite reads
// the archive the sink is actually writing to.
func teeSinkField(field string) string {
	GinkgoHelper()
	out, err := kubectl("get", "s3sink", teeColdSinkName, "-o", "jsonpath={.spec."+field+"}")
	Expect(err).NotTo(HaveOccurred(), "failed to read spec.%s of s3sink/%s: %s", field, teeColdSinkName, out)
	value := strings.TrimSpace(out)
	Expect(value).NotTo(BeEmpty(), "the example's S3Sink sets no spec.%s", field)
	return value
}
