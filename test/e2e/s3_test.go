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
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/controller"
	"github.com/kuberecord/kuberecord/test/harness"
)

// The Phase 6 gate: an S3Sink and a StreamRule streaming a Deployment's lifecycle
// to a real MinIO, with every assertion made by reading the bucket rather than by
// reading the operator's own state (Task 6.6).
//
// What this scenario is *for* is the part the archive cannot say for itself. A
// Writer-only sink (D12) produces an archive with no deletions in it and a full
// re-snapshot of everything after every restart, and an archive like that looks
// exactly like the archive of a cluster where nothing was ever deleted. So the
// assertions here are deliberately about the documented behaviour rather than
// about the behaviour a queryable backend would have — the same discipline the
// v0.1.0 suite applied to the reincarnation gap, where the honest claim was also
// "this is what the design says happens" rather than "this is what one would
// wish for":
//
//   - An object's first sighting is a permanent Snapshot carrying full state, never
//     an Added. The cache cannot be warmed from this backend, so "is this new?" is
//     a question it can never answer, and Snapshot is the safe answer to it.
//   - A change to an object this process has already seen is an ordinary Modified
//     with a diff. In-process dedup state is not affected by the missing read half.
//   - After a restart the live object is re-snapshotted in full, and the object
//     deleted during the outage produces nothing at all. Its last record is its
//     last observed state, and the absence of anything after that is the only
//     evidence it is gone.
//
// The fixture — MinIO, the credentials, the bucket, the S3Sink and the rule — is
// brought up by this container rather than in BeforeSuite, so a focused run of the
// ClickHouse scenarios (which is what the install-path smokes are) pays none of
// its cost. Measured at about two minutes end to end with the image already
// pulled: the side-load and rollout, the sink's first write probe, three rotation
// windows at the CRD's 10-second floor, the operator going down and coming back,
// and one quiet window holding the absence claims open.

// Per-scenario fixtures. The namespace is this scenario's alone, so a count over
// it can never see another scenario's objects.
const (
	archiveNamespace = "s3archive"
	archiveRule      = "s3archive-deployments"
	// archivedDeployment lives through the restart and must be re-snapshotted after
	// it; vanishedDeployment is deleted while the operator is down and must produce
	// no Deleted record, ever.
	archivedDeployment = "archived"
	vanishedDeployment = "vanished"
)

// archiveQuietWindow is how long this scenario's absence claims are held open.
//
// It is longer than the suite's usual quiet window for a reason specific to this
// backend: a record is not visible until rotation closes the object holding it, so
// a window shorter than the sink's maxObjectAge (10s) would report the absence of
// a record that is merely still in a worker's open object. Three rotation periods
// is long enough for a wrong record to have been written, closed, and listed.
const archiveQuietWindow = 35 * time.Second

var _ = Describe("Phase 6 archive acceptance scenario", Ordered, Serial, func() {
	BeforeAll(func() {
		By("side-loading the MinIO image into the kind node")
		harness.SideloadImage(minioImage)

		By("creating the MinIO namespace and its root credentials")
		createNamespace(minioNamespace)
		createKeyPairSecret(minioSecret, minioNamespace)

		By("deploying MinIO")
		applyFile(minioManifest)
		out, err := kubectl("rollout", "status", "deployment/"+minioDeployment,
			"-n", minioNamespace, "--timeout="+rolloutTimeout.String())
		Expect(err).NotTo(HaveOccurred(), "MinIO never became ready: %s", out)

		By("creating the archive bucket")
		// Before the sink, deliberately: the sink's health probe *writes*, so a
		// bucket that did not exist yet would make the sink's first reported status a
		// failure it then recovered from — slower, and confusing to anyone watching.
		mio.MakeBucket()

		By("giving the operator the same key pair")
		// In the operator's own namespace, the only one it may read Secrets from
		// (Task 1.9). Created before the S3Sink so the very first reconcile resolves
		// it, rather than reporting CredentialsResolved=False and waiting for the
		// Secret watch to bring it back.
		createKeyPairSecret(s3CredentialsSecret, operatorNamespace)

		By("creating the S3Sink and waiting for it to report itself writable")
		// Reaching Ready is a real assertion, not just a barrier: the probe writes an
		// object, so Ready means the operator resolved the Secret, built a client from
		// the endpoint and path-style settings, and successfully put an object in this
		// bucket. A HEAD would have passed for a read-only credential.
		applyFile(s3SinkManifest)
		Eventually(func(g Gomega) {
			expectCondition(g, "s3sink", s3SinkName, "", v1alpha1.ConditionCredentialsResolved, statusTrue)
			expectCondition(g, "s3sink", s3SinkName, "", v1alpha1.ConditionBucketReachable, statusTrue)
			expectCondition(g, "s3sink", s3SinkName, "", v1alpha1.ConditionReady, statusTrue)
		}, sinkReadyTimeout, pollInterval).Should(Succeed())

		By("creating the namespace and a StreamRule naming the S3Sink")
		createNamespace(archiveNamespace)
		applyYAML(s3StreamRuleYAML(archiveNamespace, archiveRule, []ruleResource{
			{Group: groupApps, Version: "v1", Kind: kindDeployment},
		}))
		expectRuleStreaming("streamrule", archiveRule, archiveNamespace)
	})

	AfterAll(func() {
		// Ordered so each component still has what it needs while it shuts down: the
		// rule first, so nothing new is enqueued; then the sink, so the operator
		// drains what it holds against a MinIO that is still up; then the credentials
		// and the store itself.
		By("removing the rule and its namespace")
		deleteResourceQuietly("streamrule", archiveRule, archiveNamespace)
		deleteResourceQuietly("namespace", archiveNamespace, "")

		By("removing the S3Sink")
		deleteResourceQuietly("s3sink", s3SinkName, "")

		By("removing the operator's S3 credentials")
		deleteResourceQuietly("secret", s3CredentialsSecret, operatorNamespace)

		By("removing the MinIO fixture")
		deleteResourceQuietly("namespace", minioNamespace, "")
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		// Two things explain a failure here and neither is in the assertion: the
		// operator's log says why a record was not written, and the bucket's key list
		// says what *was* written instead — which, for an archive, is the whole
		// question.
		By("dumping the controller-manager log")
		logs, err := kubectl("logs", "-l", operatorPodSelector, "-n", operatorNamespace, "--tail=200")
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to fetch controller logs: %v\n", err)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
		}

		By("listing the archive")
		keys, err := mio.Keys()
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "failed to list the bucket: %v\n", err)
			return
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "objects in %s/%s:\n%s\n",
			s3Bucket, s3Prefix, strings.Join(keys, "\n"))
	})

	It("reports its own inability to read history, on the sink and on the rule", func() {
		// The criterion that keeps this release honest (Task 6.5), asserted against a
		// real cluster: the limitation is *reported*, not left to be discovered from
		// record counts months later. And Ready stays True, because this is a declared
		// capability limit and not a fault.
		By("asserting the sink reports HistoryUnavailable while staying Ready")
		Eventually(func(g Gomega) {
			history := expectCondition(g, "s3sink", s3SinkName, "",
				v1alpha1.ConditionHistoryUnavailable, statusTrue)
			g.Expect(history.Reason).To(Equal(controller.ReasonWriterOnlySink))
			// The message has to name the three disabled behaviours and the
			// consequence: it is the only place an operator is told what this archive
			// will and will not contain.
			g.Expect(history.Message).To(SatisfyAll(
				ContainSubstring("warm-up"),
				ContainSubstring("garbage collection"),
				ContainSubstring("boot reconciliation"),
				ContainSubstring("Snapshot"),
			))
			expectCondition(g, "s3sink", s3SinkName, "", v1alpha1.ConditionReady, statusTrue)
		}, sinkReadyTimeout, pollInterval).Should(Succeed())

		By("asserting the rule says so too, in its own words")
		// A rule's author may not own the sink it names and may never look at it, so a
		// rule that reported only Ready=True would be telling them their stream is
		// fine while leaving the missing deletions to be discovered later.
		Eventually(func(g Gomega) {
			history := expectCondition(g, "streamrule", archiveRule, archiveNamespace,
				v1alpha1.ConditionHistoryUnavailable, statusTrue)
			g.Expect(history.Reason).To(Equal(controller.ReasonWriterOnlySink))
			g.Expect(history.Message).To(ContainSubstring(s3SinkName))
		}, ruleReadyTimeout, pollInterval).Should(Succeed())
	})

	It("archives a Deployment's lifecycle as rotated, compressed JSONL objects", func() {
		By("creating two Deployments under the archiving rule")
		applyYAML(deploymentYAML(archiveNamespace, archivedDeployment, 1))
		applyYAML(deploymentYAML(archiveNamespace, vanishedDeployment, 1))
		archivedUID := eventuallyUID("deployment", archivedDeployment, archiveNamespace)
		vanishedUID := eventuallyUID("deployment", vanishedDeployment, archiveNamespace)

		archived := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: archiveNamespace, Name: archivedDeployment, UID: archivedUID,
		}
		vanished := objectFilter{
			Group: groupApps, Kind: kindDeployment,
			Namespace: archiveNamespace, Name: vanishedDeployment, UID: vanishedUID,
		}

		By("asserting each object's first sighting is one full Snapshot")
		// Snapshot exactly, not "either creation event": on this backend the tag is
		// not a race between warm-up and the informer's initial list, it is the
		// documented and only possible outcome. Accepting Added as well would let a
		// sink that had somehow warmed itself pass.
		first := eventuallyExactlyOneRecord(withEvent(archived, eventSnapshot))
		Expect(first.Data).NotTo(BeEmpty(), "a Snapshot must carry the object's full normalized state")
		Expect(first.SHA256).NotTo(BeEmpty())
		Expect(first.Diff).To(BeEmpty(), "a Snapshot has nothing to diff against")
		Expect(first.Actors).To(ContainElement(fieldManager))
		Expect(first.APIVersion).To(Equal("v1"))
		Expect(first.Labels).To(HaveKeyWithValue("app", archivedDeployment))
		Expect(first.ClusterID).To(Equal(clusterID))
		eventuallyExactlyOneRecord(withEvent(vanished, eventSnapshot))

		By("scaling the archived Deployment")
		applyYAML(deploymentYAML(archiveNamespace, archivedDeployment, 2))

		By("asserting a Modified record whose diff is the replica change")
		// Modified with a diff and no full state: the in-process dedup cache is
		// unaffected by the missing read half, so a change to an object this process
		// already holds is diffed exactly as it would be for a queryable backend.
		Eventually(func(g Gomega) {
			records, err := archiveRecords(withEvent(archived, eventModified))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(records).To(ContainElement(SatisfyAll(
				HaveField("Diff", ContainSubstring("/spec/replicas")),
				HaveField("Data", BeEmpty()),
				HaveField("Actors", ContainElement(fieldManager)),
			)), "no Modified record describes the scale")
		}).Should(Succeed())

		By("asserting every object landed at the documented key layout")
		assertArchiveLayout()

		By("taking the operator down")
		scaleOperator(0)
		DeferCleanup(func() {
			// If an assertion below fails mid-outage, the suite must not leave the
			// operator scaled to zero for every later spec.
			scaleOperator(1)
		})

		By("deleting one Deployment while nothing is watching")
		deleteResource("deployment", vanishedDeployment, archiveNamespace)

		By("bringing the operator back")
		scaleOperator(1)

		By("asserting the live object was re-snapshotted in full")
		// The second Snapshot is the documented cost of a Writer-only sink: a
		// restarting operator cannot learn what the archive already holds, so it
		// records everything in scope again. Exactly two, not "at least two" — an
		// archive that re-snapshotted in a loop would satisfy a lower bound too, and
		// would be a very different bill.
		snapshots := eventuallyRecordCount(withEvent(archived, eventSnapshot), 2, restartTimeout)
		for i, snapshot := range snapshots {
			// Both are asserted rather than "the second one": two objects in one hour
			// partition are listed in content-hash order, not chronological order, so
			// there is no positional "second". Both must carry full state anyway —
			// that is what makes a re-snapshot a usable record rather than a marker.
			Expect(snapshot.Data).NotTo(BeEmpty(), "Snapshot %d carries no full state", i)
			Expect(snapshot.SHA256).NotTo(BeEmpty(), "Snapshot %d carries no hash", i)
		}

		By("asserting the offline deletion produced no record at all")
		// The heart of D12, and the claim this whole scenario exists for. Zombie
		// garbage collection is off for this sink, so the deletion is simply never
		// recorded: the archive's last word on that object is its last observed state.
		//
		// It is held open over the whole namespace rather than over the one object,
		// because the mechanism is per-scope — a claim about a single object could
		// pass by luck — and for longer than a rotation period, so this is an absence
		// rather than a record still sitting in an open object.
		consistentlyRecordCount(withEvent(namespaceFilter(), eventDeleted), 0, archiveQuietWindow)

		By("asserting nothing in the archive was ever recorded as Added")
		// The other half of what the sink's condition promises, checked in the bucket
		// over every record this rule has written: an object seen for the first time
		// by this process is never Added, because this backend cannot tell "new" from
		// "unseen by me".
		added, err := archiveRecords(withEvent(namespaceFilter(), eventAdded))
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(BeEmpty(), "a Writer-only sink must never record an Added: %v", added)

		By("asserting the deleted object's own history still ends where it ended")
		// Its records are neither rewritten nor removed by its disappearance — that is
		// what makes the archive's silence readable rather than merely empty. A single
		// read is enough: objects in an archive do not change, so there is nothing for
		// a quiet window to catch here.
		survivors, err := archiveRecords(vanished)
		Expect(err).NotTo(HaveOccurred())
		Expect(survivors).NotTo(BeEmpty(), "the deleted object's history is gone from the archive")
		for _, record := range survivors {
			Expect(record.EventType).NotTo(Equal(eventDeleted))
		}
	})
})

// namespaceFilter matches every record this scenario's rule could have written:
// its kind, in its namespace, whatever the object and whatever the event.
func namespaceFilter() objectFilter {
	return objectFilter{Group: groupApps, Kind: kindDeployment, Namespace: archiveNamespace}
}

// createKeyPairSecret creates an access-key Secret under the key names the S3Sink
// reconciler reads (accessKeyId / secretAccessKey).
//
// Both sides of the connection are created from it: the MinIO fixture takes its
// root credentials from one, and the operator takes the sink's credentials from
// another with the same contents. Rendering them from the same two constants is
// what makes "the operator authenticates as the identity this store knows" true by
// construction rather than by review.
func createKeyPairSecret(name, namespace string) {
	GinkgoHelper()
	// Recreated rather than reused: the suite reuses whatever kind cluster it finds,
	// and a Secret left behind by an interrupted run could hold a different key.
	deleteResourceQuietly("secret", name, namespace)
	out, err := kubectl("create", "secret", "generic", name, "-n", namespace,
		"--from-literal="+controller.DefaultAccessKeyIDSecretKey+"="+s3AccessKeyID,
		"--from-literal="+controller.DefaultSecretAccessKeySecretKey+"="+s3SecretAccessKey)
	Expect(err).NotTo(HaveOccurred(), "failed to create Secret %s/%s: %s", namespace, name, out)
}

// assertArchiveLayout checks every object in the bucket against the published key
// layout (D15).
//
// It is asserted from the outside, spelled out here, because the layout is a
// public contract that readers depend on — the documented DuckDB and Athena
// recipes, and anyone else pointing a query engine at the bucket. Three claims:
// the records tree is where the contract says it is, the scope log is beside it
// rather than inside it, and nothing else is in the bucket except the operator's
// own health probe, which sits outside format=jsonl-v1 precisely so that no
// reader's glob ever meets it.
func assertArchiveLayout() {
	GinkgoHelper()
	keys, err := mio.Keys()
	Expect(err).NotTo(HaveOccurred(), "failed to list the archive")
	Expect(keys).NotTo(BeEmpty(), "the bucket is empty, so this would assert nothing")

	recordPrefix := s3Prefix + "/format=jsonl-v1/cluster_id=" + clusterID + "/"
	scopePrefix := s3Prefix + "/format=jsonl-v1/scopes/date="
	var records, scopes int
	for _, key := range keys {
		switch {
		case mio.IsRecordKey(key):
			records++
			Expect(key).To(HavePrefix(recordPrefix),
				"a record object sits outside the documented cluster partition")
			Expect(key).To(MatchRegexp(`/date=\d{4}-\d{2}-\d{2}/hour=\d{2}/[0-9a-f]{64}\.jsonl\.zst$`),
				"a record object's key does not end in date=/hour=/<sha256>.jsonl.zst")
		case mio.IsScopeKey(key):
			scopes++
			Expect(key).To(HavePrefix(scopePrefix),
				"a scope object sits outside the documented scope partition")
		case mio.IsProbeKey(key):
			Expect(key).To(Equal(s3Prefix+"/.kuberecord-probe"),
				"the health probe's object is not where the sink documents it")
		default:
			Fail(fmt.Sprintf("object %q is neither a record object, a scope object nor the health "+
				"probe; a reader globbing this archive would not know what to do with it", key))
		}
	}
	Expect(records).To(BeNumerically(">", 0), "the archive holds no record objects")
	// The scope log is what makes a gap in the records explicable rather than merely
	// empty, and on this backend it is written but never reconciled — so its presence
	// is the only thing that says when watching began.
	Expect(scopes).To(BeNumerically(">", 0), "the archive holds no scope objects")
}
