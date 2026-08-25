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
	"time"

	. "github.com/onsi/gomega" //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/internal/sink"
	"github.com/kuberecord/kuberecord/test/harness"
)

// This file binds the shared acceptance vocabulary (test/harness) to this
// suite's fixtures, so the scenarios read as scenarios rather than as plumbing.
//
// The vocabulary itself — kubectl wrappers, ClickHouse querying, condition and
// pod decoding, manifest rendering — is shared with test/chaos on purpose: the
// two suites make the same claims about the same schema from opposite ends
// (happy path and failure path), and Task 2.1 requires the chaos restart
// scenario to reuse these assertions rather than paraphrase them. What stays
// here is only what this suite alone decides: which sink and cluster it queries,
// which field manager it applies under, and which pod it calls "the operator".

// ch is this suite's view of the backend. Password is filled in during
// BeforeSuite, once the credentials Secret has been read.
var ch = &harness.ClickHouse{
	Namespace:  clickHouseNamespace,
	Deployment: clickHouseDeployment,
	User:       clickHouseUser,
	Database:   clickHouseDatabase,
	ClusterID:  clusterID,
}

// mio is this suite's view of the S3 archive: the MinIO pod to exec into, the
// credentials the fixture and the operator share, and the bucket and prefix the
// S3Sink writes under.
//
// It is populated here rather than in the scenario for the reason this whole file
// exists: which store the suite reads, and as whom, is a property of the suite.
var mio = &harness.MinIO{
	Namespace:  minioNamespace,
	Deployment: minioDeployment,
	User:       s3AccessKeyID,
	Password:   s3SecretAccessKey,
	Bucket:     s3Bucket,
	Prefix:     s3Prefix,
	ClusterID:  clusterID,
}

// teeArchive is this suite's view of the *example's* object store (Task 7.1).
//
// A second view rather than a reuse of mio, because it addresses a different
// store: examples/tee ships its own MinIO, in its own namespace, under its own
// bucket. Keeping them separate is what lets the two scenarios run in either
// order — Ginkgo randomises top-level containers — and what keeps the archive
// scenario's whole-bucket layout assertion from meeting objects the tee example
// wrote.
//
// User, Password, Bucket and Prefix are deliberately absent here and filled in by
// the scenario from the cluster it just applied the example to. They are the
// example's values, not the suite's, and reading them back is what makes it
// impossible for this view to drift from the manifests CI applied.
var teeArchive = &harness.MinIO{
	Namespace:  teeMinIONamespace,
	Deployment: teeMinIODeployment,
	ClusterID:  clusterID,
}

// fieldManager is the field-manager name every object the suite applies is
// written under.
//
// It exists so the `actors` column has a value the assertions can name exactly.
// The column is harvested from metadata.managedFields, so an object created with
// a plain `kubectl create` would be attributed to whatever manager name that
// kubectl build happens to use; server-side-applying under a fixed manager makes
// "the applying manager appears in actors" a precise claim rather than a
// substring guess.
const fieldManager = "kuberecord-e2e"

// The shared types, spelled in this suite's own vocabulary. Aliases rather than
// wrappers: a scenario builds an objectFilter and hands it straight to the
// shared query layer, with no conversion to keep in step.
type (
	objectFilter    = harness.ObjectFilter
	scopeQuery      = harness.ScopeQuery
	resourceRow     = harness.ResourceRow
	scopeRow        = harness.ScopeRow
	kubeCondition   = harness.Condition
	operatorPodInfo = harness.PodInfo
	ruleResource    = harness.RuleResource
	redactionEntry  = harness.RedactionEntry
	// archiveRecord is one line of an S3 object: the logical sink.Record itself,
	// because that is what this backend stores (D9) — there is no physical row type
	// standing between the contract and the bytes, the way ResourceRow stands
	// between it and ClickHouse's columns.
	archiveRecord = sink.Record
)

// Event types, condition statuses, groups and kinds, as this suite spells them.
const (
	eventAdded    = harness.EventAdded
	eventModified = harness.EventModified
	eventDeleted  = harness.EventDeleted
	eventSnapshot = harness.EventSnapshot

	statusTrue  = harness.StatusTrue
	statusFalse = harness.StatusFalse

	groupCore       = harness.GroupCore
	groupApps       = harness.GroupApps
	groupNetworking = harness.GroupNetworking
	groupEvents     = harness.GroupEvents

	kindDeployment = harness.KindDeployment
	kindNode       = harness.KindNode
	kindIngress    = harness.KindIngress
	kindEvent      = harness.KindEvent
	kindConfigMap  = harness.KindConfigMap

	// sinkKindS3 is the spec.sink.kind an S3-bound rule names. It is the CRD's
	// enum value, spelled once here rather than at each scenario.
	sinkKindS3 = "S3Sink"
)

// creationEvents are the two tags an object's first appearance can carry — see
// harness.CreationEvents for which one it gets and why a count must accept both.
var creationEvents = harness.CreationEvents

func kubectl(args ...string) (string, error) { return harness.Kubectl(args...) }

func applyYAML(manifest string) { harness.ApplyYAML(fieldManager, manifest) }

// clientSideApplyYAML applies without --server-side, which is the only mode that
// writes kubectl.kubernetes.io/last-applied-configuration. Only the redaction
// scenario wants that; see harness.ClientSideApplyYAML.
func clientSideApplyYAML(manifest string) { harness.ClientSideApplyYAML(manifest) }

func applyFile(path string) { harness.ApplyFile(path) }

// applyFileAs and applyKustomization are how the tee scenario applies a
// *published* example rather than a suite-local copy of one: the overlay for its
// sinks and rules, the example's own file for the workload whose actors it then
// asserts on. See harness.ApplyKustomization.
func applyFileAs(path string) { harness.ApplyFileAs(fieldManager, path) }

func applyKustomization(dir string) { harness.ApplyKustomization(dir) }

func secretValue(name, namespace, key string) (string, error) {
	return harness.SecretValue(name, namespace, key)
}

func deleteResource(kind, name, namespace string) { harness.DeleteResource(kind, name, namespace) }

func deleteResourceQuietly(kind, name, namespace string) {
	harness.DeleteResourceQuietly(kind, name, namespace)
}

func createNamespace(name string) { harness.CreateNamespace(name) }

func expectCondition(g Gomega, kind, name, namespace, condType, status string) kubeCondition {
	return harness.ExpectCondition(g, kind, name, namespace, condType, status)
}

func resourceRows(filter objectFilter) ([]resourceRow, error) { return ch.ResourceRows(filter) }

// The archive's side of the same vocabulary. A scenario builds one objectFilter
// and asks either backend about it; only the verb changes — rows from ClickHouse,
// records from the bucket.
func archiveRecords(filter objectFilter) ([]archiveRecord, error) { return mio.Records(filter) }

func eventuallyExactlyOneRecord(filter objectFilter, timeout ...time.Duration) archiveRecord {
	return mio.EventuallyExactlyOneRecord(filter, timeout...)
}

func eventuallyRecordCount(filter objectFilter, want int, timeout ...time.Duration) []archiveRecord {
	return mio.EventuallyRecordCount(filter, want, timeout...)
}

func consistentlyRecordCount(filter objectFilter, want int, window ...time.Duration) {
	mio.ConsistentlyRecordCount(filter, want, window...)
}

// s3StreamRuleYAML renders a StreamRule pointed at this suite's S3Sink.
//
// It names the sink's kind, which a rule reaching anything other than a
// ClickHouseSink must: the CRD defaults spec.sink.kind, so a rule that left it out
// would look for a ClickHouseSink called "archive" and park with SinkMissing.
func s3StreamRuleYAML(namespace, name string, resources []ruleResource) string {
	return harness.StreamRuleYAMLForSinkKind(namespace, name, sinkKindS3, s3SinkName, resources)
}

func scopeRows(query scopeQuery) ([]scopeRow, error) { return ch.ScopeRows(query) }

func eventuallyUID(kind, name, namespace string) string {
	return harness.EventuallyUID(kind, name, namespace, ruleReadyTimeout)
}

func eventuallyExactlyOneRow(filter objectFilter, timeout ...time.Duration) resourceRow {
	return ch.EventuallyExactlyOneRow(filter, timeout...)
}

func eventuallyAnyRows(filter objectFilter) []resourceRow { return ch.EventuallyAnyRows(filter) }

func consistentlyRowCount(filter objectFilter, want int) { ch.ConsistentlyRowCount(filter, want) }

func eventuallyScopeRows(query scopeQuery) []scopeRow { return ch.EventuallyScopeRows(query) }

func operatorPods() ([]operatorPodInfo, error) {
	return harness.Pods(operatorPodSelector, operatorNamespace)
}

// theOperatorPod returns the single serving manager pod, failing if there is not
// exactly one — the shipped Deployment runs one replica, and any other count
// means a rollout is mid-flight and nothing read from it would be stable.
func theOperatorPod(g Gomega) operatorPodInfo {
	return harness.SolePod(g, operatorPodSelector, operatorNamespace)
}

// streamRuleYAML renders a StreamRule pointed at this suite's sink.
//
// The sink is bound here rather than at each scenario for the reason this whole
// file exists: which backend the suite streams to is a property of the suite, and
// a scenario that had to name it would be stating something it does not care
// about.
func streamRuleYAML(namespace, name string, resources []ruleResource) string {
	return harness.StreamRuleYAML(namespace, name, sinkName, resources)
}

// redactingStreamRuleYAML renders a StreamRule that scrubs the given paths out
// of every object it streams (Task 3.3).
func redactingStreamRuleYAML(namespace, name string, resources []ruleResource,
	redaction []redactionEntry) string {
	return harness.RedactingStreamRuleYAML(namespace, name, sinkName, resources, redaction)
}

func configMapYAML(namespace, name string, data map[string]string) string {
	return harness.ConfigMapYAML(namespace, name, data)
}

func clusterStreamRuleYAML(name string, resources []ruleResource) string {
	return harness.ClusterStreamRuleYAML(name, sinkName, resources)
}

func deploymentYAML(namespace, name string, replicas int) string {
	return harness.DeploymentYAML(namespace, name, replicas)
}

func ingressYAML(namespace, name string) string { return harness.IngressYAML(namespace, name) }

func crashLoopPodYAML(namespace, name string) string {
	return harness.CrashLoopPodYAML(namespace, name)
}
