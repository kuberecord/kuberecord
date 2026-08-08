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

	"github.com/yelzhy/kuberecord/test/harness"
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

// fieldManager is the field-manager name every object the suite applies is
// written under.
//
// It exists so the `actors` column has a value the assertions can name exactly.
// The column is harvested from metadata.managedFields, so an object created with
// a plain `kubectl create` would be attributed to whatever manager name that
// kubectl build happens to use; server-side-applying under a fixed manager makes
// "the applying manager appears in actors" a precise claim rather than a
// substring guess.
const fieldManager = "kubestream-e2e"

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

func deleteResource(kind, name, namespace string) { harness.DeleteResource(kind, name, namespace) }

func deleteResourceQuietly(kind, name, namespace string) {
	harness.DeleteResourceQuietly(kind, name, namespace)
}

func createNamespace(name string) { harness.CreateNamespace(name) }

func expectCondition(g Gomega, kind, name, namespace, condType, status string) kubeCondition {
	return harness.ExpectCondition(g, kind, name, namespace, condType, status)
}

func resourceRows(filter objectFilter) ([]resourceRow, error) { return ch.ResourceRows(filter) }

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

func streamRuleYAML(namespace, name string, resources []ruleResource) string {
	return harness.StreamRuleYAML(namespace, name, resources)
}

// redactingStreamRuleYAML renders a StreamRule that scrubs the given paths out
// of every object it streams (Task 3.3).
func redactingStreamRuleYAML(namespace, name string, resources []ruleResource,
	redaction []redactionEntry) string {
	return harness.RedactingStreamRuleYAML(namespace, name, resources, redaction)
}

func configMapYAML(namespace, name string, data map[string]string) string {
	return harness.ConfigMapYAML(namespace, name, data)
}

func clusterStreamRuleYAML(name string, resources []ruleResource) string {
	return harness.ClusterStreamRuleYAML(name, resources)
}

func deploymentYAML(namespace, name string, replicas int) string {
	return harness.DeploymentYAML(namespace, name, replicas)
}

func ingressYAML(namespace, name string) string { return harness.IngressYAML(namespace, name) }

func crashLoopPodYAML(namespace, name string) string {
	return harness.CrashLoopPodYAML(namespace, name)
}
