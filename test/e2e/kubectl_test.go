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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kubestream/test/utils"
)

// This file holds the cluster-side vocabulary the scenarios are written in:
// thin kubectl wrappers, condition reading, and the manifests the suite
// generates per scenario.
//
// Manifests that vary per scenario (a namespace, an object name, a replica
// count) are built as strings and applied through stdin rather than templated
// onto disk: there is no temporary file to clean up, the YAML is readable next
// to the assertion that depends on it, and a scenario cannot accidentally
// inherit another's leftover file.

// fieldManager is the field-manager name every object the suite applies is
// written under.
//
// It exists so the `actors` column has a value the assertions can name exactly.
// The column is harvested from metadata.managedFields, so an object created
// with a plain `kubectl create` would be attributed to whatever manager name
// that kubectl build happens to use; server-side-applying under a fixed manager
// makes "the applying manager appears in actors" a precise claim rather than a
// substring guess.
const fieldManager = "kubestream-e2e"

// kubectl runs one kubectl invocation from the project root and returns its
// combined output.
func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

// kubectlStdin runs kubectl with manifest piped to its stdin.
func kubectlStdin(manifest string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	return utils.Run(cmd)
}

// applyYAML server-side-applies a manifest under the suite's field manager.
// Server-side apply (rather than the client-side default) is what puts
// fieldManager into the object's managedFields, and therefore into the actors
// column the happy-path scenario asserts on.
func applyYAML(manifest string) {
	GinkgoHelper()
	out, err := kubectlStdin(manifest, "apply", "--server-side",
		"--field-manager="+fieldManager, "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "failed to apply manifest: %s", out)
}

// applyFile applies a manifest that lives in the repository. The path is
// relative to the project root, which is the directory utils.Run runs in.
func applyFile(path string) {
	GinkgoHelper()
	out, err := kubectl("apply", "-f", path)
	Expect(err).NotTo(HaveOccurred(), "failed to apply %s: %s", path, out)
}

// deleteResource removes one object and waits for it to be gone. Failures are
// reported, not swallowed: a delete that silently did nothing would leave the
// next scenario running against state it did not create (Invariant 4's spirit,
// applied to the suite itself).
func deleteResource(kind, name, namespace string) {
	GinkgoHelper()
	args := []string{"delete", kind, name, "--ignore-not-found", "--wait"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := kubectl(args...)
	Expect(err).NotTo(HaveOccurred(), "failed to delete %s/%s: %s", kind, name, out)
}

// deleteResourceQuietly is deleteResource for cleanup paths that must not fail
// a spec — an AfterEach tidying up after an assertion has already failed, where
// a second failure would only bury the first.
func deleteResourceQuietly(kind, name, namespace string) {
	args := []string{"delete", kind, name, "--ignore-not-found", "--wait"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	if out, err := kubectl(args...); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: deleting %s/%s: %v\n%s", kind, name, err, out)
	}
}

// createNamespace makes a namespace exist and be empty, deleting any previous
// incarnation first.
//
// The delete is what makes the suite recoverable. Scenarios assert exact row
// counts, so each needs to start from a namespace with nothing in it — but the
// suite also reuses whatever Kind cluster it finds, so refusing to run when the
// namespace already exists would let one interrupted run poison every run after
// it. Recreating gives the isolation the assertions need and repairs a dirty
// cluster on the way, instead of demanding a clean one.
//
// Within a run this can never mask a collision: every scenario uses its own
// namespace, so the only thing that could be here is debris from a previous run,
// which is exactly what should be cleared.
func createNamespace(name string) {
	GinkgoHelper()
	out, err := kubectl("delete", "namespace", name, "--ignore-not-found", "--wait")
	Expect(err).NotTo(HaveOccurred(), "failed to clear namespace %s: %s", name, out)
	out, err = kubectl("create", "namespace", name)
	Expect(err).NotTo(HaveOccurred(), "failed to create namespace %s: %s", name, out)
}

// objectUID reads an object's UID, which is the identity the sink rows are
// keyed by in every reincarnation and offline-deletion assertion.
func objectUID(kind, name, namespace string) (string, error) {
	args := []string{"get", kind, name, "-o", "jsonpath={.metadata.uid}"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := kubectl(args...)
	if err != nil {
		return "", err
	}
	uid := strings.TrimSpace(out)
	if uid == "" {
		return "", fmt.Errorf("%s/%s has no UID yet", kind, name)
	}
	return uid, nil
}

// Condition statuses, as metav1.ConditionStatus renders them in JSON. Spelled
// as constants because every scenario asserts on one of the two, and "true"
// would silently never match.
const (
	statusTrue  = "True"
	statusFalse = "False"
)

// kubeCondition is the subset of metav1.Condition the suite asserts on.
type kubeCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

// conditionOf reads one status condition off any kubestream CR.
func conditionOf(kind, name, namespace, condType string) (kubeCondition, error) {
	args := []string{"get", kind, name, "-o", "jsonpath={.status.conditions}"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := kubectl(args...)
	if err != nil {
		return kubeCondition{}, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return kubeCondition{}, fmt.Errorf("%s/%s has no status conditions yet", kind, name)
	}

	var conditions []kubeCondition
	if err := json.Unmarshal([]byte(out), &conditions); err != nil {
		return kubeCondition{}, fmt.Errorf("decode conditions of %s/%s from %q: %w", kind, name, out, err)
	}
	for _, condition := range conditions {
		if condition.Type == condType {
			return condition, nil
		}
	}
	return kubeCondition{}, fmt.Errorf("%s/%s has no %s condition (has %d others)", kind, name, condType, len(conditions))
}

// expectCondition asserts, within g, that a CR carries condType with the given
// status, and returns the condition so a caller can go on to assert its reason.
func expectCondition(g Gomega, kind, name, namespace, condType, status string) kubeCondition {
	condition, err := conditionOf(kind, name, namespace, condType)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(condition.Status).To(Equal(status),
		"%s/%s condition %s: reason=%s message=%s", kind, name, condType, condition.Reason, condition.Message)
	return condition
}

// operatorPodInfo is what the suite needs to know about a manager pod: which
// one it is, whether it is serving, and whether it has crashed.
//
// RestartCount is the field the RBAC scenario turns into its "zero restarts"
// claim: an operator that crash-looped its way back to health would satisfy
// "the condition eventually flipped" while violating Invariant 5, and only the
// restart count tells the two apart.
type operatorPodInfo struct {
	Name         string
	Phase        string
	Ready        bool
	RestartCount int
	// Terminating marks a pod that has a deletionTimestamp but has not gone yet.
	//
	// The distinction is load-bearing in both directions, which is why it is
	// reported rather than filtered away here. Asking "which pod is serving?"
	// must ignore a terminating pod, or it sees two. Asking "is the operator
	// down?" must count it, because a pod inside its termination grace period is
	// still running — treating it as gone would let the restart scenario delete
	// objects while the operator was still watching, and quietly stop testing the
	// offline path it exists to test.
	Terminating bool
}

// operatorPods lists every manager pod, terminating ones included.
func operatorPods() ([]operatorPodInfo, error) {
	out, err := kubectl("get", "pods", "-l", operatorPodSelector, "-n", operatorNamespace, "-o", "json")
	if err != nil {
		return nil, err
	}

	var list struct {
		Items []struct {
			Metadata struct {
				Name              string `json:"name"`
				DeletionTimestamp string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, fmt.Errorf("decode operator pod list: %w", err)
	}

	var pods []operatorPodInfo
	for _, item := range list.Items {
		pod := operatorPodInfo{
			Name:        item.Metadata.Name,
			Phase:       item.Status.Phase,
			Terminating: item.Metadata.DeletionTimestamp != "",
		}
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" {
				pod.Ready = condition.Status == statusTrue
			}
		}
		for _, container := range item.Status.ContainerStatuses {
			pod.RestartCount += container.RestartCount
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// theOperatorPod returns the single serving manager pod, failing if there is not
// exactly one — the shipped Deployment runs one replica, and any other count
// means a rollout is mid-flight and nothing read from it would be stable.
// Terminating pods do not count as serving.
func theOperatorPod(g Gomega) operatorPodInfo {
	pods, err := operatorPods()
	g.Expect(err).NotTo(HaveOccurred())
	serving := make([]operatorPodInfo, 0, len(pods))
	for _, pod := range pods {
		if !pod.Terminating {
			serving = append(serving, pod)
		}
	}
	g.Expect(serving).To(HaveLen(1), "expected exactly one serving controller-manager pod, got %v", pods)
	return serving[0]
}

// ruleResource is one entry of a rule's spec.resources.
type ruleResource struct {
	Group   string
	Version string
	Kind    string
}

// resourcesYAML renders a rule's spec.resources list at the given indent.
func resourcesYAML(resources []ruleResource, indent string) string {
	var b strings.Builder
	for _, r := range resources {
		fmt.Fprintf(&b, "%s- group: %q\n%s  version: %q\n%s  kind: %q\n", indent, r.Group, indent, r.Version, indent, r.Kind)
	}
	return b.String()
}

// streamRuleYAML renders a namespaced StreamRule. sinkRef is left out on
// purpose: it defaults to "default", which is the sink the suite installs, and
// spelling it would only hide that the default works.
func streamRuleYAML(namespace, name string, resources []ruleResource) string {
	return fmt.Sprintf(`apiVersion: kubestream.io/v1alpha1
kind: StreamRule
metadata:
  name: %s
  namespace: %s
spec:
  resources:
%s`, name, namespace, resourcesYAML(resources, "  "))
}

// clusterStreamRuleYAML renders a cluster-scoped ClusterStreamRule with no
// namespaceSelector, i.e. one all-namespaces target per named resource.
func clusterStreamRuleYAML(name string, resources []ruleResource) string {
	return fmt.Sprintf(`apiVersion: kubestream.io/v1alpha1
kind: ClusterStreamRule
metadata:
  name: %s
spec:
  resources:
%s`, name, resourcesYAML(resources, "  "))
}

// deploymentYAML renders the object the workload scenarios stream.
//
// The pause image is what every kind node already has cached, so no scenario
// ever waits on a registry pull; whether the pods actually run is irrelevant,
// since what is being watched is the Deployment object itself.
func deploymentYAML(namespace, name string, replicas int) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
        imagePullPolicy: IfNotPresent
`, name, namespace, name, replicas, name, name)
}

// ingressYAML renders a minimal, valid Ingress. It needs no ingress controller
// to exist: the scenario watches the object, not the traffic.
func ingressYAML(namespace, name string) string {
	return fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: %s
  namespace: %s
spec:
  rules:
  - host: %s.e2e.kubestream.io
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: %s
            port:
              number: 80
`, name, namespace, name, name)
}
