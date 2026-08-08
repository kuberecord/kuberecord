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
	"os"
	"path/filepath"
	"strconv"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
)

// serviceAccountName created for the project
const serviceAccountName = "kuberecord-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "kuberecord-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "kuberecord-metrics-binding"

// This container covers the manager as a *process*: that it runs, and that it
// serves its metrics endpoint under authn/authz. The operator's actual behaviour
// — rules, watches, rows — is the subject of scenarios_test.go.
//
// It runs first, before any scenario touches the cluster, because the pod
// identity it reads must be the one the deploy created; the restart scenario
// later replaces it on purpose.
var _ = Describe("Manager", Ordered, Serial, func() {
	var controllerPodName string

	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		deleteResourceQuietly("pod", "curl-metrics", operatorNamespace)

		By("removing the metrics ClusterRoleBinding")
		deleteResourceQuietly("clusterrolebinding", metricsRoleBindingName, "")
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		By("Fetching controller manager pod logs")
		controllerLogs, err := kubectl("logs", controllerPodName, "-n", operatorNamespace)
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
		}

		By("Fetching Kubernetes events")
		eventsOutput, err := kubectl("get", "events", "-n", operatorNamespace, "--sort-by=.lastTimestamp")
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
		}

		By("Fetching curl-metrics logs")
		metricsOutput, err := getMetricsOutput()
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
		}

		By("Fetching controller manager pod description")
		podDescription, err := kubectl("describe", "pod", controllerPodName, "-n", operatorNamespace)
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Pod description:\n%s", podDescription)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to describe controller pod\n")
		}
	})

	It("should run successfully", func() {
		By("validating that the controller-manager pod is running as expected")
		Eventually(func(g Gomega) {
			pod := theOperatorPod(g)
			g.Expect(pod.Name).To(ContainSubstring("controller-manager"))
			g.Expect(pod.Phase).To(Equal("Running"), "Incorrect controller-manager pod status")
			controllerPodName = pod.Name
		}).Should(Succeed())
	})

	It("should ensure the metrics endpoint is serving metrics", func() {
		By("creating a ClusterRoleBinding for the service account to allow access to metrics")
		out, err := kubectl("create", "clusterrolebinding", metricsRoleBindingName,
			"--clusterrole=kuberecord-metrics-reader",
			fmt.Sprintf("--serviceaccount=%s:%s", operatorNamespace, serviceAccountName),
		)
		Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding: %s", out)

		By("validating that the metrics service is available")
		_, err = kubectl("get", "service", metricsServiceName, "-n", operatorNamespace)
		Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

		By("getting the service account token")
		token, err := serviceAccountToken()
		Expect(err).NotTo(HaveOccurred())
		Expect(token).NotTo(BeEmpty())

		By("ensuring the controller pod is ready")
		Eventually(func(g Gomega) {
			g.Expect(theOperatorPod(g).Ready).To(BeTrue(), "Controller pod not ready")
		}).Should(Succeed())

		By("verifying that the controller manager is serving the metrics server")
		Eventually(func(g Gomega) {
			logs, err := kubectl("logs", controllerPodName, "-n", operatorNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring("Serving metrics server"), "Metrics server not yet started")
		}).Should(Succeed())

		// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

		By("creating the curl-metrics pod to access the metrics endpoint")
		out, err = kubectl("run", "curl-metrics", "--restart=Never",
			"--namespace", operatorNamespace,
			"--image=curlimages/curl:latest",
			"--overrides", curlPodOverrides(token))
		Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod: %s", out)

		By("waiting for the curl-metrics pod to complete.")
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "pods", "curl-metrics", "-o", "jsonpath={.status.phase}",
				"-n", operatorNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Succeeded"), "curl pod in wrong status")
		}, rolloutTimeout, pollInterval).Should(Succeed())

		By("getting the metrics by checking curl-metrics logs")
		Eventually(func(g Gomega) {
			metricsOutput, err := getMetricsOutput()
			g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
			g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
		}).Should(Succeed())
	})

	// +kubebuilder:scaffold:e2e-webhooks-checks
})

// curlPodOverrides renders the pod spec for the metrics-scraping curl pod. It is
// spelled out rather than defaulted because the operator's namespace enforces
// the restricted Pod Security Standard, which this pod must satisfy too.
func curlPodOverrides(token string) string {
	// The endpoint is served with a self-signed certificate and comes up a moment
	// after the pod does, hence -k and the retry loop.
	scrape := fmt.Sprintf(
		"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' %s && exit 0 || sleep 2; done; exit 1",
		token,
		fmt.Sprintf("https://%s.%s.svc.cluster.local:8443/metrics", metricsServiceName, operatorNamespace),
	)
	return fmt.Sprintf(`{
		"spec": {
			"containers": [{
				"name": "curl",
				"image": "curlimages/curl:latest",
				"command": ["/bin/sh", "-c"],
				"args": [
					%s
				],
				"securityContext": {
					"readOnlyRootFilesystem": true,
					"allowPrivilegeEscalation": false,
					"capabilities": {
						"drop": ["ALL"]
					},
					"runAsNonRoot": true,
					"runAsUser": 1000,
					"seccompProfile": {
						"type": "RuntimeDefault"
					}
				}
			}],
			"serviceAccountName": "%s"
		}
	}`, strconv.Quote(scrape), serviceAccountName)
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join(os.TempDir(), secretName)
	if err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644)); err != nil {
		return "", err
	}

	var out string
	Eventually(func(g Gomega) {
		By("executing kubectl command to create the token")
		output, err := kubectl("create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			operatorNamespace,
			serviceAccountName,
		), "-f", tokenRequestFile)
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		g.Expect(json.Unmarshal([]byte(output), &token)).To(Succeed())

		out = token.Status.Token
	}).Should(Succeed())

	return out, nil
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	return kubectl("logs", "curl-metrics", "-n", operatorNamespace)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
