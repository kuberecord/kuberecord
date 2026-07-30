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

package harness

import (
	"encoding/json"
	"fmt"

	. "github.com/onsi/gomega" //nolint:revive,staticcheck
)

// PodInfo is what a suite needs to know about a pod: which one it is, whether it
// is serving, and whether it has crashed.
//
// RestartCount is the field a suite turns into its "zero restarts" claim: an
// operator that crash-looped its way back to health would satisfy "the condition
// eventually flipped" while violating Invariant 5, and only the restart count
// tells the two apart.
type PodInfo struct {
	Name         string
	Phase        string
	Ready        bool
	RestartCount int
	// Terminating marks a pod that has a deletionTimestamp but has not gone yet.
	//
	// The distinction is load-bearing in both directions, which is why it is
	// reported rather than filtered away here. Asking "which pod is serving?" must
	// ignore a terminating pod, or it sees two. Asking "is the operator down?"
	// must count it, because a pod inside its termination grace period is still
	// running — treating it as gone would let a restart scenario delete objects
	// while the operator was still watching, and quietly stop testing the offline
	// path it exists to test.
	Terminating bool
}

// Pods lists every pod matching selector in namespace, terminating ones
// included.
func Pods(selector, namespace string) ([]PodInfo, error) {
	out, err := Kubectl("get", "pods", "-l", selector, "-n", namespace, "-o", "json")
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
		return nil, fmt.Errorf("decode pod list for %q: %w", selector, err)
	}

	var pods []PodInfo
	for _, item := range list.Items {
		pod := PodInfo{
			Name:        item.Metadata.Name,
			Phase:       item.Status.Phase,
			Terminating: item.Metadata.DeletionTimestamp != "",
		}
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" {
				pod.Ready = condition.Status == StatusTrue
			}
		}
		for _, container := range item.Status.ContainerStatuses {
			pod.RestartCount += container.RestartCount
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// SolePod returns the single serving pod matching selector, failing if there is
// not exactly one — the shipped operator Deployment runs one replica, and any
// other count means a rollout is mid-flight and nothing read from it would be
// stable. Terminating pods do not count as serving.
func SolePod(g Gomega, selector, namespace string) PodInfo {
	pods, err := Pods(selector, namespace)
	g.Expect(err).NotTo(HaveOccurred())
	serving := make([]PodInfo, 0, len(pods))
	for _, pod := range pods {
		if !pod.Terminating {
			serving = append(serving, pod)
		}
	}
	g.Expect(serving).To(HaveLen(1), "expected exactly one serving pod for %q, got %v", selector, pods)
	return serving[0]
}
