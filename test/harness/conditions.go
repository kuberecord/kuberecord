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
	"strings"

	. "github.com/onsi/gomega" //nolint:revive,staticcheck
)

// Condition statuses, as metav1.ConditionStatus renders them in JSON. Spelled as
// constants because every suite asserts on one of the two, and "true" would
// silently never match.
const (
	StatusTrue  = "True"
	StatusFalse = "False"
)

// Condition is the subset of metav1.Condition the suites assert on.
type Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

// ConditionOf reads one status condition off any kubestream CR. It returns an
// error rather than failing, so a caller can poll it inside an Eventually while
// the reconciler is still writing status for the first time.
func ConditionOf(kind, name, namespace, condType string) (Condition, error) {
	args := []string{"get", kind, name, "-o", "jsonpath={.status.conditions}"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := Kubectl(args...)
	if err != nil {
		return Condition{}, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return Condition{}, fmt.Errorf("%s/%s has no status conditions yet", kind, name)
	}

	var conditions []Condition
	if err := json.Unmarshal([]byte(out), &conditions); err != nil {
		return Condition{}, fmt.Errorf("decode conditions of %s/%s from %q: %w", kind, name, out, err)
	}
	for _, condition := range conditions {
		if condition.Type == condType {
			return condition, nil
		}
	}
	return Condition{}, fmt.Errorf("%s/%s has no %s condition (has %d others)", kind, name, condType, len(conditions))
}

// ExpectCondition asserts, within g, that a CR carries condType with the given
// status, and returns the condition so a caller can go on to assert its reason.
func ExpectCondition(g Gomega, kind, name, namespace, condType, status string) Condition {
	condition, err := ConditionOf(kind, name, namespace, condType)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(condition.Status).To(Equal(status),
		"%s/%s condition %s: reason=%s message=%s", kind, name, condType, condition.Reason, condition.Message)
	return condition
}
