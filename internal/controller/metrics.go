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

package controller

import (
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
)

// metricsNamespace prefixes the control plane's metric names with "kubestream_",
// matching the data plane's collectors (internal/pipeline) so one operator's
// series all live under one greppable prefix on the same /metrics endpoint.
const metricsNamespace = "kubestream"

// conditionStatuses is the closed set of values a metav1.Condition status can
// take, lowercased for the metric label.
//
// It is enumerated rather than derived from what has been observed because every
// (condition, status) pair must exist as a series *before* the first rule of that
// kind degrades: an alert written as `kubestream_rules{...,status="false"} > 0`
// over a series that does not exist yet evaluates to no data, which is neither
// firing nor healthy, and which starts firing the instant the series appears —
// resetting the alert's `for` clock at exactly the wrong moment.
var conditionStatuses = []string{"true", "false", "unknown"}

// RuleMetrics publishes how many rules currently hold each condition at each
// status, as kubestream_rules{condition,status}.
//
// It exists for one panel and one alert: "how many rules are broken right now?".
// The rule reconcilers already answer that per rule, in status conditions and
// events, but neither is aggregatable — an operator watching a fleet cannot ask
// `kubectl get streamrules -A` on an interval, and an event is an edge, not a
// level. This is the level.
//
// The gauge deliberately carries no rule identity. Naming the rule would make the
// series cardinality a function of how many rules a cluster defines and would
// resurrect a deleted rule's series until the next scrape; the identity of a
// degraded rule is a `kubectl get` away once the count says one exists. For the
// same reason the two rule kinds share one series: the label set the acceptance
// criteria pin is (condition, status), and an operator alerted on "a rule is not
// Ready" reaches for the rule list either way.
//
// Because the gauge is a count over a set, it cannot be derived from a single
// reconcile pass — a pass sees one rule. RuleMetrics therefore keeps the last
// observed condition status of every live rule and recomputes the counts on each
// change. That state is derived, not durable (Invariant 6): a restart rebuilds it
// from the level-triggered reconciliation of every rule.
type RuleMetrics struct {
	rules *prometheus.GaugeVec

	// mu guards observed and published. Both rule reconcilers share one
	// RuleMetrics and run concurrently, so every mutation of the set — and the
	// recount that follows it — has to be atomic with respect to the others.
	mu sync.Mutex
	// observed maps a rule key (see RuleKey) to that rule's last written
	// conditions, by condition type. It is the set the counts are computed over.
	observed map[string]map[string]metav1.ConditionStatus
	// published is every condition type a series has already been emitted for.
	// Types accumulate and are never dropped, so a condition that stops appearing
	// on any rule reports 0 rather than disappearing — a gap a range query cannot
	// tell apart from "the operator stopped scraping".
	published map[string]bool
}

// NewRuleMetrics constructs the kubestream_rules gauge and registers it on reg.
//
// Registration uses MustRegister, so a registry that already holds the metric
// panics: production registers exactly once through RuleMetricsInstance, and each
// test passes a fresh registry, which is what lets repeated test setups exist at
// all.
func NewRuleMetrics(reg prometheus.Registerer) *RuleMetrics {
	m := &RuleMetrics{
		rules: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "rules",
			Help:      "Number of StreamRules and ClusterStreamRules currently holding a condition at a status.",
		}, []string{"condition", "status"}),
		observed:  make(map[string]map[string]metav1.ConditionStatus),
		published: make(map[string]bool),
	}

	// Seed the roll-up condition at 0 for all three statuses. Ready is the one
	// condition every rule always carries and the one the shipped alert is written
	// against, so its series must exist from process start rather than from the
	// first reconcile.
	m.publishType(v1alpha1.ConditionReady)

	reg.MustRegister(m.rules)
	return m
}

var (
	ruleMetricsOnce      sync.Once
	ruleMetricsSingleton *RuleMetrics
)

// RuleMetricsInstance returns the process-wide RuleMetrics, registered exactly
// once on controller-runtime's global registry so the existing
// --metrics-bind-address server exposes it. Both rule reconcilers must share one
// instance — the gauge counts a set that spans them — and the sync.Once makes
// "both fetch it" the same object rather than a duplicate-registration panic.
func RuleMetricsInstance() *RuleMetrics {
	ruleMetricsOnce.Do(func() {
		ruleMetricsSingleton = NewRuleMetrics(ctrlmetrics.Registry)
	})
	return ruleMetricsSingleton
}

// Observe records the conditions one rule was just written with, and republishes
// the counts.
//
// It is called after the status update succeeds rather than before it, so the
// gauge describes what the API server actually holds. A pass whose status write
// failed leaves the previous observation in place: the reconcile is retried, and
// reporting a verdict that was never persisted would make the dashboard disagree
// with `kubectl get`.
//
// Conditions absent from conds are dropped for this rule rather than retained.
// The rule reconcilers write a rule's conditions as one merged set, so a type
// that is not in the set is a type the rule no longer carries.
func (m *RuleMetrics) Observe(ruleKey string, conds []metav1.Condition) {
	if m == nil {
		return
	}
	current := make(map[string]metav1.ConditionStatus, len(conds))
	for _, c := range conds {
		current[c.Type] = c.Status
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.observed[ruleKey] = current
	for condType := range current {
		m.publishType(condType)
	}
	m.recount()
}

// Forget drops a rule from the counted set, for a rule that is gone or on its way
// out. Without it a deleted rule would hold the count it degraded with forever,
// and the Ready=False alert would never clear for a rule the operator already
// deleted to make it stop.
func (m *RuleMetrics) Forget(ruleKey string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.observed[ruleKey]; !ok {
		// Not counted — a rule deleted before it was ever reconciled, or a
		// duplicate withdrawal (the deletionTimestamp pass and the NotFound pass
		// both run for an ordinary delete). Recounting would be harmless but
		// pointless.
		return
	}
	delete(m.observed, ruleKey)
	m.recount()
}

// publishType materializes every status series for one condition type at 0, if it
// has not been published before. Callers hold m.mu (the constructor runs before
// the value is shared).
func (m *RuleMetrics) publishType(condType string) {
	if m.published[condType] {
		return
	}
	m.published[condType] = true
	for _, status := range conditionStatuses {
		m.rules.WithLabelValues(condType, status).Set(0)
	}
}

// recount recomputes every published series from the observed set. Callers hold
// m.mu.
//
// Every published (type, status) pair is Set on every pass, including the ones
// that count zero. The alternative — Reset plus Set — would leave a window in
// which a scrape sees no series at all, and DeleteLabelValues on the pairs that
// reached zero would turn "no rule is degraded" into missing data. Both read to a
// range query as an operator that stopped reporting.
func (m *RuleMetrics) recount() {
	counts := make(map[string]map[string]int, len(m.published))
	for condType := range m.published {
		counts[condType] = make(map[string]int, len(conditionStatuses))
	}
	for _, conds := range m.observed {
		for condType, status := range conds {
			byStatus, ok := counts[condType]
			if !ok {
				// Unreachable: Observe publishes every type it records before
				// recounting. Guarded so a future caller that forgets cannot make a
				// rule vanish from the accounting (Invariant 4).
				byStatus = make(map[string]int, len(conditionStatuses))
				counts[condType] = byStatus
				m.publishType(condType)
			}
			byStatus[statusLabel(status)]++
		}
	}

	for condType, byStatus := range counts {
		for _, status := range conditionStatuses {
			m.rules.WithLabelValues(condType, status).Set(float64(byStatus[status]))
		}
	}
}

// statusLabel renders a condition status as its metric label value.
//
// An unrecognized status is reported as "unknown" rather than as a new label
// value: metav1.ConditionStatus is a three-value enum, so a fourth value is a bug,
// and inventing a series for it would hide the affected rule from both the
// "true" and the "false" count — which is precisely the case an operator is
// alerting on.
func statusLabel(status metav1.ConditionStatus) string {
	lowered := strings.ToLower(string(status))
	if slices.Contains(conditionStatuses, lowered) {
		return lowered
	}
	return "unknown"
}
