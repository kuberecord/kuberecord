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
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
)

// ready builds the roll-up condition at the given status, which is what almost
// every case below is really about.
func ready(status metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{Type: v1alpha1.ConditionReady, Status: status, Reason: "Test"}
}

// gaugeValue reads one kubestream_rules series. A series the registry does not
// hold is reported as absent rather than as 0, because the difference between
// those two is exactly what the seeding behaviour is about.
func gaugeValue(t *testing.T, reg *prometheus.Registry, condition, status string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "kubestream_rules" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, condition, status) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func labelsMatch(m *dto.Metric, condition, status string) bool {
	var gotCondition, gotStatus string
	for _, l := range m.GetLabel() {
		switch l.GetName() {
		case "condition":
			gotCondition = l.GetValue()
		case "status":
			gotStatus = l.GetValue()
		}
	}
	return gotCondition == condition && gotStatus == status
}

// TestRuleMetricsSeedsReady pins the property the shipped alert depends on: the
// Ready series exist, at 0, before any rule has ever been reconciled.
func TestRuleMetricsSeedsReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewRuleMetrics(reg)

	for _, status := range []string{"true", "false", "unknown"} {
		got, ok := gaugeValue(t, reg, v1alpha1.ConditionReady, status)
		if !ok {
			t.Errorf("kubestream_rules{condition=%q,status=%q} is absent; the alert would read no data",
				v1alpha1.ConditionReady, status)
			continue
		}
		if got != 0 {
			t.Errorf("kubestream_rules{condition=%q,status=%q} = %v, want 0", v1alpha1.ConditionReady, status, got)
		}
	}
}

// TestRuleMetricsCounts drives the gauge through the sequences a fleet of rules
// actually produces, asserting the count after each step.
func TestRuleMetricsCounts(t *testing.T) {
	type step struct {
		// forget names a rule to drop; otherwise observe/conds record one.
		forget  string
		observe string
		conds   []metav1.Condition
	}
	tests := []struct {
		name string
		// steps are applied in order.
		steps []step
		// want maps "<condition>/<status>" to the expected gauge value.
		want map[string]float64
	}{
		{
			name: "one healthy rule",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionTrue)}},
			},
			want: map[string]float64{"Ready/true": 1, "Ready/false": 0, "Ready/unknown": 0},
		},
		{
			name: "one degraded rule is what the alert counts",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
			},
			want: map[string]float64{"Ready/true": 0, "Ready/false": 1, "Ready/unknown": 0},
		},
		{
			name: "both rule kinds count into one series",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
				{observe: "clusterstreamrule//b", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
			},
			want: map[string]float64{"Ready/false": 2, "Ready/true": 0},
		},
		{
			name: "same rule observed twice counts once",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
			},
			want: map[string]float64{"Ready/false": 1},
		},
		{
			name: "a rule that recovers moves between series",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionTrue)}},
			},
			want: map[string]float64{"Ready/false": 0, "Ready/true": 1},
		},
		{
			name: "forgetting a degraded rule clears the alert",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
				{forget: "streamrule/demo/a"},
			},
			want: map[string]float64{"Ready/false": 0, "Ready/true": 0},
		},
		{
			name: "forgetting an unobserved rule is a no-op",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionFalse)}},
				{forget: "streamrule/demo/never-reconciled"},
				// The ordinary delete path calls Forget twice (deletionTimestamp,
				// then NotFound); the second must not disturb anything.
				{forget: "streamrule/demo/a"},
				{forget: "streamrule/demo/a"},
			},
			want: map[string]float64{"Ready/false": 0},
		},
		{
			name: "gate conditions get their own series",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{
					ready(metav1.ConditionFalse),
					{Type: v1alpha1.ConditionRBACGranted, Status: metav1.ConditionFalse, Reason: "Test"},
					{Type: v1alpha1.ConditionResourceResolved, Status: metav1.ConditionTrue, Reason: "Test"},
				}},
			},
			want: map[string]float64{
				"Ready/false":                                   1,
				v1alpha1.ConditionRBACGranted + "/false":        1,
				v1alpha1.ConditionRBACGranted + "/true":         0,
				v1alpha1.ConditionResourceResolved + "/true":    1,
				v1alpha1.ConditionResourceResolved + "/false":   0,
				v1alpha1.ConditionResourceResolved + "/unknown": 0,
			},
		},
		{
			name: "a condition that stops appearing reports 0, not nothing",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{
					ready(metav1.ConditionFalse),
					{Type: v1alpha1.ConditionRBACGranted, Status: metav1.ConditionFalse, Reason: "Test"},
				}},
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionTrue)}},
			},
			want: map[string]float64{
				"Ready/true":                             1,
				v1alpha1.ConditionRBACGranted + "/false": 0,
			},
		},
		{
			name: "an unknown status is counted, never dropped",
			steps: []step{
				{observe: "streamrule/demo/a", conds: []metav1.Condition{ready(metav1.ConditionUnknown)}},
				{observe: "streamrule/demo/b", conds: []metav1.Condition{ready("Bogus")}},
			},
			want: map[string]float64{"Ready/unknown": 2, "Ready/true": 0, "Ready/false": 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := NewRuleMetrics(reg)
			for _, s := range tt.steps {
				if s.forget != "" {
					m.Forget(s.forget)
					continue
				}
				m.Observe(s.observe, s.conds)
			}
			for key, want := range tt.want {
				condition, status := splitKey(t, key)
				got, ok := gaugeValue(t, reg, condition, status)
				if !ok {
					t.Errorf("kubestream_rules{condition=%q,status=%q} is absent, want %v", condition, status, want)
					continue
				}
				if got != want {
					t.Errorf("kubestream_rules{condition=%q,status=%q} = %v, want %v", condition, status, got, want)
				}
			}
		})
	}
}

func splitKey(t *testing.T, key string) (condition, status string) {
	t.Helper()
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	t.Fatalf("malformed want key %q, expected <condition>/<status>", key)
	return "", ""
}

// TestRuleMetricsNilIsInert proves the zero value is safe: a reconciler built
// without metrics (as some tests do) must not panic on the paths that report.
func TestRuleMetricsNilIsInert(t *testing.T) {
	var m *RuleMetrics
	m.Observe("streamrule/demo/a", []metav1.Condition{ready(metav1.ConditionFalse)})
	m.Forget("streamrule/demo/a")
}

// TestRuleMetricsConcurrent runs both rule kinds' access patterns at once under
// -race. One RuleMetrics is shared by two reconcilers whose workers run
// concurrently, so the observed set and the recount that follows every mutation
// have to be atomic with respect to each other — and the total must be exact
// afterwards, not merely race-free.
func TestRuleMetricsConcurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRuleMetrics(reg)

	const rules = 64
	var wg sync.WaitGroup
	for i := range rules {
		key := "streamrule/demo/" + strconv.Itoa(i)
		wg.Go(func() {
			// Every rule degrades, half of them are then deleted, and a scrape
			// runs alongside — the three things that touch the gauge in
			// production.
			m.Observe(key, []metav1.Condition{ready(metav1.ConditionFalse)})
			if _, err := reg.Gather(); err != nil {
				t.Errorf("Gather: %v", err)
			}
			if i%2 == 0 {
				m.Forget(key)
			}
		})
	}
	wg.Wait()

	if got := testutil.ToFloat64(m.rules.WithLabelValues(v1alpha1.ConditionReady, "false")); got != rules/2 {
		t.Errorf("kubestream_rules{condition=Ready,status=false} = %v, want %v", got, rules/2)
	}
}
