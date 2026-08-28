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

package pipeline

import (
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// TestPipelineMetricsRegistration builds an isolated PipelineMetrics on a fresh
// registry (proving repeated setups never collide on the global one) and
// asserts every metric the acceptance criteria name is present and typed as
// specified.
func TestPipelineMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPipelineMetrics(reg)

	// The labelled collectors only materialize a series once a label value is
	// used; touch one apiece so they appear in Gather like the others. ForSink does
	// that for the whole write path in one call (which is also how production
	// reaches them), leaving only the two pipeline-owned label sets. The
	// pipeline_dropped_total and pipeline_diff_refusals_total series are already
	// seeded by the constructor.
	m.ForSink(clickHouseSink("default"))
	m.hashcacheEntries.WithLabelValues(clickHouseSink("default").String())
	m.safeMode.WithLabelValues(clickHouseSink("default").String(), "apps", "Deployment", "demo")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := make(map[string]dto.MetricType, len(families))
	for _, mf := range families {
		got[mf.GetName()] = mf.GetType()
	}

	want := map[string]dto.MetricType{
		"kuberecord_write_batch_rows":             dto.MetricType_HISTOGRAM,
		"kuberecord_write_queue_depth":            dto.MetricType_GAUGE,
		"kuberecord_write_queue_capacity":         dto.MetricType_GAUGE,
		"kuberecord_writes_total":                 dto.MetricType_COUNTER,
		"kuberecord_write_latency_seconds":        dto.MetricType_HISTOGRAM,
		"kuberecord_write_retry_attempts_total":   dto.MetricType_COUNTER,
		"kuberecord_enqueue_block_seconds":        dto.MetricType_HISTOGRAM,
		"kuberecord_enqueue_timeouts_total":       dto.MetricType_COUNTER,
		"kuberecord_dedup_skips_total":            dto.MetricType_COUNTER,
		"kuberecord_hashcache_entries":            dto.MetricType_GAUGE,
		"kuberecord_safe_mode":                    dto.MetricType_GAUGE,
		"kuberecord_pipeline_dropped_total":       dto.MetricType_COUNTER,
		"kuberecord_pipeline_diff_refusals_total": dto.MetricType_COUNTER,
	}

	for name, wantType := range want {
		gotType, ok := got[name]
		if !ok {
			t.Errorf("metric %q not registered", name)
			continue
		}
		if gotType != wantType {
			t.Errorf("metric %q has type %s, want %s", name, gotType, wantType)
		}
	}

	// The requeue-channel gauge died with the channel it described: the workqueue
	// owns retries now, so a "dropped requeue trigger" is not a state the operator
	// can be in. Asserting its absence keeps a stale dashboard panel from being
	// mistaken for a live signal.
	if _, stale := got["kuberecord_requeue_drops_total"]; stale {
		t.Error("kuberecord_requeue_drops_total is still registered; the requeue channel it measured no longer exists")
	}
}

// seriesLabels gathers reg and returns, per metric family, the label sets of
// every series in it as sorted "name=value" pairs.
func seriesLabels(t *testing.T, reg prometheus.Gatherer) map[string][]string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string][]string, len(families))
	for _, mf := range families {
		for _, mtc := range mf.GetMetric() {
			pairs := make([]string, 0, len(mtc.GetLabel()))
			for _, lp := range mtc.GetLabel() {
				pairs = append(pairs, lp.GetName()+"="+lp.GetValue())
			}
			slices.Sort(pairs)
			out[mf.GetName()] = append(out[mf.GetName()], strings.Join(pairs, ","))
		}
		slices.Sort(out[mf.GetName()])
	}
	return out
}

// TestSinkMetricsAreLabelledPerSink is the Task 1.8 per-sink-label guard: two
// sinks reporting the same write-path metric must produce two independent series,
// not one series overwritten by whichever writer reported last. It asserts the
// label sets directly, and that the pipeline-owned counters deliberately stay
// unlabelled.
//
// The label *values* are sink.ID.String() — "<Kind>/<Name>", not the bare CR name
// (Task 4.1, a breaking change for any dashboard that matched on the old value).
// Asserting them in full is what makes that format a tested contract rather than
// an incidental rendering.
func TestSinkMetricsAreLabelledPerSink(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPipelineMetrics(reg)

	primary := m.ForSink(clickHouseSink("primary"))
	audit := m.ForSink(clickHouseSink("audit"))

	primary.SetWriteQueueDepth(7)
	primary.SetWriteQueueCapacity(100)
	primary.IncWrite("success")
	audit.SetWriteQueueDepth(3)
	audit.SetWriteQueueCapacity(50)
	audit.IncWrite("failed")
	m.dedupSkips.Inc()

	labels := seriesLabels(t, reg)

	// Spelled out rather than derived, so the expected label format is visible here
	// and a change to ID.String() fails this test instead of silently passing it.
	const (
		auditSeries   = "sink=ClickHouseSink/audit"
		primarySeries = "sink=ClickHouseSink/primary"
	)

	tests := []struct {
		metric string
		want   []string
	}{
		{"kuberecord_write_queue_depth", []string{auditSeries, primarySeries}},
		{"kuberecord_write_queue_capacity", []string{auditSeries, primarySeries}},
		{"kuberecord_write_latency_seconds", []string{auditSeries, primarySeries}},
		{"kuberecord_write_retry_attempts_total", []string{auditSeries, primarySeries}},
		{"kuberecord_write_batch_rows", []string{auditSeries, primarySeries}},
		{"kuberecord_enqueue_block_seconds", []string{auditSeries, primarySeries}},
		{"kuberecord_enqueue_timeouts_total", []string{auditSeries, primarySeries}},
		{"kuberecord_writes_total", []string{
			"outcome=failed," + auditSeries, "outcome=failed," + primarySeries,
			"outcome=success," + auditSeries, "outcome=success," + primarySeries,
		}},
		// Pipeline-owned, and deliberately not per-sink: dedup is a property of the
		// shared workqueue's short-circuit rate, not of a backend.
		{"kuberecord_dedup_skips_total", []string{""}},
	}
	for _, tc := range tests {
		t.Run(tc.metric, func(t *testing.T) {
			if got := labels[tc.metric]; !slices.Equal(got, tc.want) {
				t.Errorf("series of %s = %v, want %v", tc.metric, got, tc.want)
			}
		})
	}

	// Both sinks' values survive independently — the point of the label.
	if got := gaugeValue(t, reg, "kuberecord_write_queue_depth", clickHouseSink("primary")); got != 7 {
		t.Errorf("primary write_queue_depth = %v, want 7", got)
	}
	if got := gaugeValue(t, reg, "kuberecord_write_queue_depth", clickHouseSink("audit")); got != 3 {
		t.Errorf("audit write_queue_depth = %v, want 3", got)
	}
}

// TestRemoveSinkDeletesSinkSeries proves a deleted sink leaves no series behind:
// a lingering queue-depth gauge would read as a live-but-idle backend forever
// (Task 1.8's SinkManager calls RemoveSink once a deleted sink has drained).
func TestRemoveSinkDeletesSinkSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPipelineMetrics(reg)

	p, err := New(Options{Lister: newFakeLister(), Router: newFakeRouter(), Metrics: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	goneID := clickHouseSink("gone")
	gone := m.ForSink(goneID)
	gone.SetWriteQueueDepth(9)
	gone.IncWrite("success")
	m.hashcacheEntries.WithLabelValues(goneID.String()).Set(4)
	m.safeMode.WithLabelValues(goneID.String(), "apps", "Deployment", "demo").Set(1)
	// A second sink proves the eviction is scoped to the one sink, not a wipe.
	m.ForSink(clickHouseSink("kept")).SetWriteQueueDepth(2)

	p.RemoveSink(goneID)

	labels := seriesLabels(t, reg)
	for _, metric := range []string{
		"kuberecord_write_queue_depth", "kuberecord_write_queue_capacity", "kuberecord_writes_total",
		"kuberecord_write_latency_seconds", "kuberecord_write_retry_attempts_total",
		"kuberecord_write_batch_rows", "kuberecord_enqueue_block_seconds",
		"kuberecord_enqueue_timeouts_total", "kuberecord_hashcache_entries", "kuberecord_safe_mode",
	} {
		for _, series := range labels[metric] {
			if strings.Contains(series, "sink="+goneID.String()) {
				t.Errorf("%s still has a series for the removed sink: %q", metric, series)
			}
		}
	}
	if got := gaugeValue(t, reg, "kuberecord_write_queue_depth", clickHouseSink("kept")); got != 2 {
		t.Errorf("the surviving sink's write_queue_depth = %v, want 2", got)
	}
}

// gaugeValue returns the value of metric{sink=id}, failing the test if the series
// is absent. It matches on ID.String(), which is what the label carries.
func gaugeValue(t *testing.T, reg prometheus.Gatherer, metric string, id sink.ID) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
		for _, mtc := range mf.GetMetric() {
			for _, lp := range mtc.GetLabel() {
				if lp.GetName() == sinkLabel && lp.GetValue() == id.String() {
					return mtc.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("gauge %s{sink=%q} not found", metric, id)
	return 0
}
