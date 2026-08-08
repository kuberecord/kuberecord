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
	"math"
	"testing"
)

// The metrics parser is the only real logic in this package — everything else
// shells out to kubectl — and it is what every chaos assertion about the write
// path is read through. A parser bug here would not fail loudly: an unmatched
// series reads exactly like a counter sitting at zero, which is the most
// misleading way a failure-mode assertion can go wrong. Hence a unit test rather
// than trust in the acceptance suite that consumes it.

func TestParseMetrics(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []Sample
	}{
		{
			name: "comments and blank lines are skipped",
			body: "# HELP kuberecord_dedup_skips_total Count.\n" +
				"# TYPE kuberecord_dedup_skips_total counter\n" +
				"\n" +
				"kuberecord_dedup_skips_total 12\n",
			want: []Sample{{Name: "kuberecord_dedup_skips_total", Labels: map[string]string{}, Value: 12}},
		},
		{
			name: "labelled counter",
			body: `kuberecord_writes_total{sink="default",outcome="failed"} 3`,
			want: []Sample{{
				Name:   "kuberecord_writes_total",
				Labels: map[string]string{"sink": "default", "outcome": "failed"},
				Value:  3,
			}},
		},
		{
			name: "histogram bucket keeps its le label",
			body: `kuberecord_enqueue_block_seconds_bucket{sink="default",le="2.5"} 41`,
			want: []Sample{{
				Name:   "kuberecord_enqueue_block_seconds_bucket",
				Labels: map[string]string{"sink": "default", "le": "2.5"},
				Value:  41,
			}},
		},
		{
			name: "the +Inf bucket parses as an infinite value, not an error",
			body: `kuberecord_enqueue_block_seconds_bucket{le="+Inf"} 41`,
			want: []Sample{{
				Name:   "kuberecord_enqueue_block_seconds_bucket",
				Labels: map[string]string{"le": "+Inf"},
				Value:  41,
			}},
		},
		{
			name: "a label value may contain an escaped quote and a comma",
			body: `kuberecord_safe_mode{namespace="a,b",kind="Con\"figMap"} 1`,
			want: []Sample{{
				Name:   "kuberecord_safe_mode",
				Labels: map[string]string{"namespace": "a,b", "kind": `Con"figMap`},
				Value:  1,
			}},
		},
		{
			name: "an empty label value is preserved, not treated as absent",
			// The core API group and a cluster-scoped object both render as "",
			// and a scope query that treated them as unset would silently widen.
			body: `kuberecord_safe_mode{group="",namespace=""} 0`,
			want: []Sample{{
				Name:   "kuberecord_safe_mode",
				Labels: map[string]string{"group": "", "namespace": ""},
				Value:  0,
			}},
		},
		{
			name: "a trailing scrape timestamp is ignored",
			body: `kuberecord_write_queue_depth{sink="default"} 7 1700000000000`,
			want: []Sample{{
				Name:   "kuberecord_write_queue_depth",
				Labels: map[string]string{"sink": "default"},
				Value:  7,
			}},
		},
		{
			name: "an exponent-form value parses",
			body: "kuberecord_write_latency_seconds_sum 1.5e-05",
			want: []Sample{{Name: "kuberecord_write_latency_seconds_sum", Labels: map[string]string{}, Value: 1.5e-05}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMetrics(tt.body)
			if err != nil {
				t.Fatalf("ParseMetrics() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ParseMetrics() returned %d samples, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, want := range tt.want {
				assertSample(t, got[i], want)
			}
		})
	}
}

func assertSample(t *testing.T, got, want Sample) {
	t.Helper()
	if got.Name != want.Name {
		t.Errorf("name = %q, want %q", got.Name, want.Name)
	}
	if got.Value != want.Value {
		t.Errorf("value = %v, want %v", got.Value, want.Value)
	}
	if len(got.Labels) != len(want.Labels) {
		t.Fatalf("labels = %v, want %v", got.Labels, want.Labels)
	}
	for key, value := range want.Labels {
		if got.Labels[key] != value {
			t.Errorf("label %q = %q, want %q", key, got.Labels[key], value)
		}
	}
}

func TestParseMetricsNaN(t *testing.T) {
	// NaN is what a summary quantile reports before it has observations. It must
	// parse rather than error, or one such series would make a whole scrape
	// unreadable and every assertion in the scenario fail for the wrong reason.
	got, err := ParseMetrics(`go_gc_duration_seconds{quantile="0"} NaN`)
	if err != nil {
		t.Fatalf("ParseMetrics() error = %v", err)
	}
	if len(got) != 1 || !math.IsNaN(got[0].Value) {
		t.Fatalf("ParseMetrics() = %+v, want a single NaN sample", got)
	}
}

func TestParseMetricsRejectsMalformedLines(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "no value", body: "kuberecord_writes_total"},
		{name: "no value after a label set", body: `kuberecord_writes_total{sink="default"}`},
		{name: "unterminated label value", body: `kuberecord_writes_total{sink="default} 1`},
		{name: "unquoted label value", body: `kuberecord_writes_total{sink=default} 1`},
		{name: "label with no value", body: `kuberecord_writes_total{sink} 1`},
		{name: "non-numeric value", body: "kuberecord_writes_total not-a-number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Erroring rather than skipping is deliberate: a silently dropped sample
			// is indistinguishable from an absent metric at the call site.
			if _, err := ParseMetrics(tt.body); err == nil {
				t.Fatalf("ParseMetrics(%q) returned no error", tt.body)
			}
		})
	}
}

func TestSumAndFind(t *testing.T) {
	samples := []Sample{
		{Name: "writes", Labels: map[string]string{"sink": "a", "outcome": "failed"}, Value: 2},
		{Name: "writes", Labels: map[string]string{"sink": "b", "outcome": "failed"}, Value: 5},
		{Name: "writes", Labels: map[string]string{"sink": "a", "outcome": "success"}, Value: 9},
		{Name: "safe_mode", Labels: map[string]string{"sink": "a", "namespace": "demo"}, Value: 0},
	}

	tests := []struct {
		name  string
		match map[string]string
		want  float64
	}{
		{name: "every series in the family", match: nil, want: 16},
		{name: "narrowed by one label", match: map[string]string{"outcome": "failed"}, want: 7},
		{name: "narrowed to a single series", match: map[string]string{"sink": "a", "outcome": "failed"}, want: 2},
		{name: "no match sums to zero", match: map[string]string{"sink": "c"}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sum(samples, "writes", tt.match); got != tt.want {
				t.Errorf("Sum() = %v, want %v", got, tt.want)
			}
		})
	}

	// Find must distinguish "present and zero" from "absent" — the boot scenario
	// reads safe_mode=0 as proof a scope finished warming, which is a different
	// claim from the scope never having been seen.
	if sample, ok := Find(samples, "safe_mode", map[string]string{"namespace": "demo"}); !ok || sample.Value != 0 {
		t.Errorf("Find() = %+v, %v; want a zero-valued sample", sample, ok)
	}
	if _, ok := Find(samples, "safe_mode", map[string]string{"namespace": "absent"}); ok {
		t.Error("Find() reported a series that is not in the scrape")
	}
}
