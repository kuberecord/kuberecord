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

package render_test

import (
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// TestParseEventReadsBothAPISpellings is the half of Task 3.1's "both spellings"
// promise that lives in the renderer.
//
// The engine already correlates Events captured through either API. Reading only
// one of them here would render the other half of a cluster's commentary as blank
// cells — a silence the renderer manufactured, which is no easier to notice than
// one a query manufactured.
func TestParseEventReadsBothAPISpellings(t *testing.T) {
	tests := []struct {
		name string
		data string
		want render.EventDetail
	}{
		{
			name: "v1, with message and source.component",
			data: `{"type":"Warning","reason":"BackOff","message":"Back-off restarting failed container",
			        "source":{"component":"kubelet"}}`,
			want: render.EventDetail{
				Type: "Warning", Reason: "BackOff",
				Message: "Back-off restarting failed container", Reporter: "kubelet",
			},
		},
		{
			name: "events.k8s.io/v1, with note and reportingController",
			data: `{"type":"Normal","reason":"Scheduled","note":"Successfully assigned payments/checkout",
			        "reportingController":"default-scheduler"}`,
			want: render.EventDetail{
				Type: "Normal", Reason: "Scheduled",
				Message: "Successfully assigned payments/checkout", Reporter: "default-scheduler",
			},
		},
		{
			name: "an events.k8s.io object written through the legacy API",
			data: `{"type":"Normal","reason":"Pulled","note":"Container image pulled",
			        "deprecatedSource":{"component":"kubelet"}}`,
			want: render.EventDetail{
				Type: "Normal", Reason: "Pulled", Message: "Container image pulled", Reporter: "kubelet",
			},
		},
		{
			name: "a field of the wrong type is absent, not coerced",
			data: `{"type":"Normal","reason":3}`,
			want: render.EventDetail{Type: "Normal"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := render.ParseEvent(test.data)
			if !ok {
				t.Fatalf("ParseEvent reported no data for %q", test.data)
			}
			if got != test.want {
				t.Errorf("ParseEvent = %+v, want %+v", got, test.want)
			}
		})
	}
}

// TestParseEventReadsTheOccurrenceCountInEverySpelling is the count half of the
// same promise, and it matters for a reason the other fields do not have.
//
// A sink coalesces the rows of a bursting Event inside its
// spec.writer.coalesceWindow, so how often an Event fired is carried by `count`
// and by nothing else — a reader cannot recover it by counting rows. Missing the
// spelling one API uses would therefore not render a blank cell, which is
// visible: it would render a crash-loop that fired two hundred times exactly like
// one that fired once.
//
// The three spellings are taken as a maximum rather than as a first-populated
// chain, which is the form docs/QUERIES.md publishes as `greatest(count,
// deprecatedCount, series.count)`. The CLI and that SQL read the same stored row,
// so a divergence here would make a `jq` recipe and a query disagree about a
// number they both take off `data`.
func TestParseEventReadsTheOccurrenceCountInEverySpelling(t *testing.T) {
	tests := []struct {
		name string
		data string
		want int
	}{
		{
			name: "v1 count",
			data: `{"reason":"BackOff","count":47}`,
			want: 47,
		},
		{
			name: "events.k8s.io/v1 deprecatedCount",
			data: `{"reason":"BackOff","deprecatedCount":12}`,
			want: 12,
		},
		{
			name: "events.k8s.io/v1 series.count",
			data: `{"reason":"BackOff","series":{"count":9,"lastObservedTime":"2026-08-28T14:06:44Z"}}`,
			want: 9,
		},
		{
			name: "an Event carrying two spellings takes the larger",
			data: `{"reason":"BackOff","deprecatedCount":1,"series":{"count":9}}`,
			want: 9,
		},
		{
			name: "no count at all is nothing to render, not one",
			data: `{"reason":"Scheduled","message":"Assigned to node-3"}`,
			want: 0,
		},
		{
			name: "a series with no count in it",
			data: `{"reason":"BackOff","series":{"lastObservedTime":"2026-08-28T14:06:44Z"}}`,
			want: 0,
		},
		{
			name: "a string where a number belongs is absent, not parsed",
			data: `{"reason":"BackOff","count":"47"}`,
			want: 0,
		},
		{
			name: "a fractional count is not rounded into one a reader would trust",
			data: `{"reason":"BackOff","count":3.5}`,
			want: 0,
		},
		{
			name: "a count no API server wrote is not believed",
			data: `{"reason":"BackOff","count":1e30}`,
			want: 0,
		},
		{
			name: "a negative count is not believed either",
			data: `{"reason":"BackOff","count":-4}`,
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := render.ParseEvent(test.data)
			if !ok {
				t.Fatalf("ParseEvent reported no data for %q", test.data)
			}
			if got.Count != test.want {
				t.Errorf("ParseEvent(%s).Count = %d, want %d", test.data, got.Count, test.want)
			}
		})
	}
}

// TestParseEventReportsAbsentData covers the only case that is genuinely nothing
// to render.
func TestParseEventReportsAbsentData(t *testing.T) {
	for _, data := range []string{"", "   ", "not json", "[]", "null"} {
		if _, ok := render.ParseEvent(data); ok {
			t.Errorf("ParseEvent(%q) claimed to have read an Event out of it", data)
		}
	}
}

// TestEventSummary covers the glyph and the two fallbacks.
func TestEventSummary(t *testing.T) {
	tests := []struct {
		name   string
		detail render.EventDetail
		want   string
	}{
		{
			name:   "a warning leads with the glyph",
			detail: render.EventDetail{Type: "Warning", Reason: "BackOff", Message: "Back-off restarting"},
			want:   "⚠ BackOff: Back-off restarting",
		},
		{
			name:   "a normal event does not",
			detail: render.EventDetail{Type: "Normal", Reason: "Scheduled", Message: "Assigned to node-3"},
			want:   "Scheduled: Assigned to node-3",
		},
		{
			name:   "a reason with no message",
			detail: render.EventDetail{Type: "Normal", Reason: "Killing"},
			want:   "Killing",
		},
		{
			name:   "a message with no reason",
			detail: render.EventDetail{Type: "Normal", Message: "something happened"},
			want:   "something happened",
		},
		{
			name:   "neither is said rather than left blank",
			detail: render.EventDetail{Type: "Normal"},
			want:   "Kubernetes Event with no reason or message recorded",
		},
		{
			name:   "a multi-line message is flattened",
			detail: render.EventDetail{Type: "Normal", Reason: "Pulled", Message: "line one\nline two"},
			want:   "Pulled: line one line two",
		},
		{
			name: "a recurrence carries its count beside the reason",
			detail: render.EventDetail{
				Type: "Warning", Reason: "BackOff",
				Message: "Back-off restarting failed container", Count: 47,
			},
			want: "⚠ BackOff ×47: Back-off restarting failed container",
		},
		{
			name:   "a reason with no message keeps it too",
			detail: render.EventDetail{Type: "Warning", Reason: "Unhealthy", Count: 6},
			want:   "⚠ Unhealthy ×6",
		},
		{
			name:   "with no reason to hang it on, the count leads the cell",
			detail: render.EventDetail{Type: "Normal", Message: "something happened", Count: 3},
			want:   "×3 something happened",
		},
		{
			name:   "and leads the fallback sentence, which is most of what is left to say",
			detail: render.EventDetail{Type: "Warning", Count: 200},
			want:   "⚠ ×200 Kubernetes Event with no reason or message recorded",
		},
		{
			name:   "one occurrence is the ordinary case and is not marked",
			detail: render.EventDetail{Type: "Normal", Reason: "Scheduled", Message: "Assigned", Count: 1},
			want:   "Scheduled: Assigned",
		},
		{
			name:   "and neither is an Event that carries no count at all",
			detail: render.EventDetail{Type: "Normal", Reason: "Scheduled", Message: "Assigned"},
			want:   "Scheduled: Assigned",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.detail.Summary(); got != test.want {
				t.Errorf("Summary() = %q, want %q", got, test.want)
			}
		})
	}
}
