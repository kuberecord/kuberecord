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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.detail.Summary(); got != test.want {
				t.Errorf("Summary() = %q, want %q", got, test.want)
			}
		})
	}
}
