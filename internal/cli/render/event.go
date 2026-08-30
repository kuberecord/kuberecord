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

package render

import (
	"encoding/json"
	"strings"
)

// A merged Kubernetes Event row describes the Event object, not the object whose
// timeline it was merged into (see query.EventKubernetes). Everything this file
// reads therefore comes out of Change.Data, and it reads both API spellings.
//
// v1/Event and events.k8s.io/v1/Event are one storage behind two APIs, and a
// cluster's StreamRules may name either. The engine already correlates both when
// selecting rows; reading only one of them here would render half a cluster's
// commentary as blank cells — a silence produced by the renderer, which is the
// same failure as a silence produced by a query and no easier to notice.

// Event field names, in both spellings.
const (
	// eventTypeField is "Normal" or "Warning" in both APIs.
	eventTypeField = "type"
	// eventReasonField is the short machine reason in both APIs.
	eventReasonField = "reason"
	// eventMessageField is v1's human-readable text.
	eventMessageField = "message"
	// eventNoteField is what events.k8s.io/v1 renamed it to.
	eventNoteField = "note"
	// eventReportingControllerField is events.k8s.io/v1's author.
	eventReportingControllerField = "reportingController"
	// eventSourceField holds v1's author, under a "component" member.
	eventSourceField = "source"
	// eventDeprecatedSourceField is the same thing on an events.k8s.io/v1 object
	// that was written through the legacy API.
	eventDeprecatedSourceField = "deprecatedSource"
	// eventComponentField is the member inside either source object.
	eventComponentField = "component"
)

// EventTypeWarning is the Event type that earns a glyph.
const EventTypeWarning = "Warning"

// EventDetail is the part of a Kubernetes Event a timeline row shows.
type EventDetail struct {
	// Type is "Normal", "Warning", or whatever else a cluster put there. The
	// field is open in the API and is treated as open here.
	Type string
	// Reason is the short machine-readable cause: BackOff, FailedScheduling.
	Reason string
	// Message is the human-readable text, from either API's spelling of it.
	Message string
	// Reporter is the controller that wrote the Event, used as the row's actor
	// when the Event object itself carries no field managers.
	Reporter string
}

// Warning reports whether this Event earns the warning glyph.
func (e EventDetail) Warning() bool { return e.Type == EventTypeWarning }

// ParseEvent reads the Event fields out of a merged row's recorded data.
//
// It returns false only when there is nothing to read — no data, or data that is
// not a JSON object. A row whose data is present but holds none of the fields
// below still returns true with an empty detail, because the row is still an
// Event and the renderer still has to say so; suppressing it would drop a
// Kubernetes Event from a timeline that asked for them.
func ParseEvent(data string) (EventDetail, bool) {
	if strings.TrimSpace(data) == "" {
		return EventDetail{}, false
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(data), &object); err != nil || object == nil {
		return EventDetail{}, false
	}
	return EventDetail{
		Type:     stringField(object, eventTypeField),
		Reason:   stringField(object, eventReasonField),
		Message:  firstNonEmpty(stringField(object, eventMessageField), stringField(object, eventNoteField)),
		Reporter: eventReporter(object),
	}, true
}

// Summary renders the Event's contribution to the CHANGE column, with the
// warning glyph attached when it belongs there.
//
// The glyph leads because it qualifies everything after it, and because a column
// scanned downwards for trouble is scanned down its left edge.
func (e EventDetail) Summary() string {
	text := e.Reason
	switch {
	case text == "" && e.Message == "":
		// Both empty is a real state — an Event object stripped by a redaction
		// policy, or one this cluster wrote oddly — and it is said rather than
		// rendered as an empty cell.
		text = "Kubernetes Event with no reason or message recorded"
	case text == "":
		text = collapseWhitespace(e.Message)
	case e.Message != "":
		text += ": " + collapseWhitespace(e.Message)
	}
	if e.Warning() {
		return WarningGlyph + " " + text
	}
	return text
}

// eventReporter finds the controller that authored the Event, across both APIs.
func eventReporter(object map[string]any) string {
	if controller := stringField(object, eventReportingControllerField); controller != "" {
		return controller
	}
	for _, field := range []string{eventSourceField, eventDeprecatedSourceField} {
		if source, ok := object[field].(map[string]any); ok {
			if component := stringField(source, eventComponentField); component != "" {
				return component
			}
		}
	}
	return ""
}

// stringField reads a string member, treating a member of any other type as
// absent.
//
// Absent rather than coerced: a number where a reason should be is a cluster
// writing something this renderer does not understand, and "%!s(float64=3)" in
// an audit timeline is worse than a blank.
func stringField(object map[string]any, name string) string {
	text, _ := object[name].(string)
	return text
}

// firstNonEmpty returns the first value that has anything in it.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
