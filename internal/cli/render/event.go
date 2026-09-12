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
	"strconv"
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
	// eventCountField is core v1's cumulative occurrence count.
	eventCountField = "count"
	// eventDeprecatedCountField is the same number on an events.k8s.io/v1 object
	// that was written through the legacy API.
	eventDeprecatedCountField = "deprecatedCount"
	// eventSeriesField holds events.k8s.io/v1's {count, lastObservedTime} for an
	// Event the API server aggregated into a series.
	eventSeriesField = "series"
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
	// Count is how many times this Event had fired when the row was recorded, or
	// 0 when the Event carries no count at all.
	//
	// It is **cumulative and not per-row**, which is the whole reason it is
	// rendered. A sink coalesces the rows of a bursting Event within its
	// spec.writer.coalesceWindow (see docs/SCHEMA.md's "Rows and occurrences"), so
	// how often an Event fired is in this number and never in how many rows the
	// timeline holds. A row that did not say so would leave a reader counting
	// rows, which is the one reading the archive cannot support.
	Count int
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
		Count:    eventCount(object),
	}, true
}

// Summary renders the Event's contribution to the CHANGE column, with the
// warning glyph attached when it belongs there and the occurrence count when the
// Event has fired more than once.
//
// The glyph leads because it qualifies everything after it, and because a column
// scanned downwards for trouble is scanned down its left edge.
//
// # Why the count is near the left edge too
//
// `BackOff: Back-off restarting failed container` beside the same line reading
// ×47 is the difference between noise and a signal, and the two have to be
// distinguishable at the width a terminal actually is. The CHANGE column
// truncates from the right, so a count appended to the message is the first thing
// a narrow terminal removes — and it would be removed from exactly the rows whose
// messages are longest, which for an Event means the ones carrying a quota or a
// scheduling failure. Attaching it to the reason puts it where `kubectl get
// events` puts it and where nothing can take it.
func (e EventDetail) Summary() string {
	text := e.Reason
	switch {
	case text == "" && e.Message == "":
		// Both empty is a real state — an Event object stripped by a redaction
		// policy, or one this cluster wrote oddly — and it is said rather than
		// rendered as an empty cell. The count still leads it, because "this
		// unreadable thing happened 47 times" is more than half of what the row
		// has left to say.
		text = e.countPrefix() + "Kubernetes Event with no reason or message recorded"
	case text == "":
		text = e.countPrefix() + collapseWhitespace(e.Message)
	case e.Message != "":
		text += e.countSuffix() + ": " + collapseWhitespace(e.Message)
	default:
		text += e.countSuffix()
	}
	if e.Warning() {
		return WarningGlyph + " " + text
	}
	return text
}

// countSuffix is " ×47" for an Event that fired 47 times, and empty otherwise.
//
// Empty for a count of one as well as for an absent one: a marker on every row
// would be noise on the majority of them, and it is the *contrast* between a row
// carrying one and a row carrying none that makes a recurrence visible at a
// glance. An Event's first occurrence is also the count the modern API omits
// entirely, so rendering ×1 would print a number for the rows that have one and
// nothing for the rows that mean the same thing.
func (e EventDetail) countSuffix() string {
	if e.Count < 2 {
		return ""
	}
	return " " + CountGlyph + strconv.Itoa(e.Count)
}

// countPrefix is the same marker for a cell with no reason to hang it on.
func (e EventDetail) countPrefix() string {
	if suffix := e.countSuffix(); suffix != "" {
		return strings.TrimPrefix(suffix, " ") + " "
	}
	return ""
}

// eventCount reads the cumulative occurrence count across both APIs, taking the
// largest of the three spellings.
//
// The largest rather than the first populated, which is the form docs/QUERIES.md
// publishes as `greatest(count, deprecatedCount, series.count)`: core v1 writes
// `count`; events.k8s.io/v1 renders a legacy Event's count as `deprecatedCount`
// and fills `series.count` only for an Event the API server aggregated into a
// series. An Event may carry more than one of them, and which one is populated is
// a property of the recorder rather than of the occurrence — so the CLI and the
// SQL must agree, or a `jq` recipe and a query over the same row would disagree
// about how often something happened.
//
// An Event carrying none of them fired once, and reports 0 here rather than 1:
// see countSuffix for why that distinction does not reach the screen.
func eventCount(object map[string]any) int {
	count := numberField(object, eventCountField)
	count = max(count, numberField(object, eventDeprecatedCountField))
	if series, ok := object[eventSeriesField].(map[string]any); ok {
		count = max(count, numberField(series, eventCountField))
	}
	return count
}

// maxEventCount is the largest count this renderer will believe.
//
// Both APIs type the field as an int32, so anything above it was not written by a
// Kubernetes API server, and converting an out-of-range float64 to an int is
// implementation-defined in Go — a cell reading ×-9223372036854775808 would be
// this renderer inventing a fact rather than reporting a strange one.
const maxEventCount = 1<<31 - 1

// numberField reads a positive whole JSON number, treating anything else as
// absent.
//
// Absent rather than coerced, for stringField's reason one type along: JSON has
// one number type, so a count of 3 arrives as 3.0 and is read, while 3.5 is a
// cluster writing something this renderer does not understand and is not rounded
// into a number a reader would trust.
func numberField(object map[string]any, name string) int {
	value, ok := object[name].(float64)
	if !ok || value < 1 || value > maxEventCount {
		return 0
	}
	whole := int(value)
	if float64(whole) != value {
		return 0
	}
	return whole
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
