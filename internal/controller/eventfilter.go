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
	"fmt"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/watch"
)

// canonicalEventFilter projects one watched resource's `eventFilter` into the
// single canonical string a plan.WatchTarget carries (Task 19.2). A nil filter —
// every resource that is not an Event, and every Event entry whose author
// narrowed nothing — yields the empty string, which the data plane reads as
// "record every Event in scope".
//
// It is the conversion, and the only conversion, between the CRD's Event filter
// and the data plane's: internal/watch, internal/plan and internal/pipeline
// import no API types at all, so the CR-shaped value stops here and a
// watch.EventFilterSpec of plain strings continues. That is the same seam
// canonicalRedaction sits on, for the same reason — the data plane
// level-triggers towards a registry of strings and must stay free of any notion
// of a custom resource.
//
// Rendering (sorting, deduplicating, dropping empty entries) is deliberately not
// duplicated here. watch.CanonicalEventFilter owns the wire form because the
// package that has to *parse* it is the one that should decide how it is
// written; a renderer here and a parser there would be two descriptions of one
// format that agree until one of them is edited.
func canonicalEventFilter(filter *v1alpha1.EventFilter) (string, error) {
	if filter == nil {
		return "", nil
	}

	canonical, err := watch.CanonicalEventFilter(watch.EventFilterSpec{
		Types:            eventTypeStrings(filter.Types),
		Reasons:          filter.Reasons,
		ExcludeReasons:   filter.ExcludeReasons,
		SourceComponents: filter.SourceComponents,
		SubjectKinds:     filter.SubjectKinds,
		SubjectNames:     filter.SubjectNames,
	})
	if err != nil {
		return "", fmt.Errorf("canonicalize event filter: %w", err)
	}
	return canonical, nil
}

// eventTypeStrings widens the CRD's enum-typed list into the plain strings the
// data plane stores.
//
// The enum lives on the API type so admission can close the set (a rule naming
// `types: [Info]` is rejected before it reaches a reconciler), and it is widened
// rather than carried through because the data plane compares an Event's own
// `type` — an arbitrary string out of an unstructured object — against it. A
// named type on one side of that comparison would buy nothing and would put an
// API type in the registry.
func eventTypeStrings(types []v1alpha1.EventType) []string {
	if len(types) == 0 {
		return nil
	}
	out := make([]string, 0, len(types))
	for _, eventType := range types {
		out = append(out, string(eventType))
	}
	return out
}
