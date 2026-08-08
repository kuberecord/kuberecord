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

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/pipeline"
)

// canonicalRedaction projects a sink's redaction floor and a rule's additions
// into the single canonical string a plan.WatchTarget carries: the union of both
// path sets, sorted, deduplicated, newline-separated. The empty string means
// "nothing beyond the data plane's built-in scrubs".
//
// Union — never override — is the whole point of splitting the policy across two
// CRs (see v1alpha1.RedactionRule): the sink's owner sets a floor that the rule's
// author can raise but not lower.
//
// Sorting and deduplicating here, rather than in the registry or the data plane,
// is what keeps a policy edit that only reorders or repeats entries from looking
// like a change: the canonical form is byte-identical, so the registry's target
// diff is empty and no informer, epoch, or compiled policy churns.
func canonicalRedaction(sinkFloor, ruleExtra []v1alpha1.RedactionRule) string {
	paths := make([]string, 0, len(sinkFloor)+len(ruleExtra))
	for _, rule := range slices.Concat(sinkFloor, ruleExtra) {
		if path := redactionPath(rule); path != "" {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return strings.Join(slices.Compact(paths), "\n")
}

// redactionPath renders one rule as a data-plane path.
//
// The annotation shorthand is expanded through pipeline.AnnotationRedactionPath
// rather than by formatting a string here, so the control plane's rendering and
// the data plane's parsing are the same definition rather than two that agree
// until one of them is edited.
//
// A rule with neither field set renders empty and is dropped by the caller. CRD
// validation makes that unreachable (exactly one field is required), but a rule
// that reached the reconciler through some other path must not turn into a path
// that scrubs the object's root.
func redactionPath(rule v1alpha1.RedactionRule) string {
	switch {
	case rule.Annotation != "":
		return pipeline.AnnotationRedactionPath(rule.Annotation)
	default:
		return rule.FieldPath
	}
}
