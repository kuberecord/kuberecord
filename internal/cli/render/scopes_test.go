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
	"bytes"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The three cells of a scope row whose blank would mean something else.
//
// Each of these renders an absence, and each absence is a different fact: a scope
// that is still being watched, a scope that covers every namespace, and a rule
// that is genuinely not recorded. A blank cell for any of them reads as a value —
// and for the first two it reads as the *opposite* of the truth.

// TestScopeCellsNameEveryAbsence is the table for those three.
func TestScopeCellsNameEveryAbsence(t *testing.T) {
	stop := mustInstant("2026-08-11T17:31:22Z")

	tests := []struct {
		name                          string
		interval                      query.ScopeInterval
		kind, namespace, rule, ending string
	}{
		{
			name: "a namespaced scope in a group, closed",
			interval: query.ScopeInterval{
				APIGroup: "apps", Kind: "Deployment", Namespace: "payments",
				RuleRef: "StreamRule/payments/workloads",
				From:    mustInstant("2026-06-01T08:00:00Z"), To: &stop,
			},
			kind: "apps/Deployment", namespace: "payments",
			rule: "StreamRule/payments/workloads", ending: "2026-08-11",
		},
		{
			name: "a core-group scope, still open",
			interval: query.ScopeInterval{
				Kind: "ConfigMap", Namespace: "payments", RuleRef: "StreamRule/payments/config",
				From: mustInstant("2026-07-02T09:14:00Z"),
			},
			kind: "ConfigMap", namespace: "payments",
			rule: "StreamRule/payments/config", ending: render.OpenInterval,
		},
		{
			name: "a cluster-wide scope whose rule is gone",
			interval: query.ScopeInterval{
				APIGroup: "apps", Kind: "Deployment",
				From: mustInstant("2026-07-02T09:14:00Z"), To: &stop,
			},
			kind: "apps/Deployment", namespace: render.AllNamespaces,
			rule: "(not recorded)", ending: "2026-08-11",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := render.ScopeKind(test.interval); got != test.kind {
				t.Errorf("kind cell is %q, want %q", got, test.kind)
			}
			if got := render.ScopeNamespace(test.interval); got != test.namespace {
				t.Errorf("namespace cell is %q, want %q", got, test.namespace)
			}
			if got := render.ScopeRule(test.interval); got != test.rule {
				t.Errorf("rule cell is %q, want %q", got, test.rule)
			}

			var out bytes.Buffer
			doc := render.ScopesDocument{
				Cluster:   "prod-eu-1",
				Scope:     "every kind in every namespace",
				Window:    "all recorded history",
				Intervals: []query.ScopeInterval{test.interval},
			}
			if err := render.WriteScopes(&out, nil, doc, render.Options{Width: 120}); err != nil {
				t.Fatalf("WriteScopes: %v", err)
			}
			if !strings.Contains(out.String(), test.ending) {
				t.Errorf("the TO cell does not read %q:\n%s", test.ending, out.String())
			}
		})
	}
}

// TestScopesWritesNoTableForNoPeriods keeps an empty listing from reading as an
// answer.
//
// A heading row with nothing under it says the question was asked and the answer
// was none. For this command that is precisely the reading that must never be
// offered without the finding on the other stream, so the header is written and
// the table is not.
func TestScopesWritesNoTableForNoPeriods(t *testing.T) {
	var out, errOut bytes.Buffer
	doc := render.ScopesDocument{
		Cluster: "prod-eu-1",
		Scope:   "Secret in namespace payments",
		Window:  "all recorded history",
		Notices: []render.Notice{{Text: "nothing was ever watching this", Warning: true}},
	}
	if err := render.WriteScopes(&out, &errOut, doc, render.Options{Width: 120}); err != nil {
		t.Fatalf("WriteScopes: %v", err)
	}

	if strings.Contains(out.String(), "KIND") {
		t.Errorf("an empty listing printed a heading row:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Secret in namespace payments") {
		t.Errorf("the header does not name the question that came back empty:\n%s", out.String())
	}
	if !strings.Contains(errOut.String(), "nothing was ever watching this") {
		t.Errorf("the notice did not reach stderr:\n%s", errOut.String())
	}
	if strings.Contains(out.String(), "nothing was ever watching this") {
		t.Errorf("a notice reached stdout, where a pipe would receive it:\n%s", out.String())
	}
}
