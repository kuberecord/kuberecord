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

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `scopes`, the command every other command's empty result points at.
//
// The rendering is asserted by golden file for the reason the timeline's is: what
// is under test is a whole page of characters whose alignment and stream split
// matter at once. What is asserted by content, separately, is the pair of facts a
// regenerated golden file would carry along with a regression — that an open
// interval says so in a word, and that an answer with no periods in it exits 3
// rather than presenting itself as an empty list.

// closedAt returns a pointer to a fixture timestamp, for an interval that ended.
func closedAt(clock string) *time.Time {
	stamp := at(clock)
	return &stamp
}

// recordedScopes is the fixture listing: one closed period, one still open, and
// one cluster-wide scope that covers the namespaced question.
//
// The three are deliberately not variations on each other. A reader has to be able
// to tell a period that ended from one that is still running, and a scope pinned
// to a namespace from one that covers every namespace — and those two distinctions
// are where a listing of periods can most easily mislead.
func recordedScopes() []query.ScopeInterval {
	return []query.ScopeInterval{
		{
			APIGroup: "apps", Kind: "Deployment", Namespace: "payments",
			RuleRef: "StreamRule/payments/workloads",
			From:    at("2026-06-01T08:00:00Z"), To: closedAt("2026-07-02T09:14:00Z"),
		},
		{
			APIGroup: "apps", Kind: "Deployment",
			RuleRef: "ClusterStreamRule/all-workloads",
			From:    at("2026-07-02T09:14:00Z"),
		},
		{
			Kind: "ConfigMap", Namespace: "payments",
			From: at("2026-07-02T09:14:00Z"), To: closedAt("2026-08-11T17:31:22Z"),
		},
	}
}

// scopesRequest is a bare `scopes` over the fixture's cluster.
func scopesRequest() cli.ScopesRequest {
	return cli.ScopesRequest{ClusterID: fixtureCluster}
}

// runScopes drives the command against a fake engine and returns both streams.
func runScopes(
	t *testing.T, engine *fakeEngine, request cli.ScopesRequest, opts render.Options,
) (stdout, stderr string, err error) {
	t.Helper()

	if opts.Width == 0 {
		opts.Width = goldenWidth
	}
	var out, errOut bytes.Buffer
	backend := &resolve.Backend{Engine: engine, ClusterID: fixtureCluster}

	err = cli.RunScopes(context.Background(), backend, request, ioStreams(&out, &errOut), opts)
	return out.String(), errOut.String(), err
}

// TestScopesRendersThePeriods is the acceptance criterion's column list.
func TestScopesRendersThePeriods(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}

	stdout, stderr, err := runScopes(t, engine, scopesRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunScopes: %v", err)
	}
	assertGoldenIn(t, "scopes", "table", stdout, stderr)

	for _, want := range []string{"KIND", "NAMESPACE", "FROM", "TO", "RULE"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the %s column is missing:\n%s", want, stdout)
		}
	}

	// The two cells that carry a meaning a blank would not. An interval with no
	// end is being watched now, and an interval with no namespace covers all of
	// them — both are facts, and a reader must not have to infer them from an
	// empty cell.
	if !strings.Contains(stdout, render.OpenInterval) {
		t.Errorf("a still-open period is not marked %s:\n%s", render.OpenInterval, stdout)
	}
	if !strings.Contains(stdout, render.AllNamespaces) {
		t.Errorf("a cluster-wide scope is not marked %s:\n%s", render.AllNamespaces, stdout)
	}
}

// TestScopesWideKeepsFullPrecision covers `-o wide`.
func TestScopesWideKeepsFullPrecision(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}

	stdout, stderr, err := runScopes(t, engine, scopesRequest(), render.Options{Wide: true})
	if err != nil {
		t.Fatalf("RunScopes: %v", err)
	}
	assertGoldenIn(t, "scopes", "wide", stdout, stderr)
}

// TestScopesStructuredRenderings covers the three serializations of the envelope.
func TestScopesStructuredRenderings(t *testing.T) {
	for name, format := range map[string]render.StructuredFormat{
		"json":  render.StructuredJSON,
		"jsonl": render.StructuredJSONL,
		"yaml":  render.StructuredYAML,
	} {
		t.Run(name, func(t *testing.T) {
			engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}
			request := scopesRequest()
			request.Structured = format

			stdout, stderr, err := runScopes(t, engine, request, render.Options{})
			if err != nil {
				t.Fatalf("RunScopes: %v", err)
			}
			assertGoldenIn(t, "scopes", name, stdout, stderr)
		})
	}
}

// TestScopesItemsAreTheIntervals checks the Coverage envelope's item shape.
//
// The items are query.ScopeInterval's own field names, and a still-open period
// carries a null `to` rather than a zero timestamp — which is the whole reason
// that field is a pointer in the contract: "still open" and "ended at the epoch"
// must not be the same value.
func TestScopesItemsAreTheIntervals(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}
	request := scopesRequest()
	request.Structured = render.StructuredJSON

	stdout, _, err := runScopes(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunScopes: %v", err)
	}

	items := assertEnvelope(t, decodeJSON(t, stdout), render.KindCoverage)
	if len(items) != len(recordedScopes()) {
		t.Fatalf("the envelope holds %d items, want %d", len(items), len(recordedScopes()))
	}

	open, ok := items[1].(map[string]any)
	if !ok {
		t.Fatalf("an item is not an object: %#v", items[1])
	}
	for _, field := range []string{"api_group", "kind", "namespace", "rule_ref", "from", "to"} {
		if _, present := open[field]; !present {
			t.Errorf("the item has no %q field:\n%#v", field, open)
		}
	}
	if open["to"] != nil {
		t.Errorf("a still-open period's `to` is %#v, want null: a zero timestamp would read as a "+
			"period that ended", open["to"])
	}
}

// TestScopesEmptyIsAFindingNotAList is Invariant 9 reached directly.
//
// Nothing was ever watching, so nothing this cluster recorded — or failed to
// record — about that scope means what it appears to mean. That is exit 3, the
// same code `timeline` reaches when it works the fact out from the other end, so
// that one script keys on one code whichever command it asked.
func TestScopesEmptyIsAFindingNotAList(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities()}
	request := scopesRequest()
	request.Kind, request.APIGroup = "Secret", ""
	request.Namespace = "payments"

	stdout, _, err := runScopes(t, engine, request, render.Options{})
	if err == nil {
		t.Fatal("an empty listing was reported as a successful empty answer")
	}
	if code := exit.CodeFor(err); code != exit.NoCoverage {
		t.Errorf("exit code %d, want %d: %v", code, exit.NoCoverage, err)
	}
	for _, want := range []string{"Secret in namespace payments", fixtureCluster} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the finding does not name %q: %v", want, err)
		}
	}

	// The header is still written: it is what says which question came back
	// empty, and a finding with no question attached is not actionable.
	assertGoldenIn(t, "scopes", "empty", stdout, "")
}

// TestScopesRefusesToGuessWithoutAScopeLog covers the one command that cannot
// degrade.
//
// Every other command answers what it can and says what it could not (Invariant
// 5). This command *is* the scope log, so there is no remaining half — and an
// empty table here would read as "nothing was watching", which is the one
// conclusion it must never reach by accident.
func TestScopesRefusesToGuessWithoutAScopeLog(t *testing.T) {
	engine := &fakeEngine{
		caps:        archiveCapabilities(),
		coverageErr: query.ErrCapabilityUnsupported,
	}

	stdout, _, err := runScopes(t, engine, scopesRequest(), render.Options{})
	if err == nil {
		t.Fatal("a backend with no scope log produced a listing anyway")
	}
	if code := exit.CodeFor(err); code != exit.RuntimeError {
		t.Errorf("exit code %d, want %d: %v", code, exit.RuntimeError, err)
	}
	if !strings.Contains(err.Error(), "has no scope log") {
		t.Errorf("the message does not say why there is no answer: %v", err)
	}
	if stdout != "" {
		t.Errorf("an empty listing was written, which reads as \"nothing was watching\":\n%s", stdout)
	}
}

// TestScopesExplainsACoveringClusterWideScope keeps a filter from looking ignored.
//
// A namespaced question returns the cluster-wide scopes too, because they
// genuinely were watching objects in that namespace. Without the notice, a row
// saying (all) in reply to `-n payments` reads as the --namespace flag having been
// dropped.
func TestScopesExplainsACoveringClusterWideScope(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}
	request := scopesRequest()
	request.Namespace = "payments"

	_, stderr, err := runScopes(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunScopes: %v", err)
	}
	for _, want := range []string{render.AllNamespaces, "cluster-wide", "payments"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the notice does not mention %q:\n%s", want, stderr)
		}
	}
}

// TestScopesRefusesMalformedInvocations pins the usage errors.
//
// Exit code 2 for all of them, and every one is rejected before a backend is
// opened: a mistyped flag must not cost a connection.
func TestScopesRefusesMalformedInvocations(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{
			name: "an object address, which this command does not take",
			argv: []string{"scopes", "deploy/checkout"},
			want: "takes no arguments",
		},
		{
			name: "the hunk rendering, which has no patch to lay out",
			argv: []string{"scopes", "-o", "diff"},
			want: "scopes does not render diff",
		},
		{
			name: "an unreadable --since",
			argv: []string{"scopes", "--since", "last tuesday"},
			want: "neither a duration",
		},
		{
			name: "a window that ends before it starts",
			argv: []string{"scopes", "--since", "2026-08-20", "--until", "2026-08-01"},
			want: "ends before it starts",
		},
		{
			name: "a flag this command does not have",
			argv: []string{"scopes", "--nonesuch"},
			want: "unknown flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			code := cli.Run(append([]string{"kuberecord"}, test.argv...), io)

			if code != exit.UsageError {
				t.Errorf("exit code %d, want %d.\nstderr:\n%s", code, exit.UsageError, errOut.String())
			}
			if !strings.Contains(errOut.String(), test.want) {
				t.Errorf("the message does not explain the mistake.\nwant it to contain %q\ngot:\n%s",
					test.want, errOut.String())
			}
			if out.Len() != 0 {
				t.Errorf("a usage error reached stdout, where a pipe would receive it:\n%s", out.String())
			}
		})
	}
}

// The offline path, end to end through the real entry point.
//
// An archive on a laptop with the cluster it came from long gone is a supported
// way to work (D18), and `scopes` is the command an auditor reaches for first —
// so the kind has to resolve without discovery data, and the answer has to be
// honest about an archive that recorded nothing.

// TestScopesReadsARecordedKindWithoutACluster resolves --kind against no cluster
// at all.
//
// Exit 3 follows because the empty archive holds no scope log entries: nothing was
// ever watching, which is the finding rather than an empty list.
func TestScopesReadsARecordedKindWithoutACluster(t *testing.T) {
	unreachableCluster(t)

	io, out, errOut := streams()
	code := cli.Run([]string{
		"kuberecord", "scopes", "--kind", "Deployment.apps",
		"--source", t.TempDir(), "--cluster-id", "c", "--color=never",
	}, io)

	if code != exit.NoCoverage {
		t.Fatalf("exit code %d, want %d.\nstdout:\n%s\nstderr:\n%s",
			code, exit.NoCoverage, out.String(), errOut.String())
	}
	if !strings.Contains(errOut.String(), "read --kind Deployment.apps as apps/Deployment as recorded") {
		t.Errorf("the offline resolution was not announced:\n%s", errOut.String())
	}
	if !strings.Contains(out.String(), "Scope:   apps/Deployment in every namespace") {
		t.Errorf("the header does not name the question that came back empty:\n%s", out.String())
	}
}

// TestScopesRefusesAShortNameWithoutACluster is the other half of that story.
//
// Expanding `deploy` needs the server's own discovery data. Guessing at it would
// report that a kind nobody spells that way was never watched, which is the one
// conclusion this command must never reach by accident.
func TestScopesRefusesAShortNameWithoutACluster(t *testing.T) {
	unreachableCluster(t)

	io, _, errOut := streams()
	code := cli.Run([]string{
		"kuberecord", "scopes", "--kind", "deploy",
		"--source", t.TempDir(), "--cluster-id", "c",
	}, io)

	if code != exit.RuntimeError {
		t.Fatalf("exit code %d, want %d.\nstderr:\n%s", code, exit.RuntimeError, errOut.String())
	}
	for _, want := range []string{"could not be reached", "Deployment.apps"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("the message does not contain %q:\n%s", want, errOut.String())
		}
	}
}

// TestScopesWritesTheEnvelopeWithTheFinding is the property a script depends on
// when the answer is the finding.
//
// Exit 3 and a parseable document are not alternatives. A wrapper that branches on
// the code still has stdout to log, and one that parses stdout still sees an empty
// items list with the coverage that explains it.
func TestScopesWritesTheEnvelopeWithTheFinding(t *testing.T) {
	unreachableCluster(t)

	io, out, _ := streams()
	code := cli.Run([]string{
		"kuberecord", "scopes", "-o", "json",
		"--source", t.TempDir(), "--cluster-id", "c", "--color=never",
	}, io)

	if code != exit.NoCoverage {
		t.Fatalf("exit code %d, want %d.\nstdout:\n%s", code, exit.NoCoverage, out.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("stdout is not valid JSON, so the finding is not scriptable: %v\n%s", err, out.String())
	}
	if decoded["kind"] != render.KindCoverage {
		t.Errorf("kind is %#v, want %q", decoded["kind"], render.KindCoverage)
	}
	items, ok := decoded["items"].([]any)
	if !ok || len(items) != 0 {
		t.Errorf("items is %#v, want an empty list", decoded["items"])
	}
}

// TestScopesIsInTheCommandTree is the wiring check.
//
// It matters more here than for the other commands: three notices elsewhere in
// this package tell a reader to run `scopes`, and a tree without it would make
// every one of them advice that does not work.
func TestScopesIsInTheCommandTree(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "--help"}, io); code != exit.Success {
		t.Fatalf("`--help` exited %d", code)
	}
	if !strings.Contains(out.String(), "scopes") {
		t.Errorf("`scopes` is not listed in the root command's help:\n%s", out.String())
	}
}
