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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/buildinfo"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	chquery "github.com/kuberecord/kuberecord/internal/query/clickhouse"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
)

// versionOutput is the parsed structured document, spelled here rather than
// imported so that a rename of an exported field in the production type is a
// failure of this test rather than a silent agreement with it. The tags are the
// contract (D19).
type versionOutput struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	BuildDate  string `json:"buildDate"`
	GoVersion  string `json:"goVersion"`
	Platform   string `json:"platform"`
	Backends   []struct {
		Name        string `json:"name"`
		Engine      string `json:"engine"`
		Description string `json:"description"`
	} `json:"backends"`
}

// TestVersionPrintsTheBuildAndItsBackends covers the human form, which is what
// somebody pastes into a bug report.
func TestVersionPrintsTheBuildAndItsBackends(t *testing.T) {
	stdout, stderr, code := run(t, "version")

	if code != exit.Success {
		t.Fatalf("exit code = %d, want %d\nstderr: %s", code, exit.Success, stderr)
	}
	// Diagnostics have a stream of their own, and `version` produces none: it
	// resolves nothing, so there is no notice to write.
	if stderr != "" {
		t.Errorf("stderr = %q, want nothing", stderr)
	}

	for _, want := range []string{"commit", "built", "go", "query backends compiled in:"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not mention %q:\n%s", want, stdout)
		}
	}
	// Every backend, by both names. The pair is the point: `name` is what a
	// profile spells and `engine` is what metadata.backend carries, and a reader
	// holding an answer needs to be able to match one to the other.
	for _, backend := range resolve.CompiledBackends() {
		if !strings.Contains(stdout, string(backend.Kind)) {
			t.Errorf("output does not name the %q backend:\n%s", backend.Kind, stdout)
		}
		if !strings.Contains(stdout, backend.Engine) {
			t.Errorf("output does not name the %q engine:\n%s", backend.Engine, stdout)
		}
	}
}

// TestVersionStructuredOutputCarriesTheContract asserts the shape a script reads.
func TestVersionStructuredOutputCarriesTheContract(t *testing.T) {
	stdout, stderr, code := run(t, "version", "-o", "json")
	if code != exit.Success {
		t.Fatalf("exit code = %d, want %d\nstderr: %s", code, exit.Success, stderr)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want nothing: a banner here would reach a `| jq`", stderr)
	}

	var doc versionOutput
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("the output is not JSON: %v\n%s", err, stdout)
	}

	if doc.APIVersion != render.EnvelopeAPIVersion {
		t.Errorf("apiVersion = %q, want %q", doc.APIVersion, render.EnvelopeAPIVersion)
	}
	if doc.Kind != cli.VersionKind {
		t.Errorf("kind = %q, want %q", doc.Kind, cli.VersionKind)
	}

	// An unstamped test binary reports the toolchain's answer or the word that
	// says there is none; what must never happen is an empty string, which reads
	// as a field that does not exist rather than as a fact nothing could supply.
	for name, value := range map[string]string{
		"version": doc.Version, "commit": doc.Commit, "buildDate": doc.BuildDate,
		"goVersion": doc.GoVersion, "platform": doc.Platform,
	} {
		if value == "" {
			t.Errorf("%s is empty; it must be a fact or %q", name, buildinfo.Unknown)
		}
	}

	compiled := resolve.CompiledBackends()
	if len(doc.Backends) != len(compiled) {
		t.Fatalf("backends = %d entries, want %d", len(doc.Backends), len(compiled))
	}
	for i, backend := range compiled {
		got := doc.Backends[i]
		if got.Name != string(backend.Kind) || got.Engine != backend.Engine ||
			got.Description != backend.Description {
			t.Errorf("backends[%d] = %+v, want {%s %s %s}",
				i, got, backend.Kind, backend.Engine, backend.Description)
		}
	}
}

// TestVersionYAMLIsTheSameDocument is the property the two serializations
// promise: one document in two syntaxes, not two documents that resemble each
// other.
func TestVersionYAMLIsTheSameDocument(t *testing.T) {
	asJSON, _, code := run(t, "version", "-o", "json")
	if code != exit.Success {
		t.Fatalf("json exit code = %d", code)
	}
	asYAML, _, code := run(t, "version", "-o", "yaml")
	if code != exit.Success {
		t.Fatalf("yaml exit code = %d", code)
	}

	var fromJSON, fromYAML versionOutput
	if err := json.Unmarshal([]byte(asJSON), &fromJSON); err != nil {
		t.Fatalf("decoding the JSON: %v", err)
	}
	if err := yaml.Unmarshal([]byte(asYAML), &fromYAML); err != nil {
		t.Fatalf("decoding the YAML: %v\n%s", err, asYAML)
	}
	if !equalVersions(fromJSON, fromYAML) {
		t.Errorf("the two serializations disagree:\njson: %+v\nyaml: %+v", fromJSON, fromYAML)
	}
}

// TestVersionRefusesFormatsItHasNoDocumentFor is the "no silent errors" half.
//
// `jsonl` streams one item per line for a result larger than memory and `diff`
// renders change operations; this document is one item and has no operations.
// Rendering something else regardless would leave a user believing their flag did
// something.
func TestVersionRefusesFormatsItHasNoDocumentFor(t *testing.T) {
	for _, format := range []string{"jsonl", "diff"} {
		t.Run(format, func(t *testing.T) {
			stdout, stderr, code := run(t, "version", "-o", format)

			if code != exit.UsageError {
				t.Errorf("exit code = %d, want %d for a format this command does not render",
					code, exit.UsageError)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want nothing: a refusal must not reach a pipe", stdout)
			}
			if !strings.Contains(stderr, format) {
				t.Errorf("the refusal does not name %q, so a reader cannot tell what was "+
					"rejected:\n%s", format, stderr)
			}
		})
	}
}

// TestVersionTakesNoArguments keeps a typo a usage error, which is the code a
// wrapper script is told not to retry.
func TestVersionTakesNoArguments(t *testing.T) {
	_, stderr, code := run(t, "version", "extra")
	if code != exit.UsageError {
		t.Errorf("exit code = %d, want %d\nstderr: %s", code, exit.UsageError, stderr)
	}
}

// TestVersionContactsNothing is the property that makes the command useful when
// everything else has failed.
//
// A kubeconfig that does not exist and no --source at all would fail any command
// that resolves a backend, so a `version` that succeeds here has resolved
// nothing.
func TestVersionContactsNothing(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-version-test")

	stdout, stderr, code := run(t, "version")
	if code != exit.Success {
		t.Fatalf("exit code = %d with no reachable cluster, want %d\nstderr: %s",
			code, exit.Success, stderr)
	}
	if stdout == "" {
		t.Error("no output")
	}
}

// TestCompiledBackendsCoversEveryBackendKind is the anti-drift check between what
// this build can open and what it says it can open.
//
// The two lists are written in two places on purpose — one is the vocabulary a
// profile is validated against, the other is what `version` prints — and the
// failure this catches is the quiet one: a backend added to the resolver and not
// to the report, which produces a CLI that can read something it denies being
// able to read.
func TestCompiledBackendsCoversEveryBackendKind(t *testing.T) {
	compiled := resolve.CompiledBackends()

	kinds := make([]resolve.BackendKind, 0, len(compiled))
	for _, backend := range compiled {
		kinds = append(kinds, backend.Kind)

		if backend.Engine == "" || backend.Description == "" {
			t.Errorf("the %q backend reports engine %q and description %q; both are read by "+
				"somebody deciding whether their build can open their archive",
				backend.Kind, backend.Engine, backend.Description)
		}
		// The engine names are the two the read plane actually ships. A third
		// spelling here would be a name that appears in `version` and never in
		// metadata.backend, which is the comparison the column exists for.
		if backend.Engine != chquery.BackendName && backend.Engine != objectsource.BackendName {
			t.Errorf("the %q backend names the engine %q, which is neither %q nor %q",
				backend.Kind, backend.Engine, chquery.BackendName, objectsource.BackendName)
		}
	}

	if !slices.Equal(kinds, resolve.BackendKinds) {
		t.Errorf("CompiledBackends reports %v, but a profile may name %v; a backend in one "+
			"list and not the other is a build that can read something it denies reading",
			kinds, resolve.BackendKinds)
	}
}

// TestVersionIsInTheCommandList keeps it discoverable: a command nobody is told
// about is one nobody runs when they need it.
func TestVersionIsInTheCommandList(t *testing.T) {
	stdout, _, code := run(t, "--help")
	if code != exit.Success {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(stdout, "version") {
		t.Errorf("`--help` does not list `version`:\n%s", stdout)
	}
}

// equalVersions compares two decoded documents field by field, since the backend
// slice makes the struct incomparable.
func equalVersions(a, b versionOutput) bool {
	if a.APIVersion != b.APIVersion || a.Kind != b.Kind || a.Version != b.Version ||
		a.Commit != b.Commit || a.BuildDate != b.BuildDate ||
		a.GoVersion != b.GoVersion || a.Platform != b.Platform {
		return false
	}
	if len(a.Backends) != len(b.Backends) {
		return false
	}
	for i := range a.Backends {
		if a.Backends[i] != b.Backends[i] {
			return false
		}
	}
	return true
}

//
// `version --check` (Task 14.2)
//
// The fixture cluster, the flags and the planted probe are a resolveCase's, and
// the golden's three sections are assertReportGolden's. That sharing is the point
// rather than a convenience: the acceptance criteria say that if `config resolve
// --check` and `version --check` produce different code paths one of them is
// wrong, and two test harnesses describing one walk would be the first place the
// two could quietly diverge.

// fixedBuild is a build identity a golden file can hold.
//
// Every field of buildinfo.Get() varies with the machine, the toolchain and
// whether the tree is dirty, so a golden of the real one would fail on every
// second run. What these cases are about is the setup block; the build lines are
// here so that the golden shows the whole page a user would paste.
func fixedBuild() buildinfo.Info {
	return buildinfo.Info{
		Version:   "v0.4.0",
		Commit:    "77514b632925",
		BuildDate: "2026-09-05T11:02:44Z",
		GoVersion: "go1.25.7",
		Platform:  "linux/amd64",
	}
}

// versionCase is one `version` invocation.
type versionCase struct {
	// chains is the fixture the resolution chains run against. Its own `check`
	// field is this invocation's --check: it decides both whether the identity
	// chain's last step is taken and whether the backend is probed, which is the
	// one flag doing one thing in two places rather than two flags agreeing.
	chains resolveCase

	// format is the rendering. Empty means the global default, which is `table`.
	format options.OutputFormat
}

// run drives one case through cli.RunVersion and returns both streams.
func (c versionCase) run(t *testing.T) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	streams := ioStreams(&out, &errOut)

	// Built only under --check, exactly as the command builds it: a bare
	// invocation must have nothing to inspect, and handing one an inspection
	// anyway would test a path the command cannot reach.
	var inspection *resolve.Inspection
	if c.chains.check {
		inspection = c.chains.inspect(t, streams)
	}

	format := c.format
	if format == "" {
		format = options.OutputTable
	}
	err = cli.RunVersion(t.Context(), fixedBuild(), inspection, cli.VersionRequest{
		Check: c.chains.check, Output: format, InvokedAs: options.StandaloneName,
	}, streams)
	return out.String(), errOut.String(), err
}

// TestVersionCheckReportsTheSetup walks the states `--check` can find, one golden
// each.
//
// One table rather than five functions because the block's value is comparative:
// what a reader does with two of these is put them side by side, and so does a
// reviewer reading the diff.
func TestVersionCheckReportsTheSetup(t *testing.T) {
	tests := []struct {
		name    string
		golden  string
		invoke  versionCase
		wantErr int
	}{
		{
			name:   "no --check: the build, and not a word about a setup nobody asked about",
			golden: "bare",
			invoke: versionCase{chains: resolveCase{sinks: discoverableCluster()}},
		},
		{
			name:   "the backend answered, and the operator's Deployment named the cluster",
			golden: "reachable",
			invoke: versionCase{chains: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				check: true,
				swap:  reachableBackend,
			}},
		},
		{
			name:   "an address that resolves only inside the cluster",
			golden: "unreachable",
			invoke: versionCase{chains: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				check: true,
				swap:  unreachableBackend,
			}},
			wantErr: exit.RuntimeError,
		},
		{
			name:   "nothing to reach, because the backend chain resolved nothing",
			golden: "backend-unresolved",
			invoke: versionCase{chains: resolveCase{
				objects: []runtime.Object{operatorDeployment(operatorNamespace, theCluster)},
				check:   true,
			}},
			wantErr: exit.RuntimeError,
		},
		{
			name:   "an engine that cannot say whether it is reachable",
			golden: "unsupported",
			invoke: versionCase{chains: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				check: true,
				// A bare fakeEngine is not a query.ClusterIDLister, which is the
				// shape this path exists for.
				swap: func(in *resolve.Inspection) {
					in.Backend.Engine = &fakeEngine{caps: clickHouseCapabilities()}
				},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := tc.invoke.run(t)
			if got := exit.CodeFor(err); got != tc.wantErr {
				t.Errorf("exit code %d, want %d (error: %v)", got, tc.wantErr, err)
			}
			assertReportGolden(t, "version-check", tc.golden, "kuberecord version",
				stdout, stderr, err)
		})
	}
}

// reachableBackend plants a backend that answers the cluster listing.
func reachableBackend(inspection *resolve.Inspection) {
	inspection.Backend.Engine = &probeEngine{
		fakeEngine: &fakeEngine{caps: clickHouseCapabilities()},
		ids:        []string{theCluster},
	}
}

// TestVersionCheckIsTheSameProbeAsConfigResolve is the anti-drift check the
// acceptance criteria ask for by name.
//
// Two commands, one fixture, one planted failure. What must match is the probe's
// own report — the outcome word, the sentence under it, the error it carries —
// and the failure each command exits with, because those are produced by the two
// functions the commands share. A copy of checkReachability made for `version`
// would pass every other test in this file and fail this one.
func TestVersionCheckIsTheSameProbeAsConfigResolve(t *testing.T) {
	fixture := func() resolveCase {
		return resolveCase{
			sinks: discoverableCluster(),
			objects: []runtime.Object{
				credentialsSecret(operatorNamespace),
				operatorDeployment(operatorNamespace, theCluster),
			},
			check: true,
			swap:  unreachableBackend,
		}
	}

	resolveJSON := fixture()
	resolveJSON.format = render.StructuredJSON
	resolveOut, _, resolveErr := resolveJSON.run(t)

	versionOut, _, versionErr := versionCase{
		chains: fixture(), format: options.OutputJSON,
	}.run(t)

	var fromResolve struct {
		Check map[string]any `json:"check"`
	}
	var fromVersion struct {
		Setup struct {
			Check map[string]any `json:"check"`
		} `json:"setup"`
	}
	if err := json.Unmarshal([]byte(resolveOut), &fromResolve); err != nil {
		t.Fatalf("the config resolve document is not JSON: %v\n%s", err, resolveOut)
	}
	if err := json.Unmarshal([]byte(versionOut), &fromVersion); err != nil {
		t.Fatalf("the version document is not JSON: %v\n%s", err, versionOut)
	}

	if !maps.Equal(fromResolve.Check, fromVersion.Setup.Check) {
		t.Errorf("the two commands report the same probe differently.\nconfig resolve: %v\nversion: %v",
			fromResolve.Check, fromVersion.Setup.Check)
	}
	if fmt.Sprint(resolveErr) != fmt.Sprint(versionErr) {
		t.Errorf("the two commands fail differently.\nconfig resolve: %v\nversion: %v",
			resolveErr, versionErr)
	}
	if exit.CodeFor(resolveErr) != exit.CodeFor(versionErr) {
		t.Errorf("the two commands exit %d and %d for one unreachable backend",
			exit.CodeFor(resolveErr), exit.CodeFor(versionErr))
	}
	// And the failure carries Task 13.1's explanation, which is what makes the
	// message identical wherever it is met.
	var unreachable *resolve.UnreachableSinkError
	if !errors.As(versionErr, &unreachable) {
		t.Errorf("version --check did not return a diagnosable failure: %v", versionErr)
	}
}

// TestVersionCheckStructuredCarriesTheSetup pins the contract by name.
//
// A golden regenerated after a rename would keep passing; this will not. D19
// makes these field names public, and the reason the structured form exists is
// that a support runbook says "paste the output of `version --check -o json`".
func TestVersionCheckStructuredCarriesTheSetup(t *testing.T) {
	invoke := versionCase{
		chains: resolveCase{
			sinks: discoverableCluster(),
			objects: []runtime.Object{
				credentialsSecret(operatorNamespace),
				operatorDeployment(operatorNamespace, theCluster),
			},
			check: true,
			swap:  reachableBackend,
		},
		format: options.OutputJSON,
	}

	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("RunVersion: %v", err)
	}

	var document map[string]any
	if unmarshalErr := json.Unmarshal([]byte(stdout), &document); unmarshalErr != nil {
		t.Fatalf("the document is not JSON: %v\n%s", unmarshalErr, stdout)
	}

	setup, ok := document["setup"].(map[string]any)
	if !ok {
		t.Fatalf("--check produced no setup block:\n%s", stdout)
	}
	for field, want := range map[string]any{
		"backend":         "ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)",
		"engine":          "clickhouse",
		"clusterID":       theCluster,
		"clusterIDSource": "from the operator Deployment kuberecord-system/kuberecord-controller-manager",
	} {
		if setup[field] != want {
			t.Errorf("setup.%s = %v, want %v", field, setup[field], want)
		}
	}

	// The probe's own field names are `config resolve`'s, which is what lets one
	// `jq` recipe read either document.
	check, ok := setup["check"].(map[string]any)
	if !ok {
		t.Fatalf("the setup block has no check object:\n%s", stdout)
	}
	if check["requested"] != true || check["outcome"] != "reachable" {
		t.Errorf("check = %v, want a requested:true / reachable pair", check)
	}
}

// TestVersionWithoutCheckIsTheDocumentItAlwaysWas.
//
// The bare command is unchanged, and the structured half of "unchanged" is that
// the document gains no key. A `setup` present-and-empty would be this command
// claiming to describe a configuration it never looked at.
func TestVersionWithoutCheckIsTheDocumentItAlwaysWas(t *testing.T) {
	for _, format := range []options.OutputFormat{options.OutputJSON, options.OutputYAML} {
		t.Run(string(format), func(t *testing.T) {
			stdout, _, code := run(t, "version", "-o", string(format))
			if code != exit.Success {
				t.Fatalf("exit code = %d", code)
			}

			var document map[string]any
			source := []byte(stdout)
			if format == options.OutputYAML {
				converted, err := yaml.YAMLToJSON(source)
				if err != nil {
					t.Fatalf("the YAML does not parse: %v\n%s", err, stdout)
				}
				source = converted
			}
			if err := json.Unmarshal(source, &document); err != nil {
				t.Fatalf("the document does not parse: %v\n%s", err, stdout)
			}
			if _, present := document["setup"]; present {
				t.Errorf("a bare `version` carries a setup block it never looked at:\n%s", stdout)
			}
		})
	}
}

// TestVersionCheckDialsNothingWithoutTheFlag is the property that keeps `version`
// usable when everything else has failed.
//
// The assertion is over the engine rather than over the output: an engine that
// was asked anything at all would have recorded the call, and a bare command that
// happened to print the right page while having dialled would still stall for a
// dial timeout in front of the user it exists for.
func TestVersionCheckDialsNothingWithoutTheFlag(t *testing.T) {
	stdout, stderr, err := versionCase{chains: resolveCase{
		sinks:   discoverableCluster(),
		objects: []runtime.Object{credentialsSecret(operatorNamespace)},
		swap: func(*resolve.Inspection) {
			t.Error("the chains were walked by an invocation that gave no --check")
		},
	}}.run(t)

	if err != nil {
		t.Fatalf("RunVersion: %v", err)
	}
	if strings.Contains(stdout, "setup:") {
		t.Errorf("a bare `version` printed a setup block:\n%s", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want nothing", stderr)
	}
}

// TestVersionRefusesAFormatBeforeItDials.
//
// A format this command cannot render is a usage error whether or not --check was
// given, and it must be reported before anything is contacted: making somebody
// wait out a dial timeout to be told their `-o` was misspelled is the opposite of
// what this command is for. The kubeconfig points nowhere, so an invocation that
// resolved first would fail with a cluster error instead.
func TestVersionRefusesAFormatBeforeItDials(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-version-test")
	configHome(t)

	for _, format := range []string{"jsonl", "diff"} {
		t.Run(format, func(t *testing.T) {
			stdout, stderr, code := run(t, "version", "--check", "-o", format)

			if code != exit.UsageError {
				t.Errorf("exit code = %d, want %d", code, exit.UsageError)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want nothing: a refusal must not reach a pipe", stdout)
			}
			if !strings.Contains(stderr, "version renders") || !strings.Contains(stderr, format) {
				t.Errorf("the refusal is not the format's:\n%s", stderr)
			}
		})
	}
}

// TestVersionCheckReportsAnUnreachableClusterThroughTheBinary drives the whole
// command, which is where the flag, the resolver construction and the exit code
// actually live.
//
// A kubeconfig that does not exist is the cheapest way to make the backend chain
// fail without reaching anybody's real cluster, and a `version --check` that
// exited zero from it would be answering "is my setup working" with a yes it
// could not have.
func TestVersionCheckReportsAnUnreachableClusterThroughTheBinary(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-version-test")
	configHome(t)

	stdout, stderr, code := run(t, "version", "--check")

	if code != exit.RuntimeError {
		t.Errorf("exit code = %d, want %d\nstderr: %s", code, exit.RuntimeError, stderr)
	}
	// The build lines are still there: the reason to print the document before
	// returning the failure is that a bug report needs both halves.
	if !strings.Contains(stdout, "query backends compiled in:") {
		t.Errorf("a failed --check swallowed the version itself:\n%s", stdout)
	}
	if !strings.Contains(stdout, "setup:") {
		t.Errorf("a failed --check printed no setup block:\n%s", stdout)
	}
	if !strings.Contains(stdout, "config resolve --check") {
		t.Errorf("the failure does not name the command that reports the steps:\n%s", stdout)
	}
	if !strings.Contains(stderr, "error:") {
		t.Errorf("the failure did not reach stderr:\n%s", stderr)
	}
}

// TestVersionCheckYAMLIsTheSameDocument, with the setup block in it.
func TestVersionCheckYAMLIsTheSameDocument(t *testing.T) {
	fixture := func(format options.OutputFormat) versionCase {
		return versionCase{
			chains: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				check: true,
				swap:  reachableBackend,
			},
			format: format,
		}
	}

	asJSON, _, err := fixture(options.OutputJSON).run(t)
	if err != nil {
		t.Fatalf("RunVersion -o json: %v", err)
	}
	asYAML, _, err := fixture(options.OutputYAML).run(t)
	if err != nil {
		t.Fatalf("RunVersion -o yaml: %v", err)
	}

	converted, err := yaml.YAMLToJSON([]byte(asYAML))
	if err != nil {
		t.Fatalf("the YAML document does not parse: %v\n%s", err, asYAML)
	}

	var fromJSON, fromYAML any
	if unmarshalErr := json.Unmarshal([]byte(asJSON), &fromJSON); unmarshalErr != nil {
		t.Fatalf("the JSON document does not parse: %v", unmarshalErr)
	}
	if unmarshalErr := json.Unmarshal(converted, &fromYAML); unmarshalErr != nil {
		t.Fatalf("the converted YAML does not parse: %v", unmarshalErr)
	}
	if fmt.Sprint(fromJSON) != fmt.Sprint(fromYAML) {
		t.Errorf("the two serializations are different documents.\n--- json ---\n%s\n--- yaml ---\n%s",
			asJSON, asYAML)
	}
}

// TestVersionCheckNeverPrintsACredential, at any verbosity and in any format.
//
// This document is pasted into bug reports — that is what --check is for — so the
// assertion is over the whole invocation rather than over the one line written
// with it in mind.
func TestVersionCheckNeverPrintsACredential(t *testing.T) {
	t.Setenv(clickHousePasswordEnv, theSecret)

	for _, format := range []options.OutputFormat{
		options.OutputTable, options.OutputJSON, options.OutputYAML,
	} {
		t.Run(string(format), func(t *testing.T) {
			stdout, stderr, err := versionCase{
				chains: resolveCase{
					sinks: discoverableCluster(),
					objects: []runtime.Object{
						credentialsSecret(operatorNamespace),
						operatorDeployment(operatorNamespace, theCluster),
					},
					args:  []string{"-v", "10"},
					check: true,
					swap:  reachableBackend,
				},
				format: format,
			}.run(t)
			if err != nil {
				t.Fatalf("RunVersion: %v", err)
			}
			for name, written := range map[string]string{"stdout": stdout, "stderr": stderr} {
				if strings.Contains(written, theSecret) {
					t.Errorf("%s carries the password read from the Secret:\n%s", name, written)
				}
			}
		})
	}
}

// TestVersionHelpNamesTheCheckFlag keeps it discoverable where a user looks for
// it, which is the same place the Definition of Done requires every flag to
// appear.
func TestVersionHelpNamesTheCheckFlag(t *testing.T) {
	stdout, _, code := run(t, "version", "--help")
	if code != exit.Success {
		t.Fatalf("version --help exited %d", code)
	}
	if !strings.Contains(stdout, "--"+options.FlagCheck) {
		t.Errorf("--%s is absent from the command's help:\n%s", options.FlagCheck, stdout)
	}
	// And the help says where the detail is, because a summary that does not name
	// its long form leaves a reader with nowhere to go.
	if !strings.Contains(stdout, "config resolve --check") {
		t.Errorf("the help does not point at the command that reports the steps:\n%s", stdout)
	}
}
