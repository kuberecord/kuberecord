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
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `config resolve`, asserted against golden files.
//
// Golden files rather than assembled expectations for the reason the timeline's
// are: the thing under test is a whole page whose alignment and section order are
// the readability it exists for, and a test asserting it field by field would
// assert everything except how it reads.
//
// Each file carries three sections. The two streams are there because the split
// between them is under test — the document is data and belongs on stdout, and a
// notice that migrated there would corrupt a pipe. The third is what the top of
// the CLI would print for the failure the command returned, which is where Task
// 13.1's explanation reaches a reader: this command routes an unreachable backend
// through exactly the path every query command routes it through, and a golden
// that stopped at the command's own two streams would not show that it did.

// The marker the golden files use for the third section. The other two are
// timeline_test.go's, shared so that a reader of one file can read all of them.
const errorMarker = "=== error ===\n"

// resolveConfigPath is the configuration file every case here names.
//
// It is a fixed string and no file exists at it. The report quotes the path in
// four different explanations — no profile is active, no context is mapped — and a
// t.TempDir() would put a different one in the golden on every run. Nothing reads
// it: these cases supply the parsed configuration directly, which is also what
// lets one of them describe a file the machine running the test does not have.
const resolveConfigPath = "/home/engineer/.config/kuberecord/config.yaml"

// twoClusterArchive is a checked-in archive holding two clusters' keys.
//
// It exists because the cluster-identity chain's last step is the only one that
// can fail, and it fails by finding more than one identity to choose from. A
// directory made at test time would name itself in the golden.
const twoClusterArchive = "testdata/two-clusters"

// clickHousePasswordEnv is where the profile fixture's password comes from.
//
// It is the variable docs/CLI.md and Task 13.1's remediation both name, so a
// reader of the golden sees the shape they were told to write.
const clickHousePasswordEnv = "KUBERECORD_CLICKHOUSE_PASSWORD"

// probeEngine is a fake engine that answers the cluster listing, which is the
// question a reachability check puts to a backend.
//
// It exists so that the unreachable case is planted rather than dialled. The real
// failure is a DNS lookup, and a test that performed one would depend on the
// resolver of whatever machine ran it: a sandbox that answers "server misbehaving"
// instead of NXDOMAIN produces no diagnosis at all, and the golden would fail for
// a reason that has nothing to do with the code.
type probeEngine struct {
	*fakeEngine
	ids []string
	err error
}

func (e *probeEngine) ClusterIDs(context.Context) ([]string, error) { return e.ids, e.err }

// unresolvableAddress is the failure a laptop meets against a Service name,
// spelled exactly as the net package spells it.
//
// Constructed rather than provoked, for the reason probeEngine exists. The
// classification under test is structural — a *net.DNSError with IsNotFound —
// so a planted one exercises the same branch a real lookup would.
func unresolvableAddress(host string) error {
	return &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

// resolveCase is one invocation of `config resolve`, as these tests describe it.
type resolveCase struct {
	// config is the configuration file's parsed contents, or nil for an empty
	// one.
	config *resolve.Config

	// sinks are the sink custom resources the fixture cluster holds.
	sinks []*unstructured.Unstructured

	// objects are the core resources it holds: the credentials Secret, the
	// operator's Deployment.
	objects []runtime.Object

	// args are the flags this invocation was given, beyond --kubeconfig.
	args []string

	// check is --check.
	check bool

	// swap replaces the opened engine before the report is written, which is how
	// a probe failure is planted. Nil leaves the real one in place.
	swap func(*resolve.Inspection)

	// format is the serialization; empty means the human-readable form.
	format render.StructuredFormat
}

// inspect walks both chains against this case's fixture cluster.
//
// It is separated from run because `version --check` needs exactly this half: the
// two commands put one question through one piece of machinery (Task 14.2), and a
// second fixture resolver written beside the version tests would be a second
// opinion about what the chains decide.
func (c resolveCase) inspect(t *testing.T, streams genericiooptions.IOStreams) *resolve.Inspection {
	t.Helper()

	args := append([]string{"--kubeconfig", kubeconfigPath}, c.args...)
	root, flags := cli.NewRootCommand(options.StandaloneName, streams)
	if parseErr := root.ParseFlags(args); parseErr != nil {
		t.Fatalf("parsing %v: %v", args, parseErr)
	}

	config := c.config
	if config == nil {
		config = &resolve.Config{}
	}
	resolver := &resolve.BackendResolver{
		Flags: flags, Streams: streams, InvokedAs: options.StandaloneName,
		Config: config, ConfigPath: resolveConfigPath,
		Clients: fakeClients(c.sinks, c.objects...),
	}

	inspection := resolver.Inspect(t.Context(), c.check)
	if inspection.Backend != nil {
		t.Cleanup(func() { closeBackend(t, inspection.Backend) })
	}
	if c.swap != nil {
		c.swap(inspection)
	}
	return inspection
}

// run drives one case through the whole command layer and returns both streams.
//
// It goes through resolve.Inspect and cli.RunResolve rather than through the
// cobra command for the reason every rendering test in this package does: the
// command's own job is flag parsing, and what these cases are about is what the
// report says.
func (c resolveCase) run(t *testing.T) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	streams := ioStreams(&out, &errOut)

	err = cli.RunResolve(t.Context(), c.inspect(t, streams),
		cli.ResolveRequest{Check: c.check, Structured: c.format}, streams)
	return out.String(), errOut.String(), err
}

// assertResolveGolden compares an invocation's whole visible result against the
// checked-in file.
func assertResolveGolden(t *testing.T, name, stdout, stderr string, err error) {
	t.Helper()
	assertReportGolden(t, "config-resolve", name, "kuberecord config resolve", stdout, stderr, err)
}

// assertReportGolden is the comparison both of the reporting commands use.
//
// dir names the subdirectory of testdata/ the golden lives in and commandPath is
// what cobra would have called the command that failed. Those two are the whole
// of how the two callers differ: the three sections, the update flag and the
// top-level diagnostic are shared, because the property they assert — that a
// failure reaches the reader through the same path whichever command met it — is
// the same property.
func assertReportGolden(t *testing.T, dir, name, commandPath, stdout, stderr string, err error) {
	t.Helper()

	path := filepath.Join("testdata", dir, name+".golden")
	got := stdoutMarker + stdout + stderrMarker + stderr +
		errorMarker + topLevelDiagnostic(commandPath, err)

	if *updateGolden {
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
			t.Fatalf("creating the golden directory: %v", mkErr)
		}
		if writeErr := os.WriteFile(path, []byte(got), 0o600); writeErr != nil {
			t.Fatalf("writing %s: %v", path, writeErr)
		}
		return
	}

	want, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/ -update` to create it): %v", path, readErr)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// topLevelDiagnostic is what cli.RunContext writes to stderr for a failure one of
// these commands returned.
//
// It is reproduced here rather than driven through RunContext because these cases
// hold a fixture cluster that only a directly-constructed resolver can be given.
// What it must stay in step with is the composition in RunContext: the `error:`
// line, and then the unreachable-backend block for a failure that carries one.
// The block is the whole reason this section exists — an unreachable sink
// resolved by `config resolve` has to produce the same explanation it produces
// under `timeline`, and this is where a golden can see that it does.
func topLevelDiagnostic(commandPath string, err error) string {
	if err == nil {
		return ""
	}
	out := fmt.Sprintf("error: %v\n", err)

	var unreachable *resolve.UnreachableSinkError
	if errors.As(err, &unreachable) {
		out += "\n" + unreachable.Render(commandPath, false)
	}
	return out
}

// clickHouseProfileConfig is a configuration file with one ClickHouse profile
// active and one kubeconfig context mapped.
//
// The address is a literal rather than anything discovered, so the case says
// nothing about the fixture cluster: the point of the profile step is that it
// answers before discovery is consulted at all.
func clickHouseProfileConfig() *resolve.Config {
	return &resolve.Config{
		CurrentProfile: "prod",
		Profiles: map[string]resolve.Profile{
			"prod": {
				Backend: resolve.BackendClickHouse,
				ClickHouse: &resolve.ClickHouseProfile{
					Addr: "clickhouse.example:9000", Database: "kuberecord",
					Username: "kuberecord_ro", PasswordEnv: clickHousePasswordEnv,
				},
			},
		},
		Contexts: map[string]string{"kuberecord-test": theCluster},
	}
}

// discoverableCluster is the fixture every discovery case resolves against: one
// ClickHouseSink recording the Service name an in-cluster operator dials, and the
// Secret it points at.
func discoverableCluster() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
	}
}

// TestResolveReportsWhichStepAnswered walks the cases the acceptance criteria
// name, one golden each.
//
// They are one table rather than six functions because the report's value is
// comparative: a reader puts two of these files side by side to see what changed
// between two setups, and the same is true of a reviewer reading the diff.
func TestResolveReportsWhichStepAnswered(t *testing.T) {
	// The SDK's own variables leak in from a developer's shell otherwise, and the
	// bucket case would name a different region on their machine than in CI.
	t.Setenv("AWS_REGION", "eu-west-1")
	// The profile case names an environment variable as where its password comes
	// from, which is the posture `config set-profile` writes and docs/CLI.md
	// recommends. Resolution reads it, so it has to be there — and the
	// credential test below asserts that having read it changes no output.
	t.Setenv(clickHousePasswordEnv, theSecret)

	tests := []struct {
		name    string
		golden  string
		invoke  resolveCase
		wantErr int
	}{
		{
			name:   "discovery answers, and so does the operator's Deployment",
			golden: "discovered",
			invoke: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
			},
		},
		{
			name:   "an active profile answers before discovery is consulted",
			golden: "profile",
			invoke: resolveCase{
				config:  clickHouseProfileConfig(),
				sinks:   discoverableCluster(),
				objects: []runtime.Object{credentialsSecret(operatorNamespace)},
			},
		},
		{
			name:   "--source answers before anything reaches the cluster",
			golden: "source",
			invoke: resolveCase{
				config: clickHouseProfileConfig(),
				args:   []string{"--source", "s3://acme-audit/kuberecord", "--cluster-id", theCluster},
			},
		},
		{
			name:   "nothing named a cluster and the last step was not taken",
			golden: "undetermined",
			invoke: resolveCase{
				sinks:   discoverableCluster(),
				objects: []runtime.Object{credentialsSecret(operatorNamespace)},
			},
		},
		{
			name:   "the backend chain fails, and the identity chain is reported anyway",
			golden: "backend-unresolved",
			invoke: resolveCase{
				objects: []runtime.Object{operatorDeployment(operatorNamespace, theCluster)},
			},
			wantErr: exit.RuntimeError,
		},
		{
			name:   "the identity chain fails on an archive holding two clusters",
			golden: "cluster-id-unresolved",
			invoke: resolveCase{
				args:  []string{"--source", twoClusterArchive},
				check: true,
			},
			wantErr: exit.RuntimeError,
		},
		{
			name:   "--check against an address that resolves only inside the cluster",
			golden: "unreachable",
			invoke: resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				check: true,
				swap:  unreachableBackend,
			},
			wantErr: exit.RuntimeError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := tc.invoke.run(t)
			if got := exit.CodeFor(err); got != tc.wantErr {
				t.Errorf("exit code %d, want %d (error: %v)", got, tc.wantErr, err)
			}
			assertResolveGolden(t, tc.golden, stdout, stderr, err)
		})
	}
}

// unreachableBackend plants the failure a Service name produces from a laptop.
//
// The engine is replaced rather than the address, so everything the report says
// about the backend — the sink it was discovered from, the address it recorded,
// the diagnosis carried with it — is the real chain's work. Only the round trip
// is fake.
func unreachableBackend(inspection *resolve.Inspection) {
	inspection.Backend.Engine = &probeEngine{
		fakeEngine: &fakeEngine{caps: clickHouseCapabilities()},
		err:        unresolvableAddress("clickhouse.kuberecord-system.svc"),
	}
}

// TestResolveDialsNothingWithoutCheck is the property the command exists for
// (D26).
//
// The configuration most worth inspecting is the one whose backend cannot be
// reached, so an invocation without --check must not put a single question to it.
// The assertion is over the engine rather than over the output: an engine that
// was asked anything at all would have recorded the call, and a report that
// happened to look right while having dialled would still stall for a dial
// timeout in front of the user this command is for.
func TestResolveDialsNothingWithoutCheck(t *testing.T) {
	probe := &probeEngine{
		fakeEngine: &fakeEngine{caps: clickHouseCapabilities()},
		err:        errors.New("the backend was asked a question by a command that promised not to"),
	}

	invoke := resolveCase{
		sinks:   discoverableCluster(),
		objects: []runtime.Object{credentialsSecret(operatorNamespace)},
		swap:    func(in *resolve.Inspection) { in.Backend.Engine = probe },
	}
	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("RunResolve: %v", err)
	}

	if !strings.Contains(stdout, "withheld") {
		t.Errorf("the report does not say the last step was withheld:\n%s", stdout)
	}
	if !strings.Contains(stdout, "undetermined") {
		t.Errorf("an identity nobody named is reported as something other than undetermined:\n%s", stdout)
	}
}

// TestResolveWithCheckReportsAReachableBackend is the other half of --check.
func TestResolveWithCheckReportsAReachableBackend(t *testing.T) {
	invoke := resolveCase{
		sinks: discoverableCluster(),
		objects: []runtime.Object{
			credentialsSecret(operatorNamespace),
			operatorDeployment(operatorNamespace, theCluster),
		},
		check: true,
		swap: func(in *resolve.Inspection) {
			in.Backend.Engine = &probeEngine{
				fakeEngine: &fakeEngine{caps: clickHouseCapabilities()},
				ids:        []string{theCluster},
			}
		},
	}

	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("RunResolve: %v", err)
	}
	if !strings.Contains(stdout, "reachable") {
		t.Errorf("a backend that answered is not reported as reachable:\n%s", stdout)
	}
}

// TestResolveStructuredFieldNames pins the contract by name.
//
// A golden file regenerated after a rename would keep passing; this will not.
// D19 makes these field names a public contract, and the whole point of the
// structured form is that somebody's support runbook says "paste the output of
// `config resolve -o json`" and something downstream reads it.
func TestResolveStructuredFieldNames(t *testing.T) {
	invoke := resolveCase{
		sinks: discoverableCluster(),
		objects: []runtime.Object{
			credentialsSecret(operatorNamespace),
			operatorDeployment(operatorNamespace, theCluster),
		},
		format: render.StructuredJSON,
	}

	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("RunResolve: %v", err)
	}

	var document map[string]any
	if unmarshalErr := json.Unmarshal([]byte(stdout), &document); unmarshalErr != nil {
		t.Fatalf("the document is not JSON: %v\n%s", unmarshalErr, stdout)
	}

	if document["apiVersion"] != "cli.kuberecord.io/v1alpha1" {
		t.Errorf("apiVersion = %v, want the CLI contract's", document["apiVersion"])
	}
	if document["kind"] != cli.ResolutionKind {
		t.Errorf("kind = %v, want %q", document["kind"], cli.ResolutionKind)
	}

	backend, ok := document["backend"].(map[string]any)
	if !ok {
		t.Fatalf("the document has no backend object:\n%s", stdout)
	}
	for _, field := range []string{"resolved", "origin", "description", "capabilities", "steps"} {
		if _, present := backend[field]; !present {
			t.Errorf("backend has no %q field:\n%s", field, stdout)
		}
	}

	// The four capability names are the read plane's own, and they are the names
	// docs/CLI.md's capability table uses. A reader comparing the two must not
	// have to translate.
	capabilities, ok := backend["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("the backend declares no capabilities:\n%s", stdout)
	}
	for _, field := range []string{
		"backend", "deletions", "server_side_filter", "point_query", "time_bound_required",
	} {
		if _, present := capabilities[field]; !present {
			t.Errorf("capabilities has no %q field:\n%s", field, stdout)
		}
	}

	identity, ok := document["clusterID"].(map[string]any)
	if !ok {
		t.Fatalf("the document has no clusterID object:\n%s", stdout)
	}
	for _, field := range []string{"resolved", "value", "source", "steps"} {
		if _, present := identity[field]; !present {
			t.Errorf("clusterID has no %q field:\n%s", field, stdout)
		}
	}

	// Present even when nothing was checked, so that a consumer reads a value
	// rather than inferring one from a missing key.
	check, ok := document["check"].(map[string]any)
	if !ok {
		t.Fatalf("the document has no check object:\n%s", stdout)
	}
	if check["requested"] != false || check["outcome"] != "not checked" {
		t.Errorf("an unchecked invocation reports %v, want a requested:false / not checked pair", check)
	}
}

// TestResolveYAMLIsTheSameDocument.
//
// The two serializations must be one document in two syntaxes rather than two
// documents that resemble each other, which is the agreement every other
// structured output in this CLI keeps.
func TestResolveYAMLIsTheSameDocument(t *testing.T) {
	fixture := func(format render.StructuredFormat) resolveCase {
		return resolveCase{
			sinks: discoverableCluster(),
			objects: []runtime.Object{
				credentialsSecret(operatorNamespace),
				operatorDeployment(operatorNamespace, theCluster),
			},
			format: format,
		}
	}

	asJSON, _, err := fixture(render.StructuredJSON).run(t)
	if err != nil {
		t.Fatalf("RunResolve -o json: %v", err)
	}
	asYAML, _, err := fixture(render.StructuredYAML).run(t)
	if err != nil {
		t.Fatalf("RunResolve -o yaml: %v", err)
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

// TestResolveNeverPrintsACredential, at any verbosity and in any format.
//
// The report is pasted into support requests — that is what the structured form
// is for — so this asserts over the whole invocation rather than over the one
// line written with it in mind. The fixture's Secret holds a distinctive value so
// that a substring search is asserting something it could actually find.
func TestResolveNeverPrintsACredential(t *testing.T) {
	t.Setenv(clickHousePasswordEnv, theSecret)

	for _, format := range []render.StructuredFormat{"", render.StructuredJSON, render.StructuredYAML} {
		t.Run("format="+string(format), func(t *testing.T) {
			invoke := resolveCase{
				sinks: discoverableCluster(),
				objects: []runtime.Object{
					credentialsSecret(operatorNamespace),
					operatorDeployment(operatorNamespace, theCluster),
				},
				args:   []string{"-v", "10"},
				format: format,
			}

			stdout, stderr, err := invoke.run(t)
			if err != nil {
				t.Fatalf("RunResolve: %v", err)
			}
			for name, written := range map[string]string{"stdout": stdout, "stderr": stderr} {
				if strings.Contains(written, theSecret) {
					t.Errorf("%s carries the password read from the Secret:\n%s", name, written)
				}
			}
		})
	}
}

// TestResolveIsAConfigSubcommand drives the whole binary, which is where the
// flags, the exit code and the argument checking actually live.
func TestResolveIsAConfigSubcommand(t *testing.T) {
	configHome(t)

	tests := []struct {
		name string
		args []string
		want []string
		code int
		// report says whether this invocation gets as far as producing one. A
		// refusal about the output format cannot render anything; a refusal about
		// the chain is the report's own subject, and printing which step was
		// malformed is the command doing its job.
		report bool
	}{
		{
			name: "a positional argument is a usage error",
			args: []string{"config", "resolve", "deploy/checkout"},
			want: []string{"takes no arguments", "deploy/checkout"},
			code: exit.UsageError,
		},
		{
			name: "jsonl is refused by name rather than rendered as something else",
			args: []string{"config", "resolve", "-o", "jsonl"},
			want: []string{"config resolve renders", "jsonl"},
			code: exit.UsageError,
		},
		{
			name: "so is diff",
			args: []string{"config", "resolve", "-o", "diff"},
			want: []string{"config resolve renders", "diff"},
			code: exit.UsageError,
		},
		{
			name:   "a malformed --sink is the same usage error a query command gives",
			args:   []string{"config", "resolve", "--sink", "default"},
			want:   []string{"expected <kind>/<name>"},
			code:   exit.UsageError,
			report: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, code := run(t, append([]string{"--kubeconfig", kubeconfigPath}, tc.args...)...)
			if code != tc.code {
				t.Errorf("exit code %d, want %d: %s", code, tc.code, stderr)
			}
			for _, want := range tc.want {
				if !strings.Contains(stderr, want) {
					t.Errorf("stderr does not mention %q:\n%s", want, stderr)
				}
			}
			switch {
			case !tc.report && stdout != "":
				t.Errorf("a refused invocation wrote to stdout, which belongs to data: %q", stdout)
			case tc.report && !strings.Contains(stdout, "failed"):
				t.Errorf("the report does not say which step failed:\n%s", stdout)
			}
		})
	}
}

// TestResolveHelpNamesTheFlag keeps --check discoverable where a user looks for
// it, which is the same place the Definition of Done requires every flag to
// appear.
func TestResolveHelpNamesTheFlag(t *testing.T) {
	stdout, _, code := run(t, "config", "resolve", "--help")
	if code != exit.Success {
		t.Fatalf("config resolve --help exited %d", code)
	}
	if !strings.Contains(stdout, "--"+options.FlagCheck) {
		t.Errorf("--%s is absent from the command's help:\n%s", options.FlagCheck, stdout)
	}
}

// TestResolveDocumentsEveryStepOutcome.
//
// The five outcomes are the vocabulary the report is read in, and the help text
// is where a reader learns what they mean. A sixth added without a line in the
// help would be a word nobody could look up.
func TestResolveDocumentsEveryStepOutcome(t *testing.T) {
	stdout, _, code := run(t, "config", "resolve", "--help")
	if code != exit.Success {
		t.Fatalf("config resolve --help exited %d", code)
	}
	for _, outcome := range []resolve.StepOutcome{
		resolve.StepAnswered, resolve.StepSilent, resolve.StepFailed,
		resolve.StepNotReached, resolve.StepWithheld,
	} {
		if !strings.Contains(stdout, string(outcome)) {
			t.Errorf("the help does not explain the %q outcome:\n%s", outcome, stdout)
		}
	}
}

// TestResolveCheckDegradesOnAnEngineThatCannotBeProbed.
//
// A backend that cannot answer the cluster listing has said something true about
// itself, and reporting that as unreachable would be inventing a fault. It
// degrades with a statement and still exits zero (Invariant 5).
func TestResolveCheckDegradesOnAnEngineThatCannotBeProbed(t *testing.T) {
	invoke := resolveCase{
		sinks: discoverableCluster(),
		objects: []runtime.Object{
			credentialsSecret(operatorNamespace),
			operatorDeployment(operatorNamespace, theCluster),
		},
		check: true,
		swap: func(in *resolve.Inspection) {
			// A bare fakeEngine is not a query.ClusterIDLister, which is exactly
			// the shape this path exists for.
			in.Backend.Engine = &fakeEngine{caps: clickHouseCapabilities()}
		},
	}

	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("a capability gap failed the command: %v", err)
	}
	if !strings.Contains(stdout, "cannot be checked") {
		t.Errorf("the report does not say the backend could not be probed:\n%s", stdout)
	}
	if strings.Contains(stdout, "unreachable") {
		t.Errorf("a backend that could not be probed is reported as unreachable:\n%s", stdout)
	}
}

// TestResolveReportsTheDeclaredCapabilities.
//
// They are what make one backend answer a question differently from another
// (D17), and a user comparing two setups reads them side by side. The assertion
// is that the report states the engine's own declaration rather than a constant
// somebody wrote next to it.
func TestResolveReportsTheDeclaredCapabilities(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")

	invoke := resolveCase{
		args:   []string{"--source", "s3://acme-audit/kuberecord", "--cluster-id", theCluster},
		format: render.StructuredJSON,
	}
	stdout, _, err := invoke.run(t)
	if err != nil {
		t.Fatalf("RunResolve: %v", err)
	}

	var document struct {
		Backend struct {
			Capabilities query.Capabilities `json:"capabilities"`
		} `json:"backend"`
	}
	if unmarshalErr := json.Unmarshal([]byte(stdout), &document); unmarshalErr != nil {
		t.Fatalf("the document is not JSON: %v", unmarshalErr)
	}

	want := archiveCapabilities()
	if document.Backend.Capabilities != want {
		t.Errorf("the report declares %+v, and the archive engine declares %+v",
			document.Backend.Capabilities, want)
	}
}
