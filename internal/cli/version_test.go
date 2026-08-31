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
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/buildinfo"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
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
