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
	"os"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// profileProd is the profile name these cases write and read back, named once so
// that an assertion reading a *different* profile is a compile error rather than a
// passing test about the wrong stanza.
const profileProd = "prod"

// run drives the whole binary the way a shell does, and returns what each stream
// received along with the exit code.
//
// Through cli.Run rather than through a command object, because the exit code is
// half of what these subcommands promise and the only honest test of it is the path
// that produces it.
func run(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()

	io, out, errOut := streams()
	code = cli.Run(append([]string{options.StandaloneName}, args...), io)
	return out.String(), errOut.String(), code
}

// TestConfigSetProfileWritesWhatItWasTold.
func TestConfigSetProfileWritesWhatItWasTold(t *testing.T) {
	path := configHome(t)

	stdout, stderr, code := run(t, "config", "set-profile", profileProd,
		"--backend", "clickhouse",
		"--addr", "clickhouse.example:9000",
		"--database", "kuberecord",
		"--username", "kuberecord_ro",
		"--password-env", "KUBERECORD_CLICKHOUSE_PASSWORD")
	if code != exit.Success {
		t.Fatalf("config set-profile exited %d: %s", code, stderr)
	}
	if stdout != "" {
		t.Errorf("config set-profile wrote to stdout, which belongs to data: %q", stdout)
	}
	if !strings.Contains(stderr, `wrote profile "prod"`) {
		t.Errorf("stderr does not confirm the write: %s", stderr)
	}
	// The first profile in an empty file becomes the active one, and says so.
	if !strings.Contains(stderr, `"prod" is now the active profile`) {
		t.Errorf("stderr does not say the profile was activated: %s", stderr)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig of what the command wrote: %v", err)
	}
	profile, ok := cfg.Profiles[profileProd]
	if !ok {
		t.Fatalf("the file holds no profile named prod: %+v", cfg)
	}
	if profile.Backend != resolve.BackendClickHouse || profile.ClickHouse == nil {
		t.Fatalf("the profile is not a ClickHouse one: %+v", profile)
	}
	if profile.ClickHouse.Addr != "clickhouse.example:9000" ||
		profile.ClickHouse.Username != "kuberecord_ro" ||
		profile.ClickHouse.PasswordEnv != "KUBERECORD_CLICKHOUSE_PASSWORD" {
		t.Errorf("the profile does not carry what was typed: %+v", profile.ClickHouse)
	}
	if cfg.CurrentProfile != profileProd {
		t.Errorf("currentProfile = %q, want the only profile in the file", cfg.CurrentProfile)
	}

	// A second profile does not steal the active one: that is a decision, and
	// `use-profile` is where it is made.
	if _, stderr, code = run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit", "--prefix", "kuberecord"); code != exit.Success {
		t.Fatalf("config set-profile for the second profile exited %d: %s", code, stderr)
	}
	if strings.Contains(stderr, "is now the active profile") {
		t.Errorf("adding a second profile silently changed the active one: %s", stderr)
	}
	cfg, err = resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != profileProd {
		t.Errorf("currentProfile = %q, want it unchanged at prod", cfg.CurrentProfile)
	}
}

// TestConfigSetProfileRefusesAProfileThatCannotWork.
//
// Refused before the file is opened, so a mistyped command cannot rewrite a
// configuration only to be rejected on the way back in.
func TestConfigSetProfileRefusesAProfileThatCannotWork(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no backend",
			args: []string{"config", "set-profile", "prod", "--addr", "clickhouse:9000"},
			want: "clickhouse, s3, local",
		},
		{
			name: "a ClickHouse profile with no address",
			args: []string{"config", "set-profile", "prod", "--backend", "clickhouse"},
			want: "clickhouse.addr is required",
		},
		{
			name: "both password references",
			args: []string{"config", "set-profile", "prod", "--backend", "clickhouse",
				"--addr", "clickhouse:9000", "--password-env", "A", "--password-file", "/b"},
			want: "a password comes from one place",
		},
		{
			name: "an S3 profile with no bucket",
			args: []string{"config", "set-profile", "archive", "--backend", "s3"},
			want: "s3.bucket is required",
		},
		{
			name: "a local profile with no path",
			args: []string{"config", "set-profile", "laptop", "--backend", "local"},
			want: "local.path is required",
		},
		{
			name: "no profile name",
			args: []string{"config", "set-profile", "--backend", "local", "--path", "/archives"},
			want: "takes one argument",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := configHome(t)

			_, stderr, code := run(t, tc.args...)
			if code != exit.UsageError {
				t.Errorf("exit code %d, want %d for a malformed invocation", code, exit.UsageError)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr does not say what is wrong (%q):\n%s", tc.want, stderr)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a refused set-profile created %s anyway", path)
			}
		})
	}
}

// TestConfigSetProfileFromSinkRefusesWhatTheSinkAlreadyAnswers.
//
// Every one of these is refused before the cluster is contacted, which is why the
// cases need no cluster: a mistyped command must not cost an API round trip and a
// Secret read to learn that two of its flags disagree. A flag accepted and quietly
// ignored would be the worse outcome — a profile reading somewhere its author did
// not choose (Invariant 4).
func TestConfigSetProfileFromSinkRefusesWhatTheSinkAlreadyAnswers(t *testing.T) {
	const clickHouse = "ClickHouseSink/default"

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "--backend, which the sink's kind decides",
			args: []string{"--from-sink", clickHouse, "--backend", "clickhouse"},
			want: []string{"--from-sink " + clickHouse, "--backend", "decides the backend"},
		},
		{
			name: "a field the custom resource states",
			args: []string{"--from-sink", clickHouse, "--database", "somewhere_else"},
			want: []string{"--database", "states it", "somewhere other than where that sink writes"},
		},
		{
			name: "a flag belonging to another backend",
			args: []string{"--from-sink", clickHouse, "--bucket", "acme-audit"},
			want: []string{"--bucket", "configures the s3 backend", "writes a clickhouse profile"},
		},
		{
			name: "an endpoint override against an object store",
			args: []string{"--from-sink", "S3Sink/archive", "--addr", "127.0.0.1:9000"},
			want: []string{"--addr", "configures the clickhouse backend", "writes a s3 profile"},
		},
		{
			name: "the per-invocation override, which writes nothing",
			args: []string{"--from-sink", clickHouse, "--sink-addr", "127.0.0.1:9000"},
			want: []string{"--sink-addr", "writes a file", "--addr"},
		},
		{
			name: "a value that is not kind/name",
			args: []string{"--from-sink", "default"},
			want: []string{`malformed --from-sink "default"`, "<kind>/<name>"},
		},
		{
			name: "a kind no CRD in this build defines",
			args: []string{"--from-sink", "PostgresSink/default"},
			want: []string{"--from-sink names the kind", "ClickHouseSink, S3Sink"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := configHome(t)

			_, stderr, code := run(t, append([]string{"config", "set-profile", "local"}, tc.args...)...)
			if code != exit.UsageError {
				t.Errorf("exit code %d, want %d for a malformed invocation", code, exit.UsageError)
			}
			for _, want := range tc.want {
				if !strings.Contains(stderr, want) {
					t.Errorf("stderr does not say %q:\n%s", want, stderr)
				}
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a refused set-profile created %s anyway", path)
			}
		})
	}
}

// TestConfigSetProfileFromSinkAcceptsWhatTheSinkCannotState is the other half:
// the four flags that must survive --from-sink, because a ClickHouseSink states
// none of them for a reader.
//
// It asserts the absence of a conflict rather than a successful write, since there
// is no cluster here to read a sink out of. What comes back is whatever the
// kubeconfig fixture's unreachable API server says, and that is not this test's
// subject.
func TestConfigSetProfileFromSinkAcceptsWhatTheSinkCannotState(t *testing.T) {
	configHome(t)

	_, stderr, _ := run(t, "--kubeconfig", kubeconfigPath,
		"config", "set-profile", "local", "--from-sink", "ClickHouseSink/default",
		"--addr", "127.0.0.1:9000", "--username", "kuberecord_ro",
		"--password-env", "KUBERECORD_CLICKHOUSE_PASSWORD", "--tls")

	if strings.Contains(stderr, "cannot be given together") {
		t.Errorf("a flag the custom resource cannot state was refused:\n%s", stderr)
	}
}

// TestConfigUseProfileSwitchesAndRefusesTheUnknown.
func TestConfigUseProfileSwitchesAndRefusesTheUnknown(t *testing.T) {
	path := configHome(t)
	writeConfigFile(t, path, `currentProfile: prod
profiles:
  prod:
    backend: local
    local:
      path: /archives/prod
  archive:
    backend: local
    local:
      path: /archives/cold
`)

	if _, stderr, code := run(t, "config", "use-profile", "archive"); code != exit.Success {
		t.Fatalf("config use-profile exited %d: %s", code, stderr)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != "archive" {
		t.Errorf("currentProfile = %q, want archive", cfg.CurrentProfile)
	}

	_, stderr, code := run(t, "config", "use-profile", "staging")
	if code != exit.UsageError {
		t.Errorf("exit code %d, want %d for a profile that does not exist", code, exit.UsageError)
	}
	for _, want := range []string{"staging", "archive, prod"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q, so it does not say what could be chosen:\n%s",
				want, stderr)
		}
	}
}

// TestConfigSetContextClusterIDTakesBothForms.
//
// One argument is the common case — the context you are already pointed at — and
// two is what an engineer writing several mappings in a row wants. The failure with
// no current context names the two-argument form rather than describing it,
// because the reader is one copy-paste from being finished.
func TestConfigSetContextClusterIDTakesBothForms(t *testing.T) {
	t.Run("one argument uses the current kubeconfig context", func(t *testing.T) {
		path := configHome(t)

		_, stderr, code := run(t, "--kubeconfig", kubeconfigPath,
			"config", "set-context-cluster-id", "prod-eu-1")
		if code != exit.Success {
			t.Fatalf("config set-context-cluster-id exited %d: %s", code, stderr)
		}
		cfg, err := resolve.LoadConfig(path)
		if err != nil {
			t.Fatalf("resolve.LoadConfig: %v", err)
		}
		if got := cfg.Contexts[kubeconfigContext]; got != "prod-eu-1" {
			t.Errorf("contexts[%q] = %q, want prod-eu-1 (mappings: %v)",
				kubeconfigContext, got, cfg.Contexts)
		}
	})

	t.Run("--context selects which context is mapped", func(t *testing.T) {
		path := configHome(t)

		_, stderr, code := run(t, "--kubeconfig", kubeconfigPath, "--context", "kuberecord-test-no-namespace",
			"config", "set-context-cluster-id", "prod-us-1")
		if code != exit.Success {
			t.Fatalf("config set-context-cluster-id exited %d: %s", code, stderr)
		}
		cfg, err := resolve.LoadConfig(path)
		if err != nil {
			t.Fatalf("resolve.LoadConfig: %v", err)
		}
		if got := cfg.Contexts["kuberecord-test-no-namespace"]; got != "prod-us-1" {
			t.Errorf("the mapping was written under the wrong context: %v", cfg.Contexts)
		}
	})

	t.Run("two arguments name the context explicitly", func(t *testing.T) {
		path := configHome(t)

		_, stderr, code := run(t, "config", "set-context-cluster-id", "prod-eu", "prod-eu-1")
		if code != exit.Success {
			t.Fatalf("config set-context-cluster-id exited %d: %s", code, stderr)
		}
		cfg, err := resolve.LoadConfig(path)
		if err != nil {
			t.Fatalf("resolve.LoadConfig: %v", err)
		}
		if got := cfg.Contexts["prod-eu"]; got != "prod-eu-1" {
			t.Errorf("contexts[prod-eu] = %q, want prod-eu-1", got)
		}
		if !strings.Contains(stderr, `context "prod-eu" reads cluster "prod-eu-1"`) {
			t.Errorf("stderr does not confirm the mapping: %s", stderr)
		}
	})

	t.Run("no current context names the two-argument form", func(t *testing.T) {
		configHome(t)
		// A kubeconfig that exists and selects nothing: the state of a machine that
		// has never run kubectl against a cluster.
		empty := t.TempDir() + "/kubeconfig"
		if err := os.WriteFile(empty, []byte("apiVersion: v1\nkind: resolve.Config\n"), 0o600); err != nil {
			t.Fatalf("writing an empty kubeconfig: %v", err)
		}

		_, stderr, code := run(t, "--kubeconfig", empty, "config", "set-context-cluster-id", "prod-eu-1")
		if code != exit.UsageError {
			t.Errorf("exit code %d, want %d", code, exit.UsageError)
		}
		if !strings.Contains(stderr, "set-context-cluster-id <context> prod-eu-1") {
			t.Errorf("stderr does not show the form that would work:\n%s", stderr)
		}
	})
}

// TestConfigViewPutsTheDocumentOnStdoutAndThePathOnStderr.
//
// The split is what makes `config view -o json | jq` work, and it is the same
// discipline every other command in this CLI follows: data to stdout, everything
// about the data to stderr.
func TestConfigViewPutsTheDocumentOnStdoutAndThePathOnStderr(t *testing.T) {
	path := configHome(t)
	writeConfigFile(t, path, `currentProfile: prod
profiles:
  prod:
    backend: local
    local:
      path: /archives/prod
contexts:
  prod-eu: prod-eu-1
`)

	stdout, stderr, code := run(t, "config", "view")
	if code != exit.Success {
		t.Fatalf("config view exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "currentProfile: prod") {
		t.Errorf("stdout does not carry the document:\n%s", stdout)
	}
	if strings.Contains(stdout, path) {
		t.Errorf("the file path reached stdout, where a document is being piped:\n%s", stdout)
	}
	if !strings.Contains(stderr, path) {
		t.Errorf("stderr does not say which file was read:\n%s", stderr)
	}
	if !strings.Contains(stdout, "apiVersion: "+resolve.ConfigAPIVersion) {
		t.Errorf("the rendered document does not carry its apiVersion:\n%s", stdout)
	}

	t.Run("-o json is valid JSON", func(t *testing.T) {
		stdout, stderr, code := run(t, "config", "view", "-o", "json")
		if code != exit.Success {
			t.Fatalf("config view -o json exited %d: %s", code, stderr)
		}
		var decoded resolve.Config
		if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
			t.Fatalf("config view -o json did not produce JSON: %v\n%s", err, stdout)
		}
		if decoded.CurrentProfile != "prod" {
			t.Errorf("the JSON document does not carry the configuration: %+v", decoded)
		}
	})

	t.Run("a format that means nothing here is refused by name", func(t *testing.T) {
		_, stderr, code := run(t, "config", "view", "-o", "jsonl")
		if code != exit.UsageError {
			t.Errorf("exit code %d, want %d", code, exit.UsageError)
		}
		if !strings.Contains(stderr, "renders yaml or json") {
			t.Errorf("stderr does not name the formats that do work:\n%s", stderr)
		}
	})

	t.Run("a missing file views as an empty configuration", func(t *testing.T) {
		configHome(t)

		stdout, stderr, code := run(t, "config", "view")
		if code != exit.Success {
			t.Fatalf("config view of a missing file exited %d: %s", code, stderr)
		}
		if !strings.Contains(stdout, "kind: "+resolve.ConfigKind) {
			t.Errorf("stdout does not carry an empty document:\n%s", stdout)
		}
	})
}

// TestAMisspelledConfigSubcommandIsAUsageError.
//
// Cobra's own unknown-command error is a plain one, which exit.CodeFor reads as a
// runtime failure — the code a wrapper script is told to retry. A typo must not be
// retried, so the tree classifies it.
func TestAMisspelledConfigSubcommandIsAUsageError(t *testing.T) {
	configHome(t)

	_, stderr, code := run(t, "config", "set-profil", "prod")
	if code != exit.UsageError {
		t.Errorf("exit code %d, want %d", code, exit.UsageError)
	}
	if !strings.Contains(stderr, "set-profile") {
		t.Errorf("stderr does not suggest the command that was meant:\n%s", stderr)
	}
}
