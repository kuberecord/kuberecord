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
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// `config current-profile`: one token, or a failure.
//
// The command's whole contract is a shape rather than a wording, so most of what
// is asserted here is what is *not* on stdout. A header, a path, a trailing
// space or a second line would each be invisible to a reader and fatal to
// `PROFILE=$(…)`, which is the invocation the command exists for — so the bare
// form is compared byte for byte rather than with strings.Contains.
//
// The two failures are the other half. Nothing on stdout when there is no name,
// and exit 1 rather than 0, because a script capturing "" and carrying on is the
// outcome the code prevents.

// TestCurrentProfilePrintsOneToken is the acceptance criterion, asserted as an
// equality rather than a containment.
//
// stdout is exactly the name and a newline. The newline is part of it: a token
// without one leaves a shell prompt on the same line as the answer, and `$( )`
// strips it anyway.
func TestCurrentProfilePrintsOneToken(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	stdout, stderr, code := run(t, "config", "current-profile")
	if code != exit.Success {
		t.Fatalf("config current-profile exited %d: %s", code, stderr)
	}
	if stdout != profileLocal+"\n" {
		t.Errorf("stdout = %q, want exactly %q: a script captures this and a header, a path or a "+
			"second line would each break it", stdout, profileLocal+"\n")
	}
	// Nothing on stderr either, which is where this command departs from
	// `config view` and `config get-profiles`. Both print the file's path there;
	// this one is a token somebody captures, and `kubectl config current-context`
	// prints nothing beside it.
	if stderr != "" {
		t.Errorf("stderr = %q, want nothing: the path is in -o json for a program that needs it",
			stderr)
	}
	assertProfileGolden(t, "current-profile", path, stdout, stderr, code)
}

// TestCurrentProfileFailsWithNoActiveProfileAndNamesTheRoutes is the exit-1 half.
//
// The state is reached the way a user reaches it — `delete-profile --force`
// clears the active pointer and leaves the other stanzas — rather than by a
// hand-written file, so a change to what `--force` leaves behind fails here as
// well as in its own test.
func TestCurrentProfileFailsWithNoActiveProfileAndNamesTheRoutes(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)
	for _, name := range []string{"archive", "prod"} {
		if _, stderr, code := run(t, "config", "set-profile", name,
			"--backend", "s3", "--bucket", "acme-audit"); code != exit.Success {
			t.Fatalf("writing profile %q exited %d: %s", name, code, stderr)
		}
	}
	if _, stderr, code := run(t, "config", "delete-profile", profileLocal,
		"--"+options.FlagForce); code != exit.Success {
		t.Fatalf("clearing the active pointer exited %d: %s", code, stderr)
	}

	stdout, stderr, code := run(t, "config", "current-profile")
	if code != exit.RuntimeError {
		t.Fatalf("config current-profile with no active profile exited %d, want %d: an empty "+
			"success is the outcome this code prevents", code, exit.RuntimeError)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing: there is no name, and a partial token is worse than "+
			"none", stdout)
	}
	assertProfileGolden(t, "current-profile-none-active", path, stdout, stderr, code)

	// Both routes the acceptance criterion names, and the file the pointer is
	// missing from.
	for _, want := range []string{"config use-profile archive", "config get-profiles", path} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the failure does not name %q:\n%s", want, stderr)
		}
	}
	// The alternatives beyond the one it suggested, as errDeletingTheActiveProfile
	// lists them: a reader choosing between profiles needs the names.
	if !strings.Contains(stderr, "also defined: prod") {
		t.Errorf("the failure does not name the other profiles in the file:\n%s", stderr)
	}
}

// TestCurrentProfileFailsWithAnEmptyFileAndNamesTheWriteRoute is the second
// branch, and the reason there are two (D34).
//
// `use-profile` is not a route out of a file with no profiles in it, and offering
// it would be a remedy naming nothing — the call errDeletingTheActiveProfile and
// switchRoute both make for their only-profile cases. What this reader needs is
// `set-profile`, and the sentence saying an empty file is not a broken one:
// nothing else in the CLI would tell them that a query needs no profile at all.
func TestCurrentProfileFailsWithAnEmptyFileAndNamesTheWriteRoute(t *testing.T) {
	path := configHome(t)

	stdout, stderr, code := run(t, "config", "current-profile")
	if code != exit.RuntimeError {
		t.Fatalf("config current-profile against an empty file exited %d, want %d", code, exit.RuntimeError)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing", stdout)
	}
	assertProfileGolden(t, "current-profile-empty-file", path, stdout, stderr, code)

	for _, want := range []string{"config set-profile", "config get-profiles",
		"falls through to discovering a sink from the cluster"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the failure does not name %q:\n%s", want, stderr)
		}
	}
	// The route that does not exist here. A file with no profiles has nothing to
	// switch to, and a suggested `use-profile` would read as though the reader
	// should have known which name to substitute.
	if strings.Contains(stderr, "use-profile") {
		t.Errorf("the failure offers `use-profile` against a file that defines no profiles:\n%s",
			stderr)
	}
}

// TestCurrentProfileRendersTheVersionedDocument is the --output half.
//
// The apiVersion and the kind are compared against literals rather than against
// the package's constants, exactly as profilelifecycle_test.go compares them:
// this is a public contract asserted from outside the package, and a test reading
// the constant would pass after somebody changed it.
func TestCurrentProfileRendersTheVersionedDocument(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	t.Run("json", func(t *testing.T) {
		stdout, stderr, code := run(t, "config", "current-profile", "-o", "json")
		if code != exit.Success {
			t.Fatalf("config current-profile -o json exited %d: %s", code, stderr)
		}

		var document map[string]any
		if err := json.Unmarshal([]byte(stdout), &document); err != nil {
			t.Fatalf("the document is not JSON: %v\n%s", err, stdout)
		}
		if document["apiVersion"] != contractAPIVersion {
			t.Errorf("apiVersion = %v, want %q", document["apiVersion"], contractAPIVersion)
		}
		if document["kind"] != "CurrentProfile" {
			t.Errorf("kind = %v, want %q", document["kind"], "CurrentProfile")
		}
		if document["currentProfile"] != profileLocal {
			t.Errorf("currentProfile = %v, want %q", document["currentProfile"], profileLocal)
		}
		// The path is here and not on stderr, which is the trade the bare form
		// makes: a program collecting these from several machines still needs to
		// tell them apart.
		if document["path"] != path {
			t.Errorf("path = %v, want %q", document["path"], path)
		}
		// Two fields and the two that say what the document is. The restraint is a
		// decision — what the profile points at is `get-profiles`' subject — and a
		// field added silently here is a field a consumer starts depending on.
		if len(document) != 4 {
			t.Errorf("the document carries %d fields (%v), want apiVersion, kind, path and "+
				"currentProfile", len(document), document)
		}
	})

	t.Run("yaml", func(t *testing.T) {
		stdout, stderr, code := run(t, "config", "current-profile", "-o", "yaml")
		if code != exit.Success {
			t.Fatalf("config current-profile -o yaml exited %d: %s", code, stderr)
		}
		// Declaration order, which every YAML document this CLI renders opens in:
		// what it is, then what it says.
		if !strings.HasPrefix(stdout, "apiVersion: "+contractAPIVersion+"\nkind: CurrentProfile\n") {
			t.Errorf("the YAML document does not open with apiVersion and kind:\n%s", stdout)
		}

		var document map[string]any
		if err := yaml.Unmarshal([]byte(stdout), &document); err != nil {
			t.Fatalf("the document is not YAML: %v\n%s", err, stdout)
		}
		if document["currentProfile"] != profileLocal {
			t.Errorf("currentProfile = %v, want %q", document["currentProfile"], profileLocal)
		}
	})

	// The kind constant is exported, so it is asserted to be the string the
	// contract above names. That is the one direction the literals cannot check.
	if cli.CurrentProfileKind != "CurrentProfile" {
		t.Errorf("cli.CurrentProfileKind = %q, want %q", cli.CurrentProfileKind, "CurrentProfile")
	}
}

// TestCurrentProfileRefusesWhatItCannotRender.
//
// Refused by name, and refused before the file is read, for the reason every
// other `config` subcommand refuses these two: `jsonl` is a streaming format for
// a result larger than memory and this is one token, and `diff` renders change
// operations that a configuration file has none of. Rendering something else
// regardless would leave a user wondering why their flag did nothing (D31).
func TestCurrentProfileRefusesWhatItCannotRender(t *testing.T) {
	configHome(t)
	writeClickHouseProfile(t)

	for _, format := range []options.OutputFormat{options.OutputJSONL, options.OutputDiff} {
		t.Run(string(format), func(t *testing.T) {
			stdout, stderr, code := run(t, "config", "current-profile", "-o", string(format))
			if code != exit.UsageError {
				t.Fatalf("-o %s exited %d, want %d", format, code, exit.UsageError)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want nothing: a refused format renders nothing", stdout)
			}
			if !strings.Contains(stderr, string(format)) {
				t.Errorf("the refusal does not name the format that was asked for:\n%s", stderr)
			}
		})
	}
}

// TestCurrentProfileRejectsAnArgumentAndSaysWhatOneWouldHaveMeant.
//
// A name typed at this command is somebody reaching for `use-profile`, so that is
// what the message names — the same judgement `get-profiles` makes about its own
// stray argument, which points at `config view` rather than at a flag.
func TestCurrentProfileRejectsAnArgumentAndSaysWhatOneWouldHaveMeant(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	stdout, stderr, code := run(t, "config", "current-profile", profileProd)
	if code != exit.UsageError {
		t.Fatalf("config current-profile with an argument exited %d, want %d", code, exit.UsageError)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing", stdout)
	}
	assertProfileGolden(t, "current-profile-argument", path, stdout, stderr, code)

	if !strings.Contains(stderr, "config use-profile "+profileProd) {
		t.Errorf("the refusal does not name the command the argument was meant for:\n%s", stderr)
	}
}

// TestCurrentProfileContactsNothing.
//
// This command reads one file. It does not even read the environment, which
// `get-profiles` does to resolve a credential reference — so a kubeconfig that
// does not exist and an address that resolves nowhere must both be irrelevant to
// it.
//
// The shape TestCompletionContactsNothing uses, for the same reason: pointing
// KUBECONFIG at nothing is what makes "it did not need a cluster" an assertion
// rather than an assumption about the machine running the tests.
func TestCurrentProfileContactsNothing(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-current-profile-test")

	path := configHome(t)
	// An address that only resolves inside a cluster, which is the fixture the
	// whole dial-diagnostic phase is about. Naming the profile must not dial it.
	writeConfigFile(t, path, `apiVersion: cli.kuberecord.io/v1alpha1
kind: Config
currentProfile: local
profiles:
  local:
    backend: clickhouse
    clickhouse:
      addr: clickhouse.kuberecord-system.svc:9000
      database: kuberecord
`)

	stdout, stderr, code := run(t, "config", "current-profile")
	if code != exit.Success {
		t.Fatalf("config current-profile exited %d without a cluster: %s", code, stderr)
	}
	if stdout != profileLocal+"\n" {
		t.Errorf("stdout = %q, want %q", stdout, profileLocal+"\n")
	}
	// Nothing about the address, in either direction. This command has not asked
	// whether that backend answers and must not appear to have an opinion — and
	// the address itself is not part of its answer.
	for _, absent := range []string{"unreachable", "reachable", "no such host", "dial",
		"clickhouse.kuberecord-system.svc"} {
		if strings.Contains(stdout+stderr, absent) {
			t.Errorf("the answer says %q about a backend it never contacted:\n%s\n%s",
				absent, stdout, stderr)
		}
	}
}
