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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"sigs.k8s.io/yaml"
)

// `config get-profiles`: the file's state, and whether any of it is usable.
//
// The renderings are pinned by golden files because the table *is* the
// deliverable, and its CREDENTIAL column is the whole reason the command exists:
// a listing that reported an unexported variable as though it were exported would
// be worse than no listing, since the reader would conclude the profile works and
// go looking for the failure somewhere else.
//
// The properties asserted beside the goldens are the ones a reworded table must
// still have: no credential value anywhere, no cluster contact, and a `*` that
// follows the file rather than this invocation's flags.

// exportedPasswordEnv is the variable the fixture's second profile names and this
// file exports, against clickHousePasswordEnv which the first names and which
// these cases guarantee is absent.
//
// Two variables rather than one toggled between cases, because the table's value
// is the contrast: `(set)` and `(not set)` on adjacent rows of one golden is the
// thing a reader is meant to be able to see at a glance.
const exportedPasswordEnv = "KUBERECORD_TEST_EXPORTED_PASSWORD"

// missingPasswordFile is a path that does not exist, written as a literal so the
// golden is the same on every machine. The present and unreadable states use
// t.TempDir() and are asserted without a golden for that reason.
const missingPasswordFile = "/etc/kuberecord/password"

// profileListingConfig is the fixture: six profiles, one per credential state
// that can be pinned in a golden, with the active one in the middle of the sorted
// order so that the `*` is visibly not just the first row.
//
// `writer` is the sixth and it is there for Task 18.5's property: it differs from
// `local` in nothing but the ClickHouse user, down to naming the same password
// variable, so the two rows are identical in every column except TARGET. A
// listing that could not tell them apart is the finding, and this is the pair a
// golden shows it on.
const profileListingConfig = `apiVersion: cli.kuberecord.io/v1alpha1
kind: Config
currentProfile: local
profiles:
  local:
    backend: clickhouse
    clickhouse:
      addr: 127.0.0.1:9000
      database: kuberecord
      username: kuberecord_ro
      passwordEnv: KUBERECORD_CLICKHOUSE_PASSWORD
  writer:
    backend: clickhouse
    clickhouse:
      addr: 127.0.0.1:9000
      database: kuberecord
      username: kuberecord
      passwordEnv: KUBERECORD_CLICKHOUSE_PASSWORD
  prod:
    backend: clickhouse
    clickhouse:
      addr: ch.observability:9000
      database: audit
      username: kuberecord_ro
      passwordEnv: KUBERECORD_TEST_EXPORTED_PASSWORD
  onfile:
    backend: clickhouse
    clickhouse:
      addr: 10.0.1.5:9000
      passwordFile: /etc/kuberecord/password
  nopass:
    backend: clickhouse
    clickhouse:
      addr: 127.0.0.1:9000
  archive:
    backend: s3
    s3:
      bucket: audit-archive
      prefix: prod
`

// profileListing writes the fixture, fixes the environment the credential column
// reads, and returns the configuration file's path.
//
// The unset half is done with t.Setenv followed by os.Unsetenv, which is the
// shape the resolution tests use: t.Setenv is what registers the restoration, and
// the removal that follows is the state the case needs. A developer with
// KUBERECORD_CLICKHOUSE_PASSWORD exported in their own shell would otherwise get
// a different table.
func profileListing(t *testing.T) string {
	t.Helper()

	path := configHome(t)
	writeConfigFile(t, path, profileListingConfig)

	t.Setenv(exportedPasswordEnv, "hunter2")
	t.Setenv(clickHousePasswordEnv, "")
	if err := os.Unsetenv(clickHousePasswordEnv); err != nil {
		t.Fatalf("unsetting %s: %v", clickHousePasswordEnv, err)
	}
	return path
}

// TestGetProfilesListsWhichProfilesThereAreAndWhichCouldAuthenticate is the
// acceptance criterion's table.
func TestGetProfilesListsWhichProfilesThereAreAndWhichCouldAuthenticate(t *testing.T) {
	path := profileListing(t)

	stdout, stderr, code := run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "get-profiles", path, stdout, stderr, code)

	// The five headings, in the order the layout puts them. Asserted beside the
	// golden because the golden pins the whole table and this says which part of
	// it people `awk` against.
	for _, heading := range []string{"CURRENT", "NAME", "BACKEND", "TARGET", "CREDENTIAL"} {
		if !strings.Contains(stdout, heading) {
			t.Errorf("the table has no %s column:\n%s", heading, stdout)
		}
	}

	// One row per profile, sorted, with the marker on the active one and nowhere
	// else. `*` follows `kubectl config get-contexts` so that the output is
	// legible without instruction, and a second marker would make it meaningless.
	rows := tableRows(t, stdout)
	if len(rows) != 6 {
		t.Fatalf("the table has %d rows, want one per profile:\n%s", len(rows), stdout)
	}
	marked := make([]string, 0, 1)
	for _, row := range rows {
		if strings.HasPrefix(row, "*") {
			marked = append(marked, strings.Fields(row)[1])
		}
	}
	if len(marked) != 1 || marked[0] != profileLocal {
		t.Errorf("the marked rows are %v, want exactly [local]:\n%s", marked, stdout)
	}

	// And the column the command exists for: the source, the reference and
	// whether it resolves, for each of the four states this fixture holds.
	for _, want := range []string{
		"env " + clickHousePasswordEnv + " (not set)",
		"env " + exportedPasswordEnv + " (set)",
		"file " + missingPasswordFile + " (missing)",
		"ambient",
		"none",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the credential column does not report %q:\n%s", want, stdout)
		}
	}

	// And Task 18.5's property, on the pair the fixture holds for it: `local` and
	// `writer` read the same database at the same address through the same password
	// variable, so before the principal joined the locator they were two rows a
	// reader could not tell apart. Asserted as the two cells being different
	// strings rather than as the spelling, which the golden pins.
	local, writer := profileRow(t, rows, profileLocal), profileRow(t, rows, "writer")
	if local == writer {
		t.Errorf("two profiles differing only by user render identically:\n%s", stdout)
	}

	// The file this is a listing of, on the other stream, so that
	// `get-profiles -o json | jq` receives the document alone.
	if !strings.Contains(stderr, path) {
		t.Errorf("stderr does not name the file the listing came from: %s", stderr)
	}
}

// profileRow returns the row for a profile, with the NAME cell taken out.
//
// The name is removed because it is the one column these two rows are *meant* to
// differ in: comparing whole rows would pass however the rest of them rendered,
// which is the opposite of the property being asserted.
func profileRow(t *testing.T, rows []string, name string) string {
	t.Helper()

	for _, row := range rows {
		fields := strings.Fields(row)
		if len(fields) > 0 && fields[0] == name {
			return strings.Join(fields[1:], " ")
		}
		// The active row carries the marker ahead of the name.
		if len(fields) > 1 && fields[0] == "*" && fields[1] == name {
			return strings.Join(fields[2:], " ")
		}
	}
	t.Fatalf("the table has no row for profile %q:\n%s", name, strings.Join(rows, "\n"))
	return ""
}

// TestGetProfilesReportsAFilePresentAndUnreadableAsDifferentStates.
//
// A password file that exists and cannot be read is not absent, and the two send
// a reader to different fixes: one file has to be created and the other has to be
// made readable. Reporting the second as the first is a claim the check did not
// verify.
//
// Not a golden, because a path under t.TempDir() would put a different string in
// it on every run — and unlike the configuration file's own path, which
// assertProfileGolden substitutes, this one appears in a cell whose width the
// layout is computed from.
func TestGetProfilesReportsAFilePresentAndUnreadableAsDifferentStates(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-0000 file, so the unreadable state cannot be produced")
	}

	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte(plantedProfilePassword), 0o600); err != nil {
		t.Fatalf("writing the password file: %v", err)
	}
	path := configHome(t)
	writeConfigFile(t, path, fmt.Sprintf(`apiVersion: cli.kuberecord.io/v1alpha1
kind: Config
currentProfile: onfile
profiles:
  onfile:
    backend: clickhouse
    clickhouse:
      addr: 10.0.1.5:9000
      passwordFile: %s
`, file))

	stdout, stderr, code := run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d: %s", code, stderr)
	}
	if want := "file " + file + " (present)"; !strings.Contains(stdout, want) {
		t.Errorf("a readable password file is not reported as %q:\n%s", want, stdout)
	}

	if err := os.Chmod(file, 0o000); err != nil {
		t.Fatalf("making the password file unreadable: %v", err)
	}
	stdout, stderr, code = run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d: %s", code, stderr)
	}
	if want := "file " + file + " (unreadable)"; !strings.Contains(stdout, want) {
		t.Errorf("an unreadable password file is not reported as %q:\n%s", want, stdout)
	}
	if strings.Contains(stdout, "(missing)") {
		t.Errorf("a file that is there is reported as absent, which is the wrong fix:\n%s", stdout)
	}
}

// TestAnEmptyConfigurationIsNotAnError.
//
// It is the state every new user is in, and the state of every user whose cluster
// has a sink custom resource to discover — who needs no profile at all. So it
// exits 0, prints the header that shows the file was read and holds nothing, and
// names the command that writes one (Invariant 9: an empty answer must be
// explicable).
func TestAnEmptyConfigurationIsNotAnError(t *testing.T) {
	path := configHome(t)

	stdout, stderr, code := run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles on an empty configuration exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "get-profiles-empty", path, stdout, stderr, code)

	if !strings.Contains(stdout, "CREDENTIAL") {
		t.Errorf("the header is missing, so an empty file is indistinguishable from an unread "+
			"one:\n%s", stdout)
	}
	if rows := tableRows(t, stdout); len(rows) != 0 {
		t.Errorf("an empty configuration produced %d rows: %v", len(rows), rows)
	}
	if !strings.Contains(stderr, "config set-profile") {
		t.Errorf("the empty listing does not name the command that writes a profile: %s", stderr)
	}
}

// TestGetProfilesRendersTheVersionedDocument.
//
// The document is a public contract (D19), so the apiVersion is asserted as a
// literal rather than read from the constant that defines it — a test reading the
// constant would pass after somebody changed it.
func TestGetProfilesRendersTheVersionedDocument(t *testing.T) {
	path := profileListing(t)

	stdout, stderr, code := run(t, "config", "get-profiles", "-o", "json")
	if code != exit.Success {
		t.Fatalf("config get-profiles -o json exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "get-profiles-json", path, stdout, stderr, code)

	var document profileListingDocument
	if err := json.Unmarshal([]byte(stdout), &document); err != nil {
		t.Fatalf("the document does not parse as JSON: %v\n%s", err, stdout)
	}
	if document.APIVersion != contractAPIVersion || document.Kind != "Profiles" {
		t.Errorf("the document is %s/%s, want %s/Profiles",
			document.APIVersion, document.Kind, contractAPIVersion)
	}
	if document.Path != path || document.CurrentProfile != profileLocal {
		t.Errorf("the document names %q/%q, want %q/%q",
			document.Path, document.CurrentProfile, path, profileLocal)
	}

	// Sorted, which is the order every other listing of profiles in this CLI uses
	// and the only deterministic one available.
	names := make([]string, 0, len(document.Profiles))
	for _, entry := range document.Profiles {
		names = append(names, entry.Name)
	}
	if want := []string{"archive", "local", "nopass", "onfile", "prod", "writer"}; strings.Join(names, ",") !=
		strings.Join(want, ",") {
		t.Errorf("the document lists %v, want %v sorted", names, want)
	}

	byName := make(map[string]profileListingEntry, len(document.Profiles))
	for _, entry := range document.Profiles {
		byName[entry.Name] = entry
	}
	for _, tc := range []struct {
		name       string
		current    bool
		backend    string
		target     string
		credential profileListingCredential
	}{
		{
			name: "local", current: true, backend: "clickhouse",
			target: "kuberecord_ro@127.0.0.1:9000/kuberecord",
			credential: profileListingCredential{
				Source: "env", Reference: clickHousePasswordEnv, State: "not set",
			},
		},
		{
			// Identical to `local` in every field but this one, which is the whole
			// of what the principal in the target is here to distinguish.
			name: "writer", backend: "clickhouse",
			target: "kuberecord@127.0.0.1:9000/kuberecord",
			credential: profileListingCredential{
				Source: "env", Reference: clickHousePasswordEnv, State: "not set",
			},
		},
		{
			name: "prod", backend: "clickhouse", target: "kuberecord_ro@ch.observability:9000/audit",
			credential: profileListingCredential{
				Source: "env", Reference: exportedPasswordEnv, State: "set",
			},
		},
		{
			// No user in the stanza, so the target names the one the driver would
			// authenticate as rather than leaving the cell to start with an `@`.
			name: "onfile", backend: "clickhouse", target: "default@10.0.1.5:9000/kuberecord",
			credential: profileListingCredential{
				Source: "file", Reference: missingPasswordFile, State: "missing",
			},
		},
		{
			name: "nopass", backend: "clickhouse", target: "default@127.0.0.1:9000/kuberecord",
			credential: profileListingCredential{Source: "none", State: "not checked"},
		},
		{
			name: "archive", backend: "s3", target: "s3://audit-archive/prod",
			credential: profileListingCredential{Source: "ambient", State: "not checked"},
		},
	} {
		entry := byName[tc.name]
		if entry.Current != tc.current || entry.Backend != tc.backend || entry.Target != tc.target {
			t.Errorf("%s is reported as %+v, want current=%v backend=%s target=%s",
				tc.name, entry, tc.current, tc.backend, tc.target)
		}
		if entry.Credential != tc.credential {
			t.Errorf("%s's credential is %+v, want %+v", tc.name, entry.Credential, tc.credential)
		}
	}

	// The two serializations are one document in two syntaxes. YAML is compared
	// by round-tripping rather than by a second set of expectations, which is
	// what makes that a property and not two assertions that happen to agree.
	yamlOut, stderr, code := run(t, "config", "get-profiles", "-o", "yaml")
	if code != exit.Success {
		t.Fatalf("config get-profiles -o yaml exited %d: %s", code, stderr)
	}
	converted, err := yaml.YAMLToJSON([]byte(yamlOut))
	if err != nil {
		t.Fatalf("the YAML document does not convert: %v\n%s", err, yamlOut)
	}
	var fromYAML profileListingDocument
	if err := json.Unmarshal(converted, &fromYAML); err != nil {
		t.Fatalf("the converted YAML does not parse: %v", err)
	}
	if fmt.Sprintf("%+v", fromYAML) != fmt.Sprintf("%+v", document) {
		t.Errorf("the YAML and JSON documents differ:\n yaml: %+v\n json: %+v", fromYAML, document)
	}
}

// TestGetProfilesCarriesNoStanza.
//
// `config view -o json` renders the file, stanzas and all. Repeating them here
// would be a second spelling of one piece of data reached from the command whose
// subject is something else — and the fields a stanza holds are the ones this
// document deliberately summarizes.
func TestGetProfilesCarriesNoStanza(t *testing.T) {
	profileListing(t)

	stdout, stderr, code := run(t, "config", "get-profiles", "-o", "json")
	if code != exit.Success {
		t.Fatalf("config get-profiles -o json exited %d: %s", code, stderr)
	}
	for _, key := range []string{`"clickhouse"`, `"s3"`, `"addr"`, `"passwordEnv"`, `"bucket"`} {
		if strings.Contains(stdout, key+":") {
			t.Errorf("the document carries the stanza key %s; `config view` is the command "+
				"whose subject is the file:\n%s", key, stdout)
		}
	}
}

// TestGetProfilesRefusesTheFormatsItCannotRender.
//
// By name rather than by rendering something else regardless, which is the rule
// the whole `-o` surface follows: a user who asked for one shape and received
// another has been answered in a form their script cannot read, and finding that
// out at the `jq` is worse than finding it out here (D31).
func TestGetProfilesRefusesTheFormatsItCannotRender(t *testing.T) {
	profileListing(t)

	for _, format := range []options.OutputFormat{options.OutputJSONL, options.OutputDiff} {
		t.Run(string(format), func(t *testing.T) {
			stdout, stderr, code := run(t, "config", "get-profiles", "-o", string(format))
			if code != exit.UsageError {
				t.Fatalf("-o %s exited %d, want %d", format, code, exit.UsageError)
			}
			if stdout != "" {
				t.Errorf("a refused format still wrote to stdout: %q", stdout)
			}
			if !strings.Contains(stderr, string(format)) {
				t.Errorf("the refusal does not name the format that was asked for: %s", stderr)
			}
		})
	}

	// `wide` is accepted and renders exactly what `table` does, which is that
	// flag's guarantee honoured rather than dropped: nothing here is elided at
	// any width, so there is nothing for `wide` to add.
	table, _, code := run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d", code)
	}
	wide, stderr, code := run(t, "config", "get-profiles", "-o", "wide")
	if code != exit.Success {
		t.Fatalf("config get-profiles -o wide exited %d: %s", code, stderr)
	}
	if wide != table {
		t.Errorf("-o wide renders something else:\n--- table ---\n%s\n--- wide ---\n%s", table, wide)
	}
}

// plantedProfilePassword is the value that must never reach any rendering. It is
// distinctive so that a substring search for it is searching for something.
const plantedProfilePassword = "correct-horse-battery-staple"

// TestGetProfilesNeverPrintsACredential.
//
// Two plants, because there are two references a profile can hold and the value
// behind each is read a few lines from where this listing is assembled: the
// environment variable's value, and the content of the password file — which
// resolve.Profile.Credential reads in order to decide whether the reference
// resolves at all.
//
// All four renderings, and both streams. `-o json` is the one that matters most:
// it is the rendering that ends up in a bug report.
func TestGetProfilesNeverPrintsACredential(t *testing.T) {
	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte(plantedProfilePassword), 0o600); err != nil {
		t.Fatalf("writing the password file: %v", err)
	}
	path := configHome(t)
	writeConfigFile(t, path, fmt.Sprintf(`apiVersion: cli.kuberecord.io/v1alpha1
kind: Config
currentProfile: local
profiles:
  local:
    backend: clickhouse
    clickhouse:
      addr: 127.0.0.1:9000
      passwordEnv: %s
  onfile:
    backend: clickhouse
    clickhouse:
      addr: 10.0.1.5:9000
      passwordFile: %s
`, exportedPasswordEnv, file))
	t.Setenv(exportedPasswordEnv, plantedProfilePassword)

	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			for _, verbosity := range []string{"0", "9"} {
				stdout, stderr, code := run(t, "config", "get-profiles", "-o", format, "-v", verbosity)
				if code != exit.Success {
					t.Fatalf("config get-profiles -o %s exited %d: %s", format, code, stderr)
				}
				if strings.Contains(stdout, plantedProfilePassword) {
					t.Errorf("-o %s printed the password to stdout:\n%s", format, stdout)
				}
				if strings.Contains(stderr, plantedProfilePassword) {
					t.Errorf("-o %s printed the password to stderr at -v %s:\n%s",
						format, verbosity, stderr)
				}
				// The reference itself must still be there: a column that named
				// neither the variable nor its state would be a column with
				// nothing in it.
				if format == "table" && !strings.Contains(stdout, exportedPasswordEnv+" (set)") {
					t.Errorf("the credential column does not report the reference:\n%s", stdout)
				}
			}
		})
	}
}

// TestGetProfilesContactsNothing.
//
// This command reads a file and the environment. Reachability belongs to
// `config resolve --check`, and a second command that dialled would be two
// answers to one question that could differ — so a kubeconfig that does not exist
// and an address that resolves nowhere must both be irrelevant to it.
//
// The shape TestCompletionContactsNothing uses, for the same reason: pointing
// KUBECONFIG at nothing is what makes "it did not need a cluster" an assertion
// rather than an assumption about the machine running the tests.
func TestGetProfilesContactsNothing(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-profile-listing")

	path := configHome(t)
	// An address that only resolves inside a cluster, which is the fixture the
	// whole dial-diagnostic phase is about. Listing it must not dial it.
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

	stdout, stderr, code := run(t, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d without a cluster: %s", code, stderr)
	}
	if !strings.Contains(stdout, "clickhouse.kuberecord-system.svc:9000/kuberecord") {
		t.Errorf("the listing does not report the profile it read:\n%s", stdout)
	}
	// Nothing about reachability, in either direction: this command has not asked
	// and must not appear to have an opinion.
	for _, absent := range []string{"unreachable", "reachable", "no such host", "dial"} {
		if strings.Contains(stdout+stderr, absent) {
			t.Errorf("the listing says %q about a backend it never contacted:\n%s\n%s",
				absent, stdout, stderr)
		}
	}
}

// TestGetProfilesReportsTheFileRatherThanTheInvocation.
//
// CURRENT is the file's active pointer, as the `*` in `kubectl config
// get-contexts` is the file's current context. What *this* invocation would
// resolve to is a different question with nine steps behind it, and D26 gives it
// to `config resolve` — so --profile does not move the marker, and the noop audit
// row for that flag is what says why a command walking no chain ignores it.
func TestGetProfilesReportsTheFileRatherThanTheInvocation(t *testing.T) {
	profileListing(t)

	stdout, stderr, code := run(t, "--profile", profileProd, "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles exited %d: %s", code, stderr)
	}
	for _, row := range tableRows(t, stdout) {
		fields := strings.Fields(row)
		marked := strings.HasPrefix(row, "*")
		if marked && fields[1] != profileLocal {
			t.Errorf("--%s moved the CURRENT marker to %q; the column is the file's active "+
				"pointer:\n%s", options.FlagProfile, fields[1], stdout)
		}
	}
}

// TestGetProfilesTakesNoArguments, with its own sentence rather than the one
// `scopes` writes: a reader told to narrow this listing with --kind and
// --namespace would go looking for two flags it does not have.
func TestGetProfilesTakesNoArguments(t *testing.T) {
	profileListing(t)

	stdout, stderr, code := run(t, "config", "get-profiles", profileProd)
	if code != exit.UsageError {
		t.Fatalf("a stray argument exited %d, want %d", code, exit.UsageError)
	}
	if stdout != "" {
		t.Errorf("a refused invocation still wrote a listing: %q", stdout)
	}
	if !strings.Contains(stderr, "config view") {
		t.Errorf("the refusal does not name the command that renders one profile's stanza: %s", stderr)
	}
}

// TestGetProfilesColourIsNothingButColour.
//
// The layout is computed over unpainted text and the colour applied afterwards,
// because escape sequences carry no display width: a table padded over painted
// cells wobbles by the length of its codes, on a terminal, which is the one place
// nobody runs the tests. The heading is the only painted thing — a cell here is a
// value somebody copies, and the register that would suit `(not set)` is the one
// D30 reserves for lines a reader must not skip.
func TestGetProfilesColourIsNothingButColour(t *testing.T) {
	profileListing(t)

	plain, stderr, code := run(t, "--color=never", "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles --color=never exited %d: %s", code, stderr)
	}
	painted, stderr, code := run(t, "--color=always", "config", "get-profiles")
	if code != exit.Success {
		t.Fatalf("config get-profiles --color=always exited %d: %s", code, stderr)
	}

	if !strings.Contains(painted, "\x1b[") {
		t.Fatal("--color=always produced no colour at all")
	}
	if stripped := ansiSequence.ReplaceAllString(painted, ""); stripped != plain {
		t.Errorf("colour changed the layout.\n--- without ---\n%s\n--- with, stripped ---\n%s",
			plain, stripped)
	}
	// The heading and nothing else. A painted cell would be this table asserting
	// an importance it has no vocabulary for.
	for _, line := range strings.Split(strings.TrimRight(painted, "\n"), "\n")[1:] {
		if strings.Contains(line, "\x1b[") {
			t.Errorf("a row of the table is painted, not only the heading: %q", line)
		}
	}
}

// tableRows returns the listing's rows without its heading.
func tableRows(t *testing.T, stdout string) []string {
	t.Helper()

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "CURRENT") {
		t.Fatalf("the listing does not begin with the heading row:\n%s", stdout)
	}
	return lines[1:]
}

// The document, restated for the test rather than read from the package.
//
// Restated because it is a public contract asserted from outside: a test that
// unmarshalled into the production struct would pass after a field was renamed,
// which is the one change the additive-only policy forbids (D19).
type profileListingDocument struct {
	APIVersion     string                `json:"apiVersion"`
	Kind           string                `json:"kind"`
	Path           string                `json:"path"`
	CurrentProfile string                `json:"currentProfile"`
	Profiles       []profileListingEntry `json:"profiles"`
}

type profileListingEntry struct {
	Name       string                   `json:"name"`
	Current    bool                     `json:"current"`
	Backend    string                   `json:"backend"`
	Target     string                   `json:"target"`
	Credential profileListingCredential `json:"credential"`
}

type profileListingCredential struct {
	Source    string `json:"source"`
	Reference string `json:"reference"`
	State     string `json:"state"`
}
