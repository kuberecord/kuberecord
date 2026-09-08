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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"sigs.k8s.io/yaml"
)

// The profile lifecycle: create, inspect, switch, delete.
//
// Two findings are under test here and they are the same one twice. `set-profile`
// against an existing name overwrote a stanza and said only "replaced", so a
// hand-tuned profile could be destroyed by a command the wizard routes new users
// into and the only account of what was lost was the word itself. And there was
// no way to remove a profile at all, which left a stale one shadowing discovery
// from step 3 of the resolution chain with nothing but a text editor as the fix.
//
// The renderings are pinned by golden files because the messages *are* the
// deliverable: an upsert whose line does not name the profile it replaced is the
// defect, not a wording preference.

// profileGoldens is the testdata subdirectory these cases pin their output in.
const profileGoldens = "config-profile"

// profileLocal is the profile every case here writes and reads back, named once
// so that an assertion reading a *different* profile is a compile error rather
// than a passing test about the wrong stanza. It is spelled the way the quickstart
// spells the profile it walks a new user into writing.
const profileLocal = "local"

// contextName and remappedCluster are the kubeconfig context the mapping case
// writes and the identity it is then re-pointed at. theCluster, from
// resolve_test.go, is the identity it maps to first.
const (
	contextName     = "prod-eu"
	remappedCluster = "prod-eu-2"
)

// contractAPIVersion is the group the structured documents carry (D19).
//
// A literal rather than render.EnvelopeAPIVersion, deliberately: this is a public
// contract asserted from outside the package, and a test that read the constant
// would pass after somebody changed it.
const contractAPIVersion = "cli.kuberecord.io/v1alpha1"

// assertProfileGolden compares one invocation's two streams and its exit code
// against the checked-in rendering.
//
// The configuration file's path is substituted for the fixed one the
// `config resolve` goldens use. These cases have to write a real file, so the
// path is a t.TempDir() and would otherwise put a different string in the golden
// on every run — and the path is in almost every line, since a message about a
// configuration file that did not say which file would be the uninformative error
// Invariant 4 rules out.
//
// The exit code is a section of its own rather than a separate assertion because
// half of what these commands promise is which code they fail with: a refusal
// that exited 1 would be retried forever by a wrapper script told to retry on 1
// and stop on 2.
func assertProfileGolden(t *testing.T, name, path, stdout, stderr string, code int) {
	t.Helper()

	stable := func(text string) string { return strings.ReplaceAll(text, path, stableConfigPath) }
	assertGoldenDocument(t, profileGoldens, name,
		stdoutMarker+stable(stdout)+stderrMarker+stable(elideUsageBlock(stderr))+
			exitMarker+exitCodeName(code)+"\n")
}

// elideUsageBlock replaces cobra's usage block with one line.
//
// It is dropped rather than pinned for two reasons that both matter. It is the
// command's own --help, rendered by cobra from the whole flag surface, so a
// golden holding it would be a second copy of --help that churns on every
// unrelated flag added anywhere in the tree — and it carries this machine's
// kubeconfig cache directory in a default value, so the golden would fail on
// somebody else's laptop and in CI. That the block accompanies a usage error at
// all is asserted where it belongs, by TestUsageErrorsCarryTheUsageBlock.
func elideUsageBlock(stderr string) string {
	at := strings.Index(stderr, "\nUsage:\n")
	if at < 0 {
		return stderr
	}
	return stderr[:at+1] + "<the command's usage block>\n"
}

// stableConfigPath is the path the goldens name, spelled as resolvecmd_test.go's
// fixture spells it so that a reader of one set of goldens reads all of them.
const stableConfigPath = resolveConfigPath

// exitMarker is this harness's third section.
const exitMarker = "=== exit ===\n"

// exitCodeName renders a code as the number and the name docs/CLI.md's table
// gives it, so a golden that changes says which contract changed.
func exitCodeName(code int) string {
	switch code {
	case exit.Success:
		return "0 (success)"
	case exit.RuntimeError:
		return "1 (runtime error)"
	case exit.UsageError:
		return "2 (usage error)"
	case exit.NoCoverage:
		return "3 (no coverage)"
	}
	return "unknown"
}

// writeClickHouseProfile is the setup every case here starts from: one profile
// naming a password *file*, so that a rewrite naming a variable instead can be
// checked for the reference it must not keep.
func writeClickHouseProfile(t *testing.T) {
	t.Helper()
	_, stderr, code := run(t, "config", "set-profile", profileLocal,
		"--backend", "clickhouse",
		"--addr", "10.0.1.5:9000",
		"--database", "kuberecord",
		"--username", "kuberecord_ro",
		"--password-file", "/etc/kuberecord/password")
	if code != exit.Success {
		t.Fatalf("writing the profile under test exited %d: %s", code, stderr)
	}
}

// TestSetProfileIsAnUpsertThatNamesWhatItReplaced is the first finding, fixed.
//
// Overwriting was already the behaviour; what was missing was any account of it.
// The line has to name the profile that is gone because nothing else does: the
// file holds the new stanza, and the old one exists nowhere once it is saved.
func TestSetProfileIsAnUpsertThatNamesWhatItReplaced(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	stdout, stderr, code := run(t, "config", "set-profile", profileLocal,
		"--backend", "clickhouse",
		"--addr", "127.0.0.1:9000",
		"--database", "kuberecord",
		"--password-env", "KUBERECORD_CLICKHOUSE_PASSWORD")
	if code != exit.Success {
		t.Fatalf("the upsert exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "updated", path, stdout, stderr, code)

	// The verb, and the description of what it replaced. Asserted beside the
	// golden because the golden pins the whole line and this says which part of it
	// carries the contract.
	if !strings.Contains(stderr, `updated profile "local"`) {
		t.Errorf("stderr does not report an update: %s", stderr)
	}
	if !strings.Contains(stderr, "was: ClickHouse at 10.0.1.5:9000/kuberecord") {
		t.Errorf("stderr does not name the profile that was replaced: %s", stderr)
	}

	// Whole-stanza replacement. The password *file* the first write named must be
	// gone, not merged into a profile that now also names a variable — which the
	// file would refuse anyway, and that refusal arriving from SaveConfig instead
	// of from here would be the merge failing rather than the merge being absent.
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	stanza := cfg.Profiles[profileLocal].ClickHouse
	if stanza == nil {
		t.Fatalf("the file holds no ClickHouse profile named local: %+v", cfg.Profiles)
	}
	if stanza.PasswordFile != "" {
		t.Errorf("the rewritten profile still names the password file %q: an upsert replaces the "+
			"whole stanza", stanza.PasswordFile)
	}
	if stanza.PasswordEnv != "KUBERECORD_CLICKHOUSE_PASSWORD" {
		t.Errorf("passwordEnv = %q, want what the second write named", stanza.PasswordEnv)
	}
	// The same rule about a field the second write did not mention at all.
	if stanza.Username != "" {
		t.Errorf("username = %q, want empty: the second command did not name it, and a merge "+
			"would have carried it over", stanza.Username)
	}
	if stanza.Addr != "127.0.0.1:9000" {
		t.Errorf("addr = %q, want the address the second write named", stanza.Addr)
	}
}

// TestSetProfileCreatesQuietly is the other half: the ordinary case is not
// dressed up as a replacement.
//
// It matters because the two lines differ in register as well as in wording. A
// creation that printed the provenance-tier line would spend the tier on the
// outcome somebody expected, and the replacement would then have nothing left to
// distinguish it.
func TestSetProfileCreatesQuietly(t *testing.T) {
	path := configHome(t)

	stdout, stderr, code := run(t, "config", "set-profile", profileLocal,
		"--backend", "clickhouse", "--addr", "10.0.1.5:9000", "--database", "kuberecord",
		"--username", "kuberecord_ro", "--password-file", "/etc/kuberecord/password")
	if code != exit.Success {
		t.Fatalf("config set-profile exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "created", path, stdout, stderr, code)

	if strings.Contains(stderr, "was:") {
		t.Errorf("a first write reports replacing something: %s", stderr)
	}
}

// TestUpsertLeavesTheActivePointerAlone.
//
// Replacing a profile is a write to one stanza and says nothing about which
// profile is active. The rule is the one `set-profile` already followed for a
// second profile — an existing choice is never overridden — and it has to keep
// holding when the name being written is one the file already has, since that is
// the case where a pointer *could* plausibly be re-decided.
func TestUpsertLeavesTheActivePointerAlone(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	if _, stderr, code := run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit"); code != exit.Success {
		t.Fatalf("writing the second profile exited %d: %s", code, stderr)
	}
	if _, stderr, code := run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit", "--prefix", "kuberecord"); code != exit.Success {
		t.Fatalf("the upsert exited %d: %s", code, stderr)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != profileLocal {
		t.Errorf("currentProfile = %q, want it still local: an upsert writes a stanza and does not "+
			"decide which profile is active", cfg.CurrentProfile)
	}
}

// TestDeleteProfileRemovesTheStanza is the ordinary deletion: a profile that is
// not the active one.
func TestDeleteProfileRemovesTheStanza(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)
	if _, stderr, code := run(t, "config", "set-profile", "stale",
		"--backend", "local", "--path", "/archives/kuberecord"); code != exit.Success {
		t.Fatalf("writing the profile to be deleted exited %d: %s", code, stderr)
	}

	stdout, stderr, code := run(t, "config", "delete-profile", "stale")
	if code != exit.Success {
		t.Fatalf("config delete-profile exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "deleted", path, stdout, stderr, code)

	// What was removed, so the deletion is auditable in scrollback. Nothing else
	// holds that stanza once the file is written.
	if !strings.Contains(stderr, "local archive at /archives/kuberecord") {
		t.Errorf("stderr does not say what was removed: %s", stderr)
	}
	// Nothing about the active pointer, because nothing about it changed.
	if strings.Contains(stderr, "no profile is active") {
		t.Errorf("deleting a profile that was not active reported clearing the pointer: %s", stderr)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig of what the command left behind: %v", err)
	}
	if _, ok := cfg.Profiles["stale"]; ok {
		t.Error("the deleted profile is still in the file")
	}
	if _, ok := cfg.Profiles[profileLocal]; !ok {
		t.Errorf("deleting one profile removed another: %+v", cfg.Profiles)
	}
	if cfg.CurrentProfile != profileLocal {
		t.Errorf("currentProfile = %q, want it untouched at local", cfg.CurrentProfile)
	}
}

// TestDeleteProfileRefusesTheActiveOneAndNamesBothRoutesPastIt is the first half
// of the active-profile rule (D34).
//
// Both routes, because they are different decisions: switching first keeps a
// profile active and is what somebody with a replacement wants, and --force
// leaves the chain to fall through to discovery and is what somebody clearing up
// wants. And nothing may be written, since a refusal that had already half
// applied itself would be worse than one that had not refused at all.
func TestDeleteProfileRefusesTheActiveOneAndNamesBothRoutesPastIt(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)
	if _, stderr, code := run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit"); code != exit.Success {
		t.Fatalf("writing the second profile exited %d: %s", code, stderr)
	}

	stdout, stderr, code := run(t, "config", "delete-profile", profileLocal)
	if code != exit.UsageError {
		t.Fatalf("deleting the active profile exited %d, want %d", code, exit.UsageError)
	}
	assertProfileGolden(t, "refused-active", path, stdout, stderr, code)

	for _, route := range []string{
		"config use-profile archive",
		"config delete-profile local --force",
	} {
		if !strings.Contains(stderr, route) {
			t.Errorf("the refusal does not name the route `%s`:\n%s", route, stderr)
		}
	}
	// With one other profile the switch suggestion has already named it, so the
	// sentence does not go on to list it a second time.
	if strings.Contains(stderr, "also defined") {
		t.Errorf("the refusal names the one alternative twice:\n%s", stderr)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if _, ok := cfg.Profiles[profileLocal]; !ok {
		t.Error("the refused deletion removed the profile anyway")
	}
	if cfg.CurrentProfile != profileLocal {
		t.Errorf("currentProfile = %q, want it unchanged by a refusal", cfg.CurrentProfile)
	}
}

// TestDeleteProfileWithForceClearsTheActivePointer is the second half, and both
// halves of it are asserted.
//
// The stanza going away is only half the promise. A file left naming a
// currentProfile that no longer exists would fail to load at all — Config.Validate
// refuses it — so the next command would not resolve to a missing profile, it
// would refuse to read the file. Clearing the pointer is what makes --force a
// deletion rather than a way to corrupt the configuration.
func TestDeleteProfileWithForceClearsTheActivePointer(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)
	if _, stderr, code := run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit"); code != exit.Success {
		t.Fatalf("writing the second profile exited %d: %s", code, stderr)
	}

	stdout, stderr, code := run(t, "config", "delete-profile", profileLocal, "--force")
	if code != exit.Success {
		t.Fatalf("config delete-profile --force exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "deleted-active-force", path, stdout, stderr, code)

	// The pointer being cleared is announced, and the announcement names the
	// command that chooses the next one — there is another profile to choose.
	if !strings.Contains(stderr, "no profile is active now") {
		t.Errorf("stderr does not say the active pointer was cleared: %s", stderr)
	}
	if !strings.Contains(stderr, "config use-profile archive") {
		t.Errorf("stderr does not name the command that chooses another profile: %s", stderr)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig of what --force left behind: %v", err)
	}
	if _, ok := cfg.Profiles[profileLocal]; ok {
		t.Error("--force did not remove the stanza")
	}
	if cfg.CurrentProfile != "" {
		t.Errorf("currentProfile = %q, want it cleared", cfg.CurrentProfile)
	}
}

// TestDeleteTheOnlyProfileIsToldThereIsNothingToSwitchTo.
//
// A remedy naming no command is worse than no remedy: it reads as though the
// reader should have known which name to substitute. With one profile in the file
// there is nothing to switch to, so the refusal says that and offers the one route
// that exists — and the --force message that follows stops after the line that is
// still true, since there is no other profile to choose.
func TestDeleteTheOnlyProfileIsToldThereIsNothingToSwitchTo(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	stdout, stderr, code := run(t, "config", "delete-profile", profileLocal)
	if code != exit.UsageError {
		t.Fatalf("exited %d, want %d", code, exit.UsageError)
	}
	assertProfileGolden(t, "refused-only-profile", path, stdout, stderr, code)

	if !strings.Contains(stderr, "the only one in this file") {
		t.Errorf("the refusal offers a switch that is not available:\n%s", stderr)
	}
	if strings.Contains(stderr, "config use-profile") {
		t.Errorf("the refusal names use-profile with no profile to name:\n%s", stderr)
	}

	stdout, stderr, code = run(t, "config", "delete-profile", profileLocal, "--force")
	if code != exit.Success {
		t.Fatalf("config delete-profile --force exited %d: %s", code, stderr)
	}
	assertProfileGolden(t, "deleted-last-profile", path, stdout, stderr, code)

	if strings.Contains(stderr, "to choose another") {
		t.Errorf("an emptied file is offered another profile to choose:\n%s", stderr)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if len(cfg.Profiles) != 0 || cfg.CurrentProfile != "" {
		t.Errorf("the file still describes a profile: %+v (current %q)",
			cfg.Profiles, cfg.CurrentProfile)
	}
}

// TestDeleteProfileNamesTheProfilesThatExist is the requireSecretKey shape,
// applied to a name that is almost always a typo or a profile written on another
// machine — both of which are settled by seeing the list.
func TestDeleteProfileNamesTheProfilesThatExist(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)
	if _, stderr, code := run(t, "config", "set-profile", "archive",
		"--backend", "s3", "--bucket", "acme-audit"); code != exit.Success {
		t.Fatalf("writing the second profile exited %d: %s", code, stderr)
	}

	stdout, stderr, code := run(t, "config", "delete-profile", "locl")
	if code != exit.UsageError {
		t.Fatalf("exited %d, want %d", code, exit.UsageError)
	}
	assertProfileGolden(t, "unknown-name", path, stdout, stderr, code)

	if !strings.Contains(stderr, "defined: archive, local") {
		t.Errorf("the error does not list the profiles that do exist:\n%s", stderr)
	}

	// And the same sentence from `use-profile`, which is why it is one function.
	_, useStderr, useCode := run(t, "config", "use-profile", "locl")
	if useCode != exit.UsageError {
		t.Fatalf("config use-profile of an unknown name exited %d, want %d", useCode, exit.UsageError)
	}
	if !strings.Contains(useStderr, "defined: archive, local") {
		t.Errorf("config use-profile does not list the profiles either:\n%s", useStderr)
	}
}

// TestProfileWritesRenderAStructuredDocument is the scriptability half.
//
// Every writing subcommand of `config` reports what it did to a program, because
// writing configuration is a step in a script and diffing the file around the
// command is not an interface. The fields under test are the ones a script
// branches on: which action happened, and what was displaced by it.
func TestProfileWritesRenderAStructuredDocument(t *testing.T) {
	path := configHome(t)
	writeClickHouseProfile(t)

	type change struct {
		APIVersion     string           `json:"apiVersion"`
		Kind           string           `json:"kind"`
		Action         string           `json:"action"`
		Name           string           `json:"name"`
		Path           string           `json:"path"`
		Profile        *resolve.Profile `json:"profile"`
		Previous       *resolve.Profile `json:"previous"`
		CurrentProfile string           `json:"currentProfile"`
	}

	decode := func(t *testing.T, stdout string) change {
		t.Helper()
		var document change
		if err := json.Unmarshal([]byte(stdout), &document); err != nil {
			t.Fatalf("the document on stdout is not JSON: %v\n%s", err, stdout)
		}
		if document.APIVersion != contractAPIVersion || document.Kind != "ProfileChange" {
			t.Errorf("the document identifies itself as %s/%s", document.APIVersion, document.Kind)
		}
		if document.Path != path {
			t.Errorf("path = %q, want %q", document.Path, path)
		}
		return document
	}

	t.Run("an upsert reports the stanza it displaced", func(t *testing.T) {
		stdout, stderr, code := run(t, "-o", "json", "config", "set-profile", profileLocal,
			"--backend", "clickhouse", "--addr", "127.0.0.1:9000")
		if code != exit.Success {
			t.Fatalf("exited %d: %s", code, stderr)
		}
		document := decode(t, stdout)
		if document.Action != "updated" {
			t.Errorf("action = %q, want updated", document.Action)
		}
		if document.Previous == nil || document.Previous.ClickHouse == nil ||
			document.Previous.ClickHouse.Addr != "10.0.1.5:9000" {
			t.Errorf("previous does not carry the displaced stanza: %+v", document.Previous)
		}
		if document.Profile == nil || document.Profile.ClickHouse == nil ||
			document.Profile.ClickHouse.Addr != "127.0.0.1:9000" {
			t.Errorf("profile does not carry what was written: %+v", document.Profile)
		}
		if document.CurrentProfile != profileLocal {
			t.Errorf("currentProfile = %q, want local", document.CurrentProfile)
		}
	})

	t.Run("a creation reports no previous stanza", func(t *testing.T) {
		stdout, stderr, code := run(t, "-o", "json", "config", "set-profile", "archive",
			"--backend", "s3", "--bucket", "acme-audit")
		if code != exit.Success {
			t.Fatalf("exited %d: %s", code, stderr)
		}
		document := decode(t, stdout)
		if document.Action != "created" || document.Previous != nil {
			t.Errorf("action = %q with previous %+v, want created and nothing displaced",
				document.Action, document.Previous)
		}
	})

	t.Run("use-profile reports the pointer it moved", func(t *testing.T) {
		stdout, stderr, code := run(t, "-o", "json", "config", "use-profile", "archive")
		if code != exit.Success {
			t.Fatalf("exited %d: %s", code, stderr)
		}
		document := decode(t, stdout)
		if document.Action != "activated" || document.CurrentProfile != "archive" {
			t.Errorf("action = %q with currentProfile %q, want activated and archive",
				document.Action, document.CurrentProfile)
		}
		// Nothing was displaced: the profile it moved away from is still there.
		if document.Previous != nil {
			t.Errorf("activating a profile reports displacing one: %+v", document.Previous)
		}
	})

	t.Run("a deletion reports the stanza and the cleared pointer", func(t *testing.T) {
		stdout, stderr, code := run(t, "-o", "json",
			"config", "delete-profile", "archive", "--force")
		if code != exit.Success {
			t.Fatalf("exited %d: %s", code, stderr)
		}
		document := decode(t, stdout)
		if document.Action != "deleted" {
			t.Errorf("action = %q, want deleted", document.Action)
		}
		if document.Profile != nil {
			t.Errorf("a deleted profile still carries a stanza: %+v", document.Profile)
		}
		if document.Previous == nil || document.Previous.S3 == nil ||
			document.Previous.S3.Bucket != "acme-audit" {
			t.Errorf("previous does not carry what was removed: %+v", document.Previous)
		}
		// Present and empty, so a consumer reads the state rather than inferring
		// it from a missing key.
		if !strings.Contains(stdout, `"currentProfile": ""`) {
			t.Errorf("the cleared pointer is not rendered as an empty value:\n%s", stdout)
		}
	})
}

// TestContextMappingRendersAStructuredDocumentAndReportsARemap.
//
// `set-context-cluster-id` is an upsert too, and the same class of silent
// overwrite: a mapping is what makes `--context prod-eu` carry an identity, so a
// reader who has just pointed a context at a different cluster's history is a
// reader who will trust the next answer for the wrong reason.
func TestContextMappingRendersAStructuredDocumentAndReportsARemap(t *testing.T) {
	path := configHome(t)

	stdout, stderr, code := run(t, "-o", "yaml",
		"config", "set-context-cluster-id", contextName, theCluster)
	if code != exit.Success {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	if strings.Contains(stderr, "was:") {
		t.Errorf("a first mapping reports replacing something: %s", stderr)
	}

	var mapping struct {
		APIVersion        string `json:"apiVersion"`
		Kind              string `json:"kind"`
		Context           string `json:"context"`
		ClusterID         string `json:"clusterID"`
		PreviousClusterID string `json:"previousClusterID"`
		Path              string `json:"path"`
	}
	if err := yaml.Unmarshal([]byte(stdout), &mapping); err != nil {
		t.Fatalf("the document on stdout is not YAML: %v\n%s", err, stdout)
	}
	if mapping.Kind != "ContextMapping" || mapping.APIVersion != contractAPIVersion {
		t.Errorf("the document identifies itself as %s/%s", mapping.APIVersion, mapping.Kind)
	}
	if mapping.Context != contextName || mapping.ClusterID != theCluster || mapping.Path != path {
		t.Errorf("the document does not describe the write: %+v", mapping)
	}
	if mapping.PreviousClusterID != "" {
		t.Errorf("previousClusterID = %q on a context that mapped to nothing",
			mapping.PreviousClusterID)
	}

	stdout, stderr, code = run(t, "-o", "json",
		"config", "set-context-cluster-id", contextName, remappedCluster)
	if code != exit.Success {
		t.Fatalf("the remap exited %d: %s", code, stderr)
	}
	if !strings.Contains(stderr, fmt.Sprintf("(was: %q)", theCluster)) {
		t.Errorf("the remap does not name the identity it replaced: %s", stderr)
	}
	if err := json.Unmarshal([]byte(stdout), &mapping); err != nil {
		t.Fatalf("the document on stdout is not JSON: %v\n%s", err, stdout)
	}
	if mapping.PreviousClusterID != theCluster {
		t.Errorf("previousClusterID = %q, want the identity that was replaced",
			mapping.PreviousClusterID)
	}
}

// TestConfigWritesRefuseTheFormatsTheyCannotRender, and refuse them before
// anything reaches the file.
//
// The second half is the one worth a test. A format decided after the write would
// leave a user with a rewritten configuration, a non-zero exit and no rendering —
// which is the worst of the three outcomes, because it is the one that looks like
// nothing happened.
func TestConfigWritesRefuseTheFormatsTheyCannotRender(t *testing.T) {
	for _, format := range []options.OutputFormat{options.OutputJSONL, options.OutputDiff} {
		for _, args := range [][]string{
			{"config", "set-profile", profileLocal, "--backend", "s3", "--bucket", "acme-audit"},
			{"config", "use-profile", profileLocal},
			{"config", "delete-profile", profileLocal},
			{"config", "set-context-cluster-id", contextName, theCluster},
		} {
			t.Run(string(format)+" "+strings.Join(args[:2], " "), func(t *testing.T) {
				path := configHome(t)
				stdout, stderr, code := run(t, append([]string{"-o", string(format)}, args...)...)

				if code != exit.UsageError {
					t.Fatalf("exited %d, want %d: %s", code, exit.UsageError, stderr)
				}
				if !strings.Contains(stderr, string(format)) {
					t.Errorf("the refusal does not name the format asked for:\n%s", stderr)
				}
				if stdout != "" {
					t.Errorf("a refused format still wrote to stdout: %q", stdout)
				}
				if _, err := resolve.LoadConfig(path); err != nil {
					t.Fatalf("resolve.LoadConfig: %v", err)
				}
				if cfg, _ := resolve.LoadConfig(path); len(cfg.Profiles) != 0 ||
					len(cfg.Contexts) != 0 {
					t.Errorf("the refusal came after the write: %+v", cfg)
				}
			})
		}
	}
}
